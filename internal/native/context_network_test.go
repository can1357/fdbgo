package native

import (
	"context"
	"io"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/can1357/fdbgo/internal/cluster"
	"github.com/can1357/fdbgo/internal/fdberr"
	"github.com/can1357/fdbgo/internal/transport"
	"github.com/can1357/fdbgo/internal/wire"
)

func TestMonitorProxiesPreservesLiteralCoordinatorTLS(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	requests := make(chan []byte, 1)
	server := newNativeTestServer(t, func(_ *nativeTestConn, dest wire.UID, msg []byte) {
		if dest != wire.WellKnown(wire.TokenOpenDatabase) {
			t.Errorf("unexpected coordinator endpoint: %v", dest)
			return
		}
		select {
		case requests <- append([]byte(nil), msg...):
		case <-ctx.Done():
		}
	})
	// Port zero cannot host a coordinator. These literal TLS coordinators fail
	// immediately if shuffled first; the plaintext peer captures the monitor's
	// first successful request, before any coordinator has initialized it.
	cs, err := cluster.ParseConnectionString("monitor:tls@" + server.addr().String() + ",127.0.0.1:0:tls,[::1]:0:tls")
	if err != nil {
		t.Fatal(err)
	}
	db := NewDbContext(cluster.ConnectionRecord{Conn: cs}, false)
	defer db.Close()
	var msg []byte
	select {
	case msg = <-requests:
	case <-ctx.Done():
		t.Fatal("coordinator monitor did not issue OpenDatabase")
	}
	req, err := lifecycleRequestTable(msg)
	if err != nil {
		t.Fatal(err)
	}
	coords, err := req.Vector(5)
	if err != nil || coords.Len() != len(cs.Coordinators) {
		t.Fatalf("literal coordinator vector length = %d, error %v", coords.Len(), err)
	}
	for i, want := range cs.Coordinators {
		addr, err := coords.Table(i)
		if err != nil {
			t.Fatal(err)
		}
		flags, err := addr.U16(2)
		if err != nil {
			t.Fatal(err)
		}
		wantFlags := uint16(0)
		if want.TLS {
			wantFlags = 2 // NetworkAddress::FLAG_TLS
		}
		port, err := addr.U16(1)
		if err != nil || flags != wantFlags || port != want.Addr.Port() {
			t.Fatalf("coordinator %d: port %d flags %d error %v; want port %d flags %d", i, port, flags, err, want.Addr.Port(), wantFlags)
		}
	}
}

func TestRangeSplitPointsUnauthorizedEndpoint(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	baseToken := wire.UID{First: 91, Second: 100}
	splitToken := (wire.Endpoint{Token: baseToken}).Adjusted(ssGetRangeSplitPoints).Token
	server := newNativeTestServer(t, func(conn *nativeTestConn, dest wire.UID, _ []byte) {
		if dest != splitToken {
			t.Errorf("unexpected storage endpoint: %v", dest)
			return
		}
		// A real FlowTransport private-endpoint notice names the refused
		// destination, not the request's reply promise.
		if err := conn.send(wire.WellKnown(wire.TokenUnauthorizedEndpoint), func(b *wire.Builder) []byte {
			b.StartTable(1)
			b.AddUID(0, dest)
			return b.Finish(b.EndTable(), 0)
		}); err != nil {
			t.Errorf("send unauthorized endpoint notice: %v", err)
		}
	})
	ep := wire.Endpoint{Addr: server.addr(), Token: baseToken}
	db := newLifecycleTestDB(t, ep)
	tr := NewNativeTxn(db, DefaultNativeOptions())
	defer tr.Close()
	if _, err := tr.Reader().RangeSplitPoints(ctx, []byte("a"), []byte("z"), 100); err != fdberr.UnauthorizedAttempt {
		t.Fatalf("private split endpoint returned %v, want unauthorized_attempt", err)
	}
	if conn := db.Peers.Live(ep.Addr); conn == nil || conn.IsDead() {
		t.Fatal("an unauthorized endpoint notice must not fail the connection")
	}
}

func TestDbContextCloseCancelsBlockedStorageHandshake(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	deadline, _ := ctx.Deadline()
	ln, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	if err := ln.SetDeadline(deadline); err != nil {
		t.Fatal(err)
	}
	ep := wire.Endpoint{
		Addr:  wire.NetAddr{Addr: netip.MustParseAddrPort(ln.Addr().String())},
		Token: wire.UID{First: 92, Second: 100},
	}
	db := newLifecycleTestDB(t, ep)
	tr := NewNativeTxn(db, DefaultNativeOptions())
	defer tr.Close()
	if err := tr.Shared.SetReadVersion(42); err != nil {
		t.Fatal(err)
	}
	// Storage attempts deliberately outlive the database context. Close must
	// cancel the pool's dial itself, not rely on this caller being cancelled.
	opctx, stopOperation := context.WithCancel(context.Background())
	defer stopOperation()
	result := make(chan error, 1)
	go func() {
		_, err := tr.Reader().Get(opctx, []byte("key"))
		result <- err
	}()
	nc, err := ln.AcceptTCP()
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	if err := nc.SetDeadline(deadline); err != nil {
		t.Fatal(err)
	}
	var hello [44]byte
	if _, err := io.ReadFull(nc, hello[:]); err != nil {
		t.Fatalf("receive client handshake: %v", err)
	}
	// Withhold the server's ConnectPacket. The operation is now blocked in
	// transport.Connect, not in the coordinator monitor or a queued RPC.
	closed := make(chan struct{})
	go func() {
		db.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-ctx.Done():
		t.Fatal("Close did not cancel and join the blocked storage dial")
	}
	select {
	case err := <-result:
		if err != fdberr.OperationCancelled {
			t.Fatalf("storage read after Close = %v, want operation_cancelled", err)
		}
	case <-ctx.Done():
		t.Fatal("storage read survived database Close")
	}
	if opctx.Err() != nil {
		t.Fatal("database Close cancelled the independent caller context")
	}
	if n, err := io.Copy(io.Discard, nc); err != nil || n != 0 {
		t.Fatalf("abandoned handshake socket was not disposed: read %d bytes, error %v", n, err)
	}
	for _, peer := range db.Peers.Status() {
		if peer.Connection != nil {
			t.Fatal("storage dial published a connection after Close")
		}
	}
	if conn, err := db.Peers.Get(context.Background(), ep.Addr, db.Failures); conn != nil || err != fdberr.OperationCancelled {
		t.Fatalf("closed pool redial = %v, %v", conn, err)
	}
}

func TestPeersRecoverFailedAddressWithoutRequests(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &nativeTestServer{t: t, ln: ln, handle: func(_ *nativeTestConn, _ wire.UID, _ []byte) {
		t.Error("recovery should only establish a connection")
	}}
	t.Cleanup(server.close)
	server.wg.Go(func() {
		// Deterministically fail the first handshake, then serve reconnects.
		first := true
		for {
			nc, err := ln.Accept()
			if err != nil {
				return
			}
			if first {
				first = false
				nc.Close()
				continue
			}
			server.mu.Lock()
			if server.closed {
				server.mu.Unlock()
				nc.Close()
				return
			}
			server.conns = append(server.conns, nc)
			server.mu.Unlock()
			server.wg.Go(func() { server.serve(nc) })
		}
	})
	var peers Peers
	t.Cleanup(peers.closeAll)
	failures := &transport.FailureMonitor{}
	ep := wire.Endpoint{Addr: server.addr()}
	if conn, err := peers.Get(ctx, ep.Addr, failures); err == nil || conn != nil {
		t.Fatalf("first handshake = %v, %v; want failure", conn, err)
	}
	if !failures.AddressIsFailed(ep.Addr) {
		t.Fatal("failed handshake did not persist the address failure")
	}
	// No new application request attempts the failed address. The pool-owned
	// probe must establish a connection and notify the failure monitor.
	if err := failures.WaitForRecovery(ctx, 1, func(int) wire.Endpoint { return ep }, false, 4*time.Second); err != nil {
		t.Fatalf("background reconnect failed: %v", err)
	}
	if peers.Live(ep.Addr) == nil {
		t.Fatal("failure cleared before a connection was published")
	}
	peers.closeAll()
	if failures.AddressIsFailed(ep.Addr) {
		t.Fatal("successful reconnect left the address failed")
	}
}

func TestPeersShareDialAndReuseConnection(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	server := newNativeTestServer(t, func(_ *nativeTestConn, dest wire.UID, _ []byte) {
		t.Errorf("unexpected request: %v", dest)
	})
	var peers Peers
	t.Cleanup(peers.closeAll)
	failures := &transport.FailureMonitor{}
	type result struct {
		conn *transport.Connection
		err  error
	}
	const callers = 16
	results := make(chan result, callers)
	start := make(chan struct{})
	for range callers {
		go func() {
			<-start
			conn, err := peers.Get(ctx, server.addr(), failures)
			results <- result{conn, err}
		}()
	}
	close(start)
	var shared *transport.Connection
	for range callers {
		select {
		case got := <-results:
			if got.err != nil || got.conn == nil {
				t.Fatalf("shared dial = %v, %v", got.conn, got.err)
			}
			if shared == nil {
				shared = got.conn
			} else if got.conn != shared {
				t.Fatal("concurrent callers did not share the same connection")
			}
		case <-ctx.Done():
			t.Fatal("concurrent peer acquisition did not finish")
		}
	}
	if conn, err := peers.Get(ctx, server.addr(), failures); err != nil || conn != shared {
		t.Fatalf("reused connection = %v, %v; want %v", conn, err, shared)
	}
	server.mu.Lock()
	connections := len(server.conns)
	server.mu.Unlock()
	if connections != 1 {
		t.Fatalf("shared peer opened %d sockets, want one", connections)
	}
	shared.Close()
	replacement, err := peers.Get(ctx, server.addr(), failures)
	if err != nil || replacement == nil || replacement == shared {
		t.Fatalf("reconnect = %v, %v; want a new live connection", replacement, err)
	}
	select {
	case <-shared.Done():
	default:
		t.Fatal("peer replacement abandoned the failed connection's goroutines")
	}
	shared = replacement
	peers.closeAll()
	select {
	case <-shared.Done():
	default:
		t.Fatal("pool shutdown returned before connection goroutines stopped")
	}
	if conn := peers.Live(server.addr()); conn != nil {
		t.Fatal("closed pool retained a live connection")
	}
	if conn, err := peers.Get(ctx, server.addr(), failures); conn != nil || err != fdberr.OperationCancelled {
		t.Fatalf("closed pool acquisition = %v, %v", conn, err)
	}
}
