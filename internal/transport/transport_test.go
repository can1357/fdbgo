package transport

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/can1357/fdbgo/internal/cluster"
	"github.com/can1357/fdbgo/internal/fdberr"
	"github.com/can1357/fdbgo/internal/wire"
)

var (
	echoTok   = wire.UID{First: 10, Second: 1}
	silentTok = wire.UID{First: 10, Second: 2}
	errTok    = wire.UID{First: 10, Second: 3}
	goneTok   = wire.UID{First: 10, Second: 4}
	deniedTok = wire.UID{First: 10, Second: 5}
	bigTok    = wire.UID{First: 10, Second: 6}
)

// testHandler answers by destination: echoTok (and pings) get Void, silentTok
// nothing, errTok a remote error, goneTok/deniedTok a notice, bigTok a 1 MiB reply.
func testHandler(s *serverConn, dest wire.UID, msg []byte) {
	reply, err := wire.PingReplyToken(msg)
	if err != nil {
		return
	}
	switch dest {
	case silentTok:
	case errTok:
		s.send(reply, errorReply(fdberr.FutureVersion))
	case goneTok:
		s.send(wire.WellKnown(wire.TokenEndpointNotFound), noticeMsg(goneTok))
	case deniedTok:
		s.send(wire.WellKnown(wire.TokenUnauthorizedEndpoint), noticeMsg(deniedTok))
	case bigTok:
		s.send(reply, paddedVoidReply(1<<20))
	default:
		s.send(reply, wire.EncodeVoidReply)
	}
}

func connect(t testing.TB, fs *fakeServer) *Connection {
	t.Helper()
	c, err := Connect(context.Background(), fs.addr())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(c.Close)
	return c
}

func call(c *Connection, dest wire.UID) error {
	_, err := Call(context.Background(), c, dest, wire.Ping{})
	return err
}

// Exercise batches queued before a write and while a write is blocked, including
// reuse of both output buffers. Neither framing mode may wait for a timer.
func TestWriteLoopBatches(t *testing.T) {
	for _, checksum := range []bool{false, true} {
		name := "withoutChecksum"
		if checksum {
			name = "withChecksum"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				client, server := net.Pipe()
				defer server.Close()
				c := &Connection{
					raw:      client,
					rw:       client,
					checksum: checksum,
					closed:   make(chan struct{}),
					done:     make(chan struct{}),
					wake:     make(chan struct{}, 1),
					created:  time.Now(),
				}
				defer c.Close()
				c.workers.Store(1)
				queue := func(n int) []byte {
					for i := range n {
						c.Send(wire.UID{First: uint64(n), Second: uint64(i)}, wire.Ping{})
					}
					c.wmu.Lock()
					want := bytes.Clone(c.out)
					c.wmu.Unlock()
					return want
				}
				readBatch := func(want []byte) {
					t.Helper()
					got := make([]byte, len(want)+1)
					n, err := server.Read(got)
					if err != nil {
						t.Fatal(err)
					}
					if !bytes.Equal(got[:n], want) {
						t.Fatalf("batch changed: got %d bytes, want %d", n, len(want))
					}
					synctest.Wait()
				}
				started := time.Now()
				first := queue(3)
				go c.writeLoop()
				synctest.Wait()
				second := queue(4)
				readBatch(first)
				third := queue(5)
				readBatch(second)
				readBatch(third)
				if elapsed := time.Since(started); elapsed != 0 {
					t.Fatalf("batching waited for %v", elapsed)
				}
				wantSent := uint64(len(first) + len(second) + len(third))
				if got := c.bytesSent.Load(); got != wantSent {
					t.Fatalf("bytes sent: got %d, want %d", got, wantSent)
				}
				queue(2)
				synctest.Wait()
				c.Close()
				<-c.Done()
				if got := c.bytesSent.Load(); got != wantSent {
					t.Fatalf("failed write counted: got %d, want %d", got, wantSent)
				}
			})
		})
	}
}

func TestCallRoundTrip(t *testing.T) {
	fs := newFakeServer(t, testHandler).start()
	c := connect(t, fs)
	for range 100 {
		if err := call(c, echoTok); err != nil {
			t.Fatal(err)
		}
	}
	if err := call(c, bigTok); err != nil {
		t.Fatalf("large reply: %v", err)
	}
	if !c.idle() {
		t.Fatal("pending slots leaked")
	}
	if recv, sent, _ := c.Stats(); recv < 1<<20 || sent == 0 {
		t.Fatalf("stats: received %d sent %d", recv, sent)
	}
}

func TestConcurrentCalls(t *testing.T) {
	fs := newFakeServer(t, testHandler).start()
	c := connect(t, fs)
	var wg sync.WaitGroup
	for g := range 16 {
		wg.Go(func() {
			for i := range 200 {
				dest := echoTok
				if (g+i)%50 == 0 {
					dest = bigTok
				}
				if err := call(c, dest); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	wg.Wait()
	if !c.idle() {
		t.Fatal("pending slots leaked")
	}
}

func TestRemoteErrorAndNotices(t *testing.T) {
	fs := newFakeServer(t, testHandler).start()
	c := connect(t, fs)
	if err := call(c, errTok); err != fdberr.FutureVersion {
		t.Fatalf("remote error: got %v", err)
	}
	if err := call(c, goneTok); err != EndpointNotFound {
		t.Fatalf("endpoint not found: got %v", err)
	}
	if err := call(c, deniedTok); err != Unauthorized {
		t.Fatalf("unauthorized: got %v", err)
	}
	if c.IsDead() {
		t.Fatal("notices must not fail the connection")
	}
	if err := call(c, echoTok); err != nil {
		t.Fatal(err)
	}
}

func TestHandshakeFailures(t *testing.T) {
	fs := newFakeServer(t, testHandler)
	fs.connectLen = 4
	fs.start()
	if _, err := Connect(context.Background(), fs.addr()); err != Malformed {
		t.Fatalf("short connect packet: got %v", err)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := (&fakeServer{ln: ln}).addr()
	ln.Close()
	if _, err := Connect(context.Background(), addr); err != NotDelivered {
		t.Fatalf("refused: got %v", err)
	}
}

func TestConnectHonorsContext(t *testing.T) {
	// A listener that never answers the handshake.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := Connect(ctx, (&fakeServer{ln: ln}).addr()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v", err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("Connect ignored ctx")
	}
}

func TestFailureFailsOutstanding(t *testing.T) {
	fs := newFakeServer(t, testHandler).start()
	c := connect(t, fs)
	errs := make(chan error, 2)
	go func() { errs <- call(c, silentTok) }()
	go func() { errs <- call(c, silentTok) }()
	waitFor(t, func() bool { return pendingCount(c) == 2 })
	fs.dropAll()
	for range 2 {
		if err := <-errs; err != MaybeDelivered {
			t.Fatalf("outstanding request: got %v", err)
		}
	}
	<-c.Done()
	if !c.IsDead() {
		t.Fatal("connection should be dead")
	}
	if err := call(c, echoTok); err != NotDelivered {
		t.Fatalf("call on dead connection: got %v", err)
	}

}

func TestCloseFailsOutstanding(t *testing.T) {
	fs := newFakeServer(t, testHandler).start()
	c := connect(t, fs)
	errs := make(chan error, 1)
	go func() { errs <- call(c, silentTok) }()
	waitFor(t, func() bool { return !c.idle() })
	c.Close()
	if err := <-errs; err != MaybeDelivered {
		t.Fatalf("got %v", err)
	}
}

func TestCancelledCallReleasesSlot(t *testing.T) {
	fs := newFakeServer(t, testHandler).start()
	c := connect(t, fs)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := Call(ctx, c, silentTok, wire.Ping{}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v", err)
	}
	if !c.idle() {
		t.Fatal("cancelled call leaked its pending slot")
	}
	if err := call(c, echoTok); err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentCallResults(t *testing.T) {
	fs := newFakeServer(t, testHandler).start()
	c := connect(t, fs)
	done := make(chan error, 3)
	for _, dest := range []wire.UID{echoTok, errTok, goneTok} {
		go func() { done <- call(c, dest) }()
	}
	got := map[error]bool{}
	for range 3 {
		got[<-done] = true
	}
	if !got[nil] || !got[fdberr.FutureVersion] || !got[EndpointNotFound] {
		t.Fatalf("results %v", got)
	}
}

func TestAnswersServerPings(t *testing.T) {
	replyTok := wire.UID{First: 77, Second: 88}
	answered := make(chan struct{}, 1)
	fs := newFakeServer(t, func(s *serverConn, dest wire.UID, msg []byte) {
		if dest == replyTok {
			if _, err := wire.ReplyTable(msg); err == nil {
				answered <- struct{}{}
			}
			return
		}
		testHandler(s, dest, msg)
	}).start()
	c := connect(t, fs)
	if err := call(c, echoTok); err != nil { // the server connection now exists
		t.Fatal(err)
	}
	fs.mu.Lock()
	s := fs.conns[0]
	fs.mu.Unlock()
	s.send(wire.WellKnown(wire.TokenPing), pingRequest(replyTok))
	if err := s.flush(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-answered:
	case <-time.After(5 * time.Second):
		t.Fatal("client did not answer the server's ping")
	}
}

func TestBadChecksumFailsConnection(t *testing.T) {
	fs := newFakeServer(t, testHandler).start()
	c := connect(t, fs)
	if err := call(c, echoTok); err != nil {
		t.Fatal(err)
	}
	fs.mu.Lock()
	s := fs.conns[0]
	fs.mu.Unlock()
	// A raw packet whose checksum is wrong.
	s.mu.Lock()
	s.w.Write([]byte{16, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8})
	s.w.Write(make([]byte, 16))
	s.w.Flush()
	s.mu.Unlock()
	select {
	case <-c.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("bad checksum did not fail the connection")
	}
}

// pipeServer serves the client end of a net.Pipe inside a synctest bubble;
// it answers pings when answer is set.
func pipeServer(nc net.Conn, answer bool) {
	s := &serverConn{nc: nc, checksum: true, w: bufio.NewWriter(nc)}
	defer nc.Close()
	var hdr [12]byte
	for {
		if _, err := readFull(nc, hdr[:]); err != nil {
			return
		}
		pkt := make([]byte, int(hdr[0])|int(hdr[1])<<8|int(hdr[2])<<16)
		if _, err := readFull(nc, pkt); err != nil {
			return
		}
		if !answer {
			continue
		}
		if reply, err := wire.PingReplyToken(pkt[16:]); err == nil {
			s.send(reply, wire.EncodeVoidReply)
			if s.flush() != nil {
				return
			}
		}
	}
}

func TestMonitorFailsSilentPeer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client, server := net.Pipe()
		go pipeServer(server, false)
		c := start(wire.NetAddr{}, client, client, true, wire.Protocol74)
		time.Sleep(2900 * time.Millisecond)
		synctest.Wait()
		if c.IsDead() {
			t.Fatal("failed before the ping timed out")
		}
		time.Sleep(time.Second)
		synctest.Wait()
		if !c.IsDead() {
			t.Fatal("silent peer not detected")
		}
		<-c.Done()
	})
}

func TestMonitorClosesIdleConnection(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client, server := net.Pipe()
		go pipeServer(server, true)
		c := start(wire.NetAddr{}, client, client, true, wire.Protocol74)
		if err := call(c, echoTok); err != nil {
			t.Fatal(err)
		}
		time.Sleep(179 * time.Second)
		synctest.Wait()
		if c.IsDead() {
			t.Fatal("closed before the idle timeout")
		}
		time.Sleep(3 * time.Second)
		synctest.Wait()
		if !c.IsDead() {
			t.Fatal("idle connection kept open")
		}
		<-c.Done()
	})
}

func TestMonitorKeepsBusyConnection(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client, server := net.Pipe()
		go pipeServer(server, true)
		c := start(wire.NetAddr{}, client, client, true, wire.Protocol74)
		for range 30 {
			time.Sleep(10 * time.Second)
			if err := call(c, echoTok); err != nil {
				t.Fatal(err)
			}
		}
		if c.IsDead() {
			t.Fatal("active connection closed")
		}
		c.Close()
		<-c.Done()
	})
}

func TestRPCErrorStrings(t *testing.T) {
	for e := NotDelivered; e <= Incompatible; e++ {
		if !strings.HasPrefix(e.Error(), "transport: ") || strings.Contains(e.Error(), "unknown") {
			t.Errorf("%d: %q", e, e.Error())
		}
	}
}

func TestBusynessSampler(t *testing.T) {
	StartBusynessSampler()
	StartBusynessSampler()
	if b := Busyness(); b < 0 || b > 1 {
		t.Fatalf("busyness %v", b)
	}
}

// TestDevClusterHandshake connects to a coordinator of the cluster named by
// FDB_CLUSTER_FILE and pings it.
func TestDevClusterHandshake(t *testing.T) {
	path := os.Getenv("FDB_CLUSTER_FILE")
	if path == "" {
		t.Skip("FDB_CLUSTER_FILE not set")
	}
	addr, err := firstCoordinator(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := Connect(ctx, addr)
	if err != nil {
		t.Fatalf("connect %v: %v", addr, err)
	}
	defer c.Close()
	if _, err := Call(ctx, c, wire.WellKnown(wire.TokenPing), wire.Ping{}); err != nil {
		t.Fatalf("ping %v: %v", addr, err)
	}
}

// firstCoordinator returns the first literal coordinator address of a cluster file.
func firstCoordinator(path string) (wire.NetAddr, error) {
	rec, err := cluster.Open(path)
	if err != nil {
		return wire.NetAddr{}, err
	}
	for _, co := range rec.Conn.Coordinators {
		if !co.IsHostname() {
			return wire.NetAddr{Addr: co.Addr, TLS: co.TLS}, nil
		}
	}
	return wire.NetAddr{}, errors.New("no literal coordinator address")
}

// pendingCount returns the number of outstanding requests.
func pendingCount(c *Connection) int {
	n := 0
	for i := range c.pending {
		s := &c.pending[i]
		s.mu.Lock()
		n += len(s.m)
		s.mu.Unlock()
	}
	return n
}

// waitFor polls cond for up to five seconds.
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not reached")
		}
		time.Sleep(time.Millisecond)
	}
}
