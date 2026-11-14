package native

import (
	"context"
	"encoding/binary"
	"net/netip"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/can1357/fdbgo/internal/cluster"
	"github.com/can1357/fdbgo/internal/kv"
	"github.com/can1357/fdbgo/internal/ryw"
	"github.com/can1357/fdbgo/internal/transport"
	"github.com/can1357/fdbgo/internal/wire"
)

func TestForwardingKeepsExternallyUpdatedClusterFile(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	reached := make(chan struct{}, 1)
	current := newNativeTestServer(t, func(_ *nativeTestConn, dest wire.UID, _ []byte) {
		if dest == wire.WellKnown(wire.TokenOpenDatabase) {
			select {
			case reached <- struct{}{}:
			default:
			}
		}
	})
	path := filepath.Join(t.TempDir(), "fdb.cluster")
	replacement := statusTestConnection(current).String()
	old := newNativeTestServer(t, func(conn *nativeTestConn, _ wire.UID, msg []byte) {
		token, err := nativeRequestReplyToken(msg, 6)
		if err != nil {
			t.Error(err)
			return
		}
		if err := os.WriteFile(path, []byte(replacement), 0o600); err != nil {
			t.Error(err)
			return
		}
		err = conn.send(token, func(b *wire.Builder) []byte {
			forward := b.Bytes([]byte("status:test@127.0.0.1:0"))
			b.StartTable(5)
			b.AddUnion(3, 1, forward)
			return lifecycleReply(b, b.EndTable(), (wire.OpenDatabase{}).ReplyFileID())
		})
		if err != nil {
			t.Error(err)
		}
	})
	original := statusTestConnection(old)
	if err := os.WriteFile(path, []byte(original.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	db := NewDbContext(cluster.ConnectionRecord{Path: path, Conn: original}, false)
	defer db.Close()
	select {
	case <-reached:
	case <-ctx.Done():
		t.Fatal("client did not recover using the externally updated cluster file")
	}
	contents, err := os.ReadFile(path)
	if err != nil || string(contents) != replacement {
		t.Fatalf("forwarding overwrote cluster file: %q, %v", contents, err)
	}
}

func TestCachedReadVersionAcceptsOutOfOrderCommit(t *testing.T) {
	db := newLifecycleTestDB(t, wire.Endpoint{})
	now := transport.Now()
	db.UpdateCachedReadVersion(now, 100)
	db.UpdateCachedReadVersion(now-0.001, 200)
	db.grvCacheMu.Lock()
	db.grvCache.UpdaterRunning = true
	db.grvCacheMu.Unlock()
	fetchReadVersion(db, now, grvParams{grvCache: true}, func(rv ReadVersion, err error) {
		if err != nil || rv.Version != 200 {
			t.Errorf("cached read after observed commit = %d, %v; want 200", rv.Version, err)
		}
	})
	if got := db.grvCacheSnapshot().Time; got != now {
		t.Fatalf("newer commit moved freshness backwards: %v, want %v", got, now)
	}
}

func TestProxyRefreshPreservesCommitAndExpiresIdempotency(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	type request struct {
		conn  *nativeTestConn
		reply wire.UID
	}
	pending := make(chan request, 1)
	expired := make(chan struct{}, 1)
	base := wire.UID{First: 900, Second: 100}
	server := newNativeTestServer(t, func(conn *nativeTestConn, dest wire.UID, msg []byte) {
		table, err := lifecycleRequestTable(msg)
		if err != nil {
			t.Error(err)
			return
		}
		if dest == (wire.Endpoint{Token: base}).Adjusted(10).Token {
			version, verr := table.I64(0)
			batch, berr := table.U8(1)
			tenant, present, terr := table.Table(2)
			tenantID, ierr := tenant.I64(0)
			if verr != nil || berr != nil || terr != nil || ierr != nil || !present ||
				version != 200 || batch != 2 || tenantID != -1 {
				t.Errorf("invalid expiration: version=%d batch=%d tenant=%d", version, batch, tenantID)
			}
			expired <- struct{}{}
			return
		}
		tag, field, err := table.Union(3)
		id, iderr := field.I64()
		if err != nil || iderr != nil || tag != 1 || id != 123 {
			t.Errorf("server tracing identifier missing from commit: tag=%d id=%d %v %v", tag, id, err, iderr)
		}
		reply, err := nativeRequestReplyToken(msg, 1)
		if err != nil {
			t.Error(err)
			return
		}
		pending <- request{conn, reply}
	})
	ep := wire.Endpoint{Addr: server.addr(), Token: base}
	db := newLifecycleTestDB(t, ep)
	opts := DefaultNativeOptions()
	opts.AutomaticIdempotency = true
	opts.IdempotencyID = make([]byte, 16)
	opts.ReadOpts().DebugID = wire.UID{First: 123, Second: 456}
	tr := NewNativeTxn(db, opts)
	defer tr.Close()
	if err := tr.Shared.SetReadVersion(100); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		done <- tr.Commit(ryw.CommitPlan{
			Mutations:      []wire.Mutation{{Kind: 0, Param1: []byte("key"), Param2: []byte("value")}},
			WriteConflicts: []kv.KeyRange{{Begin: []byte("key"), End: []byte("key\x00")}},
		})
	}()
	var req request
	select {
	case req = <-pending:
	case <-ctx.Done():
		t.Fatal("commit was not sent")
	}
	generation := db.info.Load().ctx
	db.publish(&ClusterInfo{ID: wire.UID{First: 42}, GRV: []wire.Endpoint{ep}, Commit: []wire.Endpoint{ep}})
	if generation.Err() != nil {
		t.Fatal("unchanged proxies cancelled the pending commit")
	}
	if err := req.conn.send(req.reply, func(b *wire.Builder) []byte {
		b.StartTable(2)
		b.AddI64(0, 200)
		b.AddU16(1, 0x203)
		return lifecycleReply(b, b.EndTable(), (&wire.Commit{}).ReplyFileID())
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("commit did not complete")
	}
	select {
	case <-expired:
	case <-ctx.Done():
		t.Fatal("successful automatic-idempotency commit was not acknowledged")
	}
}

func routingLocationReply(b *wire.Builder, ep wire.Endpoint) []byte {
	ip := ep.Addr.Addr.Addr().As4()
	v := b.ScalarU32(binary.BigEndian.Uint32(ip[:]))
	b.StartTable(2)
	b.AddUnion(0, 1, v)
	ipTable := b.EndTable()
	b.StartTable(4)
	b.AddOffset(0, ipTable)
	b.AddU16(1, ep.Addr.Addr.Port())
	addr := b.EndTable()
	b.StartTable(3)
	b.AddOffset(0, addr)
	addresses := b.EndTable()
	b.StartTable(2)
	b.AddOffset(0, addresses)
	b.AddUID(1, ep.Token)
	endpoint := b.EndTable()
	b.StartTable(1)
	b.AddOffset(0, endpoint)
	stream := b.EndTable()
	b.StartTable(3)
	b.AddUID(0, ep.Token)
	b.AddOffset(2, stream)
	b.Push(b.EndTable())
	servers := b.Offsets(0)
	begin, end := b.Bytes([]byte{}), b.Bytes([]byte("\xff\xff"))
	b.StartTable(2)
	b.AddOffset(0, begin)
	b.AddOffset(1, end)
	keyRange := b.EndTable()
	b.StartTable(2)
	b.AddOffset(0, keyRange)
	b.AddOffset(1, servers)
	b.Push(b.EndTable())
	locations := b.Offsets(0)
	b.StartTable(1)
	b.AddOffset(0, locations)
	reply := b.EndTable()
	b.StartTable(2)
	b.AddUnion(0, 2, reply)
	return b.Finish(b.EndTable(), (wire.GetKeyServerLocations{}).ReplyFileID())
}

func TestLocateRefreshesRetiredEndpointsWithHealthyReplica(t *testing.T) {
	for _, stream := range []uint32{ssGetValue, ssGetKey, ssGetKeyValues, ssGetMappedKeyValues, ssWatchValue, ssWaitMetrics, ssGetRangeSplitPoints} {
		for _, reverse := range []bool{false, true} {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			var fresh wire.Endpoint
			ready := make(chan struct{})
			var requests atomic.Int64
			server := newNativeTestServer(t, func(conn *nativeTestConn, _ wire.UID, msg []byte) {
				<-ready
				requests.Add(1)
				reply, err := nativeRequestReplyToken(msg, 5)
				if err != nil {
					t.Error(err)
					return
				}
				if err := conn.send(reply, func(b *wire.Builder) []byte { return routingLocationReply(b, fresh) }); err != nil {
					t.Error(err)
				}
			})
			fresh = wire.Endpoint{Addr: server.addr(), Token: wire.UID{First: 100, Second: 100}}
			close(ready)
			db := newLifecycleTestDB(t, fresh)
			old := fresh
			old.Token.First++
			cached := Location{Begin: []byte{}, End: []byte("\xff\xff"), Servers: []wire.StorageServer{{Base: old}, {Base: fresh}}}
			db.Locations.Insert(cached)
			db.Failures.EndpointNotFound(old.Adjusted(stream))
			got, err := db.Locate(ctx, []byte("key"), reverse, false, stream)
			if err != nil || len(got.Servers) != 1 || got.Servers[0].Base != fresh || requests.Load() != 1 {
				t.Fatalf("stream %d reverse %v: location %+v, error %v, refreshes %d", stream, reverse, got, err, requests.Load())
			}
			// Cached range consumers must perform the same check, including
			// mapped reads, metrics and split points, not just point reads.
			db.Locations.Insert(cached)
			tr := NewNativeTxn(db, DefaultNativeOptions())
			locs, err := tr.Reader().rangeLocations(ctx, []byte("a"), []byte("z"), 10, reverse, stream)
			tr.Close()
			if err != nil || len(locs) != 1 || len(locs[0].Servers) != 1 || requests.Load() != 2 {
				t.Fatalf("stream %d range reverse %v: locations %+v, error %v, refreshes %d", stream, reverse, locs, err, requests.Load())
			}
			db.Close()
			server.close()
			cancel()
		}
	}
}

func TestLocationEndpointRefreshThrottle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := &DbContext{Failures: &transport.FailureMonitor{}}
		ep := wire.Endpoint{Addr: wire.NetAddr{Addr: netip.MustParseAddrPort("127.0.0.1:4500")}, Token: wire.UID{First: 1, Second: 1}}
		loc := Location{Servers: []wire.StorageServer{{Base: ep}}}
		c.Failures.EndpointNotFound(ep.Adjusted(ssGetKey))
		if c.locationNeedsRefresh(loc, ssGetValue) {
			t.Fatal("unrelated request stream invalidated the cache")
		}
		if !c.locationNeedsRefresh(loc, ssGetKey) {
			t.Fatal("first endpoint failure did not refresh")
		}
		time.Sleep(59 * time.Second)
		if !c.locationNeedsRefresh(loc, ssGetKey) {
			t.Fatal("grace-period refresh was throttled")
		}
		time.Sleep(time.Second)
		if c.locationNeedsRefresh(loc, ssGetKey) {
			t.Fatal("persistent endpoint failure was not throttled")
		}
		time.Sleep(60 * time.Second)
		if !c.locationNeedsRefresh(loc, ssGetKey) || c.locationNeedsRefresh(loc, ssGetKey) {
			t.Fatal("retry interval did not allow exactly one refresh")
		}
		c.Failures.AddressFailed(ep.Addr)
		if c.locationNeedsRefresh(loc, ssGetKey) {
			t.Fatal("address failure treated as endpoint-only")
		}
		c.Failures.AddressOK(ep.Addr)
		if !c.locationNeedsRefresh(loc, ssGetKey) {
			t.Fatal("recovered address did not restart endpoint grace period")
		}
	})
}

func TestAllFailedReplicasRefreshAndRead(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	var fresh wire.Endpoint
	ready := make(chan struct{})
	var refreshes atomic.Int64
	server := newNativeTestServer(t, func(conn *nativeTestConn, dest wire.UID, msg []byte) {
		<-ready
		if dest == fresh.Adjusted(cpGetKeyServersLocations).Token {
			refreshes.Add(1)
			reply, err := nativeRequestReplyToken(msg, 5)
			if err != nil {
				t.Error(err)
				return
			}
			if err := conn.send(reply, func(b *wire.Builder) []byte { return routingLocationReply(b, fresh) }); err != nil {
				t.Error(err)
			}
			return
		}
		reply, err := nativeRequestReplyToken(msg, 4)
		balanceTestRequest{conn: conn, dest: dest, reply: reply, err: err}.respond(t, []byte("recovered"), 0, 1)
	})
	fresh = wire.Endpoint{Addr: server.addr(), Token: wire.UID{First: 100, Second: 100}}
	close(ready)
	db := newLifecycleTestDB(t, fresh)
	servers := make([]wire.StorageServer, 2)
	for i := range servers {
		ep := wire.Endpoint{Addr: wire.NetAddr{Addr: netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), uint16(i+1))}, Token: wire.UID{First: uint64(i + 1), Second: 1}}
		servers[i].Base = ep
		db.Failures.AddressFailed(ep.Addr)
	}
	db.Locations.Insert(Location{Begin: []byte{}, End: []byte("\xff\xff"), Servers: servers})
	tr := NewNativeTxn(db, DefaultNativeOptions())
	defer tr.Close()
	if err := tr.Shared.SetReadVersion(42); err != nil {
		t.Fatal(err)
	}
	value, err := tr.Reader().Get(ctx, []byte("key"))
	if err != nil || string(value) != "recovered" || refreshes.Load() != 1 {
		t.Fatalf("all-failed read = %q, %v; refreshes %d", value, err, refreshes.Load())
	}
}
