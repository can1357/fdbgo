package native

import (
	"context"
	"testing"
	"time"

	"github.com/can1357/fdbgo/internal/fdberr"
	"github.com/can1357/fdbgo/internal/transport"
	"github.com/can1357/fdbgo/internal/wire"
)

type balanceTestRequest struct {
	conn  *nativeTestConn
	dest  wire.UID
	reply wire.UID
	err   error
}

type balanceTestResult struct {
	value []byte
	alt   int
	err   error
}

type balanceTestDB struct {
	c        *DbContext
	ctx      context.Context
	cancel   context.CancelFunc
	servers  []wire.StorageServer
	requests chan balanceTestRequest
}

func newBalanceTestDB(t *testing.T, replicas int) *balanceTestDB {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	t.Cleanup(cancel)
	requests := make(chan balanceTestRequest, 16)
	server := newNativeTestServer(t, func(conn *nativeTestConn, dest wire.UID, msg []byte) {
		reply, err := nativeRequestReplyToken(msg, 4)
		select {
		case requests <- balanceTestRequest{conn, dest, reply, err}:
		case <-ctx.Done():
		}
	})
	dbctx, closeDB := context.WithCancelCause(context.Background())
	c := &DbContext{ctx: dbctx, cancel: closeDB, Failures: &transport.FailureMonitor{}, Model: transport.NewQueueModel()}
	t.Cleanup(c.Close)
	servers := make([]wire.StorageServer, replicas)
	for i := range servers {
		servers[i].Base = wire.Endpoint{Addr: server.addr(), Token: wire.UID{First: uint64(i + 1), Second: 1}}
	}
	return &balanceTestDB{c, ctx, cancel, servers, requests}
}

func (f *balanceTestDB) next(t *testing.T) balanceTestRequest {
	t.Helper()
	select {
	case req := <-f.requests:
		if req.err != nil {
			t.Fatalf("decode storage request: %v", req.err)
		}
		return req
	case <-f.ctx.Done():
		t.Fatal("timed out waiting for storage request")
		return balanceTestRequest{}
	}
}

func (req balanceTestRequest) respond(t *testing.T, value []byte, code fdberr.Error, penalty float64) {
	t.Helper()
	if err := req.conn.send(req.reply, func(b *wire.Builder) []byte {
		var failure, data wire.Off
		if code != 0 {
			b.StartTable(1)
			b.AddU16(0, uint16(code))
			failure = b.EndTable()
		}
		if value != nil {
			data = b.Bytes(value)
		}
		b.StartTable(6)
		b.AddF64(0, penalty)
		if code != 0 {
			b.AddUnion(1, 1, failure)
		}
		if value != nil {
			b.AddUnion(3, 1, data)
		}
		reply := b.EndTable()
		b.StartTable(2)
		b.AddUnion(0, 2, reply)
		return b.Finish(b.EndTable(), (&wire.GetValue{}).ReplyFileID())
	}); err != nil {
		t.Fatalf("send storage reply: %v", err)
	}
}

// race starts A first, then runs a fixed replica plan that immediately races B.
// Capturing A lets tests synchronize its completion without sleeps or polling.
func (f *balanceTestDB) race(t *testing.T) (*lbAttempt[[]byte], balanceTestRequest, balanceTestRequest, <-chan balanceTestResult) {
	t.Helper()
	n := len(f.servers)
	r := &lbRun[[]byte]{
		c: f.c, servers: f.servers, req: &wire.GetValue{Key: []byte("key")},
		n: n, best: 0, next: 2 % n, startAlt: 1, startDistance: n - 1,
	}
	primary := r.start(f.ctx, 0, 0)
	a := f.next(t)
	if a.dest != f.servers[0].Base.Token {
		t.Fatalf("primary replica = %v, want A", a.dest)
	}
	done := make(chan balanceTestResult, 1)
	go func() {
		value, alt, err := r.run(f.ctx, primary)
		done <- balanceTestResult{value, alt, err}
	}()
	b := f.next(t)
	if b.dest != f.servers[1].Base.Token {
		t.Fatalf("hedge replica = %v, want B", b.dest)
	}
	return primary, a, b, done
}

func (f *balanceTestDB) result(t *testing.T, done <-chan balanceTestResult, value string, alt int) {
	t.Helper()
	select {
	case got := <-done:
		if got.err != nil || got.alt != alt || string(got.value) != value {
			t.Fatalf("read = (%q, replica %d, %v), want %q from replica %d", got.value, got.alt, got.err, value, alt)
		}
	case <-f.ctx.Done():
		t.Fatal("timed out waiting for load-balanced result")
	}
}

func waitBalanceLagging(t *testing.T, c *DbContext) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		c.lagging.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("lagging request did not finish cleanup")
	}
}

func TestLoadBalanceUnhedgedRetriesAndCancels(t *testing.T) {
	for _, cancelRead := range []bool{false, true} {
		t.Run(map[bool]string{false: "retry", true: "cancel"}[cancelRead], func(t *testing.T) {
			f := newBalanceTestDB(t, 1)
			ctx, cancel := context.WithCancel(f.ctx)
			defer cancel()
			done := make(chan balanceTestResult, 1)
			go func() {
				value, err := loadBalance(ctx, f.c, f.servers, 0, wire.Request[wire.Lb[[]byte]](&wire.GetValue{Key: []byte("key")}))
				done <- balanceTestResult{value: value, err: err}
			}()
			first := f.next(t)
			if cancelRead {
				cancel()
				select {
				case got := <-done:
					if got.err != fdberr.OperationCancelled {
						t.Fatalf("cancelled read = %v", got.err)
					}
				case <-f.ctx.Done():
					t.Fatal("unhedged request ignored cancellation")
				}
				return
			}
			first.respond(t, nil, fdberr.ServerOverloaded, 1)
			f.next(t).respond(t, []byte("retried"), 0, 1)
			f.result(t, done, "retried", 0)
		})
	}
}

func TestLoadBalancePrimaryKeepsInitialTriedAllState(t *testing.T) {
	f := newBalanceTestDB(t, 3)
	primary, a, b, done := f.race(t)
	b.respond(t, nil, fdberr.ServerOverloaded, 1)
	c := f.next(t)
	if c.dest != f.servers[2].Base.Token {
		t.Fatalf("replacement replica = %v, want C", c.dest)
	}
	c.respond(t, nil, fdberr.ServerOverloaded, 1)
	b = f.next(t) // Starting B again proves that the hedge exhausted the replicas.
	if b.dest != f.servers[1].Base.Token {
		t.Fatalf("retried replica = %v, want B", b.dest)
	}
	a.respond(t, nil, fdberr.ProcessBehind, 1)
	select {
	case <-primary.ctx.Done(): // A has been consumed before B can reply.
	case <-f.ctx.Done():
		t.Fatal("primary reply was not processed")
	}
	b.respond(t, []byte("retry"), 0, 1)
	f.result(t, done, "retry", 1)
}

func TestLoadBalanceLateReplyUpdatesReplicaChoice(t *testing.T) {
	for _, tc := range []struct {
		name    string
		code    fdberr.Error
		penalty float64
	}{
		{"penalty", 0, 10},
		{"process_behind", fdberr.ProcessBehind, 1},
		{"future_version", fdberr.FutureVersion, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newBalanceTestDB(t, 2)
			primary, a, b, done := f.race(t)
			b.respond(t, []byte("hedge"), 0, 1)
			f.result(t, done, "hedge", 1)
			if primary.ctx.Err() != nil {
				t.Fatal("losing primary was cancelled before its reply")
			}
			// A is the only local replica. The unused remote replica is only
			// eligible as best when A's late penalty/backoff makes A unhealthy.
			plan := func() transport.Plan {
				return f.c.Model.Plan(2, 1, func(i int) uint64 { return uint64(1 + 2*i) }, func(int) bool { return true })
			}
			if got := plan(); got.Best != 0 {
				t.Fatalf("local replica excluded before late reply: %+v", got)
			}
			a.respond(t, []byte("late"), tc.code, tc.penalty)
			waitBalanceLagging(t, f.c)
			if got := plan(); got.Best != 1 {
				t.Fatalf("late %s did not redirect next read to remote replica: %+v", tc.name, got)
			}
		})
	}
}

func TestLoadBalanceLaggingCleanupOnCancelAndClose(t *testing.T) {
	for _, mode := range []string{"cancel", "close"} {
		t.Run(mode, func(t *testing.T) {
			f := newBalanceTestDB(t, 2)
			primary, _, b, done := f.race(t)
			b.respond(t, []byte("hedge"), 0, 1)
			f.result(t, done, "hedge", 1)
			if mode == "cancel" {
				f.cancel()
			} else {
				f.c.Close()
			}
			waitBalanceLagging(t, f.c)
			if primary.ctx.Err() == nil {
				t.Fatal("lagging RPC survived cancellation/Close")
			}
			f.c.lagging.mu.Lock()
			count := f.c.lagging.count
			f.c.lagging.mu.Unlock()
			if count != 0 {
				t.Fatalf("%d lagging requests retained after cleanup", count)
			}
		})
	}
}
