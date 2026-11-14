package native

import (
	"context"
	"testing"
	"time"

	"github.com/can1357/fdbgo/internal/fdberr"
	"github.com/can1357/fdbgo/internal/transport"
	"github.com/can1357/fdbgo/internal/wire"
)

func TestLoadBalanceRetainsPrimaryAfterHedgeRetry(t *testing.T) {
	for _, tc := range []struct {
		name string
		code fdberr.Error
	}{
		{"server_overloaded", fdberr.ServerOverloaded},
		{"process_behind", fdberr.ProcessBehind},
		{"broken_promise", fdberr.BrokenPromise},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			type request struct {
				conn  *nativeTestConn
				dest  wire.UID
				reply wire.UID
				err   error
			}
			requests := make(chan request, 3)
			server := newNativeTestServer(t, func(conn *nativeTestConn, dest wire.UID, msg []byte) {
				reply, err := nativeRequestReplyToken(msg, 4)
				select {
				case requests <- request{conn, dest, reply, err}:
				case <-ctx.Done():
				}
			})
			c := &DbContext{ctx: ctx, Failures: &transport.FailureMonitor{}, Model: transport.NewQueueModel()}
			t.Cleanup(c.Peers.closeAll)
			t.Cleanup(c.lagging.close)
			for range 1 + int(1/transport.SecondRequestBudgetGrowth) {
				c.Model.FirstReplied()
			}
			servers := make([]wire.StorageServer, 3)
			for i := range servers {
				servers[i].Base = wire.Endpoint{Addr: server.addr(), Token: wire.UID{First: uint64(i + 1), Second: 1}}
			}
			// Fix the replica plan and hedge immediately, without relying on random
			// selection or sleeps. Every attempt still uses the real RPC path.
			r := lbRun[[]byte]{
				c: c, servers: servers, req: &wire.GetValue{Key: []byte("key")},
				n: 3, best: 0, next: 1, startAlt: 1, startDistance: 2, race: true,
			}
			type result struct {
				value []byte
				alt   int
				err   error
			}
			done := make(chan result, 1)
			go func() {
				value, alt, err := r.run(ctx, nil)
				done <- result{value, alt, err}
			}()
			nextRequest := func() request {
				t.Helper()
				select {
				case req := <-requests:
					if req.err != nil {
						t.Fatalf("decode GetValue request: %v", req.err)
					}
					return req
				case got := <-done:
					t.Fatalf("read completed before replacement replica started: %+v", got)
				case <-ctx.Done():
					t.Fatal("timed out waiting for storage request")
				}
				return request{}
			}
			respond := func(req request, value []byte, code fdberr.Error) {
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
					b.AddF64(0, 1)
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

			// A and B may reach their shared connection in either order, but both
			// must be pending before B fails. C cannot start until B is processed.
			var pending [2]request
			for range pending {
				req := nextRequest()
				if req.dest.First < 1 || req.dest.First > 2 || pending[req.dest.First-1].conn != nil {
					t.Fatalf("unexpected initial replica: %v", req.dest)
				}
				pending[req.dest.First-1] = req
			}
			respond(pending[1], nil, tc.code)
			if req := nextRequest(); req.dest != servers[2].Base.Token {
				t.Fatalf("replacement replica = %v, want C", req.dest)
			}
			// Leave C unanswered: returning A proves that B's retry did not discard it.
			respond(pending[0], []byte("primary"), 0)
			select {
			case got := <-done:
				if got.err != nil || got.alt != 0 || string(got.value) != "primary" {
					t.Fatalf("read = (%q, replica %d, %v), want primary from A", got.value, got.alt, got.err)
				}
			case <-ctx.Done():
				t.Fatal("read waited for C instead of returning A")
			}
		})
	}
}
