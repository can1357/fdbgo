package native

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/can1357/fdbgo/internal/fdberr"
	"github.com/can1357/fdbgo/internal/kv"
	"github.com/can1357/fdbgo/internal/wire"
)

func newValueBatchTestDB(t *testing.T) (*balanceTestDB, *NativeTxn) {
	t.Helper()
	f := newBalanceTestDB(t, 1)
	f.c.Locations = newLocationCache()
	f.c.Locations.Insert(Location{Begin: []byte{}, End: []byte("\xff\xff"), Servers: f.servers})
	tr := NewNativeTxn(f.c, DefaultNativeOptions())
	t.Cleanup(tr.Close)
	if err := tr.Shared.SetReadVersion(42); err != nil {
		t.Fatal(err)
	}
	return f, tr
}

func TestGetManyOutOfOrderValuesAndErrors(t *testing.T) {
	f, tr := newValueBatchTestDB(t)
	done := make(chan []kv.ValueResult, 1)
	go func() {
		done <- tr.Reader().GetMany(f.ctx, [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")})
	}()
	requests := make([]balanceTestRequest, 4)
	for i := range requests {
		requests[i] = f.next(t)
	}
	// All requests must arrive before any replies are made available.
	requests[3].respond(t, []byte("last"), 0, 1)
	requests[2].respond(t, []byte{}, 0, 1)
	requests[1].respond(t, nil, fdberr.FutureVersion, 1)
	requests[0].respond(t, nil, 0, 1)
	select {
	case results := <-done:
		if len(results) != 4 || results[0].Err != nil || results[0].Value != nil ||
			results[1].Err != fdberr.FutureVersion || results[1].Value != nil ||
			results[2].Err != nil || results[2].Value == nil || len(results[2].Value) != 0 ||
			results[3].Err != nil || string(results[3].Value) != "last" {
			t.Fatalf("out-of-order batch = %+v", results)
		}
	case <-f.ctx.Done():
		t.Fatal("batch did not collect all outcomes")
	}
}

func TestGetManyPreservesRetryProgression(t *testing.T) {
	for _, code := range []fdberr.Error{fdberr.ServerOverloaded, fdberr.ProcessBehind} {
		t.Run(code.Error(), func(t *testing.T) {
			f, tr := newValueBatchTestDB(t)
			done := make(chan []kv.ValueResult, 1)
			go func() {
				done <- tr.Reader().GetMany(f.ctx, [][]byte{[]byte("a"), []byte("b")})
			}()
			first, second := f.next(t), f.next(t)
			second.respond(t, []byte("sibling"), 0, 1)
			first.respond(t, nil, code, 1)
			retry := f.next(t)
			if code == fdberr.ProcessBehind {
				// triedAll must carry across the batch's initial attempt, so
				// the second ProcessBehind ends the read instead of looping.
				retry.respond(t, nil, code, 1)
			} else {
				retry.respond(t, []byte("retried"), 0, 1)
			}
			select {
			case results := <-done:
				if len(results) != 2 || results[1].Err != nil || string(results[1].Value) != "sibling" {
					t.Fatalf("retry lost sibling outcome: %+v", results)
				}
				if code == fdberr.ProcessBehind {
					if results[0].Err != code {
						t.Fatalf("second process_behind = %v", results[0].Err)
					}
				} else if results[0].Err != nil || string(results[0].Value) != "retried" {
					t.Fatalf("overloaded retry = %+v", results[0])
				}
			case <-f.ctx.Done():
				t.Fatal("retry did not preserve load-balancer progress")
			}
		})
	}
}

func TestGetManyCancellationAndLateReplies(t *testing.T) {
	for _, reason := range []string{"caller", "reset", "database-close"} {
		t.Run(reason, func(t *testing.T) {
			f, tr := newValueBatchTestDB(t)
			ctx, cancel := context.WithCancel(f.ctx)
			defer cancel()
			done := make(chan []kv.ValueResult, 1)
			go func() {
				done <- tr.Reader().GetMany(ctx, [][]byte{[]byte("a"), []byte("b"), []byte("c")})
			}()
			requests := []balanceTestRequest{f.next(t), f.next(t), f.next(t)}
			want := fdberr.OperationCancelled
			switch reason {
			case "caller":
				cancel()
			case "reset":
				tr.Reset()
				want = fdberr.TransactionCancelled
			case "database-close":
				f.c.Close()
			}
			select {
			case results := <-done:
				if len(results) != 3 {
					t.Fatalf("cancelled batch length = %d", len(results))
				}
				for i, result := range results {
					if result.Err != want {
						t.Errorf("cancelled row %d = %v, want %v", i, result.Err, want)
					}
				}
			case <-f.ctx.Done():
				t.Fatal("batch did not settle cancellation")
			}
			if reason == "database-close" {
				return
			}
			for _, req := range requests {
				req.respond(t, []byte("late"), 0, 1)
			}
			if reason == "reset" {
				if err := tr.Shared.SetReadVersion(42); err != nil {
					t.Fatal(err)
				}
			}
			go func() { done <- tr.Reader().GetMany(f.ctx, [][]byte{[]byte("new")}) }()
			f.next(t).respond(t, []byte("fresh"), 0, 1)
			select {
			case results := <-done:
				if len(results) != 1 || results[0].Err != nil || string(results[0].Value) != "fresh" {
					t.Fatalf("read after late replies = %+v", results)
				}
			case <-f.ctx.Done():
				t.Fatal("late replies poisoned subsequent reads")
			}
		})
	}
}

func TestGetManyReroutesLocationErrors(t *testing.T) {
	for _, failure := range []string{"wrong-shard", "all-alternatives", "endpoint-not-found", "already-retired"} {
		t.Run(failure, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			var fresh, old wire.Endpoint
			ready := make(chan struct{})
			var refreshes, oldReads atomic.Int64
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
				req := balanceTestRequest{conn: conn, dest: dest, reply: reply, err: err}
				if err != nil {
					t.Error(err)
					return
				}
				if dest == old.Adjusted(ssGetValue).Token {
					oldReads.Add(1)
					switch failure {
					case "wrong-shard":
						req.respond(t, nil, fdberr.WrongShardServer, 1)
					case "all-alternatives":
						req.respond(t, nil, fdberr.AllAlternativesFailed, 1)
					default:
						if err := conn.send(wire.WellKnown(wire.TokenEndpointNotFound), func(b *wire.Builder) []byte {
							b.StartTable(1)
							b.AddUID(0, dest)
							return b.Finish(b.EndTable(), 0)
						}); err != nil {
							t.Error(err)
						}
					}
					return
				}
				req.respond(t, []byte("rerouted"), 0, 1)
			})
			fresh = wire.Endpoint{Addr: server.addr(), Token: wire.UID{First: 500, Second: 100}}
			old = fresh
			old.Token.First++
			close(ready)
			db := newLifecycleTestDB(t, fresh)
			db.Locations.Insert(Location{Begin: []byte{}, End: []byte("\xff\xff"), Servers: []wire.StorageServer{{Base: old}}})
			if failure == "already-retired" {
				db.Failures.EndpointNotFound(old.Adjusted(ssGetValue))
			}
			tr := NewNativeTxn(db, DefaultNativeOptions())
			defer tr.Close()
			if err := tr.Shared.SetReadVersion(42); err != nil {
				t.Fatal(err)
			}
			results := tr.Reader().GetMany(ctx, [][]byte{[]byte("a"), []byte("b")})
			for i, result := range results {
				if result.Err != nil || string(result.Value) != "rerouted" {
					t.Errorf("rerouted row %d = %+v", i, result)
				}
			}
			if len(results) != 2 || refreshes.Load() == 0 {
				t.Fatalf("batch did not refresh its location: %+v, refreshes %d", results, refreshes.Load())
			}
			if failure == "already-retired" && oldReads.Load() != 0 {
				t.Fatal("batch dispatched to an already retired endpoint")
			}
			if failure == "endpoint-not-found" && !db.Failures.OnlyEndpointFailed(old.Adjusted(ssGetValue), db.TLS) {
				t.Fatal("batch did not record endpoint retirement")
			}
		})
	}
}

func TestGetManyMetadataCachePreservesAbsentAndEmpty(t *testing.T) {
	for _, value := range [][]byte{nil, {}, []byte("metadata")} {
		t.Run(string(value), func(t *testing.T) {
			f, tr := newValueBatchTestDB(t)
			f.c.RecordMetadataVersion(42, value)
			done := make(chan []kv.ValueResult, 1)
			go func() {
				done <- tr.Reader().GetMany(f.ctx, [][]byte{kv.MetadataVersionKey, []byte("ordinary"), kv.MetadataVersionKey})
			}()
			f.next(t).respond(t, []byte("storage"), 0, 1)
			select {
			case results := <-done:
				if len(results) != 3 || results[0].Err != nil || !equalValue(results[0].Value, value) ||
					results[1].Err != nil || string(results[1].Value) != "storage" ||
					results[2].Err != nil || !equalValue(results[2].Value, value) {
					t.Fatalf("metadata batch = %+v, want %#v", results, value)
				}
			case <-f.ctx.Done():
				t.Fatal("metadata batch did not use its cached read-version value")
			}
		})
	}
}
