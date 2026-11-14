package native

import (
	"context"
	"testing"
	"testing/synctest"

	"github.com/can1357/fdbgo/internal/fdberr"
	"github.com/can1357/fdbgo/internal/wire"
)

func TestClusterIDWaitsForValidGeneration(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancelCause(t.Context())
		defer cancel(nil)
		db := &DbContext{ctx: ctx}
		generation, endGeneration := context.WithCancel(ctx)
		db.info.Store(&infoState{info: &ClusterInfo{}, ctx: generation, cancel: endGeneration, changed: make(chan struct{})})
		type result struct {
			id  wire.UID
			err error
		}
		done := make(chan result, 1)
		go func() {
			id, err := db.ClusterID(t.Context())
			done <- result{id, err}
		}()
		synctest.Wait()
		select {
		case got := <-done:
			t.Fatalf("unknown cluster ID completed: %+v", got)
		default:
		}
		db.publish(&ClusterInfo{ID: wire.UID{First: 1}})
		synctest.Wait()
		select {
		case got := <-done:
			t.Fatalf("generation without cluster ID completed: %+v", got)
		default:
		}
		want := wire.UID{First: 12, Second: 34}
		db.publish(&ClusterInfo{ClusterID: want})
		synctest.Wait()
		got := <-done
		if got.err != nil || got.id != want {
			t.Fatalf("cluster ID = %v, %v; want %v", got.id, got.err, want)
		}
	})
}

func TestClusterIDCancellation(t *testing.T) {
	for _, database := range []bool{false, true} {
		name := "caller"
		if database {
			name = "database"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				dbCtx, closeDB := context.WithCancelCause(t.Context())
				defer closeDB(nil)
				ctx, cancel := context.WithCancelCause(t.Context())
				defer cancel(nil)
				db := &DbContext{ctx: dbCtx}
				generation, endGeneration := context.WithCancel(dbCtx)
				db.info.Store(&infoState{info: &ClusterInfo{}, ctx: generation, cancel: endGeneration, changed: make(chan struct{})})
				done := make(chan error, 1)
				go func() {
					_, err := db.ClusterID(ctx)
					done <- err
				}()
				synctest.Wait()
				want := fdberr.TransactionCancelled
				if database {
					want = fdberr.OperationCancelled
					closeDB(want)
				} else {
					cancel(want)
				}
				synctest.Wait()
				if err := <-done; err != want {
					t.Fatalf("cancelled cluster ID read = %v; want %v", err, want)
				}
			})
		})
	}
}
