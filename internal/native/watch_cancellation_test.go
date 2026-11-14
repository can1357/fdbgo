package native

import (
	"context"
	"fmt"
	"testing"
	"testing/synctest"

	"github.com/can1357/fdbgo/internal/fdberr"
	"github.com/can1357/fdbgo/internal/ryw"
	"github.com/can1357/fdbgo/internal/wire"
)

func TestWatchCancellationReleasesReservation(t *testing.T) {
	for _, committed := range []bool{false, true} {
		t.Run(fmt.Sprintf("committed=%v", committed), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				db := newLifecycleTestDB(t, wire.Endpoint{})
				db.Watches.SetMax(1)
				tr := NewNativeTxn(db, DefaultNativeOptions())
				defer tr.Close()
				r := ryw.New(ryw.NewRywOptions(1))
				r.Set([]byte("key"), []byte("value"))
				_, wait, err := r.Watch(tr.Reader(), []byte("key"))
				if err != nil || wait == nil {
					t.Fatalf("watch: pending=%v, %v", wait != nil, err)
				}
				if committed {
					if err := publishCommit(context.Background(), tr.Shared, 100, versionstamp(100, 0), true, nil); err != nil {
						t.Fatal(err)
					}
					tr.Close()
				}
				ctx, cancel := context.WithCancel(context.Background())
				done := make(chan error, 1)
				go func() { _, err := wait(ctx); done <- err }()
				synctest.Wait()
				cancel()
				synctest.Wait()
				if err := <-done; err != fdberr.OperationCancelled {
					t.Fatalf("cancelled watch = %v", err)
				}
				if got := db.Watches.outstanding.Load(); got != 0 {
					t.Fatalf("cancelled watch retains %d quota", got)
				}
				db.wg.Wait()
				db.Watches.mu.Lock()
				remaining := len(db.Watches.m)
				db.Watches.mu.Unlock()
				if remaining != 0 {
					t.Fatal("cancelled watch retains its storage waiter")
				}
			})
		})
	}
}

func TestAttemptEndReleasesUncommittedWatch(t *testing.T) {
	for _, ended := range []fdberr.Error{fdberr.TransactionCancelled, fdberr.TransactionTimedOut} {
		t.Run(ended.Error(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				db := newLifecycleTestDB(t, wire.Endpoint{})
				tr := NewNativeTxn(db, DefaultNativeOptions())
				defer tr.Close()
				r := ryw.New(ryw.NewRywOptions(1))
				tr.Shared.BindAttempt(r.ResetSignal())
				r.Set([]byte("key"), []byte("value"))
				_, wait, err := r.Watch(tr.Reader(), []byte("key"))
				if err != nil || wait == nil {
					t.Fatalf("watch: pending=%v, %v", wait != nil, err)
				}
				r.Abort(ended)
				synctest.Wait()
				if _, err := wait(context.Background()); err != ended {
					t.Fatalf("watch after %v = %v", ended, err)
				}
				if got := db.Watches.outstanding.Load(); got != 0 {
					t.Fatalf("attempt end retains %d watch quota", got)
				}
			})
		})
	}
}

func TestWatchCancellationDetachesOnlyItsCoalescedWaiter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		db := newLifecycleTestDB(t, wire.Endpoint{})
		tr1 := NewNativeTxn(db, DefaultNativeOptions())
		defer tr1.Close()
		tr2 := NewNativeTxn(db, DefaultNativeOptions())
		defer tr2.Close()
		ch1, finish1, err := tr1.Reader().RegisterWatch([]byte("key"), []byte("value"))
		if err != nil {
			t.Fatal(err)
		}
		ch2, finish2, err := tr2.Reader().RegisterWatch([]byte("key"), []byte("value"))
		if err != nil {
			t.Fatal(err)
		}
		for _, tr := range []*NativeTxn{tr1, tr2} {
			if err := publishCommit(context.Background(), tr.Shared, 100, versionstamp(100, 0), true, nil); err != nil {
				t.Fatal(err)
			}
		}
		synctest.Wait()
		finish1(fdberr.OperationCancelled)
		if err := <-ch1; err != fdberr.OperationCancelled {
			t.Fatal(err)
		}
		synctest.Wait()
		select {
		case err := <-ch2:
			t.Fatalf("other watch was cancelled: %v", err)
		default:
		}
		db.Watches.mu.Lock()
		m := db.Watches.m["key"]
		remaining := m != nil && m.waiters == 1
		db.Watches.mu.Unlock()
		if !remaining || db.Watches.outstanding.Load() != 1 {
			t.Fatal("cancelling one future removed the other storage watch")
		}
		finish2(nil)
		if err := <-ch2; err != nil {
			t.Fatal(err)
		}
		db.wg.Wait()
	})
}
