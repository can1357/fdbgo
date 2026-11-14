package native

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/can1357/fdbgo/internal/fdberr"
	"github.com/can1357/fdbgo/internal/ryw"
	"github.com/can1357/fdbgo/internal/transport"
	"github.com/can1357/fdbgo/internal/wire"
)

// newLifecycleTestDB creates a database without a coordinator monitor. An
// empty endpoint leaves location requests waiting for a proxy generation.
func newLifecycleTestDB(t *testing.T, ep wire.Endpoint) *DbContext {
	t.Helper()
	ctx, cancel := context.WithCancelCause(context.Background())
	db := &DbContext{
		ctx:       ctx,
		cancel:    cancel,
		Failures:  &transport.FailureMonitor{},
		Model:     transport.NewQueueModel(),
		Locations: newLocationCache(),
		Watches:   newWatches(),
		grv:       make(map[uint32]*grvBatcher),
	}
	info := &ClusterInfo{}
	if ep.Addr.Addr.IsValid() {
		info.GRV = []wire.Endpoint{ep}
		info.Commit = []wire.Endpoint{ep}
		db.Locations.Insert(Location{
			Begin:   []byte{},
			End:     []byte("\xff\xff"),
			Servers: []wire.StorageServer{{Base: ep}},
		})
	}
	ictx, icancel := context.WithCancel(ctx)
	db.info.Store(&infoState{info: info, ctx: ictx, cancel: icancel, changed: make(chan struct{})})
	t.Cleanup(db.Close)
	return db
}

func TestWatchLocalCompletionReleasesReservationBeforeCommit(t *testing.T) {
	for _, finish := range []string{"commit", "reset", "commit-error"} {
		t.Run(finish, func(t *testing.T) {
			db := newLifecycleTestDB(t, wire.Endpoint{})
			db.Watches.SetMax(1)
			tr := NewNativeTxn(db, DefaultNativeOptions())
			defer tr.Close()
			r := ryw.New(ryw.NewRywOptions(1))
			key := []byte("key")
			r.Set(key, []byte("A"))
			for _, next := range []string{"B", "C"} {
				_, wait, err := r.Watch(tr.Reader(), key)
				if err != nil || wait == nil {
					t.Fatalf("watch before %s: wait %v, error %v", next, wait != nil, err)
				}
				if got := db.Watches.outstanding.Load(); got != 1 {
					t.Fatalf("outstanding before %s = %d", next, got)
				}
				r.Set(key, []byte(next))
				if _, err := wait(context.Background()); err != nil {
					t.Fatalf("local completion at %s: %v", next, err)
				}
				if got := db.Watches.outstanding.Load(); got != 0 {
					t.Fatalf("outstanding after %s = %d", next, got)
				}
			}
			switch finish {
			case "commit":
				if err := publishCommit(context.Background(), tr.Shared, 100, versionstamp(100, 0), true, nil); err != nil {
					t.Fatal(err)
				}
			case "reset":
				tr.Reset()
			case "commit-error":
				tr.Shared.watchMu.Lock()
				pending := tr.Shared.watches
				tr.Shared.watches = nil
				tr.Shared.watchMu.Unlock()
				for _, w := range pending {
					w.arm(0, fdberr.NotCommitted)
				}
			}
			if got := db.Watches.outstanding.Load(); got != 0 {
				t.Fatalf("completion released reservations twice: %d", got)
			}
		})
	}
}

func TestWatchLocalCompletionDetachesOnlyItsCoalescedWaiter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		db := newLifecycleTestDB(t, wire.Endpoint{})
		tr1 := NewNativeTxn(db, DefaultNativeOptions())
		defer tr1.Close()
		tr2 := NewNativeTxn(db, DefaultNativeOptions())
		defer tr2.Close()
		ch1, complete1, err := tr1.Reader().RegisterWatch([]byte("key"), []byte("A"))
		if err != nil {
			t.Fatal(err)
		}
		ch2, complete2, err := tr2.Reader().RegisterWatch([]byte("key"), []byte("A"))
		if err != nil {
			t.Fatal(err)
		}
		for _, tr := range []*NativeTxn{tr1, tr2} {
			if err := publishCommit(context.Background(), tr.Shared, 100, versionstamp(100, 0), true, nil); err != nil {
				t.Fatal(err)
			}
		}
		synctest.Wait()
		db.Watches.mu.Lock()
		m := db.Watches.m["key"]
		coalesced := m != nil && m.waiters == 2
		db.Watches.mu.Unlock()
		if !coalesced {
			t.Fatal("watches did not coalesce")
		}

		tr1.Reset()
		tr2.Reset()
		synctest.Wait()
		select {
		case err := <-ch1:
			t.Fatalf("armed watch ended on reset: %v", err)
		default:
		}
		complete1(nil)
		complete1(nil)
		if err := <-ch1; err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		db.Watches.mu.Lock()
		kept := db.Watches.m["key"] == m && m.waiters == 1 && !m.fired
		db.Watches.mu.Unlock()
		if !kept || db.Watches.outstanding.Load() != 1 {
			t.Fatal("local completion cancelled the other coalesced watch")
		}
		select {
		case err := <-ch2:
			t.Fatalf("second watch completed early: %v", err)
		default:
		}
		complete2(nil)
		complete2(nil)
		if err := <-ch2; err != nil {
			t.Fatal(err)
		}
		db.wg.Wait()
		db.Watches.mu.Lock()
		remaining := len(db.Watches.m)
		db.Watches.mu.Unlock()
		if remaining != 0 || db.Watches.outstanding.Load() != 0 {
			t.Fatal("last local completion retained its storage watch or reservation")
		}
	})
}

func TestArmedWatchLocalWriteReleasesStorageWaiter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		db := newLifecycleTestDB(t, wire.Endpoint{})
		tr := NewNativeTxn(db, DefaultNativeOptions())
		defer tr.Close()
		r := ryw.New(ryw.NewRywOptions(1))
		key := []byte("key")
		r.Set(key, []byte("A"))
		_, wait, err := r.Watch(tr.Reader(), key)
		if err != nil || wait == nil {
			t.Fatalf("watch setup: wait %v, error %v", wait != nil, err)
		}
		if err := publishCommit(context.Background(), tr.Shared, 100, versionstamp(100, 0), true, nil); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		r.Set(key, []byte("B"))
		if _, err := wait(context.Background()); err != nil {
			t.Fatal(err)
		}
		db.wg.Wait()
		db.Watches.mu.Lock()
		remaining := len(db.Watches.m)
		db.Watches.mu.Unlock()
		if remaining != 0 || db.Watches.outstanding.Load() != 0 {
			t.Fatal("armed local completion retained its storage watch or reservation")
		}
	})
}

func TestWatchLocalCompletionRacesAttemptEnd(t *testing.T) {
	db := newLifecycleTestDB(t, wire.Endpoint{})
	for range 100 {
		tr := NewNativeTxn(db, DefaultNativeOptions())
		ch, complete, err := tr.Reader().RegisterWatch([]byte("key"), nil)
		if err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		wg.Go(func() { complete(nil) })
		wg.Go(tr.Close)
		wg.Wait()
		if err := <-ch; err != nil && err != fdberr.TransactionCancelled {
			t.Fatalf("watch outcome: %v", err)
		}
		complete(nil)
		if got := db.Watches.outstanding.Load(); got != 0 {
			t.Fatalf("racing completions left %d reservations", got)
		}
		select {
		case err := <-ch:
			t.Fatalf("duplicate watch outcome: %v", err)
		default:
		}
	}
}
