package fdb

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/can1357/fdbgo/internal/kv"
	"github.com/can1357/fdbgo/internal/ryw"
)

// cancellationReadGate holds storage replies until the test releases them.
type cancellationReadGate struct {
	release chan struct{}
	err     error
}

func (g *cancellationReadGate) StartReadVersion() {}

func (g *cancellationReadGate) wait(ctx context.Context) error {
	select {
	case <-g.release:
		return g.err
	case <-ctx.Done():
		return ErrOperationCancelled
	}
}

func (g *cancellationReadGate) Get(ctx context.Context, _ []byte) ([]byte, error) {
	return nil, g.wait(ctx)
}

func (g *cancellationReadGate) GetMany(ctx context.Context, keys [][]byte) []kv.ValueResult {
	err := g.wait(ctx)
	results := make([]kv.ValueResult, len(keys))
	for i := range results {
		results[i].Err = err
	}
	return results
}

func (g *cancellationReadGate) GetKey(ctx context.Context, _ kv.KeySelector) ([]byte, error) {
	return []byte{0xff}, g.wait(ctx)
}

func (g *cancellationReadGate) GetRange(ctx context.Context, _, _ kv.KeySelector, _ kv.Limits, _ bool) (kv.RangeResult, error) {
	return kv.RangeResult{ReadToBegin: true, ReadThroughEnd: true}, g.wait(ctx)
}

func TestPendingReadPreservesCommitTracking(t *testing.T) {
	for _, kind := range []string{"get", "get-many", "get-key", "get-range", "native"} {
		for _, snapshot := range []bool{false, true} {
			for _, outcome := range []string{"success", "read-error", "transaction-cancel", "context-cancel"} {
				t.Run(fmt.Sprintf("%s/snapshot=%v/%s", kind, snapshot, outcome), func(t *testing.T) {
					synctest.Test(t, func(t *testing.T) {
						r := ryw.New(ryw.NewRywOptions(1))
						defer r.Cancel()
						gate := &cancellationReadGate{release: make(chan struct{})}
						if outcome == "read-error" {
							gate.err = ErrTransactionTooOld
						}
						var wait func(context.Context) error
						var err error
						begin, end := FirstGreaterOrEqual([]byte("a")), FirstGreaterOrEqual([]byte("z"))
						switch kind {
						case "get":
							_, pending, startErr := r.Get(gate, []byte("a"), snapshot)
							err = startErr
							wait = func(ctx context.Context) error { _, err := pending(ctx); return err }
						case "get-many":
							_, pending, startErr := r.GetMany(gate, [][]byte{[]byte("a"), []byte("b")}, snapshot)
							err = startErr
							wait = func(ctx context.Context) error { _, err := pending(ctx); return err }
						case "get-key":
							_, pending, startErr := r.GetKey(gate, begin, snapshot)
							err = startErr
							wait = func(ctx context.Context) error { _, err := pending(ctx); return err }
						case "get-range":
							_, pending, startErr := r.GetRange(gate, begin, end, kv.DefaultLimits(), snapshot, false)
							err = startErr
							wait = func(ctx context.Context) error { _, err := pending(ctx); return err }
						case "native":
							_, pending, startErr := ryw.NativeRead(r, func(ryw.RywOptions, func([]byte, []byte)) (int, func(context.Context) (int, error), error) {
								return 0, func(ctx context.Context) (int, error) { return 1, gate.wait(ctx) }, nil
							})
							err = startErr
							wait = func(ctx context.Context) error { _, err := pending(ctx); return err }
						}
						if err != nil {
							t.Fatal(err)
						}
						ctx, cancel := context.WithCancel(context.Background())
						defer cancel()
						readDone := make(chan error, 1)
						go func() { readDone <- wait(ctx) }()
						synctest.Wait()
						r.Set([]byte("write"), []byte("value"))
						_, commitWait, err := r.Commit()
						if err != nil || commitWait == nil {
							t.Fatalf("commit did not retain pending read: pending=%v, %v", commitWait != nil, err)
						}
						want := gate.err
						switch outcome {
						case "transaction-cancel":
							r.Cancel()
							want = ErrTransactionCancelled
						case "context-cancel":
							cancel()
							want = ErrOperationCancelled
						default:
							close(gate.release)
						}
						if err := <-readDone; !errors.Is(err, want) {
							t.Fatalf("read = %v; want %v", err, want)
						}
						plan, err := commitWait(context.Background())
						if !errors.Is(err, want) {
							t.Fatalf("commit = %v; want %v", err, want)
						}
						if err == nil {
							if len(plan.Mutations) != 1 {
								t.Fatalf("commit lost its write: %v", plan.Mutations)
							}
							if kind != "native" && (len(plan.ReadConflicts) != 0) == snapshot {
								t.Fatalf("commit read conflicts (snapshot=%v): %v", snapshot, plan.ReadConflicts)
							}
						}
					})
				})
			}
		}
	}
}

func TestBlockingReadCancellationSettlesTracking(t *testing.T) {
	for _, kind := range []string{"get", "get-many", "get-key", "get-range"} {
		t.Run(kind, func(t *testing.T) {
			db := openParityMemoryDB(t)
			tr := mustTr(t, db)
			defer tr.Close()
			ctx, cancel := context.WithCancel(testCtx(t))
			defer cancel()
			readCtx := &readWaitContext{Context: ctx, waiting: make(chan struct{})}
			done := make(chan error, 1)
			go func() {
				var err error
				switch kind {
				case "get":
					_, err = tr.Get(readCtx, []byte("unavailable"))
				case "get-many":
					_, err = tr.GetMany(readCtx, [][]byte{[]byte("first"), []byte("second")})
				case "get-key":
					_, err = tr.GetKey(readCtx, FirstGreaterOrEqual([]byte("unavailable")))
				case "get-range":
					_, err = tr.GetRange(readCtx, KeyRange{[]byte("a"), []byte("z")}, RangeOptions{})
				}
				done <- err
			}()
			select {
			case <-readCtx.waiting:
			case <-ctx.Done():
				t.Fatal("read did not start waiting")
			}
			cancel()
			if err := <-done; !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled read = %v; want context.Canceled", err)
			}
			tr.Set([]byte("write"), []byte("value"))
			if err := tr.Commit(testCtx(t)); !errors.Is(err, ErrOperationCancelled) {
				t.Fatalf("commit after cancelled read = %v; want operation_cancelled", err)
			}
		})
	}
}

func TestGetManyRegistersAllReadsBeforeWaiting(t *testing.T) {
	for _, snapshot := range []bool{false, true} {
		t.Run(fmt.Sprintf("snapshot=%v", snapshot), func(t *testing.T) {
			db := openTestDB(t)
			ctx, cancel := context.WithCancel(testCtx(t))
			defer cancel()
			prefix := testPrefix(t, db)
			remote, local := key(prefix, "remote"), key(prefix, "local")
			if err := db.Transact(ctx, func(tr *Transaction) error {
				tr.Set(remote, []byte("stored"))
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			tr := mustTr(t, db)
			defer tr.Close()
			tr.Set(local, []byte("before"))
			var reader ReadTransaction = tr
			if snapshot {
				reader = tr.Snapshot()
			}
			release := make(chan struct{})
			readCtx := &readWaitContext{Context: ctx, waiting: make(chan struct{}), release: release}
			done := make(chan error, 1)
			var values [][]byte
			go func() {
				var err error
				values, err = reader.GetMany(readCtx, [][]byte{remote, local})
				done <- err
			}()
			select {
			case <-readCtx.waiting:
			case <-ctx.Done():
				t.Fatal("GetMany did not start waiting")
			}
			// Hold the first wait until this write changes the transaction's
			// current view. The second read must retain its earlier view.
			tr.Set(local, []byte("after"))
			close(release)
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			if len(values) != 2 || string(values[0]) != "stored" || string(values[1]) != "before" {
				t.Fatalf("GetMany issue-time values = %q; want [stored before]", values)
			}
			current, err := reader.Get(ctx, local)
			if err != nil || string(current) != "after" {
				t.Fatalf("current local value = %q, %v; want after", current, err)
			}
		})
	}
}

func TestCancelledWatchImmediatelyReleasesQuota(t *testing.T) {
	db := openParityMemoryDB(t)
	if err := db.Options().SetMaxWatches(1); err != nil {
		t.Fatal(err)
	}
	tr := mustTr(t, db)
	defer tr.Close()
	key := []byte("cancelled-watch")
	tr.Set(key, []byte("value"))
	watch := tr.Watch(key)
	if watch.IsReady() {
		t.Fatalf("watch setup = %v", watch.Wait(t.Context()))
	}
	watch.Cancel()
	watch.Cancel()
	replacement := tr.Watch(key)
	defer replacement.Cancel()
	if replacement.IsReady() {
		t.Fatalf("watch immediately after Cancel = %v", replacement.Wait(t.Context()))
	}
	if err := watch.Wait(t.Context()); !errors.Is(err, ErrOperationCancelled) {
		t.Fatalf("cancelled watch = %v", err)
	}
}

func TestWatchWaitCancellationDoesNotCancelWatch(t *testing.T) {
	db := openParityMemoryDB(t)
	tr := mustTr(t, db)
	defer tr.Close()
	key := []byte("watch-wait")
	tr.Set(key, []byte("before"))
	watch := tr.Watch(key)
	defer watch.Cancel()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := watch.Wait(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled Wait = %v", err)
	}
	if watch.IsReady() {
		t.Fatal("cancelled Wait completed the watch")
	}
	tr.Set(key, []byte("after"))
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			if err := watch.Wait(t.Context()); err != nil {
				t.Errorf("repeated Wait = %v", err)
			}
		})
	}
	wg.Wait()
	watch.Cancel()
	if err := watch.Wait(t.Context()); err != nil {
		t.Fatalf("Cancel changed completed watch result: %v", err)
	}
	if !watch.IsReady() {
		t.Fatal("completed watch is not ready")
	}
}
