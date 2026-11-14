package fdb

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"
)

func collectUntil(t *testing.T, done <-chan struct{}) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		runtime.GC()
		select {
		case <-done:
			return
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("abandoned operation was not reclaimed")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestDonePinsAbandonedWatchUntilCompletion(t *testing.T) {
	db := openParityMemoryDB(t)
	tr := mustTr(t, db)
	defer tr.Close()
	key := []byte("done-watch")
	tr.Set(key, []byte("before"))
	makeWatch := func() <-chan struct{} { return tr.Watch(key).Done() }
	done := makeWatch()
	for range 3 {
		runtime.GC()
	}
	select {
	case <-done:
		t.Fatal("Done-only waiter was cancelled by reclamation")
	default:
	}
	tr.Set(key, []byte("after"))
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Done-only waiter did not complete")
	}
}

func TestAbandonedTransactionCancelsPendingVersionstamp(t *testing.T) {
	db := openParityMemoryDB(t)
	done := make(chan struct{})
	result := make(chan error, 1)
	makeTransaction := func() {
		tr := mustTr(t, db)
		_, wait, err := tr.t.GetVersionstamp()
		if err != nil || wait == nil {
			t.Fatalf("versionstamp: pending=%v, %v", wait != nil, err)
		}
		go func() {
			_, err := wait(context.Background())
			result <- err
			close(done)
		}()
		runtime.KeepAlive(tr)
	}
	makeTransaction()
	collectUntil(t, done)
	if err := <-result; !errors.Is(err, ErrTransactionCancelled) {
		t.Fatalf("abandoned transaction's versionstamp = %v", err)
	}
}

func TestRetainedWatchKeepsTransactionAlive(t *testing.T) {
	db := openParityMemoryDB(t)
	tr := mustTr(t, db)
	key := []byte("local")
	tr.Set(key, []byte("before"))
	watch := tr.Watch(key)
	internal := tr.t // Observing the engine must not retain its public owner.
	tr = nil
	for range 3 {
		runtime.GC()
	}
	internal.Set(key, []byte("after"))
	if err := watch.Wait(testCtx(t)); err != nil {
		t.Fatalf("transaction retained by watch: %v", err)
	}
	internal.Close()
	runtime.KeepAlive(watch)
}

func TestRangeAndSnapshotKeepTransactionAlive(t *testing.T) {
	db := openParityMemoryDB(t)
	ctx := testCtx(t)
	makeViews := func() (*RangeResult, Snapshot) {
		tr := mustTr(t, db)
		tr.ClearRange(KeyRange{[]byte("a"), []byte("z")})
		tr.Set([]byte("key"), []byte("value"))
		return mustRange(t, ctx, tr, KeyRange{[]byte("a"), []byte("z")}, RangeOptions{}), tr.Snapshot()
	}
	r, snapshot := makeViews()
	for range 3 {
		runtime.GC()
	}
	rows, err := r.Slice(ctx)
	if err != nil || len(rows) != 1 || string(rows[0].Value) != "value" {
		t.Fatalf("retained range = %v, %v", rows, err)
	}
	v, err := snapshot.Get(ctx, []byte("key"))
	if err != nil || string(v) != "value" {
		t.Fatalf("retained snapshot = %q, %v", v, err)
	}
	snapshot.t.Close()
}

func TestTransactionClosePreservesReturnedValues(t *testing.T) {
	db := openParityMemoryDB(t)
	tr := mustTr(t, db)
	key := []byte("key")
	tr.Set(key, []byte("value"))
	value, err := tr.Get(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}
	watch := tr.Watch(key)
	tr.Close()
	tr.Close()
	if string(value) != "value" {
		t.Fatalf("completed read after Close = %q", value)
	}
	if err := watch.Wait(testCtx(t)); !errors.Is(err, ErrTransactionCancelled) {
		t.Fatalf("pending watch after Close = %v", err)
	}
	if _, err := tr.Get(t.Context(), key); !errors.Is(err, ErrTransactionCancelled) {
		t.Fatalf("read after Close = %v", err)
	}
}
