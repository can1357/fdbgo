package fdb

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"
)

// setTestOptions gives tr a 30-second timeout.
func setTestOptions(tr *Transaction) error {
	if err := tr.Options().SetRetryLimit(10); err != nil {
		return err
	}
	return tr.Options().SetTimeout(30_000)
}

func TestAtomic(t *testing.T) {
	const n = 1000
	db := openTestDB(t)
	ctx := testCtx(t)
	k := key(testPrefix(t, db), "test-atomic")

	if err := db.Transact(ctx, func(tr *Transaction) error {
		if err := setTestOptions(tr); err != nil {
			return err
		}
		tr.Clear(k)
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	add := func(delta int64) error {
		return db.Transact(ctx, func(tr *Transaction) error {
			if err := setTestOptions(tr); err != nil {
				return err
			}
			tr.Add(k, binary.LittleEndian.AppendUint64(nil, uint64(delta)))
			return nil
		})
	}
	var wg sync.WaitGroup
	errs := make(chan error, 2*n)
	for range n {
		wg.Go(func() { errs <- add(1) })
		wg.Go(func() { errs <- add(-1) })
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}

	v, err := ReadTransact(ctx, db, func(tr ReadTransaction) ([]byte, error) {
		return tr.Get(ctx, k)
	})
	if err != nil || len(v) != 8 || binary.LittleEndian.Uint64(v) != 0 {
		t.Fatalf("counter = %x, %v; want 0", v, err)
	}
}

func TestWatch(t *testing.T) {
	db := openTestDB(t)
	ctx := testCtx(t)
	k := key(testPrefix(t, db), "test-watch")

	tr := mustTr(t, db)
	w := tr.Watch(k)
	mustCommit(t, ctx, tr)

	tr = mustTr(t, db)
	tr.Set(k, []byte(randStr(10)))
	mustCommit(t, ctx, tr)

	wctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := w.Wait(wctx); err != nil {
		t.Fatal(err)
	}
}

// TestWatchWithoutCommit checks that a watch on a transaction that is
// cancelled instead of committed fails with transaction_cancelled.
func TestWatchWithoutCommit(t *testing.T) {
	db := openTestDB(t)
	ctx := testCtx(t)
	k := key(testPrefix(t, db), "test-watch-2")

	tr := mustTr(t, db)
	w := tr.Watch(k)
	tr.Cancel()
	if err := w.Wait(ctx); !errors.Is(err, ErrTransactionCancelled) {
		t.Fatalf("watch = %v, want transaction_cancelled", err)
	}
}

// TestWatchOutlivesTransact checks a watch set inside Transact fires after
// Transact returned.
func TestWatchOutlivesTransact(t *testing.T) {
	db := openTestDB(t)
	ctx := testCtx(t)
	k := key(testPrefix(t, db), "watch")
	w, err := Transact(ctx, db, func(tr *Transaction) (*Watch, error) {
		return tr.Watch(k), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Transact(ctx, func(tr *Transaction) error {
		tr.Set(k, []byte(randStr(10)))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	wctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := w.Wait(wctx); err != nil {
		t.Fatal(err)
	}
}

func TestStatus(t *testing.T) {
	db := openTestDB(t)
	ctx := testCtx(t)
	status, err := db.ClientStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err := json.Unmarshal(status, &doc); err != nil {
		t.Fatalf("status is not JSON: %v: %s", err, status)
	}
	if _, ok := doc["Healthy"]; !ok {
		t.Fatalf("no Healthy in %s", status)
	}
}

// TestMainThreadBusyness checks that busyness is in [0, 1]. The sampler is
// process-wide and other tests share the process, so only the range is checked.
func TestMainThreadBusyness(t *testing.T) {
	db := openTestDB(t)
	if b := db.MainThreadBusyness(); b < 0 || b > 1 {
		t.Fatalf("busyness = %v", b)
	}
}
