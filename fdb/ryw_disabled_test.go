package fdb

// Reads with read_your_writes_disable go straight to the native transaction
// (internal/client's NativeRead paths).

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestReadYourWritesDisabledReads(t *testing.T) {
	db := openTestDB(t)
	ctx := testCtx(t)
	p := testPrefix(t, db)
	k := func(i int) []byte { return key(p, fmt.Sprintf("%03d", i)) }
	if err := db.Transact(ctx, func(tr *Transaction) error {
		for i := range 100 {
			tr.Set(k(i), []byte{byte(i)})
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	tr := mustTr(t, db)
	if err := tr.Options().SetReadYourWritesDisable(); err != nil {
		t.Fatal(err)
	}
	v, err := tr.Get(ctx, k(5))
	if err != nil || !bytes.Equal(v, []byte{5}) {
		t.Fatalf("get = %x, %v", v, err)
	}
	got, err := tr.GetKey(ctx, FirstGreaterThan(k(5)).Add(2))
	if err != nil || !bytes.Equal(got, k(8)) {
		t.Fatalf("get key = %q, %v", got, err)
	}
	rows, err := sliceRange(ctx, tr, KeyRange{k(10), k(20)}, RangeOptions{})
	if err != nil || len(rows) != 10 {
		t.Fatalf("range = %d rows, %v", len(rows), err)
	}
	// A reverse read whose end selector must be resolved first.
	rows, err = sliceRange(ctx, tr, SelectorRange{FirstGreaterOrEqual(k(10)), FirstGreaterOrEqual(k(20)).Add(2)}, RangeOptions{Reverse: true})
	if err != nil || len(rows) != 12 || !bytes.Equal(rows[0].Key, k(21)) {
		t.Fatalf("reverse range = %d rows, %v", len(rows), err)
	}
	// Writes are not visible to reads.
	tr.Set(k(5), []byte("new"))
	if v, err := tr.Get(ctx, k(5)); err != nil || !bytes.Equal(v, []byte{5}) {
		t.Fatalf("get after set = %q, %v", v, err)
	}
	// Non-snapshot reads still add read conflict ranges (coalesced).
	ranges := mustConflicts(t, ctx, tr, readConflictPrefix)
	if !covers(ranges, k(5), append(k(5), 0)) || !covers(ranges, k(8), append(k(8), 0)) || !covers(ranges, k(10), k(22)) {
		t.Fatalf("read conflicts %q", ranges)
	}
	if err := tr.Watch(k(1)).Wait(ctx); !errors.Is(err, ErrWatchesDisabled) {
		t.Fatalf("watch = %v, want watches_disabled", err)
	}
}

func TestSynchronousSnapshotReadPreventsReadYourWritesDisable(t *testing.T) {
	db := openParityMemoryDB(t)
	tr := mustTr(t, db)
	defer tr.Close()
	key, err := tr.Snapshot().GetKey(t.Context(), FirstGreaterOrEqual([]byte{0xff}))
	if err != nil || !bytes.Equal(key, []byte{0xff}) {
		t.Fatalf("end-of-keyspace selector = %x, %v", key, err)
	}
	if err := tr.Options().SetReadYourWritesDisable(); err != nil {
		t.Fatal(err)
	}
	if _, err := tr.Snapshot().GetKey(t.Context(), FirstGreaterOrEqual([]byte{0xff})); !errors.Is(err, ErrClientInvalidOperation) {
		t.Fatalf("read after disabling used transaction = %v; want client_invalid_operation", err)
	}
}

// TestCancelDuringBackoff checks Cancel ends an OnError backoff with
// transaction_cancelled.
func TestCancelDuringBackoff(t *testing.T) {
	db := openTestDB(t)
	ctx := testCtx(t)
	tr := mustTr(t, db)
	if err := tr.Options().SetMaxRetryDelay(5000); err != nil {
		t.Fatal(err)
	}
	// Grow the backoff past a few milliseconds.
	for range 6 {
		_ = tr.OnError(ctx, ErrNotCommitted)
	}
	// The delay is the backoff times a random fraction, so it can end before
	// Cancel: retry until Cancel lands inside a backoff.
	var done chan error
	for attempt := 0; ; attempt++ {
		done = make(chan error, 1)
		go func() { done <- tr.OnError(ctx, ErrNotCommitted) }()
		time.Sleep(20 * time.Millisecond)
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("on error = %v", err)
			}
			if attempt == 20 {
				t.Fatal("every backoff ended within 20ms")
			}
			continue
		default:
		}
		break
	}
	tr.Cancel()
	select {
	case err := <-done:
		if !errors.Is(err, ErrTransactionCancelled) {
			t.Fatalf("on error = %v, want transaction_cancelled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancel did not end the backoff")
	}
	if _, err := tr.Get(ctx, []byte("x")); !errors.Is(err, ErrTransactionCancelled) {
		t.Fatalf("get after cancel = %v", err)
	}
}

// TestVersionstampAfterReset checks that a new attempt has no committed stamp.
func TestVersionstampAfterReset(t *testing.T) {
	db := openTestDB(t)
	ctx := testCtx(t)
	tr := mustTr(t, db)
	tr.Set(key(testPrefix(t, db), "stamp"), []byte("value"))
	mustCommit(t, ctx, tr)
	stamp, err := tr.Versionstamp()
	if err != nil || len(stamp) != 10 {
		t.Fatalf("committed versionstamp = %x, %v", stamp, err)
	}
	saved := bytes.Clone(stamp)
	tr.Reset()
	if _, err := tr.Versionstamp(); !errors.Is(err, ErrTransactionInvalidVersion) {
		t.Fatalf("versionstamp after Reset = %v; want transaction_invalid_version", err)
	}
	if err := tr.OnError(ctx, ErrNotCommitted); err != nil {
		t.Fatal(err)
	}
	if _, err := tr.Versionstamp(); !errors.Is(err, ErrTransactionInvalidVersion) {
		t.Fatalf("versionstamp after OnError = %v; want transaction_invalid_version", err)
	}
	if !bytes.Equal(stamp, saved) {
		t.Fatal("reset invalidated the returned stamp")
	}
}

func TestInternalCompletedVersionstampSurvivesReset(t *testing.T) {
	db := openTestDB(t)
	ctx := testCtx(t)
	tr := mustTr(t, db)
	_, wait, err := tr.t.GetVersionstamp()
	if err != nil {
		t.Fatal(err)
	}
	if err := tr.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	tr.Reset()
	if _, err := wait(ctx); !errors.Is(err, ErrNoCommitVersion) {
		t.Fatalf("versionstamp = %v, want no_commit_version", err)
	}
}

// covers reports whether one of ranges contains [begin, end).
func covers(ranges []KeyRange, begin, end []byte) bool {
	for _, r := range ranges {
		if bytes.Compare(r.Begin, begin) <= 0 && bytes.Compare(end, r.End) <= 0 {
			return true
		}
	}
	return false
}
