package fdb

import (
	"bytes"
	"errors"
	"fmt"
	"runtime"
	"testing"
)

func TestAddressLookupFailureFailsCommit(t *testing.T) {
	db := openTestDB(t)
	ctx := testCtx(t)
	p := testPrefix(t, db)
	for _, write := range []bool{false, true} {
		t.Run(fmt.Sprintf("write=%v", write), func(t *testing.T) {
			tr := mustTr(t, db)
			defer tr.Close()
			tr.SetReadVersion(1)
			if _, err := tr.AddressesForKey(ctx, key(p, "lookup")); !errors.Is(err, ErrTransactionTooOld) {
				t.Fatalf("lookup = %v; want transaction_too_old", err)
			}
			if write {
				tr.Set(key(p, "write"), []byte("value"))
			}
			if err := tr.Commit(ctx); !errors.Is(err, ErrTransactionTooOld) {
				t.Fatalf("commit after failed lookup = %v; want transaction_too_old", err)
			}
		})
	}
}

func TestMetadataRangesRejectInversionBeforeAttemptErrors(t *testing.T) {
	db := openParityMemoryDB(t)
	ctx := testCtx(t)
	tr := mustTr(t, db)
	defer tr.Close()
	tr.Cancel()
	r := KeyRange{[]byte("a"), []byte{}}
	if _, err := tr.EstimatedRangeSizeBytes(ctx, r); !errors.Is(err, ErrInvertedRange) {
		t.Fatalf("inverted estimate on cancelled transaction = %v", err)
	}
	if _, err := tr.RangeSplitPoints(ctx, r, 1); !errors.Is(err, ErrInvertedRange) {
		t.Fatalf("inverted split on cancelled transaction = %v", err)
	}
	tr.Reset()
	// Equal endpoints are legal and must reach normal attempt validation.
	tr.Cancel()
	r.End = r.Begin
	if _, err := tr.EstimatedRangeSizeBytes(ctx, r); !errors.Is(err, ErrTransactionCancelled) {
		t.Fatalf("equal estimate = %v; want transaction_cancelled, not inverted_range", err)
	}
	if _, err := tr.RangeSplitPoints(ctx, r, 1); !errors.Is(err, ErrTransactionCancelled) {
		t.Fatalf("equal split = %v; want transaction_cancelled, not inverted_range", err)
	}
}

func TestRawAccessLongKeysAreReadable(t *testing.T) {
	db := openTestDB(t)
	ctx := testCtx(t)
	p := testPrefix(t, db)
	for _, size := range []int{10_001, 10_008} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			k := append(bytes.Clone(p), bytes.Repeat([]byte("k"), size-len(p))...)
			seed := mustTr(t, db)
			defer seed.Close()
			if err := seed.Options().SetRawAccess(); err != nil {
				t.Fatal(err)
			}
			seed.Set(k, []byte("stored"))
			mustCommit(t, ctx, seed)
			for _, disabled := range []bool{false, true} {
				tr := mustTr(t, db)
				if disabled {
					if err := tr.Options().SetReadYourWritesDisable(); err != nil {
						t.Fatal(err)
					}
				}
				for _, read := range []ReadTransaction{tr, tr.Snapshot()} {
					v, err := read.Get(ctx, k)
					if err != nil || string(v) != "stored" {
						t.Fatalf("long read (ryw disabled=%v) = %q, %v", disabled, v, err)
					}
				}
				tr.Close()
			}
		})
	}
}

func TestAbandonedWatchReleasesQuota(t *testing.T) {
	for _, committed := range []bool{false, true} {
		t.Run(fmt.Sprintf("committed=%v", committed), func(t *testing.T) {
			var db *Database
			if committed {
				db = openTestDB(t)
			} else {
				db = openParityMemoryDB(t)
			}
			ctx := testCtx(t)
			if err := db.Options().SetMaxWatches(1); err != nil {
				t.Fatal(err)
			}
			k := []byte("local-watch")
			if committed {
				k = key(testPrefix(t, db), "watch")
			}
			tr := mustTr(t, db)
			defer tr.Close()
			tr.Set(k, []byte("value"))
			makeWatch := func() <-chan struct{} {
				watch := tr.Watch(k)
				if watch.IsReady() {
					t.Fatalf("watch setup = %v", watch.Wait(ctx))
				}
				if committed {
					mustCommit(t, ctx, tr)
					tr.Close()
				}
				// Observe reclamation without transferring ownership through Done.
				done := watch.state.released
				runtime.KeepAlive(watch)
				return done
			}
			collectUntil(t, makeWatch())
			if committed {
				tr = mustTr(t, db)
				defer tr.Close()
				tr.Set(k, []byte("value"))
			}
			watch := tr.Watch(k)
			if watch.IsReady() {
				t.Fatalf("quota after abandoning watch = %v", watch.Wait(ctx))
			}
			watch.Cancel()
		})
	}
}
