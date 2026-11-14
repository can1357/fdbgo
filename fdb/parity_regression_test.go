package fdb

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
)

func openParityMemoryDB(t *testing.T) *Database {
	t.Helper()
	db, err := OpenConnectionString("parity:parity@127.0.0.1:1")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestNativeReverseRangeEndSelectorConflict(t *testing.T) {
	db := openTestDB(t)
	ctx := testCtx(t)
	for _, snapshot := range []bool{false, true} {
		t.Run(fmt.Sprintf("snapshot=%v", snapshot), func(t *testing.T) {
			p := testPrefix(t, db)
			seed := mustTr(t, db)
			for _, suffix := range []string{"b", "d", "f"} {
				seed.Set(key(p, suffix), []byte(suffix))
			}
			mustCommit(t, ctx, seed)
			tr := mustTr(t, db)
			if err := tr.Options().SetReadYourWritesDisable(); err != nil {
				t.Fatal(err)
			}
			var reader ReadTransaction = tr
			if snapshot {
				reader = tr.Snapshot()
			}
			r := SelectorRange{FirstGreaterOrEqual(key(p, "a")), FirstGreaterOrEqual(key(p, "b")).Add(2)}
			rows, err := sliceRange(ctx, reader, r, RangeOptions{Reverse: true, Limit: 1})
			if err != nil || len(rows) != 1 || !bytes.Equal(rows[0].Key, key(p, "d")) {
				t.Fatalf("reverse range = %q, %v; want d", rows, err)
			}
			other := mustTr(t, db)
			other.Set(key(p, "c"), []byte("inserted"))
			mustCommit(t, ctx, other)
			tr.Set(key(p, "z"), []byte("write"))
			err = tr.Commit(ctx)
			if snapshot {
				if err != nil {
					t.Fatalf("snapshot commit = %v", err)
				}
			} else if !errors.Is(err, ErrNotCommitted) {
				t.Fatalf("commit after selector changed = %v; want not_committed (1020)", err)
			}
		})
	}
}

func TestOnErrorRefreshesSnapshotRYWDatabaseCounter(t *testing.T) {
	for _, delay := range []int64{0, 1} {
		t.Run(fmt.Sprintf("retry-delay=%d", delay), func(t *testing.T) {
			db := openTestDB(t)
			ctx := testCtx(t)
			k := key(testPrefix(t, db), "value")
			seed := mustTr(t, db)
			seed.Set(k, []byte("committed"))
			mustCommit(t, ctx, seed)
			tr := mustTr(t, db)
			if err := tr.Options().SetMaxRetryDelay(delay); err != nil {
				t.Fatal(err)
			}
			if err := tr.Options().SetRetryLimit(1); err != nil {
				t.Fatal(err)
			}
			tr.Set(k, []byte("old attempt"))
			if value, err := tr.Snapshot().Get(ctx, k); err != nil || string(value) != "old attempt" {
				t.Fatalf("initial snapshot = %q, %v", value, err)
			}
			changeDefaults := func() {
				for range 2 {
					if err := db.Options().SetSnapshotRYWDisable(); err != nil {
						t.Fatal(err)
					}
				}
				if err := db.Options().SetTransactionRetryLimit(0); err != nil {
					t.Fatal(err)
				}
			}
			if delay == 0 {
				changeDefaults()
			}
			_, wait, err := tr.t.OnError(ErrTransactionTooOld)
			if err != nil {
				t.Fatal(err)
			}
			if delay != 0 {
				if wait == nil {
					t.Fatal("expected a backoff before retry reset")
				}
				// Change the database while retry reset is still pending.
				changeDefaults()
			}
			if wait != nil {
				if _, err := wait(ctx); err != nil {
					t.Fatal(err)
				}
			}
			tr.Set(k, []byte("new attempt"))
			for enables, want := range []string{"committed", "committed", "new attempt"} {
				if enables > 0 {
					if err := tr.Options().SetSnapshotRYWEnable(); err != nil {
						t.Fatal(err)
					}
				}
				if value, err := tr.Snapshot().Get(ctx, k); err != nil || string(value) != want {
					t.Fatalf("snapshot after %d enables = %q, %v; want %q", enables, value, err, want)
				}
			}
			if err := tr.OnError(ctx, ErrTransactionTooOld); !errors.Is(err, ErrTransactionTooOld) {
				t.Fatalf("persistent retry limit = %v; want transaction_too_old", err)
			}
		})
	}
}

func TestTransactionTagsAcceptOpaqueBytes(t *testing.T) {
	db := openParityMemoryDB(t)
	ctx := testCtx(t)
	for _, option := range []struct {
		name string
		set  func(TransactionOptions, string) error
	}{
		{"tag", TransactionOptions.SetTag},
		{"auto-throttle-tag", TransactionOptions.SetAutoThrottleTag},
	} {
		t.Run(option.name, func(t *testing.T) {
			tr := mustTr(t, db)
			tag := string([]byte{0xff, 0x80, 0})
			if err := option.set(tr.Options(), tag); err != nil {
				t.Fatalf("opaque tag = %v", err)
			}
			if err := option.set(tr.Options(), tag); err != nil {
				t.Fatal(err)
			}
			for i := range 4 {
				if err := option.set(tr.Options(), string([]byte{0xff, byte(i)})); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := tr.ApproximateSize(ctx); err != nil {
				t.Fatalf("five distinct binary tags = %v", err)
			}
			if err := option.set(tr.Options(), string([]byte{0xff, 5})); err != nil {
				t.Fatal(err)
			}
			if _, err := tr.ApproximateSize(ctx); !errors.Is(err, ErrTooManyTags) {
				t.Fatalf("sixth tag = %v; want too_many_tags", err)
			}
			tr = mustTr(t, db)
			if err := option.set(tr.Options(), string(bytes.Repeat([]byte{0xff}, 16))); err != nil {
				t.Fatal(err)
			}
			if _, err := tr.ApproximateSize(ctx); err != nil {
				t.Fatalf("16-byte opaque tag = %v", err)
			}
			if err := option.set(tr.Options(), string(bytes.Repeat([]byte{0xff}, 17))); err != nil {
				t.Fatal(err)
			}
			if _, err := tr.ApproximateSize(ctx); !errors.Is(err, ErrTagTooLong) {
				t.Fatalf("17-byte opaque tag = %v; want tag_too_long", err)
			}
		})
	}
}

func TestAtomicOpcodeDoesNotWrap(t *testing.T) {
	db := openParityMemoryDB(t)
	ctx := testCtx(t)
	for _, op := range []MutationType{258, -254, 270, -238, 65538} {
		t.Run(fmt.Sprint(op), func(t *testing.T) {
			tr := mustTr(t, db)
			tr.Atomic(op, []byte("key"), []byte{1})
			if _, err := tr.ApproximateSize(ctx); !errors.Is(err, ErrInvalidMutationType) {
				t.Fatalf("atomic %d = %v; want invalid_mutation_type", op, err)
			}
		})
	}
	t.Run("deferred-error-priority", func(t *testing.T) {
		tr := mustTr(t, db)
		tr.Set([]byte{0xff}, nil)
		tr.Atomic(258, []byte("key"), []byte{1})
		if _, err := tr.ApproximateSize(ctx); !errors.Is(err, ErrKeyOutsideLegalRange) {
			t.Fatalf("atomic after deferred error = %v; want key_outside_legal_range", err)
		}
	})
	t.Run("used-during-commit-priority", func(t *testing.T) {
		tr := mustTr(t, db)
		mustCommit(t, ctx, tr)
		tr.Atomic(258, []byte("key"), []byte{1})
		if _, err := tr.ApproximateSize(ctx); !errors.Is(err, ErrUsedDuringCommit) {
			t.Fatalf("atomic after commit = %v; want used_during_commit", err)
		}
	})
}

func TestSpecialGetKeyUsesNormalLegalityChecks(t *testing.T) {
	db := openParityMemoryDB(t)
	ctx := testCtx(t)
	for _, disabled := range []bool{false, true} {
		for _, access := range []string{"default", "system", "raw"} {
			t.Run(fmt.Sprintf("ryw-disabled=%v/access=%s", disabled, access), func(t *testing.T) {
				tr := mustTr(t, db)
				if disabled {
					if err := tr.Options().SetReadYourWritesDisable(); err != nil {
						t.Fatal(err)
					}
				}
				var err error
				switch access {
				case "system":
					err = tr.Options().SetReadSystemKeys()
				case "raw":
					err = tr.Options().SetRawAccess()
				}
				if err != nil {
					t.Fatal(err)
				}
				if err := tr.Options().SetSpecialKeySpaceRelaxed(); err != nil {
					t.Fatal(err)
				}
				for _, reader := range []ReadTransaction{tr, tr.Snapshot()} {
					for _, selector := range []KeySelector{
						FirstGreaterOrEqual([]byte("\xff\xff/connection_string")),
						LastLessThan([]byte(readConflictPrefix)),
					} {
						if _, err := reader.GetKey(ctx, selector); !errors.Is(err, ErrKeyOutsideLegalRange) {
							t.Fatalf("special GetKey(%+v) = %v; want key_outside_legal_range (2004)", selector, err)
						}
					}
				}
			})
		}
	}
}

func TestMemoryDatabaseClusterFilePathIsStable(t *testing.T) {
	db := openParityMemoryDB(t)
	ctx := testCtx(t)
	location := func(tr *Transaction) string {
		t.Helper()
		value, err := tr.Get(ctx, []byte("\xff\xff/cluster_file_path"))
		if err != nil || len(value) == 0 {
			t.Fatalf("memory cluster_file_path = %q, %v; want a nonempty location", value, err)
		}
		return string(value)
	}
	tr := mustTr(t, db)
	want := location(tr)
	tr.Reset()
	if got := location(tr); got != want {
		t.Fatalf("location after reset = %q; want %q", got, want)
	}
	if got := location(mustTr(t, db)); got != want {
		t.Fatalf("location in another transaction = %q; want %q", got, want)
	}
	if got := location(mustTr(t, openParityMemoryDB(t))); got == want {
		t.Fatalf("independent memory records share location %q", got)
	}
}
