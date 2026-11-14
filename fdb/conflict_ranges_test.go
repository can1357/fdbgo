package fdb

// Tests of read and write conflict ranges, read back through the special key
// space by conflictRanges.

import (
	"bytes"
	"context"
	"fmt"
	"testing"
)

const (
	readConflictPrefix  = "\xff\xff/transaction/read_conflict_range/"
	writeConflictPrefix = "\xff\xff/transaction/write_conflict_range/"
)

// conflictRanges decodes the boundary rows ("1" begins a range, "0" ends
// it) under one of the conflict range special keyspaces.
func conflictRanges(ctx context.Context, tr *Transaction, prefix string) ([]KeyRange, error) {
	r := KeyRange{[]byte(prefix), []byte(prefix + "\xff\xff")}
	rows, err := sliceRange(ctx, tr, r, RangeOptions{})
	if err != nil {
		return nil, err
	}
	var out []KeyRange
	var begin []byte
	open := false
	for _, kv := range rows {
		k := kv.Key[len(prefix):]
		switch string(kv.Value) {
		case "1":
			begin, open = k, true
		case "0":
			if open {
				out = append(out, KeyRange{begin, k})
				open = false
			}
		default:
			return nil, fmt.Errorf("unexpected conflict row %q=%q", kv.Key, kv.Value)
		}
	}
	return out, nil
}

func hasRange(ranges []KeyRange, begin, end []byte) bool {
	for _, r := range ranges {
		if bytes.Equal(r.Begin, begin) && bytes.Equal(r.End, end) {
			return true
		}
	}
	return false
}

func mustConflicts(t *testing.T, ctx context.Context, tr *Transaction, prefix string) []KeyRange {
	t.Helper()
	r, err := conflictRanges(ctx, tr, prefix)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// TestReadConflictRangesReportReads: a point read registers key..key\x00, a
// range read the range itself.
func TestReadConflictRangesReportReads(t *testing.T) {
	db := openTestDB(t)
	ctx := testCtx(t)
	p := testPrefix(t, db)
	point, rb, re := key(p, "point"), key(p, "range-a"), key(p, "range-z")
	tr := mustTr(t, db)
	if _, err := tr.Get(ctx, point); err != nil {
		t.Fatal(err)
	}
	if _, err := sliceRange(ctx, tr, KeyRange{rb, re}, RangeOptions{}); err != nil {
		t.Fatal(err)
	}
	ranges := mustConflicts(t, ctx, tr, readConflictPrefix)
	if !hasRange(ranges, point, append(point, 0)) {
		t.Errorf("point read missing from %q", ranges)
	}
	if !hasRange(ranges, rb, re) {
		t.Errorf("range read missing from %q", ranges)
	}
}

// TestSnapshotReadsRegisterNoReadConflictRange: snapshot reads are not serializable.
func TestSnapshotReadsRegisterNoReadConflictRange(t *testing.T) {
	db := openTestDB(t)
	ctx := testCtx(t)
	tr := mustTr(t, db)
	if _, err := tr.Snapshot().Get(ctx, key(testPrefix(t, db), "snapshot")); err != nil {
		t.Fatal(err)
	}
	if r := mustConflicts(t, ctx, tr, readConflictPrefix); len(r) != 0 {
		t.Fatalf("read conflicts %q", r)
	}
}

// TestWriteConflictRangesReportWrites: writes register write conflict
// ranges, and only those.
func TestWriteConflictRangesReportWrites(t *testing.T) {
	db := openTestDB(t)
	ctx := testCtx(t)
	p := testPrefix(t, db)
	k, cb, ce := key(p, "key"), key(p, "cleared-a"), key(p, "cleared-z")
	tr := mustTr(t, db)
	tr.Set(k, []byte("value"))
	tr.ClearRange(KeyRange{cb, ce})
	ranges := mustConflicts(t, ctx, tr, writeConflictPrefix)
	if !hasRange(ranges, k, append(k, 0)) {
		t.Errorf("set missing from %q", ranges)
	}
	if !hasRange(ranges, cb, ce) {
		t.Errorf("cleared range missing from %q", ranges)
	}
	if r := mustConflicts(t, ctx, tr, readConflictPrefix); len(r) != 0 {
		t.Errorf("a write is not a read: %q", r)
	}
}

// TestAddConflictRangeShowsUp: added ranges show up in the keyspace of their type.
func TestAddConflictRangeShowsUp(t *testing.T) {
	db := openTestDB(t)
	ctx := testCtx(t)
	p := testPrefix(t, db)
	rr := KeyRange{key(p, "read-a"), key(p, "read-z")}
	wr := KeyRange{key(p, "write-a"), key(p, "write-z")}
	tr := mustTr(t, db)
	if err := tr.AddReadConflictRange(rr); err != nil {
		t.Fatal(err)
	}
	if err := tr.AddWriteConflictRange(wr); err != nil {
		t.Fatal(err)
	}
	reads := mustConflicts(t, ctx, tr, readConflictPrefix)
	writes := mustConflicts(t, ctx, tr, writeConflictPrefix)
	if !hasRange(reads, rr.Begin, rr.End) || hasRange(reads, wr.Begin, wr.End) {
		t.Errorf("read conflicts %q", reads)
	}
	if !hasRange(writes, wr.Begin, wr.End) || hasRange(writes, rr.Begin, rr.End) {
		t.Errorf("write conflicts %q", writes)
	}
}

// TestFreshTransactionHasNoConflictRange.
func TestFreshTransactionHasNoConflictRange(t *testing.T) {
	db := openTestDB(t)
	ctx := testCtx(t)
	tr := mustTr(t, db)
	if r := mustConflicts(t, ctx, tr, readConflictPrefix); len(r) != 0 {
		t.Errorf("read conflicts %q", r)
	}
	if r := mustConflicts(t, ctx, tr, writeConflictPrefix); len(r) != 0 {
		t.Errorf("write conflicts %q", r)
	}
}

// TestManyConflictRangesAreFullyPaginated: hundreds of ranges span several
// batches of the special key read.
func TestManyConflictRangesAreFullyPaginated(t *testing.T) {
	const n = 500
	db := openTestDB(t)
	ctx := testCtx(t)
	p := testPrefix(t, db)
	tr := mustTr(t, db)
	for i := range n {
		if err := tr.AddReadConflictRange(KeyRange{key(p, fmt.Sprintf("%05d/a", i)), key(p, fmt.Sprintf("%05d/b", i))}); err != nil {
			t.Fatal(err)
		}
	}
	ranges := mustConflicts(t, ctx, tr, readConflictPrefix)
	if len(ranges) != n {
		t.Fatalf("%d conflict ranges, want %d", len(ranges), n)
	}
	for i, r := range ranges {
		if !bytes.Equal(r.Begin, key(p, fmt.Sprintf("%05d/a", i))) || !bytes.Equal(r.End, key(p, fmt.Sprintf("%05d/b", i))) {
			t.Fatalf("range %d = %q", i, r)
		}
	}
	// The iterator mode needs several batches for these rows.
	pages, _, err := countPages(ctx, mustRange(t, ctx, tr, KeyRange{[]byte(readConflictPrefix), []byte(readConflictPrefix + "\xff\xff")}, RangeOptions{}))
	if err != nil || pages <= 1 {
		t.Fatalf("%d pages, %v", pages, err)
	}
}

// TestSpecialKeyErrors checks reads and writes of the special key space fail
// like libfdb_c.
func TestSpecialKeyErrors(t *testing.T) {
	db := openTestDB(t)
	ctx := testCtx(t)
	tr := mustTr(t, db)
	if _, err := tr.Get(ctx, []byte("\xff\xff/nope")); !isErr(err, ErrSpecialKeysNoModuleFound) {
		t.Errorf("unknown module = %v", err)
	}
	if _, err := sliceRange(ctx, tr, KeyRange{[]byte("\xff\xff/transaction/"), []byte("\xff\xff/zzz")}, RangeOptions{}); !isErr(err, ErrSpecialKeysCrossModuleRead) {
		t.Errorf("cross module = %v", err)
	}
	v, err := tr.Get(ctx, []byte("\xff\xff/status/json"))
	if err != nil || !bytes.HasPrefix(v, []byte("{")) {
		t.Errorf("status/json = %.40q, %v", v, err)
	}
	tr.Set([]byte("\xff\xff/connection_string"), []byte("x"))
	if err := tr.Commit(ctx); !isErr(err, ErrSpecialKeysWriteDisabled) {
		t.Errorf("special key write = %v", err)
	}
}
