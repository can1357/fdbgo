package fdb

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/can1357/fdbgo/internal/client"
	"github.com/can1357/fdbgo/internal/kv"
	"github.com/can1357/fdbgo/subspace"
	"github.com/can1357/fdbgo/tuple"
)

func sliceRange(ctx context.Context, tr ReadTransaction, r Range, opts RangeOptions) ([]KeyValue, error) {
	result, err := tr.GetRange(ctx, r, opts)
	if err != nil {
		return nil, err
	}
	return result.Slice(ctx)
}

func sliceMappedRange(ctx context.Context, tr ReadTransaction, r Range, mapper []byte, opts RangeOptions) ([]MappedKeyValue, error) {
	result, err := tr.GetMappedRange(ctx, r, mapper, opts)
	if err != nil {
		return nil, err
	}
	return result.Slice(ctx)
}

func mustRange(t *testing.T, ctx context.Context, tr ReadTransaction, r Range, opts RangeOptions) *RangeResult {
	t.Helper()
	result, err := tr.GetRange(ctx, r, opts)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func mustMappedRange(t *testing.T, ctx context.Context, tr ReadTransaction, r Range, mapper []byte, opts RangeOptions) *MappedRangeResult {
	t.Helper()
	result, err := tr.GetMappedRange(ctx, r, mapper, opts)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

// countPages reads r batch by batch like All and reports the number of
// batches and rows.
func countPages(ctx context.Context, r *RangeResult) (pages, rows int, err error) {
	c, b := r.start, r.first
	for {
		pages++
		rows += len(b.Data)
		n := len(b.Data)
		if n == 0 || !c.advance(n, b.More, b.Data[n-1].Key) {
			return pages, rows, nil
		}
		var wait func(context.Context) (kv.RangeResult, error)
		b, wait, err = r.t.t.GetRange(c.begin, c.end, c.cOpts(), r.snapshot)
		if err == nil && wait != nil {
			b, err = wait(ctx)
		}
		if err != nil {
			return pages, rows, err
		}
	}
}

// countMappedPages is countPages for a mapped range.
func countMappedPages(ctx context.Context, r *MappedRangeResult) (pages, rows int, err error) {
	c, b := r.start, r.first
	for {
		pages++
		rows += len(b.Data)
		n := len(b.Data)
		if n == 0 || !c.advance(n, b.More, b.Data[n-1].Key) {
			return pages, rows, nil
		}
		var wait func(context.Context) (client.MappedRangeResult, error)
		b, wait, err = r.t.t.GetMappedRange(c.begin, c.end, r.mapper, c.cOpts(), r.snapshot)
		if err == nil && wait != nil {
			b, err = wait(ctx)
		}
		if err != nil {
			return pages, rows, err
		}
	}
}

// collect drains All.
func collect(ctx context.Context, r *RangeResult) ([]KeyValue, error) {
	var out []KeyValue
	for kv, err := range r.All(ctx) {
		if err != nil {
			return nil, err
		}
		out = append(out, kv)
	}
	return out, nil
}

// TestGetRangeReadsOwnWrites checks a range read over the
// transaction's own uncommitted writes.
func TestGetRangeReadsOwnWrites(t *testing.T) {
	const n = 10000
	db := openTestDB(t)
	ctx := testCtx(t)
	p := testPrefix(t, db)
	tr := mustTr(t, db)
	for range n {
		tr.Set(key(p, "-"+randStr(10)), []byte(randStr(10)))
	}
	r := SelectorRange{FirstGreaterOrEqual(p), FirstGreaterThan(prefixRange(t, p).End)}

	// The first batch of the iterator mode is partial.
	first := mustRange(t, ctx, tr, r, RangeOptions{}).first
	if len(first.Data) == 0 || !first.More {
		t.Fatalf("first batch: %d rows, more=%v", len(first.Data), first.More)
	}
	for _, kv := range first.Data {
		if len(kv.Key) == 0 || len(kv.Value) == 0 {
			t.Fatalf("empty row %q=%q", kv.Key, kv.Value)
		}
	}

	all, err := collect(ctx, mustRange(t, ctx, tr, r, RangeOptions{}))
	if err != nil {
		t.Fatal(err)
	}
	slice, err := sliceRange(ctx, tr, r, RangeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	rev, err := sliceRange(ctx, tr, r, RangeOptions{Reverse: true})
	if err != nil {
		t.Fatal(err)
	}
	slices.Reverse(rev)
	if len(all) != n || !slices.EqualFunc(all, slice, kvEqual) || !slices.EqualFunc(all, rev, kvEqual) {
		t.Fatalf("All %d rows, Slice %d, reverse %d", len(all), len(slice), len(rev))
	}
	if !slices.IsSortedFunc(all, func(a, b KeyValue) int { return bytes.Compare(a.Key, b.Key) }) {
		t.Fatal("rows out of order")
	}
}

func kvEqual(a, b KeyValue) bool { return bytes.Equal(a.Key, b.Key) && bytes.Equal(a.Value, b.Value) }

// TestRangeBatches checks that batches of an unlimited read add up to the range, in both directions.
func TestRangeBatches(t *testing.T) {
	const n = 10000
	db := openTestDB(t)
	ctx := testCtx(t)
	p := testPrefix(t, db)
	tr := mustTr(t, db)
	for range n {
		tr.Set(key(p, "-"+randStr(10)), []byte(randStr(10)))
	}
	r := SelectorRange{FirstGreaterOrEqual(p), FirstGreaterThan(prefixRange(t, p).End)}
	for _, reverse := range []bool{false, true} {
		pages, rows, err := countPages(ctx, mustRange(t, ctx, tr, r, RangeOptions{Reverse: reverse}))
		if err != nil {
			t.Fatal(err)
		}
		if pages <= 1 || rows != n {
			t.Errorf("reverse=%v: %d pages, %d rows; want paginated %d rows", reverse, pages, rows, n)
		}
	}
}

// TestRangeBounds checks range bounds given as key ranges and selectors.
func TestRangeBounds(t *testing.T) {
	db := openTestDB(t)
	ctx := testCtx(t)
	p := testPrefix(t, db)
	k := func(i int) []byte { return key(p, fmt.Sprintf("-%010d", i)) }
	tr := mustTr(t, db)
	for i := range 10000 {
		tr.Set(k(i), []byte(randStr(10)))
	}
	cases := []struct {
		r    Range
		want int
	}{
		{KeyRange{k(100), k(5000)}, 4900},
		{SelectorRange{FirstGreaterOrEqual(k(100)), FirstGreaterOrEqual(k(5000))}, 4900},
		{SelectorRange{FirstGreaterOrEqual(k(100)), FirstGreaterThan(k(5000))}, 4901},
		{SelectorRange{FirstGreaterThan(k(99)), LastLessOrEqual(k(5000)).Add(1)}, 4901},
	}
	for i, c := range cases {
		n := 0
		for _, err := range mustRange(t, ctx, tr, c.r, RangeOptions{}).All(ctx) {
			if err != nil {
				t.Fatal(err)
			}
			n++
		}
		if n != c.want {
			t.Errorf("case %d: %d rows, want %d", i, n, c.want)
		}
	}
	// Limits and reverse reads.
	got, err := sliceRange(ctx, tr, KeyRange{k(100), k(5000)}, RangeOptions{Limit: 10, Reverse: true})
	if err != nil || len(got) != 10 || !bytes.Equal(got[0].Key, k(4999)) || !bytes.Equal(got[9].Key, k(4990)) {
		t.Fatalf("limit 10 reverse: %d rows, %v", len(got), err)
	}
	n := 0
	for _, err := range mustRange(t, ctx, tr, KeyRange{k(0), k(10000)}, RangeOptions{Limit: 7000, Mode: StreamingModeSmall}).All(ctx) {
		if err != nil {
			t.Fatal(err)
		}
		n++
	}
	if n != 7000 {
		t.Fatalf("limit 7000 small mode: %d rows", n)
	}
	if _, err := sliceRange(ctx, tr, KeyRange{k(0), k(1)}, RangeOptions{Mode: StreamingModeExact}); !isErr(err, ErrExactModeWithoutLimits) {
		t.Fatalf("exact mode without limit: %v", err)
	}
}

func isErr(err error, code Error) bool {
	e, ok := err.(Error)
	return ok && e == code
}

// TestEstimatedRangeSizeBytes checks that a written range has a positive size estimate.
func TestEstimatedRangeSizeBytes(t *testing.T) {
	db := openTestDB(t)
	ctx := testCtx(t)
	p := testPrefix(t, db)
	tr := mustTr(t, db)
	for i := range 10000 {
		tr.Set(key(p, fmt.Sprintf("-%010d", i)), []byte(randStr(10)))
	}
	mustCommit(t, ctx, tr)
	est, err := mustTr(t, db).EstimatedRangeSizeBytes(ctx, prefixRange(t, p))
	if err != nil || est <= 0 {
		t.Fatalf("estimate = %d, %v", est, err)
	}
	t.Logf("estimate %d bytes", est)
}

// TestRangeSplitPoints checks that split points of a written range start
// and end at its bounds.
func TestRangeSplitPoints(t *testing.T) {
	db := openTestDB(t)
	ctx := testCtx(t)
	p := testPrefix(t, db)
	tr := mustTr(t, db)
	for i := range 10000 {
		tr.Set(key(p, fmt.Sprintf("-%010d", i)), []byte(randStr(10)))
	}
	mustCommit(t, ctx, tr)
	r := prefixRange(t, p)
	splits, err := mustTr(t, db).RangeSplitPoints(ctx, r, 100)
	if err != nil || len(splits) == 0 {
		t.Fatalf("splits = %q, %v", splits, err)
	}
	if !bytes.Equal(splits[0], r.Begin) || !bytes.Equal(splits[len(splits)-1], r.End) {
		t.Errorf("splits do not include the bounds: %q", splits)
	}
}

// mappedData holds the subspaces of the mapped range tests: records in
// data, keyed (pk, ...), and an eye-color index (color, pk) in index.
type mappedData struct {
	data, index subspace.Subspace
	mapper      []byte
	prefix      []byte
}

func newMappedData(t *testing.T, db *Database) mappedData {
	root := string(testPrefix(t, db))
	data, err := subspace.Sub(root, "data")
	if err != nil {
		t.Fatal(err)
	}
	index, err := subspace.Sub(root, "index")
	if err != nil {
		t.Fatal(err)
	}
	// Maps (root, "index", color, pk) to the range (root, "data", pk, ...).
	mapper, err := tuple.Tuple{root, "data", "{K[3]}", "{...}"}.Pack()
	if err != nil {
		t.Fatal(err)
	}
	pr, err := tuple.Tuple{root}.Pack()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx := context.Background()
		_ = db.Transact(ctx, func(tr *Transaction) error {
			tr.ClearRange(data)
			tr.ClearRange(index)
			return nil
		})
	})
	return mappedData{data: data, index: index, mapper: mapper, prefix: pr}
}

// setup writes records 0..n-1 in transactions of about 100 kB and returns
// how many are blue.
func (m mappedData) setup(t *testing.T, ctx context.Context, db *Database, n int) int {
	blue, written := 0, 0
	for written < n {
		tr := mustTr(t, db)
		for written < n {
			pk := written
			color := [...]string{"blue", "brown", "green"}[pk%3]
			if color == "blue" {
				blue++
			}
			k1, _ := m.data.Pack(tuple.Tuple{pk, "eye_color", color})
			k2, _ := m.data.Pack(tuple.Tuple{pk, "some_data"})
			v2, _ := tuple.Tuple{"payload"}.Pack()
			k3, _ := m.index.Pack(tuple.Tuple{color, pk})
			tr.Set(k1, []byte(color))
			tr.Set(k2, v2)
			tr.Set(k3, []byte{})
			written++
			size, err := tr.ApproximateSize(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if size >= 100_000 {
				break
			}
		}
		mustCommit(t, ctx, tr)
	}
	return blue
}

// verify checks every mapped row: a blue index key mapped to its two data rows.
func (m mappedData) verify(t *testing.T, rows []MappedKeyValue, blue int) {
	t.Helper()
	if len(rows) != blue {
		t.Fatalf("%d mapped rows, want %d", len(rows), blue)
	}
	for _, row := range rows {
		parent, err := m.index.Unpack(row.Key)
		if err != nil || len(parent) != 2 || parent[0] != "blue" {
			t.Fatalf("parent key %v, %v", parent, err)
		}
		if len(row.Range) != 2 {
			t.Fatalf("%d secondary rows, want 2", len(row.Range))
		}
		for _, kv := range row.Range {
			if !m.data.Contains(kv.Key) {
				t.Fatalf("secondary key %q outside the data subspace", kv.Key)
			}
		}
	}
}

// TestMappedValue checks a mapped range read against reading each secondary
// range on its own.
func TestMappedValue(t *testing.T) {
	db := openTestDB(t)
	ctx := testCtx(t)
	m := newMappedData(t, db)
	blue := m.setup(t, ctx, db, 20)

	blueIndex, err := m.index.Sub("blue")
	if err != nil {
		t.Fatal(err)
	}
	tr := mustTr(t, db)
	rows, err := sliceMappedRange(ctx, tr, blueIndex, m.mapper, RangeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	// Each secondary range read on its own returns the same rows.
	for _, row := range rows {
		child, err := sliceRange(ctx, tr, SelectorRange{row.RangeBegin, row.RangeEnd}, RangeOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if !slices.EqualFunc(child, row.Range, kvEqual) {
			t.Fatalf("child range %d rows, mapped %d", len(child), len(row.Range))
		}
	}
	m.verify(t, rows, blue)
}

// TestMappedValues checks a paginated mapped range.
func TestMappedValues(t *testing.T) {
	db := openTestDB(t)
	ctx := testCtx(t)
	m := newMappedData(t, db)
	blue := m.setup(t, ctx, db, 10_000)

	blueIndex, err := m.index.Sub("blue")
	if err != nil {
		t.Fatal(err)
	}
	tr := mustTr(t, db)
	var rows []MappedKeyValue
	for row, err := range mustMappedRange(t, ctx, tr, blueIndex, m.mapper, RangeOptions{}).All(ctx) {
		if err != nil {
			t.Fatal(err)
		}
		rows = append(rows, row)
	}
	m.verify(t, rows, blue)
	pages, parents, err := countMappedPages(ctx, mustMappedRange(t, ctx, tr, blueIndex, m.mapper, RangeOptions{}))
	if err != nil {
		t.Fatal(err)
	}
	if pages <= 1 || parents != blue {
		t.Fatalf("%d pages, %d parents; want paginated %d", pages, parents, blue)
	}
	if _, err := sliceMappedRange(ctx, tr.Snapshot(), blueIndex, m.mapper, RangeOptions{}); !isErr(err, ErrUnsupportedOperation) {
		t.Fatalf("snapshot mapped range = %v, want unsupported_operation", err)
	}
}
