package ryw

import (
	"bytes"
	"context"
	"runtime"
	"sort"
	"sync"
	"testing"

	"github.com/can1357/fdbgo/internal/kv"
)

// fakeDB is a snapshot of the database backed by sorted rows. In async mode
// replies come from another goroutine, like network replies; with a gate
// they wait until the gate is closed.
type fakeDB struct {
	rows  []kv.KeyValue
	async bool
	gate  chan struct{}
}

func newFakeDB(pairs [][2]string, async bool) *fakeDB {
	m := map[string][]byte{}
	for _, p := range pairs {
		m[p[0]] = []byte(p[1])
	}
	return fakeFromMap(m, async)
}

func fakeFromMap(m map[string][]byte, async bool) *fakeDB {
	db := &fakeDB{async: async}
	for k, v := range m {
		db.rows = append(db.rows, kv.KeyValue{Key: []byte(k), Value: v})
	}
	sort.Slice(db.rows, func(i, j int) bool { return bytes.Compare(db.rows[i].Key, db.rows[j].Key) < 0 })
	return db
}

// gated returns a copy of db whose replies wait until release is called
// (at test end at the latest).
func (db *fakeDB) gated(t *testing.T) (*fakeDB, func()) {
	g := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(g) }) }
	t.Cleanup(release)
	return &fakeDB{rows: db.rows, async: true, gate: g}, release
}

func reply[T any](ctx context.Context, db *fakeDB, v T) (T, error) {
	if db.gate != nil {
		select {
		case <-db.gate:
		case <-ctx.Done():
			return *new(T), ctx.Err()
		}
	} else if db.async {
		runtime.Gosched()
	}
	return v, nil
}

func (db *fakeDB) StartReadVersion() {}

func keysOf(rows []kv.KeyValue) [][]byte {
	out := make([][]byte, len(rows))
	for i := range rows {
		out[i] = rows[i].Key
	}
	return out
}

// resolveIn resolves sel over sorted keys; off the end resolves to end.
func resolveIn(keys [][]byte, sel kv.KeySelector, end []byte) []byte {
	n := 0
	for _, k := range keys {
		if c := bytes.Compare(k, sel.Key); c < 0 || (sel.OrEqual && c == 0) {
			n++
		}
	}
	idx := n - 1 + sel.Offset
	switch {
	case idx < 0:
		return []byte{}
	case idx >= len(keys):
		return end
	}
	return keys[idx]
}

func (db *fakeDB) Get(ctx context.Context, key []byte) ([]byte, error) {
	for _, r := range db.rows {
		if bytes.Equal(r.Key, key) {
			return reply(ctx, db, r.Value)
		}
	}
	return reply[[]byte](ctx, db, nil)
}

func (db *fakeDB) GetKey(ctx context.Context, sel kv.KeySelector) ([]byte, error) {
	return reply(ctx, db, resolveIn(keysOf(db.rows), sel, kv.SpecialKeysBegin))
}

func (db *fakeDB) GetRange(ctx context.Context, begin, end kv.KeySelector, limits kv.Limits, reverse bool) (kv.RangeResult, error) {
	// Like a storage server, resolve takes first-greater selectors at face value.
	resolve := func(s kv.KeySelector) []byte {
		switch {
		case s.IsFirstGreaterOrEqual():
			return s.Key
		case s.IsFirstGreaterThan():
			return kv.KeyAfter(s.Key)
		}
		return resolveIn(keysOf(db.rows), s, kv.SpecialKeysBegin)
	}
	b, e := resolve(begin), resolve(end)
	var result kv.RangeResult
	if bytes.Compare(b, e) < 0 {
		var rows []kv.KeyValue
		for _, r := range db.rows {
			if bytes.Compare(r.Key, b) >= 0 && bytes.Compare(r.Key, e) < 0 {
				rows = append(rows, r)
			}
		}
		for i := range rows {
			r := rows[i]
			if reverse {
				r = rows[len(rows)-1-i]
			}
			if limits.IsReached() {
				break
			}
			limits.DecrementOne(&r)
			result.Data = append(result.Data, r)
		}
		result.More = len(result.Data) < len(rows)
		result.ReadToBegin = len(b) == 0 && (!reverse || !result.More)
		result.ReadThroughEnd = bytes.Equal(e, kv.SpecialKeysBegin) && (reverse || !result.More)
	}
	return reply(ctx, db, result)
}

// RegisterWatch arms a watch that never fires on its own.
func (db *fakeDB) RegisterWatch(key, value []byte) (<-chan error, func(error), error) {
	ch := make(chan error, 1)
	var once sync.Once
	return ch, func(err error) { once.Do(func() { ch <- err }) }, nil
}

func newRyw() *Ryw { return New(NewRywOptions(1)) }

func fgoe(s string) kv.KeySelector { return kv.FirstGreaterOrEqual([]byte(s)) }

// testResult starts every pending wait immediately, even when discarded.
type testResult[T any] struct {
	done  chan struct{}
	value T
	err   error
}

func capture[T any](value T, wait func(context.Context) (T, error), err error) *testResult[T] {
	f := &testResult[T]{done: make(chan struct{}), value: value, err: err}
	if wait == nil {
		close(f.done)
	} else {
		go func() { f.value, f.err = wait(context.Background()); close(f.done) }()
	}
	return f
}
func (f *testResult[T]) IsReady() bool {
	select {
	case <-f.done:
		return true
	default:
		return false
	}
}
func (f *testResult[T]) Get(ctx context.Context) (T, error) {
	select {
	case <-f.done:
		return f.value, f.err
	case <-ctx.Done():
		return *new(T), ctx.Err()
	}
}
func await_[T any](t *testing.T, f *testResult[T]) (T, error) {
	t.Helper()
	return f.Get(context.Background())
}
func must[T any](t *testing.T, f *testResult[T]) T {
	t.Helper()
	v, err := f.Get(context.Background())
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	return v
}

type pair struct{ b, e string }

// readConflicts returns all read conflict ranges.
func readConflicts(r *Ryw) []pair {
	end := append(append([]byte{}, ReadConflictRangePrefix...), kv.SpecialKeysBegin...)
	return pairsFromMarks(r.ReadConflictRanges(ReadConflictRangePrefix, end), ReadConflictRangePrefix)
}

// pairsFromMarks turns "1"/"0" boundary marks into ranges.
func pairsFromMarks(marks []kv.KeyValue, prefix []byte) []pair {
	var out []pair
	var open *string
	for _, m := range marks {
		k := string(m.Key[len(prefix):])
		switch {
		case string(m.Value) == "1" && open == nil:
			open = &k
		case string(m.Value) == "0" && open != nil:
			out = append(out, pair{*open, k})
			open = nil
		}
	}
	return out
}

func rangesOf(krs []kv.KeyRange) []pair {
	var out []pair
	for _, kr := range krs {
		out = append(out, pair{string(kr.Begin), string(kr.End)})
	}
	return out
}

func eqPairs(t *testing.T, got []pair, want ...pair) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %q, want %q", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("got %q, want %q", got, want)
		}
	}
}

func eqKeys(t *testing.T, rows []kv.KeyValue, want ...string) {
	t.Helper()
	var got []string
	for _, r := range rows {
		got = append(got, string(r.Key))
	}
	if len(got) != len(want) {
		t.Fatalf("keys %q, want %q", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("keys %q, want %q", got, want)
		}
	}
}

func eqErr(t *testing.T, got, want error) {
	t.Helper()
	if got != want {
		t.Fatalf("error %v, want %v", got, want)
	}
}

func eqVal(t *testing.T, got []byte, want string, present bool) {
	t.Helper()
	if (got != nil) != present || string(got) != want {
		if present {
			t.Fatalf("value %q (present %v), want %q", got, got != nil, want)
		}
		t.Fatalf("value %q, want absent", got)
	}
}
