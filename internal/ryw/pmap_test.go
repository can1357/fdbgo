package ryw

import (
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"sort"
	"testing"

	"github.com/can1357/fdbgo/internal/kv"
)

// sortedKeys returns the model's keys in order.
func sortedKeys(m map[string]int) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// contents lists a version's entries in order.
func contents(t ptree[int]) []string {
	var out []string
	t.ascend(nil, func(n *pnode[int]) bool {
		out = append(out, fmt.Sprintf("%s=%d", n.key, n.val))
		return true
	})
	return out
}

func modelContents(m map[string]int) []string {
	var out []string
	for _, k := range sortedKeys(m) {
		out = append(out, fmt.Sprintf("%s=%d", k, m[k]))
	}
	return out
}

func TestPmapMatchesModelAndSnapshotsStayFixed(t *testing.T) {
	rnd := rand.New(rand.NewPCG(1, 2))
	key := func() []byte { return []byte{byte('a' + rnd.IntN(26)), byte('a' + rnd.IntN(4))} }
	m := newPmap[int](nil)
	model := map[string]int{}
	type snap struct {
		t    ptree[int]
		want []string
	}
	var snaps []snap
	for i := range 5000 {
		switch rnd.IntN(10) {
		case 0, 1, 2, 3, 4:
			k := key()
			m.set(k, i)
			model[string(k)] = i
		case 5, 6:
			k := key()
			m.delete(k)
			delete(model, string(k))
		case 7:
			lo, hi := key(), key()
			if bytes.Compare(lo, hi) > 0 {
				lo, hi = hi, lo
			}
			loExcl, hiIncl := rnd.IntN(2) == 0, rnd.IntN(2) == 0
			if bytes.Equal(lo, hi) && (loExcl || !hiIncl) {
				break
			}
			m.deleteRange(lo, loExcl, hi, hiIncl)
			for k := range model {
				c1, c2 := bytes.Compare([]byte(k), lo), bytes.Compare([]byte(k), hi)
				if (c1 > 0 || (c1 == 0 && !loExcl)) && (c2 < 0 || (c2 == 0 && hiIncl)) {
					delete(model, k)
				}
			}
		default:
			snaps = append(snaps, snap{m.freeze(), modelContents(model)})
		}
		if i%97 == 0 {
			if got, want := contents(m.view()), modelContents(model); fmt.Sprint(got) != fmt.Sprint(want) {
				t.Fatalf("step %d: %v, want %v", i, got, want)
			}
			probe := key()
			checkNeighbours(t, m.view(), model, probe)
		}
	}
	for i, s := range snaps {
		if got := contents(s.t); fmt.Sprint(got) != fmt.Sprint(s.want) {
			t.Fatalf("snapshot %d changed: %v, want %v", i, got, s.want)
		}
	}
}

func checkNeighbours(t *testing.T, tr ptree[int], model map[string]int, probe []byte) {
	t.Helper()
	keys := sortedKeys(model)
	p := string(probe)
	var floor, lower, ceil, higher string
	var hasFloor, hasLower, hasCeil, hasHigher bool
	for _, k := range keys {
		if k <= p {
			floor, hasFloor = k, true
		}
		if k < p {
			lower, hasLower = k, true
		}
		if k >= p && !hasCeil {
			ceil, hasCeil = k, true
		}
		if k > p && !hasHigher {
			higher, hasHigher = k, true
		}
	}
	check := func(name string, n *pnode[int], want string, has bool) {
		if (n != nil) != has || (has && string(n.key) != want) {
			t.Fatalf("%s(%q) wrong", name, probe)
		}
	}
	check("floor", tr.floor(probe), floor, hasFloor)
	check("lower", tr.lower(probe), lower, hasLower)
	check("ceil", tr.ceil(probe), ceil, hasCeil)
	check("higher", tr.higher(probe), higher, hasHigher)
}

func benchKeys(n int) [][]byte {
	keys := make([][]byte, n)
	for i := range keys {
		keys[i] = fmt.Appendf(nil, "key%08d", (i*7919)%n)
	}
	return keys
}

// BenchmarkPmapSet inserts into a map of 1000 keys with no snapshots.
func BenchmarkPmapSet(b *testing.B) {
	keys := benchKeys(1000)
	m := newPmap[int](nil)
	i := 0
	for b.Loop() {
		m.set(keys[i%len(keys)], i)
		i++
	}
}

// BenchmarkPmapSnapshotSet snapshots before every insert (worst case path copying).
func BenchmarkPmapSnapshotSet(b *testing.B) {
	keys := benchKeys(1000)
	m := newPmap[int](nil)
	for i, k := range keys {
		m.set(k, i)
	}
	i := 0
	for b.Loop() {
		m.freeze()
		m.set(keys[i%len(keys)], i)
		i++
	}
}

// BenchmarkPmapFloor looks up keys in a map of 1000 keys.
func BenchmarkPmapFloor(b *testing.B) {
	keys := benchKeys(1000)
	m := newPmap[int](nil)
	for i, k := range keys {
		m.set(k, i)
	}
	t := m.freeze()
	i := 0
	for b.Loop() {
		t.floor(keys[i%len(keys)])
		i++
	}
}

func tenWrites(r *Ryw, keys [][]byte) {
	for _, k := range keys[:10] {
		r.Set(k, k)
	}
}

// BenchmarkSet is a transaction of 10 Sets (ns/op per transaction).
func BenchmarkSet(b *testing.B) {
	keys := benchKeys(10)
	b.ReportAllocs()
	for b.Loop() {
		tenWrites(New(NewRywOptions(1)), keys)
	}
}

// BenchmarkSetCommit includes the commit plan of a 10-write transaction.
func BenchmarkSetCommit(b *testing.B) {
	keys := benchKeys(10)
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		r := New(NewRywOptions(1))
		tenWrites(r, keys)
		if _, err := capture(r.Commit()).Get(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkGetHit reads keys written by a 10-write transaction.
func BenchmarkGetHit(b *testing.B) {
	keys := benchKeys(10)
	db := newFakeDB(nil, false)
	r := New(NewRywOptions(1))
	tenWrites(r, keys)
	ctx := context.Background()
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		if _, err := capture(r.Get(db, keys[i%10], false)).Get(ctx); err != nil {
			b.Fatal(err)
		}
		i++
	}
}

// BenchmarkGetMiss reads keys a 10-write transaction did not write and has
// not read before, from a reader whose replies are ready.
func BenchmarkGetMiss(b *testing.B) {
	keys := benchKeys(10)
	misses := make([][]byte, 1024)
	for i := range misses {
		misses[i] = fmt.Appendf(nil, "miss%06d", i)
	}
	db := newFakeDB([][2]string{{"miss000001", "v"}}, false)
	r := New(NewRywOptions(1))
	tenWrites(r, keys)
	ctx := context.Background()
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		if i == len(misses) {
			b.StopTimer()
			r.Reset()
			tenWrites(r, keys)
			i = 0
			b.StartTimer()
		}
		if _, err := capture(r.Get(db, misses[i], false)).Get(ctx); err != nil {
			b.Fatal(err)
		}
		i++
	}
}

// BenchmarkGetRangeCached reads a 10-row range that is already cached, over 10 writes.
func BenchmarkGetRangeCached(b *testing.B) {
	keys := benchKeys(10)
	pairs := make([][2]string, 10)
	for i := range pairs {
		pairs[i] = [2]string{fmt.Sprintf("row%02d", i), "v"}
	}
	db := newFakeDB(pairs, false)
	r := New(NewRywOptions(1))
	tenWrites(r, keys)
	ctx := context.Background()
	begin, end := kv.FirstGreaterOrEqual([]byte("row")), kv.FirstGreaterOrEqual([]byte("rox"))
	if _, err := capture(r.GetRange(db, begin, end, kv.DefaultLimits(), false, false)).Get(ctx); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := capture(r.GetRange(db, begin, end, kv.DefaultLimits(), false, false)).Get(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

// manualReader blocks until the benchmark supplies a reply.
type manualReader struct {
	fakeDB
	pending chan []byte
}

func (m *manualReader) Get(ctx context.Context, _ []byte) ([]byte, error) {
	select {
	case v := <-m.pending:
		return v, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// BenchmarkGetMissAsync is BenchmarkGetMiss with replies that arrive after
// the call: a blocking wait receives the reply.
func BenchmarkGetMissAsync(b *testing.B) {
	keys := benchKeys(10)
	misses := make([][]byte, 1024)
	for i := range misses {
		misses[i] = fmt.Appendf(nil, "miss%06d", i)
	}
	db := &manualReader{pending: make(chan []byte, 1)}
	r := New(NewRywOptions(1))
	tenWrites(r, keys)
	ctx := context.Background()
	value := []byte("v")
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		if i == len(misses) {
			b.StopTimer()
			r.Reset()
			tenWrites(r, keys)
			i = 0
			b.StartTimer()
		}
		f := capture(r.Get(db, misses[i], false))
		db.pending <- value
		if _, err := f.Get(ctx); err != nil {
			b.Fatal(err)
		}
		i++
	}
}
