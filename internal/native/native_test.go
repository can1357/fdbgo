package native

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"slices"
	"sort"
	"testing"

	"github.com/can1357/fdbgo/internal/fdberr"
	"github.com/can1357/fdbgo/internal/kv"
	"github.com/can1357/fdbgo/internal/wire"
)

func key(n byte) []byte { return []byte{n} }

// TestIntersectsMatchesPairwiseCheck checks that
// the sweep agrees with checking every pair of non-empty ranges, on unsorted
// lists with overlapping and empty ranges.
func TestIntersectsMatchesPairwiseCheck(t *testing.T) {
	seed := uint64(7)
	ranges := func(n int) []kv.KeyRange {
		out := make([]kv.KeyRange, n)
		for i := range out {
			seed = seed*6364136223846793005 + 1442695040888963407
			b := byte(seed>>33) % 40
			l := byte(seed>>50) % 6
			out[i] = kv.KeyRange{Begin: key(b), End: key(b + l)}
		}
		return out
	}
	nonEmpty := func(r kv.KeyRange) bool { return bytes.Compare(r.Begin, r.End) < 0 }
	pairwise := func(a, b []kv.KeyRange) bool {
		for _, x := range a {
			for _, y := range b {
				if nonEmpty(x) && nonEmpty(y) && bytes.Compare(x.Begin, y.End) < 0 && bytes.Compare(y.Begin, x.End) < 0 {
					return true
				}
			}
		}
		return false
	}
	inside := func(v []kv.KeyRange, k []byte) bool {
		for _, r := range v {
			if bytes.Compare(r.Begin, k) <= 0 && bytes.Compare(k, r.End) < 0 {
				return true
			}
		}
		return false
	}
	for range 2000 {
		a, b := ranges(6), ranges(6)
		k, found := intersects(a, b)
		if found != pairwise(a, b) {
			t.Fatalf("%v vs %v: found=%v", a, b, found)
		}
		if found && !(inside(a, k) && inside(b, k)) {
			t.Fatalf("%q not in both: %v vs %v", k, a, b)
		}
	}
}

func loc(b, e string) Location {
	return Location{Begin: []byte(b), End: []byte(e), Servers: []wire.StorageServer{{ID: wire.UID{First: uint64(len(b))}}}}
}

func TestLocationCacheGetInsertInvalidate(t *testing.T) {
	c := newLocationCache()
	c.Insert(loc("", "c"))
	c.Insert(loc("c", "m"))
	c.Insert(loc("m", "\xff\xff"))
	check := func(k string, reverse bool, wantBegin string, ok bool) {
		t.Helper()
		l, found := c.Get([]byte(k), reverse)
		if found != ok || (ok && string(l.Begin) != wantBegin) {
			t.Fatalf("Get(%q, %v) = %q, %v; want %q, %v", k, reverse, l.Begin, found, wantBegin, ok)
		}
	}
	check("", false, "", true)
	check("b", false, "", true)
	check("c", false, "c", true)
	check("c", true, "", true)
	check("m", true, "c", true)
	check("z", false, "m", true)
	check("", true, "", false)

	// Overlapping insert trims neighbours, keeping head and tail.
	c.Insert(loc("e", "g"))
	check("d", false, "c", true)
	check("e", false, "e", true)
	check("h", false, "g", true)
	if l, _ := c.Get([]byte("d"), false); string(l.End) != "e" {
		t.Fatalf("head not trimmed: %q", l.End)
	}
	if l, _ := c.Get([]byte("h"), false); string(l.End) != "m" {
		t.Fatalf("tail lost: %q", l.End)
	}
	// An insert covering several entries replaces them.
	c.Insert(loc("b", "n"))
	if c.Len() != 3 {
		t.Fatalf("len %d", c.Len())
	}
	check("a", false, "", true)
	check("m", false, "b", true)
	check("n", false, "n", true)

	c.Invalidate([]byte("f"), false)
	check("f", false, "", false)
	check("a", false, "", true)
	c.InvalidateRange([]byte("a"), []byte("o"))
	if c.Len() != 0 {
		t.Fatalf("len after InvalidateRange %d", c.Len())
	}
}

func TestLocationCacheCapacity(t *testing.T) {
	c := newLocationCache()
	c.SetCapacity(4)
	for i := range 20 {
		k := fmt.Sprintf("%02d", i)
		c.Insert(loc(k, fmt.Sprintf("%02d", i+1)))
		if _, ok := c.Get([]byte(k), false); !ok {
			t.Fatalf("just inserted %s evicted", k)
		}
	}
	if c.Len() != 4 {
		t.Fatalf("len %d, want 4", c.Len())
	}
}

func TestLocationCacheDescendingBinaryCapacity(t *testing.T) {
	for _, capacity := range []int{1, 4, 17} {
		c := newLocationCache()
		c.SetCapacity(capacity)
		for i := 255; i >= 0; i-- {
			// Every random four-byte key-space probe falls after these keys,
			// selecting entry zero: the just-inserted descending key.
			begin := []byte{0, 0, 0, 0, byte(i)}
			end := append(append([]byte(nil), begin...), 0)
			c.Insert(Location{Begin: begin, End: end})
			if c.Len() > capacity {
				t.Fatalf("capacity %d: %d entries after inserting %x", capacity, c.Len(), begin)
			}
			if got, ok := c.Get(begin, false); !ok || !bytes.Equal(got.Begin, begin) || !bytes.Equal(got.End, end) {
				t.Fatalf("new binary range missing or changed: %+v, %v", got, ok)
			}
			for j, entry := range c.entries {
				if bytes.Compare(entry.begin, entry.end) >= 0 ||
					(j > 0 && bytes.Compare(c.entries[j-1].end, entry.begin) > 0) {
					t.Fatalf("invalid cached ranges: %+v", c.entries)
				}
				if got, ok := c.Get(entry.end, true); !ok || !bytes.Equal(got.Begin, entry.begin) {
					t.Fatalf("reverse lookup lost retained range: %+v, %v", got, ok)
				}
			}
		}
		// Shrinking by more than one entry and splitting an old range must
		// still retain the newly inserted range and meet the budget.
		c.SetCapacity(1)
		c.Insert(loc("a", "z"))
		c.Insert(loc("c", "d"))
		if c.Len() != 1 {
			t.Fatalf("split exceeds reduced capacity: %d", c.Len())
		}
		if got, ok := c.Get([]byte("c"), false); !ok || string(got.End) != "d" {
			t.Fatalf("split range was evicted: %+v, %v", got, ok)
		}
	}
}

func TestCachedLocations(t *testing.T) {
	c := newLocationCache()
	c.Insert(loc("", "c"))
	c.Insert(loc("c", "m"))
	c.Insert(loc("m", "t"))
	locs, ok := cachedLocations(c, []byte("b"), []byte("p"), 10, false)
	if !ok || len(locs) != 3 || string(locs[0].Begin) != "b" || string(locs[2].End) != "p" {
		t.Fatalf("forward: %v %v", locs, ok)
	}
	locs, ok = cachedLocations(c, []byte("b"), []byte("p"), 2, false)
	if !ok || len(locs) != 2 || string(locs[1].End) != "m" {
		t.Fatalf("limit: %v", locs)
	}
	locs, ok = cachedLocations(c, []byte("b"), []byte("p"), 10, true)
	if !ok || len(locs) != 3 || string(locs[0].Begin) != "m" || string(locs[0].End) != "p" || string(locs[2].Begin) != "b" {
		t.Fatalf("reverse: %v", locs)
	}
	if _, ok := cachedLocations(c, []byte("b"), []byte("z"), 10, false); ok {
		t.Fatal("gap reported as cached")
	}
}

func TestShrinkPrefersLastChoice(t *testing.T) {
	eps := make([]wire.Endpoint, 6)
	for i := range eps {
		eps[i].Token = wire.UID{Second: uint64(i)}
	}
	kept, last := shrink(slices.Clone(eps), nil, 3)
	if len(kept) != 3 || len(last) != 3 {
		t.Fatalf("kept %d", len(kept))
	}
	again, _ := shrink(slices.Clone(eps), last, 3)
	for _, e := range again {
		if !slices.Contains(last, e.Token) {
			t.Fatalf("not sticky: %v vs %v", again, last)
		}
	}
	small, toks := shrink(eps[:2], last, 3)
	if len(small) != 2 || len(toks) != 2 {
		t.Fatal("under the limit must keep all")
	}
}

func TestTransformRangeLimits(t *testing.T) {
	cases := []struct {
		l       kv.Limits
		reverse bool
		rows    int
		bytes   int
	}{
		{kv.DefaultLimits(), false, replyByteLimit, replyByteLimit},
		{kv.NewLimits(10, kv.Unlimited), true, -10, replyByteLimit},
		{kv.NewLimits(kv.Unlimited, 100), false, replyByteLimit, 100},
		{kv.Limits{Rows: 50, MinRows: 3, Bytes: 0}, false, 3, replyByteLimit},
		{kv.Limits{Rows: 50, MinRows: 3, Bytes: 0}, true, -3, replyByteLimit},
	}
	for _, c := range cases {
		r, b := transformRangeLimits(c.l, c.reverse)
		if r != c.rows || b != c.bytes {
			t.Errorf("%+v rev=%v: got %d,%d want %d,%d", c.l, c.reverse, r, b, c.rows, c.bytes)
		}
	}
}

func TestNormalizeRange(t *testing.T) {
	_, _, ok, err := normalizeRange(kv.FirstGreaterOrEqual(key('b')), kv.FirstGreaterOrEqual(key('a')), kv.DefaultLimits())
	if ok || err != nil {
		t.Fatal("inverted range must be empty")
	}
	_, _, _, err = normalizeRange(kv.FirstGreaterOrEqual(key('a')), kv.FirstGreaterOrEqual(key('b')), kv.Limits{Rows: -5, MinRows: 1, Bytes: kv.Unlimited})
	if err != fdberr.RangeLimitsInvalid {
		t.Fatalf("err %v", err)
	}
	b, e, ok, _ := normalizeRange(kv.FirstGreaterThan(key('a')), kv.LastLessOrEqual(key('z')), kv.DefaultLimits())
	if !ok || b.OrEqual || e.OrEqual || !bytes.Equal(b.Key, []byte("a\x00")) || !bytes.Equal(e.Key, []byte("z\x00")) {
		t.Fatalf("%v %v %v", b, e, ok)
	}
	if _, _, ok, _ := normalizeRange(kv.FirstGreaterOrEqual(key('a')), kv.FirstGreaterOrEqual(key('b')), kv.NewLimits(0, kv.Unlimited)); ok {
		t.Fatal("reached limits must be empty")
	}
}

func TestGetKeyConflict(t *testing.T) {
	r := GetKeyConflict(kv.LastLessOrEqual([]byte("m")), []byte("k"))
	if string(r.Begin) != "k" || string(r.End) != "m\x00" {
		t.Fatalf("%q", r)
	}
	r = GetKeyConflict(kv.FirstGreaterThan([]byte("m")).Add(1), []byte("p"))
	if string(r.Begin) != "m\x00" || string(r.End) != "p\x00" {
		t.Fatalf("%q", r)
	}
}

func TestGetRangeConflict(t *testing.T) {
	kvk := func(r *kv.KeyValue) []byte { return r.Key }
	out := &RangeOut[kv.KeyValue]{Data: []kv.KeyValue{{Key: []byte("b")}, {Key: []byte("d")}}, More: true}
	b, e := kv.FirstGreaterOrEqual([]byte("a")), kv.FirstGreaterOrEqual([]byte("z"))
	r := GetRangeConflict(b, e, out, kvk, false)
	if string(r.Begin) != "a" || string(r.End) != "d\x00" {
		t.Fatalf("forward more: %q", r)
	}
	r = GetRangeConflict(b, e, out, kvk, true)
	if string(r.Begin) != "d" || string(r.End) != "z" {
		t.Fatalf("reverse more: %q", r)
	}
	out.More = false
	r = GetRangeConflict(b, e, out, kvk, false)
	if string(r.Begin) != "a" || string(r.End) != "z" {
		t.Fatalf("exhausted: %q", r)
	}
	out.ReadToBegin, out.ReadThroughEnd = true, true
	r = GetRangeConflict(b, e, out, kvk, false)
	if len(r.Begin) != 0 || !bytes.Equal(r.End, kv.SpecialKeysBegin) {
		t.Fatalf("whole keyspace: %q", r)
	}
}

func TestIdempotencyKVContains(t *testing.T) {
	id := []byte("0123456789abcdef")
	k := binary.BigEndian.AppendUint64(slices.Clone(idempotencyKeysBegin), 12345)
	k = append(k, 0x02)
	v := make([]byte, 16)
	v = append(v, 3, 'x', 'y', 'z', 9)
	v = append(v, byte(len(id)))
	v = append(v, id...)
	v = append(v, 0x07)
	row := kv.KeyValue{Key: k, Value: v}
	ver, batch, ok := idempotencyKVContains(&row, id)
	if !ok || ver != 12345 || batch != 0x0207 {
		t.Fatalf("got %d %x %v", ver, batch, ok)
	}
	if _, _, ok := idempotencyKVContains(&row, []byte("nope")); ok {
		t.Fatal("found absent id")
	}
	row.Value = row.Value[:len(row.Value)-1]
	if _, _, ok := idempotencyKVContains(&row, id); ok {
		t.Fatal("truncated value accepted")
	}
}

func TestMetadataVersionCache(t *testing.T) {
	c := &DbContext{}
	for v := int64(1); v <= metadataVersionCacheSize+10; v++ {
		c.RecordMetadataVersion(v*10, []byte{byte(v)})
	}
	c.RecordMetadataVersion(5, []byte("old")) // older than the newest: ignored
	if _, ok := c.CachedMetadataVersion(100); ok {
		t.Fatal("evicted entry still cached")
	}
	mv, ok := c.CachedMetadataVersion((metadataVersionCacheSize + 10) * 10)
	if !ok || mv[0] != byte((metadataVersionCacheSize+10)%256) {
		t.Fatalf("newest: %v %v", mv, ok)
	}
	if _, ok := c.CachedMetadataVersion(205); ok {
		t.Fatal("inexact version matched")
	}
	c.RecordMetadataVersion(1_000_000, nil)
	if mv, ok := c.CachedMetadataVersion(1_000_000); !ok || mv != nil {
		t.Fatal("absent metadata version must be cached as nil")
	}
}

func TestTagThrottles(t *testing.T) {
	var tt TagThrottles
	tags := [][]byte{[]byte("hot")}
	if err := tt.CheckBeforeGRV(PriorityDefault, tags); err != nil {
		t.Fatal(err)
	}
	var b wire.Builder
	b.Reset()
	tag := b.Bytes([]byte("hot"))
	b.StartTable(2)
	b.AddF64(1, 60)
	limits := b.EndTable()
	b.StartTable(2)
	b.AddOffset(0, tag)
	b.AddOffset(1, limits)
	pair := b.EndTable()
	m := b.Mark()
	b.Push(pair)
	throttles := b.Offsets(m)
	b.StartTable(9)
	b.AddOffset(5, throttles)
	reply, err := wire.DecodeReply[wire.ReadVersionReply](wire.GetReadVersion{}, lifecycleReply(&b, b.EndTable(), (wire.GetReadVersion{}).ReplyFileID()))
	if err != nil {
		t.Fatal(err)
	}
	tt.update(PriorityDefault, []wire.TagCount{{Tag: []byte("hot"), Count: 1}}, &reply)
	if err := tt.CheckAfterGRV(PriorityDefault, tags); err != fdberr.TagThrottled {
		t.Fatalf("after: %v", err)
	}
	if err := tt.CheckBeforeGRV(PriorityDefault, tags); err != fdberr.TagThrottled {
		t.Fatalf("before: %v", err)
	}
	if b := tt.Backoff(PriorityDefault, tags, 0.01); b != TagThrottleRecheckInterval {
		t.Fatalf("backoff %v", b)
	}
	if err := tt.CheckBeforeGRV(PriorityBatch, tags); err != nil {
		t.Fatal("other priority throttled")
	}
	// A report without the tag lifts the throttle.
	tt.update(PriorityDefault, []wire.TagCount{{Tag: []byte("hot"), Count: 1}}, &wire.ReadVersionReply{})
	if err := tt.CheckAfterGRV(PriorityDefault, tags); err != nil {
		t.Fatal(err)
	}
}

func TestDatabaseStatus(t *testing.T) {
	client := map[string]any{
		"coordinators": map[string]any{"quorum_reachable": true},
		"cluster_file": map[string]any{"up_to_date": true},
	}
	cluster := map[string]any{
		"recovery_state":     map[string]any{"name": "fully_recovered"},
		"database_available": true,
		"processes":          map[string]any{"p": map[string]any{"messages": []any{}}},
		"data":               map[string]any{"state": map[string]any{"healthy": true}},
	}
	got := databaseStatus(client, cluster)
	if got["available"] != true || got["healthy"] != true {
		t.Fatalf("%v", got)
	}
	cluster["messages"] = []any{"x"}
	if got := databaseStatus(client, cluster); got["healthy"] != false || got["available"] != true {
		t.Fatalf("%v", got)
	}
	if got := databaseStatus(client, map[string]any{}); got["available"] != false {
		t.Fatalf("%v", got)
	}
}

func TestParseClusterStatusCapsFaultTolerance(t *testing.T) {
	doc := `{"fault_tolerance":{"max_zone_failures_without_losing_data":2,"max_zone_failures_without_losing_availability":1},"big":12345678901234567890}`
	v := parseClusterStatus([]byte(doc), 0).(map[string]any)
	ft := v["fault_tolerance"].(map[string]any)
	if ft["max_zone_failures_without_losing_data"] != int64(0) || ft["max_zone_failures_without_losing_availability"] != int64(0) {
		t.Fatalf("%v", ft)
	}
	if fmt.Sprint(v["big"]) != "12345678901234567890" {
		t.Fatalf("number precision lost: %v", v["big"])
	}
}

// fakeShards is an in-memory sharded store standing in for the cluster in
// range loop tests. Storage requests must stay inside the shard they are
// sent to, like a storage server requires.
type fakeShards struct {
	t      *testing.T
	rows   []kv.KeyValue
	bounds []string // shard i is [bounds[i], bounds[i+1]); last bound is \xff\xff
	// stale, when set, is the shard set locate reports until it is invalidated.
	stale       []string
	fetches     int
	invalidated int
}

func newFakeShards(t *testing.T, bounds []string, keys ...string) *fakeShards {
	f := &fakeShards{t: t, bounds: bounds}
	for _, k := range keys {
		f.rows = append(f.rows, kv.KeyValue{Key: []byte(k), Value: []byte("v" + k)})
	}
	sort.Slice(f.rows, func(i, j int) bool { return bytes.Compare(f.rows[i].Key, f.rows[j].Key) < 0 })
	return f
}

func (f *fakeShards) readVersion(context.Context) (int64, error) { return 1, nil }

func shardOf(bounds []string, k []byte, reverse bool) int {
	for i := len(bounds) - 2; i >= 0; i-- {
		c := bytes.Compare([]byte(bounds[i]), k)
		if c < 0 || (c == 0 && !reverse) {
			return i
		}
	}
	return 0
}

func (f *fakeShards) curBounds() []string {
	if f.stale != nil {
		return f.stale
	}
	return f.bounds
}

func (f *fakeShards) locate(_ context.Context, k []byte, reverse bool, _ uint32) (Location, error) {
	b := f.curBounds()
	i := shardOf(b, k, reverse)
	return Location{Begin: []byte(b[i]), End: []byte(b[i+1])}, nil
}

func (f *fakeShards) rangeLocations(_ context.Context, begin, end []byte, limit int, reverse bool, _ uint32) ([]Location, error) {
	b := f.curBounds()
	var out []Location
	for i := 0; i+1 < len(b); i++ {
		l := Location{Begin: []byte(b[i]), End: []byte(b[i+1])}
		if bytes.Compare(l.End, begin) > 0 && bytes.Compare(l.Begin, end) < 0 {
			out = append(out, clipLocation(l, begin, end))
		}
	}
	if reverse {
		slices.Reverse(out)
	}
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (f *fakeShards) invalidate([]byte, bool) { f.invalidated++; f.stale = nil }

func (f *fakeShards) invalidateRange([]byte, []byte) { f.invalidated++; f.stale = nil }

// resolveKey resolves sel over the whole store.
func (f *fakeShards) resolveKey(_ context.Context, sel kv.KeySelector) ([]byte, error) {
	// Index of the last row < key (or <= key with OrEqual), then move Offset.
	i := sort.Search(len(f.rows), func(i int) bool {
		c := bytes.Compare(f.rows[i].Key, sel.Key)
		return c > 0 || (c == 0 && !sel.OrEqual)
	}) - 1
	i += sel.Offset
	switch {
	case i < 0:
		return []byte{}, nil
	case i >= len(f.rows):
		return slices.Clone(kv.SpecialKeysBegin), nil
	}
	return f.rows[i].Key, nil
}

func (f *fakeShards) family() family[kv.KeyValue] {
	return family[kv.KeyValue]{
		key:       func(r *kv.KeyValue) []byte { return r.Key },
		decrement: func(l *kv.Limits, rows []kv.KeyValue) { l.Decrement(rows) },
		fetch:     f.fetch,
	}
}

func (f *fakeShards) fetch(ctx context.Context, loc Location, begin, end kv.KeySelector, limit, limitBytes int, _ int64) ([]kv.KeyValue, bool, error) {
	f.fetches++
	if f.fetches > 1000 {
		f.t.Fatal("range loop does not terminate")
	}
	// A request to a stale shard is refused.
	if i := shardOf(f.bounds, loc.Begin, false); bytes.Compare(loc.End, []byte(f.bounds[i+1])) > 0 {
		return nil, false, fdberr.WrongShardServer
	}
	b, _ := f.resolveKey(ctx, begin)
	e, _ := f.resolveKey(ctx, end)
	if begin.Offset != 1 || end.Offset != 1 {
		f.t.Fatalf("fake storage only resolves offset-1 selectors: %v %v", begin, end)
	}
	b = maxKey(b, loc.Begin)
	e = minKey(e, loc.End)
	var in []kv.KeyValue
	for _, r := range f.rows {
		if bytes.Compare(r.Key, b) >= 0 && bytes.Compare(r.Key, e) < 0 {
			in = append(in, r)
		}
	}
	if limit < 0 {
		slices.Reverse(in)
		limit = -limit
	}
	var out []kv.KeyValue
	used := 0
	for _, r := range in {
		if len(out) >= limit || used >= limitBytes {
			break
		}
		out = append(out, r)
		used += kv.RowBytes(&r)
	}
	return out, len(out) < len(in), nil
}

func maxKey(a, b []byte) []byte {
	if bytes.Compare(a, b) > 0 {
		return a
	}
	return b
}

func minKey(a, b []byte) []byte {
	if bytes.Compare(a, b) < 0 {
		return a
	}
	return b
}

func keysOf(rows []kv.KeyValue) []string {
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = string(r.Key)
	}
	return out
}

func TestRangeReadAcrossShards(t *testing.T) {
	bounds := []string{"", "c", "f", "k", "\xff\xff"}
	keys := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "x", "y"}
	all := slices.Clone(keys)
	rev := slices.Clone(keys)
	slices.Reverse(rev)
	cases := []struct {
		name       string
		b, e       string
		limits     kv.Limits
		reverse    bool
		want       []string
		wantMore   bool
		staleShard bool
	}{
		{"all forward", "", "\xff", kv.DefaultLimits(), false, all, false, false},
		{"all reverse", "", "\xff", kv.DefaultLimits(), true, rev, false, false},
		{"row limit forward", "b", "z", kv.NewLimits(5, kv.Unlimited), false, []string{"b", "c", "d", "e", "f"}, true, false},
		{"row limit reverse", "b", "z", kv.NewLimits(4, kv.Unlimited), true, []string{"y", "x", "j", "i"}, true, false},
		{"inner range", "d", "h", kv.DefaultLimits(), false, []string{"d", "e", "f", "g"}, false, false},
		{"exact limit end", "a", "c", kv.NewLimits(2, kv.Unlimited), false, []string{"a", "b"}, true, false},
		{"stale location", "a", "\xff", kv.DefaultLimits(), false, all, false, true},
		{"stale location reverse", "a", "\xff", kv.DefaultLimits(), true, rev, false, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newFakeShards(t, bounds, keys...)
			if c.staleShard {
				f.stale = []string{"", "z", "\xff\xff"}
			}
			out, err := rangeRead(context.Background(), f, f.family(), kv.FirstGreaterOrEqual([]byte(c.b)), kv.FirstGreaterOrEqual([]byte(c.e)), c.limits, c.reverse)
			if err != nil {
				t.Fatal(err)
			}
			if got := keysOf(out.Data); !slices.Equal(got, c.want) {
				t.Fatalf("got %q want %q", got, c.want)
			}
			if c.limits.Rows != kv.Unlimited && out.More != c.wantMore {
				t.Fatalf("more %v want %v", out.More, c.wantMore)
			}
			if c.staleShard && f.invalidated == 0 {
				t.Fatal("stale location not invalidated")
			}
		})
	}
}

func TestRangeReadByteLimitAndReadThrough(t *testing.T) {
	bounds := []string{"", "c", "f", "\xff\xff"}
	f := newFakeShards(t, bounds, "a", "b", "c", "d", "e", "g")
	// Each row costs 8 + 1 + 2 = 11 bytes: a byte limit of 12 stops after two rows.
	out, err := rangeRead(context.Background(), f, f.family(), kv.FirstGreaterOrEqual([]byte("a")), kv.FirstGreaterOrEqual([]byte("z")), kv.NewLimits(kv.Unlimited, 12), false)
	if err != nil {
		t.Fatal(err)
	}
	if got := keysOf(out.Data); !slices.Equal(got, []string{"a", "b"}) || !out.More {
		t.Fatalf("got %q more=%v", got, out.More)
	}
	if !out.HasReadThrough || string(out.ReadThrough) != "c" {
		t.Fatalf("read through %q %v", out.ReadThrough, out.HasReadThrough)
	}
	// From the beginning of the keyspace the result reports ReadToBegin.
	out, err = rangeRead(context.Background(), f, f.family(), kv.LastLessThan(nil), kv.FirstGreaterOrEqual([]byte("b")), kv.DefaultLimits(), false)
	if err != nil || !out.ReadToBegin || !slices.Equal(keysOf(out.Data), []string{"a"}) {
		t.Fatalf("%v %v", out, err)
	}
}

func TestExactRangeWalksShardBatches(t *testing.T) {
	bounds := []string{"", "b", "d", "f", "h", "\xff\xff"}
	keys := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i"}
	f := newFakeShards(t, bounds, keys...)
	out, err := exactRange(context.Background(), f, f.family(), []byte("a"), []byte("z"), kv.DefaultLimits(), false)
	if err != nil || !slices.Equal(keysOf(out.Data), keys) || out.More {
		t.Fatalf("forward %q more=%v err=%v", keysOf(out.Data), out.More, err)
	}
	out, err = exactRange(context.Background(), f, f.family(), []byte("a"), []byte("z"), kv.DefaultLimits(), true)
	want := slices.Clone(keys)
	slices.Reverse(want)
	if err != nil || !slices.Equal(keysOf(out.Data), want) || out.More {
		t.Fatalf("reverse %q more=%v err=%v", keysOf(out.Data), out.More, err)
	}
	out, err = exactRange(context.Background(), f, f.family(), []byte("a"), []byte("z"), kv.NewLimits(3, kv.Unlimited), false)
	if err != nil || !slices.Equal(keysOf(out.Data), keys[:3]) || !out.More {
		t.Fatalf("limited %q more=%v err=%v", keysOf(out.Data), out.More, err)
	}
}

func TestRangeFallbackResolvesSelectors(t *testing.T) {
	bounds := []string{"", "m", "\xff\xff"}
	f := newFakeShards(t, bounds, "a", "b", "n", "o")
	out, err := rangeFallback(context.Background(), f, f.family(), kv.LastLessThan([]byte("a")), kv.FirstGreaterThan([]byte("n")), kv.DefaultLimits(), false)
	if err != nil || !slices.Equal(keysOf(out.Data), []string{"a", "b", "n"}) {
		t.Fatalf("%q %v", keysOf(out.Data), err)
	}
	if !out.ReadToBegin {
		t.Fatal("resolved begin is the empty key: ReadToBegin expected")
	}
	out, err = rangeFallback(context.Background(), f, f.family(), kv.FirstGreaterOrEqual([]byte("n")), kv.FirstGreaterOrEqual([]byte("z")).Add(5), kv.DefaultLimits(), false)
	if err != nil || !out.ReadThroughEnd || !slices.Equal(keysOf(out.Data), []string{"n", "o"}) {
		t.Fatalf("%q end=%v %v", keysOf(out.Data), out.ReadThroughEnd, err)
	}
}
