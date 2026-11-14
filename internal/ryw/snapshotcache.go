package ryw

import (
	"bytes"
	"sort"

	"github.com/can1357/fdbgo/internal/kv"
)

// cEntry is a known range [key, end): keys absent from
// values are known to be absent. Entries are immutable once in a tree.
type cEntry struct {
	end    ext
	values []kv.KeyValue
	one    [1]kv.KeyValue // backs values for a single cached key, saving an allocation
}

// segments is the number of segments: an empty range before each value, the
// value, a trailing empty range, plus the unknown gap before the entry.
func (e *cEntry) segments() int { return 2 * (len(e.values) + 1) }

type ctree = ptree[*cEntry]

// emptyCacheRoot holds the degenerate entries at "" and \xff\xff shared by
// every new cache; the first insert copies it.
var emptyCacheRoot = func() *pnode[*cEntry] {
	m := newPmap[*cEntry](nil)
	m.set([]byte{}, &cEntry{})
	m.set(kv.SpecialKeysBegin, &cEntry{end: ext{kv.SpecialKeysBegin, 1}})
	return m.root
}()

// snapshotCache is what the transaction already knows about
// the database at its read version.
type snapshotCache struct {
	entries pmap[*cEntry]
}

func newSnapshotCache() snapshotCache {
	return snapshotCache{entries: newPmap(emptyCacheRoot)}
}

// lowerBoundExt is the index of the first value whose key is not below key.
func lowerBoundExt(values []kv.KeyValue, key ext) int {
	return sort.Search(len(values), func(i int) bool { return key.cmpKey(values[i].Key) <= 0 })
}

// lowerBoundKey is the first index whose key is >= key.
func lowerBoundKey(values []kv.KeyValue, key []byte) int {
	return sort.Search(len(values), func(i int) bool { return bytes.Compare(values[i].Key, key) >= 0 })
}

// tree returns the current contents, for iterators.
func (c *snapshotCache) tree() ctree { return c.entries.freeze() }

// isEmpty reports whether nothing is known about the snapshot.
func (c *snapshotCache) isEmpty() bool {
	empty := true
	c.entries.view().ascend(nil, func(n *pnode[*cEntry]) bool {
		if !n.val.end.eqKey(n.key) && bytes.Compare(n.key, kv.SpecialKeysBegin) < 0 {
			empty = false
			return false
		}
		return true
	})
	return empty
}

// point reports what is known about key: known with value (nil = known
// absent), or unknown.
func (c *snapshotCache) point(key []byte) (value []byte, known bool) {
	n := c.entries.view().floor(key)
	if n == nil || n.val.end.cmpKey(key) <= 0 {
		return nil, false
	}
	vals := n.val.values
	if i := lowerBoundKey(vals, key); i < len(vals) && bytes.Equal(vals[i].Key, key) {
		return vals[i].Value, true
	}
	return nil, true
}

// insertKey records that key has value (nil: absent).
func (c *snapshotCache) insertKey(key, value []byte) bool {
	if _, known := c.point(key); known {
		return false
	}
	e := &cEntry{end: ext{key, 1}}
	if value != nil {
		e.one[0] = kv.KeyValue{Key: key, Value: value}
		e.values = e.one[:]
	}
	c.entries.set(key, e)
	return true
}

// insertRange records that [begin, end) contains
// exactly values (sorted by key).
func (c *snapshotCache) insertRange(begin, end []byte, values []kv.KeyValue) bool {
	if bytes.Compare(begin, end) >= 0 {
		return false
	}
	itb := newCIter(c.entries.view())
	itb.skip(begin)
	ite := itb
	ite.skip(end)

	b := begin
	if !itb.isUnknownRange() && !bytes.Equal(itb.key, begin) {
		b = itb.entry.end.toBytes()
		values = values[lowerBoundExt(values, extKey(b)):]
		itb.nextUnknown()
	}
	e := extKey(end)
	if !ite.isUnknownRange() {
		ite.prevUnknown()
		e = ite.endKey()
		values = values[:lowerBoundExt(values, e)]
	}
	if extKey(b).compare(e) >= 0 {
		return false
	}
	addBegin := len(b) != 0 && len(itb.key) == 0
	if bytes.Compare(itb.key, ite.key) < 0 {
		c.entries.deleteRange(itb.key, false, ite.key, false)
	}
	c.entries.set(b, &cEntry{end: e, values: values})
	if addBegin {
		c.entries.set([]byte{}, &cEntry{})
	}
	return true
}

// Segment kinds of a cache iterator.
const (
	cUnknown = 0
	cEmpty   = 1
	cKV      = 2
)

// cIter walks unknown ranges, known-empty ranges
// and known key/value pairs of one version of the cache. Copying a cIter
// copies its position.
type cIter struct {
	tree  ctree
	key   []byte
	entry *cEntry
	// offset 0: the unknown gap before entry; odd: an empty range; even: a key/value.
	offset int
}

var emptyCEntry = &cEntry{}

// newCIter returns an iterator over tree positioned at the segment containing "".
func newCIter(tree ctree) cIter {
	it := cIter{tree: tree, entry: emptyCEntry}
	if n := tree.first(); n != nil {
		it.key, it.entry = n.key, n.val
	}
	it.skip(nil)
	return it
}

func (it *cIter) seg() int {
	switch {
	case it.offset == 0:
		return cUnknown
	case it.offset&1 == 1:
		return cEmpty
	}
	return cKV
}

func (it *cIter) isUnknownRange() bool { return it.offset == 0 }
func (it *cIter) isEmptyRange() bool   { return it.seg() == cEmpty }

func (it *cIter) beginKey() ext {
	switch o := it.offset; o {
	case 0:
		if p := it.tree.lower(it.key); p != nil {
			return p.val.end
		}
		return ext{}
	case 1:
		return extKey(it.key)
	default:
		return ext{it.entry.values[(o-2)>>1].Key, o & 1}
	}
}

func (it *cIter) endKey() ext {
	o := it.offset
	if o == 0 {
		return extKey(it.key)
	}
	if o == it.entry.segments()-1 {
		return it.entry.end
	}
	return ext{it.entry.values[(o-1)>>1].Key, 1 - (o & 1)}
}

// kvIndex is the index in values() of the current key/value segment.
func (it *cIter) kvIndex() int { return (it.offset - 2) >> 1 }

// values are the known values of the current entry.
func (it *cIter) values() []kv.KeyValue { return it.entry.values }

// kv is the current key/value segment's row.
func (it *cIter) kv() *kv.KeyValue { return &it.entry.values[it.kvIndex()] }

func (it *cIter) next() {
	originalEnd := it.endKey()
	for {
		it.offset++
		if it.offset == it.entry.segments() {
			n := it.tree.higher(it.key)
			if n == nil {
				it.offset--
				return
			}
			it.key, it.entry, it.offset = n.key, n.val, 0
		}
		if it.endKey().compare(originalEnd) != 0 {
			return
		}
	}
}

func (it *cIter) prev() {
	originalBegin := it.beginKey()
	for {
		it.offset--
		if it.offset < 0 {
			n := it.tree.lower(it.key)
			if n == nil {
				it.offset = 0
				return
			}
			it.key, it.entry = n.key, n.val
			it.offset = it.entry.segments() - 1
		}
		if it.beginKey().compare(originalBegin) != 0 {
			return
		}
	}
}

// skip moves to the segment containing key.
func (it *cIter) skip(key []byte) {
	if bytes.Equal(key, kv.SpecialKeysBegin) {
		if n := it.tree.last(); n != nil {
			it.key, it.entry = n.key, n.val
		}
		it.offset = 1
		return
	}
	if n := it.tree.floor(key); n != nil {
		it.key, it.entry = n.key, n.val
	}
	if it.entry.end.cmpKey(key) <= 0 {
		it.offset = 0
		if n := it.tree.higher(it.key); n != nil {
			it.key, it.entry = n.key, n.val
		}
		return
	}
	vals := it.entry.values
	idx := lowerBoundKey(vals, key)
	it.offset = idx*2 + 1
	if idx < len(vals) && bytes.Equal(vals[idx].Key, key) {
		it.offset++
	}
}

// skipContiguous moves to the last key/value of this entry whose key is below key.
func (it *cIter) skipContiguous(key ext) {
	from := it.offset / 2
	j := from + lowerBoundExt(it.entry.values[from:], key)
	it.offset = 2 * j
}

// skipContiguousBack moves to the first key/value of this entry whose key is at least key.
func (it *cIter) skipContiguousBack(key ext) {
	to := it.offset/2 - 1
	j := lowerBoundExt(it.entry.values[:to], key)
	it.offset = 2*j + 2
}

// nextUnknown moves to the unknown gap after this entry.
func (it *cIter) nextUnknown() {
	if n := it.tree.higher(it.key); n != nil {
		it.key, it.entry = n.key, n.val
	}
	it.offset = 0
}

// prevUnknown moves to the unknown gap before this entry.
func (it *cIter) prevUnknown() { it.offset = 0 }

func (it *cIter) equal(r *cIter) bool { return bytes.Equal(it.key, r.key) && it.offset == r.offset }
