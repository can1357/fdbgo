package ryw

import (
	"github.com/can1357/fdbgo/internal/fdberr"
	"github.com/can1357/fdbgo/internal/kv"
)

// Segment kinds as seen through the transaction.
const (
	rUnknown = 0
	rEmpty   = 1
	rKV      = 2
)

// typeMap combines write and cache segment kinds into a transaction segment
// kind, indexed by write segment*3 + cache segment.
var typeMap = [12]uint8{
	rUnknown, rEmpty, rKV, // unmodified
	rEmpty, rEmpty, rEmpty, // cleared
	rKV, rKV, rKV, // independent
	rUnknown, rKV, rKV, // dependent
}

// rywIter is the snapshot cache overlaid with a pinned write map
// version, i.e. the database as this transaction sees it. Copying a rywIter
// copies its position.
type rywIter struct {
	// beginCmp orders the cache and write segment begin keys; endCmp the end keys.
	beginCmp, endCmp int
	cache            cIter
	writes           wIter
	// bypassUnreadable allows reading ranges made unreadable by versionstamp operations.
	bypassUnreadable bool
}

// newRywIter returns an iterator positioned at "".
func newRywIter(cache ctree, writes wtree, bypassUnreadable bool) rywIter {
	it := rywIter{cache: newCIter(cache), writes: newWIter(writes), bypassUnreadable: bypassUnreadable}
	it.updateCmp()
	return it
}

// refreshCache replaces the cache version; call skip before using the iterator again.
func (it *rywIter) refreshCache(cache ctree) {
	it.cache = newCIter(cache)
	it.updateCmp()
}

// seg is the current segment kind; it fails with accessed_unreadable inside
// an unreadable range unless bypassed.
func (it *rywIter) seg() (uint8, error) {
	if it.isUnreadable() && !it.bypassUnreadable {
		return 0, fdberr.AccessedUnreadable
	}
	return typeMap[it.writes.seg()*3+it.cache.seg()], nil
}

func (it *rywIter) isKV() (bool, error) {
	s, err := it.seg()
	return s == rKV, err
}

func (it *rywIter) isUnknownRange() (bool, error) {
	s, err := it.seg()
	return s == rUnknown && err == nil, err
}

func (it *rywIter) isEmptyRange() (bool, error) {
	s, err := it.seg()
	return s == rEmpty, err
}

// isDependent reports that the segment is a write whose value needs the snapshot value.
func (it *rywIter) isDependent() bool { return it.writes.seg() == wDependent }

// isUnreadable reports an unreadable segment regardless of bypass.
func (it *rywIter) isUnreadable() bool { return it.writes.isUnreadable() }

func (it *rywIter) beginKey() ext {
	if it.beginCmp <= 0 {
		return it.writes.beginKey()
	}
	return it.cache.beginKey()
}

func (it *rywIter) endKey() ext {
	if it.endCmp <= 0 {
		return it.cache.endKey()
	}
	return it.writes.endKey()
}

// kv returns the key/value of a key/value segment; ok is false if a
// compare-and-clear deleted it.
func (it *rywIter) kv() (row kv.KeyValue, ok bool, err error) {
	if it.isUnreadable() && !it.bypassUnreadable {
		return kv.KeyValue{}, false, fdberr.AccessedUnreadable
	}
	if it.writes.isUnmodifiedRange() {
		return *it.cache.kv(), true, nil
	}
	var snapshotValue []byte
	if !it.writes.isIndependent() && !it.cache.isEmptyRange() {
		snapshotValue = it.cache.kv().Value
	}
	m, err := coalesceUnder(it.writes.op(), snapshotValue)
	if err != nil || m.value == nil {
		return kv.KeyValue{}, false, err
	}
	return kv.KeyValue{Key: it.writes.beginKey().base, Value: m.value}, true, nil
}

// cacheRunIndex returns the index of the current cache key/value when the
// segment is an unmodified cached key/value, i.e. when skipContiguous can
// extend it to a run.
func (it *rywIter) cacheRunIndex() (int, bool, error) {
	isKV, err := it.isKV()
	if err != nil || !isKV || !it.writes.isUnmodifiedRange() {
		return 0, false, err
	}
	return it.cache.kvIndex(), true, nil
}

// cacheValues are the known values of the current cache entry.
func (it *rywIter) cacheValues() []kv.KeyValue { return it.cache.values() }

func (it *rywIter) next() {
	if it.endCmp <= 0 {
		it.cache.next()
	}
	if it.endCmp >= 0 {
		it.writes.next()
	}
	it.beginCmp = -it.endCmp
	it.endCmp = it.cache.endKey().compare(it.writes.endKey())
}

func (it *rywIter) prev() {
	if it.beginCmp >= 0 {
		it.cache.prev()
	}
	if it.beginCmp <= 0 {
		it.writes.prev()
	}
	it.endCmp = -it.beginCmp
	it.beginCmp = it.cache.beginKey().compare(it.writes.beginKey())
}

// skip moves to the segment containing key.
func (it *rywIter) skip(key []byte) {
	it.cache.skip(key)
	it.writes.skip(key)
	it.updateCmp()
}

// skipContiguous extends an unmodified cached key/value to the last
// contiguous cached key below key and below the end of the write segment.
func (it *rywIter) skipContiguous(key []byte) error {
	isKV, err := it.isKV()
	if err != nil {
		return err
	}
	if isKV && it.writes.isUnmodifiedRange() {
		it.cache.skipContiguous(extMin(extKey(key), it.writes.endKey()))
		it.updateCmp()
	}
	return nil
}

// skipContiguousBack is the reverse of skipContiguous.
func (it *rywIter) skipContiguousBack(key []byte) error {
	isKV, err := it.isKV()
	if err != nil {
		return err
	}
	if isKV && it.writes.isUnmodifiedRange() {
		it.cache.skipContiguousBack(extMax(extKey(key), it.writes.beginKey().keyAfter()))
		it.updateCmp()
	}
	return nil
}

func (it *rywIter) writeIter() *wIter { return &it.writes }

func (it *rywIter) updateCmp() {
	it.beginCmp = it.cache.beginKey().compare(it.writes.beginKey())
	it.endCmp = it.cache.endKey().compare(it.writes.endKey())
}

func (it *rywIter) equal(r *rywIter) bool {
	return it.cache.equal(&r.cache) && it.writes.equal(&r.writes)
}
