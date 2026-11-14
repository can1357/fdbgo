package ryw

import (
	"bytes"
	"context"
	"math"
	"sort"
	"sync"

	"github.com/can1357/fdbgo/internal/fdberr"
	"github.com/can1357/fdbgo/internal/kv"
)

// replyByteLimit is the byte limit of one range read reply, libfdb_c's default.
const replyByteLimit = 80_000

// readCtx is a read's link to its transaction attempt.
type readCtx struct {
	r          *Ryw
	reader     SnapshotReader
	generation uint64
	end        *AttemptEnd
	// maxKey is RywOptions.MaxReadKey, refreshed whenever the transaction is visited.
	maxKey      []byte
	ctx         context.Context
	firstIO     func() error
	survivesEnd func() bool
}

func (r *Ryw) newCtx(reader SnapshotReader) *readCtx {
	m := r.options.MaxReadKey()
	return &readCtx{r: r, reader: reader, generation: r.generation, end: r.reset, maxKey: m}
}

// pointRead pins an unresolved point read. The common unmodified case needs
// no iterator; dependent local mutations retain their issue-time view in merge.
type pointRead struct {
	pending    pendingRead
	reader     SnapshotReader
	end        *AttemptEnd
	r          *Ryw
	generation uint64
	after      []byte // owned key followed by its keyAfter sentinel
	add        bool
	index      int
	merge      *pointReadMerge
}

type pointReadMerge struct {
	it rywIter
	c  readCtx
}

func (pr *pointRead) key() []byte {
	return pr.after[: len(pr.after)-1 : len(pr.after)-1]
}

func (pr *pointRead) wait(ctx context.Context) ([]byte, error) {
	ctx, release := readAttemptContext(ctx, pr.end, pr.reader)
	if release != nil {
		defer release()
	}
	v, err := pr.reader.Get(ctx, pr.key())
	return pr.complete(v, err)
}

// complete publishes and settles one captured plan even when another read in
// its batch failed. Reset results can never enter the next attempt's cache.
func (pr *pointRead) complete(v []byte, err error) ([]byte, error) {
	if err == nil {
		v, err = pr.post(v)
	} else {
		v = nil
	}
	pr.end.Resolve(func(ended error) {
		if ended != nil {
			v, err = nil, ended
		}
		pr.pending.finish(err, ended != nil)
	})
	return v, err
}

// batchSnapshotReader is optional so scalar-only readers retain their existing
// contract. Results must match the input keys exactly in length and order.
type batchSnapshotReader interface {
	GetMany(context.Context, [][]byte) []kv.ValueResult
}

func waitPointReads(ctx context.Context, reader SnapshotReader, plans []pointRead, values [][]byte, firstErr error, firstIndex int) ([][]byte, error) {
	if batch, ok := reader.(batchSnapshotReader); ok {
		ctx, release := readAttemptContext(ctx, plans[0].end, reader)
		if release != nil {
			defer release()
		}
		keys := make([][]byte, len(plans))
		for i := range plans {
			keys[i] = plans[i].key()
		}
		results := batch.GetMany(ctx, keys)
		for i := range plans {
			pr := &plans[i]
			value, err := pr.complete(results[i].Value, results[i].Err)
			values[pr.index] = value
			if err != nil && pr.index < firstIndex {
				firstErr, firstIndex = err, pr.index
			}
		}
	} else {
		// Scalar-only readers may require concurrent calls to make progress. Capture
		// is already complete; each goroutine owns one plan and result slot.
		var wg sync.WaitGroup
		var mu sync.Mutex
		run := func(pr *pointRead) {
			value, err := pr.wait(ctx)
			values[pr.index] = value
			if err != nil {
				mu.Lock()
				if pr.index < firstIndex {
					firstErr, firstIndex = err, pr.index
				}
				mu.Unlock()
			}
		}
		wg.Add(len(plans) - 1)
		for i := range plans[:len(plans)-1] {
			go func() {
				defer wg.Done()
				run(&plans[i])
			}()
		}
		run(&plans[len(plans)-1])
		wg.Wait()
	}
	if firstErr != nil {
		return nil, firstErr
	}
	return values, nil
}

func (pr *pointRead) post(v []byte) ([]byte, error) {
	if m := pr.merge; m != nil {
		key := pr.key()
		v, err := cacheReadValue(&m.c, key, &m.it, v)
		if err == nil && pr.add {
			err = m.c.with(func(r *Ryw) error { return r.updateConflictMapKey(key, m.it.writeIter(), false) })
		}
		if err != nil {
			return nil, err
		}
		return v, nil
	}
	r := pr.r
	if err := r.lockAttempt(pr.generation, pr.end); err != nil {
		return v, err
	}
	key := pr.after[: len(pr.after)-1 : len(pr.after)-1]
	r.cache.insertKey(key, v)
	if pr.add {
		r.addPointReadConflictAfter(key, pr.after)
	}
	r.mu.Unlock()
	return v, nil
}

// lockAttempt locks the transaction unless the read's attempt has ended.
func (r *Ryw) lockAttempt(generation uint64, end *AttemptEnd) error {
	r.mu.Lock()
	if r.generation != generation {
		r.mu.Unlock()
		if err := end.Err(); err != nil {
			return err
		}
		return fdberr.TransactionCancelled
	}
	if err := end.Err(); err != nil {
		r.mu.Unlock()
		return err
	}
	return nil
}

// lock locks the transaction unless the attempt this read belongs to has ended.
func (c *readCtx) lock() error {
	if err := c.r.lockAttempt(c.generation, c.end); err != nil {
		return err
	}
	c.maxKey = c.r.options.MaxReadKey()
	return nil
}

// with runs f on the transaction unless the attempt this read belongs to has ended.
func (c *readCtx) with(f func(r *Ryw) error) error {
	if err := c.lock(); err != nil {
		return err
	}
	err := f(c.r)
	c.r.mu.Unlock()
	return err
}

// refresh points both iterators at the current snapshot cache.
func (c *readCtx) refresh(a, b *rywIter) error {
	var tree ctree
	if err := c.with(func(r *Ryw) error { tree = r.cache.tree(); return nil }); err != nil {
		return err
	}
	a.refreshCache(tree)
	b.refreshCache(tree)
	return nil
}

// get and getRange block on native snapshot operations. The first operation
// pins the read version before the issuing call returns.
func (c *readCtx) get(key []byte) ([]byte, error) {
	if err := c.firstIO(); err != nil {
		return nil, err
	}
	return c.reader.Get(c.ctx, key)
}

func (c *readCtx) getRange(begin, end kv.KeySelector, limits kv.Limits, reverse bool) (kv.RangeResult, error) {
	if err := c.firstIO(); err != nil {
		return kv.RangeResult{}, err
	}
	return c.reader.GetRange(c.ctx, begin, end, limits, reverse)
}

// flagsOnly is a result with no rows, only the begin/end flags.
func flagsOnly(readToBegin, readThroughEnd bool) kv.RangeResult {
	return kv.RangeResult{ReadToBegin: readToBegin, ReadThroughEnd: readThroughEnd}
}

// ---- request bodies ----

// getBody reads key through the transaction and records its read conflict.
func getBody(c *readCtx, it rywIter, key []byte, addConflicts bool) ([]byte, error) {
	value, err := readValue(c, key, &it)
	if err != nil {
		return nil, err
	}
	if addConflicts {
		if err := c.with(func(r *Ryw) error { return r.updateConflictMapKey(key, it.writeIter(), false) }); err != nil {
			return nil, err
		}
	}
	return value, nil
}

// getKeyBody resolves sel through the transaction and records its read conflict.
func getKeyBody(c *readCtx, it rywIter, sel kv.KeySelector, addConflicts bool) ([]byte, error) {
	key, err := readKey(c, sel, &it)
	if err != nil {
		return nil, err
	}
	if addConflicts {
		if err := c.with(func(r *Ryw) error { return r.addGetKeyConflict(sel, key, it.writeIter()) }); err != nil {
			return nil, err
		}
	}
	if len(key) > 0 && len(key) == len(c.maxKey) && &key[0] == &c.maxKey[0] {
		key = dup(key) // never hand out the shared boundary key
	}
	return key, nil
}

// getRangeBody reads a range through the transaction and records its read conflict.
func getRangeBody(c *readCtx, it rywIter, begin, end kv.KeySelector, limits kv.Limits, reverse, addConflicts bool) (kv.RangeResult, error) {
	var result kv.RangeResult
	var err error
	if reverse {
		result, err = getRangeValueBack(c, begin, end, limits, &it)
	} else {
		result, err = getRangeValue(c, begin, end, limits, &it)
	}
	if err != nil {
		return kv.RangeResult{}, err
	}
	if addConflicts {
		extent := RangeExtentOf(&result)
		err := c.with(func(r *Ryw) error {
			return r.addRangeConflict(begin, end, reverse, &extent, it.writeIter(), false)
		})
		if err != nil {
			return kv.RangeResult{}, err
		}
	}
	return result, nil
}

// ---- reads ----

// readValue reads key through the cache and local writes; it leaves the
// write iterator on key's segment.
func readValue(c *readCtx, key []byte, it *rywIter) ([]byte, error) {
	it.skip(key)
	isKV, err := it.isKV()
	if err != nil {
		return nil, err
	}
	if isKV {
		row, _, err := it.kv()
		return row.Value, err
	}
	if empty, err := it.isEmptyRange(); err != nil || empty {
		return nil, err
	}
	res, err := c.get(key)
	if err != nil {
		return nil, err
	}
	return cacheReadValue(c, key, it, res)
}

// cacheReadValue publishes a snapshot reply and merges only the writes captured
// at issue time. it is already positioned on key; do not replay the local lookup.
func cacheReadValue(c *readCtx, key []byte, it *rywIter, res []byte) ([]byte, error) {
	dependent := it.isDependent()
	var tree ctree
	if err := c.with(func(r *Ryw) error {
		r.cache.insertKey(key, res)
		tree = r.cache.tree()
		return nil
	}); err != nil {
		return nil, err
	}
	if !dependent {
		return res, nil
	}
	it.refreshCache(tree)
	it.skip(key)
	row, _, err := it.kv()
	return row.Value, err
}

// readKey resolves a selector by reading at most one row.
func readKey(c *readCtx, sel kv.KeySelector, it *rywIter) ([]byte, error) {
	one := kv.NewLimits(1, kv.Unlimited)
	if sel.Offset > 0 {
		result, err := getRangeValue(c, sel, kv.FirstGreaterOrEqual(c.maxKey), one, it)
		if err != nil {
			return nil, err
		}
		if result.ReadToBegin {
			return emptyKey, nil
		}
		if result.ReadThroughEnd || len(result.Data) == 0 {
			return c.maxKey, nil
		}
		return result.Data[0].Key, nil
	}
	sel.Offset++
	result, err := getRangeValueBack(c, kv.FirstGreaterOrEqual(emptyKey), sel, one, it)
	if err != nil {
		return nil, err
	}
	if result.ReadThroughEnd {
		return c.maxKey, nil
	}
	if result.ReadToBegin || len(result.Data) == 0 {
		return emptyKey, nil
	}
	return result.Data[0].Key, nil
}

// rawSeg is the segment type without the unreadable check, for positions
// already known to be readable or where the caller tests isUnreadable first.
func (it *rywIter) rawSeg() uint8 { return typeMap[it.writes.seg()*3+it.cache.seg()] }

// resolveKeySelectorFromCache moves key toward firstGreaterOrEqual form using
// what is known locally. Afterwards it is on the resolved key, on an unknown
// segment the selector's base key adjoins, or at the database's first/last segment.
func resolveKeySelectorFromCache(key *kv.KeySelector, it *rywIter, maxKey []byte, readToBegin, readThroughEnd *bool, actualOffset *int) {
	key.RemoveOrEqual()
	alreadyExhausted := key.Offset == 1
	it.skip(key.Key)
	if key.Offset <= 0 && it.beginKey().eqKey(key.Key) && len(key.Key) != 0 {
		it.prev()
	}

	keykey := extKey(key.Key)
	needsCopy := false

	for key.Offset > 1 && !it.isUnreadable() && it.rawSeg() != rUnknown && it.endKey().cmpKey(maxKey) < 0 {
		if it.rawSeg() == rKV {
			key.Offset--
		}
		it.next()
		keykey = it.beginKey()
		needsCopy = true
	}
	for key.Offset < 1 && !it.isUnreadable() && it.rawSeg() != rUnknown && !it.beginKey().eqKey(nil) {
		if it.rawSeg() == rKV {
			key.Offset++
			if key.Offset == 1 {
				keykey = it.beginKey()
				needsCopy = true
				break
			}
		}
		it.prev()
		keykey = it.endKey()
		needsCopy = true
	}

	if !alreadyExhausted {
		*actualOffset = key.Offset
	}
	if !it.isUnreadable() && it.rawSeg() != rUnknown && key.Offset < 1 {
		*readToBegin = true
		key.Key = emptyKey
		key.Offset = 1
		return
	}
	if !it.isUnreadable() && it.rawSeg() != rUnknown && key.Offset > 1 {
		*readThroughEnd = true
		key.Key = maxKey
		key.Offset = 1
		return
	}
	for !it.isUnreadable() && it.rawSeg() == rEmpty && it.endKey().cmpKey(maxKey) < 0 {
		it.next()
		keykey = it.beginKey()
		needsCopy = true
	}
	if needsCopy {
		key.Key = keykey.toBytes()
	}
}

// getKnownKeyRange is the range a forward snapshot read proves complete.
func getKnownKeyRange(data *kv.RangeResult, begin, end kv.KeySelector) (kv.KeyRange, bool) {
	beginKey := kv.SpecialKeysBegin
	if begin.Offset <= 1 {
		beginKey = begin.Key
	}
	var endKey ext
	if !data.More && end.Offset >= 1 {
		endKey = extKey(end.Key)
	}
	if data.ReadToBegin {
		beginKey = emptyKey
	}
	if data.ReadThroughEnd {
		endKey = extKey(kv.SpecialKeysBegin)
	}
	if n := len(data.Data); n > 0 {
		first, last := data.Data[0].Key, data.Data[n-1].Key
		if bytes.Compare(first, beginKey) < 0 {
			beginKey = first
		}
		switch {
		case data.HasReadThrough:
			endKey = extMax(endKey, extKey(data.ReadThrough))
		case !data.More && endKey.cmpKey(last) > 0:
		default:
			endKey = ext{last, 1}
		}
	}
	if endKey.cmpKey(beginKey) <= 0 {
		return kv.KeyRange{}, false
	}
	return kv.KeyRange{Begin: beginKey, End: endKey.toBytes()}, true
}

// getKnownKeyRangeBack is the range a reverse snapshot
// read (rows in descending order) proves complete.
func getKnownKeyRangeBack(data *kv.RangeResult, begin, end kv.KeySelector) (kv.KeyRange, bool) {
	beginKey := kv.SpecialKeysBegin
	if !data.More && begin.Offset <= 1 {
		beginKey = begin.Key
	}
	var endKey ext
	if end.Offset >= 1 {
		endKey = extKey(end.Key)
	}
	if data.ReadToBegin {
		beginKey = emptyKey
	}
	if data.ReadThroughEnd {
		endKey = extKey(kv.SpecialKeysBegin)
	}
	if n := len(data.Data); n > 0 {
		first, last := data.Data[0].Key, data.Data[n-1].Key
		switch {
		case data.HasReadThrough:
			if bytes.Compare(data.ReadThrough, beginKey) < 0 {
				beginKey = data.ReadThrough
			}
		case !data.More && bytes.Compare(last, beginKey) > 0:
		default:
			beginKey = last
		}
		if endKey.cmpKey(first) <= 0 {
			endKey = ext{first, 1}
		}
	}
	if endKey.cmpKey(beginKey) <= 0 {
		return kv.KeyRange{}, false
	}
	return kv.KeyRange{Begin: beginKey, End: endKey.toBytes()}, true
}

// skipUncached advances an unknown segment past single-key empty ranges,
// stopping on the unknown segment before the next nontrivial known range;
// returns the empties skipped.
func skipUncached(it, end *rywIter, iterationLimit int) int {
	b, e := it.beginKey(), it.endKey()
	singleEmpty := 0
	for !it.equal(end) {
		iterationLimit--
		if iterationLimit < 0 {
			break
		}
		if it.isUnreadable() || it.rawSeg() == rEmpty {
			if it.isUnreadable() || !e.isKeyAfter(b) {
				for it.isUnreadable() || it.rawSeg() != rUnknown {
					it.prev()
				}
				return singleEmpty
			}
			singleEmpty++
		} else {
			b = e
		}
		it.next()
		e = it.endKey()
	}
	for it.isUnreadable() || it.rawSeg() != rUnknown {
		it.prev()
	}
	return singleEmpty
}

// countUncached counts single-key empty ranges following an unknown segment,
// at most maxClears.
func countUncached(it rywIter, maxKey []byte, maxClears int) int {
	if maxClears <= 0 {
		return 0
	}
	b, e := it.beginKey(), it.endKey()
	singleEmpty := 0
	for e.cmpKey(maxKey) < 0 {
		if it.isUnreadable() || it.rawSeg() == rEmpty {
			if it.isUnreadable() || !e.isKeyAfter(b) {
				return singleEmpty
			}
			singleEmpty++
			if singleEmpty >= maxClears {
				return maxClears
			}
		} else {
			b = e
		}
		it.next()
		e = it.endKey()
	}
	return singleEmpty
}

// skipUncachedBack is the reverse of skipUncached.
func skipUncachedBack(it, end *rywIter, iterationLimit int) int {
	b, e := it.beginKey(), it.endKey()
	singleEmpty := 0
	for !it.equal(end) {
		iterationLimit--
		if iterationLimit < 0 {
			break
		}
		if it.isUnreadable() || it.rawSeg() == rEmpty {
			if it.isUnreadable() || !e.isKeyAfter(b) {
				for it.isUnreadable() || it.rawSeg() != rUnknown {
					it.next()
				}
				return singleEmpty
			}
			singleEmpty++
		} else {
			e = b
		}
		it.prev()
		b = it.beginKey()
	}
	for it.isUnreadable() || it.rawSeg() != rUnknown {
		it.next()
	}
	return singleEmpty
}

// countUncachedBack counts single-key empty ranges preceding an unknown
// segment, at most maxClears.
func countUncachedBack(it rywIter, maxClears int) int {
	if maxClears <= 0 {
		return 0
	}
	b, e := it.beginKey(), it.endKey()
	singleEmpty := 0
	for b.cmpKey(nil) > 0 {
		if it.isUnreadable() || it.rawSeg() == rEmpty {
			if it.isUnreadable() || !e.isKeyAfter(b) {
				return singleEmpty
			}
			singleEmpty++
			if singleEmpty >= maxClears {
				return maxClears
			}
		} else {
			e = b
		}
		it.prev()
		b = it.beginKey()
	}
	return singleEmpty
}

// setRequestLimits sets the limits of a snapshot read that must
// also cover rows past the requested ones (selector offsets, known empty
// keys), growing with the request count.
func setRequestLimits(l *kv.Limits, additionalRows int64, offset, requestCount int) {
	grow := func(rows int) int {
		return int(min(max(int64(max(rows, 1))+additionalRows, int64(offset)), math.MaxInt32))
	}
	l.MinRows = int(min(max(1+additionalRows, int64(offset)), math.MaxInt32))
	if l.HasRowLimit() {
		l.Rows = grow(l.Rows)
	}
	if l.Bytes == 0 {
		l.Bytes = kv.Unlimited
		if !l.HasRowLimit() {
			l.Rows = grow(l.Rows)
		}
	} else if l.HasByteLimit() {
		l.Bytes = int(min(int64(l.Bytes)<<min(requestCount, 20), replyByteLimit))
	}
}

// forwardRun returns the rows of the current key/value segment, extended
// through contiguous cached rows below limitKey (skipContiguous); leaves it
// on the last row taken.
func forwardRun(it *rywIter, start kv.KeyValue, limitKey []byte) ([]kv.KeyValue, error) {
	first, ok, err := it.cacheRunIndex()
	if err != nil {
		return nil, err
	}
	if !ok {
		return []kv.KeyValue{start}, nil
	}
	if err := it.skipContiguous(limitKey); err != nil {
		return nil, err
	}
	last, ok, err := it.cacheRunIndex()
	if err != nil {
		return nil, err
	}
	if !ok {
		last = first
	}
	return it.cacheValues()[first : last+1], nil
}

// backwardRun returns the rows of the current key/value segment, extended
// back through contiguous cached rows at or above limitKey
// (skipContiguousBack), ascending; leaves it on the first row taken.
func backwardRun(it *rywIter, end kv.KeyValue, limitKey []byte) ([]kv.KeyValue, error) {
	last, ok, err := it.cacheRunIndex()
	if err != nil {
		return nil, err
	}
	if !ok {
		return []kv.KeyValue{end}, nil
	}
	if err := it.skipContiguousBack(limitKey); err != nil {
		return nil, err
	}
	first, ok, err := it.cacheRunIndex()
	if err != nil {
		return nil, err
	}
	if !ok {
		first = last
	}
	return it.cacheValues()[first : last+1], nil
}

// insertSnapshot records a snapshot read's known range in the cache and
// points both iterators at the new cache version.
func (c *readCtx) insertSnapshot(kr kv.KeyRange, known bool, rows []kv.KeyValue, it, itEnd *rywIter) error {
	var tree ctree
	err := c.with(func(r *Ryw) error {
		if known {
			r.cache.insertRange(kr.Begin, kr.End, rows)
		}
		tree = r.cache.tree()
		return nil
	})
	if err != nil {
		return err
	}
	it.refreshCache(tree)
	itEnd.refreshCache(tree)
	return nil
}

// getRangeValue is a forward range read through the cache and
// local writes, filling the cache from snapshot reads of the unknown parts.
func getRangeValue(c *readCtx, begin, end kv.KeySelector, limits kv.Limits, it *rywIter) (kv.RangeResult, error) {
	itEnd := *it
	var result kv.RangeResult
	var additionalRows int64
	itemsPastEnd, requestCount := 0, 0
	readToBegin, readThroughEnd := false, false
	actualBeginOffset, actualEndOffset := begin.Offset, end.Offset
	resolve := func() {
		resolveKeySelectorFromCache(&begin, it, c.maxKey, &readToBegin, &readThroughEnd, &actualBeginOffset)
		resolveKeySelectorFromCache(&end, &itEnd, c.maxKey, &readToBegin, &readThroughEnd, &actualEndOffset)
	}
	resolve()

	if actualBeginOffset >= actualEndOffset && bytes.Compare(begin.Key, end.Key) >= 0 {
		return kv.RangeResult{}, nil
	} else if (begin.IsFirstGreaterOrEqual() && bytes.Equal(begin.Key, c.maxKey)) ||
		(end.IsFirstGreaterOrEqual() && len(end.Key) == 0) {
		return flagsOnly(readToBegin, readThroughEnd), nil
	}

	if !end.IsFirstGreaterOrEqual() && bytes.Compare(begin.Key, end.Key) > 0 {
		resolvedEnd, err := readKey(c, end, it)
		if err != nil {
			return kv.RangeResult{}, err
		}
		if len(resolvedEnd) == 0 {
			readToBegin = true
		}
		if bytes.Equal(resolvedEnd, c.maxKey) {
			readThroughEnd = true
		}
		if bytes.Compare(begin.Key, resolvedEnd) >= 0 && !begin.IsBackward() {
			return kv.RangeResult{}, nil
		} else if len(resolvedEnd) == 0 {
			return flagsOnly(readToBegin, readThroughEnd), nil
		}
		if err := c.refresh(it, &itEnd); err != nil {
			return kv.RangeResult{}, err
		}
		resolve()
	}

	for {
		if len(result.Data) == 0 && actualBeginOffset >= actualEndOffset && bytes.Compare(begin.Key, end.Key) >= 0 {
			return kv.RangeResult{}, nil
		}
		if end.Offset <= 1 && len(end.Key) == 0 {
			return flagsOnly(readToBegin, readThroughEnd), nil
		}
		if (begin.Offset >= end.Offset && bytes.Compare(begin.Key, end.Key) >= 0) ||
			(begin.Offset >= 1 && bytes.Compare(begin.Key, c.maxKey) >= 0) {
			if end.IsFirstGreaterOrEqual() || len(result.Data) == 0 {
				break
			}
			resolvedEnd, err := readKey(c, end, it)
			if err != nil {
				return kv.RangeResult{}, err
			}
			if len(resolvedEnd) == 0 {
				readToBegin = true
			}
			if bytes.Equal(resolvedEnd, c.maxKey) {
				readThroughEnd = true
			}
			end = kv.FirstGreaterOrEqual(resolvedEnd)
			break
		}
		if !it.isUnreadable() && it.rawSeg() != rUnknown && it.beginKey().compare(itEnd.beginKey()) > 0 {
			if end.IsFirstGreaterOrEqual() {
				break
			}
			return flagsOnly(readToBegin, readThroughEnd), nil
		}
		if limits.IsReached() && itemsPastEnd >= 1-end.Offset {
			break
		}
		if it.equal(&itEnd) && ((!it.isUnreadable() && it.rawSeg() != rUnknown) ||
			(begin.Offset > 0 && end.IsFirstGreaterOrEqual() && it.beginKey().eqKey(end.Key))) {
			break
		}

		unknown, err := it.isUnknownRange()
		if err != nil {
			return kv.RangeResult{}, err
		}
		if unknown {
			if limits.HasByteLimit() && limits.HasSatisfiedMinRows() && len(result.Data) > 0 && itemsPastEnd >= 1-end.Offset {
				result.More = true
				break
			}

			ucEnd := *it
			singleClears := 0
			clearLimit := 0
			if requestCount != 0 {
				clearLimit = 1 << min(requestCount, 20)
			}
			if it.beginKey().compare(itEnd.beginKey()) < 0 {
				singleClears = min(skipUncached(&ucEnd, &itEnd, clearLimit+100), clearLimit)
			}

			var readEnd kv.KeySelector
			switch {
			case !ucEnd.equal(&itEnd):
				readEnd = kv.FirstGreaterOrEqual(ucEnd.endKey().toBytes())
				if end.Offset < 1 {
					additionalRows += int64(1 - end.Offset)
				}
			case end.Offset < 1:
				readEnd = kv.FirstGreaterOrEqual(end.Key)
				additionalRows += int64(1 - end.Offset)
			default:
				readEnd = end
				if end.Offset > 1 {
					singleClears += countUncached(ucEnd, c.maxKey, clearLimit-singleClears)
					readEnd.Offset += singleClears
				}
			}
			additionalRows += int64(singleClears)

			var readBegin kv.KeySelector
			switch {
			case begin.IsFirstGreaterOrEqual():
				k := begin.Key
				if ib := it.beginKey(); ib.cmpKey(begin.Key) > 0 {
					k = ib.toBytes()
				}
				begin = kv.FirstGreaterOrEqual(k)
				readBegin = begin
			case begin.Offset > 1:
				readBegin = kv.FirstGreaterOrEqual(begin.Key)
				additionalRows += int64(begin.Offset - 1)
			default:
				readBegin = begin
				singleClears = countUncachedBack(*it, clearLimit)
				readBegin.Offset -= singleClears
				additionalRows += int64(singleClears)
			}

			if bytes.Compare(readEnd.Key, readBegin.Key) < 0 {
				readEnd.Key = readBegin.Key
			}

			requestLimit := limits
			setRequestLimits(&requestLimit, additionalRows, 2-readBegin.Offset, requestCount)
			requestCount++
			additionalRows = 0

			snap, err := c.getRange(readBegin, readEnd, requestLimit, false)
			if err != nil {
				return kv.RangeResult{}, err
			}
			kr, known := getKnownKeyRange(&snap, readBegin, readEnd)
			if err := c.insertSnapshot(kr, known, snap.Data, it, &itEnd); err != nil {
				return kv.RangeResult{}, err
			}
			resolve()
			continue
		}
		isKV, err := it.isKV()
		if err != nil {
			return kv.RangeResult{}, err
		}
		if !isKV {
			it.next()
			continue
		}
		start, ok, err := it.kv()
		if err != nil {
			return kv.RangeResult{}, err
		}
		if !ok {
			it.next()
			continue
		}
		limitKey := c.maxKey
		if end.IsFirstGreaterOrEqual() {
			limitKey = end.Key
		}
		run, err := forwardRun(it, start, limitKey)
		if err != nil {
			return kv.RangeResult{}, err
		}
		count := 0
		for count < len(run) && !limits.IsReached() {
			limits.DecrementOne(&run[count])
			count++
		}
		itemsPastEnd += len(run) - count
		result.Data = append(result.Data, run[:count]...)
		it.next()
	}

	result.More = result.More || limits.IsReached()
	if end.IsFirstGreaterOrEqual() {
		keep := sort.Search(len(result.Data), func(i int) bool { return bytes.Compare(result.Data[i].Key, end.Key) >= 0 })
		if keep < len(result.Data) {
			result.More = false
		}
		result.Data = result.Data[:keep]
	}
	result.ReadToBegin = readToBegin
	result.ReadThroughEnd = !result.More && readThroughEnd
	return result, nil
}

// getRangeValueBack is the reverse of getRangeValue; rows
// come out in descending order.
func getRangeValueBack(c *readCtx, begin, end kv.KeySelector, limits kv.Limits, it *rywIter) (kv.RangeResult, error) {
	itEnd := *it
	var result kv.RangeResult
	var additionalRows int64
	itemsPastBegin, requestCount := 0, 0
	readToBegin, readThroughEnd := false, false
	actualBeginOffset, actualEndOffset := begin.Offset, end.Offset
	resolve := func() {
		resolveKeySelectorFromCache(&end, it, c.maxKey, &readToBegin, &readThroughEnd, &actualEndOffset)
		resolveKeySelectorFromCache(&begin, &itEnd, c.maxKey, &readToBegin, &readThroughEnd, &actualBeginOffset)
	}
	resolve()

	if actualBeginOffset >= actualEndOffset && bytes.Compare(begin.Key, end.Key) >= 0 {
		return kv.RangeResult{}, nil
	} else if (begin.IsFirstGreaterOrEqual() && bytes.Equal(begin.Key, c.maxKey)) ||
		(end.IsFirstGreaterOrEqual() && len(end.Key) == 0) {
		return flagsOnly(readToBegin, readThroughEnd), nil
	}

	if !begin.IsFirstGreaterOrEqual() && bytes.Compare(begin.Key, end.Key) > 0 {
		resolvedBegin, err := readKey(c, begin, it)
		if err != nil {
			return kv.RangeResult{}, err
		}
		if len(resolvedBegin) == 0 {
			readToBegin = true
		}
		if bytes.Equal(resolvedBegin, c.maxKey) {
			readThroughEnd = true
		}
		if bytes.Compare(resolvedBegin, end.Key) >= 0 && end.Offset <= 1 {
			return kv.RangeResult{}, nil
		} else if bytes.Equal(resolvedBegin, c.maxKey) {
			return flagsOnly(readToBegin, readThroughEnd), nil
		}
		if err := c.refresh(it, &itEnd); err != nil {
			return kv.RangeResult{}, err
		}
		resolve()
	}

	for {
		if len(result.Data) == 0 && actualBeginOffset >= actualEndOffset && bytes.Compare(begin.Key, end.Key) >= 0 {
			return kv.RangeResult{}, nil
		}
		if !begin.IsBackward() && bytes.Compare(begin.Key, c.maxKey) >= 0 {
			return flagsOnly(readToBegin, readThroughEnd), nil
		}
		if (begin.Offset >= end.Offset && bytes.Compare(begin.Key, end.Key) >= 0) || (end.Offset <= 1 && len(end.Key) == 0) {
			if begin.IsFirstGreaterOrEqual() || len(result.Data) == 0 {
				break
			}
			resolvedBegin, err := readKey(c, begin, it)
			if err != nil {
				return kv.RangeResult{}, err
			}
			if len(resolvedBegin) == 0 {
				readToBegin = true
			}
			if bytes.Equal(resolvedBegin, c.maxKey) {
				readThroughEnd = true
			}
			begin = kv.FirstGreaterOrEqual(resolvedBegin)
			break
		}
		if itemsPastBegin >= begin.Offset-1 && !it.isUnreadable() && it.rawSeg() != rUnknown &&
			it.beginKey().compare(itEnd.beginKey()) < 0 {
			if begin.IsFirstGreaterOrEqual() {
				break
			}
			return flagsOnly(readToBegin, readThroughEnd), nil
		}
		if limits.IsReached() && itemsPastBegin >= begin.Offset-1 {
			break
		}
		if end.IsFirstGreaterOrEqual() && it.beginKey().eqKey(end.Key) {
			if itemsPastBegin >= begin.Offset-1 && it.equal(&itEnd) {
				break
			}
			it.prev()
		}

		unknown, err := it.isUnknownRange()
		if err != nil {
			return kv.RangeResult{}, err
		}
		if unknown {
			if limits.HasByteLimit() && len(result.Data) > 0 && itemsPastBegin >= begin.Offset-1 {
				result.More = true
				break
			}

			ucEnd := *it
			singleClears := 0
			clearLimit := 0
			if requestCount != 0 {
				clearLimit = 1 << min(requestCount, 20)
			}
			if it.beginKey().compare(itEnd.beginKey()) > 0 {
				singleClears = min(skipUncachedBack(&ucEnd, &itEnd, clearLimit+100), clearLimit)
			}

			var readBegin kv.KeySelector
			switch {
			case !ucEnd.equal(&itEnd):
				readBegin = kv.FirstGreaterOrEqual(ucEnd.beginKey().toBytes())
				if begin.Offset > 1 {
					additionalRows += int64(begin.Offset - 1)
				}
			case begin.Offset > 1:
				readBegin = kv.FirstGreaterOrEqual(begin.Key)
				additionalRows += int64(begin.Offset - 1)
			default:
				readBegin = begin
				if begin.Offset < 1 {
					singleClears += countUncachedBack(ucEnd, clearLimit-singleClears)
					readBegin.Offset -= singleClears
				}
			}
			additionalRows += int64(singleClears)

			var readEnd kv.KeySelector
			switch {
			case end.IsFirstGreaterOrEqual():
				k := end.Key
				if ie := it.endKey(); ie.cmpKey(end.Key) < 0 {
					k = ie.toBytes()
				}
				end = kv.FirstGreaterOrEqual(k)
				readEnd = end
			case end.Offset < 1:
				readEnd = kv.FirstGreaterOrEqual(end.Key)
				additionalRows += int64(1 - end.Offset)
			default:
				readEnd = end
				singleClears = countUncached(*it, c.maxKey, clearLimit)
				readEnd.Offset += singleClears
				additionalRows += int64(singleClears)
			}

			if bytes.Compare(readBegin.Key, readEnd.Key) > 0 {
				readBegin.Key = readEnd.Key
			}

			requestLimit := limits
			setRequestLimits(&requestLimit, additionalRows, readEnd.Offset, requestCount)
			requestCount++
			additionalRows = 0

			snap, err := c.getRange(readBegin, readEnd, requestLimit, true)
			if err != nil {
				return kv.RangeResult{}, err
			}
			kr, known := getKnownKeyRangeBack(&snap, readBegin, readEnd)
			var rows []kv.KeyValue
			if known {
				rows = make([]kv.KeyValue, len(snap.Data))
				for i, row := range snap.Data {
					rows[len(rows)-1-i] = row
				}
			}
			if err := c.insertSnapshot(kr, known, rows, it, &itEnd); err != nil {
				return kv.RangeResult{}, err
			}
			resolve()
			continue
		}
		isKV, err := it.isKV()
		if err != nil {
			return kv.RangeResult{}, err
		}
		if isKV {
			endRow, ok, err := it.kv()
			if err != nil {
				return kv.RangeResult{}, err
			}
			if ok {
				var limitKey []byte = emptyKey
				if begin.IsFirstGreaterOrEqual() {
					limitKey = begin.Key
				}
				run, err := backwardRun(it, endRow, limitKey)
				if err != nil {
					return kv.RangeResult{}, err
				}
				count := 0
				for count < len(run) && !limits.IsReached() {
					limits.DecrementOne(&run[len(run)-count-1])
					count++
				}
				itemsPastBegin += len(run) - count
				for i := range count {
					result.Data = append(result.Data, run[len(run)-1-i])
				}
			}
		}
		if it.equal(&itEnd) {
			break
		}
		it.prev()
	}

	result.More = result.More || limits.IsReached()
	if begin.IsFirstGreaterOrEqual() {
		keep := 0
		for keep < len(result.Data) && bytes.Compare(result.Data[keep].Key, begin.Key) >= 0 {
			keep++
		}
		if keep < len(result.Data) {
			result.More = false
		}
		result.Data = result.Data[:keep]
	}
	result.ReadToBegin = !result.More && readToBegin
	result.ReadThroughEnd = readThroughEnd
	return result, nil
}

// ---- read conflict ranges (the transaction is locked) ----

// keyRange returns [begin, end), rejecting inverted ranges with inverted_range.
func keyRange(begin, end []byte) (kv.KeyRange, error) {
	if bytes.Compare(begin, end) > 0 {
		return kv.KeyRange{}, fdberr.InvertedRange
	}
	return kv.KeyRange{Begin: begin, End: end}, nil
}

// updateConflictMapKey records the read conflict of a point read: it conflicts
// unless this transaction's own independent write determines the value. it
// is on key's segment.
func (r *Ryw) updateConflictMapKey(key []byte, it *wIter, mustUnmodified bool) error {
	if mustUnmodified && !it.isUnmodifiedRange() {
		return fdberr.GetMappedRangeReadsYourWrites
	}
	if it.isUnmodifiedRange() || (it.isOperation() && !it.isIndependent()) {
		r.addPointReadConflict(key)
	}
	return nil
}

// addPointReadConflict is the read conflict of a point read whose value came
// from the database. key is retained.
func (r *Ryw) addPointReadConflict(key []byte) {
	r.addPointReadConflictAfter(key, kv.KeyAfter(key))
}

// addPointReadConflictAfter adds the read conflict [key, after), after being keyAfter(key).
func (r *Ryw) addPointReadConflictAfter(key, after []byte) {
	r.approxSize += 2*int64(len(key)) + 1 + sizeofKeyRangeRef
	r.readConflicts.add(key, after)
}

// updateConflictMapRange adds the parts of
// [begin, end) whose values come from the database. it is on begin's segment.
func (r *Ryw) updateConflictMapRange(begin, end []byte, it *wIter, mustUnmodified bool) error {
	endExt, beginExt := extKey(end), extKey(begin)
	for it.beginKey().compare(endExt) < 0 {
		if mustUnmodified && !it.isUnmodifiedRange() {
			return fdberr.GetMappedRangeReadsYourWrites
		}
		if it.isUnmodifiedRange() || (it.isOperation() && !it.isIndependent()) {
			b := extMax(beginExt, it.beginKey()).toBytes()
			e := extMin(endExt, it.endKey()).toBytes()
			if bytes.Compare(b, e) < 0 {
				r.approxSize += int64(len(begin)+len(end)) + sizeofKeyRangeRef
				r.readConflicts.add(b, e)
			}
		}
		it.next()
	}
	return nil
}

// addGetKeyConflict adds the read conflict of a key selector resolution:
// everything between the selector's base key and the result.
func (r *Ryw) addGetKeyConflict(sel kv.KeySelector, result []byte, it *wIter) error {
	base := sel.Key
	if sel.OrEqual {
		base = kv.KeyAfter(sel.Key)
	}
	var kr kv.KeyRange
	var err error
	if sel.Offset <= 0 {
		kr, err = keyRange(result, base)
	} else {
		kr, err = keyRange(base, kv.KeyAfter(result))
	}
	if err != nil {
		return err
	}
	it.skip(kr.Begin)
	return r.updateConflictMapRange(kr.Begin, kr.End, it, false)
}

// addRangeConflict adds the read conflict of a range read: the keys whose
// contents could change the result.
func (r *Ryw) addRangeConflict(begin, end kv.KeySelector, reverse bool, result *RangeExtent, it *wIter, mustUnmodified bool) error {
	var rangeBegin, rangeEnd []byte
	switch {
	case bytes.Compare(begin.Key, end.Key) >= 0:
		rangeBegin, rangeEnd = end.Key, begin.Key
	case reverse:
		rangeBegin, rangeEnd = begin.Key, end.Key
		if begin.Offset <= 1 && result.More {
			rangeBegin = end.Key
		}
	default:
		rangeBegin, rangeEnd = begin.Key, end.Key
		if end.Offset > 0 && result.More {
			rangeEnd = begin.Key
		}
	}
	if result.ReadToBegin && begin.Offset <= 0 {
		rangeBegin = emptyKey
	}
	if result.ReadThroughEnd && end.Offset > 0 {
		rangeEnd = r.options.MaxReadKey()
	}
	if result.HasRows {
		first, last := result.First, result.Last
		if reverse {
			if bytes.Compare(last, rangeBegin) < 0 {
				rangeBegin = last
			}
			if end.Offset > 0 && bytes.Compare(rangeEnd, first) <= 0 {
				rangeEnd = kv.KeyAfter(first)
			}
		} else {
			if begin.Offset <= 0 && bytes.Compare(first, rangeBegin) < 0 {
				rangeBegin = first
			}
			if bytes.Compare(rangeEnd, last) <= 0 {
				rangeEnd = kv.KeyAfter(last)
			}
		}
	}
	kr, err := keyRange(rangeBegin, rangeEnd)
	if err != nil {
		return err
	}
	it.skip(kr.Begin)
	return r.updateConflictMapRange(kr.Begin, kr.End, it, mustUnmodified)
}
