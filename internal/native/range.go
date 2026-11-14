package native

import (
	"bytes"
	"context"

	"github.com/can1357/fdbgo/internal/fdberr"
	"github.com/can1357/fdbgo/internal/kv"
	"github.com/can1357/fdbgo/internal/wire"
)

// RangeOut is a range read result over any row type (key-value or mapped rows).
type RangeOut[R any] struct {
	Data []R
	More bool
	// ReadThrough, when HasReadThrough, is the end (begin if reverse) of the
	// range actually read.
	ReadThrough    []byte
	HasReadThrough bool
	ReadToBegin    bool
	ReadThroughEnd bool
}

// Result converts a plain range read to a kv.RangeResult.
func (o *RangeRead) Result() kv.RangeResult {
	return kv.RangeResult{
		Data:           o.Out.Data,
		More:           o.Out.More,
		ReadThrough:    o.Out.ReadThrough,
		HasReadThrough: o.Out.HasReadThrough,
		ReadToBegin:    o.Out.ReadToBegin,
		ReadThroughEnd: o.Out.ReadThroughEnd,
	}
}

// family is one row type of range reads: how to key and count its rows and
// fetch a page of them from a shard.
type family[R any] struct {
	stream    uint32
	key       func(*R) []byte
	decrement func(*kv.Limits, []R)
	fetch     func(ctx context.Context, loc Location, begin, end kv.KeySelector, limit, limitBytes int, version int64) ([]R, bool, error)
}

// rangeSource is what the range loops need from a transaction attempt.
type rangeSource interface {
	readVersion(ctx context.Context) (int64, error)
	locate(ctx context.Context, key []byte, reverse bool, stream uint32) (Location, error)
	rangeLocations(ctx context.Context, begin, end []byte, limit int, reverse bool, stream uint32) ([]Location, error)
	invalidate(key []byte, reverse bool)
	invalidateRange(begin, end []byte)
	resolveKey(ctx context.Context, sel kv.KeySelector) ([]byte, error)
}

func (r Reader) readVersion(ctx context.Context) (int64, error) { return r.shared.version(ctx) }

func (r Reader) locate(ctx context.Context, key []byte, reverse bool, stream uint32) (Location, error) {
	return r.db().Locate(ctx, key, reverse, r.params.provisional, stream)
}

func (r Reader) invalidate(key []byte, reverse bool) { r.db().Locations.Invalidate(key, reverse) }

func (r Reader) invalidateRange(begin, end []byte) { r.db().Locations.InvalidateRange(begin, end) }

func equalKVs(a, b []kv.KeyValue) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !bytes.Equal(a[i].Key, b[i].Key) || !bytes.Equal(a[i].Value, b[i].Value) {
			return false
		}
	}
	return true
}

// plainFamily reads GetKeyValuesRequest pages.
func plainFamily(r Reader) family[kv.KeyValue] {
	return family[kv.KeyValue]{
		stream:    ssGetKeyValues,
		key:       func(row *kv.KeyValue) []byte { return row.Key },
		decrement: func(l *kv.Limits, rows []kv.KeyValue) { l.Decrement(rows) },
		fetch: func(ctx context.Context, loc Location, begin, end kv.KeySelector, limit, limitBytes int, version int64) ([]kv.KeyValue, bool, error) {
			req := &wire.GetKeyValues{Begin: begin, End: end, Limit: int32(limit), LimitBytes: int32(limitBytes), Ctx: r.readCtx(version)}
			page, err := lbRead(ctx, r, loc, ssGetKeyValues, wire.Request[wire.Lb[wire.RangePage]](req), func(a, b wire.RangePage) bool {
				return a.More == b.More && equalKVs(a.Data, b.Data)
			})
			return page.Data, page.More, err
		},
	}
}

// mappedFamily reads GetMappedKeyValuesRequest pages with mapper.
func mappedFamily(r Reader, mapper []byte) family[wire.MappedRow] {
	return family[wire.MappedRow]{
		stream: ssGetMappedKeyValues,
		key:    func(row *wire.MappedRow) []byte { return row.Key },
		decrement: func(l *kv.Limits, rows []wire.MappedRow) {
			for i := range rows {
				l.DecrementOne(&kv.KeyValue{Key: rows[i].Key, Value: rows[i].Value})
			}
		},
		fetch: func(ctx context.Context, loc Location, begin, end kv.KeySelector, limit, limitBytes int, version int64) ([]wire.MappedRow, bool, error) {
			req := &wire.GetMappedKeyValues{Begin: begin, End: end, Mapper: mapper, Limit: int32(limit), LimitBytes: int32(limitBytes), Ctx: r.readCtx(version)}
			page, err := lbRead(ctx, r, loc, ssGetMappedKeyValues, wire.Request[wire.Lb[wire.MappedPage]](req), equalMappedPage)
			return page.Data, page.More, err
		},
	}
}

func equalMappedPage(a, b wire.MappedPage) bool {
	if a.More != b.More || len(a.Data) != len(b.Data) {
		return false
	}
	for i := range a.Data {
		x, y := &a.Data[i], &b.Data[i]
		if !bytes.Equal(x.Key, y.Key) || !bytes.Equal(x.Value, y.Value) || x.IsValue != y.IsValue ||
			!equalSel(x.Begin, y.Begin) || !equalSel(x.End, y.End) || x.More != y.More || !equalKVs(x.Data, y.Data) {
			return false
		}
	}
	return true
}

// rangeStep is what one page of a range read leads to.
type rangeStep uint8

const (
	stepDone     rangeStep = iota // out is the result
	stepNext                      // fetch the next page
	stepRetry                     // location error: wait WRONG_SHARD_SERVER_DELAY, then retry
	stepFallback                  // resolve the selectors and read the exact range
	stepFail                      // err is the result
)

// rangeState is one snapshot range read for any row family: it walks
// shards from the cached locations, falling back to resolving both selectors
// when a shard boundary makes them ambiguous. Pages are prepared and handled
// separately as the read walks each shard.
type rangeState[R any] struct {
	src                rangeSource
	fam                family[R]
	origBegin, origEnd kv.KeySelector
	originalLimits     kv.Limits
	begin, end         kv.KeySelector
	limits             kv.Limits
	reverse            bool
	version            int64
	out                RangeOut[R]

	// The page in flight.
	loc         Location
	locationKey []byte
	backward    bool
	modified    bool
}

func newRangeState[R any](src rangeSource, fam family[R], begin, end kv.KeySelector, limits kv.Limits, reverse bool, version int64) *rangeState[R] {
	st := &rangeState[R]{src: src, fam: fam, origBegin: begin, origEnd: end, originalLimits: limits,
		begin: begin, end: end, limits: limits, reverse: reverse, version: version}
	if len(begin.Key) == 0 && begin.Offset < 1 {
		st.out.ReadToBegin = true
		st.begin = kv.FirstGreaterOrEqual(begin.Key)
	}
	return st
}

// exhausted reports that the remaining range is empty.
func (st *rangeState[R]) exhausted() bool {
	return len(st.end.Key) == 0 && (st.end.Offset < 1 || st.end.IsFirstGreaterOrEqual())
}

// locationKeyFor returns the key whose shard the next page is read from.
func (st *rangeState[R]) locationKeyFor() ([]byte, bool) {
	if st.reverse {
		return st.end.Key, st.end.Add(-1).IsBackward()
	}
	return st.begin.Key, st.begin.IsBackward()
}

// prepare sets up the next page from shard loc and returns its request selectors and limits.
func (st *rangeState[R]) prepare(loc Location, locationKey []byte, backward bool) (kv.KeySelector, kv.KeySelector, int, int) {
	st.loc, st.locationKey, st.backward = loc, locationKey, backward
	st.modified = false
	reqBegin := st.begin
	if st.reverse && st.begin.Add(-1).IsDefinitelyLess(loc.Begin) &&
		(!st.begin.IsFirstGreaterOrEqual() || !bytes.Equal(st.begin.Key, loc.Begin)) {
		st.modified = true
		reqBegin = kv.FirstGreaterOrEqual(loc.Begin)
	}
	reqEnd := st.end
	if !st.reverse && st.end.IsDefinitelyGreater(loc.End) {
		st.modified = true
		reqEnd = kv.FirstGreaterOrEqual(loc.End)
	}
	limit, limitBytes := transformRangeLimits(st.limits, st.reverse)
	return reqBegin, reqEnd, limit, limitBytes
}

// handle processes the page's reply.
func (st *rangeState[R]) handle(data []R, repMore bool, err error) (rangeStep, error) {
	switch {
	case err == nil:
	case isLocationError(err):
		st.src.invalidate(st.locationKey, st.backward)
		if err == fdberr.WrongShardServer {
			return stepFallback, nil
		}
		return stepRetry, nil
	default:
		return stepFail, err
	}
	loc := st.loc
	rows := len(data)
	st.fam.decrement(&st.limits, data)
	modified := st.modified
	if st.reverse && st.begin.IsLastLessOrEqual() && rows > 0 && bytes.Equal(st.fam.key(&data[rows-1]), st.begin.Key) {
		modified = false
	}
	finished := st.limits.IsReached() || (!modified && !repMore) || st.limits.HasSatisfiedMinRows()
	readThrough := modified && !repMore
	if st.out.Data == nil {
		st.out.Data = data
	} else {
		st.out.Data = append(st.out.Data, data...)
	}
	if finished {
		st.out.More = modified || st.limits.IsReached() || repMore
		if readThrough {
			st.out.More = true
			st.out.HasReadThrough = true
			if st.reverse {
				st.out.ReadThrough = loc.Begin
			} else {
				st.out.ReadThrough = loc.End
			}
		}
		return stepDone, nil
	}
	if !repMore {
		if rows == 0 {
			return stepFallback, nil
		}
		if st.reverse {
			st.end = kv.FirstGreaterOrEqual(loc.Begin)
		} else {
			st.begin = kv.FirstGreaterOrEqual(loc.End)
		}
	} else {
		if len(st.out.Data) == 0 {
			return stepFail, fdberr.InternalError
		}
		last := st.fam.key(&st.out.Data[len(st.out.Data)-1])
		if st.reverse {
			st.end = kv.FirstGreaterOrEqual(last)
		} else {
			st.begin = kv.FirstGreaterThan(last)
		}
	}
	return stepNext, nil
}

// after continues the loop after a page was handled with step.
func (st *rangeState[R]) after(ctx context.Context, step rangeStep, err error) (RangeOut[R], error) {
	for {
		switch step {
		case stepDone:
			return st.out, nil
		case stepFail:
			return RangeOut[R]{}, err
		case stepFallback:
			return rangeFallback(ctx, st.src, st.fam, st.origBegin, st.origEnd, st.originalLimits, st.reverse)
		case stepRetry:
			if err := sleep(ctx, wrongShardServerDelay); err != nil {
				return RangeOut[R]{}, err
			}
		}
		if st.exhausted() {
			return st.out, nil
		}
		locationKey, backward := st.locationKeyFor()
		loc, lerr := st.src.locate(ctx, locationKey, backward, st.fam.stream)
		if lerr != nil {
			return RangeOut[R]{}, lerr
		}
		b, e, limit, limitBytes := st.prepare(loc, locationKey, backward)
		data, more, ferr := st.fam.fetch(ctx, loc, b, e, limit, limitBytes, st.version)
		step, err = st.handle(data, more, ferr)
	}
}

// rangeRead is a snapshot range read for any row family.
func rangeRead[R any](ctx context.Context, src rangeSource, fam family[R], begin, end kv.KeySelector, limits kv.Limits, reverse bool) (RangeOut[R], error) {
	version, err := src.readVersion(ctx)
	if err != nil {
		return RangeOut[R]{}, err
	}
	return newRangeState(src, fam, begin, end, limits, reverse, version).after(ctx, stepNext, nil)
}

// rangeFallback is getRangeFallback: it resolves both selectors, then reads the exact range.
func rangeFallback[R any](ctx context.Context, src rangeSource, fam family[R], begin, end kv.KeySelector, limits kv.Limits, reverse bool) (RangeOut[R], error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	type res struct {
		key []byte
		err error
	}
	ech := make(chan res, 1)
	go func() {
		k, err := src.resolveKey(ctx, end)
		ech <- res{k, err}
	}()
	b, berr := src.resolveKey(ctx, begin)
	if berr != nil {
		return RangeOut[R]{}, berr
	}
	e := <-ech
	if e.err != nil {
		return RangeOut[R]{}, e.err
	}
	if bytes.Compare(b, e.key) >= 0 {
		return RangeOut[R]{}, nil
	}
	r, err := exactRange(ctx, src, fam, b, e.key, limits, reverse)
	if err != nil {
		return RangeOut[R]{}, err
	}
	if len(b) == 0 && ((reverse && !r.More) || !reverse) {
		r.ReadToBegin = true
	}
	if bytes.Equal(e.key, kv.SpecialKeysBegin) && ((!reverse && !r.More) || reverse) {
		r.ReadThroughEnd = true
	}
	return r, nil
}

// exactRange is getExactRange: it reads [begin, end) shard by shard.
func exactRange[R any](ctx context.Context, src rangeSource, fam family[R], begin, end []byte, limits kv.Limits, reverse bool) (RangeOut[R], error) {
	var out RangeOut[R]
	version, err := src.readVersion(ctx)
	if err != nil {
		return out, err
	}
locations:
	for {
		locs, err := src.rangeLocations(ctx, begin, end, getRangeShardLimit, reverse, fam.stream)
		if err != nil {
			return RangeOut[R]{}, err
		}
		if len(locs) == 0 {
			return RangeOut[R]{}, fdberr.InternalError
		}
		shard := 0
		for {
			rng := locs[shard]
			limit, limitBytes := transformRangeLimits(limits, reverse)
			data, repMore, err := fam.fetch(ctx, rng, kv.FirstGreaterOrEqual(rng.Begin), kv.FirstGreaterOrEqual(rng.End), limit, limitBytes, version)
			if isLocationError(err) {
				if reverse {
					end = rng.End
				} else {
					begin = rng.Begin
				}
				src.invalidateRange(begin, end)
				if err := sleep(ctx, wrongShardServerDelay); err != nil {
					return RangeOut[R]{}, err
				}
				continue locations
			}
			if err != nil {
				return RangeOut[R]{}, err
			}
			rows := len(data)
			fam.decrement(&limits, data)
			out.Data = append(out.Data, data...)
			if limits.IsReached() {
				out.More = true
				return out, nil
			}
			more := repMore
			if reverse && more && rows > 0 && bytes.Equal(fam.key(&out.Data[len(out.Data)-1]), locs[shard].Begin) {
				more = false
			}
			if more {
				if rows == 0 {
					return RangeOut[R]{}, fdberr.InternalError
				}
				last := fam.key(&out.Data[len(out.Data)-1])
				if reverse {
					locs[shard].End = last
				} else {
					locs[shard].Begin = kv.KeyAfter(last)
				}
			}
			redo := false
			if !more || bytes.Compare(locs[shard].Begin, locs[shard].End) >= 0 {
				if shard == len(locs)-1 {
					cur := locs[shard]
					b, e := cur.End, end
					if reverse {
						b, e = begin, cur.Begin
					}
					if bytes.Compare(b, e) >= 0 {
						out.More = false
						return out, nil
					}
					begin, end = b, e
					redo = true
				}
				shard++
			}
			if limits.HasSatisfiedMinRows() && len(out.Data) > 0 {
				out.More = true
				return out, nil
			}
			if redo {
				continue locations
			}
		}
	}
}

// transformRangeLimits is transformRangeLimits: per-request row and byte limits.
func transformRangeLimits(limits kv.Limits, reverse bool) (int, int) {
	if limits.Bytes != 0 {
		limit := replyByteLimit
		if limits.HasRowLimit() {
			limit = min(replyByteLimit, limits.Rows)
		}
		if reverse {
			limit = -limit
		}
		byteLimit := replyByteLimit
		if limits.HasByteLimit() {
			byteLimit = min(replyByteLimit, limits.Bytes)
		}
		return limit, byteLimit
	}
	if reverse {
		return -limits.MinRows, replyByteLimit
	}
	return limits.MinRows, replyByteLimit
}

// normalizeRange validates and normalizes range read arguments;
// ok false means an empty result.
func normalizeRange(b, e kv.KeySelector, limits kv.Limits) (kv.KeySelector, kv.KeySelector, bool, error) {
	b.Normalize()
	e.Normalize()
	if limits.IsReached() {
		return b, e, false, nil
	}
	if !limits.IsValid() {
		return b, e, false, fdberr.RangeLimitsInvalid
	}
	b.RemoveOrEqual()
	e.RemoveOrEqual()
	if b.Offset >= e.Offset && bytes.Compare(b.Key, e.Key) >= 0 {
		return b, e, false, nil
	}
	return b, e, true, nil
}

// GetKeyConflict is the conflict range a non-snapshot getKey adds (getKeyAndConflictRange).
func GetKeyConflict(sel kv.KeySelector, resolved []byte) kv.KeyRange {
	selKey := sel.Key
	if sel.OrEqual {
		selKey = kv.KeyAfter(sel.Key)
	}
	if sel.Offset <= 0 {
		return kv.KeyRange{Begin: resolved, End: selKey}
	}
	return kv.KeyRange{Begin: selKey, End: kv.KeyAfter(resolved)}
}

// GetRangeConflict is the conflict range a non-snapshot getRange adds
// (getRangeFinished); key returns a row's key.
func GetRangeConflict[R any](begin, end kv.KeySelector, out *RangeOut[R], key func(*R) []byte, reverse bool) kv.KeyRange {
	var rangeBegin []byte
	n := len(out.Data)
	switch {
	case out.ReadToBegin:
		rangeBegin = []byte{}
	case ((!reverse || !out.More || begin.Offset > 1) && begin.Offset > 0) || n == 0:
		rangeBegin = begin.Key
	case reverse:
		rangeBegin = key(&out.Data[n-1])
	default:
		rangeBegin = key(&out.Data[0])
	}
	if end.Offset > begin.Offset && bytes.Compare(end.Key, rangeBegin) < 0 {
		rangeBegin = end.Key
	}
	var rangeEnd []byte
	switch {
	case out.ReadThroughEnd:
		rangeEnd = kv.SpecialKeysBegin
	case ((reverse || !out.More || end.Offset <= 0) && end.Offset <= 1) || n == 0:
		rangeEnd = end.Key
	case reverse:
		rangeEnd = kv.KeyAfter(key(&out.Data[0]))
	default:
		rangeEnd = kv.KeyAfter(key(&out.Data[n-1]))
	}
	if begin.Offset < end.Offset && bytes.Compare(begin.Key, rangeEnd) > 0 {
		rangeEnd = begin.Key
	}
	return kv.KeyRange{Begin: rangeBegin, End: rangeEnd}
}
