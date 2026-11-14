package client

import (
	"bytes"
	"slices"
	"sort"

	"github.com/can1357/fdbgo/internal/fdberr"
	"github.com/can1357/fdbgo/internal/kv"
)

// The special key space [\xff\xff, \xff\xff\xff):
// virtual keys computed by the client, read through ordinary get/get_range calls.
//
// Module boundaries and the selector/limit algorithm match libfdb_c 7.4, so reads
// that cross modules or hit unknown modules fail exactly like libfdb_c.
// Implemented read modules: the transaction module (conflicting_keys/,
// read_conflict_range/, write_conflict_range/), status/json,
// connection_string, cluster_file_path and cluster_id.

const (
	// conflictingKeysBegin/End is conflictingKeysRange.
	conflictingKeysBegin = "\xff\xff/transaction/conflicting_keys/"
	conflictingKeysEnd   = "\xff\xff/transaction/conflicting_keys/\xff\xff"
	// readConflictRangeBegin/End is readConflictRangeKeysRange.
	readConflictRangeBegin = "\xff\xff/transaction/read_conflict_range/"
	readConflictRangeEnd   = "\xff\xff/transaction/read_conflict_range/\xff\xff"
	// writeConflictRangeBegin/End is writeConflictRangeKeysRange.
	writeConflictRangeBegin = "\xff\xff/transaction/write_conflict_range/"
	writeConflictRangeEnd   = "\xff\xff/transaction/write_conflict_range/\xff\xff"
	connectionStringKey     = "\xff\xff/connection_string"
	clusterFilePathKey      = "\xff\xff/cluster_file_path"
	clusterIDKey            = "\xff\xff/cluster_id"
	// statusJSONKey is the status document key; reading it requires fetching status first (see needsStatus).
	statusJSONKey    = "\xff\xff/status/json"
	statusJSONKeyEnd = "\xff\xff/status/json\x00"

	specialKeysBegin = "\xff\xff"
	specialKeysEnd   = "\xff\xff\xff"
)

// specialModules lists the [begin, end) boundary of every special key space module.
var specialModules = [...][2]string{
	{"\xff\xff/actor_lineage/", "\xff\xff/actor_lineage0"},
	{"\xff\xff/actor_profiler_conf/", "\xff\xff/actor_profiler_conf0"},
	{"\xff\xff/cluster_file_path", "\xff\xff/cluster_file_path\x00"},
	{"\xff\xff/cluster_id", "\xff\xff/cluster_id\x00"},
	{"\xff\xff/configuration/", "\xff\xff/configuration0"},
	{"\xff\xff/connection_string", "\xff\xff/connection_string\x00"},
	{"\xff\xff/error_message", "\xff\xff/error_message\x00"},
	{"\xff\xff/global_config/", "\xff\xff/global_config0"},
	{"\xff\xff/management/", "\xff\xff/management0"},
	{"\xff\xff/metrics/", "\xff\xff/metrics0"},
	{"\xff\xff/status/json", "\xff\xff/status/json\x00"},
	{"\xff\xff/tracing/", "\xff\xff/tracing0"},
	{"\xff\xff/transaction/", "\xff\xff/transaction0"},
	{"\xff\xff/worker_interfaces/", "\xff\xff/worker_interfaces0"},
}

// specialImpl is one registered read implementation; implNone marks a gap.
type specialImpl uint8

const (
	implNone specialImpl = iota
	implConflictingKeys
	implReadConflicts
	implWriteConflicts
	implConnectionString
	implClusterFilePath
	implClusterID
	implStatusJSON
)

// specialImpls are the registered read implementations, by range.
var specialImpls = [...]struct {
	begin, end string
	imp        specialImpl
}{
	{clusterFilePathKey, "\xff\xff/cluster_file_path\x00", implClusterFilePath},
	{clusterIDKey, "\xff\xff/cluster_id\x00", implClusterID},
	{statusJSONKey, statusJSONKeyEnd, implStatusJSON},
	{connectionStringKey, "\xff\xff/connection_string\x00", implConnectionString},
	{conflictingKeysBegin, conflictingKeysEnd, implConflictingKeys},
	{readConflictRangeBegin, readConflictRangeEnd, implReadConflicts},
	{writeConflictRangeBegin, writeConflictRangeEnd, implWriteConflicts},
}

// specialSource is where special keys get their data from: the transaction and its database.
type specialSource interface {
	// readConflictRows returns the rows of read_conflict_range/ within [begin, end) (full keys).
	readConflictRows(begin, end []byte) ([]kv.KeyValue, error)
	// writeConflictRows returns the rows of write_conflict_range/ within [begin, end) (full keys).
	writeConflictRows(begin, end []byte) []kv.KeyValue
	// conflictingRanges returns the read conflict ranges the last commit attempt was
	// rejected for, if reported.
	conflictingRanges() ([]kv.KeyRange, bool)
	connectionString() string
	// statusJSON loads the status document on demand, including during selector resolution.
	statusJSON() ([]byte, error)
	clusterFilePath() (string, bool)
	clusterID() (string, error)
}

// coalescedRows returns the rows of a CoalescedKeyRangeMap<Value> defaulting to "0"
// with ranges set to "1", keyed by prefix + range boundary, restricted to keys in
// [begin, end).
func coalescedRows(prefix string, ranges []kv.KeyRange, begin, end []byte) []kv.KeyValue {
	sorted := make([]kv.KeyRange, 0, len(ranges))
	for _, r := range ranges {
		if bytes.Compare(r.Begin, r.End) < 0 {
			sorted = append(sorted, r)
		}
	}
	sort.SliceStable(sorted, func(i, j int) bool { return bytes.Compare(sorted[i].Begin, sorted[j].Begin) < 0 })
	var merged []kv.KeyRange
	for _, r := range sorted {
		if n := len(merged); n > 0 && bytes.Compare(r.Begin, merged[n-1].End) <= 0 {
			if bytes.Compare(r.End, merged[n-1].End) > 0 {
				merged[n-1].End = r.End
			}
			continue
		}
		merged = append(merged, r)
	}
	var rows []kv.KeyValue
	push := func(k []byte, v string) {
		key := make([]byte, 0, len(prefix)+len(k))
		key = append(append(key, prefix...), k...)
		if bytes.Compare(key, begin) >= 0 && bytes.Compare(key, end) < 0 {
			rows = append(rows, kv.KeyValue{Key: key, Value: []byte(v)})
		}
	}
	for _, r := range merged {
		push(r.Begin, "1")
		push(r.End, "0")
	}
	return rows
}

// rows returns the implementation's rows within [begin, end).
func (i specialImpl) rows(src specialSource, begin, end []byte) ([]kv.KeyValue, error) {
	single := func(key string, value string, ok bool) []kv.KeyValue {
		if ok && string(begin) <= key && key < string(end) {
			return []kv.KeyValue{{Key: []byte(key), Value: []byte(value)}}
		}
		return nil
	}
	switch i {
	case implReadConflicts:
		return src.readConflictRows(begin, end)
	case implWriteConflicts:
		return src.writeConflictRows(begin, end), nil
	case implConflictingKeys:
		if ranges, ok := src.conflictingRanges(); ok {
			return coalescedRows(conflictingKeysBegin, ranges, begin, end), nil
		}
	case implConnectionString:
		return single(connectionStringKey, src.connectionString(), true), nil
	case implClusterFilePath:
		p, ok := src.clusterFilePath()
		return single(clusterFilePathKey, p, ok), nil
	case implClusterID:
		if string(begin) <= clusterIDKey && clusterIDKey < string(end) {
			id, err := src.clusterID()
			if err != nil {
				return nil, err
			}
			return single(clusterIDKey, id, true), nil
		}
	case implStatusJSON:
		if string(begin) <= statusJSONKey && statusJSONKey < string(end) {
			v, err := src.statusJSON()
			if err != nil {
				return nil, err
			}
			if v != nil {
				return []kv.KeyValue{{Key: []byte(statusJSONKey), Value: v}}, nil
			}
		}
	}
	return nil, nil
}

// specialSegment is one segment of the readImpls range map over the special key space.
type specialSegment struct {
	begin, end string
	imp        specialImpl
}

// specialSegments is readImpls: the special key space partitioned into implemented
// and empty segments.
var specialSegments = func() []specialSegment {
	points := []string{specialKeysBegin, specialKeysEnd}
	for _, m := range specialModules {
		points = append(points, m[0], m[1])
	}
	for _, i := range specialImpls {
		points = append(points, i.begin, i.end)
	}
	slices.Sort(points)
	points = slices.Compact(points)
	segs := make([]specialSegment, 0, len(points)-1)
	for w := 0; w+1 < len(points); w++ {
		s := specialSegment{begin: points[w], end: points[w+1]}
		for _, i := range specialImpls {
			if i.begin <= s.begin && s.end <= i.end {
				s.imp = i.imp
				break
			}
		}
		segs = append(segs, s)
	}
	return segs
}()

// moduleOf returns the module containing key.
func moduleOf(key []byte) (begin, end string, ok bool) {
	for _, m := range specialModules {
		if m[0] <= string(key) && string(key) < m[1] {
			return m[0], m[1], true
		}
	}
	return "", "", false
}

// moveOver is moveKeySelectorOverRangeActor.
func moveOver(src specialSource, seg *specialSegment, ks *kv.KeySelector) error {
	start, end := []byte(seg.begin), []byte(seg.end)
	contains := seg.begin <= string(ks.Key) && string(ks.Key) < seg.end
	if ks.Offset < 1 {
		if contains {
			end = ks.Key
		}
	} else if contains {
		start = ks.Key
	}
	if seg.imp == implNone {
		return nil
	}
	rows, err := seg.imp.rows(src, start, end)
	if err != nil || len(rows) == 0 {
		return err
	}
	n := len(rows)
	if ks.Offset < 1 {
		if n >= 1-ks.Offset {
			ks.Key = rows[n-(1-ks.Offset)].Key
			ks.Offset = 1
		} else {
			ks.Key = rows[0].Key
			ks.Offset += n
		}
	} else if n >= ks.Offset {
		ks.Key = rows[ks.Offset-1].Key
		ks.Offset = 1
	} else {
		ks.Key = kv.KeyAfter(rows[n-1].Key)
		ks.Offset -= n
	}
	return nil
}

// normalizeSelector is normalizeKeySelectorActor: rewrites ks into
// firstGreaterOrEqual form within [lo, hi).
func normalizeSelector(src specialSource, ks *kv.KeySelector, lo, hi string, out *kv.RangeResult) (int, error) {
	segs := specialSegments
	var i int
	if ks.Offset < 1 {
		// rangeContainingKeyBefore
		i = 0
		for j := len(segs) - 1; j >= 0; j-- {
			if segs[j].begin < string(ks.Key) {
				i = j
				break
			}
		}
	} else {
		i = len(segs) - 1
		for j := range segs {
			if segs[j].begin <= string(ks.Key) && string(ks.Key) < segs[j].end {
				i = j
				break
			}
		}
	}
	for (ks.Offset < 1 && segs[i].begin >= lo) || (ks.Offset > 1 && segs[i].begin < hi) {
		if segs[i].imp != implNone {
			if err := moveOver(src, &segs[i], ks); err != nil {
				return 0, err
			}
		}
		if ks.Offset < 1 {
			if i == 0 {
				break
			}
			i--
		} else if ks.Offset > 1 {
			i++
			if i == len(segs) {
				break
			}
		}
	}
	actual := ks.Offset
	if !ks.IsFirstGreaterOrEqual() {
		if ks.Offset < 1 {
			out.ReadToBegin = true
			ks.Key = []byte(lo)
		} else {
			out.ReadThroughEnd = true
			ks.Key = []byte(hi)
		}
		ks.Offset = 1
	}
	return actual, nil
}

// specialGetRange reads a range of the special key space.
func specialGetRange(src specialSource, relaxed bool, begin, end kv.KeySelector, limits kv.Limits, reverse bool) (kv.RangeResult, error) {
	if !limits.IsValid() {
		return kv.RangeResult{}, fdberr.RangeLimitsInvalid
	}
	if limits.IsReached() {
		return kv.RangeResult{}, nil
	}
	begin.RemoveOrEqual()
	end.RemoveOrEqual()
	if begin.Offset >= end.Offset && bytes.Compare(begin.Key, end.Key) >= 0 {
		return kv.RangeResult{}, nil
	}
	lo, hi := specialKeysBegin, specialKeysEnd
	if !relaxed {
		// Bound the read by the module containing begin, or by the gap between modules.
		mb, me, found := moduleOf(begin.Key)
		if found {
			lo, hi = mb, me
		} else {
			lo, hi = specialKeysBegin, specialKeysEnd
			for _, m := range specialModules {
				if m[1] <= string(begin.Key) && m[1] > lo {
					lo = m[1]
				}
				if m[0] > string(begin.Key) && m[0] < hi {
					hi = m[0]
				}
			}
		}
		if !(lo <= string(end.Key) && string(end.Key) <= hi) {
			return kv.RangeResult{}, fdberr.SpecialKeysCrossModuleRead
		}
		if !found {
			return kv.RangeResult{}, fdberr.SpecialKeysNoModuleFound
		}
	}
	var result kv.RangeResult
	beginOffset, err := normalizeSelector(src, &begin, lo, hi, &result)
	if err != nil {
		return kv.RangeResult{}, err
	}
	endOffset, err := normalizeSelector(src, &end, lo, hi, &result)
	if err != nil {
		return kv.RangeResult{}, err
	}
	if beginOffset >= endOffset && bytes.Compare(begin.Key, end.Key) >= 0 {
		return kv.RangeResult{}, nil
	}
	if string(begin.Key) == hi || string(end.Key) == lo {
		return result, nil
	}
	var pieces []*specialSegment
	for i := range specialSegments {
		s := &specialSegments[i]
		if s.imp != implNone && s.begin < string(end.Key) && string(begin.Key) < s.end {
			pieces = append(pieces, s)
		}
	}
	emit := func(seg *specialSegment) (bool, error) {
		start := begin.Key
		if string(begin.Key) < seg.begin {
			start = []byte(seg.begin)
		}
		stop := end.Key
		if seg.end <= string(end.Key) {
			stop = []byte(seg.end)
		}
		rows, err := seg.imp.rows(src, start, stop)
		if err != nil {
			return false, err
		}
		if reverse {
			slices.Reverse(rows)
		}
		for i := range rows {
			limits.DecrementOne(&rows[i])
			result.Data = append(result.Data, rows[i])
			if limits.IsReached() {
				result.More = true
				if reverse {
					result.ReadToBegin = false
				} else {
					result.ReadThroughEnd = false
				}
				return true, nil
			}
		}
		return false, nil
	}
	if reverse {
		slices.Reverse(pieces)
	}
	for _, seg := range pieces {
		done, err := emit(seg)
		if err != nil {
			return kv.RangeResult{}, err
		}
		if done {
			return result, nil
		}
	}
	return result, nil
}

// specialGet is a one-key range read. The value is nil when absent.
func specialGet(src specialSource, relaxed bool, key []byte) ([]byte, error) {
	r, err := specialGetRange(src, relaxed, kv.FirstGreaterOrEqual(key), kv.FirstGreaterOrEqual(kv.KeyAfter(key)),
		kv.NewLimits(1_000_000, kv.Unlimited), false)
	if err != nil {
		return nil, err
	}
	if len(r.Data) == 0 {
		return nil, nil
	}
	return r.Data[0].Value, nil
}

// isSpecialKey reports whether key lies in the special key space.
func isSpecialKey(key []byte) bool {
	return string(key) >= specialKeysBegin && string(key) < specialKeysEnd
}
