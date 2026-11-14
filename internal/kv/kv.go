// Package kv is the key/value vocabulary shared by every client layer: key
// selectors, rows, range limits and range results.
// The public package fdb re-exports KeyValue and KeySelector.
//
// Byte slices follow one convention throughout the client: nil means absent,
// a non-nil empty slice is a present empty key or value.
package kv

import "bytes"

// Keyspace boundaries and size limits.
var (
	// SpecialKeysBegin is the first key past the system keyspace.
	SpecialKeysBegin = []byte("\xff\xff")
	// SpecialKeysEnd is the end of the special key space.
	SpecialKeysEnd = []byte("\xff\xff\xff")
	// SystemKeysBegin is the first key of the system keyspace.
	SystemKeysBegin = []byte("\xff")
	// MetadataVersionKey is readable without system key access; its value comes from the GRV reply.
	MetadataVersionKey = []byte("\xff/metadataVersion")
	// MetadataVersionKeyEnd is the key after MetadataVersionKey.
	MetadataVersionKeyEnd = []byte("\xff/metadataVersion\x00")
)

const (
	// KeySizeLimit is the largest key size, in bytes.
	KeySizeLimit = 10_000
	// SystemKeySizeLimit is the largest key size, in bytes, for keys in the system keyspace.
	SystemKeySizeLimit = 30_000
	// ValueSizeLimit is the largest value size, in bytes.
	ValueSizeLimit = 100_000
)

// MaxReadKeySize is the largest stored key size, including the eight-byte
// tenant prefix for raw user keys. System keys have their own larger limit.
func MaxReadKeySize(key []byte) int {
	if bytes.HasPrefix(key, SystemKeysBegin) {
		return SystemKeySizeLimit
	}
	return KeySizeLimit + 8
}

// KeyAfter returns the successor of key, preserving the end-of-keyspace sentinel.
func KeyAfter(key []byte) []byte {
	if bytes.Equal(key, SpecialKeysBegin) {
		return bytes.Clone(key)
	}
	k := make([]byte, len(key)+1)
	copy(k, key)
	return k
}

// KeyValue is one row of a range read. Both slices may alias a reply buffer
// and must not be modified.
type KeyValue struct {
	Key   []byte
	Value []byte
}

// ValueResult is one ordered point-read outcome in an internal batch.
type ValueResult struct {
	Value []byte
	Err   error
}

// KeyRange is the keys in [Begin, End): a conflict range, a clear range or
// a shard's extent.
type KeyRange struct {
	Begin []byte
	End   []byte
}

// KeySelector identifies a key by its position relative to Key:
// the last key less than (or, with OrEqual, equal to) Key,
// moved Offset keys forward.
type KeySelector struct {
	Key     []byte
	OrEqual bool
	Offset  int
}

// FirstGreaterOrEqual selects the first key >= key.
func FirstGreaterOrEqual(key []byte) KeySelector { return KeySelector{key, false, 1} }

// FirstGreaterThan selects the first key > key.
func FirstGreaterThan(key []byte) KeySelector { return KeySelector{key, true, 1} }

// LastLessOrEqual selects the last key <= key.
func LastLessOrEqual(key []byte) KeySelector { return KeySelector{key, true, 0} }

// LastLessThan selects the last key < key.
func LastLessThan(key []byte) KeySelector { return KeySelector{key, false, 0} }

// Add returns the selector moved by delta keys.
func (s KeySelector) Add(delta int) KeySelector {
	return KeySelector{s.Key, s.OrEqual, s.Offset + delta}
}

// IsBackward reports whether resolution depends only on keys less than Key.
func (s KeySelector) IsBackward() bool { return !s.OrEqual && s.Offset <= 0 }

// IsFirstGreaterOrEqual reports whether s is FirstGreaterOrEqual(s.Key).
func (s KeySelector) IsFirstGreaterOrEqual() bool { return !s.OrEqual && s.Offset == 1 }

// IsFirstGreaterThan reports whether s is FirstGreaterThan(s.Key).
func (s KeySelector) IsFirstGreaterThan() bool { return s.OrEqual && s.Offset == 1 }

// IsLastLessOrEqual reports whether s is LastLessOrEqual(s.Key).
func (s KeySelector) IsLastLessOrEqual() bool { return s.OrEqual && s.Offset == 0 }

// IsDefinitelyGreater reports whether s resolves to a key > k regardless of
// the database contents.
func (s KeySelector) IsDefinitelyGreater(k []byte) bool {
	if s.Offset < 1 {
		return false
	}
	if s.IsFirstGreaterOrEqual() {
		return bytes.Compare(s.Key, k) > 0
	}
	return bytes.Compare(s.Key, k) >= 0
}

// IsDefinitelyLess reports whether s resolves to a key < k regardless of the
// database contents.
func (s KeySelector) IsDefinitelyLess(k []byte) bool {
	if s.Offset > 0 {
		return false
	}
	if s.IsLastLessOrEqual() {
		return bytes.Compare(s.Key, k) < 0
	}
	return bytes.Compare(s.Key, k) <= 0
}

// Normalize shortens an oversized selector to an equivalent bound, retaining
// one byte beyond the largest stored key. It preserves OrEqual and Offset and
// borrows the original key storage; callers retaining the selector must copy it.
func (s *KeySelector) Normalize() {
	if limit := MaxReadKeySize(s.Key) + 1; len(s.Key) > limit {
		s.Key = s.Key[:limit:limit]
	}
}

// RemoveOrEqual folds OrEqual into the key and normalizes its size.
func (s *KeySelector) RemoveOrEqual() {
	s.Normalize()
	if s.OrEqual {
		// An oversized bound cannot equal any stored key, so OrEqual has
		// no effect. Avoid allocating a successor that would be truncated.
		if len(s.Key) <= MaxReadKeySize(s.Key) {
			s.Key = KeyAfter(s.Key)
		}
		s.OrEqual = false
	}
}

// Unlimited means no row or byte limit.
const Unlimited = -1

// Limits are the row and byte limits of a range read.
type Limits struct {
	// Rows is the row limit, or Unlimited.
	Rows int
	// MinRows must be returned even past the byte limit.
	MinRows int
	// Bytes is the byte limit (8 bytes of overhead per row plus key and
	// value), or Unlimited.
	Bytes int
}

// DefaultLimits returns unlimited rows and bytes with MinRows 1.
func DefaultLimits() Limits { return Limits{Unlimited, 1, Unlimited} }

// NewLimits returns the given row and byte limits with MinRows 1.
func NewLimits(rows, bytes int) Limits { return Limits{rows, 1, bytes} }

// RowBytes is what one row counts against the byte limit.
func RowBytes(kv *KeyValue) int { return 8 + len(kv.Key) + len(kv.Value) }

// Decrement charges data against the limits.
func (l *Limits) Decrement(data []KeyValue) {
	if l.Rows != Unlimited {
		l.Rows -= min(len(data), l.Rows)
	}
	l.MinRows = max(l.MinRows-len(data), 0)
	if l.Bytes != Unlimited {
		used := 0
		for i := range data {
			used += RowBytes(&data[i])
		}
		l.Bytes = max(l.Bytes-used, 0)
	}
}

// DecrementOne charges one row against the limits.
func (l *Limits) DecrementOne(kv *KeyValue) {
	if l.Rows != Unlimited {
		l.Rows -= min(1, l.Rows)
	}
	l.MinRows = max(l.MinRows-1, 0)
	if l.Bytes != Unlimited {
		l.Bytes = max(l.Bytes-RowBytes(kv), 0)
	}
}

// IsReached reports whether the row or byte limit has been reached.
func (l Limits) IsReached() bool { return l.Rows == 0 || (l.Bytes == 0 && l.MinRows == 0) }

// HasByteLimit reports whether a byte limit is set.
func (l Limits) HasByteLimit() bool { return l.Bytes != Unlimited }

// HasRowLimit reports whether a row limit is set.
func (l Limits) HasRowLimit() bool { return l.Rows != Unlimited }

// HasSatisfiedMinRows reports whether a byte limit is set and MinRows is met.
func (l Limits) HasSatisfiedMinRows() bool { return l.HasByteLimit() && l.MinRows == 0 }

// IsValid reports whether the limits are well-formed.
func (l Limits) IsValid() bool {
	return (l.Rows >= 0 || l.Rows == Unlimited) &&
		(l.Bytes >= 0 || l.Bytes == Unlimited) &&
		l.MinRows >= 0 &&
		(l.MinRows <= l.Rows || l.Rows == Unlimited)
}

// RangeResult is a range read result.
type RangeResult struct {
	Data []KeyValue
	// More reports that the range may have more keys, possibly beyond the
	// limits; false means it is exhausted.
	More bool
	// ReadThrough, when HasReadThrough, is the end (begin if reverse) of the
	// range actually read, when the last key does not imply it. It may be
	// the empty key, hence the separate flag.
	ReadThrough    []byte
	HasReadThrough bool
	// ReadToBegin reports that the read covered everything from the
	// beginning of the keyspace.
	ReadToBegin bool
	// ReadThroughEnd reports that the read covered everything through the
	// end of the keyspace.
	ReadThroughEnd bool
}
