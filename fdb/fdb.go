// Package fdb is a pure-Go FoundationDB 7.4 client. It speaks FlowTransport
// to the cluster itself instead of linking libfdb_c, with the semantics of
// the C client.
//
// Reads and commits block and take a context.Context. Cancelling a read's
// context stops that operation and returns ctx.Err(); a failed tracked read
// also prevents its transaction from committing. Watch is the dedicated
// asynchronous handle for observing changes after commit:
//
//	db, err := fdb.Open("") // FDB_CLUSTER_FILE, ./fdb.cluster, then the platform default
//	if err != nil {
//		return err
//	}
//	defer db.Close()
//
//	n, err := fdb.Transact(ctx, db, func(tr *fdb.Transaction) (int, error) {
//		v, err := tr.Get(ctx, []byte("hello"))
//		if err != nil {
//			return 0, err
//		}
//		tr.Set([]byte("hello"), []byte("world"))
//		return len(v), nil
//	})
//
// Keys and values are plain byte slices. A nil value means the key is
// absent; a present empty value is a non-nil empty slice. Slices returned by
// reads may alias reply buffers and must not be modified.
//
// Errors from the cluster are Error codes; match them with errors.Is
// (errors.Is(err, fdb.ErrNotCommitted)) or inspect them with errors.As.
package fdb

import (
	"bytes"
	"errors"
	"strings"

	"github.com/can1357/fdbgo/internal/fdberr"
	"github.com/can1357/fdbgo/internal/kv"
)

// Error is a FoundationDB error code. It is comparable, so
// errors.Is(err, fdb.ErrTransactionTooOld) matches by code; the Err*
// constants list every code.
type Error = fdberr.Error

// KeyValue is one row of a range read.
type KeyValue = kv.KeyValue

// KeySelector identifies a key by its position relative to another key: the
// last key less than (or, with OrEqual, equal to) Key, moved Offset keys
// forward.
type KeySelector = kv.KeySelector

// FirstGreaterOrEqual selects the first key >= key.
func FirstGreaterOrEqual(key []byte) KeySelector { return kv.FirstGreaterOrEqual(key) }

// FirstGreaterThan selects the first key > key.
func FirstGreaterThan(key []byte) KeySelector { return kv.FirstGreaterThan(key) }

// LastLessOrEqual selects the last key <= key.
func LastLessOrEqual(key []byte) KeySelector { return kv.LastLessOrEqual(key) }

// LastLessThan selects the last key < key.
func LastLessThan(key []byte) KeySelector { return kv.LastLessThan(key) }

// Range is a range of keys bounded by key selectors. KeyRange,
// SelectorRange, subspace.Subspace and directory subspaces implement it.
type Range interface {
	// FDBRangeKeySelectors returns the selectors of the first key in the
	// range and of the first key past it.
	FDBRangeKeySelectors() (begin, end KeySelector)
}

// ExactRange is a Range with literal bounds, as needed by clears, conflict
// ranges and size estimates.
type ExactRange interface {
	// FDBRangeKeys returns the inclusive begin and exclusive end keys.
	FDBRangeKeys() (begin, end []byte)
	Range
}

// KeyRange is the keys in [Begin, End).
type KeyRange struct {
	Begin, End []byte
}

// FDBRangeKeys implements ExactRange.
func (r KeyRange) FDBRangeKeys() (begin, end []byte) { return r.Begin, r.End }

// FDBRangeKeySelectors implements Range.
func (r KeyRange) FDBRangeKeySelectors() (begin, end KeySelector) {
	return FirstGreaterOrEqual(r.Begin), FirstGreaterOrEqual(r.End)
}

// SelectorRange is the keys from the key Begin resolves to (inclusive) to
// the key End resolves to (exclusive).
type SelectorRange struct {
	Begin, End KeySelector
}

// FDBRangeKeySelectors implements Range.
func (r SelectorRange) FDBRangeKeySelectors() (begin, end KeySelector) { return r.Begin, r.End }

// errNoStrinc is returned for prefixes with no strict successor.
var errNoStrinc = errors.New("fdb: key must contain at least one byte not equal to 0xFF")

// Strinc returns the first key that does not have prefix as a prefix: prefix
// with trailing 0xFF bytes removed and its last byte incremented. It fails if
// prefix is empty or all 0xFF.
func Strinc(prefix []byte) ([]byte, error) {
	// A byte loop: bytes.TrimRight decodes UTF-8 and would also trim any
	// other byte >= 0x80, which decodes to the same RuneError as "\xff".
	n := len(prefix)
	for n > 0 && prefix[n-1] == 0xff {
		n--
	}
	if n == 0 {
		return nil, errNoStrinc
	}
	out := bytes.Clone(prefix[:n])
	out[len(out)-1]++
	return out, nil
}

// PrefixRange returns the range of keys that start with prefix.
func PrefixRange(prefix []byte) (KeyRange, error) {
	end, err := Strinc(prefix)
	if err != nil {
		return KeyRange{}, err
	}
	return KeyRange{Begin: bytes.Clone(prefix), End: end}, nil
}

// Printable formats a key or value for humans: printable ASCII as is,
// backslash doubled, every other byte as \xNN.
func Printable(b []byte) string {
	var s strings.Builder
	for _, c := range b {
		switch {
		case c == '\\':
			s.WriteString(`\\`)
		case c >= 32 && c < 127:
			s.WriteByte(c)
		default:
			const hex = "0123456789abcdef"
			s.WriteString(`\x`)
			s.WriteByte(hex[c>>4])
			s.WriteByte(hex[c&15])
		}
	}
	return s.String()
}
