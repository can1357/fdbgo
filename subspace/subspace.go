// Package subspace provides Subspace, a key prefix (usually a packed tuple)
// that namespaces keys, as in the official bindings' subspace package. A
// Subspace also tracks an incomplete versionstamp packed into the prefix so that versionstamped keys can be built under it.
package subspace

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"strings"

	"github.com/can1357/fdbgo/internal/kv"
	"github.com/can1357/fdbgo/tuple"
)

// ErrKeyNotInSubspace is returned by Unpack for keys outside the subspace.
var ErrKeyNotInSubspace = errors.New("subspace: key is not in subspace")

// Subspace is a well-defined region of keyspace: every key that starts with
// its prefix. The zero value is AllKeys. Subspaces are immutable values.
type Subspace struct {
	prefix []byte
	// stamp tracks incomplete versionstamps packed into prefix by Sub: 0 none,
	// n > 0 one whose transaction version starts at prefix[n-1], -1 several.
	stamp int
}

// AllKeys returns the subspace of every key in the database (empty prefix).
func AllKeys() Subspace { return Subspace{} }

// FromBytes returns the subspace with a copy of b as its raw prefix.
// Versionstamps inside b are not tracked.
func FromBytes(b []byte) Subspace { return Subspace{prefix: bytes.Clone(b)} }

// Sub returns the subspace whose prefix is the packed tuple of elems.
func Sub(elems ...any) (Subspace, error) { return AllKeys().Sub(elems...) }

// Sub returns the subspace whose prefix extends s with the packed tuple of
// elems. It fails if an element cannot be packed.
func (s Subspace) Sub(elems ...any) (Subspace, error) {
	t := tuple.Tuple(elems)
	has, err := t.HasIncompleteVersionstamp()
	several := errors.Is(err, tuple.ErrMultipleIncompleteVersionstamps)
	if has && !several && s.stamp == 0 {
		key, err := t.PackWithVersionstamp(s.prefix)
		if err != nil {
			return Subspace{}, err
		}
		n := len(key) - 4
		return Subspace{prefix: key[:n:n], stamp: int(binary.LittleEndian.Uint32(key[n:])) + 1}, nil
	}
	prefix, err := t.AppendPack(s.prefix[:len(s.prefix):len(s.prefix)])
	if err != nil {
		return Subspace{}, err
	}
	stamp := s.stamp
	if has {
		stamp = -1
	}
	return Subspace{prefix: prefix, stamp: stamp}, nil
}

// Bytes returns the prefix of the subspace. It must not be modified.
func (s Subspace) Bytes() []byte { return s.prefix }

// Pack returns a caller-owned key of t in the subspace: the prefix followed by the packed tuple.
func (s Subspace) Pack(t tuple.Tuple) ([]byte, error) {
	if len(t) == 0 {
		return bytes.Clone(s.prefix), nil
	}
	return t.AppendPack(s.prefix[:len(s.prefix):len(s.prefix)])
}

// PackWithVersionstamp returns the key of t in the subspace for
// SetVersionstampedKey: exactly one incomplete versionstamp, in t or in the
// prefix built by Sub, and its 4-byte little-endian offset appended.
func (s Subspace) PackWithVersionstamp(t tuple.Tuple) ([]byte, error) {
	switch {
	case s.stamp == 0:
		return t.PackWithVersionstamp(s.prefix)
	case s.stamp < 0:
		return nil, tuple.ErrMultipleIncompleteVersionstamps
	}
	has, err := t.HasIncompleteVersionstamp()
	if has || err != nil {
		return nil, tuple.ErrMultipleIncompleteVersionstamps
	}
	if s.stamp-1 > math.MaxUint32 {
		return nil, errors.New("subspace: versionstamp offset does not fit in 32 bits")
	}
	key, err := t.AppendPack(s.prefix[:len(s.prefix):len(s.prefix)])
	if err != nil {
		return nil, err
	}
	return binary.LittleEndian.AppendUint32(key, uint32(s.stamp-1)), nil
}

// Unpack decodes the tuple of key with the subspace prefix removed. It fails
// if key is outside the subspace or not a well-formed tuple.
func (s Subspace) Unpack(key []byte) (tuple.Tuple, error) {
	if !s.Contains(key) {
		return nil, ErrKeyNotInSubspace
	}
	return tuple.Unpack(key[len(s.prefix):])
}

// Contains reports whether key starts with the subspace prefix.
func (s Subspace) Contains(key []byte) bool { return bytes.HasPrefix(key, s.prefix) }

// FDBRangeKeys implements fdb.ExactRange: the keys strictly inside the
// subspace that encode tuples, [prefix+0x00, prefix+0xff).
func (s Subspace) FDBRangeKeys() (begin, end []byte) {
	n := len(s.prefix) + 1
	buf := make([]byte, 2*n)
	copy(buf, s.prefix)
	copy(buf[n:], s.prefix)
	buf[2*n-1] = 0xff
	return buf[:n:n], buf[n:]
}

// FDBRangeKeySelectors implements fdb.Range: the first keys >= each bound of FDBRangeKeys.
func (s Subspace) FDBRangeKeySelectors() (begin, end kv.KeySelector) {
	b, e := s.FDBRangeKeys()
	return kv.FirstGreaterOrEqual(b), kv.FirstGreaterOrEqual(e)
}

// String returns the prefix with printable ASCII as is, backslash doubled
// and every other byte as \xNN (fdb.Printable).
func (s Subspace) String() string {
	const hex = "0123456789abcdef"
	var sb strings.Builder
	for _, c := range s.prefix {
		switch {
		case c == '\\':
			sb.WriteString(`\\`)
		case c >= 32 && c < 127:
			sb.WriteByte(c)
		default:
			sb.WriteString(`\x`)
			sb.WriteByte(hex[c>>4])
			sb.WriteByte(hex[c&15])
		}
	}
	return sb.String()
}
