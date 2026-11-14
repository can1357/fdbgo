// Package tuple is the FoundationDB tuple layer: an order-preserving encoding
// of typed element lists into byte keys, byte-compatible with every official
// binding (FoundationDB's design/tuple.md spec), with the API shape of the
// official Go bindings.
//
// Encodable element types: nil, []byte, string, every int and uint width,
// *big.Int and big.Int, float32, float64, bool, UUID, Versionstamp and nested
// Tuple (or []any). Unpack returns nil, []byte, string, int64 (uint64 for
// values above math.MaxInt64, *big.Int beyond 64 bits), float32, float64,
// bool, UUID, Versionstamp and Tuple.
//
// Strings are packed as their raw bytes; like the official Go binding, no
// UTF-8 validation is done on either side.
package tuple

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/can1357/fdbgo/internal/kv"
)

// Tuple is an ordered list of elements that packs into an order-preserving
// key. Packing two tuples preserves their order: the packed bytes compare
// like the tuples (elements of different types order by type code).
type Tuple []any

// UUID is a 16-byte RFC 4122 UUID element, stored in network byte order.
type UUID [16]byte

// String formats the UUID in the canonical 8-4-4-4-12 hex form.
func (u UUID) String() string {
	const hex = "0123456789abcdef"
	var b [36]byte
	j := 0
	for i, c := range u {
		if i == 4 || i == 6 || i == 8 || i == 10 {
			b[j] = '-'
			j++
		}
		b[j], b[j+1] = hex[c>>4], hex[c&15]
		j += 2
	}
	return string(b[:])
}

// Versionstamp is a 12-byte versionstamp element: the 10-byte commit
// version of a transaction plus a 2-byte user version ordering keys written
// by the same transaction. An incomplete versionstamp (transaction version
// all 0xff) is filled in by the cluster at commit through
// SetVersionstampedKey/Value; see Tuple.PackWithVersionstamp.
type Versionstamp struct {
	TransactionVersion [10]byte
	UserVersion        uint16
}

var incompleteTransactionVersion = [10]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}

// IncompleteVersionstamp returns a versionstamp whose transaction version
// the cluster fills in at commit time.
func IncompleteVersionstamp(userVersion uint16) Versionstamp {
	return Versionstamp{TransactionVersion: incompleteTransactionVersion, UserVersion: userVersion}
}

// IsComplete reports whether the transaction version is set (not all 0xff).
func (v Versionstamp) IsComplete() bool {
	return v.TransactionVersion != incompleteTransactionVersion
}

// Bytes returns the 12-byte encoding: transaction version then the user
// version big-endian.
func (v Versionstamp) Bytes() []byte {
	b := make([]byte, 12)
	copy(b, v.TransactionVersion[:])
	binary.BigEndian.PutUint16(b[10:], v.UserVersion)
	return b
}

// String formats the versionstamp as Versionstamp(<escaped tx version>, <user version>).
func (v Versionstamp) String() string {
	return "Versionstamp(" + printable(v.TransactionVersion[:]) + ", " + strconv.Itoa(int(v.UserVersion)) + ")"
}

// Errors returned by packing and unpacking. Unpack failures wrap
// ErrMalformed with the offending offset.
var (
	// ErrUnsupportedType is wrapped by Pack errors for elements of a type the tuple layer cannot encode.
	ErrUnsupportedType = errors.New("tuple: unsupported element type")
	// ErrIntTooLarge is returned for integers whose magnitude needs more than 255 bytes.
	ErrIntTooLarge = errors.New("tuple: integer magnitude needs more than 255 bytes")
	// ErrNoIncompleteVersionstamp is returned by PackWithVersionstamp when the tuple has no incomplete versionstamp.
	ErrNoIncompleteVersionstamp = errors.New("tuple: no incomplete versionstamp to pack with versionstamp")
	// ErrMultipleIncompleteVersionstamps is returned when a key would contain more than one incomplete versionstamp.
	ErrMultipleIncompleteVersionstamps = errors.New("tuple: more than one incomplete versionstamp")
	// ErrMalformed is wrapped by every Unpack error.
	ErrMalformed = errors.New("tuple: malformed encoding")
)

// String formats the tuple for humans, Python style: strings quoted, byte
// strings as b"..." with non-printable bytes escaped, nested tuples in
// parentheses.
func (t Tuple) String() string {
	var sb strings.Builder
	printTuple(t, &sb)
	return sb.String()
}

func printTuple(t Tuple, sb *strings.Builder) {
	sb.WriteByte('(')
	for i, e := range t {
		if i > 0 {
			sb.WriteString(", ")
		}
		switch e := e.(type) {
		case Tuple:
			printTuple(e, sb)
		case []any:
			printTuple(e, sb)
		case nil:
			sb.WriteString("<nil>")
		case string:
			sb.WriteString(strconv.Quote(e))
		case UUID:
			sb.WriteString("UUID(")
			sb.WriteString(e.String())
			sb.WriteByte(')')
		case []byte:
			sb.WriteString(`b"`)
			sb.WriteString(printable(e))
			sb.WriteByte('"')
		default:
			fmt.Fprintf(sb, "%v", e)
		}
	}
	sb.WriteByte(')')
}

// printable matches fdb.Printable: printable ASCII as is, backslash doubled,
// every other byte as \xNN.
func printable(b []byte) string {
	const hex = "0123456789abcdef"
	var s strings.Builder
	s.Grow(len(b))
	for _, c := range b {
		switch {
		case c == '\\':
			s.WriteString(`\\`)
		case c >= 32 && c < 127:
			s.WriteByte(c)
		default:
			s.WriteString(`\x`)
			s.WriteByte(hex[c>>4])
			s.WriteByte(hex[c&15])
		}
	}
	return s.String()
}

// RangeKeys returns the range of keys strictly inside the packed tuple's
// prefix: every key that packs t followed by at least one more element,
// [pack(t)+0x00, pack(t)+0xff).
func (t Tuple) RangeKeys() (begin, end []byte, err error) {
	p, err := t.AppendPack(nil)
	if err != nil {
		return nil, nil, err
	}
	n := len(p) + 1
	buf := make([]byte, 2*n)
	copy(buf, p)
	copy(buf[n:], p)
	buf[2*n-1] = 0xff
	return buf[:n:n], buf[n:], nil
}

// FDBRangeKeys implements fdb.ExactRange with RangeKeys. The interface has no
// error return, so a tuple that cannot be packed yields the inverted range
// ["\xff", ""): operations that take exact ranges (clears, conflict ranges)
// reject it with inverted_range or key_outside_legal_range instead of
// silently touching an unrelated range, and reads find no keys. Call
// RangeKeys to get the packing error.
func (t Tuple) FDBRangeKeys() (begin, end []byte) {
	b, e, err := t.RangeKeys()
	if err != nil {
		return []byte{0xff}, []byte{}
	}
	return b, e
}

// FDBRangeKeySelectors implements fdb.Range: the first keys >= each bound of FDBRangeKeys.
func (t Tuple) FDBRangeKeySelectors() (begin, end kv.KeySelector) {
	b, e := t.FDBRangeKeys()
	return kv.FirstGreaterOrEqual(b), kv.FirstGreaterOrEqual(e)
}
