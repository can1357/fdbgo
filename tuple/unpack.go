package tuple

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"math/big"
	"strings"
)

// Unpack decodes a packed tuple. Returned byte strings are copies; b may be
// reused. Integers decode as int64, as uint64 when above math.MaxInt64, and
// as *big.Int beyond 64 bits.
func Unpack(b []byte) (Tuple, error) {
	t, _, err := decodeTuple(b, 0, false)
	if err != nil {
		return nil, err
	}
	return t, nil
}

func malformed(pos int, what string) error {
	return fmt.Errorf("%w: %s at offset %d", ErrMalformed, what, pos)
}

func missing(pos int) error { return malformed(pos, "missing bytes") }

// decodeTuple decodes elements from b[pos:] until the end of b or, when
// nested, the end-of-tuple 0x00. It returns the offset past the tuple.
func decodeTuple(b []byte, pos int, nested bool) (Tuple, int, error) {
	t := make(Tuple, 0, min(len(b)-pos, 4))
	for pos < len(b) {
		c := b[pos]
		if nested && c == nilCode {
			if pos+1 < len(b) && b[pos+1] == escapeCode {
				t = append(t, nil)
				pos += 2
				continue
			}
			return t, pos + 1, nil
		}
		start := pos
		pos++
		switch {
		case c == nilCode:
			t = append(t, nil)
		case c == bytesCode:
			v, next, err := decodeBytes(b, pos)
			if err != nil {
				return nil, 0, err
			}
			t, pos = append(t, v), next
		case c == stringCode:
			v, next, err := decodeString(b, pos)
			if err != nil {
				return nil, 0, err
			}
			t, pos = append(t, v), next
		case c == nestedCode:
			v, next, err := decodeTuple(b, pos, true)
			if err != nil {
				return nil, 0, err
			}
			t, pos = append(t, v), next
		case c >= intZeroCode-8 && c <= intZeroCode+8:
			n := int(c) - intZeroCode
			neg := n < 0
			if neg {
				n = -n
			}
			if len(b)-pos < n {
				return nil, 0, missing(pos)
			}
			var u uint64
			for _, x := range b[pos : pos+n] {
				u = u<<8 | uint64(x)
			}
			pos += n
			t = append(t, smallInt(u, n, neg))
		case c == negIntStart || c == posIntEnd:
			if pos >= len(b) {
				return nil, 0, missing(pos)
			}
			n := int(b[pos])
			if c == negIntStart {
				n ^= 0xff
			}
			pos++
			if len(b)-pos < n {
				return nil, 0, missing(pos)
			}
			t = append(t, bigInt(b[pos:pos+n], c == negIntStart))
			pos += n
		case c == floatCode:
			if len(b)-pos < 4 {
				return nil, 0, missing(pos)
			}
			u := binary.BigEndian.Uint32(b[pos:])
			if u&(1<<31) != 0 {
				u ^= 1 << 31
			} else {
				u = ^u
			}
			t = append(t, math.Float32frombits(u))
			pos += 4
		case c == doubleCode:
			if len(b)-pos < 8 {
				return nil, 0, missing(pos)
			}
			u := binary.BigEndian.Uint64(b[pos:])
			if u&(1<<63) != 0 {
				u ^= 1 << 63
			} else {
				u = ^u
			}
			t = append(t, math.Float64frombits(u))
			pos += 8
		case c == falseCode:
			t = append(t, false)
		case c == trueCode:
			t = append(t, true)
		case c == uuidCode:
			if len(b)-pos < 16 {
				return nil, 0, missing(pos)
			}
			t = append(t, UUID(b[pos:pos+16]))
			pos += 16
		case c == versionstampCode:
			if len(b)-pos < 12 {
				return nil, 0, missing(pos)
			}
			t = append(t, Versionstamp{
				TransactionVersion: [10]byte(b[pos : pos+10]),
				UserVersion:        binary.BigEndian.Uint16(b[pos+10:]),
			})
			pos += 12
		default:
			return nil, 0, malformed(start, fmt.Sprintf("unknown type code 0x%02x", c))
		}
	}
	if nested {
		return nil, 0, missing(pos)
	}
	return t, pos, nil
}

// smallInt converts an integer encoded in n <= 8 bytes (u, one's complement
// for negatives) to int64, uint64 above MaxInt64, or *big.Int below MinInt64.
func smallInt(u uint64, n int, neg bool) any {
	if !neg {
		if u > math.MaxInt64 {
			return u
		}
		return int64(u)
	}
	mag := ^u
	if n < 8 {
		mag &= 1<<(8*n) - 1
	}
	if mag <= 1<<63 {
		return int64(-mag)
	}
	v := new(big.Int).SetUint64(mag)
	return v.Neg(v)
}

// bigInt decodes the magnitude bytes of a 0x0b/0x1d integer, normalizing
// values that fit in 64 bits to int64 or uint64.
func bigInt(mag []byte, neg bool) any {
	v := new(big.Int)
	if neg {
		inv := make([]byte, len(mag))
		for i, x := range mag {
			inv[i] = ^x
		}
		v.SetBytes(inv).Neg(v)
	} else {
		v.SetBytes(mag)
	}
	switch {
	case v.IsInt64():
		return v.Int64()
	case v.IsUint64():
		return v.Uint64()
	}
	return v
}

// escapedEnd scans an escaped byte string starting at b[pos] and returns the
// offset of its 0x00 terminator and the number of escaped zeros in it.
func escapedEnd(b []byte, pos int) (end, escapes int, err error) {
	for i := pos; ; {
		j := bytes.IndexByte(b[i:], 0)
		if j < 0 {
			return 0, 0, missing(len(b))
		}
		i += j
		if i+1 < len(b) && b[i+1] == escapeCode {
			escapes++
			i += 2
			continue
		}
		return i, escapes, nil
	}
}

func decodeBytes(b []byte, pos int) ([]byte, int, error) {
	end, escapes, err := escapedEnd(b, pos)
	if err != nil {
		return nil, 0, err
	}
	out := make([]byte, 0, end-pos-escapes)
	if escapes == 0 {
		return append(out, b[pos:end]...), end + 1, nil
	}
	for i := pos; i < end; i++ {
		out = append(out, b[i])
		if b[i] == 0 {
			i++
		}
	}
	return out, end + 1, nil
}

func decodeString(b []byte, pos int) (string, int, error) {
	end, escapes, err := escapedEnd(b, pos)
	if err != nil {
		return "", 0, err
	}
	if escapes == 0 {
		return string(b[pos:end]), end + 1, nil
	}
	var sb strings.Builder
	sb.Grow(end - pos - escapes)
	for i := pos; i < end; i++ {
		sb.WriteByte(b[i])
		if b[i] == 0 {
			i++
		}
	}
	return sb.String(), end + 1, nil
}
