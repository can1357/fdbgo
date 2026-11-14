package tuple

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"math/big"
	"math/bits"
	"slices"
	"strings"
)

// Type codes from FoundationDB's tuple layer spec (design/tuple.md).
const (
	nilCode          = 0x00
	bytesCode        = 0x01
	stringCode       = 0x02
	nestedCode       = 0x05
	negIntStart      = 0x0b
	intZeroCode      = 0x14
	posIntEnd        = 0x1d
	floatCode        = 0x20
	doubleCode       = 0x21
	falseCode        = 0x26
	trueCode         = 0x27
	uuidCode         = 0x30
	versionstampCode = 0x33
	escapeCode       = 0xff
)

// packer appends encoded elements to buf and records where incomplete
// versionstamps land.
type packer struct {
	buf []byte
	// stamps counts incomplete versionstamps; stampAt is the offset in buf of
	// the last one's transaction version.
	stamps  int
	stampAt int
}

// Pack encodes the tuple. It fails if an element has an unsupported type or
// an integer needs more than 255 bytes.
func (t Tuple) Pack() ([]byte, error) {
	return t.AppendPack(nil)
}

// AppendPack appends the encoding of the tuple to dst and returns the
// extended slice; on error it returns dst unchanged. It grows dst at most
// once for typical tuples.
func (t Tuple) AppendPack(dst []byte) ([]byte, error) {
	p := packer{buf: slices.Grow(dst, t.sizeHint())}
	if err := p.tuple(t, false); err != nil {
		return dst, err
	}
	return p.buf, nil
}

// PackWithVersionstamp encodes prefix followed by the tuple, which must hold
// exactly one incomplete versionstamp, and appends the 4-byte little-endian
// offset of that versionstamp in the key, as SetVersionstampedKey expects
// (API version 520 and later).
func (t Tuple) PackWithVersionstamp(prefix []byte) ([]byte, error) {
	buf := make([]byte, 0, len(prefix)+t.sizeHint()+4)
	p := packer{buf: append(buf, prefix...)}
	if err := p.tuple(t, false); err != nil {
		return nil, err
	}
	switch {
	case p.stamps == 0:
		return nil, ErrNoIncompleteVersionstamp
	case p.stamps > 1:
		return nil, ErrMultipleIncompleteVersionstamps
	case p.stampAt > math.MaxUint32:
		return nil, fmt.Errorf("tuple: versionstamp offset %d does not fit in 32 bits", p.stampAt)
	}
	return binary.LittleEndian.AppendUint32(p.buf, uint32(p.stampAt)), nil
}

// HasIncompleteVersionstamp reports whether the tuple, including nested
// tuples, holds an incomplete versionstamp. It fails with
// ErrMultipleIncompleteVersionstamps if it holds more than one.
func (t Tuple) HasIncompleteVersionstamp() (bool, error) {
	n := countIncomplete(t)
	if n > 1 {
		return true, ErrMultipleIncompleteVersionstamps
	}
	return n == 1, nil
}

func countIncomplete(t Tuple) int {
	n := 0
	for _, e := range t {
		switch e := e.(type) {
		case Versionstamp:
			if !e.IsComplete() {
				n++
			}
		case Tuple:
			n += countIncomplete(e)
		case []any:
			n += countIncomplete(e)
		}
	}
	return n
}

// sizeHint estimates the packed size of t, ignoring escapes.
func (t Tuple) sizeHint() int {
	n := 0
	for _, e := range t {
		switch e := e.(type) {
		case nil, bool:
			n++
		case string:
			n += len(e) + 2
		case []byte:
			n += len(e) + 2
		case float32:
			n += 5
		case UUID:
			n += 17
		case Versionstamp:
			n += 13
		case Tuple:
			n += e.sizeHint() + 2
		case []any:
			n += Tuple(e).sizeHint() + 2
		default:
			n += 9
		}
	}
	return n
}

func (p *packer) tuple(t Tuple, nested bool) error {
	for _, e := range t {
		if err := p.elem(e, nested); err != nil {
			return err
		}
	}
	return nil
}

func (p *packer) elem(e any, nested bool) error {
	switch v := e.(type) {
	case nil:
		if nested {
			p.buf = append(p.buf, nilCode, escapeCode)
		} else {
			p.buf = append(p.buf, nilCode)
		}
	case []byte:
		p.buf = appendEscapedBytes(append(p.buf, bytesCode), v)
	case string:
		p.buf = appendEscapedString(append(p.buf, stringCode), v)
	case int:
		p.int(int64(v))
	case int64:
		p.int(v)
	case int32:
		p.int(int64(v))
	case int16:
		p.int(int64(v))
	case int8:
		p.int(int64(v))
	case uint:
		p.uint(uint64(v))
	case uint64:
		p.uint(v)
	case uint32:
		p.uint(uint64(v))
	case uint16:
		p.uint(uint64(v))
	case uint8:
		p.uint(uint64(v))
	case *big.Int:
		if v == nil {
			return fmt.Errorf("%w: nil *big.Int", ErrUnsupportedType)
		}
		return p.bigInt(v)
	case big.Int:
		return p.bigInt(&v)
	case float32:
		u := math.Float32bits(v)
		if u&(1<<31) != 0 {
			u = ^u
		} else {
			u ^= 1 << 31
		}
		p.buf = binary.BigEndian.AppendUint32(append(p.buf, floatCode), u)
	case float64:
		u := math.Float64bits(v)
		if u&(1<<63) != 0 {
			u = ^u
		} else {
			u ^= 1 << 63
		}
		p.buf = binary.BigEndian.AppendUint64(append(p.buf, doubleCode), u)
	case bool:
		if v {
			p.buf = append(p.buf, trueCode)
		} else {
			p.buf = append(p.buf, falseCode)
		}
	case UUID:
		p.buf = append(append(p.buf, uuidCode), v[:]...)
	case Versionstamp:
		p.buf = append(p.buf, versionstampCode)
		if !v.IsComplete() {
			p.stamps++
			p.stampAt = len(p.buf)
		}
		p.buf = binary.BigEndian.AppendUint16(append(p.buf, v.TransactionVersion[:]...), v.UserVersion)
	case Tuple:
		return p.nested(v)
	case []any:
		return p.nested(v)
	default:
		return fmt.Errorf("%w: %T", ErrUnsupportedType, e)
	}
	return nil
}

func (p *packer) nested(t Tuple) error {
	p.buf = append(p.buf, nestedCode)
	if err := p.tuple(t, true); err != nil {
		return err
	}
	p.buf = append(p.buf, nilCode)
	return nil
}

// appendEscapedBytes appends b with every 0x00 escaped as 0x00 0xff, then the 0x00 terminator.
func appendEscapedBytes(dst, b []byte) []byte {
	for {
		i := bytes.IndexByte(b, 0)
		if i < 0 {
			return append(append(dst, b...), nilCode)
		}
		dst = append(append(dst, b[:i+1]...), escapeCode)
		b = b[i+1:]
	}
}

// appendEscapedString is appendEscapedBytes for a string.
func appendEscapedString(dst []byte, s string) []byte {
	for {
		i := strings.IndexByte(s, 0)
		if i < 0 {
			return append(append(dst, s...), nilCode)
		}
		dst = append(append(dst, s[:i+1]...), escapeCode)
		s = s[i+1:]
	}
}

// appendBE appends the low n bytes of v big-endian.
func appendBE(dst []byte, v uint64, n int) []byte {
	var s [8]byte
	binary.BigEndian.PutUint64(s[:], v)
	return append(dst, s[8-n:]...)
}

func (p *packer) uint(u uint64) {
	n := (bits.Len64(u) + 7) / 8
	p.buf = appendBE(append(p.buf, byte(intZeroCode+n)), u, n)
}

func (p *packer) int(i int64) {
	if i >= 0 {
		p.uint(uint64(i))
		return
	}
	// The magnitude of math.MinInt64 is 1<<63, which -i yields as a uint64.
	mag := uint64(-i)
	n := (bits.Len64(mag) + 7) / 8
	p.buf = appendBE(append(p.buf, byte(intZeroCode-n)), ^mag, n)
}

func (p *packer) bigInt(v *big.Int) error {
	if v.IsInt64() {
		p.int(v.Int64())
		return nil
	}
	if v.Sign() > 0 && v.IsUint64() {
		p.uint(v.Uint64())
		return nil
	}
	n := (v.BitLen() + 7) / 8
	switch {
	case n > 255:
		return ErrIntTooLarge
	case n > 8 && v.Sign() > 0:
		p.buf = append(p.buf, posIntEnd, byte(n))
	case n > 8:
		p.buf = append(p.buf, negIntStart, byte(n)^0xff)
	case v.Sign() > 0:
		p.buf = append(p.buf, byte(intZeroCode+n))
	default:
		p.buf = append(p.buf, byte(intZeroCode-n))
	}
	start := len(p.buf)
	p.buf = append(p.buf, make([]byte, n)...)
	mag := p.buf[start:]
	v.FillBytes(mag) // the absolute value
	if v.Sign() < 0 {
		for i := range mag {
			mag[i] = ^mag[i]
		}
	}
	return nil
}
