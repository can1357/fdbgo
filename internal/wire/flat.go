// Package wire implements Flow's ObjectSerializer wire format and the
// FoundationDB RPC messages the client sends or receives.
//
// The format is FlatBuffers-shaped but schema-less: every server type with a
// serialize() method is a table whose vtable slots are its serializer()
// arguments in order. Union-like members (Optional, ErrorOr, variants)
// take two slots (u8 type tag, 1-based; uoffset to the value), zero-size
// members (Arena) take none, everything else one. Scalars and struct-like
// types live inline; strings, vectors and tables are reached through a forward
// uoffset. Union alternatives that would be inline (scalars) are stored out of
// line behind the uoffset instead.
//
// A message is [u32 uoffset to root][u32 file identifier]... where the root is
// a synthetic table whose slots are the serialized values (one for requests, an
// ErrorOr<EnsureTable<T>> union for replies). Builder emits standard
// back-to-front FlatBuffers.
//
// Decoding is zero-copy: decoded []byte fields alias the message. Present
// empty values decode as non-nil empty slices; nil means absent.
package wire

import (
	"encoding/binary"
	"errors"
	"math"
)

// ErrMalformed is returned when a peer's message does not parse.
var ErrMalformed = errors.New("wire: malformed message")

// Off is the position of a written object, measured from the end of the buffer.
type Off = uint32

// Builder assembles one message back to front so every uoffset points forward.
//
// The finished bytes live at the tail of the buffer; transport framing is
// prepended in place afterwards (see Raw). A Builder is reused across messages
// (callers pool it) and does not allocate once its buffer is large enough.
type Builder struct {
	// Protocol selects the upstream serializer layout. Zero preserves 7.4 codec fixtures.
	Protocol ProtocolVersion
	buf      []byte
	head     int
	minAlign int
	vtable   []Off
	objStart Off
	offs     []Off
}

// Reset clears the builder for a new message, keeping its allocations.
func (b *Builder) Reset() {
	b.head = len(b.buf)
	b.minAlign = 1
	b.offs = b.offs[:0]
}

// Offset returns the bytes written so far, i.e. the position of the most recent object.
func (b *Builder) Offset() Off { return Off(len(b.buf) - b.head) }

// Finished returns the bytes written so far.
func (b *Builder) Finished() []byte { return b.buf[b.head:] }

func (b *Builder) grow(n int) {
	if b.head >= n {
		return
	}
	used := len(b.buf) - b.head
	size := max(2*len(b.buf), used+n, 512)
	next := make([]byte, size)
	copy(next[size-used:], b.buf[b.head:])
	b.buf = next
	b.head = size - used
}

// prep pads so that after writing additional bytes the next size-byte write
// is size-aligned (relative to the end of the buffer).
func (b *Builder) prep(size, additional int) {
	if size > b.minAlign {
		b.minAlign = size
	}
	pad := -(int(b.Offset()) + additional) & (size - 1)
	b.grow(pad + size + additional)
	b.head -= pad
	clear(b.buf[b.head : b.head+pad])
}

// Raw reserves n unaligned bytes in front of everything written so far.
func (b *Builder) Raw(n int) []byte {
	b.grow(n)
	b.head -= n
	return b.buf[b.head : b.head+n]
}

func (b *Builder) put8(v uint8) {
	b.prep(1, 0)
	b.head--
	b.buf[b.head] = v
}

func (b *Builder) put16(v uint16) {
	b.prep(2, 0)
	b.head -= 2
	binary.LittleEndian.PutUint16(b.buf[b.head:], v)
}

func (b *Builder) put32(v uint32) {
	b.prep(4, 0)
	b.head -= 4
	binary.LittleEndian.PutUint32(b.buf[b.head:], v)
}

func (b *Builder) put64(v uint64) {
	b.prep(8, 0)
	b.head -= 8
	binary.LittleEndian.PutUint64(b.buf[b.head:], v)
}

// uoffset writes a forward reference to the object at target.
func (b *Builder) uoffset(target Off) {
	b.prep(4, 0)
	b.put32(b.Offset() + 4 - target)
}

// Bytes writes a length-prefixed byte string (StringRef, Key, Value).
func (b *Builder) Bytes(v []byte) Off {
	b.prep(4, len(v))
	copy(b.Raw(len(v)), v)
	b.put32(uint32(len(v)))
	return b.Offset()
}

// String writes s as a length-prefixed byte string without converting it to []byte.
func (b *Builder) String(s string) Off {
	b.prep(4, len(s))
	copy(b.Raw(len(s)), s)
	b.put32(uint32(len(s)))
	return b.Offset()
}

// Blob reserves a dynamic-size blob of n bytes for custom dynamic_size_traits
// encodings (TagSet); the caller fills the returned slice before writing anything else.
func (b *Builder) Blob(n int) (Off, []byte) {
	b.prep(4, n)
	dst := b.Raw(n)
	b.put32(uint32(n))
	// put32 never moves earlier bytes: prep(4, n) reserved room for both.
	return b.Offset(), dst
}

// Push records off on the builder's scratch stack, used to build vectors of
// offsets without allocating. Returns nothing; see Mark and Offsets.
func (b *Builder) Push(off Off) { b.offs = append(b.offs, off) }

// Mark returns the current scratch stack depth, to pass to Offsets later.
func (b *Builder) Mark() int { return len(b.offs) }

// Offsets writes a vector of uoffsets to the objects pushed since mark and pops them.
func (b *Builder) Offsets(mark int) Off {
	offs := b.offs[mark:]
	b.prep(4, 4*len(offs))
	for i := len(offs) - 1; i >= 0; i-- {
		b.uoffset(offs[i])
	}
	b.put32(uint32(len(offs)))
	b.offs = b.offs[:mark]
	return b.Offset()
}

// ScalarU32 writes a free-standing u32: the out-of-line value of a scalar union alternative.
func (b *Builder) ScalarU32(v uint32) Off {
	b.put32(v)
	return b.Offset()
}

// ScalarI64 writes a free-standing i64: the out-of-line value of a scalar union alternative.
func (b *Builder) ScalarI64(v int64) Off {
	b.put64(uint64(v))
	return b.Offset()
}

// StartTable starts a table with slots vtable entries; children must already be written.
func (b *Builder) StartTable(slots int) {
	if cap(b.vtable) < slots {
		b.vtable = make([]Off, slots, max(slots, 16))
	} else {
		b.vtable = b.vtable[:slots]
		clear(b.vtable)
	}
	b.objStart = b.Offset()
}

func (b *Builder) mark(slot int) { b.vtable[slot] = b.Offset() }

// AddBool sets slot to a bool.
func (b *Builder) AddBool(slot int, v bool) {
	var x uint8
	if v {
		x = 1
	}
	b.put8(x)
	b.mark(slot)
}

// AddU8 sets slot to a u8.
func (b *Builder) AddU8(slot int, v uint8) { b.put8(v); b.mark(slot) }

// AddU16 sets slot to a u16.
func (b *Builder) AddU16(slot int, v uint16) { b.put16(v); b.mark(slot) }

// AddU32 sets slot to a u32.
func (b *Builder) AddU32(slot int, v uint32) { b.put32(v); b.mark(slot) }

// AddI32 sets slot to an i32.
func (b *Builder) AddI32(slot int, v int32) { b.put32(uint32(v)); b.mark(slot) }

// AddI64 sets slot to an i64.
func (b *Builder) AddI64(slot int, v int64) { b.put64(uint64(v)); b.mark(slot) }

// AddF64 sets slot to an f64.
func (b *Builder) AddF64(slot int, v float64) { b.put64(math.Float64bits(v)); b.mark(slot) }

// AddUID writes a UID inline (scalar of 16 bytes, 8-aligned).
func (b *Builder) AddUID(slot int, u UID) {
	b.ScalarUID(u)
	b.mark(slot)
}

// ScalarUID writes an out-of-line UID for an Optional<UID> alternative.
func (b *Builder) ScalarUID(u UID) Off {
	b.prep(8, 8)
	b.put64(u.Second)
	b.put64(u.First)
	return b.Offset()
}

// AddOffset points slot at an already-written object.
func (b *Builder) AddOffset(slot int, target Off) {
	b.uoffset(target)
	b.mark(slot)
}

// AddUnion sets the union slot pair at slot to alternative tag (1-based) stored at target.
func (b *Builder) AddUnion(slot int, tag uint8, target Off) {
	b.AddOffset(slot+1, target)
	b.AddU8(slot, tag)
}

// EndTable finishes the current table and returns its position.
func (b *Builder) EndTable() Off {
	b.prep(4, 0)
	b.head -= 4 // soffset to the vtable, patched below
	obj := b.Offset()
	n := len(b.vtable)
	for n > 0 && b.vtable[n-1] == 0 {
		n--
	}
	for i := n - 1; i >= 0; i-- {
		p := b.vtable[i]
		var off uint16
		if p != 0 {
			off = uint16(obj - p)
		}
		b.put16(off)
	}
	b.put16(uint16(obj - b.objStart))
	b.put16(uint16(2 * (n + 2)))
	at := len(b.buf) - int(obj)
	binary.LittleEndian.PutUint32(b.buf[at:], uint32(int32(b.Offset())-int32(obj)))
	return obj
}

// EmptyTable writes an empty table (e.g. Void).
func (b *Builder) EmptyTable() Off {
	b.StartTable(0)
	return b.EndTable()
}

// FinishRequest wraps msg in the one-slot root table and writes the message header.
func (b *Builder) FinishRequest(msg Off, fileID uint32) []byte {
	b.StartTable(1)
	b.AddOffset(0, msg)
	root := b.EndTable()
	return b.Finish(root, fileID)
}

// Finish writes the message header pointing at root and returns the message.
func (b *Builder) Finish(root Off, fileID uint32) []byte {
	b.prep(max(b.minAlign, 8), 8)
	b.put32(fileID)
	b.uoffset(root)
	return b.Finished()
}

// UID is Flow's 128-bit identifier; endpoint tokens are UIDs.
type UID struct{ First, Second uint64 }

// IsValid reports whether u is non-zero.
func (u UID) IsValid() bool { return u.First != 0 || u.Second != 0 }

// String renders u as 32 lowercase hex digits.
func (u UID) String() string {
	const hex = "0123456789abcdef"
	var out [32]byte
	for i := range 16 {
		out[15-i] = hex[(u.First>>(4*i))&15]
		out[31-i] = hex[(u.Second>>(4*i))&15]
	}
	return string(out[:])
}

func le16(b []byte, p int) (uint16, error) {
	if p < 0 || p > len(b)-2 {
		return 0, ErrMalformed
	}
	return binary.LittleEndian.Uint16(b[p:]), nil
}

func le32(b []byte, p int) (uint32, error) {
	if p < 0 || p > len(b)-4 {
		return 0, ErrMalformed
	}
	return binary.LittleEndian.Uint32(b[p:]), nil
}

func le64(b []byte, p int) (uint64, error) {
	if p < 0 || p > len(b)-8 {
		return 0, ErrMalformed
	}
	return binary.LittleEndian.Uint64(b[p:]), nil
}

// deref follows the uoffset stored at p.
func deref(b []byte, p int) (int, error) {
	o, err := le32(b, p)
	if err != nil {
		return 0, err
	}
	t := p + int(o)
	if t >= len(b) {
		return 0, ErrMalformed
	}
	return t, nil
}

// vecBytes reads the length-prefixed byte string at p, aliasing msg.
func vecBytes(msg []byte, p int) ([]byte, error) {
	n, err := le32(msg, p)
	if err != nil {
		return nil, err
	}
	end := p + 4 + int(n)
	if end > len(msg) {
		return nil, ErrMalformed
	}
	return msg[p+4 : end : end], nil
}

// Table is a read-only view of a table inside a message.
type Table struct {
	// Protocol is inherited by nested tables, fields, and vectors.
	Protocol ProtocolVersion
	msg      []byte
	pos, vt  int
	nvt      int
}

func tableAt(msg []byte, pos int) (Table, error) {
	s, err := le32(msg, pos)
	if err != nil {
		return Table{}, err
	}
	vt := pos - int(int32(s))
	if vt < 0 {
		return Table{}, ErrMalformed
	}
	n, err := le16(msg, vt)
	if err != nil {
		return Table{}, err
	}
	nvt := int(n) / 2
	if vt+2*nvt > len(msg) {
		return Table{}, ErrMalformed
	}
	return Table{msg: msg, pos: pos, vt: vt, nvt: nvt}, nil
}

// Root returns the synthetic root table of a message.
func Root(msg []byte, protocol ...ProtocolVersion) (Table, error) {
	p, err := le32(msg, 0)
	if err != nil {
		return Table{}, err
	}
	t, err := tableAt(msg, int(p))
	if len(protocol) != 0 {
		t.Protocol = protocol[0]
	}
	return t, err
}

// field returns the position of slot's value, or -1 when absent.
func (t Table) field(slot int) int {
	i := slot + 2
	if i >= t.nvt {
		return -1
	}
	off := binary.LittleEndian.Uint16(t.msg[t.vt+2*i:])
	if off < 4 {
		return -1
	}
	return t.pos + int(off)
}

// U8 reads a u8 slot (0 when absent).
func (t Table) U8(slot int) (uint8, error) {
	p := t.field(slot)
	if p < 0 {
		return 0, nil
	}
	if p >= len(t.msg) {
		return 0, ErrMalformed
	}
	return t.msg[p], nil
}

// Bool reads a bool slot (false when absent).
func (t Table) Bool(slot int) (bool, error) {
	v, err := t.U8(slot)
	return v != 0, err
}

// U16 reads a u16 slot (0 when absent).
func (t Table) U16(slot int) (uint16, error) {
	p := t.field(slot)
	if p < 0 {
		return 0, nil
	}
	return le16(t.msg, p)
}

// U32 reads a u32 slot (0 when absent).
func (t Table) U32(slot int) (uint32, error) {
	p := t.field(slot)
	if p < 0 {
		return 0, nil
	}
	return le32(t.msg, p)
}

// I32 reads an i32 slot (0 when absent).
func (t Table) I32(slot int) (int32, error) {
	v, err := t.U32(slot)
	return int32(v), err
}

// U64 reads a u64 slot (0 when absent).
func (t Table) U64(slot int) (uint64, error) {
	p := t.field(slot)
	if p < 0 {
		return 0, nil
	}
	return le64(t.msg, p)
}

// I64 reads an i64 slot (0 when absent).
func (t Table) I64(slot int) (int64, error) {
	v, err := t.U64(slot)
	return int64(v), err
}

// F64 reads an f64 slot (0 when absent).
func (t Table) F64(slot int) (float64, error) {
	v, err := t.U64(slot)
	return math.Float64frombits(v), err
}

// UID reads an inline UID slot (zero when absent).
func (t Table) UID(slot int) (UID, error) {
	p := t.field(slot)
	if p < 0 {
		return UID{}, nil
	}
	a, err := le64(t.msg, p)
	if err != nil {
		return UID{}, err
	}
	b, err := le64(t.msg, p+8)
	return UID{a, b}, err
}

// Bytes reads a byte-string slot aliasing the message; nil when absent.
func (t Table) Bytes(slot int) ([]byte, error) {
	p := t.field(slot)
	if p < 0 {
		return nil, nil
	}
	p, err := deref(t.msg, p)
	if err != nil {
		return nil, err
	}
	return vecBytes(t.msg, p)
}

// bytesOrEmpty reads a byte-string slot, returning a non-nil empty slice when absent.
func (t Table) bytesOrEmpty(slot int) ([]byte, error) {
	v, err := t.Bytes(slot)
	if v == nil && err == nil {
		v = t.msg[:0:0]
	}
	return v, err
}

// Table reads a table slot; ok is false when absent.
func (t Table) Table(slot int) (Table, bool, error) {
	p := t.field(slot)
	if p < 0 {
		return Table{}, false, nil
	}
	p, err := deref(t.msg, p)
	if err != nil {
		return Table{}, false, err
	}
	sub, err := tableAt(t.msg, p)
	sub.Protocol = t.Protocol
	return sub, err == nil, err
}

// mustTable reads a table slot that must be present.
func (t Table) mustTable(slot int) (Table, error) {
	sub, ok, err := t.Table(slot)
	if err == nil && !ok {
		err = ErrMalformed
	}
	return sub, err
}

// Vector reads a vector slot; empty when absent.
func (t Table) Vector(slot int) (Vector, error) {
	p := t.field(slot)
	if p < 0 {
		return Vector{msg: t.msg, Protocol: t.Protocol}, nil
	}
	return Field{msg: t.msg, pos: p, Protocol: t.Protocol}.Vector()
}

// Union returns the 1-based alternative of the union slot pair at slot and the
// uoffset field of its value; tag 0 when the union is empty.
func (t Table) Union(slot int) (uint8, Field, error) {
	tag, err := t.U8(slot)
	if err != nil {
		return 0, Field{}, err
	}
	p := t.field(slot + 1)
	if tag == 0 || p < 0 {
		return 0, Field{}, nil
	}
	return tag, Field{msg: t.msg, pos: p, Protocol: t.Protocol}, nil
}

// Field is the uoffset field holding a union alternative's value.
type Field struct {
	Protocol ProtocolVersion
	msg      []byte
	pos      int
}

// Table reads the alternative as a table.
func (f Field) Table() (Table, error) {
	p, err := deref(f.msg, f.pos)
	if err != nil {
		return Table{}, err
	}
	t, err := tableAt(f.msg, p)
	t.Protocol = f.Protocol
	return t, err
}

// Bytes reads the alternative as a byte string aliasing the message.
func (f Field) Bytes() ([]byte, error) {
	p, err := deref(f.msg, f.pos)
	if err != nil {
		return nil, err
	}
	return vecBytes(f.msg, p)
}

// U32 reads the alternative as an out-of-line u32.
func (f Field) U32() (uint32, error) {
	p, err := deref(f.msg, f.pos)
	if err != nil {
		return 0, err
	}
	return le32(f.msg, p)
}

// I64 reads the alternative as an out-of-line i64.
func (f Field) I64() (int64, error) {
	p, err := deref(f.msg, f.pos)
	if err != nil {
		return 0, err
	}
	v, err := le64(f.msg, p)
	return int64(v), err
}

// Vector reads the alternative as a vector.
func (f Field) Vector() (Vector, error) {
	p, err := deref(f.msg, f.pos)
	if err != nil {
		return Vector{}, err
	}
	n, err := le32(f.msg, p)
	if err != nil {
		return Vector{}, err
	}
	return Vector{msg: f.msg, start: p + 4, n: int(n), Protocol: f.Protocol}, nil
}

// Vector is a read-only view of a vector inside a message.
type Vector struct {
	Protocol ProtocolVersion
	msg      []byte
	start    int
	n        int
}

// Len returns the element count claimed by the message.
func (v Vector) Len() int { return v.n }

// capHint bounds a preallocation by what the message can actually hold.
func (v Vector) capHint() int {
	return min(v.n, max(len(v.msg)-v.start, 0)/4)
}

// Table returns element i of a vector of tables.
func (v Vector) Table(i int) (Table, error) {
	p, err := deref(v.msg, v.start+4*i)
	if err != nil {
		return Table{}, err
	}
	t, err := tableAt(v.msg, p)
	t.Protocol = v.Protocol
	return t, err
}

// Bytes returns element i of a vector of byte strings.
func (v Vector) Bytes(i int) ([]byte, error) {
	p, err := deref(v.msg, v.start+4*i)
	if err != nil {
		return nil, err
	}
	return vecBytes(v.msg, p)
}

// I32 returns element i of a vector of i32 scalars.
func (v Vector) I32(i int) (int32, error) {
	x, err := le32(v.msg, v.start+4*i)
	return int32(x), err
}

// AsBytes returns the raw element bytes of a vector of u8 scalars.
func (v Vector) AsBytes() ([]byte, error) {
	end := v.start + v.n
	if v.start > len(v.msg) || end > len(v.msg) {
		return nil, ErrMalformed
	}
	return v.msg[v.start:end:end], nil
}
