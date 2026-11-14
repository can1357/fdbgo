package wire

import (
	"encoding/binary"
	"math"
	"net/netip"

	"github.com/can1357/fdbgo/internal/fdberr"
	"github.com/can1357/fdbgo/internal/kv"
)

// BinaryWriter implements Flow's unaligned, little-endian BinaryWriter format.
// Protocol is the archive's actual version, without transport flags.
// Scalar writes append directly to the reusable message buffer.
type BinaryWriter struct {
	Protocol ProtocolVersion
	buf      []byte
	err      error
}

// Err reports an encoding that cannot be represented by this protocol.
func (w *BinaryWriter) Err() error { return w.err }

// U8 writes an unsigned byte.
func (w *BinaryWriter) U8(v uint8) { w.buf = append(w.buf, v) }

// U16 writes an unsigned little-endian 16-bit integer.
func (w *BinaryWriter) U16(v uint16) { w.buf = binary.LittleEndian.AppendUint16(w.buf, v) }

// U32 writes an unsigned little-endian 32-bit integer.
func (w *BinaryWriter) U32(v uint32) { w.buf = binary.LittleEndian.AppendUint32(w.buf, v) }

// U64 writes an unsigned little-endian 64-bit integer.
func (w *BinaryWriter) U64(v uint64) { w.buf = binary.LittleEndian.AppendUint64(w.buf, v) }

// I32 writes a signed little-endian 32-bit integer.
func (w *BinaryWriter) I32(v int32) { w.U32(uint32(v)) }

// I64 writes a signed little-endian 64-bit integer.
func (w *BinaryWriter) I64(v int64) { w.U64(uint64(v)) }

// F64 writes an IEEE 754 binary64 value.
func (w *BinaryWriter) F64(v float64) { w.U64(math.Float64bits(v)) }

// Bool writes false as zero and true as one.
func (w *BinaryWriter) Bool(v bool) {
	if v {
		w.U8(1)
	} else {
		w.U8(0)
	}
}

// UID writes the two consecutive 64-bit words of a Flow UID.
func (w *BinaryWriter) UID(v UID) { w.U64(v.First); w.U64(v.Second) }

// Raw writes bytes without a length prefix.
func (w *BinaryWriter) Raw(v []byte) { w.buf = append(w.buf, v...) }

// Bytes writes a uint32 length followed by the byte string.
func (w *BinaryWriter) Bytes(v []byte) { w.U32(uint32(len(v))); w.Raw(v) }

// OptionalBytes writes a presence boolean and, for non-nil values, a byte string.
func (w *BinaryWriter) OptionalBytes(v []byte) {
	w.Bool(v != nil)
	if v != nil {
		w.Bytes(v)
	}
}

// OptionalUID writes a presence boolean and, for valid values, a UID.
func (w *BinaryWriter) OptionalUID(v UID) {
	w.Bool(v.IsValid())
	if v.IsValid() {
		w.UID(v)
	}
}

// KeySelector writes the selector's key, equality flag, and signed offset.
func (w *BinaryWriter) KeySelector(v kv.KeySelector) {
	w.Bytes(v.Key)
	w.Bool(v.OrEqual)
	w.I32(int32(v.Offset))
}

// KeyRange writes the inclusive beginning and exclusive ending byte strings.
func (w *BinaryWriter) KeyRange(v kv.KeyRange) { w.Bytes(v.Begin); w.Bytes(v.End) }

// Release boundaries deliberately omit the compatibility-ignored low16 bits:
// transport may pass either the advertised version (...0001) or normalized
// negotiated version (...0000). Persisted feature versions retain their bits.
const (
	legacy50           ProtocolVersion = 0x0fdb00a550010000
	legacy51           ProtocolVersion = 0x0fdb00a551040000
	legacy52           ProtocolVersion = 0x0fdb00a552000000
	legacy60           ProtocolVersion = 0x0fdb00a570010000
	legacy61           ProtocolVersion = 0x0fdb00b061060000
	legacy62           ProtocolVersion = 0x0fdb00b062010000
	legacyEndpointList ProtocolVersion = 0x0fdb00b061020000
	legacyIPv6         ProtocolVersion = 0x0fdb00b061030000
)

// NetAddr follows flow/network.h: pre-6.1 IPv4 is a raw uint32; IPv6-capable
// archives prepend a bool and encode IPv6 as sixteen network-order bytes.
func (w *BinaryWriter) NetAddr(v NetAddr) {
	a := v.Addr.Addr()
	if a.Is6() && w.Protocol <= legacyIPv6 {
		w.err = fdberr.UnsupportedOperation
		return
	}
	if w.Protocol > legacyIPv6 {
		w.Bool(a.Is6())
	}
	if a.Is6() {
		ip := a.As16()
		w.Raw(ip[:])
	} else {
		var ip [4]byte
		if a.Is4() {
			ip = a.As4()
		}
		w.U32(binary.BigEndian.Uint32(ip[:]))
	}
	w.U16(v.Addr.Port())
	var flags uint16
	if !v.Addr.IsValid() {
		flags = 1
	}
	if v.TLS {
		flags |= 2
	}
	w.U16(flags)
}

// ReplyPromise writes the callback address and token used by fdbrpc.h. Before
// 6.1 a promise is an Endpoint with an unspecified private IPv4 address; the
// receiver substitutes the delivering peer. From 6.1 only the token is sent.
func (w *BinaryWriter) ReplyPromise(reply UID) {
	if w.Protocol < legacy61 {
		w.NetAddr(NetAddr{})
	}
	w.UID(reply)
}

// Endpoint writes version-specific addresses followed by the endpoint token.
func (w *BinaryWriter) Endpoint(v Endpoint) {
	w.NetAddr(v.Addr)
	if w.Protocol > legacyEndpointList {
		w.Bool(v.Alt.Addr.IsValid())
		if v.Alt.Addr.IsValid() {
			w.NetAddr(v.Alt)
		}
	}
	w.UID(v.Token)
}

// BinaryReader is a bounded, zero-copy Flow binary archive. A malformed scalar,
// length, boolean or count sets a sticky error; subsequent reads do not advance.
// Bytes preserves the distinction between absent and present-empty optionals.
type BinaryReader struct {
	Protocol ProtocolVersion
	buf      []byte
	pos      int
	err      error
}

// NewBinaryReader creates a bounded archive that aliases msg.
func NewBinaryReader(msg []byte, protocol ProtocolVersion) *BinaryReader {
	return &BinaryReader{Protocol: protocol & 0x0fffffffffffffff, buf: msg}
}

// Err reports the first malformed field encountered by the reader.
func (r *BinaryReader) Err() error { return r.err }

// Remaining returns the number of unconsumed input bytes.
func (r *BinaryReader) Remaining() int { return len(r.buf) - r.pos }

// Done rejects malformed input and unexpected trailing bytes.
func (r *BinaryReader) Done() error {
	if r.err != nil {
		return r.err
	}
	if r.Remaining() != 0 {
		return ErrMalformed
	}
	return nil
}

// Raw reads exactly n bytes, aliasing the input with bounded slice capacity.
func (r *BinaryReader) Raw(n int) []byte {
	if r.err != nil {
		return nil
	}
	if n < 0 || n > r.Remaining() {
		r.err = ErrMalformed
		return nil
	}
	v := r.buf[r.pos : r.pos+n : r.pos+n]
	r.pos += n
	return v
}

// U8 reads an unsigned byte.
func (r *BinaryReader) U8() uint8 {
	v := r.Raw(1)
	if v == nil {
		return 0
	}
	return v[0]
}

// U16 reads an unsigned little-endian 16-bit integer.
func (r *BinaryReader) U16() uint16 {
	v := r.Raw(2)
	if v == nil {
		return 0
	}
	return binary.LittleEndian.Uint16(v)
}

// U32 reads an unsigned little-endian 32-bit integer.
func (r *BinaryReader) U32() uint32 {
	v := r.Raw(4)
	if v == nil {
		return 0
	}
	return binary.LittleEndian.Uint32(v)
}

// U64 reads an unsigned little-endian 64-bit integer.
func (r *BinaryReader) U64() uint64 {
	v := r.Raw(8)
	if v == nil {
		return 0
	}
	return binary.LittleEndian.Uint64(v)
}

// I32 reads a signed little-endian 32-bit integer.
func (r *BinaryReader) I32() int32 { return int32(r.U32()) }

// I64 reads a signed little-endian 64-bit integer.
func (r *BinaryReader) I64() int64 { return int64(r.U64()) }

// F64 reads an IEEE 754 binary64 value.
func (r *BinaryReader) F64() float64 { return math.Float64frombits(r.U64()) }

// Bool reads a boolean and rejects bytes other than zero and one.
func (r *BinaryReader) Bool() bool {
	v := r.U8()
	if v > 1 {
		r.err = ErrMalformed
	}
	return v == 1
}

// UID reads the two consecutive 64-bit words of a Flow UID.
func (r *BinaryReader) UID() UID { return UID{r.U64(), r.U64()} }

// Bytes reads a uint32-sized byte string without copying it.
func (r *BinaryReader) Bytes() []byte {
	n := r.U32()
	if uint64(n) > uint64(r.Remaining()) {
		r.err = ErrMalformed
		return nil
	}
	return r.Raw(int(n))
}

// OptionalBytes reads an optional string, preserving present-empty values.
func (r *BinaryReader) OptionalBytes() []byte {
	if r.Bool() {
		return r.Bytes()
	}
	return nil
}

// OptionalUID reads an optional UID, returning the zero UID when absent.
func (r *BinaryReader) OptionalUID() UID {
	if r.Bool() {
		return r.UID()
	}
	return UID{}
}

// Count reads a vector count and proves even its smallest entries fit before
// allocating. All callers supply a positive minimum encoded element size.
func (r *BinaryReader) Count(minBytes int) int {
	n := r.U32()
	if minBytes <= 0 || uint64(n) > uint64(r.Remaining()/max(1, minBytes)) {
		r.err = ErrMalformed
		return 0
	}
	return int(n)
}

// KeySelector reads a selector's key, equality flag, and signed offset.
func (r *BinaryReader) KeySelector() kv.KeySelector {
	return kv.KeySelector{Key: r.Bytes(), OrEqual: r.Bool(), Offset: int(r.I32())}
}

// KeyRange reads the inclusive beginning and exclusive ending byte strings.
func (r *BinaryReader) KeyRange() kv.KeyRange { return kv.KeyRange{Begin: r.Bytes(), End: r.Bytes()} }

// NetAddr reads the version-specific IPv4 or IPv6 network address representation.
func (r *BinaryReader) NetAddr() NetAddr {
	var ip netip.Addr
	v6 := r.Protocol > legacyIPv6 && r.Bool()
	if v6 {
		b := r.Raw(16)
		if b != nil {
			ip = netip.AddrFrom16([16]byte(b))
		}
	} else {
		var b [4]byte
		binary.BigEndian.PutUint32(b[:], r.U32())
		ip = netip.AddrFrom4(b)
	}
	port, flags := r.U16(), r.U16()
	if flags & ^uint16(3) != 0 {
		r.err = ErrMalformed
	}
	return NetAddr{Addr: netip.AddrPortFrom(ip, port), TLS: flags&2 != 0}
}

// Endpoint reads version-specific addresses followed by the endpoint token.
func (r *BinaryReader) Endpoint() Endpoint {
	e := Endpoint{Addr: r.NetAddr()}
	if r.Protocol > legacyEndpointList && r.Bool() {
		e.Alt = r.NetAddr()
	}
	e.Token = r.UID()
	return e
}

// Locality reads the uint64-sized map used by LocalityData, not the int32 size
// of std::map (fdbrpc/Locality.h). All selected release protocols have Locality.
func (r *BinaryReader) Locality() (zone, dc []byte) {
	n := r.U64()
	if n > uint64(r.Remaining()/5) {
		r.err = ErrMalformed
		return
	}
	for range int(n) {
		key, value := r.Bytes(), r.OptionalBytes()
		switch string(key) {
		case "zoneid":
			zone = value
		case "dcid":
			dc = value
		}
	}
	return
}

// StorageServer decodes the full (non-compact) interface through 6.2. Request
// streams have independent endpoint tokens; no offset arithmetic is valid.
func (r *BinaryReader) StorageServer() StorageServer {
	s := StorageServer{ID: r.UID()}
	s.Zone, s.DC = r.Locality()
	streams := new(EndpointStreams)
	for i := range 12 {
		ep := r.Endpoint()
		canonical := -1
		switch i {
		case 1:
			canonical = 0
			s.Base = ep
		case 2:
			canonical = 1
		case 3:
			canonical = 2
		case 4:
			canonical = 3
		case 5:
			canonical = 4
		case 11:
			canonical = 10
		}
		if canonical >= 0 {
			streams.Tokens[canonical] = ep.Token
			streams.Present |= 1 << canonical
		}
	}
	s.Base.Streams = streams
	return s
}

// ClientDBInfo follows fdbclient/ClientDBInfo.h through6.1 and
// MasterProxyInterface.h in6.2. Proxy GRV and commit streams are independent.
func (r *BinaryReader) ClientDBInfo() ClientDBInfo {
	var out ClientDBInfo
	n := r.Count(8 + 7*24)
	out.GRV = make([]ProxyEndpoint, 0, n)
	out.Commit = make([]ProxyEndpoint, 0, n)
	for range n {
		r.Locality()
		provisional := false
		if r.Protocol >= legacy61 {
			provisional = r.Bool()
		}
		count := 7
		if r.Protocol >= legacy61 {
			count = 8
		}
		if r.Protocol >= legacy62 {
			count = 9
		}
		streams := new(EndpointStreams)
		var commit, grv Endpoint
		for j := range count {
			ep := r.Endpoint()
			switch j {
			case 0:
				commit = ep
				streams.Tokens[0] = ep.Token
				streams.Present |= 1
			case 1:
				grv = ep
			case 2:
				streams.Tokens[2] = ep.Token
				streams.Present |= 1 << 2
			}
		}
		commit.Streams = streams
		out.Commit = append(out.Commit, ProxyEndpoint{Endpoint: commit, Provisional: provisional})
		out.GRV = append(out.GRV, ProxyEndpoint{Endpoint: grv, Provisional: provisional})
	}
	out.ID = r.UID()
	r.F64()
	r.I64()
	if r.Protocol >= legacy62 {
		out.Forward = r.OptionalBytes()
	}
	return out
}
