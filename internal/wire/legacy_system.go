package wire

import (
	"encoding/binary"

	"github.com/can1357/fdbgo/internal/fdberr"
)

// DecodeServerListValueLegacy reads the IncludeVersion BinaryWriter format
// persisted by SystemData.cpp. Through6.2 the SSI is full;6.3 introduced
// SmallEndpoints and7.0 appended Optional<UID> tssPairID. The protocol prefix
// belongs to the value, not necessarily to the server currently returning it.
func DecodeServerListValueLegacy(value []byte) (StorageServer, error) {
	r := NewBinaryReader(value, 0)
	r.Protocol = ProtocolVersion(r.U64())
	if r.Err() != nil || r.Protocol&0xf000000000000000 != 0 {
		return StorageServer{}, ErrMalformed
	}
	v := r.Protocol & 0x0fffffffffffffff
	var out StorageServer
	if legacyProtocol(v) {
		out = r.StorageServer()
	} else if v >= 0x0fdb00b063010000 && v < 0x0fdb00b071010000 {
		// StorageServerInterface.h6.3:89 and7.0:98.
		out.ID = r.UID()
		out.Zone, out.DC = r.Locality()
		out.Base = r.Endpoint()
		if v >= 0x0fdb00b070010001 {
			r.OptionalUID()
		}
	} else {
		return StorageServer{}, ErrMalformed
	}
	return out, r.Done()
}

// DecodeKeyServersValueLegacy reads the UID-vector source/destination layout
// shared by all six releases (SystemData.cpp::decodeKeyServersValue). Only
// sources are returned; destinations are still parsed and bounds-checked.
func DecodeKeyServersValueLegacy(value []byte) ([]UID, error) {
	if len(value) == 0 {
		return nil, nil
	}
	r := NewBinaryReader(value, 0)
	r.Protocol = ProtocolVersion(r.U64())
	if r.Err() != nil || r.Protocol&0xf000000000000000 != 0 || !legacyProtocol(r.Protocol) {
		return nil, ErrMalformed
	}
	n := r.Count(16)
	out := make([]UID, 0, n)
	for range n {
		out = append(out, r.UID())
	}
	n = r.Count(16)
	r.Raw(n * 16)
	return out, r.Done()
}

// legacyMutation translates API>=520 versionstamp operands to the older
// server representation. Atomic.h5.0/5.1 uses signed16 key offsets and stamps
// values only at byte0 without an offset trailer.5.2+ uses signed32 for both.
// No scratch operand allocation is needed: emit its prefix and replacement
// trailer directly into the reusable writer.
func legacyMutation(w *BinaryWriter, m Mutation) error {
	maxKind := uint8(20)
	if w.Protocol < legacy51 {
		maxKind = 15
	} else if w.Protocol < legacy61 {
		maxKind = 19
	}
	if m.Kind > maxKind {
		return fdberr.UnsupportedOperation
	}
	w.U8(m.Kind)
	if m.Kind == 14 && w.Protocol < legacy52 {
		if len(m.Param1) < 4 {
			return fdberr.ClientInvalidOperation
		}
		offset := int32(binary.LittleEndian.Uint32(m.Param1[len(m.Param1)-4:]))
		if offset < 0 || int64(offset)+10 > int64(len(m.Param1)-4) {
			return fdberr.ClientInvalidOperation
		}
		if offset > 32767 {
			return fdberr.UnsupportedOperation
		}
		w.U32(uint32(len(m.Param1) - 2))
		w.Raw(m.Param1[:len(m.Param1)-4])
		w.U16(uint16(offset))
	} else {
		w.Bytes(m.Param1)
	}
	if m.Kind == 15 && w.Protocol < legacy52 {
		if len(m.Param2) < 14 {
			return fdberr.ClientInvalidOperation
		}
		offset := int32(binary.LittleEndian.Uint32(m.Param2[len(m.Param2)-4:]))
		if offset < 0 || int64(offset)+10 > int64(len(m.Param2)-4) {
			return fdberr.ClientInvalidOperation
		}
		if offset != 0 {
			return fdberr.UnsupportedOperation
		}
		w.Bytes(m.Param2[:len(m.Param2)-4])
	} else {
		w.Bytes(m.Param2)
	}
	return nil
}
