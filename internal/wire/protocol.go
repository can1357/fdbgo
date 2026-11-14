package wire

// ProtocolVersion is FoundationDB's wire protocol version, optionally carrying
// transport flags in its high nibble. It is independent of the client API version.
type ProtocolVersion uint64

const (
	// Protocol50 is the protocol of FoundationDB 5.0.8.
	Protocol50 ProtocolVersion = 0x0fdb_00a5_5001_0001
	// Protocol51 is the protocol of FoundationDB 5.1.7.
	Protocol51 ProtocolVersion = 0x0fdb_00a5_5104_0001
	// Protocol52 is the protocol of FoundationDB 5.2.8.
	Protocol52 ProtocolVersion = 0x0fdb_00a5_5200_0001
	// Protocol60 is the protocol of FoundationDB 6.0.18.
	Protocol60 ProtocolVersion = 0x0fdb_00a5_7001_0001
	// Protocol61 is the protocol of FoundationDB 6.1.13.
	Protocol61 ProtocolVersion = 0x0fdb_00b0_6106_0001
	// Protocol62 is the protocol of FoundationDB 6.2.30.
	Protocol62 ProtocolVersion = 0x0fdb_00b0_6201_0001
	// Protocol63 is the protocol of FoundationDB 6.3.25.
	Protocol63 ProtocolVersion = 0x0fdb_00b0_6301_0001
	// Protocol70 is the protocol of FoundationDB 7.0.0.
	Protocol70 ProtocolVersion = 0x0fdb_00b0_7001_0001
	// Protocol71 is the protocol of FoundationDB 7.1.67.
	Protocol71 ProtocolVersion = 0x0fdb_00b0_7101_0000
	// Protocol72 is the protocol of FoundationDB 7.2.9.
	Protocol72 ProtocolVersion = 0x0fdb_00b0_7200_0000
	// Protocol73 is the protocol shared by FoundationDB 7.3 releases.
	Protocol73 ProtocolVersion = 0x0fdb_00b0_7300_0000
	// Protocol74 is the protocol shared by FoundationDB 7.4 releases.
	Protocol74 ProtocolVersion = 0x0fdb_00b0_7400_0000
	// Protocol80 is the protocol of FoundationDB 8.0.0.
	Protocol80 ProtocolVersion = 0x0fdb_00b0_8000_0000

	protocolMask ProtocolVersion = 0x0fff_ffff_ffff_0000
)

// Normalized removes transport flags and the low 16 bits, which upstream's
// ProtocolVersion::isCompatible excludes from wire compatibility. Feature-version
// bits remain significant: an unknown protocol is not a supported release merely
// because its major/minor digits match.
func (v ProtocolVersion) Normalized() ProtocolVersion {
	return v & protocolMask
}

// Compatible reports whether v and other use the same wire protocol. It does not
// imply either protocol is implemented by this client; use Supported for that.
func (v ProtocolVersion) Compatible(other ProtocolVersion) bool {
	return v.Normalized() == other.Normalized()
}

// UsesObjectSerializer reports the release's default RPC encoding. FoundationDB
// 6.2 can override this default; transport retains the peer's actual flag.
func (v ProtocolVersion) UsesObjectSerializer() bool {
	return v.Normalized() >= Protocol62.Normalized()
}

// HasIPv6 reports whether ConnectPacket and NetworkAddress include IPv6 fields.
func (v ProtocolVersion) HasIPv6() bool {
	return v.Normalized() >= 0x0fdb_00b0_6103_0000
}

// Supported reports whether this client implements v's wire protocol.
func (v ProtocolVersion) Supported() bool {
	switch v.Normalized() {
	case Protocol50 & protocolMask, Protocol51 & protocolMask, Protocol52 & protocolMask,
		Protocol60 & protocolMask, Protocol61 & protocolMask, Protocol62 & protocolMask,
		Protocol63 & protocolMask, Protocol70 & protocolMask, Protocol71 & protocolMask,
		Protocol72, Protocol73, Protocol74, Protocol80:
		return true
	default:
		return false
	}
}
