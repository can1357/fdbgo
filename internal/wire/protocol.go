package wire

// ProtocolVersion is FoundationDB's wire protocol version, optionally carrying
// transport flags in its high nibble. It is independent of the client API version.
type ProtocolVersion uint64

const (
	// Protocol73 is the protocol shared by FoundationDB 7.3 releases.
	Protocol73 ProtocolVersion = 0x0fdb_00b0_7300_0000
	// Protocol74 is the protocol shared by FoundationDB 7.4 releases.
	Protocol74 ProtocolVersion = 0x0fdb_00b0_7400_0000
)

// Normalized removes transport flags and the low 16 bits, which upstream's
// ProtocolVersion::isCompatible excludes from wire compatibility. Feature-version
// bits remain significant: an unknown protocol is not a supported release merely
// because its major/minor digits match.
func (v ProtocolVersion) Normalized() ProtocolVersion {
	return v & 0x0fff_ffff_ffff_0000
}

// Compatible reports whether v and other use the same wire protocol. It does not
// imply either protocol is implemented by this client; use Supported for that.
func (v ProtocolVersion) Compatible(other ProtocolVersion) bool {
	return v.Normalized() == other.Normalized()
}

// Supported reports whether this client implements v's wire protocol.
func (v ProtocolVersion) Supported() bool {
	switch v.Normalized() {
	case Protocol73, Protocol74:
		return true
	default:
		return false
	}
}
