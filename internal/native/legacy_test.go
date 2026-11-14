package native

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/can1357/fdbgo/internal/wire"
)

func TestLegacyGetLeaderReplyPromise(t *testing.T) {
	// fdbrpc.h before 6.1 serializes ReplyPromise as a private/invalid
	// IPv4 Endpoint followed by the token. 6.1 serializes only the token.
	const fields = "08000000746573743a6b657900000000000000000000000000000000"
	const endpoint = "0000000000000100"
	const token = "01000000000000000200000000000000"
	for _, protocol := range []wire.ProtocolVersion{wire.Protocol50, wire.Protocol51, wire.Protocol52, wire.Protocol60, wire.Protocol61} {
		wantHex := fields + token
		if protocol.Normalized() < wire.Protocol61.Normalized() {
			wantHex = fields + endpoint + token
		}
		want, err := hex.DecodeString(wantHex)
		if err != nil {
			t.Fatal(err)
		}
		var b wire.Builder
		if err := wire.EncodeLegacy(&b, getLeaderReq{key: []byte("test:key")}, wire.UID{First: 1, Second: 2}, protocol); err != nil {
			t.Fatal(err)
		}
		if got := b.Finished(); !bytes.Equal(got, want) {
			t.Fatalf("protocol %#x GetLeader = %x; want %x", protocol, got, want)
		}
	}
}
