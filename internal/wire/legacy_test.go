package wire

import (
	"bytes"
	"encoding/binary"
	"errors"
	"net/netip"
	"testing"

	"github.com/can1357/fdbgo/internal/fdberr"
	"github.com/can1357/fdbgo/internal/kv"
)

func binaryReply(protocol ProtocolVersion, write func(*BinaryWriter)) []byte {
	w := BinaryWriter{Protocol: protocol}
	w.Bool(true)
	write(&w)
	return w.buf
}

func TestLegacyGetValueBoundsAndOptionals(t *testing.T) {
	for _, protocol := range []ProtocolVersion{legacy50, legacy51, legacy52, legacy60, legacy61, legacy62} {
		for _, value := range [][]byte{nil, {}, []byte("value")} {
			msg := binaryReply(protocol, func(w *BinaryWriter) {
				w.F64(1.25)
				if protocol >= legacy62 {
					w.Bool(false)
				}
				w.OptionalBytes(value)
			})
			got, err := DecodeLegacy(&GetValue{}, msg, protocol)
			if err != nil || got.Penalty != 1.25 || !bytes.Equal(got.Value, value) || (got.Value == nil) != (value == nil) {
				t.Fatalf("%x value=%v got=%+v err=%v", protocol, value, got, err)
			}
			for n := range len(msg) {
				if _, err := DecodeLegacy(&GetValue{}, msg[:n], protocol); !errors.Is(err, ErrMalformed) {
					t.Fatalf("%x truncation%d: %v", protocol, n, err)
				}
			}
			if _, err := DecodeLegacy(&GetValue{}, append(msg, 0), protocol); !errors.Is(err, ErrMalformed) {
				t.Fatalf("trailing bytes: %v", err)
			}
		}
	}
	if _, err := DecodeLegacy(Ping{}, []byte{2}, legacy50); !errors.Is(err, ErrMalformed) {
		t.Fatal(err)
	}
	if _, err := DecodeLegacy(Ping{}, []byte{0, 0xef, 3}, legacy50); !errors.Is(err, fdberr.Error(1007)) {
		t.Fatal(err)
	}
	for _, msg := range [][]byte{{0}, {0, 0xff, 0xff}, {0, 0, 0}, {1, 0}} {
		if _, err := DecodeLegacy(Ping{}, msg, legacy50); !errors.Is(err, ErrMalformed) {
			t.Fatalf("%x: %v", msg, err)
		}
	}
}

func TestLegacyRangeReplyAndCountBounds(t *testing.T) {
	for _, protocol := range []ProtocolVersion{legacy50, legacy62} {
		msg := binaryReply(protocol, func(w *BinaryWriter) {
			w.F64(2)
			if protocol >= legacy62 {
				w.Bool(false)
			}
			w.U32(2)
			w.Bytes([]byte("a"))
			w.Bytes(nil)
			w.Bytes([]byte("b"))
			w.Bytes([]byte("v"))
			w.I64(99)
			w.Bool(true)
		})
		got, err := DecodeLegacy(&GetKeyValues{}, msg, protocol)
		if err != nil || !got.Value.More || len(got.Value.Data) != 2 || string(got.Value.Data[1].Value) != "v" || got.Value.Data[0].Value == nil {
			t.Fatalf("%+v %v", got, err)
		}
		for n := range len(msg) {
			if _, err := DecodeLegacy(&GetKeyValues{}, msg[:n], protocol); !errors.Is(err, ErrMalformed) {
				t.Fatalf("truncated at%d: %v", n, err)
			}
		}
		bad := binaryReply(protocol, func(w *BinaryWriter) {
			w.F64(1)
			if protocol >= legacy62 {
				w.Bool(false)
			}
			w.U32(0xffffffff)
		})
		if _, err := DecodeLegacy(&GetKeyValues{}, bad, protocol); !errors.Is(err, ErrMalformed) {
			t.Fatal(err)
		}
	}
	msg := binaryReply(legacy62, func(w *BinaryWriter) { w.F64(1); w.Bool(true); w.U16(1009); w.OptionalBytes(nil) })
	got, err := DecodeLegacy(&GetValue{}, msg, legacy62)
	if err != nil || got.Err != fdberr.Error(1009) {
		t.Fatalf("%+v %v", got, err)
	}
}

func TestLegacyReadMetadataBranches(t *testing.T) {
	for _, protocol := range []ProtocolVersion{legacy50, legacy51, legacy52, legacy60, legacy61, legacy62} {
		msg := binaryReply(protocol, func(w *BinaryWriter) {
			w.I64(123)
			w.Bool(true)
			if protocol >= legacy61 {
				w.OptionalBytes([]byte("meta"))
			}
		})
		got, err := DecodeLegacy(GetReadVersion{}, msg, protocol)
		if err != nil || got.Version != 123 || !got.Locked || (protocol >= legacy61 && string(got.MetadataVersion) != "meta") {
			t.Fatalf("%x %+v %v", protocol, got, err)
		}
		msg = binaryReply(protocol, func(w *BinaryWriter) {
			w.I64(456)
			w.U16(7)
			if protocol >= legacy61 {
				w.OptionalBytes(nil)
			}
		})
		commit, err := DecodeLegacy(&Commit{}, msg, protocol)
		if err != nil || commit.Version != 456 || commit.BatchIndex != 7 {
			t.Fatalf("%+v %v", commit, err)
		}
	}
}

func TestLegacyRequestGolden(t *testing.T) {
	var b Builder
	reply := UID{1, 2}
	if err := EncodeLegacy(&b, &GetValue{Key: []byte("k"), Ctx: ReadCtx{Version: 9}}, reply, legacy50); err != nil {
		t.Fatal(err)
	}
	// key uint32 length+bytes, version int64, absent debug bool, then
	// fdbrpc.h's pre-6.1 ReplyPromise: private unspecified address + reply UID.
	want := []byte{
		1, 0, 0, 0, 'k',
		9, 0, 0, 0, 0, 0, 0, 0,
		0,
		0, 0, 0, 0, 0, 0, 1, 0,
		1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0,
	}
	if !bytes.Equal(b.Finished(), want) {
		t.Fatalf("%x != %x", b.Finished(), want)
	}
	for _, protocol := range []ProtocolVersion{legacy50, legacy51, legacy52, legacy60, legacy61, legacy62} {
		req := &GetKeyValues{Begin: kv.FirstGreaterOrEqual([]byte("a")), End: kv.FirstGreaterOrEqual([]byte("z")), Limit: -3, LimitBytes: 100, Ctx: ReadCtx{Version: 12}}
		if err := EncodeLegacy(&b, req, reply, protocol); err != nil {
			t.Fatal(err)
		}
		r := NewBinaryReader(b.Finished(), protocol)
		begin, end := r.KeySelector(), r.KeySelector()
		if string(begin.Key) != "a" || string(end.Key) != "z" || r.I64() != 12 || r.I32() != -3 || r.I32() != 100 {
			t.Fatal("range fields")
		}
		if protocol >= legacy62 && r.Bool() {
			t.Fatal("isFetchKeys")
		}
		if r.Bool() {
			t.Fatal("range debug ID")
		}
		legacyTestReplyPromise(t, r, reply)
		if r.Done() != nil {
			t.Fatal("range request tail")
		}
	}
	if err := EncodeLegacy(&b, &SplitRange{}, reply, legacy62); !errors.Is(err, fdberr.UnsupportedOperation) {
		t.Fatal(err)
	}
	if err := EncodeLegacy(&b, &GetMappedKeyValues{}, reply, legacy50); !errors.Is(err, fdberr.UnsupportedOperation) {
		t.Fatal(err)
	}
	if err := EncodeLegacy(&b, GetReadVersion{Tags: []TagCount{{Tag: []byte("t"), Count: 1}}}, reply, legacy62); !errors.Is(err, fdberr.UnsupportedOperation) {
		t.Fatal(err)
	}
}

// The old address bytes come from NetworkAddress's private default constructor,
// not from the encoder under test.
func legacyTestReplyPromise(t *testing.T, r *BinaryReader, want UID) {
	t.Helper()
	if r.Protocol < legacy61 {
		if got := r.Raw(8); !bytes.Equal(got, []byte{0, 0, 0, 0, 0, 0, 1, 0}) {
			t.Fatalf("reply address: %x", got)
		}
	}
	if got := r.UID(); got != want {
		t.Fatalf("reply token: %+v != %+v", got, want)
	}
}

func TestLegacyReplyPromiseReleaseGolden(t *testing.T) {
	reply := UID{0x0807060504030201, 0x100f0e0d0c0b0a09}
	// 5.1.7/5.2.8/6.0.18 fdbrpc.h saves ReplyPromise as the entire
	// Endpoint. 6.1.13/6.2.30 saves only its token. PingReceiver reads the
	// promise directly, so the payload is an independent wire-format golden.
	endpoint := []byte{
		0, 0, 0, 0, 0, 0, 1, 0,
		1, 2, 3, 4, 5, 6, 7, 8,
		9, 10, 11, 12, 13, 14, 15, 16,
	}
	for _, protocol := range []ProtocolVersion{legacy50, legacy51, legacy52, legacy60, legacy61, legacy62} {
		want := endpoint
		if protocol >= legacy61 {
			want = endpoint[8:]
		}
		var b Builder
		if err := EncodeLegacy(&b, Ping{}, reply, protocol); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(b.Finished(), want) {
			t.Fatalf("%x ping payload: %x != %x", protocol, b.Finished(), want)
		}
		token, err := PingReplyTokenLegacy(want, protocol)
		if err != nil || token != reply {
			t.Fatalf("%x ping token: %+v %v", protocol, token, err)
		}
		for n := range len(want) {
			if _, err := PingReplyTokenLegacy(want[:n], protocol); !errors.Is(err, ErrMalformed) {
				t.Fatalf("%x ping truncation%d: %v", protocol, n, err)
			}
		}
		if _, err := PingReplyTokenLegacy(append(bytes.Clone(want), 0), protocol); !errors.Is(err, ErrMalformed) {
			t.Fatalf("%x ping trailing bytes: %v", protocol, err)
		}
	}
}

func legacyTestAddress() NetAddr { return NetAddr{Addr: netip.MustParseAddrPort("127.0.0.1:4500")} }
func legacyTestSSI(w *BinaryWriter, id UID) {
	w.UID(id)
	w.U64(2)
	w.Bytes([]byte("zoneid"))
	w.OptionalBytes([]byte("zone"))
	w.Bytes([]byte("dcid"))
	w.OptionalBytes([]byte("dc"))
	for i := range 12 {
		w.Endpoint(Endpoint{Addr: legacyTestAddress(), Token: UID{uint64(i + 100), uint64(i + 200)}})
	}
}

func TestLegacyIndependentStorageEndpoints(t *testing.T) {
	for _, protocol := range []ProtocolVersion{legacy50, legacy61, legacy62} {
		w := BinaryWriter{Protocol: protocol}
		w.U64(uint64(protocol))
		legacyTestSSI(&w, UID{4, 5})
		got, err := DecodeServerListValue(w.buf)
		if err != nil || got.ID != (UID{4, 5}) || string(got.Zone) != "zone" || string(got.DC) != "dc" {
			t.Fatalf("%+v %v", got, err)
		}
		for canonical, index := range map[uint32]uint64{0: 1, 1: 2, 2: 3, 3: 4, 4: 5, 10: 11} {
			if ep := got.Base.Adjusted(canonical); ep.Token != (UID{index + 100, index + 200}) {
				t.Fatalf("canonical%d: %+v", canonical, ep)
			}
		}
		for n := range len(w.buf) {
			if _, err := DecodeServerListValue(w.buf[:n]); !errors.Is(err, ErrMalformed) {
				t.Fatalf("%x truncation%d: %v", protocol, n, err)
			}
		}
	}
}

func TestLegacyIPv6AndControllerPrefix(t *testing.T) {
	for _, protocol := range []ProtocolVersion{legacy50, legacy61} {
		w := BinaryWriter{Protocol: protocol}
		w.U64(uint64(protocol))
		address := legacyTestAddress()
		if protocol >= legacy61 {
			address = NetAddr{Addr: netip.MustParseAddrPort("[::1]:4500"), TLS: true}
		}
		for i := range 3 {
			w.Endpoint(Endpoint{Addr: address, Alt: legacyTestAddress(), Token: UID{uint64(i), uint64(i + 10)}})
		}
		w.U8(88) // server-only suffix, intentionally sliced by upstream clients.
		open, status, err := DecodeControllerEndpointsLegacy(w.buf)
		if err != nil || open.Token != (UID{0, 10}) || status.Token != (UID{2, 12}) || open.Addr != address {
			t.Fatalf("%+v %+v %v", open, status, err)
		}
		if protocol >= legacy61 && open.Alt != legacyTestAddress() {
			t.Fatal(open.Alt)
		}
	}
}

func TestLegacyDiscoveryProtocolGates(t *testing.T) {
	for _, protocol := range []ProtocolVersion{legacy50, legacy51, legacy52, legacy60, legacy61, legacy62, Protocol73, Protocol74, 0} {
		controllerErr := ValidateRequest(OpenDatabaseController{}, protocol)
		controllerSupported := legacyProtocol(protocol) && protocol.Normalized() < legacy62
		if controllerSupported && controllerErr != nil || !controllerSupported && !errors.Is(controllerErr, fdberr.UnsupportedOperation) {
			t.Fatalf("controller protocol%x: %v", protocol, controllerErr)
		}
		metadataErr := ValidateRequest(GetLegacyMetadataServers{}, protocol)
		metadataSupported := protocol.Compatible(legacy50)
		if metadataSupported && metadataErr != nil || !metadataSupported && !errors.Is(metadataErr, fdberr.UnsupportedOperation) {
			t.Fatalf("metadata protocol%x: %v", protocol, metadataErr)
		}
	}
}

func TestLegacyControllerOpenBranches(t *testing.T) {
	var b Builder
	for _, protocol := range []ProtocolVersion{legacy50, legacy51, legacy52, legacy60, legacy61} {
		if err := EncodeLegacy(&b, OpenDatabaseController{KnownID: UID{7, 8}}, UID{9, 10}, protocol); err != nil {
			t.Fatal(err)
		}
		r := NewBinaryReader(b.Finished(), protocol)
		if protocol < legacy61 {
			if string(r.Bytes()) != "DB" || len(r.Bytes()) != 0 || r.U32() != 0 {
				t.Fatal("old open prefix")
			}
			if protocol >= legacy51 && len(r.Bytes()) != 0 {
				t.Fatal("trace group")
			}
		} else if r.U32() != 0 || r.U32() != 0 || r.I32() != 0 || len(r.Bytes()) != 0 {
			t.Fatal("6.1 open prefix")
		}
		if r.UID() != (UID{7, 8}) {
			t.Fatal("open known ID")
		}
		legacyTestReplyPromise(t, r, UID{9, 10})
		if r.Done() != nil {
			t.Fatal("open tail")
		}
	}
}

func TestLegacyVersionstampTranslation(t *testing.T) {
	for _, protocol := range []ProtocolVersion{legacy50, legacy51, legacy52, legacy60, legacy61, legacy62} {
		operand := append(bytes.Repeat([]byte{0xff}, 10), 0, 0, 0, 0)
		for _, kind := range []uint8{14, 15} {
			w := BinaryWriter{Protocol: protocol}
			m := Mutation{Kind: kind, Param1: []byte("key"), Param2: operand}
			if kind == 14 {
				m.Param1 = operand
				m.Param2 = []byte("value")
			}
			if err := legacyMutation(&w, m); err != nil {
				t.Fatal(err)
			}
			r := NewBinaryReader(w.buf, protocol)
			if r.U8() != kind {
				t.Fatal("kind")
			}
			p1, p2 := r.Bytes(), r.Bytes()
			want1, want2 := len(m.Param1), len(m.Param2)
			if protocol < legacy52 {
				if kind == 14 {
					want1 -= 2
				} else {
					want2 -= 4
				}
			}
			if len(p1) != want1 || len(p2) != want2 || r.Done() != nil {
				t.Fatalf("%x kind%d %x %x", protocol, kind, p1, p2)
			}
		}
	}
	bad := append(bytes.Repeat([]byte{0xff}, 11), 1, 0, 0, 0)
	w := BinaryWriter{Protocol: legacy50}
	if err := legacyMutation(&w, Mutation{Kind: 15, Param2: bad}); !errors.Is(err, fdberr.UnsupportedOperation) {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		v    ProtocolVersion
		kind uint8
	}{{legacy50, 16}, {legacy50, 18}, {legacy60, 20}} {
		w := BinaryWriter{Protocol: tc.v}
		if err := legacyMutation(&w, Mutation{Kind: tc.kind}); !errors.Is(err, fdberr.UnsupportedOperation) {
			t.Fatal(err)
		}
	}
}

func TestLegacyProxyAndBootstrapReplies(t *testing.T) {
	for _, protocol := range []ProtocolVersion{legacy50, legacy61, legacy62} {
		msg := binaryReply(protocol, func(w *BinaryWriter) {
			w.U32(1)
			w.U64(0)
			if protocol >= legacy61 {
				w.Bool(true)
			}
			count := 7
			if protocol >= legacy61 {
				count = 8
			}
			if protocol >= legacy62 {
				count = 9
			}
			for i := range count {
				w.Endpoint(Endpoint{Addr: legacyTestAddress(), Token: UID{uint64(100 + i), uint64(200 + i)}})
			}
			w.UID(UID{7, 8})
			w.F64(0.1)
			w.I64(100)
			if protocol >= legacy62 {
				w.OptionalBytes([]byte("forward"))
			}
		})
		info, err := DecodeLegacy(OpenDatabaseController{}, msg, protocol)
		if err != nil || len(info.Commit) != 1 || len(info.GRV) != 1 || info.ID != (UID{7, 8}) {
			t.Fatalf("%+v %v", info, err)
		}
		if info.Commit[0].Endpoint.Token != (UID{100, 200}) || info.GRV[0].Endpoint.Token != (UID{101, 201}) || info.Commit[0].Endpoint.Adjusted(2).Token != (UID{102, 202}) {
			t.Fatal("independent proxy stream tokens")
		}
		if info.Commit[0].Provisional != (protocol >= legacy61) {
			t.Fatal("provisional")
		}
		if protocol >= legacy62 && string(info.Forward) != "forward" {
			t.Fatal("forward")
		}
	}
	msg := binaryReply(legacy50, func(w *BinaryWriter) { w.U32(1); legacyTestSSI(w, UID{9, 10}) })
	servers, err := DecodeLegacy(GetLegacyMetadataServers{}, msg, legacy50)
	if err != nil || len(servers) != 1 || servers[0].ID != (UID{9, 10}) {
		t.Fatalf("%+v %v", servers, err)
	}
	var b Builder
	if err := EncodeLegacy(&b, GetLegacyMetadataServers{}, UID{11, 12}, legacy50); err != nil {
		t.Fatal(err)
	}
	r := NewBinaryReader(b.Finished(), legacy50)
	legacyTestReplyPromise(t, r, UID{11, 12})
	if r.Done() != nil {
		t.Fatal("bootstrap request must be only the reply promise")
	}
	if err := EncodeLegacy(&b, GetKeyServerLocations{}, UID{}, legacy50); !errors.Is(err, fdberr.UnsupportedOperation) {
		t.Fatal(err)
	}
}

func TestLegacyCommitFlagBranches(t *testing.T) {
	var b Builder
	for _, protocol := range []ProtocolVersion{legacy50, legacy51, legacy52, legacy60, legacy61, legacy62} {
		req := &Commit{ReadSnapshot: 12, LockAware: true, DebugID: UID{3, 4}}
		if err := EncodeLegacy(&b, req, UID{5, 6}, protocol); err != nil {
			t.Fatal(err)
		}
		r := NewBinaryReader(b.Finished(), protocol)
		if r.U32() != 0 || r.U32() != 0 || r.U32() != 0 || r.I64() != 12 {
			t.Fatal("commit prefix")
		}
		legacyTestReplyPromise(t, r, UID{5, 6})
		if protocol < legacy60 {
			if r.OptionalUID() != (UID{3, 4}) || !r.Bool() {
				t.Fatal("old commit flags")
			}
		} else if r.U32() != CommitFlagIsLockAware || r.OptionalUID() != (UID{3, 4}) {
			t.Fatal("new commit flags")
		}
		if r.Done() != nil {
			t.Fatal(r.Done())
		}
	}
}

func TestLegacyCompactServerListValues(t *testing.T) {
	for _, protocol := range []ProtocolVersion{0x0fdb00b063010000, 0x0fdb00b070010001} {
		w := BinaryWriter{Protocol: protocol}
		w.U64(uint64(protocol))
		w.UID(UID{8, 9})
		w.U64(0)
		w.Endpoint(Endpoint{Addr: legacyTestAddress(), Token: UID{20, 30}})
		if protocol >= 0x0fdb00b070010001 {
			w.OptionalUID(UID{40, 50})
		}
		got, err := DecodeServerListValue(w.buf)
		if err != nil || got.ID != (UID{8, 9}) || got.Base.Token != (UID{20, 30}) || got.Base.Streams != nil {
			t.Fatalf("%+v %v", got, err)
		}
	}
}

func TestLegacyKeyServersValueBounds(t *testing.T) {
	for _, protocol := range []ProtocolVersion{legacy50, legacy52, legacy61, legacy62} {
		w := BinaryWriter{Protocol: protocol}
		w.U64(uint64(protocol))
		w.U32(1)
		w.UID(UID{1, 2})
		w.U32(1)
		w.UID(UID{3, 4})
		got, err := DecodeKeyServersValue(w.buf, nil)
		if err != nil || len(got) != 1 || got[0] != (UID{1, 2}) {
			t.Fatalf("%+v %v", got, err)
		}
		for n := 1; n < len(w.buf); n++ {
			if _, err := DecodeKeyServersValue(w.buf[:n], nil); !errors.Is(err, ErrMalformed) {
				t.Fatalf("truncated%d: %v", n, err)
			}
		}
	}
	bad := binary.LittleEndian.AppendUint64(nil, uint64(legacy50))
	bad = binary.LittleEndian.AppendUint32(bad, 0xffffffff)
	if _, err := DecodeKeyServersValue(bad, nil); !errors.Is(err, ErrMalformed) {
		t.Fatal(err)
	}
}
