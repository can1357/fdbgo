package wire

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"net/netip"
	"testing"

	"github.com/can1357/fdbgo/internal/fdberr"
	"github.com/can1357/fdbgo/internal/kv"
)

const object62 ProtocolVersion = 0x0fdb00b062010000

// GetLeader's ErrorOr<Optional<LeaderInfo>> identifier composes two wrappers.
type objectLeaderEnvelope struct{ GetValue }

func (objectLeaderEnvelope) ReplyFileID() uint32 { return 0x247f3d6a }

func (objectLeaderEnvelope) ReplyFileIDProtocol(p ProtocolVersion) uint32 {
	if objectProtocol(p) < object70 {
		return 0x117f3d6a
	}
	return 0x247f3d6a
}

func versionRequest(t *testing.T, p ProtocolVersion, r Message) Table {
	t.Helper()
	b := Builder{Protocol: p}
	b.Reset()
	off := r.Encode(&b, UID{7, 8})
	root, err := Root(b.FinishRequest(off, MessageFileID(r, p)), p)
	if err != nil {
		t.Fatal(err)
	}
	out, err := root.mustTable(0)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestObjectReadVersionFields(t *testing.T) {
	for _, p := range []ProtocolVersion{object62, object63, object70, object71, object72, Protocol73, Protocol74, 0} {
		t.Run(fmt.Sprintf("%x", p), func(t *testing.T) {
			opts := DefaultReadOpts()
			opts.DebugID = UID{33, 44}
			ctx := ReadCtx{Version: 123, Options: opts, HasOptions: true}
			point := versionRequest(t, p, &GetValue{Key: []byte("k"), Ctx: ctx})
			q := objectProtocol(p)
			replySlot, tenantSlot := 4, 6
			if q < object72 {
				replySlot, tenantSlot = 6, 8
				if q < object63 {
					replySlot = 4
				}
			}
			rp, err := point.mustTable(replySlot)
			if err != nil {
				t.Fatal(err)
			}
			if tok, err := rp.UID(0); err != nil || tok != (UID{7, 8}) {
				t.Fatal(tok, err)
			}
			if point.Protocol != p || rp.Protocol != p {
				t.Fatal("nested protocol not inherited")
			}
			if q >= object71 {
				tn, err := point.mustTable(tenantSlot)
				if err != nil {
					t.Fatal(err)
				}
				idSlot := 0
				if q <= object72 {
					idSlot = 2
				}
				if id, err := tn.I64(idSlot); err != nil || id != -1 {
					t.Fatal(id, err)
				}
			}
			if q < object72 {
				ds := 4
				if q < object63 {
					ds = 2
				}
				tag, f, err := point.Union(ds)
				if err != nil || tag != 1 || f.Protocol != p {
					t.Fatal(tag, err)
				}
				at, err := deref(f.msg, f.pos)
				if err != nil || binary.LittleEndian.Uint64(f.msg[at:]) != 33 {
					t.Fatal("debugID missing", err)
				}
			}
			page := versionRequest(t, p, &GetKeyValues{Begin: kv.FirstGreaterOrEqual([]byte("a")), End: kv.FirstGreaterOrEqual([]byte("z")), Ctx: ctx})
			pageSlot := 7
			if q < object72 {
				pageSlot = 10
				if q < object63 {
					pageSlot = 8
				}
			}
			if _, err := page.mustTable(pageSlot); err != nil {
				t.Fatal("range reply promise", err)
			}
		})
	}
}

func TestObjectCombinedAndSplitProxyLayouts(t *testing.T) {
	ep := Endpoint{Addr: NetAddr{Addr: netip.MustParseAddrPort("127.0.0.1:4500")}, Token: UID{8, 9}}
	for _, p := range []ProtocolVersion{object62, object63, object70, object71, object72, Protocol74} {
		fileID := (OpenDatabase{}).ReplyFileID()
		if p < object70 {
			fileID = 1<<24 | 5355080
		}
		msg := reply(fileID, func(b *Builder) Off {
			st := writeEndpoint(b, ep)
			var grv, loc Off
			if p == object62 {
				g, l := ep, ep
				g.Token, l.Token = UID{90, 91}, UID{99, 100}
				grv, loc = writeEndpoint(b, g), writeEndpoint(b, l)
			}
			b.StartTable(5)
			if p == object62 {
				b.AddBool(1, true)
				b.AddOffset(2, st)
				b.AddOffset(3, grv)
				b.AddOffset(4, loc)
			} else {
				b.AddBool(2, true)
				b.AddOffset(3, st)
			}
			b.Push(b.EndTable())
			proxies := b.Offsets(0)
			fw := b.String("desc:id@127.0.0.1:4500")
			b.StartTable(11)
			b.AddOffset(0, proxies)
			if p < object70 {
				b.AddUID(1, UID{1, 2})
				b.AddUnion(4, 1, fw)
			} else {
				b.AddOffset(1, proxies)
				b.AddUID(2, UID{1, 2})
				b.AddUnion(3, 1, fw)
				if p == object72 {
					b.AddUID(10, UID{3, 4})
				} else if p >= Protocol73 {
					b.AddUID(9, UID{3, 4})
				}
			}
			return b.EndTable()
		})
		info, err := DecodeReply[ClientDBInfo](OpenDatabase{}, msg, p)
		if err != nil || len(info.Commit) != 1 || len(info.GRV) != 1 || info.ID != (UID{1, 2}) || len(info.Forward) == 0 {
			t.Fatalf("%x: %+v %v", p, info, err)
		}
		wantGRV := ep.Token
		if p == object62 {
			wantGRV = UID{90, 91}
			if info.Commit[0].Endpoint.Adjusted(2).Token != (UID{99, 100}) {
				t.Fatal("independent locations endpoint lost")
			}
		} else if p < object70 {
			wantGRV = ep.Adjusted(1).Token
		}
		if info.GRV[0].Endpoint.Token != wantGRV {
			t.Fatalf("%x: GRV token %v want %v", p, info.GRV[0].Endpoint.Token, wantGRV)
		}
		if p >= object72 && info.ClusterID != (UID{3, 4}) || p < object72 && info.ClusterID.IsValid() {
			t.Fatalf("%x: cluster ID %v", p, info.ClusterID)
		}
	}
}

func TestObjectLocationTenantOptionalLayout(t *testing.T) {
	for _, p := range []ProtocolVersion{object63, object70, object71, object72, Protocol74} {
		q := versionRequest(t, p, GetKeyServerLocations{Begin: []byte("a")})
		if p == object71 {
			if tag, _, err := q.Union(7); err != nil || tag != 0 {
				t.Fatal("tenant name must be absent", tag, err)
			}
			if version, err := q.I64(9); err != nil || version != LatestVersion {
				t.Fatal(version, err)
			}
		} else if p >= object72 {
			tenant, err := q.mustTable(7)
			if err != nil {
				t.Fatal(err)
			}
			idSlot := 0
			if p == object72 {
				idSlot = 2
			}
			if id, err := tenant.I64(idSlot); err != nil || id != -1 {
				t.Fatal("invalid non-tenant context", id, err)
			}
			if version, err := q.I64(8); err != nil || version != LatestVersion {
				t.Fatal(version, err)
			}
		}
	}
}

// This hand-authored forward-layout fixture follows 7.2.9's
// CommitProxyInterface.h GetKeyServerLocationsRequest and fdbrpc/TenantInfo.h,
// independently of Builder and encodeLocationsOld. Unlike 7.1, tenant is a
// table, not an Optional<TenantName>. An absent table leaves trusted=false;
// the public request's verify() then rejects even a trusted connection.
func TestObject72LocationAuthorizationFixture(t *testing.T) {
	const fixture = "1000000068898b000600080004000000080000001c0000001600200004000000000008000c0010000000140018000000180000001c00000001000000010000002000000044000000feffffffffffffff010000007100000006001800080000000800000000000000070000000000000008000000000000000a0010000000000008000000000000001000000000000000ffffffffffffffff"
	msg, err := hex.DecodeString(fixture)
	if err != nil {
		t.Fatal(err)
	}
	root, err := Root(msg, Protocol72)
	if err != nil {
		t.Fatal(err)
	}
	reference, err := root.mustTable(0)
	if err != nil {
		t.Fatal(err)
	}
	type locationContext struct {
		begin, end       string
		endTag, nameTag  uint8
		limit            int32
		reverse          bool
		reply            UID
		tenantID, minVer int64
	}
	decode := func(q Table) locationContext {
		t.Helper()
		var c locationContext
		key, err := q.Bytes(0)
		if err != nil {
			t.Fatal(err)
		}
		c.begin = string(key)
		var end Field
		if c.endTag, end, err = q.Union(1); err != nil {
			t.Fatal(err)
		}
		if c.endTag != 0 {
			key, err = end.Bytes()
			if err != nil {
				t.Fatal(err)
			}
			c.end = string(key)
		}
		if c.limit, err = q.I32(3); err != nil {
			t.Fatal(err)
		}
		if c.reverse, err = q.Bool(4); err != nil {
			t.Fatal(err)
		}
		promise, err := q.mustTable(5)
		if err != nil {
			t.Fatal(err)
		}
		if c.reply, err = promise.UID(0); err != nil {
			t.Fatal(err)
		}
		tenant, err := q.mustTable(7)
		if err != nil {
			t.Fatal("missing TenantInfo would fail the server's authorization check", err)
		}
		if c.nameTag, _, err = tenant.Union(0); err != nil {
			t.Fatal(err)
		}
		if c.tenantID, err = tenant.I64(2); err != nil {
			t.Fatal(err)
		}
		if tag, _, err := tenant.Union(3); err != nil || tag != 0 {
			t.Fatal("non-tenant request must not carry a token", tag, err)
		}
		if c.minVer, err = q.I64(8); err != nil {
			t.Fatal(err)
		}
		return c
	}
	want := decode(reference)
	if want != (locationContext{begin: "q", limit: 1, reverse: true, reply: UID{7, 8}, tenantID: -1, minVer: -2}) {
		t.Fatal("invalid reference fixture", want)
	}
	for _, p := range []ProtocolVersion{Protocol72, Protocol72 | 0x1000_0000_0000_0000} {
		q := versionRequest(t, p, GetKeyServerLocations{Begin: []byte("q"), Limit: 1, Reverse: true})
		if got := decode(q); got != want {
			t.Fatalf("%x: location context = %+v, reference = %+v", p, got, want)
		}
	}
}

func TestObjectLegacyPingNoticesAndErrors(t *testing.T) {
	for _, p := range []ProtocolVersion{object62, object63, object70, object71, object72, Protocol74} {
		b := Builder{Protocol: p}
		msg := EncodeRequest[struct{}](&b, Ping{}, UID{45, 46})
		if token, err := PingReplyToken(msg, p); err != nil || token != (UID{45, 46}) {
			t.Fatal(token, err)
		}
		wantFID := (Ping{}).FileID()
		if p < object70 {
			wantFID = FIDErrorOr | FIDVoid
		}
		if binary.LittleEndian.Uint32(msg[4:]) != wantFID {
			t.Fatal("ping file identifier")
		}
		var notice Builder
		notice.Reset()
		var root Off
		if p < object70 {
			stream := writeEndpoint(&notice, Endpoint{Addr: NetAddr{Addr: netip.MustParseAddrPort("127.0.0.1:4500")}, Token: UID{45, 46}})
			// RequestStream's one-offset wrapper is the same synthetic root as Endpoint.
			root = stream
		} else {
			notice.StartTable(1)
			notice.AddUID(0, UID{45, 46})
			root = notice.EndTable()
		}
		if token, err := NoticeToken(notice.Finish(root, 0), p); err != nil || token != (UID{45, 46}) {
			t.Fatal(token, err)
		}
		msg = errorReply(1037)
		if p < object70 {
			binary.LittleEndian.PutUint32(msg[4:8], 1<<24|1378929)
		}
		if _, err := DecodeReply[Lb[[]byte]](&GetValue{}, msg, p); !errors.Is(err, fdberr.Error(1037)) {
			t.Fatal(err)
		}
	}
}

// A hand-authored ErrorOr<GetValueReply> containing error 1037. The high
// identifier byte is 1, as declared by ErrorOr in the 6.2/6.3 flow.h source.
// The envelope is intentionally independent of reply() and errorReply().
func TestObjectLegacyErrorOrFileIdentifiers(t *testing.T) {
	const fixture = "10000000710a150108000c0004000800080000000100000010000000000000000600080004000000080000000d040000"
	msg, err := hex.DecodeString(fixture)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range []ProtocolVersion{Protocol62, Protocol63, Protocol70, Protocol71, Protocol72, Protocol73, Protocol74} {
		wantFID := uint32(2<<24 | FIDVoid)
		if objectProtocol(p) < object70 {
			wantFID = 1<<24 | FIDVoid
		}
		b := Builder{Protocol: p}
		b.Reset()
		void := EncodeVoidReply(&b)
		if got := binary.LittleEndian.Uint32(void[4:8]); got != wantFID {
			t.Fatalf("%x: Void reply FID = %x, want %x", p, got, wantFID)
		}
		if _, err := DecodeReply[struct{}](Ping{}, void, p); err != nil {
			t.Fatalf("%x: Void reply rejected: %v", p, err)
		}
		_, err := DecodeReply[Lb[[]byte]](&GetValue{}, msg, p)
		wantErr := error(ErrMalformed)
		if objectProtocol(p) < object70 {
			wantErr = fdberr.Error(1037)
		}
		if !errors.Is(err, wantErr) {
			t.Fatalf("%x: old ErrorOr fixture error = %v, want %v", p, err, wantErr)
		}
		modern := append([]byte(nil), msg...)
		binary.LittleEndian.PutUint32(modern[4:8], 2<<24|1378929)
		_, err = DecodeReply[Lb[[]byte]](&GetValue{}, modern, p)
		wantErr = fdberr.Error(1037)
		if objectProtocol(p) < object70 {
			wantErr = ErrMalformed
		}
		if !errors.Is(err, wantErr) {
			t.Fatalf("%x: modern ErrorOr fixture error = %v, want %v", p, err, wantErr)
		}
	}
}

func TestObject62OmittedVoidPayloadCaptured(t *testing.T) {
	// Actual 6.2.30 server ping response. flow/serialize.h's zero-argument
	// serializer overload emits no Void table; the value offset at 32 is 8,
	// pointing to byte 40, exactly one past the message.
	const captured = "1c0000004aad1e010000000000000600060004000800090008000400080000000800000002000000"
	msg, err := hex.DecodeString(captured)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeReply[struct{}](Ping{}, msg, Protocol62); err != nil {
		t.Fatal("captured 6.2 Void reply rejected", err)
	}
	table, err := ReplyTable(msg, Protocol62)
	if err != nil || table.Protocol != Protocol62 || table.field(0) != -1 {
		t.Fatal("omitted Void must decode as a zero-field table", table, err)
	}
	for _, tc := range []struct {
		name   string
		p      ProtocolVersion
		change func([]byte)
	}{
		{"6.3 requires a table", Protocol63, func([]byte) {}},
		{"later Void requires a table", Protocol74, func(b []byte) {
			binary.LittleEndian.PutUint32(b[4:8], FIDErrorOr|FIDVoid)
		}},
		{"other payload requires a table", Protocol62, func(b []byte) {
			binary.LittleEndian.PutUint32(b[4:8], 1<<24|1378929)
		}},
		{"beyond end", Protocol62, func(b []byte) {
			binary.LittleEndian.PutUint32(b[32:36], 9)
		}},
		{"truncated table", Protocol62, func(b []byte) {
			binary.LittleEndian.PutUint32(b[32:36], 7)
		}},
		{"error requires a table", Protocol62, func(b []byte) { b[36] = 1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := append([]byte(nil), msg...)
			tc.change(b)
			if _, err := ReplyTable(b, tc.p); !errors.Is(err, ErrMalformed) {
				t.Fatal("invalid omitted payload accepted", err)
			}
		})
	}
	if _, err := DecodeReply[Lb[[]byte]](&GetValue{}, msg, Protocol62); !errors.Is(err, ErrMalformed) {
		t.Fatal("Void accepted as a different reply type", err)
	}
}

func TestObjectNestedReplyFileIdentifierOverride(t *testing.T) {
	// Pre-7.0 ErrorOr<Optional<LeaderInfo>>: (0x1|0x10)<<24 | 8338794.
	// The body is an independent ErrorOr error 1037 fixture, so no synthetic
	// serializer can accidentally reproduce the assumed nested identifier.
	const fixture = "100000006a3d7f1108000c0004000800080000000100000010000000000000000600080004000000080000000d040000"
	old, err := hex.DecodeString(fixture)
	if err != nil {
		t.Fatal(err)
	}
	modern := append([]byte(nil), old...)
	binary.LittleEndian.PutUint32(modern[4:8], 0x247f3d6a)
	for _, p := range []ProtocolVersion{Protocol62, Protocol63, Protocol70, Protocol74, 0} {
		msg, wrong := modern, old
		if objectProtocol(p) < object70 {
			msg, wrong = old, modern
		}
		req := &objectLeaderEnvelope{}
		if _, err := DecodeReply[Lb[[]byte]](req, msg, p); !errors.Is(err, fdberr.Error(1037)) {
			t.Fatalf("%x: nested reply rejected: %v", p, err)
		}
		if _, err := DecodeReply[Lb[[]byte]](req, wrong, p); !errors.Is(err, ErrMalformed) {
			t.Fatalf("%x: mismatched nested reply accepted: %v", p, err)
		}
	}
}

func TestObjectFeatureValidationAndWrapperInheritance(t *testing.T) {
	type mappedWrapper struct{ GetMappedKeyValues }
	for _, tc := range []struct {
		req Message
		p   ProtocolVersion
	}{
		{&SplitRange{}, object63}, {&mappedWrapper{}, object70},
		{&Commit{IdempotencyID: make([]byte, 16)}, object71},
		{&Commit{ReportConflictingKeys: true}, object62},
		{GetReadVersion{Tags: []TagCount{{Tag: []byte("t"), Count: 1}}}, object62},
	} {
		if err := ValidateRequest(tc.req, tc.p); !errors.Is(err, fdberr.UnsupportedOperation) {
			t.Fatalf("%T %x: %v", tc.req, tc.p, err)
		}
	}
	if err := ValidateRequest(&Commit{IdempotencyID: make([]byte, 16)}, object72); err != nil {
		t.Fatal(err)
	}
}

func TestObjectDatabaseLockAwarenessBeforeRangeLocks(t *testing.T) {
	for _, p := range []ProtocolVersion{object62, object63, object70, object71, object72, Protocol73} {
		opts := DefaultReadOpts()
		opts.LockAware = true
		req := &GetValue{Ctx: ReadCtx{HasOptions: true, Options: opts}}
		if err := ValidateRequest(req, p); err != nil {
			t.Fatalf("%x: database-lock-aware read rejected: %v", p, err)
		}
		b := Builder{Protocol: p}
		b.Reset()
		options := readOptions(&b, opts)
		root, err := Root(b.FinishRequest(options, 0), p)
		if err != nil {
			t.Fatal(err)
		}
		table, err := root.mustTable(0)
		if err != nil {
			t.Fatal(err)
		}
		if lockAware, err := table.Bool(6); err != nil || lockAware != (p >= Protocol73) {
			t.Fatalf("%x: storage range-lock bypass = %v, %v", p, lockAware, err)
		}
	}
}

func TestObjectEndpointRejectsInvalidOptionalAddress(t *testing.T) {
	for _, p := range []ProtocolVersion{object62, object63, object70, object71, object72, Protocol74} {
		var b Builder
		b.Reset()
		addr := networkAddress(&b, NetAddr{Addr: netip.MustParseAddrPort("127.0.0.1:4500")})
		b.StartTable(3)
		b.AddOffset(0, addr)
		b.AddUnion(1, 2, addr)
		list := b.EndTable()
		b.StartTable(2)
		b.AddOffset(0, list)
		b.AddUID(1, UID{1, 2})
		ep := b.EndTable()
		root, err := Root(b.FinishRequest(ep, 10618805), p)
		if err != nil {
			t.Fatal(err)
		}
		table, err := root.mustTable(0)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := endpoint(table); !errors.Is(err, ErrMalformed) {
			t.Fatalf("%x: invalid Optional accepted: %v", p, err)
		}
	}
}

func TestObject62ReadVersionReplyWithoutBusyField(t *testing.T) {
	msg := reply(1<<24|15709388, func(b *Builder) Off {
		metadata := b.String("meta")
		b.StartTable(4)
		b.AddI64(0, 1234)
		b.AddBool(1, true)
		b.AddUnion(2, 1, metadata)
		return b.EndTable()
	})
	r, err := DecodeReply[ReadVersionReply](GetReadVersion{}, msg, object62)
	if err != nil || r.Version != 1234 || !r.Locked || string(r.MetadataVersion) != "meta" {
		t.Fatal(r, err)
	}
}

func TestObject71MappedRequestDebugFetchAndTenant(t *testing.T) {
	opts := DefaultReadOpts()
	opts.ReadType, opts.DebugID = 1, UID{1, 2}
	req := &GetMappedKeyValues{Mapper: []byte("mapper"), Ctx: ReadCtx{Version: 55, HasOptions: true, Options: opts}}
	table := versionRequest(t, object71, req)
	if fetch, err := table.Bool(6); err != nil || !fetch {
		t.Fatal(fetch, err)
	}
	if tag, _, err := table.Union(9); err != nil || tag != 1 {
		t.Fatal(tag, err)
	}
	if _, err := table.mustTable(11); err != nil {
		t.Fatal(err)
	}
	tenant, err := table.mustTable(13)
	if err != nil {
		t.Fatal(err)
	}
	if id, err := tenant.I64(2); err != nil || id != -1 {
		t.Fatal(id, err)
	}
	if err := ValidateRequest(req, object71); err != nil {
		t.Fatal(err)
	}
}

func TestEndpointIndependentStreamsEquality(t *testing.T) {
	a := Endpoint{Token: UID{1, 2}, Streams: &EndpointStreams{Present: 5, Tokens: [15]UID{0: {1, 2}, 2: {3, 4}}}}
	b := a
	copy := *a.Streams
	b.Streams = &copy
	if !a.Equal(b) || a.Adjusted(2).Token != (UID{3, 4}) || a.Adjusted(2).Streams != nil {
		t.Fatal("independent stream lookup/equality")
	}
	b.Streams.Tokens[2] = UID{9, 10}
	if a.Equal(b) {
		t.Fatal("different topology compared equal")
	}
}
