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

func encode[R any](r Request[R]) []byte {
	var b Builder
	return bytes.Clone(EncodeRequest(&b, r, UID{7, 8}))
}

// reply wraps the table written by body in ErrorOr<EnsureTable<T>> (value alternative).
func reply(fileID uint32, body func(b *Builder) Off) []byte {
	var b Builder
	b.Reset()
	v := body(&b)
	b.StartTable(2)
	b.AddUnion(0, 2, v)
	root := b.EndTable()
	return bytes.Clone(b.Finish(root, fileID))
}

func errorReply(code uint16) []byte {
	var b Builder
	b.Reset()
	b.StartTable(1)
	b.AddU16(0, code)
	e := b.EndTable()
	b.StartTable(2)
	b.AddUnion(0, 1, e)
	root := b.EndTable()
	return bytes.Clone(b.Finish(root, (&GetValue{}).ReplyFileID()))
}

// requestTable returns the request table of an encoded request.
func requestTable(t *testing.T, msg []byte) Table {
	t.Helper()
	r, err := Root(msg)
	if err != nil {
		t.Fatal(err)
	}
	tt, err := r.mustTable(0)
	if err != nil {
		t.Fatal(err)
	}
	return tt
}

func TestServerTracingUsesOptionalUIDWireLayout(t *testing.T) {
	id := UID{First: 0x123456789abcdef, Second: 0xfedcba9876543210}
	readOpts := DefaultReadOpts()
	readOpts.DebugID = id
	read := requestTable(t, encode[Lb[[]byte]](&GetValue{
		Key: []byte("key"), Ctx: ReadCtx{Version: 42, Options: readOpts, HasOptions: true},
	}))
	_, field, err := read.Union(7)
	if err != nil {
		t.Fatal(err)
	}
	options, err := field.Table()
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name  string
		table Table
		slot  int
	}{
		{"read", options, 2},
		{"grv", requestTable(t, encode[ReadVersionReply](GetReadVersion{DebugID: id})), 3},
		{"watch", requestTable(t, encode[int64](&WatchValue{DebugID: id})), 6},
		{"commit", requestTable(t, encode[CommitReply](&Commit{DebugID: id})), 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tag, value, err := tc.table.Union(tc.slot)
			if err != nil || tag != 1 {
				t.Fatalf("Optional<UID> tag = %d, %v", tag, err)
			}
			at, err := deref(value.msg, value.pos)
			if err != nil || at < 0 || at+16 > len(value.msg) {
				t.Fatalf("invalid UID offset: %d, %v", at, err)
			}
			got := UID{First: binary.LittleEndian.Uint64(value.msg[at:]), Second: binary.LittleEndian.Uint64(value.msg[at+8:])}
			if got != id {
				t.Fatalf("server decoded UID %v; want %v", got, id)
			}
		})
	}
}

func TestPingTokenRoundTrip(t *testing.T) {
	tok, err := PingReplyToken(encode[struct{}](Ping{}))
	if err != nil || tok != (UID{7, 8}) {
		t.Fatal(tok, err)
	}
}

func TestVoidReplyIsValueAlternative(t *testing.T) {
	var b Builder
	b.Reset()
	msg := EncodeVoidReply(&b)
	if _, err := ReplyTable(msg); err != nil {
		t.Fatal(err)
	}
	if fid := binary.LittleEndian.Uint32(msg[4:]); fid != FIDErrorOr|FIDVoid {
		t.Fatal(fid)
	}
}

func TestReplyTypeMismatch(t *testing.T) {
	var b Builder
	b.Reset()
	msg := EncodeVoidReply(&b)
	if _, err := DecodeReply[CommitReply](&Commit{}, msg); !errors.Is(err, ErrMalformed) {
		t.Fatalf("Void decoded as CommitID: %v", err)
	}
	if _, err := DecodeReply[struct{}](Ping{}, msg); err != nil {
		t.Fatalf("valid Void reply: %v", err)
	}
	if _, err := DecodeReply[CommitReply](&Commit{}, errorReply(1020)); !errors.Is(err, ErrMalformed) {
		t.Fatalf("wrongly typed remote error: %v", err)
	}
}

func TestPackedRangeRejectsTrailingBytes(t *testing.T) {
	for _, count := range []uint32{0, 1} {
		blob := binary.LittleEndian.AppendUint32(nil, count)
		if count == 1 {
			for _, s := range []string{"key", "value"} {
				blob = binary.LittleEndian.AppendUint32(blob, uint32(len(s)))
				blob = append(blob, s...)
			}
		}
		for _, trailing := range []bool{false, true} {
			data := bytes.Clone(blob)
			if trailing {
				data = append(data, 0x42)
			}
			msg := reply((&GetKeyValues{}).ReplyFileID(), func(b *Builder) Off {
				packed := b.Bytes(data)
				b.StartTable(7)
				b.AddOffset(3, packed)
				return b.EndTable()
			})
			got, err := DecodeReply[Lb[RangePage]](&GetKeyValues{}, msg)
			if trailing {
				if !errors.Is(err, ErrMalformed) {
					t.Fatalf("count %d: trailing bytes accepted: %v", count, err)
				}
			} else if err != nil || len(got.Value.Data) != int(count) {
				t.Fatalf("count %d: valid page = %+v, %v", count, got, err)
			}
		}
	}
}

func TestErrorReply(t *testing.T) {
	_, err := DecodeReply[Lb[[]byte]](&GetValue{}, errorReply(1020))
	if err != fdberr.Error(1020) {
		t.Fatal(err)
	}
	if _, err := ReplyTable([]byte{1, 2}); !errors.Is(err, ErrMalformed) {
		t.Fatal(err)
	}
}

func TestAdjustedEndpointMatchesFlow(t *testing.T) {
	ep := Endpoint{
		Addr:  NetAddr{Addr: netip.MustParseAddrPort("127.0.0.1:1")},
		Token: UID{0x1000_0000_0000_0001, 0xabcd_ef01_0000_0005},
	}
	if a := ep.Adjusted(2); a.Token != (UID{0x1000_0002_0000_0001, 0xabcd_ef01_0000_0007}) {
		t.Fatalf("%x", a.Token)
	}
}

func TestEndpointAddress(t *testing.T) {
	plain := NetAddr{Addr: netip.MustParseAddrPort("10.0.0.1:4500")}
	tls := NetAddr{Addr: netip.MustParseAddrPort("10.0.0.1:4501"), TLS: true}
	ep := Endpoint{Addr: plain, Alt: tls}
	if ep.Address(true) != tls || ep.Address(false) != plain {
		t.Fatal("address choice")
	}
	if (Endpoint{Addr: plain}).Address(true) != plain {
		t.Fatal("no alt")
	}
	if tls.String() != "10.0.0.1:4501:tls" {
		t.Fatal(tls.String())
	}
}

func TestStringKVsParsesRows(t *testing.T) {
	blob := binary.LittleEndian.AppendUint32(nil, 2)
	for _, p := range [][2]string{{"a", "1"}, {"bc", ""}} {
		for _, s := range p {
			blob = binary.LittleEndian.AppendUint32(blob, uint32(len(s)))
			blob = append(blob, s...)
		}
	}
	rows, err := stringKVs(blob)
	if err != nil || len(rows) != 2 {
		t.Fatal(rows, err)
	}
	if string(rows[1].Key) != "bc" || rows[1].Value == nil || len(rows[1].Value) != 0 {
		t.Fatalf("%q %v", rows[1].Key, rows[1].Value)
	}
	if _, err := stringKVs(blob[:len(blob)-1]); err == nil {
		t.Fatal("truncated blob parsed")
	}
}

func TestGetValueRequestFields(t *testing.T) {
	req := &GetValue{Key: []byte("hello"), Ctx: ReadCtx{
		Version: 1234, Tags: [][]byte{[]byte("t1"), []byte("xyz")},
		Options: ReadOpts{ReadType: 4, LockAware: true}, HasOptions: true,
	}}
	tt := requestTable(t, encode(req))
	if fid := req.FileID(); fid != 8454530 {
		t.Fatal(fid)
	}
	k, _ := tt.Bytes(0)
	v, _ := tt.I64(1)
	if string(k) != "hello" || v != 1234 {
		t.Fatal(k, v)
	}
	tag, f, _ := tt.Union(2)
	ts, err := f.Bytes()
	if tag != 1 || err != nil || string(ts) != "\x02t1\x03xyz" {
		t.Fatalf("tags %d %q %v", tag, ts, err)
	}
	rp, _ := tt.mustTable(4)
	if u, _ := rp.UID(0); u != (UID{7, 8}) {
		t.Fatal(u)
	}
	tn, _ := tt.mustTable(6)
	if id, _ := tn.I64(0); id != -1 {
		t.Fatal(id)
	}
	tag, f, _ = tt.Union(7)
	ot, _ := f.Table()
	rt, _ := ot.I32(0)
	cr, _ := ot.Bool(1)
	la, _ := ot.Bool(6)
	if tag != 1 || rt != 4 || cr || !la {
		t.Fatal(tag, rt, cr, la)
	}
}

func TestCommitRequestFields(t *testing.T) {
	req := &Commit{
		ReadConflicts:  []kv.KeyRange{{Begin: []byte("a"), End: []byte("b")}},
		WriteConflicts: []kv.KeyRange{{Begin: []byte("c"), End: []byte("d")}, {Begin: []byte("e"), End: []byte("f")}},
		Mutations:      []Mutation{{0, []byte("k"), []byte("v")}, {1, []byte("a"), []byte("b")}},
		ReadSnapshot:   77, ReportConflictingKeys: true, Flags: 3,
		IdempotencyID: []byte("0123456789abcdef"),
	}
	tt := requestTable(t, encode(req))
	tx, _ := tt.mustTable(0)
	wc, _ := tx.Vector(1)
	if wc.Len() != 2 {
		t.Fatal(wc.Len())
	}
	r1, _ := wc.Table(1)
	if b, _ := r1.Bytes(0); string(b) != "e" {
		t.Fatal(string(b))
	}
	mv, _ := tx.Vector(2)
	m1, _ := mv.Table(1)
	kind, _ := m1.U8(0)
	p2, _ := m1.Bytes(2)
	if mv.Len() != 2 || kind != 1 || string(p2) != "b" {
		t.Fatal(kind, p2)
	}
	if rs, _ := tx.I64(3); rs != 77 {
		t.Fatal(rs)
	}
	if fl, _ := tt.U32(2); fl != 3 {
		t.Fatal(fl)
	}
	if id, _ := tt.Bytes(11); string(id) != "0123456789abcdef" {
		t.Fatal(id)
	}
}

func TestGetValueReplyDecode(t *testing.T) {
	msg := reply((&GetValue{}).ReplyFileID(), func(b *Builder) Off {
		v := b.Bytes(nil)
		b.StartTable(6)
		b.AddF64(0, 1.5)
		b.AddUnion(3, 1, v)
		return b.EndTable()
	})
	r, err := DecodeReply[Lb[[]byte]](&GetValue{}, msg)
	if err != nil || r.Penalty != 1.5 || r.Err != 0 || r.Value == nil || len(r.Value) != 0 {
		t.Fatalf("%+v %v", r, err)
	}
	absent := reply((&GetValue{}).ReplyFileID(), func(b *Builder) Off { b.StartTable(6); b.AddF64(0, 1); return b.EndTable() })
	if r, err := DecodeReply[Lb[[]byte]](&GetValue{}, absent); err != nil || r.Value != nil {
		t.Fatalf("%+v %v", r, err)
	}
	lbErr := reply((&GetValue{}).ReplyFileID(), func(b *Builder) Off {
		b.StartTable(1)
		b.AddU16(0, 1037)
		e := b.EndTable()
		b.StartTable(6)
		b.AddUnion(1, 1, e)
		return b.EndTable()
	})
	if r, err := DecodeReply[Lb[[]byte]](&GetValue{}, lbErr); err != nil || r.Err != 1037 {
		t.Fatalf("%+v %v", r, err)
	}
}

func getKeyValuesReply() []byte {
	blob := binary.LittleEndian.AppendUint32(nil, 2)
	for _, s := range []string{"a", "1", "b", ""} {
		blob = binary.LittleEndian.AppendUint32(blob, uint32(len(s)))
		blob = append(blob, s...)
	}
	return reply((&GetKeyValues{}).ReplyFileID(), func(b *Builder) Off {
		d := b.Bytes(blob)
		b.StartTable(7)
		b.AddF64(0, 1)
		b.AddOffset(3, d)
		b.AddI64(4, 100)
		b.AddBool(5, true)
		return b.EndTable()
	})
}

func TestGetKeyValuesReplyDecode(t *testing.T) {
	msg := getKeyValuesReply()
	r, err := DecodeReply[Lb[RangePage]](&GetKeyValues{}, msg)
	if err != nil || !r.Value.More || len(r.Value.Data) != 2 {
		t.Fatalf("%+v %v", r, err)
	}
	if string(r.Value.Data[0].Key) != "a" || string(r.Value.Data[0].Value) != "1" || r.Value.Data[1].Value == nil {
		t.Fatalf("%+v", r.Value.Data)
	}
}

func TestCommitReplyDecode(t *testing.T) {
	msg := reply((&Commit{}).ReplyFileID(), func(b *Builder) Off {
		b.prep(4, 8)
		b.put32(9)
		b.put32(4)
		b.put32(2)
		vec := b.Offset()
		b.StartTable(6)
		b.AddI64(0, 555)
		b.AddU16(1, 3)
		b.AddUnion(4, 1, vec)
		return b.EndTable()
	})
	r, err := DecodeReply[CommitReply](&Commit{}, msg)
	if err != nil || r.Version != 555 || r.BatchIndex != 3 || len(r.Conflicting) != 2 || r.Conflicting[0] != 4 || r.Conflicting[1] != 9 {
		t.Fatalf("%+v %v", r, err)
	}
}

// writeEndpoint encodes an Endpoint as a RequestStream table.
func writeEndpoint(b *Builder, e Endpoint) Off {
	addr := networkAddress(b, e.Addr)
	var alt Off
	if e.Alt.Addr.IsValid() {
		alt = networkAddress(b, e.Alt)
	}
	b.StartTable(3)
	b.AddOffset(0, addr)
	if alt != 0 {
		b.AddUnion(1, 1, alt)
	}
	list := b.EndTable()
	b.StartTable(2)
	b.AddOffset(0, list)
	b.AddUID(1, e.Token)
	ep := b.EndTable()
	b.StartTable(1)
	b.AddOffset(0, ep)
	return b.EndTable()
}

func TestLocationsAndClientDBInfoDecode(t *testing.T) {
	ep := Endpoint{
		Addr:  NetAddr{Addr: netip.MustParseAddrPort("127.0.0.1:4500")},
		Alt:   NetAddr{Addr: netip.MustParseAddrPort("[::1]:4501"), TLS: true},
		Token: UID{5, 6},
	}
	msg := reply((GetKeyServerLocations{}).ReplyFileID(), func(b *Builder) Off {
		zk, zv := b.String("zoneid"), b.String("z1")
		b.StartTable(3)
		b.AddOffset(0, zk)
		b.AddUnion(1, 1, zv)
		pair := b.EndTable()
		b.Push(pair)
		pairs := b.Offsets(0)
		b.StartTable(1)
		b.AddOffset(0, pairs)
		loc := b.EndTable()
		st := writeEndpoint(b, ep)
		b.StartTable(3)
		b.AddUID(0, UID{9, 9})
		b.AddOffset(1, loc)
		b.AddOffset(2, st)
		ssi := b.EndTable()
		b.Push(ssi)
		servers := b.Offsets(0)
		kr := keyRange(b, []byte("a"), []byte("z"))
		b.StartTable(2)
		b.AddOffset(0, kr)
		b.AddOffset(1, servers)
		b.Push(b.EndTable())
		res := b.Offsets(0)
		b.StartTable(1)
		b.AddOffset(0, res)
		return b.EndTable()
	})
	locs, err := DecodeReply[[]ShardLocation](GetKeyServerLocations{}, msg)
	if err != nil || len(locs) != 1 || len(locs[0].Servers) != 1 {
		t.Fatal(locs, err)
	}
	l := locs[0]
	if string(l.Range.Begin) != "a" || string(l.Range.End) != "z" {
		t.Fatalf("%q %v", l.Range.Begin, l.Range.End)
	}
	s := l.Servers[0]
	if s.ID != (UID{9, 9}) || s.Base != ep || string(s.Zone) != "z1" || s.DC != nil {
		t.Fatalf("%+v", s)
	}

	info := reply((OpenDatabase{}).ReplyFileID(), func(b *Builder) Off {
		st := writeEndpoint(b, ep)
		b.StartTable(4)
		b.AddBool(2, true)
		b.AddOffset(3, st)
		b.Push(b.EndTable())
		grv := b.Offsets(0)
		fw := b.String("desc:id@1.2.3.4:1")
		b.StartTable(10)
		b.AddOffset(0, grv)
		b.AddUID(2, UID{1, 1})
		b.AddUnion(3, 1, fw)
		b.AddUID(9, UID{2, 2})
		return b.EndTable()
	})
	ci, err := DecodeReply[ClientDBInfo](OpenDatabase{}, info)
	if err != nil || len(ci.GRV) != 1 || !ci.GRV[0].Provisional || ci.GRV[0].Endpoint != ep || len(ci.Commit) != 0 ||
		ci.ID != (UID{1, 1}) || ci.ClusterID != (UID{2, 2}) || string(ci.Forward) != "desc:id@1.2.3.4:1" {
		t.Fatalf("%+v %v", ci, err)
	}
}

func TestOpenDatabaseRequestFields(t *testing.T) {
	req := OpenDatabase{
		KnownID: UID{3, 4}, ClusterKey: []byte("desc:id"),
		Coordinators: []NetAddr{
			{Addr: netip.MustParseAddrPort("127.0.0.1:4500")},
			{Addr: netip.MustParseAddrPort("127.0.0.2:4501"), TLS: true},
			{Addr: netip.MustParseAddrPort("[::1]:4502"), TLS: true},
			{Addr: netip.MustParseAddrPort("[::2]:4503")},
		},
		Hostnames: []Hostname{{"host.example", 4500, true}},
	}
	tt := requestTable(t, encode[ClientDBInfo](req))
	cv, _ := tt.Vector(5)
	for i, want := range req.Coordinators {
		na, _ := cv.Table(i)
		got, err := netAddr(na)
		if err != nil || got != want {
			t.Fatal(i, got, err)
		}
	}
	hv, _ := tt.Vector(7)
	h, _ := hv.Table(0)
	port, _ := h.Bytes(1)
	if string(port) != "4500" {
		t.Fatal(string(port))
	}
}

var decoders = []func([]byte) error{
	func(m []byte) error { _, err := DecodeReply[Lb[[]byte]](&GetValue{}, m); return err },
	func(m []byte) error { _, err := DecodeReply[Lb[kv.KeySelector]](&GetKey{}, m); return err },
	func(m []byte) error { _, err := DecodeReply[Lb[RangePage]](&GetKeyValues{}, m); return err },
	func(m []byte) error { _, err := DecodeReply[Lb[MappedPage]](&GetMappedKeyValues{}, m); return err },
	func(m []byte) error { _, err := DecodeReply[CommitReply](&Commit{}, m); return err },
	func(m []byte) error { _, err := DecodeReply[ReadVersionReply](GetReadVersion{}, m); return err },
	func(m []byte) error { _, err := DecodeReply[[]ShardLocation](GetKeyServerLocations{}, m); return err },
	func(m []byte) error { _, err := DecodeReply[ClientDBInfo](OpenDatabase{}, m); return err },
	func(m []byte) error { _, err := DecodeReply[StorageMetrics](&WaitMetrics{}, m); return err },
	func(m []byte) error { _, err := DecodeReply[[][]byte](&SplitRange{}, m); return err },
	func(m []byte) error { _, err := DecodeReply[int64](&WatchValue{}, m); return err },
	func(m []byte) error { _, err := PingReplyToken(m); return err },
	func(m []byte) error { _, err := NoticeToken(m); return err },
}

func fuzzSeeds(f *testing.F) {
	f.Add(getKeyValuesReply())
	f.Add(errorReply(1020))
	f.Add(encode[struct{}](Ping{}))
	f.Add([]byte{})
	f.Add(reply((&GetValue{}).ReplyFileID(), func(b *Builder) Off { b.StartTable(6); b.AddF64(0, 1); return b.EndTable() }))
}

func fuzzOne(idx ...int) func(*testing.T, []byte) {
	return func(t *testing.T, msg []byte) {
		for _, i := range idx {
			err := decoders[i](msg)
			var fe fdberr.Error
			if err != nil && !errors.Is(err, ErrMalformed) && !errors.As(err, &fe) {
				t.Fatalf("unexpected error type %T", err)
			}
		}
	}
}

func FuzzDecodeGetValue(f *testing.F)     { fuzzSeeds(f); f.Fuzz(fuzzOne(0, 1)) }
func FuzzDecodeGetKeyValues(f *testing.F) { fuzzSeeds(f); f.Fuzz(fuzzOne(2, 3)) }
func FuzzDecodeCommit(f *testing.F)       { fuzzSeeds(f); f.Fuzz(fuzzOne(4, 5)) }
func FuzzDecodeLocations(f *testing.F)    { fuzzSeeds(f); f.Fuzz(fuzzOne(6, 7)) }
func FuzzDecodeOther(f *testing.F)        { fuzzSeeds(f); f.Fuzz(fuzzOne(8, 9, 10, 11, 12)) }

func BenchmarkEncodeGetValue(b *testing.B) {
	var bld Builder
	req := &GetValue{Key: []byte("some/key/of/moderate/length"), Ctx: ReadCtx{Version: 1234}}
	b.ReportAllocs()
	for b.Loop() {
		EncodeRequest(&bld, req, UID{1, 2})
	}
}

func BenchmarkEncodeCommit(b *testing.B) {
	var bld Builder
	req := &Commit{ReadSnapshot: 9}
	for i := range 10 {
		k := []byte{'k', byte(i)}
		req.ReadConflicts = append(req.ReadConflicts, kv.KeyRange{Begin: k, End: append(k, 0)})
		req.WriteConflicts = append(req.WriteConflicts, kv.KeyRange{Begin: k, End: append(k, 0)})
		req.Mutations = append(req.Mutations, Mutation{0, k, bytes.Repeat([]byte("v"), 100)})
	}
	b.ReportAllocs()
	for b.Loop() {
		EncodeRequest(&bld, req, UID{1, 2})
	}
}

func BenchmarkDecodeGetValue(b *testing.B) {
	msg := reply((&GetValue{}).ReplyFileID(), func(bl *Builder) Off {
		v := bl.Bytes(bytes.Repeat([]byte("v"), 100))
		bl.StartTable(6)
		bl.AddF64(0, 1)
		bl.AddUnion(3, 1, v)
		return bl.EndTable()
	})
	req := &GetValue{}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := DecodeReply[Lb[[]byte]](req, msg); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeGetKeyValues(b *testing.B) {
	msg := getKeyValuesReply()
	req := &GetKeyValues{}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := DecodeReply[Lb[RangePage]](req, msg); err != nil {
			b.Fatal(err)
		}
	}
}
