package wire

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestTableRoundTrip(t *testing.T) {
	var b Builder
	b.Reset()
	key := b.Bytes([]byte("hello"))
	b.StartTable(2)
	b.AddU32(0, 7)
	inner := b.EndTable()
	s := b.ScalarI64(-5)
	b.StartTable(9)
	b.AddOffset(0, key)
	b.AddI64(1, 42)
	b.AddUnion(2, 3, inner)
	b.AddUnion(4, 1, s)
	b.AddUID(6, UID{1, 2})
	b.AddBool(7, true)
	tab := b.EndTable()
	msg := bytes.Clone(b.FinishRequest(tab, 99))

	if got := binary.LittleEndian.Uint32(msg[4:]); got != 99 {
		t.Fatalf("file id %d", got)
	}
	root, err := Root(msg)
	if err != nil {
		t.Fatal(err)
	}
	tt, ok, err := root.Table(0)
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	if v, err := tt.Bytes(0); err != nil || string(v) != "hello" {
		t.Fatalf("bytes %q %v", v, err)
	}
	if v, _ := tt.I64(1); v != 42 {
		t.Fatalf("i64 %d", v)
	}
	tag, f, err := tt.Union(2)
	if err != nil || tag != 3 {
		t.Fatal(tag, err)
	}
	it, err := f.Table()
	if err != nil {
		t.Fatal(err)
	}
	if v, _ := it.U32(0); v != 7 {
		t.Fatalf("inner %d", v)
	}
	tag, f, _ = tt.Union(4)
	if v, err := f.I64(); tag != 1 || v != -5 || err != nil {
		t.Fatal(tag, v, err)
	}
	if u, _ := tt.UID(6); u != (UID{1, 2}) {
		t.Fatal(u)
	}
	if v, _ := tt.Bool(7); !v {
		t.Fatal("bool")
	}
	if v, err := tt.U32(8); v != 0 || err != nil {
		t.Fatal("absent slot reads as default", v, err)
	}
	if tag, _, err := tt.Union(8); tag != 0 || err != nil {
		t.Fatal("absent union", tag, err)
	}
}

func TestTruncatedMessagesAreMalformed(t *testing.T) {
	var b Builder
	b.Reset()
	k := b.Bytes([]byte("abcd")) // 4 bytes: no trailing padding, so every cut truncates it
	b.StartTable(1)
	b.AddOffset(0, k)
	tab := b.EndTable()
	msg := bytes.Clone(b.FinishRequest(tab, 1))
	for n := range len(msg) {
		cut := msg[:n]
		err := func() error {
			r, err := Root(cut)
			if err != nil {
				return err
			}
			tt, ok, err := r.Table(0)
			if err != nil || !ok {
				return err
			}
			_, err = tt.Bytes(0)
			return err
		}()
		if err == nil {
			t.Fatalf("len %d parsed", n)
		}
	}
}

func TestBuilderReuseKeepsMessagesIdentical(t *testing.T) {
	var b Builder
	enc := func() []byte {
		return bytes.Clone(EncodeRequest(&b, &GetValue{Key: []byte("k"), Ctx: ReadCtx{Version: 3}}, UID{1, 2}))
	}
	first := enc()
	for range 3 {
		if got := enc(); !bytes.Equal(got, first) {
			t.Fatalf("reuse changed bytes:\n%x\n%x", got, first)
		}
	}
}

func TestUIDString(t *testing.T) {
	if s := (UID{0x0123456789abcdef, 1}).String(); s != "0123456789abcdef0000000000000001" {
		t.Fatal(s)
	}
}

func TestEmptyBytesAreNonNil(t *testing.T) {
	var b Builder
	b.Reset()
	e := b.Bytes(nil)
	b.StartTable(2)
	b.AddOffset(0, e)
	tab := b.EndTable()
	msg := b.FinishRequest(tab, 1)
	r, _ := Root(msg)
	tt, _, _ := r.Table(0)
	v, err := tt.Bytes(0)
	if err != nil || v == nil || len(v) != 0 {
		t.Fatalf("present empty: %v %v", v, err)
	}
	if v, err := tt.Bytes(1); v != nil || err != nil {
		t.Fatalf("absent: %v %v", v, err)
	}
}
