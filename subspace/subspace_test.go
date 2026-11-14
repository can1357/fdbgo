package subspace

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/can1357/fdbgo/tuple"
)

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func pack(t tuple.Tuple) []byte { return must(t.Pack()) }

func TestSub(t *testing.T) {
	ss1 := must(must(Sub(1)).Sub(2))
	ss2 := must(Sub(1, 2))
	if !bytes.Equal(ss1.Bytes(), ss2.Bytes()) || !bytes.Equal(ss1.Bytes(), pack(tuple.Tuple{1, 2})) {
		t.Errorf("Sub(1).Sub(2) = %q, Sub(1, 2) = %q", ss1.Bytes(), ss2.Bytes())
	}
	if _, err := Sub(struct{}{}); !errors.Is(err, tuple.ErrUnsupportedType) {
		t.Errorf("Sub(struct{}{}) error = %v", err)
	}
	// Sub never writes into its parent's prefix.
	base := FromBytes([]byte("base"))
	a, b := must(base.Sub("a")), must(base.Sub("b"))
	if string(a.Bytes()) != "base\x02a\x00" || string(b.Bytes()) != "base\x02b\x00" || string(base.Bytes()) != "base" {
		t.Errorf("sibling subspaces share memory: %q %q %q", a.Bytes(), b.Bytes(), base.Bytes())
	}
	raw := []byte("raw")
	fb := FromBytes(raw)
	raw[0] = 'X'
	if string(fb.Bytes()) != "raw" {
		t.Error("FromBytes does not copy")
	}
	if len(AllKeys().Bytes()) != 0 || len(Subspace{}.Bytes()) != 0 {
		t.Error("AllKeys prefix is not empty")
	}
}

func TestPackOwnsKey(t *testing.T) {
	ss := FromBytes([]byte("prefix"))
	for _, tupleValue := range []tuple.Tuple{nil, {}, {"key"}} {
		key := must(ss.Pack(tupleValue))
		want := bytes.Clone(key)
		key[0] ^= 0xff
		if string(ss.Bytes()) != "prefix" {
			t.Fatalf("Pack(%v) aliases subspace prefix", tupleValue)
		}
		if got := must(ss.Pack(tupleValue)); !bytes.Equal(got, want) {
			t.Errorf("Pack(%v) after mutation = %q, want %q", tupleValue, got, want)
		}
	}
}

func TestPackUnpack(t *testing.T) {
	ss := must(Sub(1))
	packed := must(ss.Pack(tuple.Tuple{2, 3}))
	if !bytes.Equal(packed, pack(tuple.Tuple{1, 2, 3})) {
		t.Errorf("Pack = %q", packed)
	}
	got, err := ss.Unpack(packed)
	if err != nil || len(got) != 2 || got[0] != int64(2) || got[1] != int64(3) {
		t.Errorf("Unpack = %v, %v", got, err)
	}
	if _, err := must(Sub(2)).Unpack(packed); !errors.Is(err, ErrKeyNotInSubspace) {
		t.Errorf("Unpack outside subspace error = %v", err)
	}
	if _, err := ss.Unpack(append(bytes.Clone(packed), 0x03)); !errors.Is(err, tuple.ErrMalformed) {
		t.Errorf("Unpack malformed error = %v", err)
	}
	if _, err := ss.Pack(tuple.Tuple{struct{}{}}); !errors.Is(err, tuple.ErrUnsupportedType) {
		t.Errorf("Pack error = %v", err)
	}
	if got, err := AllKeys().Unpack(pack(tuple.Tuple{"x"})); err != nil || got[0] != "x" {
		t.Errorf("AllKeys().Unpack = %v, %v", got, err)
	}
}

func TestPackWithVersionstamp(t *testing.T) {
	ss := must(Sub(1))
	got := must(ss.PackWithVersionstamp(tuple.Tuple{tuple.IncompleteVersionstamp(0), 2}))
	want := must(tuple.Tuple{1, tuple.IncompleteVersionstamp(0), 2}.PackWithVersionstamp(nil))
	if !bytes.Equal(got, want) {
		t.Errorf("PackWithVersionstamp = %q, want %q", got, want)
	}
	// Raw prefixes count toward the offset; offsets above 16 bits are fine.
	long := FromBytes(bytes.Repeat([]byte("x"), 65535))
	key := must(long.PackWithVersionstamp(tuple.Tuple{tuple.IncompleteVersionstamp(0)}))
	if off := binary.LittleEndian.Uint32(key[len(key)-4:]); off != 65536 {
		t.Errorf("offset = %d, want 65536", off)
	}
	if _, err := ss.PackWithVersionstamp(tuple.Tuple{2}); !errors.Is(err, tuple.ErrNoIncompleteVersionstamp) {
		t.Errorf("no stamp error = %v", err)
	}
}

func TestPrefixVersionstamp(t *testing.T) {
	stamp := tuple.IncompleteVersionstamp(9)
	prefixed := must(Sub("prefix"))
	got := must(prefixed.PackWithVersionstamp(tuple.Tuple{tuple.Tuple{stamp}, 2}))
	want := must(tuple.Tuple{"prefix", tuple.Tuple{stamp}, 2}.PackWithVersionstamp(nil))
	if !bytes.Equal(got, want) {
		t.Errorf("nested stamp: %q, want %q", got, want)
	}

	// An incomplete stamp in the prefix is tracked through Sub.
	stamped := must(prefixed.Sub(stamp))
	got = must(stamped.PackWithVersionstamp(tuple.Tuple{2}))
	want = must(tuple.Tuple{"prefix", stamp, 2}.PackWithVersionstamp(nil))
	if !bytes.Equal(got, want) {
		t.Errorf("prefix stamp: %q, want %q", got, want)
	}
	got = must(must(stamped.Sub("deeper")).PackWithVersionstamp(tuple.Tuple{2}))
	want = must(tuple.Tuple{"prefix", stamp, "deeper", 2}.PackWithVersionstamp(nil))
	if !bytes.Equal(got, want) {
		t.Errorf("child of stamped subspace: %q, want %q", got, want)
	}
	got = must(must(Sub(tuple.IncompleteVersionstamp(0))).PackWithVersionstamp(tuple.Tuple{1, 2}))
	want = must(tuple.Tuple{tuple.IncompleteVersionstamp(0), 1, 2}.PackWithVersionstamp(nil))
	if !bytes.Equal(got, want) {
		t.Errorf("stamp-first subspace: %q, want %q", got, want)
	}
	// Plain Pack ignores the stamp.
	if got := must(stamped.Pack(tuple.Tuple{2})); !bytes.Equal(got, pack(tuple.Tuple{"prefix", stamp, 2})) {
		t.Errorf("Pack under stamped subspace = %q", got)
	}

	for name, f := range map[string]func() ([]byte, error){
		"stamp in prefix and tuple": func() ([]byte, error) { return stamped.PackWithVersionstamp(tuple.Tuple{stamp}) },
		"two stamps in prefix": func() ([]byte, error) {
			return must(stamped.Sub(stamp)).PackWithVersionstamp(tuple.Tuple{1})
		},
		"two stamps in one Sub": func() ([]byte, error) {
			return must(Sub(stamp, stamp)).PackWithVersionstamp(tuple.Tuple{1})
		},
	} {
		if _, err := f(); !errors.Is(err, tuple.ErrMultipleIncompleteVersionstamps) {
			t.Errorf("%s: error = %v", name, err)
		}
	}
}

func TestUnpackCompleteVersionstamp(t *testing.T) {
	vs := tuple.Versionstamp{TransactionVersion: [10]byte{1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, UserVersion: 2}
	ss := must(Sub(vs))
	key := must(ss.Pack(tuple.Tuple{vs, 2}))
	got, err := ss.Unpack(key)
	if err != nil || len(got) != 2 || got[0] != vs || got[1] != int64(2) {
		t.Errorf("Unpack = %v, %v", got, err)
	}
}

func TestContains(t *testing.T) {
	ss0, ss1 := must(Sub(1)), must(Sub(2))
	key := must(ss0.Pack(tuple.Tuple{2, 3}))
	start := must(Sub("start"))
	for _, c := range []struct {
		ss   Subspace
		key  []byte
		want bool
	}{
		{ss0, key, true},
		{ss1, key, false},
		{start, pack(tuple.Tuple{"start"}), true},
		{start, pack(tuple.Tuple{"starting"}), false},
		{start, pack(tuple.Tuple{"start", "end"}), true},
		{must(Sub("start", 42)), pack(tuple.Tuple{"start", 42, "end"}), true},
		{AllKeys(), []byte{0xff}, true},
	} {
		if got := c.ss.Contains(c.key); got != c.want {
			t.Errorf("%v.Contains(%q) = %v", c.ss, c.key, got)
		}
	}
}

func TestRange(t *testing.T) {
	ss := must(Sub(1))
	packed := must(ss.Pack(tuple.Tuple{2, 3}))
	begin, end := ss.FDBRangeKeys()
	if string(begin) != "\x15\x01\x00" || string(end) != "\x15\x01\xff" {
		t.Errorf("FDBRangeKeys = %q, %q", begin, end)
	}
	if !(bytes.Compare(begin, packed) <= 0 && bytes.Compare(packed, end) < 0) {
		t.Error("packed key outside the range")
	}
	if bytes.Compare(ss.Bytes(), begin) >= 0 {
		t.Error("the prefix itself is inside the range")
	}
	sb, se := ss.FDBRangeKeySelectors()
	if !bytes.Equal(sb.Key, begin) || !bytes.Equal(se.Key, end) || sb.OrEqual || se.Offset != 1 {
		t.Errorf("FDBRangeKeySelectors = %v, %v", sb, se)
	}
	if b, e := AllKeys().FDBRangeKeys(); string(b) != "\x00" || string(e) != "\xff" {
		t.Errorf("AllKeys range = %q, %q", b, e)
	}
}

func TestString(t *testing.T) {
	for _, c := range []struct {
		ss   Subspace
		want string
	}{
		{must(Sub("test")), `\x02test\x00`},
		{must(must(Sub("test2")).Sub("path1")), `\x02test2\x00\x02path1\x00`},
		{must(must(Sub("test3")).Sub("__커피__")), `\x02test3\x00\x02__\xec\xbb\xa4\xed\x94\xbc__\x00`},
		{must(Sub(`test4\path1\path2`)), `\x02test4\\path1\\path2\x00`},
	} {
		if got := c.ss.String(); got != c.want {
			t.Errorf("String() = %s, want %s", got, c.want)
		}
	}
}

func BenchmarkPack(b *testing.B) {
	ss := must(Sub("app", "users"))
	t := tuple.Tuple{"alice", int64(123456), []byte("profile")}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := ss.Pack(t); err != nil {
			b.Fatal(err)
		}
	}
}
