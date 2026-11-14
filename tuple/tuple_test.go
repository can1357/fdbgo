package tuple

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"math"
	"math/big"
	"math/rand"
	"os"
	"slices"
	"testing"
)

// equal compares decoded tuples, comparing *big.Int by value.
func equal(a, b Tuple) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		switch x := a[i].(type) {
		case *big.Int:
			y, ok := b[i].(*big.Int)
			if !ok || x.Cmp(y) != 0 {
				return false
			}
		case Tuple:
			y, ok := b[i].(Tuple)
			if !ok || !equal(x, y) {
				return false
			}
		case []byte:
			y, ok := b[i].([]byte)
			if !ok || !bytes.Equal(x, y) {
				return false
			}
		default:
			if a[i] != b[i] {
				return false
			}
		}
	}
	return true
}

func bigFromString(s string) *big.Int {
	v, ok := new(big.Int).SetString(s, 10)
	if !ok {
		panic(s)
	}
	return v
}

type serdeCase struct {
	in  Tuple
	enc string
	out Tuple // decoded form; nil means in
}

// testSerde checks pack(in) == enc and unpack(enc) == out.
func testSerde(t *testing.T, cases []serdeCase) {
	t.Helper()
	for _, c := range cases {
		got, err := c.in.Pack()
		if err != nil {
			t.Errorf("Pack(%v): %v", c.in, err)
			continue
		}
		if string(got) != c.enc {
			t.Errorf("Pack(%v) = %q, want %q", c.in, got, c.enc)
		}
		out := c.out
		if out == nil {
			out = c.in
		}
		dec, err := Unpack([]byte(c.enc))
		if err != nil {
			t.Errorf("Unpack(%q): %v", c.enc, err)
			continue
		}
		if !equal(dec, out) {
			t.Errorf("Unpack(%q) = %#v, want %#v", c.enc, dec, out)
		}
	}
}

func TestSpec(t *testing.T) {
	testSerde(t, []serdeCase{
		{Tuple{nil}, "\x00", nil},
		{Tuple{Tuple{nil}}, "\x05\x00\xff\x00", nil},
		{Tuple{"FÔO\x00bar"}, "\x02F\xc3\x94O\x00\xffbar\x00", nil},
		{Tuple{[]byte("foo\x00bar")}, "\x01foo\x00\xffbar\x00", nil},
		{
			Tuple{Tuple{"foo\x00bar", nil, Tuple{}}},
			"\x05\x02foo\x00\xffbar\x00\x00\xff\x05\x00\x00", nil,
		},
		{Tuple{-1}, "\x13\xfe", Tuple{int64(-1)}},
		{Tuple{-5551212}, "\x11\xabK\x93", Tuple{int64(-5551212)}},
		{Tuple{float32(-42)}, "\x20\x3d\xd7\xff\xff", nil},
	})
}

func TestSimple(t *testing.T) {
	maxU64 := new(big.Int).SetUint64(math.MaxUint64)
	mul := func(a *big.Int, k int64) *big.Int { return new(big.Int).Mul(a, big.NewInt(k)) }
	add := func(a *big.Int, k int64) *big.Int { return new(big.Int).Add(a, big.NewInt(k)) }
	u128Max := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 128), big.NewInt(1))
	negMaxU64 := new(big.Int).Neg(maxU64)
	testSerde(t, []serdeCase{
		{Tuple{false}, "\x26", nil},
		{Tuple{true}, "\x27", nil},
		{Tuple{int64(0)}, "\x14", nil},
		{Tuple{int64(1)}, "\x15\x01", nil},
		{Tuple{int64(-1)}, "\x13\xfe", nil},
		{Tuple{int64(100)}, "\x15\x64", nil},
		{Tuple{int32(10000)}, "\x16\x27\x10", Tuple{int64(10000)}},
		{Tuple{int16(-100)}, "\x13\x9b", Tuple{int64(-100)}},
		{Tuple{int64(-10000)}, "\x12\xd8\xef", nil},
		{Tuple{int64(-1000000)}, "\x11\xf0\xbd\xbf", nil},
		{Tuple{uint16(255)}, "\x15\xff", Tuple{int64(255)}},
		{Tuple{int32(256)}, "\x16\x01\x00", Tuple{int64(256)}},
		{Tuple{int16(-255)}, "\x13\x00", Tuple{int64(-255)}},
		{Tuple{int64(-256)}, "\x12\xfe\xff", nil},
		{Tuple{int8(-128), uint8(200)}, "\x13\x7f\x15\xc8", Tuple{int64(-128), int64(200)}},
		{Tuple{uint(7), int(-7)}, "\x15\x07\x13\xf8", Tuple{int64(7), int64(-7)}},
		{
			Tuple{Versionstamp{[10]byte{0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x00, 0x01, 0x02, 0x03}, 0}},
			"\x33\xaa\xbb\xcc\xdd\xee\xff\x00\x01\x02\x03\x00\x00", nil,
		},
		{
			Tuple{Versionstamp{[10]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, 657}},
			"\x33\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x02\x91", nil,
		},
		{Tuple{int64(65536)}, "\x17\x01\x00\x00", nil},
		{Tuple{int64(-65536)}, "\x11\xfe\xff\xff", nil},
		{Tuple{int64(math.MaxInt64)}, "\x1c\x7f\xff\xff\xff\xff\xff\xff\xff", nil},
		{Tuple{uint64(math.MaxInt64 + 1)}, "\x1c\x80\x00\x00\x00\x00\x00\x00\x00", nil},
		{Tuple{uint64(math.MaxUint64)}, "\x1c\xff\xff\xff\xff\xff\xff\xff\xff", nil},
		{Tuple{u128Max}, "\x1d\x10\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff", nil},
		{Tuple{add(maxU64, 1)}, "\x1d\x09\x01\x00\x00\x00\x00\x00\x00\x00\x00", nil},
		{Tuple{big.NewInt(math.MinInt64 + 1)}, "\x0c\x80\x00\x00\x00\x00\x00\x00\x00", Tuple{int64(math.MinInt64 + 1)}},
		{Tuple{add(big.NewInt(math.MinInt64), -1)}, "\x0c\x7f\xff\xff\xff\xff\xff\xff\xfe", nil},
		{Tuple{negMaxU64}, "\x0c\x00\x00\x00\x00\x00\x00\x00\x00", nil},
		{Tuple{add(negMaxU64, -1)}, "\x0b\xf6\xfe\xff\xff\xff\xff\xff\xff\xff\xff", nil},
		{Tuple{add(negMaxU64, -2)}, "\x0b\xf6\xfe\xff\xff\xff\xff\xff\xff\xff\xfe", nil},
		{Tuple{mul(maxU64, -2)}, "\x0b\xf6\xfe\x00\x00\x00\x00\x00\x00\x00\x01", nil},
		{Tuple{mul(maxU64, 2)}, "\x1d\x09\x01\xff\xff\xff\xff\xff\xff\xff\xfe", nil},
		{Tuple{int64(-4294967295)}, "\x10\x00\x00\x00\x00", nil},
		{Tuple{int64(math.MinInt64 + 2)}, "\x0c\x80\x00\x00\x00\x00\x00\x00\x01", nil},
		{Tuple{int64(math.MinInt64 + 1)}, "\x0c\x80\x00\x00\x00\x00\x00\x00\x00", nil},
		{Tuple{int64(math.MinInt64)}, "\x0c\x7f\xff\xff\xff\xff\xff\xff\xff", nil},
		{Tuple{uint64(9252427359321063944)}, "\x1c\x80g9\xa9np\x02\x08", nil},
		{Tuple{bigFromString("-9252427359321063944")}, "\x0c\x7f\x98\xc6V\x91\x8f\xfd\xf7", nil},
	})
}

func TestBigInt(t *testing.T) {
	// big.Int values that fit 64 bits share the canonical integer encoding and
	// decode to int64 or uint64.
	testSerde(t, []serdeCase{
		{Tuple{big.NewInt(0)}, "\x14", Tuple{int64(0)}},
		{Tuple{*big.NewInt(1)}, "\x15\x01", Tuple{int64(1)}},
		{Tuple{big.NewInt(-1)}, "\x13\xfe", Tuple{int64(-1)}},
		{Tuple{big.NewInt(255)}, "\x15\xff", Tuple{int64(255)}},
		{Tuple{big.NewInt(-256)}, "\x12\xfe\xff", Tuple{int64(-256)}},
		{Tuple{big.NewInt(math.MinInt64)}, "\x0c\x7f\xff\xff\xff\xff\xff\xff\xff", Tuple{int64(math.MinInt64)}},
		{Tuple{new(big.Int).SetUint64(math.MaxUint64)}, "\x1c\xff\xff\xff\xff\xff\xff\xff\xff", Tuple{uint64(math.MaxUint64)}},
	})
	// Non-canonical long forms of small values decode normalized.
	for enc, want := range map[string]any{
		"\x1d\x01\x05": int64(5),
		"\x0b\xfe\xfa": int64(-5),
		"\x1d\x00":     int64(0),
		"\x1d\x08" + "\xff\xff\xff\xff\xff\xff\xff\xff": uint64(math.MaxUint64),
	} {
		got, err := Unpack([]byte(enc))
		if err != nil || len(got) != 1 || got[0] != want {
			t.Errorf("Unpack(%q) = %#v, %v; want %#v", enc, got, err, want)
		}
	}
	huge := new(big.Int).Lsh(big.NewInt(1), 8*255)
	if _, err := (Tuple{huge}).Pack(); !errors.Is(err, ErrIntTooLarge) {
		t.Errorf("Pack(2^2040) error = %v, want ErrIntTooLarge", err)
	}
	maxLong := new(big.Int).Sub(huge, big.NewInt(1))
	for _, v := range []*big.Int{maxLong, new(big.Int).Neg(maxLong)} {
		b, err := Tuple{v}.Pack()
		if err != nil {
			t.Fatal(err)
		}
		got, err := Unpack(b)
		if err != nil || !equal(got, Tuple{v}) {
			t.Errorf("round trip of 255-byte integer: %v", err)
		}
	}
	if _, err := (Tuple{(*big.Int)(nil)}).Pack(); !errors.Is(err, ErrUnsupportedType) {
		t.Errorf("Pack(nil *big.Int) error = %v", err)
	}
}

func TestUUID(t *testing.T) {
	u := UUID{0xba, 0xff, 0xff, 0xff, 0xff, 0x5e, 0xba, 0x11, 0, 0, 0, 0, 0x5c, 0xa1, 0xab, 0x1e}
	testSerde(t, []serdeCase{{Tuple{u}, "\x30\xba\xff\xff\xff\xff\x5e\xba\x11\x00\x00\x00\x00\x5c\xa1\xab\x1e", nil}})
	if got := u.String(); got != "baffffff-ff5e-ba11-0000-00005ca1ab1e" {
		t.Errorf("UUID.String() = %s", got)
	}
}

func TestBindingTester(t *testing.T) {
	testSerde(t, []serdeCase{
		{Tuple{"NEW_TRANSACTION"}, "\x02NEW_TRANSACTION\x00", nil},
		{
			Tuple{"PUSH", []byte("\x01tester_output\x00\x01results\x00\x14")},
			"\x02PUSH\x00\x01\x01tester_output\x00\xff\x01results\x00\xff\x14\x00", nil,
		},
		{Tuple{"PUSH", nil}, "\x02PUSH\x00\x00", nil},
		{
			Tuple{"PUSH", Tuple{nil, float32(3299069000000.0), float32(-0.000000000000000000000000000000000000011883096)}},
			"\x02PUSH\x00\x05\x00\xff \xd4@\x07\xf5 \x7f~\x9a\xc2\x00", nil,
		},
		{Tuple{"PUSH", int64(-133525682914243904)}, "\x02PUSH\x00\x0c\xfe%\x9f\x19M\x81J\xbf", nil},
		{Tuple{nil, nil}, "\x00\x00", nil},
		{Tuple{"PUSH", Tuple{-8.251343508909708e-15}}, "\x02PUSH\x00\x05!B\xfdkl\x9f\xcc\x8eK\x00", nil},
	})
}

func TestElements(t *testing.T) {
	testSerde(t, []serdeCase{
		{Tuple{true, false}, "\x27\x26", nil},
		{Tuple{Tuple{true, false}}, "\x05\x27\x26\x00", nil},
		{Tuple{[]any{true, Tuple{nil}}}, "\x05\x27\x05\x00\xff\x00\x00", Tuple{Tuple{true, Tuple{nil}}}},
		{Tuple{}, "", nil},
		{Tuple{Tuple{}}, "\x05\x00", nil},
		{Tuple{"", []byte{}}, "\x02\x00\x01\x00", nil},
		{Tuple{1.5, float32(math.Copysign(0, -1))}, "\x21\xbf\xf8\x00\x00\x00\x00\x00\x00\x20\x7f\xff\xff\xff", nil},
	})
	if got, err := Unpack(nil); err != nil || len(got) != 0 {
		t.Errorf("Unpack(nil) = %v, %v", got, err)
	}
}

func TestVersionstamp(t *testing.T) {
	tup := Tuple{"foo", IncompleteVersionstamp(0)}
	b, err := tup.Pack()
	if err != nil || string(b) != "\x02foo\x00\x33\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00" {
		t.Errorf("Pack = %q, %v", b, err)
	}
	b, err = tup.PackWithVersionstamp(nil)
	if err != nil || string(b) != "\x02foo\x00\x33\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x06\x00\x00\x00" {
		t.Errorf("PackWithVersionstamp = %q, %v", b, err)
	}
	// The offset counts the prefix and nested tuples.
	b, err = Tuple{"foo", Tuple{1, IncompleteVersionstamp(9)}}.PackWithVersionstamp([]byte("pre"))
	if err != nil {
		t.Fatal(err)
	}
	if want := "pre\x02foo\x00\x05\x15\x01\x33" + string(incompleteTransactionVersion[:]) + "\x00\x09\x00\x0c\x00\x00\x00"; string(b) != want {
		t.Errorf("nested PackWithVersionstamp = %q, want %q", b, want)
	}
	if off := int(b[len(b)-4]); string(b[off:off+10]) != string(incompleteTransactionVersion[:]) {
		t.Errorf("offset %d does not point at the transaction version", off)
	}

	complete := Tuple{"foo", Versionstamp{[10]byte{7, 7, 7, 7, 7, 7, 7, 7, 7, 7}, 657}}
	if _, err := complete.PackWithVersionstamp(nil); !errors.Is(err, ErrNoIncompleteVersionstamp) {
		t.Errorf("complete stamp: %v", err)
	}
	if _, err := (Tuple{"foo"}).PackWithVersionstamp(nil); !errors.Is(err, ErrNoIncompleteVersionstamp) {
		t.Errorf("no stamp: %v", err)
	}
	multiple := Tuple{IncompleteVersionstamp(0), Tuple{IncompleteVersionstamp(1)}}
	if _, err := multiple.PackWithVersionstamp(nil); !errors.Is(err, ErrMultipleIncompleteVersionstamps) {
		t.Errorf("multiple stamps: %v", err)
	}

	for _, c := range []struct {
		t    Tuple
		has  bool
		fail bool
	}{
		{Tuple{"a", 1}, false, false},
		{complete, false, false},
		{tup, true, false},
		{Tuple{Tuple{IncompleteVersionstamp(1)}}, true, false},
		{multiple, true, true},
	} {
		has, err := c.t.HasIncompleteVersionstamp()
		if has != c.has || (err != nil) != c.fail {
			t.Errorf("HasIncompleteVersionstamp(%v) = %v, %v", c.t, has, err)
		}
	}
	if IncompleteVersionstamp(42).UserVersion != 42 || IncompleteVersionstamp(0).IsComplete() || !complete[1].(Versionstamp).IsComplete() {
		t.Error("versionstamp accessors")
	}
	if got := complete[1].(Versionstamp).Bytes(); string(got) != "\x07\x07\x07\x07\x07\x07\x07\x07\x07\x07\x02\x91" {
		t.Errorf("Versionstamp.Bytes() = %q", got)
	}
}

// TestTypecodeBandOrdering checks that the type code band, not the value,
// orders keys of different integer and float types.
func TestTypecodeBandOrdering(t *testing.T) {
	pack := func(e any) string {
		b, err := Tuple{e}.Pack()
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	negMin, negMax := pack(int64(math.MinInt64)), pack(int64(-1))
	u32Min, u32Max := pack(uint32(0)), pack(uint32(math.MaxUint32))
	fMin, fMax := pack(float32(1)), pack(float32(math.MaxFloat32))
	if !(negMin < negMax && u32Min < u32Max && fMin < fMax) {
		t.Error("order within a band")
	}
	if !(negMax < u32Min && negMin < u32Min && u32Max < fMin && u32Max < fMax) {
		t.Error("order across bands")
	}
}

// TestOrdering checks that packed bytes order like the values for random
// integers (across the 64-bit and big ranges), floats, strings and nesting.
func TestOrdering(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	randInt := func() *big.Int {
		v := new(big.Int).Rand(rng, new(big.Int).Lsh(big.NewInt(1), uint(rng.Intn(140))+1))
		if rng.Intn(2) == 0 {
			v.Neg(v)
		}
		return v
	}
	for range 5000 {
		a, b := randInt(), randInt()
		pa, _ := Tuple{a}.Pack()
		pb, _ := Tuple{b}.Pack()
		if a.Cmp(b) != bytes.Compare(pa, pb) {
			t.Fatalf("int order: %v vs %v packs as %x vs %x", a, b, pa, pb)
		}
		got, err := Unpack(pa)
		if err != nil || len(got) != 1 {
			t.Fatalf("Unpack(%x): %v", pa, err)
		}
		var back *big.Int
		switch v := got[0].(type) {
		case int64:
			back = big.NewInt(v)
		case uint64:
			back = new(big.Int).SetUint64(v)
		case *big.Int:
			back = v
		}
		if back == nil || back.Cmp(a) != 0 {
			t.Fatalf("round trip %v = %#v", a, got[0])
		}
	}
	floats := []float64{math.Inf(-1), -math.MaxFloat64, -1e10, -1, -math.SmallestNonzeroFloat64, math.Copysign(0, -1), 0, math.SmallestNonzeroFloat64, 1, 1e10, math.MaxFloat64, math.Inf(1)}
	for i := 1; i < len(floats); i++ {
		a, _ := Tuple{floats[i-1]}.Pack()
		b, _ := Tuple{floats[i]}.Pack()
		a32, _ := Tuple{float32(floats[i-1])}.Pack()
		b32, _ := Tuple{float32(floats[i])}.Pack()
		if bytes.Compare(a, b) >= 0 || bytes.Compare(a32, b32) > 0 {
			t.Errorf("float order %v < %v", floats[i-1], floats[i])
		}
	}
	nan, _ := Tuple{math.NaN()}.Pack()
	got, err := Unpack(nan)
	if err != nil || !math.IsNaN(got[0].(float64)) {
		t.Errorf("NaN round trip: %v %v", got, err)
	}
	ordered := []Tuple{
		{nil}, {[]byte("a")}, {[]byte("a\x00")}, {[]byte("a\x00\x00")}, {[]byte("b")},
		{"a"}, {"a", nil}, {"a", int64(1)}, {"b"},
		{Tuple{}}, {Tuple{nil}}, {Tuple{nil, nil}}, {Tuple{int64(1)}},
		{int64(-1)}, {int64(0)}, {float32(0)}, {0.0}, {false}, {true},
		{UUID{}}, {Versionstamp{}},
	}
	for i := 1; i < len(ordered); i++ {
		a, _ := ordered[i-1].Pack()
		b, _ := ordered[i].Pack()
		if bytes.Compare(a, b) >= 0 {
			t.Errorf("%v packs after %v", ordered[i-1], ordered[i])
		}
	}
}

func TestErrors(t *testing.T) {
	for _, e := range []any{struct{}{}, map[string]int{}, uintptr(1), complex(1, 2), Tuple{1, []int{2}}} {
		if _, err := (Tuple{"ok", e}).Pack(); !errors.Is(err, ErrUnsupportedType) {
			t.Errorf("Pack(%T) error = %v", e, err)
		}
	}
	dst := []byte("keep")
	if out, err := (Tuple{"x", struct{}{}}).AppendPack(dst); err == nil || string(out) != "keep" {
		t.Errorf("AppendPack on error = %q, %v", out, err)
	}
	for _, enc := range []string{
		"\x01abc",         // unterminated bytes
		"\x02abc\x00\xff", // unterminated string (escaped zero is not a terminator)
		"\x05\x15\x01",    // unterminated nested tuple
		"\x15",            // missing int byte
		"\x1c\x01\x02",    // missing int bytes
		"\x1d",            // missing length
		"\x1d\x09\x01",    // missing big int bytes
		"\x20\x00",        // short float
		"\x21\x00\x00",    // short double
		"\x30\x00",        // short UUID
		"\x33\x00",        // short versionstamp
		"\x03",            // unknown code
		"\x00\xff",        // escaped nil outside a nested tuple
		"\x15\x01\x40",    // unknown code after a valid element
	} {
		if got, err := Unpack([]byte(enc)); !errors.Is(err, ErrMalformed) {
			t.Errorf("Unpack(%q) = %v, %v; want ErrMalformed", enc, got, err)
		}
	}
}

func TestRangeKeys(t *testing.T) {
	tup := Tuple{"a", int64(1)}
	begin, end := tup.FDBRangeKeys()
	if string(begin) != "\x02a\x00\x15\x01\x00" || string(end) != "\x02a\x00\x15\x01\xff" {
		t.Errorf("FDBRangeKeys = %q, %q", begin, end)
	}
	inner, _ := Tuple{"a", int64(1), "b"}.Pack()
	self, _ := tup.Pack()
	if !(string(begin) <= string(inner) && string(inner) < string(end)) || string(self) >= string(begin) {
		t.Error("range does not hold exactly the extensions of the tuple")
	}
	sb, se := tup.FDBRangeKeySelectors()
	if !bytes.Equal(sb.Key, begin) || !bytes.Equal(se.Key, end) || sb.OrEqual || sb.Offset != 1 {
		t.Errorf("FDBRangeKeySelectors = %v, %v", sb, se)
	}
	bad := Tuple{struct{}{}}
	if _, _, err := bad.RangeKeys(); !errors.Is(err, ErrUnsupportedType) {
		t.Errorf("RangeKeys error = %v", err)
	}
	if b, e := bad.FDBRangeKeys(); bytes.Compare(b, e) <= 0 {
		t.Errorf("unpackable tuple range [%q, %q) is not inverted", b, e)
	}
}

func TestString(t *testing.T) {
	testUUID := UUID{0x11, 0x00, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x11, 0x00, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF}
	for _, c := range []struct {
		in   Tuple
		want string
	}{
		{Tuple{[]byte("hello"), "world", 42, 0x99}, `(b"hello", "world", 42, 153)`},
		{Tuple{nil, Tuple{"Ok", Tuple{1, 2}, "Go"}, 42, 0x99}, `(<nil>, ("Ok", (1, 2), "Go"), 42, 153)`},
		{Tuple{"Bool", true, false}, `("Bool", true, false)`},
		{Tuple{"UUID", testUUID}, `("UUID", UUID(1100aabb-ccdd-eeff-1100-aabbccddeeff))`},
		{
			Tuple{"Versionstamp", Versionstamp{[10]byte{0, 0, 0, 0xaa, 0, 0xbb, 0, 0xcc, 0, 0xdd}, 620}},
			`("Versionstamp", Versionstamp(\x00\x00\x00\xaa\x00\xbb\x00\xcc\x00\xdd, 620))`,
		},
		{Tuple{[]byte("a\\b\xff")}, `(b"a\\b\xff")`},
	} {
		if got := fmt.Sprint(c.in); got != c.want {
			t.Errorf("String() = %s, want %s", got, c.want)
		}
	}
}

// TestGolden checks byte compatibility with the official Go binding's golden
// file (bindings/go/src/fdb/tuple/testdata/tuples.golden), regenerating the
// same tuples from its seeded generator.
func TestGolden(t *testing.T) {
	f, err := os.Open("testdata/tuples.golden")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var golden map[string][]byte
	if err := gob.NewDecoder(f).Decode(&golden); err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewSource(1))
	testUUID := UUID{0x11, 0x00, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x11, 0x00, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF}
	mk := func(gen func() any, n int) Tuple {
		t := make(Tuple, n)
		for i := range t {
			t[i] = gen()
		}
		return t
	}
	genBytes := func() any { return []byte("namespace") }
	genBytesNil := func() any { return []byte{0xFF, 0x00, 0xFF} }
	genString := func() any { return "namespace" }
	genStringNil := func() any { return "nam\x00es\xFFpace" }
	genInt := func() any { return rng.Int63() }
	genFloat := func() any { return float32(rng.NormFloat64()) }
	genDouble := func() any { return rng.NormFloat64() }
	// Order matters: the generator is shared, as in the official test.
	cases := []struct {
		name  string
		tuple Tuple
	}{
		{"Simple", Tuple{testUUID, "foobarbaz", 1234, nil}},
		{"Namespaces", Tuple{testUUID, "github", "com", "apple", "foundationdb", "tree"}},
		{"ManyStrings", mk(genString, 8)},
		{"ManyStringsNil", mk(genStringNil, 8)},
		{"ManyBytes", mk(genBytes, 20)},
		{"ManyBytesNil", mk(genBytesNil, 20)},
		{"LargeBytes", Tuple{testUUID, bytes.Repeat([]byte("abcd"), 20)}},
		{"LargeBytesNil", Tuple{testUUID, bytes.Repeat([]byte{0xFF, 0x0, 0xFF}, 20)}},
		{"Integers", mk(genInt, 20)},
		{"Floats", mk(genFloat, 20)},
		{"Doubles", mk(genDouble, 20)},
		{"UUIDs", Tuple{testUUID, true, testUUID, false, testUUID, true, testUUID, false, testUUID, true}},
		{"NilCases", Tuple{"\x00", "\x00\xFF", "\x00\x00\x00", "\xFF\x00", ""}},
		{"Nested", Tuple{testUUID, mk(genInt, 4), nil, mk(genBytes, 4), nil, mk(genDouble, 4), nil}},
	}
	names := make([]string, 0, len(golden))
	for _, c := range cases {
		names = append(names, c.name)
		want, ok := golden[c.name]
		if !ok {
			t.Fatalf("golden file has no %s", c.name)
		}
		got, err := c.tuple.Pack()
		if err != nil || !bytes.Equal(got, want) {
			t.Errorf("%s: Pack = %x, %v; want %x", c.name, got, err, want)
			continue
		}
		dec, err := Unpack(got)
		if err != nil {
			t.Errorf("%s: Unpack: %v", c.name, err)
			continue
		}
		again, err := dec.Pack()
		if err != nil || !bytes.Equal(again, want) {
			t.Errorf("%s: repack of unpacked tuple differs", c.name)
		}
	}
	if len(names) != len(golden) {
		t.Errorf("golden file has %d cases, tested %v", len(golden), slices.Sorted(slices.Values(names)))
	}
}

var benchTuple = Tuple{"users", int64(123456), []byte("profile\x00photo")}

func BenchmarkPack(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		if _, err := benchTuple.Pack(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAppendPack(b *testing.B) {
	b.ReportAllocs()
	buf := make([]byte, 0, 64)
	for b.Loop() {
		if _, err := benchTuple.AppendPack(buf[:0]); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkUnpack(b *testing.B) {
	key, err := benchTuple.Pack()
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := Unpack(key); err != nil {
			b.Fatal(err)
		}
	}
}
