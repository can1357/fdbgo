package fdb

import (
	"bytes"
	"testing"
)

func TestStrinc(t *testing.T) {
	cases := []struct{ in, want []byte }{
		{[]byte("a"), []byte("b")},
		{[]byte("a\xff\xff"), []byte("b")},
		{[]byte{0x15, 0x80}, []byte{0x15, 0x81}},
		{[]byte{0x15, 0xfe, 0xff}, []byte{0x15, 0xff}},
		{[]byte{0xc3, 0xa9, 0xff}, []byte{0xc3, 0xaa}},
	}
	for _, c := range cases {
		got, err := Strinc(c.in)
		if err != nil || !bytes.Equal(got, c.want) {
			t.Errorf("Strinc(%x) = %x, %v; want %x", c.in, got, err, c.want)
		}
	}
	for _, in := range [][]byte{nil, {}, {0xff}, {0xff, 0xff}} {
		if _, err := Strinc(in); err == nil {
			t.Errorf("Strinc(%x) succeeded", in)
		}
	}
	r, err := PrefixRange([]byte{0x15, 0x80})
	if err != nil || !bytes.Equal(r.End, []byte{0x15, 0x81}) {
		t.Errorf("PrefixRange = %x, %v", r.End, err)
	}
}
