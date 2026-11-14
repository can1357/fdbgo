package ryw

import (
	"bytes"
	"testing"
)

func ap(t *testing.T, op uint8, existing, param []byte, want ...byte) {
	t.Helper()
	got := applyAtomic(op, existing, param)
	if got == nil || !bytes.Equal(got, want) {
		t.Fatalf("op %d on %v with %v = %v, want %v", op, existing, param, got, want)
	}
}

func b(s ...byte) []byte { return append([]byte{}, s...) }

func TestAtomicAdd(t *testing.T) {
	ap(t, MutationAddValue, b(0xff, 0x00), b(0x01, 0x00), 0x00, 0x01)
	ap(t, MutationAddValue, b(0xff, 0xff), b(0x01, 0x00), 0x00, 0x00)
	ap(t, MutationAddValue, b(0xff, 0xff, 0x07), b(0x01), 0x00)
	ap(t, MutationAddValue, b(0xff), b(0x01, 0x00, 0x00), 0x00, 0x01, 0x00)
	ap(t, MutationAddValue, nil, b(5, 6), 5, 6)
	ap(t, MutationAddValue, b(1), b())
}

func TestAtomicAndVariants(t *testing.T) {
	ap(t, MutationAnd, nil, b(0xff, 0xff), 0, 0)
	ap(t, MutationAndV2, nil, b(0xff, 0xff), 0xff, 0xff)
	ap(t, MutationAndV2, b(), b(0xff, 0xff), 0, 0)
	ap(t, MutationAndV2, b(0x0f, 0xf0, 0xff), b(0xff, 0xff), 0x0f, 0xf0)
	if normalizeAtomicOp(MutationAnd) != MutationAndV2 || normalizeAtomicOp(MutationMin) != MutationMinV2 ||
		normalizeAtomicOp(MutationOr) != MutationOr {
		t.Fatal("normalizeAtomicOp")
	}
}

func TestAtomicOrXor(t *testing.T) {
	ap(t, MutationOr, b(0x01), b(0x10, 0x22), 0x11, 0x22)
	ap(t, MutationXor, b(0xff, 0x01, 0x09), b(0x0f, 0x01), 0xf0, 0x00)
	ap(t, MutationOr, nil, b(3), 3)
	ap(t, MutationXor, b(3), b())
}

func TestAtomicAppendIfFits(t *testing.T) {
	ap(t, MutationAppendIfFits, []byte("ab"), []byte("cd"), []byte("abcd")...)
	ap(t, MutationAppendIfFits, nil, []byte("cd"), []byte("cd")...)
	ap(t, MutationAppendIfFits, []byte("ab"), b(), []byte("ab")...)
	big := bytes.Repeat([]byte{1}, appendIfFitsLimit)
	ap(t, MutationAppendIfFits, big, []byte("x"), big...)
	almost := bytes.Repeat([]byte{1}, appendIfFitsLimit-1)
	if n := len(applyAtomic(MutationAppendIfFits, almost, []byte("x"))); n != appendIfFitsLimit {
		t.Fatalf("len %d", n)
	}
}

func TestAtomicMaxMin(t *testing.T) {
	ap(t, MutationMax, b(5, 0), b(4, 0), 5, 0)
	ap(t, MutationMax, b(5, 0), b(4, 1), 4, 1)
	ap(t, MutationMax, b(5), b(4, 0, 0), 5, 0, 0)
	ap(t, MutationMax, b(5, 0, 9), b(4), 5)
	ap(t, MutationMax, nil, b(1), 1)
	ap(t, MutationMinV2, b(5, 0), b(4, 0), 4, 0)
	ap(t, MutationMinV2, b(5), b(4, 1), 5, 0)
	ap(t, MutationMinV2, b(5, 1), b(6), 5)
	ap(t, MutationMinV2, nil, b(7, 7), 7, 7)
	ap(t, MutationMin, nil, b(7, 7), 0, 0)
	ap(t, MutationMin, b(1), b())
}

func TestAtomicByteMinMax(t *testing.T) {
	ap(t, MutationByteMax, []byte("b"), []byte("ab"), []byte("b")...)
	ap(t, MutationByteMax, []byte("a"), []byte("ab"), []byte("ab")...)
	ap(t, MutationByteMin, []byte("a"), []byte("ab"), []byte("a")...)
	ap(t, MutationByteMin, []byte("b"), []byte("ab"), []byte("ab")...)
	ap(t, MutationByteMin, nil, []byte("z"), []byte("z")...)
	ap(t, MutationByteMax, nil, b())
}

func TestAtomicCompareAndClear(t *testing.T) {
	if applyAtomic(MutationCompareAndClear, []byte("x"), []byte("x")) != nil {
		t.Fatal("equal value not cleared")
	}
	ap(t, MutationCompareAndClear, []byte("y"), []byte("x"), 'y')
	if applyAtomic(MutationCompareAndClear, nil, []byte("x")) != nil {
		t.Fatal("absent value not cleared")
	}
	if applyAtomic(MutationCompareAndClear, b(), b()) != nil {
		t.Fatal("empty equal value not cleared")
	}
}
