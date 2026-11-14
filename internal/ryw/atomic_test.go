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
	ap(t, MutationAnd, b(), b(0xff, 0xff), 0, 0)
	ap(t, MutationAnd, nil, b())
	ap(t, MutationAndV2, nil, b())
	ap(t, MutationAndV2, nil, b(0xff, 0xff), 0xff, 0xff)
	ap(t, MutationAndV2, b(), b(0xff, 0xff), 0, 0)
	ap(t, MutationAndV2, b(0x0f, 0xf0, 0xff), b(0xff, 0xff), 0x0f, 0xf0)
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
	ap(t, MutationMinV2, b(), b(7, 7), 0, 0)
	ap(t, MutationMinV2, nil, b())
	ap(t, MutationMin, nil, b(7, 7), 0, 0)
	ap(t, MutationMin, b(), b(7, 7), 0, 0)
	ap(t, MutationMin, nil, b())
	ap(t, MutationMin, b(1), b())
}

func TestAtomicWireSemanticsWithPendingRead(t *testing.T) {
	for _, op := range []struct {
		name          string
		kind          uint8
		missing       string
		populated     string
		populatedNext string
	}{
		{"and", MutationAnd, "\x00\x00", "\x30\x00", "\x10"},
		{"and_v2", MutationAndV2, "\x3f\x0f", "\x30\x00", "\x10"},
		{"min", MutationMin, "\x00\x00", "\xf0\x00", "\x17"},
		{"min_v2", MutationMinV2, "\x3f\x0f", "\xf0\x00", "\x17"},
	} {
		t.Run(op.name, func(t *testing.T) {
			for _, state := range []struct {
				name  string
				value []byte
			}{
				{"missing", nil},
				{"empty", []byte{}},
				{"populated", []byte{0xf0, 0x00}},
			} {
				t.Run(state.name, func(t *testing.T) {
					data := map[string][]byte{}
					if state.value != nil {
						data["key"] = state.value
					}
					db, release := fakeFromMap(data, false).gated(t)
					r := newRyw()
					defer r.Cancel()
					key := []byte("key")
					r.AtomicOp(key, []byte{0x3f, 0x0f}, op.kind, AtomicOptions{})
					first := capture(r.Get(db, key, false))

					// Mutate while the earlier read is blocked. Its immutable
					// write-map view must retain the first operand and wire op.
					mutated := make(chan struct{})
					go func() {
						r.AtomicOp(key, []byte{0x17}, op.kind, AtomicOptions{})
						close(mutated)
					}()
					<-mutated
					next := capture(r.Get(db, key, false))
					release()

					wantFirst, wantNext := "\x00\x00", "\x00"
					switch state.name {
					case "missing":
						wantFirst = op.missing
						if op.kind == MutationAndV2 || op.kind == MutationMinV2 {
							wantNext = "\x17"
						}
					case "populated":
						wantFirst, wantNext = op.populated, op.populatedNext
					}
					eqVal(t, must(t, first), wantFirst, true)
					eqVal(t, must(t, next), wantNext, true)
					eqVal(t, must(t, capture(r.Get(db, key, true))), wantNext, true)

					plan := must(t, capture(r.Commit()))
					eqPairs(t, rangesOf(plan.ReadConflicts), pair{"key", "key\x00"})
					eqPairs(t, rangesOf(plan.WriteConflicts), pair{"key", "key\x00"})
					committed := state.value
					for _, m := range plan.Mutations {
						if m.Kind == MutationSetValue {
							committed = m.Param2
						} else {
							committed = applyAtomic(m.Kind, committed, m.Param2)
						}
					}
					eqVal(t, committed, wantNext, true)
				})
			}
		})
	}
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
