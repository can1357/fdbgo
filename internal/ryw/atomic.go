package ryw

import "bytes"

// Mutation type codes.
// They are the wire codes of wire.Mutation.Kind and equal the fdb_c
// FDBMutationType codes of the atomic operations.
const (
	// MutationSetValue sets a key to a value.
	MutationSetValue uint8 = 0
	// MutationClearRange clears a key range.
	MutationClearRange uint8 = 1
	// MutationAddValue is the add atomic operation.
	MutationAddValue uint8 = 2
	// MutationAnd is legacy bit_and; a missing value counts as zero bytes.
	MutationAnd uint8 = 6
	// MutationOr is the bit_or atomic operation.
	MutationOr uint8 = 7
	// MutationXor is the bit_xor atomic operation.
	MutationXor uint8 = 8
	// MutationAppendIfFits is the append_if_fits atomic operation.
	MutationAppendIfFits uint8 = 9
	// MutationMax is the max atomic operation.
	MutationMax uint8 = 12
	// MutationMin is legacy min; a missing value counts as zero bytes.
	MutationMin uint8 = 13
	// MutationSetVersionstampedKey is the set_versionstamped_key atomic operation.
	MutationSetVersionstampedKey uint8 = 14
	// MutationSetVersionstampedValue is the set_versionstamped_value atomic operation.
	MutationSetVersionstampedValue uint8 = 15
	// MutationByteMin is the byte_min atomic operation.
	MutationByteMin uint8 = 16
	// MutationByteMax is the byte_max atomic operation.
	MutationByteMax uint8 = 17
	// MutationMinV2 is min where a missing value yields the operand.
	MutationMinV2 uint8 = 18
	// MutationAndV2 is bit_and where a missing value yields the operand.
	MutationAndV2 uint8 = 19
	// MutationCompareAndClear is the compare_and_clear atomic operation.
	MutationCompareAndClear uint8 = 20
	// MutationMaxAtomicOp is one past the last valid mutation type.
	MutationMaxAtomicOp uint8 = 24
)

// appendIfFitsLimit is the value size limit AppendIfFits keeps its result within.
const appendIfFitsLimit = 100_000

// dup copies b into a new, never nil slice: a present value.
func dup(b []byte) []byte {
	c := make([]byte, len(b))
	copy(c, b)
	return c
}

// dupAfter copies key and returns the copy and keyAfter(key) in one buffer.
func dupAfter(key []byte) (k, after []byte) {
	after = make([]byte, len(key)+1)
	copy(after, key)
	return after[:len(key):len(key)], after
}

// applyAtomic applies atomic mutation op to existing (nil =
// absent) with operand param. The result is never nil except for
// CompareAndClear, where nil means the key is cleared. op must be one of the
// value-transforming atomic ops.
func applyAtomic(op uint8, existing, param []byte) []byte {
	switch op {
	case MutationAddValue:
		return doLittleEndianAdd(existing, param)
	case MutationAnd:
		return doAnd(existing, param)
	case MutationAndV2:
		return doAndV2(existing, param)
	case MutationOr:
		return doBitwise(existing, param, func(e, o byte) byte { return e | o })
	case MutationXor:
		return doBitwise(existing, param, func(e, o byte) byte { return e ^ o })
	case MutationAppendIfFits:
		return doAppendIfFits(existing, param)
	case MutationMax:
		return doMax(existing, param)
	case MutationMin:
		return doMin(existing, param)
	case MutationMinV2:
		return doMinV2(existing, param)
	case MutationByteMin:
		return doByteMin(existing, param)
	case MutationByteMax:
		return doByteMax(existing, param)
	case MutationCompareAndClear:
		return doCompareAndClear(existing, param)
	}
	panic("ryw: not an atomic mutation type")
}

// resized copies existing truncated or zero-extended to n bytes.
func resized(existing []byte, n int) []byte {
	v := make([]byte, n)
	copy(v, existing)
	return v
}

// doLittleEndianAdd is a little-endian unsigned add; the result has the
// operand's length (carry out is dropped).
func doLittleEndianAdd(existing, other []byte) []byte {
	if len(existing) == 0 || len(other) == 0 {
		return dup(other)
	}
	out := make([]byte, len(other))
	carry := 0
	for i, o := range other {
		sum := int(o) + carry
		if i < len(existing) {
			sum += int(existing[i])
		}
		carry = sum >> 8
		out[i] = byte(sum)
	}
	return out
}

// doAnd is bitwise and; a missing or short existing value counts as zero bytes.
func doAnd(existing, other []byte) []byte {
	out := make([]byte, len(other))
	for i, o := range other {
		if i < len(existing) {
			out[i] = o & existing[i]
		}
	}
	return out
}

// doAndV2 is doAnd where a missing existing value yields the operand.
func doAndV2(existing, other []byte) []byte {
	if existing == nil {
		return dup(other)
	}
	return doAnd(existing, other)
}

// doBitwise is the shape of doOr/doXor: the operand passes through if either
// side is empty; the result has the operand's length.
func doBitwise(existing, other []byte, f func(e, o byte) byte) []byte {
	if len(existing) == 0 || len(other) == 0 {
		return dup(other)
	}
	out := make([]byte, len(other))
	for i, o := range other {
		if i < len(existing) {
			out[i] = f(existing[i], o)
		} else {
			out[i] = o
		}
	}
	return out
}

// doAppendIfFits appends the operand unless the result would exceed the value
// size limit, in which case the value is unchanged.
func doAppendIfFits(existing, other []byte) []byte {
	if len(existing) == 0 {
		return dup(other)
	}
	if len(other) == 0 || len(existing)+len(other) > appendIfFitsLimit {
		return dup(existing)
	}
	out := make([]byte, 0, len(existing)+len(other))
	return append(append(out, existing...), other...)
}

// doMax is little-endian unsigned max; the result has the operand's length
// (existing truncated or zero-extended).
func doMax(existing, other []byte) []byte {
	if len(existing) == 0 || len(other) == 0 {
		return dup(other)
	}
	n := min(len(existing), len(other))
	for _, b := range other[n:] {
		if b != 0 {
			return dup(other)
		}
	}
	for i := n - 1; i >= 0; i-- {
		if other[i] > existing[i] {
			return dup(other)
		} else if other[i] < existing[i] {
			return resized(existing, len(other))
		}
	}
	return dup(other)
}

// doMin is little-endian unsigned min; the result has the operand's length
// and a missing value counts as zero-length.
func doMin(existing, other []byte) []byte {
	if len(other) == 0 {
		return []byte{}
	}
	n := min(len(existing), len(other))
	for _, b := range other[n:] {
		if b != 0 {
			return resized(existing, len(other))
		}
	}
	for i := n - 1; i >= 0; i-- {
		if other[i] > existing[i] {
			return resized(existing, len(other))
		} else if other[i] < existing[i] {
			return dup(other)
		}
	}
	return dup(other)
}

// doMinV2 is doMin where a missing existing value yields the operand.
func doMinV2(existing, other []byte) []byte {
	if existing == nil {
		return dup(other)
	}
	return doMin(existing, other)
}

// doByteMax is lexicographic max; a missing existing value yields the operand.
func doByteMax(existing, other []byte) []byte {
	if existing != nil && bytes.Compare(existing, other) > 0 {
		return dup(existing)
	}
	return dup(other)
}

// doByteMin is lexicographic min; a missing existing value yields the operand.
func doByteMin(existing, other []byte) []byte {
	if existing != nil && bytes.Compare(existing, other) < 0 {
		return dup(existing)
	}
	return dup(other)
}

// doCompareAndClear clears the key (nil) if it is absent or equals the
// operand, otherwise leaves it unchanged.
func doCompareAndClear(existing, other []byte) []byte {
	if existing != nil && !bytes.Equal(existing, other) {
		return dup(existing)
	}
	return nil
}
