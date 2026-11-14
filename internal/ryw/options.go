package ryw

import (
	"bytes"
	"encoding/binary"

	"github.com/can1357/fdbgo/internal/fdberr"
	"github.com/can1357/fdbgo/internal/kv"
	"github.com/can1357/fdbgo/internal/options"
)

var (
	// MetadataVersionRequiredValue is the only operand
	// allowed for versionstamping kv.MetadataVersionKey.
	MetadataVersionRequiredValue = make([]byte, 14)
	// ReadConflictRangePrefix is the special key prefix reporting read conflict ranges.
	ReadConflictRangePrefix = []byte("\xff\xff/transaction/read_conflict_range/")
	// WriteConflictRangePrefix is the special key prefix reporting write conflict ranges.
	WriteConflictRangePrefix = []byte("\xff\xff/transaction/write_conflict_range/")
)

const (
	// sizeofKeyRangeRef is the per-range overhead in approximate size accounting.
	sizeofKeyRangeRef = 32
	// sizeofMutationRef is the per-mutation overhead in approximate size accounting.
	sizeofMutationRef = 56
)

// emptyKey is the present empty key.
var emptyKey = []byte{}

// AtomicOptions captures read-version and operand-format details at the client boundary.
type AtomicOptions struct {
	// ReadVersion is used for local versionstamp placeholders when HasReadVersion.
	ReadVersion    int64
	HasReadVersion bool
	// LegacyVersionstamps accounts for canonical trailers that disappear or
	// shrink on 5.0/5.1, without widening ordinary key/value limits.
	LegacyVersionstamps bool
}

// RywOptions are the transaction options the read-your-writes layer
// interprets itself, plus raw_access for key size limits.
type RywOptions struct {
	// ReadAheadDisabled is read_ahead_disable.
	ReadAheadDisabled bool
	// ReadSystemKeys is read_system_keys / access_system_keys: reads may reach \xff\xff.
	ReadSystemKeys bool
	// WriteSystemKeys is access_system_keys: writes may reach \xff\xff.
	WriteSystemKeys bool
	// NextWriteNoWriteConflictRange is consumed by the next write.
	NextWriteNoWriteConflictRange bool
	// UsedDuringCommitProtectionDisabled is used_during_commit_protection_disable.
	UsedDuringCommitProtectionDisabled bool
	// SpecialKeySpaceRelaxed is special_key_space_relaxed.
	SpecialKeySpaceRelaxed bool
	// SpecialKeySpaceEnableWrites is special_key_space_enable_writes.
	SpecialKeySpaceEnableWrites bool
	// SnapshotRywEnabled: snapshot reads see this transaction's writes while
	// positive (snapshot_ryw_enable/disable counter).
	SnapshotRywEnabled int32
	// BypassUnreadable: Get may read keys made unreadable by versionstamp operations.
	BypassUnreadable bool
	// RawAccess: user keys may include an extra eight-byte tenant prefix.
	RawAccess bool
	// ReadYourWritesDisabled is read_your_writes_disable: operations go
	// straight to the native transaction lists.
	ReadYourWritesDisabled bool
}

// NewRywOptions returns the defaults, starting from the database's
// snapshot-RYW counter (1 unless changed by database options).
func NewRywOptions(snapshotRywEnabled int32) RywOptions {
	return RywOptions{SnapshotRywEnabled: snapshotRywEnabled}
}

// Apply sets the transaction option code (options.Transaction*)
// handled here; other options are ignored (left to the native transaction).
// read_your_writes_disable has a precondition and goes through Ryw.SetOption.
func (o *RywOptions) Apply(code int) {
	switch code {
	case options.TransactionReadAheadDisable:
		o.ReadAheadDisabled = true
	case options.TransactionNextWriteNoWriteConflictRange:
		o.NextWriteNoWriteConflictRange = true
	case options.TransactionAccessSystemKeys:
		o.ReadSystemKeys = true
		o.WriteSystemKeys = true
		o.RawAccess = true
	case options.TransactionReadSystemKeys:
		o.ReadSystemKeys = true
		o.RawAccess = true
	case options.TransactionSnapshotRYWEnable:
		o.SnapshotRywEnabled++
	case options.TransactionSnapshotRYWDisable:
		o.SnapshotRywEnabled--
	case options.TransactionUsedDuringCommitProtectionDisable:
		o.UsedDuringCommitProtectionDisabled = true
	case options.TransactionSpecialKeySpaceRelaxed:
		o.SpecialKeySpaceRelaxed = true
	case options.TransactionSpecialKeySpaceEnableWrites:
		o.SpecialKeySpaceEnableWrites = true
	case options.TransactionBypassUnreadable:
		o.BypassUnreadable = true
	case options.TransactionRawAccess:
		o.RawAccess = true
	}
}

// MaxReadKey is the exclusive upper bound of readable keys. The result is
// shared and must not be modified.
func (o *RywOptions) MaxReadKey() []byte {
	if o.ReadSystemKeys {
		return kv.SpecialKeysBegin
	}
	return kv.SystemKeysBegin
}

// MaxWriteKey is the exclusive upper bound of writable keys. The result is
// shared and must not be modified.
func (o *RywOptions) MaxWriteKey() []byte {
	if o.WriteSystemKeys {
		return kv.SpecialKeysBegin
	}
	return kv.SystemKeysBegin
}

// takeWriteConflictDisabled consumes NextWriteNoWriteConflictRange.
func (o *RywOptions) takeWriteConflictDisabled() bool {
	v := o.NextWriteNoWriteConflictRange
	o.NextWriteNoWriteConflictRange = false
	return v
}

// maxWriteKeySize is the largest allowed size of a written key.
func maxWriteKeySize(key []byte, rawAccess bool) int {
	if bytes.HasPrefix(key, kv.SystemKeysBegin) {
		return kv.SystemKeySizeLimit
	}
	if rawAccess {
		return kv.MaxReadKeySize(key)
	}
	return kv.KeySizeLimit
}

// MaxReadKeySize is the largest size of a stored key, for reads and clears.
func MaxReadKeySize(key []byte) int { return kv.MaxReadKeySize(key) }

// truncateKey copies key, shortened to an equivalent bound if it is longer
// than any stored key (to MaxReadKeySize + 1 bytes).
func truncateKey(key []byte) []byte {
	return dup(key[:min(len(key), MaxReadKeySize(key)+1)])
}

// isSpecialKey reports whether key is in the special key space.
func isSpecialKey(key []byte) bool {
	return bytes.Compare(key, kv.SpecialKeysBegin) >= 0 && bytes.Compare(key, kv.SpecialKeysEnd) < 0
}

// versionstampOffset is the little-endian offset in the last four bytes.
func versionstampOffset(key []byte) int {
	return int(int32(binary.LittleEndian.Uint32(key[len(key)-4:])))
}

// placeVersionstamp writes the big-endian version then the big-endian batch number.
func placeVersionstamp(dest []byte, version int64, transactionNumber uint16) {
	binary.BigEndian.PutUint64(dest, uint64(version))
	binary.BigEndian.PutUint16(dest[8:], transactionNumber)
}

// placeholderVersion is the version written into versionstamp templates
// before commit: the read version + 1 if known, else 0.
func placeholderVersion(readVersion int64, haveReadVersion bool) int64 {
	if haveReadVersion {
		return readVersion + 1
	}
	return 0
}

// versionstampKeyRange is every key a versionstamped
// key template can become.
func versionstampKeyRange(key []byte, readVersion int64, haveReadVersion bool, maxKey []byte) (kv.KeyRange, error) {
	if len(key) < 4 {
		return kv.KeyRange{}, fdberr.ClientInvalidOperation
	}
	pos := versionstampOffset(key)
	begin := dup(key[:len(key)-4])
	end := dup(key[:len(key)-3])
	end[len(end)-1] = 0
	if pos < 0 || pos+10 > len(begin) {
		return kv.KeyRange{}, fdberr.ClientInvalidOperation
	}
	placeVersionstamp(begin[pos:], placeholderVersion(readVersion, haveReadVersion), 0)
	for i := pos; i < pos+10; i++ {
		end[i] = 0xff
	}
	if bytes.Compare(end, maxKey) >= 0 {
		end = dup(maxKey)
	}
	if bytes.Compare(begin, end) > 0 {
		return kv.KeyRange{}, fdberr.InvertedRange
	}
	return kv.KeyRange{Begin: begin, End: end}, nil
}

// transformVersionstampKey writes a placeholder versionstamp into a copy of the template.
func transformVersionstampKey(key []byte, version int64, transactionNumber uint16) ([]byte, error) {
	if len(key) < 4 {
		return nil, fdberr.ClientInvalidOperation
	}
	pos := versionstampOffset(key)
	if pos < 0 || pos+10 > len(key) {
		return nil, fdberr.ClientInvalidOperation
	}
	k := dup(key)
	placeVersionstamp(k[pos:], version, transactionNumber)
	return k, nil
}

// writeRoute is where a validated write goes.
type writeRoute uint8

const (
	// routeApply: apply it, with or without a write conflict range.
	routeApply writeRoute = iota
	// routeSpecial: it addresses the special key space, which handles it instead.
	routeSpecial
	// routeSkip: it cannot affect any stored key and is dropped.
	routeSkip
)

// atomicWrite is a validated atomic operation.
type atomicWrite struct {
	// key is a copy of the key to write: the template with a placeholder
	// versionstamp for SetVersionstampedKey.
	key []byte
	// op is the wire mutation type selected by the client.
	op          uint8
	addConflict bool
	// For SetVersionstampedKey: the untransformed template and the range it may land in.
	template []byte
	vsRange  kv.KeyRange
}

// validateSet performs the checks of Set that apply with and without read-your-writes.
func validateSet(o *RywOptions, key []byte, usedDuringCommit func() bool) (writeRoute, bool, error) {
	if bytes.Equal(key, kv.MetadataVersionKey) {
		return 0, false, fdberr.ClientInvalidOperation
	}
	if isSpecialKey(key) {
		return routeSpecial, false, nil
	}
	addConflict := !o.takeWriteConflictDisabled()
	if usedDuringCommit() {
		return 0, false, fdberr.UsedDuringCommit
	}
	if bytes.Compare(key, o.MaxWriteKey()) >= 0 {
		return 0, false, fdberr.KeyOutsideLegalRange
	}
	return routeApply, addConflict, nil
}

// validateSetSizes checks the key and value size limits of set.
func validateSetSizes(o *RywOptions, key, value []byte) error {
	if len(key) > maxWriteKeySize(key, o.RawAccess) {
		return fdberr.KeyTooLarge
	}
	if len(value) > kv.ValueSizeLimit {
		return fdberr.ValueTooLarge
	}
	return nil
}

// validateClearRange performs the checks of ClearRange, including libfdb_c's
// inverted_range check, that apply with and without read-your-writes.
func validateClearRange(o *RywOptions, begin, end []byte, usedDuringCommit func() bool) (writeRoute, bool, error) {
	if bytes.Compare(begin, end) > 0 {
		return 0, false, fdberr.InvertedRange
	}
	addConflict := !o.takeWriteConflictDisabled()
	if usedDuringCommit() {
		return 0, false, fdberr.UsedDuringCommit
	}
	if bytes.Compare(begin, kv.SpecialKeysBegin) >= 0 && bytes.Compare(end, kv.SpecialKeysEnd) <= 0 {
		return routeSpecial, false, nil
	}
	max := o.MaxWriteKey()
	if bytes.Compare(begin, max) > 0 || bytes.Compare(end, max) > 0 {
		return 0, false, fdberr.KeyOutsideLegalRange
	}
	return routeApply, addConflict, nil
}

// truncateClearRange returns the equivalent range a clear applies to after
// shortening over-long keys (copies); ok is false if it is empty.
func truncateClearRange(begin, end []byte) (kv.KeyRange, bool) {
	b, e := truncateKey(begin), truncateKey(end)
	return kv.KeyRange{Begin: b, End: e}, bytes.Compare(b, e) < 0
}

// validateClear performs the checks of Clear that apply with and without read-your-writes.
func validateClear(o *RywOptions, key []byte, usedDuringCommit func() bool) (writeRoute, bool, error) {
	addConflict := !o.takeWriteConflictDisabled()
	if usedDuringCommit() {
		return 0, false, fdberr.UsedDuringCommit
	}
	if isSpecialKey(key) {
		return routeSpecial, false, nil
	}
	if bytes.Compare(key, o.MaxWriteKey()) >= 0 {
		return 0, false, fdberr.KeyOutsideLegalRange
	}
	if len(key) > MaxReadKeySize(key) {
		return routeSkip, false, nil
	}
	return routeApply, addConflict, nil
}

// validateAtomicOp performs the checks and key rewriting of AtomicOp that
// apply with and without read-your-writes.
func validateAtomicOp(o *RywOptions, key, param []byte, wireOp uint8, a AtomicOptions,
	usedDuringCommit func() bool) (atomicWrite, error) {
	addConflict := !o.takeWriteConflictDisabled()
	if usedDuringCommit() {
		return atomicWrite{}, fdberr.UsedDuringCommit
	}
	if bytes.Equal(key, kv.MetadataVersionKey) {
		if wireOp != MutationSetVersionstampedValue || !bytes.Equal(param, MetadataVersionRequiredValue) {
			return atomicWrite{}, fdberr.ClientInvalidOperation
		}
	} else if bytes.Compare(key, o.MaxWriteKey()) >= 0 {
		return atomicWrite{}, fdberr.KeyOutsideLegalRange
	}
	if wireOp >= MutationMaxAtomicOp || !isAtomicOp(wireOp) {
		return atomicWrite{}, fdberr.InvalidMutationType
	}
	keyLimit, valueLimit := maxWriteKeySize(key, o.RawAccess && !a.LegacyVersionstamps), kv.ValueSizeLimit
	if a.LegacyVersionstamps {
		switch wireOp {
		case MutationSetVersionstampedKey:
			keyLimit += 2
		case MutationSetVersionstampedValue:
			valueLimit += 4
		}
	}
	if len(key) > keyLimit {
		return atomicWrite{}, fdberr.KeyTooLarge
	}
	if len(param) > valueLimit {
		return atomicWrite{}, fdberr.ValueTooLarge
	}
	w := atomicWrite{op: wireOp}
	if w.op == MutationSetVersionstampedKey {
		r, err := versionstampKeyRange(key, a.ReadVersion, a.HasReadVersion, o.MaxReadKey())
		if err != nil {
			return atomicWrite{}, err
		}
		addConflict = false
		k, err := transformVersionstampKey(key, placeholderVersion(a.ReadVersion, a.HasReadVersion), 0)
		if err != nil {
			return atomicWrite{}, err
		}
		w.key, w.template, w.vsRange = k, dup(key), r
	} else {
		w.key = dup(key)
	}
	if w.op == MutationSetVersionstampedValue {
		if len(param) < 4 {
			return atomicWrite{}, fdberr.ClientInvalidOperation
		}
		if pos := versionstampOffset(param); pos < 0 || pos+10 > len(param)-4 {
			return atomicWrite{}, fdberr.ClientInvalidOperation
		}
	}
	w.addConflict = addConflict
	return w, nil
}

// validateReadConflictRange performs the checks and key
// shortening (copies); ok is false if the range is empty.
func validateReadConflictRange(o *RywOptions, begin, end []byte, usedDuringCommit func() bool) (kv.KeyRange, bool, error) {
	if usedDuringCommit() {
		return kv.KeyRange{}, false, fdberr.UsedDuringCommit
	}
	max := o.MaxReadKey()
	if (bytes.Compare(begin, max) > 0 || bytes.Compare(end, max) > 0) &&
		(!bytes.Equal(begin, kv.MetadataVersionKey) || !bytes.Equal(end, kv.MetadataVersionKeyEnd)) {
		return kv.KeyRange{}, false, fdberr.KeyOutsideLegalRange
	}
	r, ok := truncateClearRange(begin, end)
	return r, ok, nil
}

// validateWriteConflictRange performs the checks and key
// shortening (copies); ok is false if the range is empty.
func validateWriteConflictRange(o *RywOptions, begin, end []byte, usedDuringCommit func() bool) (kv.KeyRange, bool, error) {
	if usedDuringCommit() {
		return kv.KeyRange{}, false, fdberr.UsedDuringCommit
	}
	max := o.MaxWriteKey()
	if bytes.Compare(begin, max) > 0 || bytes.Compare(end, max) > 0 {
		return kv.KeyRange{}, false, fdberr.KeyOutsideLegalRange
	}
	r, ok := truncateClearRange(begin, end)
	return r, ok, nil
}

// ValidateGet checks a Get key; false means the key is too long to exist and
// the result is absent without reading.
func ValidateGet(o *RywOptions, key []byte) (bool, error) {
	if bytes.Compare(key, o.MaxReadKey()) >= 0 && !bytes.Equal(key, kv.MetadataVersionKey) {
		return false, fdberr.KeyOutsideLegalRange
	}
	return len(key) <= MaxReadKeySize(key), nil
}

// ValidateGetKey normalizes and checks a GetKey selector without copying its key.
func ValidateGetKey(o *RywOptions, sel *kv.KeySelector) error {
	sel.Normalize()
	if bytes.Compare(sel.Key, o.MaxReadKey()) > 0 {
		return fdberr.KeyOutsideLegalRange
	}
	return nil
}

// ValidateGetRange checks a GetRange and normalizes OrEqual away; false
// means the result is empty without reading.
func ValidateGetRange(o *RywOptions, begin, end *kv.KeySelector, limits kv.Limits) (bool, error) {
	begin.Normalize()
	end.Normalize()
	max := o.MaxReadKey()
	if bytes.Compare(begin.Key, max) > 0 || bytes.Compare(end.Key, max) > 0 {
		return false, fdberr.KeyOutsideLegalRange
	}
	if limits.IsReached() {
		return false, nil
	}
	if !limits.IsValid() {
		return false, fdberr.RangeLimitsInvalid
	}
	begin.RemoveOrEqual()
	end.RemoveOrEqual()
	return !(begin.Offset >= end.Offset && bytes.Compare(begin.Key, end.Key) >= 0), nil
}

// validateWatch checks a watch key.
func validateWatch(o *RywOptions, key []byte) error {
	if bytes.Compare(key, kv.SpecialKeysBegin) >= 0 ||
		(bytes.Compare(key, o.MaxReadKey()) >= 0 && !bytes.Equal(key, kv.MetadataVersionKey)) {
		return fdberr.KeyOutsideLegalRange
	}
	if len(key) > maxWriteKeySize(key, o.RawAccess) {
		return fdberr.KeyTooLarge
	}
	return nil
}

// ClipReadKey clips a key resolved by a read-your-writes-disabled read:
// keys past the readable keyspace become MaxReadKey.
func ClipReadKey(o *RywOptions, key []byte) []byte {
	if max := o.MaxReadKey(); bytes.Compare(key, max) > 0 {
		return dup(max)
	}
	return key
}

// ClipReverseEnd is the end rewrite of read-your-writes-disabled reverse reads
// whose end offset is above 1, given the native resolution of the end selector.
func ClipReverseEnd(o *RywOptions, resolvedEnd []byte) kv.KeySelector {
	if max := o.MaxReadKey(); bytes.Compare(resolvedEnd, max) > 0 {
		return kv.FirstGreaterOrEqual(dup(max))
	}
	return kv.FirstGreaterOrEqual(resolvedEnd)
}

// ClipRangeResult clips read-your-writes-disabled forward range reads to
// the readable keyspace.
func ClipRangeResult(o *RywOptions, result kv.RangeResult, reverse bool) kv.RangeResult {
	max := o.MaxReadKey()
	n := len(result.Data)
	if reverse || n == 0 || bytes.Compare(result.Data[n-1].Key, max) < 0 {
		return result
	}
	keep := 0
	for i := n - 1; i >= 0; i-- {
		if bytes.Compare(result.Data[i].Key, max) < 0 {
			keep = i + 1
			break
		}
	}
	return kv.RangeResult{Data: result.Data[:keep]}
}

// approxSet is the approximate size added by set.
func approxSet(key, value []byte, addConflict bool) int64 {
	k := int64(len(key))
	n := k + int64(len(value)) + sizeofMutationRef
	if addConflict {
		n += sizeofKeyRangeRef + 2*k + 1
	}
	return n
}

// approxClearRange is the approximate size added by a range clear.
func approxClearRange(begin, end []byte, addConflict bool) int64 {
	r := int64(len(begin) + len(end))
	n := r + sizeofMutationRef
	if addConflict {
		n += sizeofKeyRangeRef + r
	}
	return n
}

// approxAtomic is the approximate size added by an atomic operation
// (writtenKey is the rewritten key).
func approxAtomic(key, writtenKey, param []byte, addConflict bool) int64 {
	n := int64(len(writtenKey)+len(param)) + sizeofMutationRef
	if addConflict {
		n += sizeofKeyRangeRef + 2*int64(len(key)) + 1
	}
	return n
}

// approxConflictRange is the approximate size added by an explicit conflict range.
func approxConflictRange(begin, end []byte) int64 {
	return int64(len(begin)+len(end)) + sizeofKeyRangeRef
}
