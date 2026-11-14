package ryw

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"

	"github.com/can1357/fdbgo/internal/fdberr"
	"github.com/can1357/fdbgo/internal/kv"
	"github.com/can1357/fdbgo/internal/options"
	"github.com/can1357/fdbgo/internal/wire"
)

// CommitPlan is what a read-your-writes commit hands to the native transaction, in order.
type CommitPlan struct {
	Mutations      []wire.Mutation
	ReadConflicts  []kv.KeyRange
	WriteConflicts []kv.KeyRange
}

// SnapshotReader is what read-your-writes asks of the native transaction:
// snapshot reads at the transaction's read version without conflict tracking.
// StartReadVersion pins the read version before an operation
// that needs snapshot I/O returns to its caller. Results may alias
// reply buffers and are never modified.
type SnapshotReader interface {
	// StartReadVersion synchronously starts acquisition of the attempt's read version.
	StartReadVersion()
	// Get is a snapshot read of key; nil means absent.
	Get(ctx context.Context, key []byte) ([]byte, error)
	// GetKey is a snapshot key selector resolution.
	GetKey(ctx context.Context, sel kv.KeySelector) ([]byte, error)
	// GetRange is a snapshot range read.
	GetRange(ctx context.Context, begin, end kv.KeySelector, limits kv.Limits, reverse bool) (kv.RangeResult, error)
}

// WatchRegistrar registers watches with the native transaction.
type WatchRegistrar interface {
	// RegisterWatch arms a watch of key whose value in this transaction's
	// view is value (nil = absent): the channel delivers once the stored
	// value differs after this transaction commits, or with the commit's
	// error. It fails synchronously when no more watches may be outstanding
	// (too_many_watches). The channel delivers one outcome. The returned
	// completion function must be called with nil when a local write fires the
	// watch, or an error when it is cancelled. It synchronously releases its
	// reservation and cancels only its own storage wait. Repeated calls are
	// harmless.
	RegisterWatch(key, value []byte) (<-chan error, func(error), error)
}

// WatchReader is a native transaction that can both read and arm watches.
type WatchReader interface {
	SnapshotReader
	WatchRegistrar
}

// RangeExtent is a range read's extent, without its data, as needed for its
// conflict range.
type RangeExtent struct {
	// HasRows reports that First and Last are set.
	HasRows bool
	// First is the first key in result order, Last the last.
	First, Last    []byte
	More           bool
	ReadToBegin    bool
	ReadThroughEnd bool
}

// RangeExtentOf returns the extent of r.
func RangeExtentOf(r *kv.RangeResult) RangeExtent {
	e := RangeExtent{More: r.More, ReadToBegin: r.ReadToBegin, ReadThroughEnd: r.ReadThroughEnd}
	if n := len(r.Data); n > 0 {
		e.HasRows, e.First, e.Last = true, r.Data[0].Key, r.Data[n-1].Key
	}
	return e
}

// MappedSecondary is one secondary read of a mapped range row: a point read
// of Key when IsValue, else a forward range read of [Begin, End) with Result.
type MappedSecondary struct {
	IsValue    bool
	Key        []byte
	Begin, End kv.KeySelector
	Result     RangeExtent
}

// watchCell is a watch created by this transaction, its firing signal and
// the state of its setup.
type watchCell struct {
	mu sync.Mutex
	// value is the value read when the watch was created (if hasValue; nil = absent).
	value    []byte
	hasValue bool
	// set is the value this transaction wrote since (if hasSet).
	set    []byte
	hasSet bool

	once     sync.Once
	fired    chan struct{}
	err      error // published by closing fired
	complete func(error)
	// setupDone: the setup (value read and arming) finished, failed or was
	// cut short; done tracks it for commit.
	setupDone atomic.Bool
	armed     atomic.Bool
	done      pendingRead
}

// finish completes or cancels the watch, releasing its native reservation.
func (c *watchCell) finish(err error) {
	c.once.Do(func() {
		c.mu.Lock()
		c.err = err
		if c.complete != nil {
			c.complete(err)
		}
		close(c.fired)
		c.mu.Unlock()
		c.finishSetup(err, false)
	})
}

func (c *watchCell) fire() { c.finish(nil) }

// finishSetup ends the setup once; false if it had already ended.
func (c *watchCell) finishSetup(err error, aborted bool) bool {
	if !c.setupDone.CompareAndSwap(false, true) {
		return false
	}
	c.done.finish(err, aborted)
	return true
}

// isFired reports whether the watch has fired.
func (c *watchCell) isFired() bool {
	select {
	case <-c.fired:
		return true
	default:
		return false
	}
}

// optEqual compares optional values (nil = absent).
func optEqual(a, b []byte) bool { return (a == nil) == (b == nil) && bytes.Equal(a, b) }

// noWrites is the write map seen by snapshot reads that do not read this transaction's writes.
var noWrites = wtree{emptyWriteRoot}

// Ryw is all read-your-writes state of one transaction, apart from the
// native transaction, for one attempt at a time. It is safe for concurrent
// use: every method locks it.
type Ryw struct {
	mu      sync.Mutex
	options RywOptions
	// baseOptions are the options a new attempt starts from.
	baseOptions   RywOptions
	writes        writeMap
	cache         snapshotCache
	readConflicts readConflictSet
	watchMap      map[string][]*watchCell
	deferredErr   error
	approxSize    int64
	commitStarted bool
	// generation is bumped by every reset, so stale reads cannot touch the new attempt.
	generation uint64
	// reset is set to the error that ends this attempt.
	reset   *AttemptEnd
	reading *readTracker
	// versionstampKeys are the templates of versionstamped keys written so far.
	versionstampKeys [][]byte
	// native* are the lists already pushed to the native transaction
	// (by writeRangeToNative, or every write with read-your-writes disabled).
	nativeMutations      []wire.Mutation
	nativeWriteConflicts []kv.KeyRange
	nativeReadConflicts  []kv.KeyRange
	// committed* are the native
	// conflict ranges stashed by a read-your-writes-disabled commit.
	committedReadRanges  []kv.KeyRange
	committedWriteRanges []kv.KeyRange
}

// New returns the state of a new transaction with opts (also the options
// each new attempt starts from).
func New(opts RywOptions) *Ryw {
	return &Ryw{
		options:     opts,
		baseOptions: opts,
		writes:      newWriteMap(),
		cache:       newSnapshotCache(),
		reset:       newAttemptEnd(),
		reading:     &readTracker{},
	}
}

// Options returns the current options.
func (r *Ryw) Options() RywOptions {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.options
}

// SetOption sets an option code (options.Transaction*) that
// read-your-writes handles. Like every option call through the C API it is
// skipped once an error is deferred, and its own failure (setting
// read_your_writes_disable after reading or writing: client_invalid_operation)
// is deferred.
func (r *Ryw) SetOption(code int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.deferredErr != nil {
		return
	}
	if code == options.TransactionReadYourWritesDisable {
		if err := r.checkReadYourWritesDisable(); err != nil {
			r.deferredErr = err
			return
		}
		r.options.ReadYourWritesDisabled = true
		return
	}
	r.options.Apply(code)
}

// SetBaseOptions replaces the options new attempts start from (database defaults changed).
func (r *Ryw) SetBaseOptions(o RywOptions) {
	r.mu.Lock()
	r.baseOptions = o
	r.mu.Unlock()
}

// CheckReadYourWritesDisable is the read_your_writes_disable precondition:
// nothing read or written yet.
func (r *Ryw) CheckReadYourWritesDisable() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.checkReadYourWritesDisable()
}

func (r *Ryw) checkReadYourWritesDisable() error {
	if r.reading.wasAdded() || !r.cache.isEmpty() || !r.writes.isEmpty() {
		return fdberr.ClientInvalidOperation
	}
	return nil
}

// CheckUsedDuringCommit reports true once commit started;
// then it ends the attempt with used_during_commit unless that protection is disabled.
func (r *Ryw) CheckUsedDuringCommit() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.usedDuringCommit()
}

func (r *Ryw) usedDuringCommit() bool {
	if r.commitStarted && !r.options.UsedDuringCommitProtectionDisabled {
		r.reset.set(fdberr.UsedDuringCommit)
	}
	return r.commitStarted
}

// SetDeferredError records err for the next read or commit unless an error is
// already deferred.
func (r *Ryw) SetDeferredError(err error) {
	r.mu.Lock()
	if r.deferredErr == nil {
		r.deferredErr = err
	}
	r.mu.Unlock()
}

// DeferredError is the error a write recorded, reported by the next read or
// commit; nil if none.
func (r *Ryw) DeferredError() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.deferredErr
}

// ResetError is the error that ended this attempt, or nil.
func (r *Ryw) ResetError() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.reset.Err()
}

// ResetSignal returns this attempt's end, for racing native operations.
func (r *Ryw) ResetSignal() *AttemptEnd {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.reset
}

// ApproximateSize is the transaction's approximate size in bytes.
func (r *Ryw) ApproximateSize() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.approxSize
}

// Abort ends this attempt with err unless it already ended, e.g. on timeout:
// in-flight reads fail with err.
func (r *Ryw) Abort(err fdberr.Error) {
	r.mu.Lock()
	r.reset.set(err)
	r.mu.Unlock()
}

// Cancel makes pending and subsequent operations fail with transaction_cancelled.
func (r *Ryw) Cancel() { r.Abort(fdberr.TransactionCancelled) }

// Reset starts a new attempt. Operations of the old attempt
// fail with transaction_cancelled; options return to the attempt defaults.
func (r *Ryw) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reset.set(fdberr.TransactionCancelled)
	r.reset = newAttemptEnd()
	r.generation++
	r.cache = newSnapshotCache()
	r.writes = newWriteMap()
	r.readConflicts = readConflictSet{}
	r.versionstampKeys = nil
	r.nativeMutations = nil
	r.nativeWriteConflicts = nil
	r.nativeReadConflicts = nil
	r.committedReadRanges = nil
	r.committedWriteRanges = nil
	r.watchMap = nil
	r.reading = &readTracker{}
	r.approxSize = 0
	r.commitStarted = false
	r.deferredErr = nil
	r.options = r.baseOptions
}

// Set sets key to value. It reports true if key is in the special key space, which
// must handle it instead. Errors are deferred.
func (r *Ryw) Set(key, value []byte) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.deferredErr != nil {
		return false
	}
	special, err := r.set(key, value)
	if err != nil {
		r.deferredErr = err
	}
	return special
}

func (r *Ryw) set(key, value []byte) (bool, error) {
	route, addConflict, err := validateSet(&r.options, key, r.usedDuringCommit)
	if err != nil || route != routeApply {
		return route == routeSpecial, err
	}
	r.approxSize += approxSet(key, value, addConflict)
	if r.options.ReadYourWritesDisabled {
		return false, r.nativeSet(key, value, addConflict)
	}
	if err := validateSetSizes(&r.options, key, value); err != nil {
		return false, err
	}
	// One allocation for both; v stays non-nil (present) even when empty.
	buf := make([]byte, len(key)+len(value))
	copy(buf, key)
	copy(buf[len(key):], value)
	k, v := buf[:len(key):len(key)], buf[len(key):]
	if err := r.writes.mutate(k, MutationSetValue, v, addConflict); err != nil {
		return false, err
	}
	if len(r.watchMap) > 0 {
		r.triggerWatches(k, kv.KeyAfter(k), v, true)
	}
	return false, nil
}

// Clear clears key. It reports true if key is in the special key space,
// which must handle it instead. Errors are deferred.
func (r *Ryw) Clear(key []byte) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.deferredErr != nil {
		return false
	}
	route, addConflict, err := validateClear(&r.options, key, r.usedDuringCommit)
	if err != nil {
		r.deferredErr = err
		return false
	}
	if route != routeApply {
		return route == routeSpecial
	}
	if r.options.ReadYourWritesDisabled {
		r.nativeClearKey(key, addConflict)
		return false
	}
	begin := dup(key)
	end := kv.KeyAfter(key)
	size := int64(len(begin) + len(end))
	r.approxSize += size + sizeofKeyRangeRef
	if addConflict {
		r.approxSize += sizeofKeyRangeRef + size
	}
	r.writes.clear(begin, end, addConflict)
	if len(r.watchMap) > 0 {
		r.triggerWatches(begin, end, nil, true)
	}
	return false
}

// ClearRange clears [begin, end), including libfdb_c's inverted_range check. It
// reports true if the range is in the special key space, which must handle
// it instead. Errors are deferred.
func (r *Ryw) ClearRange(begin, end []byte) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.deferredErr != nil {
		return false
	}
	route, addConflict, err := validateClearRange(&r.options, begin, end, r.usedDuringCommit)
	if err != nil {
		r.deferredErr = err
		return false
	}
	if route != routeApply {
		return route == routeSpecial
	}
	r.approxSize += approxClearRange(begin, end, addConflict)
	if r.options.ReadYourWritesDisabled {
		r.nativeClearRange(begin, end, addConflict)
		return false
	}
	kr, ok := truncateClearRange(begin, end)
	if !ok {
		return false
	}
	r.writes.clear(kr.Begin, kr.End, addConflict)
	if len(r.watchMap) > 0 {
		r.triggerWatches(kr.Begin, kr.End, nil, true)
	}
	return false
}

// AtomicOp performs an atomic operation given its wire mutation type; readVersion, if
// haveReadVersion, is the native transaction's read version when already
// known (placeholder versionstamps). Errors are deferred.
func (r *Ryw) AtomicOp(key, param []byte, wireOp uint8, readVersion int64, haveReadVersion bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.deferredErr != nil {
		return
	}
	if err := r.atomicOp(key, param, wireOp, readVersion, haveReadVersion); err != nil {
		r.deferredErr = err
	}
}

func (r *Ryw) atomicOp(key, param []byte, wireOp uint8, readVersion int64, haveReadVersion bool) error {
	w, err := validateAtomicOp(&r.options, key, param, wireOp, readVersion, haveReadVersion, r.usedDuringCommit)
	if err != nil {
		return err
	}
	if w.template != nil {
		r.versionstampKeys = append(r.versionstampKeys, w.template)
		if !r.options.ReadYourWritesDisabled {
			if err := r.writeRangeToNative(w.vsRange.Begin, w.vsRange.End); err != nil {
				return err
			}
			r.writes.addUnmodifiedAndUnreadableRange(w.vsRange.Begin, w.vsRange.End)
		}
	}
	r.approxSize += approxAtomic(key, w.key, param, w.addConflict)
	if r.options.ReadYourWritesDisabled {
		return r.nativeAtomic(w.key, param, w.op, w.addConflict)
	}
	if err := r.writes.mutate(w.key, w.op, dup(param), w.addConflict); err != nil {
		return err
	}
	if len(r.watchMap) > 0 {
		r.triggerWatches(w.key, kv.KeyAfter(w.key), nil, false)
	}
	return nil
}

// AddConflictRange is fdb_transaction_add_conflict_range: an inverted range
// fails immediately, other errors are deferred.
func (r *Ryw) AddConflictRange(begin, end []byte, write bool) error {
	if bytes.Compare(begin, end) > 0 {
		return fdberr.InvertedRange
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.deferredErr != nil {
		return nil
	}
	if err := r.addConflictRange(begin, end, write); err != nil {
		r.deferredErr = err
	}
	return nil
}

func (r *Ryw) addConflictRange(begin, end []byte, write bool) error {
	if write {
		kr, ok, err := validateWriteConflictRange(&r.options, begin, end, r.usedDuringCommit)
		if err != nil || !ok {
			return err
		}
		r.approxSize += approxConflictRange(kr.Begin, kr.End)
		if r.options.ReadYourWritesDisabled {
			r.nativeWriteConflict(kr.Begin, kr.End)
		} else {
			r.writes.addConflictRange(kr.Begin, kr.End)
		}
		return nil
	}
	kr, ok, err := validateReadConflictRange(&r.options, begin, end, r.usedDuringCommit)
	if err != nil || !ok {
		return err
	}
	if r.options.ReadYourWritesDisabled {
		r.approxSize += approxConflictRange(kr.Begin, kr.End)
		r.addNativeReadConflict(kr.Begin, kr.End)
		return nil
	}
	it := r.writes.iter()
	it.skip(kr.Begin)
	return r.updateConflictMapRange(kr.Begin, kr.End, &it, false)
}

// triggerWatches fires this transaction's watches on keys
// in [begin, end) whose value now differs (or may differ, if !valueKnown)
// from what they saw. val nil means absent.
func (r *Ryw) triggerWatches(begin, end, val []byte, valueKnown bool) {
	if bytes.Compare(begin, end) >= 0 {
		return
	}
	for k, cells := range r.watchMap {
		if k < string(begin) || k >= string(end) {
			continue
		}
		kept := cells[:0]
		for _, c := range cells {
			if c.isFired() {
				continue
			}
			c.mu.Lock()
			differs := (c.hasSet && !optEqual(c.set, val)) || (c.hasValue && !optEqual(c.value, val))
			if !valueKnown || differs {
				c.mu.Unlock()
				c.fire()
				continue
			}
			c.set, c.hasSet = val, true
			c.mu.Unlock()
			kept = append(kept, c)
		}
		if len(kept) == 0 {
			delete(r.watchMap, k)
		} else {
			r.watchMap[k] = kept
		}
	}
}

// ---- commit ----

// beginCommit is the synchronous start of Commit: deferred error,
// used-during-commit and ended-attempt checks, then marks the attempt committing.
func (r *Ryw) beginCommit() error {
	if r.deferredErr != nil {
		return r.deferredErr
	}
	if r.usedDuringCommit() {
		return fdberr.UsedDuringCommit
	}
	if err := r.reset.Err(); err != nil {
		return err
	}
	r.commitStarted = true
	return nil
}

// buildCommitPlan is what a commit leaves in the native transaction: with
// read-your-writes, every write-map mutation plus the read conflict ranges;
// without, the native lists as recorded (stashed for conflict range
// reporting after commit).
func (r *Ryw) buildCommitPlan() (CommitPlan, error) {
	if r.options.ReadYourWritesDisabled {
		r.committedReadRanges = append([]kv.KeyRange(nil), r.nativeReadConflicts...)
		r.committedWriteRanges = append([]kv.KeyRange(nil), r.nativeWriteConflicts...)
	} else {
		if err := r.writeRangeToNative(nil, kv.SpecialKeysBegin); err != nil {
			return CommitPlan{}, err
		}
		for _, kr := range r.readConflicts.union() {
			r.addNativeReadConflict(kr.Begin, kr.End)
		}
	}
	plan := CommitPlan{Mutations: r.nativeMutations, ReadConflicts: r.nativeReadConflicts, WriteConflicts: r.nativeWriteConflicts}
	r.nativeMutations, r.nativeReadConflicts, r.nativeWriteConflicts = nil, nil, nil
	return plan, nil
}

// AddNativeReadConflict records a read
// conflict range in the native transaction (read-your-writes-disabled reads
// report theirs here). The keys are copied.
func (r *Ryw) AddNativeReadConflict(begin, end []byte) {
	r.mu.Lock()
	r.addNativeReadConflict(begin, end)
	r.mu.Unlock()
}

// AddNativeReadConflictForAttempt records a conflict only in the attempt that
// issued the asynchronous read.
func (r *Ryw) AddNativeReadConflictForAttempt(attempt *AttemptEnd, begin, endKey []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := attempt.Err(); err != nil {
		return err
	}
	if r.reset != attempt {
		return fdberr.TransactionCancelled
	}
	r.addNativeReadConflict(begin, endKey)
	return nil
}

func (r *Ryw) addNativeReadConflict(begin, end []byte) {
	if kr, ok := truncateClearRange(begin, end); ok {
		r.nativeReadConflicts = append(r.nativeReadConflicts, kr)
	}
}

// nativeSet records a set directly in the native lists.
func (r *Ryw) nativeSet(key, value []byte, addConflict bool) error {
	if err := validateSetSizes(&r.options, key, value); err != nil {
		return err
	}
	k := dup(key)
	if addConflict {
		r.nativeWriteConflicts = append(r.nativeWriteConflicts, kv.KeyRange{Begin: k, End: kv.KeyAfter(k)})
	}
	r.nativeMutations = append(r.nativeMutations, wire.Mutation{Kind: MutationSetValue, Param1: k, Param2: dup(value)})
	return nil
}

// nativeClearKey records a single-key clear directly in the native lists.
func (r *Ryw) nativeClearKey(key []byte, addConflict bool) {
	if len(key) > MaxReadKeySize(key) {
		return
	}
	b, e := dup(key), kv.KeyAfter(key)
	if addConflict {
		r.nativeWriteConflicts = append(r.nativeWriteConflicts, kv.KeyRange{Begin: b, End: e})
	}
	r.nativeMutations = append(r.nativeMutations, wire.Mutation{Kind: MutationClearRange, Param1: b, Param2: e})
}

// nativeClearRange records a range clear directly in the native lists.
func (r *Ryw) nativeClearRange(begin, end []byte, addConflict bool) {
	kr, ok := truncateClearRange(begin, end)
	if !ok {
		return
	}
	if addConflict {
		r.nativeWriteConflicts = append(r.nativeWriteConflicts, kr)
	}
	r.nativeMutations = append(r.nativeMutations, wire.Mutation{Kind: MutationClearRange, Param1: kr.Begin, Param2: kr.End})
}

// nativeAtomic records an atomic operation directly in the native lists;
// versionstamped keys get their conflict range from the
// commit proxy.
func (r *Ryw) nativeAtomic(key, param []byte, op uint8, addConflict bool) error {
	if err := validateSetSizes(&r.options, key, param); err != nil {
		return err
	}
	k := dup(key)
	if addConflict && op != MutationSetVersionstampedKey {
		r.nativeWriteConflicts = append(r.nativeWriteConflicts, kv.KeyRange{Begin: k, End: kv.KeyAfter(k)})
	}
	r.nativeMutations = append(r.nativeMutations, wire.Mutation{Kind: normalizeAtomicOp(op), Param1: k, Param2: dup(param)})
	return nil
}

// nativeWriteConflict records a native write conflict range. Its callers
// already own the endpoints (validated explicit ranges or write-map keys).
func (r *Ryw) nativeWriteConflict(begin, end []byte) {
	begin = begin[:min(len(begin), MaxReadKeySize(begin)+1)]
	end = end[:min(len(end), MaxReadKeySize(end)+1)]
	if bytes.Compare(begin, end) < 0 {
		r.nativeWriteConflicts = append(r.nativeWriteConflicts, kv.KeyRange{Begin: begin, End: end})
	}
}

// writeRangeToNative pushes the clears,
// operations and write conflict ranges recorded in [begin, end) to the
// native lists.
func (r *Ryw) writeRangeToNative(begin, end []byte) error {
	beginExt, endExt := extKey(begin), extKey(end)

	it := r.writes.iter()
	it.skip(begin)
	var clearBegin ext
	inClear := false
	for it.beginKey().compare(endExt) < 0 {
		if it.isClearedRange() && !inClear {
			clearBegin, inClear = extMax(beginExt, it.beginKey()), true
		} else if !it.isClearedRange() && inClear {
			r.nativeClearRange(clearBegin.toBytes(), it.beginKey().toBytes(), false)
			inClear = false
		}
		it.next()
	}
	if inClear {
		r.nativeClearRange(clearBegin.toBytes(), end, false)
	}

	it.skip(begin)
	var conflictBegin ext
	inConflict := false
	for it.beginKey().compare(endExt) < 0 {
		if it.isConflictRange() && !inConflict {
			conflictBegin, inConflict = extMax(beginExt, it.beginKey()), true
		} else if !it.isConflictRange() && inConflict {
			r.nativeWriteConflict(conflictBegin.toBytes(), it.beginKey().toBytes())
			inConflict = false
		}
		if it.isOperation() {
			key := it.beginKey().base
			for _, m := range it.op() {
				switch {
				case m.kind == MutationSetValue && m.value == nil:
					r.nativeClearKey(key, false)
				case m.kind == MutationSetValue || isAtomicOp(m.kind):
					if err := validateSetSizes(&r.options, key, m.value); err != nil {
						return err
					}
					// Write-map entries own immutable bytes, including
					// across later mutations and resets. Native callers
					// still copy their inputs through nativeSet/Atomic.
					r.nativeMutations = append(r.nativeMutations, wire.Mutation{
						Kind: normalizeAtomicOp(m.kind), Param1: key, Param2: m.value,
					})
				}
			}
		}
		it.next()
	}
	if inConflict {
		r.nativeWriteConflict(conflictBegin.toBytes(), end)
	}
	return nil
}

// ---- special key space introspection ----

// ReadConflictRanges returns, for special keys
// [begin, end) under ReadConflictRangePrefix, each read conflict boundary key
// with "1" (start of a conflict range) or "0" (end of one).
func (r *Ryw) ReadConflictRanges(begin, end []byte) []kv.KeyValue {
	r.mu.Lock()
	defer r.mu.Unlock()
	p := ReadConflictRangePrefix
	var out []kv.KeyValue
	if r.options.ReadYourWritesDisabled {
		m := newCoalescedMap(false, kv.SpecialKeysEnd)
		for _, list := range [][]kv.KeyRange{r.nativeReadConflicts, r.committedReadRanges} {
			for _, kr := range list {
				m.insert(withPrefix(p, kr.Begin), withPrefix(p, kr.End), true)
			}
		}
		m.boundaries(begin, end, func(k []byte, v bool) { out = append(out, kv.KeyValue{Key: k, Value: flag(v)}) })
		return out
	}
	b, e := stripPrefix(begin, p), stripPrefix(end, p)
	r.readConflicts.boundaries(b, e, func(k []byte, v bool) {
		out = append(out, kv.KeyValue{Key: withPrefix(p, k), Value: flag(v)})
	})
	return out
}

// WriteConflictRanges is like
// ReadConflictRanges for write conflicts under WriteConflictRangePrefix,
// including versionstamped keys (exact once the commit's versionstamp is
// known, non-nil; otherwise their possible range given the native read
// version, if haveReadVersion).
func (r *Ryw) WriteConflictRanges(begin, end, versionstamp []byte, readVersion int64, haveReadVersion bool) []kv.KeyValue {
	r.mu.Lock()
	defer r.mu.Unlock()
	p := WriteConflictRangePrefix
	m := newCoalescedMap(false, kv.SpecialKeysEnd)
	if r.options.ReadYourWritesDisabled {
		for _, list := range [][]kv.KeyRange{r.nativeWriteConflicts, r.committedWriteRanges} {
			for _, kr := range list {
				m.insert(withPrefix(p, kr.Begin), withPrefix(p, kr.End), true)
			}
		}
	} else {
		b, e := stripPrefix(begin, p), stripPrefix(end, p)
		endExt := extKey(e)
		it := r.writes.iter()
		it.skip(b)
		if it.beginKey().cmpKey(nil) > 0 {
			it.prev()
		}
		for it.beginKey().compare(endExt) < 0 {
			if it.isConflictRange() {
				m.insert(withPrefix(p, it.beginKey().toBytes()), withPrefix(p, it.endKey().toBytes()), true)
			}
			it.next()
		}
	}
	for _, k := range r.versionstampKeys {
		var kr kv.KeyRange
		if versionstamp != nil {
			pos := versionstampOffset(k)
			if pos < 0 || pos+len(versionstamp) > len(k) {
				continue
			}
			key := dup(k)
			copy(key[pos:], versionstamp)
			n := len(key)
			key[n-4] = 0
			kr = kv.KeyRange{Begin: key[:n-4], End: key[:n-3]}
		} else {
			var err error
			if kr, err = versionstampKeyRange(k, readVersion, haveReadVersion, r.options.MaxReadKey()); err != nil {
				continue
			}
		}
		m.insert(withPrefix(p, kr.Begin), withPrefix(p, kr.End), true)
	}
	var out []kv.KeyValue
	m.boundaries(begin, end, func(k []byte, v bool) { out = append(out, kv.KeyValue{Key: k, Value: flag(v)}) })
	return out
}

// stripPrefix removes prefix from key, saturating keys outside the prefix's range.
func stripPrefix(key, prefix []byte) []byte {
	if bytes.HasPrefix(key, prefix) {
		if rest := key[len(prefix):]; bytes.Compare(rest, kv.SpecialKeysBegin) <= 0 {
			return dup(rest)
		}
		return kv.SpecialKeysBegin
	}
	if bytes.Compare(key, prefix) < 0 {
		return emptyKey
	}
	return kv.SpecialKeysBegin
}

func withPrefix(prefix, key []byte) []byte {
	v := make([]byte, 0, len(prefix)+len(key))
	return append(append(v, prefix...), key...)
}

// flag is the special key space's "1" / "0" conflict flag.
func flag(v bool) []byte {
	if v {
		return []byte("1")
	}
	return []byte("0")
}
