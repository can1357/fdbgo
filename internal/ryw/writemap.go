package ryw

import (
	"bytes"
	"slices"

	"github.com/can1357/fdbgo/internal/fdberr"
	"github.com/can1357/fdbgo/internal/kv"
)

var afterAllKeys = []byte("\xff\xff\x00")

// atomicMask has a bit for every atomic operation, versionstamp operations included.
const atomicMask uint32 = 1<<MutationAddValue | 1<<MutationAnd | 1<<MutationOr | 1<<MutationXor |
	1<<MutationAppendIfFits | 1<<MutationMax | 1<<MutationMin | 1<<MutationSetVersionstampedKey |
	1<<MutationSetVersionstampedValue | 1<<MutationByteMin | 1<<MutationByteMax | 1<<MutationMinV2 |
	1<<MutationAndV2 | 1<<MutationCompareAndClear

// nonAssociativeMask marks the operations that cannot merge with a same-kind
// operation whose operand has a different length.
const nonAssociativeMask uint32 = 1<<MutationAddValue | 1<<MutationOr | 1<<MutationXor | 1<<MutationMax |
	1<<MutationMin | 1<<MutationSetVersionstampedKey | 1<<MutationSetVersionstampedValue |
	1<<MutationMinV2 | 1<<MutationCompareAndClear

// isAtomicOp reports whether kind is an atomic operation, versionstamp operations included.
func isAtomicOp(kind uint8) bool { return kind < 32 && atomicMask&(1<<kind) != 0 }

// isNonAssociativeOp reports whether kind is in nonAssociativeMask.
func isNonAssociativeOp(kind uint8) bool { return kind < 32 && nonAssociativeMask&(1<<kind) != 0 }

// equalsKeyAfter reports whether compareKey == key + "\x00".
func equalsKeyAfter(key, compareKey []byte) bool {
	return len(compareKey) == len(key)+1 && compareKey[len(key)] == 0 && bytes.HasPrefix(compareKey, key)
}

// rywMutation is one buffered operation on a key; a nil value
// with MutationSetValue means cleared.
type rywMutation struct {
	value []byte
	kind  uint8
}

// opStack is the operations on one key, oldest first; empty
// for boundary entries.
type opStack []rywMutation

// isDependent reports whether the value depends on the snapshot value of the key.
func (s opStack) isDependent() bool {
	if len(s) == 0 {
		return false
	}
	switch s[0].kind {
	case MutationSetValue, MutationClearRange, MutationSetVersionstampedValue, MutationSetVersionstampedKey:
		return false
	}
	return true
}

// wEntry is a boundary of the write map: flags describe the
// key itself (is*) and the keys up to the next entry (following*). Entries
// are immutable once in a tree.
type wEntry struct {
	stack                   opStack
	followingKeysCleared    bool
	followingKeysConflict   bool
	isConflict              bool
	followingKeysUnreadable bool
	isUnreadable            bool
	// one is the storage of a single-operation stack, saving an allocation.
	one [1]rywMutation
}

// newWEntry1 is newWEntry with a single operation.
func newWEntry1(m rywMutation, cleared, followingConflict, conflict, followingUnreadable, unreadable bool) *wEntry {
	e := &wEntry{followingKeysCleared: cleared, followingKeysConflict: followingConflict, isConflict: conflict,
		followingKeysUnreadable: followingUnreadable, isUnreadable: unreadable}
	e.one[0] = m
	e.stack = e.one[:]
	return e
}

func newWEntry(stack opStack, cleared, followingConflict, conflict, followingUnreadable, unreadable bool) *wEntry {
	return &wEntry{stack: stack, followingKeysCleared: cleared, followingKeysConflict: followingConflict,
		isConflict: conflict, followingKeysUnreadable: followingUnreadable, isUnreadable: unreadable}
}

type wtree = ptree[*wEntry]

// emptyWriteRoot holds the "", \xff\xff and \xff\xff\x00 sentinels shared by
// every new write map.
var emptyWriteRoot = func() *pnode[*wEntry] {
	m := newPmap[*wEntry](nil)
	for _, k := range [][]byte{{}, kv.SpecialKeysBegin, afterAllKeys} {
		m.set(k, &wEntry{})
	}
	return m.root
}()

// writeMap holds per-key operation stacks, cleared ranges, conflict
// and unreadable marks.
type writeMap struct {
	tree  pmap[*wEntry]
	empty bool
}

func newWriteMap() writeMap { return writeMap{tree: newPmap(emptyWriteRoot), empty: true} }

// isEmpty reports that no write, clear or conflict range has been recorded.
func (w *writeMap) isEmpty() bool { return w.empty }

// pointUnmodified reports whether key lies in an unmodified, readable range:
// no operation on it, and not cleared or made unreadable by this transaction.
func (w *writeMap) pointUnmodified(key []byte) bool {
	if w.empty {
		return true
	}
	n := w.tree.view().floor(key)
	if n == nil {
		return true
	}
	operation := bytes.Equal(n.key, key) && len(n.val.stack) != 0
	return !operation && !n.val.followingKeysCleared && !n.val.followingKeysUnreadable
}

// frozen returns the current version, for iterators that must not observe later writes.
func (w *writeMap) frozen() wtree { return w.tree.freeze() }

// iter returns an iterator over the current contents positioned at ""; it
// must not be used after the next update.
func (w *writeMap) iter() wIter { return newWIter(w.tree.view()) }

func (w *writeMap) put(key []byte, e *wEntry) { w.tree.set(key, e) }

// removeRange removes the entries in [begin, end), each bound optionally
// followed by one zero byte.
func (w *writeMap) removeRange(begin []byte, beginAfter bool, end []byte, endAfter bool) {
	c := bytes.Compare(begin, end)
	if c > 0 || (c == 0 && (beginAfter || !endAfter)) {
		return
	}
	w.tree.deleteRange(begin, beginAfter, end, endAfter)
}

// mutate records operation(param) on key. A write without a conflict range
// does not remove an existing conflict mark. key and param are retained.
func (w *writeMap) mutate(key []byte, operation uint8, param []byte, addConflict bool) error {
	w.empty = false
	it := w.iter()
	it.skip(key)

	cur := it.cur
	isCleared := cur.followingKeysCleared
	followingConflict := cur.followingKeysConflict
	isConflict := addConflict || it.isConflictRange()
	followingUnreadable := cur.followingKeysUnreadable
	isUnreadable := it.isUnreadable() || operation == MutationSetVersionstampedValue ||
		operation == MutationSetVersionstampedKey
	isDependent := operation != MutationSetValue && operation != MutationSetVersionstampedValue &&
		operation != MutationSetVersionstampedKey
	newOp := rywMutation{value: param, kind: operation}

	switch {
	case !bytes.Equal(it.curKey, key):
		if it.isClearedRange() && isDependent {
			op := opStack{{kind: MutationSetValue}}
			if err := coalesceOver(&op, newOp); err != nil {
				return err
			}
			w.put(key, newWEntry(op, true, followingConflict, isConflict, followingUnreadable, isUnreadable))
		} else {
			w.put(key, newWEntry1(newOp, isCleared, followingConflict, isConflict, followingUnreadable, isUnreadable))
		}
	case !it.isUnreadable() && (operation == MutationSetValue || operation == MutationSetVersionstampedValue):
		w.put(key, newWEntry1(newOp, isCleared, followingConflict, isConflict, followingUnreadable, isUnreadable))
	default:
		e := *cur
		e.stack = slices.Clone(e.stack)
		e.isConflict = isConflict
		e.isUnreadable = isUnreadable
		switch {
		case len(e.stack) == 0 && it.isClearedRange() && isDependent:
			e.stack = opStack{{kind: MutationSetValue}}
			if err := coalesceOver(&e.stack, newOp); err != nil {
				return err
			}
		case !isUnreadable && len(e.stack) != 0:
			if err := coalesceOver(&e.stack, newOp); err != nil {
				return err
			}
		default:
			e.stack = append(e.stack, newOp)
		}
		w.put(key, &e)
	}
	return nil
}

// clear records a clear of [begin, end).
func (w *writeMap) clear(begin, end []byte, addConflict bool) {
	w.empty = false
	if !addConflict {
		w.clearNoConflict(begin, end)
		return
	}
	endExt := extKey(end)
	it := w.iter()
	it.skip(begin)

	insertBegin := !it.isClearedRange() || !it.isConflictRange() || it.isUnreadable()

	if c := it.endKey().compare(endExt); c == 0 {
		it.next()
	} else if c < 0 {
		it.skip(end)
	}

	insertEnd := (it.isUnmodifiedRange() || !it.isConflictRange() || it.isUnreadable()) &&
		(!it.keyAtBegin() || it.beginKey().compare(endExt) != 0)
	endCoalesceClear := it.isClearedRange() && it.beginKey().compare(endExt) == 0 &&
		it.isConflictRange() && !it.isUnreadable()
	endConflict := it.isConflictRange()
	endCleared := it.isClearedRange()
	endUnreadable := it.isUnreadable()

	w.removeRange(begin, !insertBegin, end, endCoalesceClear)
	if insertBegin {
		w.put(begin, newWEntry(nil, true, true, true, false, false))
	}
	if insertEnd {
		w.put(end, newWEntry(nil, endCleared, endConflict, endConflict, endUnreadable, endUnreadable))
	}
}

// addUnmodifiedAndUnreadableRange forgets the writes in [begin, end) and
// makes it unreadable.
func (w *writeMap) addUnmodifiedAndUnreadableRange(begin, end []byte) {
	endExt := extKey(end)
	it := w.iter()
	it.skip(begin)

	insertBegin := !it.isUnmodifiedRange() || it.isConflictRange() || !it.isUnreadable()

	if c := it.endKey().compare(endExt); c == 0 {
		it.next()
	} else if c < 0 {
		it.skip(end)
	}

	insertEnd := (it.isClearedRange() || it.isConflictRange() || !it.isUnreadable()) &&
		(!it.keyAtBegin() || it.beginKey().compare(endExt) != 0)
	endCoalesceUnmodified := it.isUnmodifiedRange() && it.beginKey().compare(endExt) == 0 &&
		!it.isConflictRange() && it.isUnreadable()
	endConflict := it.isConflictRange()
	endCleared := it.isClearedRange()
	endUnreadable := it.isUnreadable()

	w.removeRange(begin, !insertBegin, end, endCoalesceUnmodified)
	if insertBegin {
		w.put(begin, newWEntry(nil, false, false, false, true, true))
	}
	if insertEnd {
		w.put(end, newWEntry(nil, endCleared, endConflict, endConflict, endUnreadable, endUnreadable))
	}
}

type wInsertion struct {
	key []byte
	e   *wEntry
}

// addConflictRange marks [begin, end) as a write conflict range.
func (w *writeMap) addConflictRange(begin, end []byte) {
	w.empty = false
	endExt := extKey(end)
	beginExt := extKey(begin)
	it := w.iter()
	it.skip(begin)

	var removals [][]byte
	var insertions []wInsertion

	if e := it.cur; !e.followingKeysConflict || !e.isConflict {
		if it.keyAtBegin() && it.beginKey().compare(beginExt) == 0 {
			removals = append(removals, begin)
		}
		var stack opStack
		if it.isOperation() {
			stack = it.cur.stack
		}
		insertions = append(insertions, wInsertion{begin,
			newWEntry(stack, e.followingKeysCleared, true, true, e.followingKeysUnreadable, e.isUnreadable)})
	}

	for it.endKey().compare(endExt) < 0 {
		it.next()
		if it.keyAtBegin() && (!it.cur.followingKeysConflict || !it.cur.isConflict) {
			e := *it.cur
			e.followingKeysConflict = true
			e.isConflict = true
			removals = append(removals, it.curKey)
			insertions = append(insertions, wInsertion{it.curKey, &e})
		}
	}

	if e := it.cur; !e.followingKeysConflict || !e.isConflict {
		isCleared := e.followingKeysCleared
		isUnreadable := e.isUnreadable
		followingUnreadable := e.followingKeysUnreadable
		it.next()
		if !it.keyAtBegin() || it.beginKey().compare(endExt) != 0 {
			insertions = append(insertions, wInsertion{end,
				newWEntry(nil, isCleared, false, false, followingUnreadable, isUnreadable)})
		}
	}

	for _, k := range removals {
		w.tree.delete(k)
	}
	for _, ins := range insertions {
		w.put(ins.key, ins.e)
	}
}

// clearNoConflict clears [begin, end) keeping the existing conflict marks.
func (w *writeMap) clearNoConflict(begin, end []byte) {
	endExt := extKey(end)
	it := w.iter()
	it.skip(begin)

	insertBegin := !it.isClearedRange() || it.isUnreadable()
	lastConflicted := it.isConflictRange()
	conflicted := lastConflicted
	var conflictRanges []ext

	if insertBegin {
		conflictRanges = append(conflictRanges, extKey(begin))
	} else {
		conflicted = !conflicted
	}

	for it.endKey().compare(endExt) < 0 {
		it.next()
		if lastConflicted != it.isConflictRange() {
			conflictRanges = append(conflictRanges, it.beginKey())
			lastConflicted = it.isConflictRange()
		}
	}

	if it.endKey().compare(endExt) == 0 {
		it.next()
	}

	atEnd := it.beginKey().compare(endExt) == 0
	insertEnd := ((it.isUnmodifiedRange() || it.isUnreadable()) && (!it.keyAtBegin() || !atEnd)) ||
		(it.cur.isConflict && !it.cur.followingKeysConflict && atEnd && !it.keyAtBegin())
	endCleared := it.isClearedRange()
	endCoalesceClear := it.isClearedRange() && atEnd && it.isConflictRange() == lastConflicted && !it.isUnreadable()
	endConflict := it.isConflictRange()
	endUnreadable := it.isUnreadable()

	w.removeRange(begin, !insertBegin, end, endCoalesceClear)
	for _, k := range conflictRanges {
		w.put(k.toBytes(), newWEntry(nil, true, conflicted, conflicted, false, false))
		conflicted = !conflicted
	}
	if insertEnd {
		w.put(end, newWEntry(nil, endCleared, endConflict, endConflict, endUnreadable, endUnreadable))
	}
}

// coalesce returns the single operation equivalent to
// existing followed by n.
func coalesce(existing, n rywMutation) (rywMutation, error) {
	if n.kind == MutationSetValue || n.kind == MutationSetVersionstampedValue {
		return n, nil
	}
	param := n.value
	switch n.kind {
	case MutationCompareAndClear:
		if existing.kind != MutationSetValue {
			return rywMutation{}, fdberr.OperationFailed
		}
		if doCompareAndClear(existing.value, param) != nil {
			return existing, nil
		}
		return rywMutation{kind: MutationSetValue}, nil
	case MutationAddValue, MutationAppendIfFits, MutationAnd, MutationOr, MutationXor, MutationMax,
		MutationMin, MutationByteMin, MutationByteMax, MutationMinV2, MutationAndV2:
		var kind uint8
		switch existing.kind {
		case MutationSetValue:
			kind = MutationSetValue
		case n.kind:
			kind = n.kind
		default:
			return rywMutation{}, fdberr.OperationFailed
		}
		return rywMutation{value: applyAtomic(n.kind, existing.value, param), kind: kind}, nil
	}
	return rywMutation{}, fdberr.OperationFailed
}

// coalesceOver pushes n onto the stack, merging
// with the top when possible. The stack must not share its backing array.
func coalesceOver(stack *opStack, n rywMutation) error {
	s := *stack
	existing := s[len(s)-1]
	push := false
	switch {
	case existing.kind == n.kind && n.kind != MutationCompareAndClear:
		push = isNonAssociativeOp(existing.kind) && existing.value != nil && len(existing.value) != len(n.value)
	case isAtomicOp(n.kind) && isAtomicOp(existing.kind):
		push = true
	}
	if push {
		*stack = append(s, n)
		return nil
	}
	m, err := coalesce(existing, n)
	if err != nil {
		return err
	}
	s[len(s)-1] = m
	return nil
}

// coalesceUnder applies the stack over the
// snapshot value of the key (nil = absent).
func coalesceUnder(stack opStack, value []byte) (rywMutation, error) {
	if !stack.isDependent() && len(stack) == 1 {
		return stack[0], nil
	}
	cur := rywMutation{value: value, kind: MutationSetValue}
	for _, m := range stack {
		var err error
		if cur, err = coalesce(cur, m); err != nil {
			return rywMutation{}, err
		}
	}
	return cur, nil
}

// Segment kinds of a write map iterator.
const (
	wUnmodified  = 0
	wCleared     = 1
	wIndependent = 2
	wDependent   = 3
)

// wIter walks the segments (unmodified ranges, cleared
// ranges, single modified keys) of one version; every key belongs to exactly
// one segment. Copying a wIter copies its position.
type wIter struct {
	tree    wtree
	curKey  []byte
	cur     *wEntry
	nextKey []byte
	next_   *wEntry
	// offset false: the operation at curKey; true: the range following it.
	offset bool
}

var emptyWEntry = &wEntry{}

// newWIter returns an iterator over tree positioned at the segment containing "".
func newWIter(tree wtree) wIter {
	it := wIter{tree: tree, cur: emptyWEntry, next_: emptyWEntry}
	if n := tree.first(); n != nil {
		it.curKey, it.cur, it.nextKey, it.next_ = n.key, n.val, n.key, n.val
	}
	it.skip(nil)
	return it
}

// skip moves to the segment containing key.
func (it *wIter) skip(key []byte) {
	next := it.tree.higher(key)
	if next == nil {
		if next = it.tree.last(); next == nil {
			return
		}
	}
	if p := it.tree.lower(next.key); p != nil {
		it.curKey, it.cur = p.key, p.val
	}
	it.nextKey, it.next_ = next.key, next.val
	it.offset = len(it.cur.stack) == 0 || !bytes.Equal(it.curKey, key)
}

func (it *wIter) next() {
	if !it.offset && !equalsKeyAfter(it.curKey, it.nextKey) {
		it.offset = true
		return
	}
	it.curKey, it.cur = it.nextKey, it.next_
	if n := it.tree.higher(it.curKey); n != nil {
		it.nextKey, it.next_ = n.key, n.val
	}
	it.offset = len(it.cur.stack) == 0
}

func (it *wIter) prev() {
	if it.offset && len(it.cur.stack) != 0 {
		it.offset = false
		return
	}
	if p := it.tree.lower(it.curKey); p != nil {
		it.nextKey, it.next_ = it.curKey, it.cur
		it.curKey, it.cur = p.key, p.val
	}
	it.offset = len(it.cur.stack) == 0 || !equalsKeyAfter(it.curKey, it.nextKey)
}

// seg is the current segment kind.
func (it *wIter) seg() int {
	if it.offset {
		if it.cur.followingKeysCleared {
			return wCleared
		}
		return wUnmodified
	}
	if it.cur.stack.isDependent() {
		return wDependent
	}
	return wIndependent
}

func (it *wIter) isClearedRange() bool    { return it.offset && it.cur.followingKeysCleared }
func (it *wIter) isUnmodifiedRange() bool { return it.offset && !it.cur.followingKeysCleared }
func (it *wIter) isOperation() bool       { return !it.offset }

func (it *wIter) isConflictRange() bool {
	if it.offset {
		return it.cur.followingKeysConflict
	}
	return it.cur.isConflict
}

func (it *wIter) isUnreadable() bool {
	if it.offset {
		return it.cur.followingKeysUnreadable
	}
	return it.cur.isUnreadable
}

// isIndependent reports, for an operation segment, that its value does not
// depend on the snapshot.
func (it *wIter) isIndependent() bool {
	return it.cur.followingKeysCleared || !it.cur.stack.isDependent()
}

func (it *wIter) beginKey() ext {
	z := 0
	if it.offset && len(it.cur.stack) != 0 {
		z = 1
	}
	return ext{it.curKey, z}
}

func (it *wIter) endKey() ext {
	if it.offset {
		return extKey(it.nextKey)
	}
	return ext{it.curKey, 1}
}

// op is the operation stack of an operation segment.
func (it *wIter) op() opStack { return it.cur.stack }

// keyAtBegin reports that the segment starts exactly at its entry's key.
func (it *wIter) keyAtBegin() bool { return !it.offset || len(it.cur.stack) == 0 }

func (it *wIter) equal(r *wIter) bool {
	return it.offset == r.offset && bytes.Equal(it.curKey, r.curKey)
}
