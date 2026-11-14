// Command stacktester is the bindingtester stack machine for the fdbgo
// client: it reads instructions stored under a prefix, executes them
// against the public fdb, tuple, subspace and directory packages and leaves
// results in the database for bindingtester.py to compare with the other
// bindings. It follows FoundationDB's
// bindings/bindingtester/spec/bindingApiTester.md and directoryLayerTester.md.
//
// The declared API version controls bindingtester formats, not the public Go
// API: before API 520, versionstamped keys use uint16 offsets and versionstamped
// values always stamp offset zero. The Go API always uses uint32 offsets.
//
// Usage: stacktester <prefix> <api_version> [cluster_file]
package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"math"
	"math/big"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/can1357/fdbgo/fdb"
	"github.com/can1357/fdbgo/tuple"
)

// Byte strings the spec defines as instruction results.
var (
	resultNotPresent     = []byte("RESULT_NOT_PRESENT")
	gotReadVersion       = []byte("GOT_READ_VERSION")
	gotCommittedVersion  = []byte("GOT_COMMITTED_VERSION")
	gotApproximateSize   = []byte("GOT_APPROXIMATE_SIZE")
	gotEstimatedSize     = []byte("GOT_ESTIMATED_RANGE_SIZE")
	gotRangeSplitPoints  = []byte("GOT_RANGE_SPLIT_POINTS")
	waitedForEmpty       = []byte("WAITED_FOR_EMPTY")
	setConflictRange     = []byte("SET_CONFLICT_RANGE")
	setConflictKey       = []byte("SET_CONFLICT_KEY")
	errorNone            = []byte("ERROR: NONE")
	errorMultiple        = []byte("ERROR: MULTIPLE")
	okResult             = []byte("OK")
	directoryErrorResult = []byte("DIRECTORY_ERROR")
)

// ctx is the context of every blocking call: the tester runs to completion
// and has no deadline of its own.
var ctx = context.Background()

// db is the database every stack machine of the process operates on.
var db *fdb.Database

// opError carries an fdb error out of an instruction to processInst, which
// pushes it as a packed ("ERROR", code) tuple like the reference testers.
type opError struct{ code int }

// operandError distinguishes malformed instruction inputs from tester defects.
type operandError string

// check aborts the current instruction with err: fdb errors become an error
// result on the stack, anything else is a tester or binding bug and fatal.
func check(err error) {
	if err == nil {
		return
	}
	var e fdb.Error
	if errors.As(err, &e) {
		panic(opError{int(e)})
	}
	log.Panicf("non-fdb error: %v", err)
}

// errorBytes is the stack encoding of err: the packed tuple ("ERROR", code).
func errorBytes(err error) []byte {
	var e fdb.Error
	if !errors.As(err, &e) {
		log.Panicf("non-fdb error: %v", err)
	}
	return packErrorCode(int(e))
}

func packErrorCode(code int) []byte {
	return mustPack(tuple.Tuple{[]byte("ERROR"), []byte(strconv.Itoa(code))})
}

func mustPack(t tuple.Tuple) []byte {
	b, err := t.Pack()
	if err != nil {
		log.Panicf("packing %v: %v", t, err)
	}
	return b
}

// versionstampResult belongs to one protocol transaction attempt. Registration
// does not call the post-commit accessor: COMMIT supplies its result, while
// RESET, ON_ERROR and CANCEL finish abandoned attempts. Stack entries retain
// this object rather than looking up a potentially replacement transaction.
type versionstampResult struct {
	done  chan struct{}
	value []byte
	err   error
}

// newVersionstampResult starts an unresolved protocol registration.
func newVersionstampResult() *versionstampResult {
	return &versionstampResult{done: make(chan struct{})}
}

// resolve is called with the owning trEntry's mutex held. The first outcome
// wins, including when a committed transaction is subsequently reset.
func (v *versionstampResult) resolve(value []byte, err error) {
	select {
	case <-v.done:
		return
	default:
	}
	v.value, v.err = value, err
	close(v.done)
}

// stackValue waits only for this attempt, never for a later commit.
func (v *versionstampResult) stackValue() any {
	<-v.done
	if v.err != nil {
		return errorBytes(v.err)
	}
	return v.value
}

// stackEntry is one stack item: a value, or a pending result (wait) that
// is resolved when the item is popped. idx is the number of the instruction
// that produced it.
type stackEntry struct {
	idx  int
	val  any
	wait func() any
}

// trEntry is a named transaction of the global transaction map. mu serializes
// protocol lifecycle operations and versionstamp registration across machines.
type trEntry struct {
	mu    sync.Mutex
	tr    *fdb.Transaction
	stamp *versionstampResult
}

// newTrEntry creates the first protocol attempt for tr.
func newTrEntry(tr *fdb.Transaction) *trEntry {
	return &trEntry{tr: tr, stamp: newVersionstampResult()}
}

// Transactions are shared by every stack machine of the process.
var (
	trMu  sync.Mutex
	trMap = map[string]*trEntry{}
)

// StackMachine is one instruction stream: its declared API version, stack,
// current transaction name, last seen version, started threads and directory list.
type StackMachine struct {
	apiVersion  int
	prefix      []byte
	trName      string
	stack       []stackEntry
	lastVersion int64
	threads     sync.WaitGroup
	de          *directoryExtension
	verbose     bool
}

func newStackMachine(prefix []byte, apiVersion int) *StackMachine {
	sm := &StackMachine{
		apiVersion: apiVersion,
		prefix:     prefix,
		trName:     string(prefix),
		de:         newDirectoryExtension(),
		verbose:    os.Getenv("STACKTESTER_VERBOSE") != "",
	}
	sm.newTransaction()
	return sm
}

func (sm *StackMachine) newTransaction() {
	tr, err := db.CreateTransaction()
	if err != nil {
		log.Fatalf("create transaction: %v", err)
	}
	trMu.Lock()
	old := trMap[sm.trName]
	trMap[sm.trName] = newTrEntry(tr)
	trMu.Unlock()
	if old != nil {
		old.mu.Lock()
		old.stamp.resolve(nil, fdb.ErrBrokenPromise)
		old.mu.Unlock()
	}
}

func (sm *StackMachine) switchTransaction(name []byte) {
	sm.trName = string(name)
	trMu.Lock()
	_, ok := trMap[sm.trName]
	trMu.Unlock()
	if !ok {
		tr, err := db.CreateTransaction()
		if err != nil {
			log.Fatalf("create transaction: %v", err)
		}
		trMu.Lock()
		if _, ok := trMap[sm.trName]; !ok {
			trMap[sm.trName] = newTrEntry(tr)
		}
		trMu.Unlock()
	}
}

// currentEntry captures the named transaction, independently of replacements.
func (sm *StackMachine) currentEntry() *trEntry {
	trMu.Lock()
	e := trMap[sm.trName]
	trMu.Unlock()
	return e
}

// currentTransaction waits for a lifecycle operation on the named transaction.
func (sm *StackMachine) currentTransaction() *fdb.Transaction {
	e := sm.currentEntry()
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.tr
}

// endsVersionstampAttempt identifies errors that end reads and pending stamps.
func endsVersionstampAttempt(err error) bool {
	return errors.Is(err, fdb.ErrTransactionTimedOut) ||
		errors.Is(err, fdb.ErrTransactionCancelled) ||
		errors.Is(err, fdb.ErrUsedDuringCommit)
}

// versionstampFailure preserves errors that end the RYW attempt. An ordinary
// native commit failure has no stamp, whereas a rejected OnError cancels the
// attempt; callers supply that operation-specific fallback.
func versionstampFailure(err error, fallback fdb.Error) error {
	if endsVersionstampAttempt(err) {
		return err
	}
	return fallback
}

// transactionOp executes lifecycle instructions synchronously. WAIT_FUTURE
// accepts their already-computed results just as it accepts completed reads.
func (sm *StackMachine) transactionOp(op string, idx int, code int) {
	e := sm.currentEntry()
	e.mu.Lock()
	defer e.mu.Unlock()
	var err error
	switch op {
	case "COMMIT":
		err = e.tr.Commit(ctx)
		if err != nil {
			e.stamp.resolve(nil, versionstampFailure(err, fdb.ErrTransactionInvalidVersion))
		} else {
			value, stampErr := e.tr.Versionstamp()
			e.stamp.resolve(value, stampErr)
		}
	case "ON_ERROR":
		err = e.tr.OnError(ctx, fdb.Error(code))
		if err != nil {
			e.stamp.resolve(nil, versionstampFailure(err, fdb.ErrTransactionCancelled))
		} else {
			e.stamp.resolve(nil, fdb.ErrBrokenPromise)
			e.stamp = newVersionstampResult()
		}
	case "RESET":
		e.tr.Reset()
		e.stamp.resolve(nil, fdb.ErrBrokenPromise)
		e.stamp = newVersionstampResult()
		return
	case "CANCEL":
		e.tr.Cancel()
		e.stamp.resolve(nil, fdb.ErrTransactionCancelled)
		return
	}
	if err != nil {
		sm.push(idx, errorBytes(err))
	} else {
		sm.push(idx, resultNotPresent)
	}
}

// registerVersionstamp pushes a pending result tied to the current attempt.
func (sm *StackMachine) registerVersionstamp(idx int) {
	e := sm.currentEntry()
	e.mu.Lock()
	defer e.mu.Unlock()
	// ApproximateSize performs the deferred-error check without registering a
	// read or tripping the used-during-commit guard.
	if _, err := e.tr.ApproximateSize(ctx); err != nil {
		sm.push(idx, errorBytes(err))
		return
	}
	sm.pushWait(idx, e.stamp.stackValue)
}

func (sm *StackMachine) push(idx int, v any) {
	sm.stack = append(sm.stack, stackEntry{idx: idx, val: v})
}

func (sm *StackMachine) pushWait(idx int, wait func() any) {
	sm.stack = append(sm.stack, stackEntry{idx: idx, wait: wait})
}

// waitAndPop pops the top item, resolving it if it is a future.
func (sm *StackMachine) waitAndPop() stackEntry {
	if len(sm.stack) == 0 {
		panic(operandError("pop from empty stack"))
	}
	e := sm.stack[len(sm.stack)-1]
	sm.stack = sm.stack[:len(sm.stack)-1]
	if e.wait != nil {
		e.val = e.wait()
		e.wait = nil
	}
	return e
}

func (sm *StackMachine) pop() any { return sm.waitAndPop().val }

func (sm *StackMachine) popBytes() []byte {
	switch v := sm.pop().(type) {
	case []byte:
		return v
	case nil:
		return nil
	default:
		panic(operandError(fmt.Sprintf("expected bytes, got %T %v", v, v)))
	}
}

func (sm *StackMachine) popString() string {
	v := sm.pop()
	s, ok := v.(string)
	if !ok {
		panic(operandError(fmt.Sprintf("expected string, got %T %v", v, v)))
	}
	return s
}

func (sm *StackMachine) popInt() int64 {
	v := sm.pop()
	switch n := v.(type) {
	case int64:
		return n
	case uint64:
		if n <= math.MaxInt64 {
			return int64(n)
		}
	case *big.Int:
		if n.IsInt64() {
			return n.Int64()
		}
	}
	panic(operandError(fmt.Sprintf("expected int64, got %T %v", v, v)))
}

func (sm *StackMachine) popSelector() fdb.KeySelector {
	key := sm.popBytes()
	orEqual := sm.popInt() != 0
	offset := sm.popInt()
	return fdb.KeySelector{Key: key, OrEqual: orEqual, Offset: int(offset)}
}

func (sm *StackMachine) popKeyRange() fdb.KeyRange {
	begin := sm.popBytes()
	end := sm.popBytes()
	return fdb.KeyRange{Begin: begin, End: end}
}

func (sm *StackMachine) popRangeOptions() fdb.RangeOptions {
	limit := sm.popInt()
	reverse := sm.popInt() != 0
	mode := sm.popInt() // the C code (FDBStreamingMode), one less than fdb.StreamingMode
	return fdb.RangeOptions{Limit: int(limit), Reverse: reverse, Mode: fdb.StreamingMode(mode + 1)}
}

func (sm *StackMachine) popPrefixRange() fdb.KeyRange {
	r, err := fdb.PrefixRange(sm.popBytes())
	if err != nil {
		log.Panicf("prefix range: %v", err)
	}
	return r
}

// popTuple pops n items as a tuple, top of the stack first.
func (sm *StackMachine) popTuple(n int64) tuple.Tuple {
	t := make(tuple.Tuple, 0, n)
	for range n {
		t = append(t, sm.pop())
	}
	return t
}

// pushRange pushes the rows of a range read packed as (k1, v1, k2, v2, ...),
// skipping keys outside prefixFilter when it is non-nil.
func (sm *StackMachine) pushRange(idx int, kvs []fdb.KeyValue, prefixFilter []byte) {
	t := make(tuple.Tuple, 0, 2*len(kvs))
	for _, kv := range kvs {
		if prefixFilter == nil || bytes.HasPrefix(kv.Key, prefixFilter) {
			t = append(t, kv.Key, kv.Value)
		}
	}
	sm.push(idx, mustPack(t))
}

// mutationTypes maps ATOMIC_OP names to mutation types, including the
// deprecated AND/OR/XOR aliases.
var mutationTypes = map[string]fdb.MutationType{
	"ADD":                      fdb.MutationTypeAdd,
	"AND":                      fdb.MutationTypeBitAnd,
	"BIT_AND":                  fdb.MutationTypeBitAnd,
	"OR":                       fdb.MutationTypeBitOr,
	"BIT_OR":                   fdb.MutationTypeBitOr,
	"XOR":                      fdb.MutationTypeBitXor,
	"BIT_XOR":                  fdb.MutationTypeBitXor,
	"APPEND_IF_FITS":           fdb.MutationTypeAppendIfFits,
	"MAX":                      fdb.MutationTypeMax,
	"MIN":                      fdb.MutationTypeMin,
	"SET_VERSIONSTAMPED_KEY":   fdb.MutationTypeSetVersionstampedKey,
	"SET_VERSIONSTAMPED_VALUE": fdb.MutationTypeSetVersionstampedValue,
	"BYTE_MIN":                 fdb.MutationTypeByteMin,
	"BYTE_MAX":                 fdb.MutationTypeByteMax,
	"COMPARE_AND_CLEAR":        fdb.MutationTypeCompareAndClear,
}

// mutationOperands converts historical bindingtester operands to the Go API's
// canonical uint32-offset format. Leave malformed operands for Atomic to reject:
// doing validation here would change deferred errors and their priority.
func (sm *StackMachine) mutationOperands(mt fdb.MutationType, key, value []byte) ([]byte, []byte) {
	if sm.apiVersion >= 520 {
		return key, value
	}
	switch mt {
	case fdb.MutationTypeSetVersionstampedKey:
		if len(key) >= 2 {
			canonical := make([]byte, len(key)+2)
			copy(canonical, key)
			key = canonical
		}
	case fdb.MutationTypeSetVersionstampedValue:
		canonical := make([]byte, len(value)+4)
		copy(canonical, value)
		value = canonical
	}
	return key, value
}

// packWithVersionstamp converts only the output trailer; tuple's public API
// remains canonical regardless of the tester's declared API version.
func (sm *StackMachine) packWithVersionstamp(t tuple.Tuple, prefix []byte) ([]byte, error) {
	packed, err := t.PackWithVersionstamp(prefix)
	if err != nil || sm.apiVersion >= 520 {
		return packed, err
	}
	if binary.LittleEndian.Uint32(packed[len(packed)-4:]) > math.MaxUint16 {
		return nil, fmt.Errorf("versionstamp offset exceeds API %d uint16 range", sm.apiVersion)
	}
	return packed[:len(packed)-2], nil
}

// executeMutation applies fn to the current transaction, or with _DATABASE
// commits it in a retried transaction of its own and pushes
// RESULT_NOT_PRESENT.
func (sm *StackMachine) executeMutation(isDB bool, idx int, fn func(tr *fdb.Transaction)) {
	if !isDB {
		fn(sm.currentTransaction())
		return
	}
	check(db.Transact(ctx, func(tr *fdb.Transaction) error {
		fn(tr)
		return nil
	}))
	sm.push(idx, resultNotPresent)
}

// read runs fn on the reader an instruction variant selects: the current
// transaction, its snapshot view, or (with _DATABASE) a retried transaction
// of its own.
func (sm *StackMachine) read(isDB, isSnapshot bool, fn func(rt fdb.ReadTransaction) error) {
	if isDB {
		check(db.ReadTransact(ctx, fn))
		return
	}
	e := sm.currentEntry()
	e.mu.Lock()
	var rt fdb.ReadTransaction = e.tr
	stamp := e.stamp
	if isSnapshot {
		rt = e.tr.Snapshot()
	}
	e.mu.Unlock()
	err := fn(rt)
	if endsVersionstampAttempt(err) {
		e.mu.Lock()
		stamp.resolve(nil, err)
		e.mu.Unlock()
	}
	check(err)
}

// logStack writes entries (stack index → item) under prefix as
// prefix+pack((index, idx)) = pack((item,)), truncated to 40000 bytes.
func logStack(prefix []byte, entries map[int]stackEntry) {
	check(db.Transact(ctx, func(tr *fdb.Transaction) error {
		for index, e := range entries {
			key := append(bytes.Clone(prefix), mustPack(tuple.Tuple{int64(index), int64(e.idx)})...)
			val := mustPack(tuple.Tuple{e.val})
			if len(val) > 40000 {
				val = val[:40000]
			}
			tr.Set(key, val)
		}
		return nil
	}))
}

func (sm *StackMachine) processInst(idx int, inst tuple.Tuple) {
	op, ok := inst[0].(string)
	if !ok {
		log.Fatalf("instruction %d: op is %T", idx, inst[0])
	}
	defer func() {
		if r := recover(); r != nil {
			if e, ok := r.(opError); ok {
				sm.push(idx, packErrorCode(e.code))
				return
			}
			log.Fatalf("instruction %d %s (prefix %q): %v", idx, op, sm.prefix, r)
		}
	}()
	if sm.verbose {
		log.Printf("%q %d. %s stack=%d", sm.prefix, idx, op, len(sm.stack))
	}

	var isDB, isSnapshot bool
	switch {
	case strings.HasSuffix(op, "_DATABASE"):
		isDB = true
		op = strings.TrimSuffix(op, "_DATABASE")
	case strings.HasSuffix(op, "_SNAPSHOT"):
		isSnapshot = true
		op = strings.TrimSuffix(op, "_SNAPSHOT")
	}

	switch op {
	case "PUSH":
		sm.push(idx, inst[1])
	case "DUP":
		sm.stack = append(sm.stack, sm.stack[len(sm.stack)-1])
	case "EMPTY_STACK":
		sm.stack = nil
	case "SWAP":
		i := int(sm.popInt())
		top := len(sm.stack) - 1
		sm.stack[top], sm.stack[top-i] = sm.stack[top-i], sm.stack[top]
	case "POP":
		sm.waitAndPop()
	case "SUB":
		a, b := toBig(sm.pop()), toBig(sm.pop())
		sm.push(idx, fromBig(a.Sub(a, b)))
	case "CONCAT":
		switch a := sm.pop().(type) {
		case string:
			sm.push(idx, a+sm.popString())
		case []byte:
			sm.push(idx, append(bytes.Clone(a), sm.popBytes()...))
		default:
			log.Panicf("CONCAT of %T", a)
		}
	case "WAIT_FUTURE":
		e := sm.waitAndPop()
		sm.stack = append(sm.stack, e)
	case "LOG_STACK":
		prefix := sm.popBytes()
		entries := make(map[int]stackEntry)
		for len(sm.stack) > 0 {
			entries[len(sm.stack)-1] = sm.waitAndPop()
			if len(entries) == 100 {
				logStack(prefix, entries)
				entries = make(map[int]stackEntry)
			}
		}
		logStack(prefix, entries)

	case "NEW_TRANSACTION":
		sm.newTransaction()
	case "USE_TRANSACTION":
		sm.switchTransaction(sm.popBytes())
	case "ON_ERROR":
		code := int(sm.popInt())
		sm.transactionOp(op, idx, code)
	case "COMMIT", "RESET", "CANCEL":
		sm.transactionOp(op, idx, 0)

	case "GET":
		key := sm.popBytes()
		var v []byte
		sm.read(isDB, isSnapshot, func(rt fdb.ReadTransaction) error {
			var err error
			v, err = rt.Get(ctx, key)
			return err
		})
		if v == nil {
			v = resultNotPresent
		}
		sm.push(idx, v)
	case "GET_KEY":
		sel := sm.popSelector()
		prefix := sm.popBytes()
		var key []byte
		sm.read(isDB, isSnapshot, func(rt fdb.ReadTransaction) error {
			var err error
			key, err = rt.GetKey(ctx, sel)
			return err
		})
		switch {
		case bytes.HasPrefix(key, prefix):
			sm.push(idx, key)
		case bytes.Compare(key, prefix) < 0:
			sm.push(idx, prefix)
		default:
			end, err := fdb.Strinc(prefix)
			if err != nil {
				log.Panicf("strinc: %v", err)
			}
			sm.push(idx, end)
		}
	case "GET_RANGE", "GET_RANGE_STARTS_WITH", "GET_RANGE_SELECTOR":
		var r fdb.Range
		switch op {
		case "GET_RANGE":
			r = sm.popKeyRange()
		case "GET_RANGE_STARTS_WITH":
			r = sm.popPrefixRange()
		default:
			begin := sm.popSelector()
			end := sm.popSelector()
			r = fdb.SelectorRange{Begin: begin, End: end}
		}
		opts := sm.popRangeOptions()
		var prefix []byte
		if op == "GET_RANGE_SELECTOR" {
			prefix = sm.popBytes()
		}
		var kvs []fdb.KeyValue
		sm.read(isDB, isSnapshot, func(rt fdb.ReadTransaction) error {
			result, err := rt.GetRange(ctx, r, opts)
			if err != nil {
				return err
			}
			kvs, err = result.Slice(ctx)
			return err
		})
		sm.pushRange(idx, kvs, prefix)
	case "GET_READ_VERSION":
		sm.read(isDB, isSnapshot, func(rt fdb.ReadTransaction) error {
			v, err := rt.ReadVersion(ctx)
			if err != nil {
				return err
			}
			sm.lastVersion = v
			return nil
		})
		sm.push(idx, gotReadVersion)
	case "GET_VERSIONSTAMP":
		sm.registerVersionstamp(idx)
	case "GET_ESTIMATED_RANGE_SIZE":
		r := sm.popKeyRange()
		sm.read(isDB, isSnapshot, func(rt fdb.ReadTransaction) error {
			_, err := rt.EstimatedRangeSizeBytes(ctx, r)
			return err
		})
		sm.push(idx, gotEstimatedSize)
	case "GET_RANGE_SPLIT_POINTS":
		r := sm.popKeyRange()
		chunkSize := sm.popInt()
		sm.read(isDB, isSnapshot, func(rt fdb.ReadTransaction) error {
			_, err := rt.RangeSplitPoints(ctx, r, chunkSize)
			return err
		})
		sm.push(idx, gotRangeSplitPoints)
	case "GET_APPROXIMATE_SIZE":
		_, err := sm.currentTransaction().ApproximateSize(ctx)
		check(err)
		sm.push(idx, gotApproximateSize)
	case "SET_READ_VERSION":
		sm.currentTransaction().SetReadVersion(sm.lastVersion)
	case "GET_COMMITTED_VERSION":
		v, err := sm.currentTransaction().CommittedVersion()
		check(err)
		sm.lastVersion = v
		sm.push(idx, gotCommittedVersion)

	case "SET":
		key := sm.popBytes()
		value := sm.popBytes()
		sm.executeMutation(isDB, idx, func(tr *fdb.Transaction) { tr.Set(key, value) })
	case "CLEAR":
		key := sm.popBytes()
		sm.executeMutation(isDB, idx, func(tr *fdb.Transaction) { tr.Clear(key) })
	case "CLEAR_RANGE":
		r := sm.popKeyRange()
		sm.executeMutation(isDB, idx, func(tr *fdb.Transaction) { tr.ClearRange(r) })
	case "CLEAR_RANGE_STARTS_WITH":
		r := sm.popPrefixRange()
		sm.executeMutation(isDB, idx, func(tr *fdb.Transaction) { tr.ClearRange(r) })
	case "ATOMIC_OP":
		name := sm.popString()
		key := sm.popBytes()
		value := sm.popBytes()
		mt, ok := mutationTypes[name]
		if !ok {
			log.Panicf("unknown atomic op %q", name)
		}
		key, value = sm.mutationOperands(mt, key, value)
		sm.executeMutation(isDB, idx, func(tr *fdb.Transaction) { tr.Atomic(mt, key, value) })
	case "READ_CONFLICT_RANGE":
		check(sm.currentTransaction().AddReadConflictRange(sm.popKeyRange()))
		sm.push(idx, setConflictRange)
	case "WRITE_CONFLICT_RANGE":
		check(sm.currentTransaction().AddWriteConflictRange(sm.popKeyRange()))
		sm.push(idx, setConflictRange)
	case "READ_CONFLICT_KEY":
		check(sm.currentTransaction().AddReadConflictKey(sm.popBytes()))
		sm.push(idx, setConflictKey)
	case "WRITE_CONFLICT_KEY":
		check(sm.currentTransaction().AddWriteConflictKey(sm.popBytes()))
		sm.push(idx, setConflictKey)
	case "DISABLE_WRITE_CONFLICT":
		check(sm.currentTransaction().Options().SetNextWriteNoWriteConflictRange())

	case "TUPLE_PACK":
		sm.push(idx, mustPack(sm.popTuple(sm.popInt())))
	case "TUPLE_PACK_WITH_VERSIONSTAMP":
		prefix := sm.popBytes()
		t := sm.popTuple(sm.popInt())
		packed, err := sm.packWithVersionstamp(t, prefix)
		switch {
		case errors.Is(err, tuple.ErrNoIncompleteVersionstamp):
			sm.push(idx, errorNone)
		case errors.Is(err, tuple.ErrMultipleIncompleteVersionstamps):
			sm.push(idx, errorMultiple)
		case err != nil:
			log.Panicf("pack with versionstamp: %v", err)
		default:
			sm.push(idx, okResult)
			sm.push(idx, packed)
		}
	case "TUPLE_UNPACK":
		t, err := tuple.Unpack(sm.popBytes())
		if err != nil {
			log.Panicf("unpack: %v", err)
		}
		for _, el := range t {
			sm.push(idx, mustPack(tuple.Tuple{el}))
		}
	case "TUPLE_RANGE":
		t := sm.popTuple(sm.popInt())
		begin, end, err := t.RangeKeys()
		if err != nil {
			log.Panicf("tuple range: %v", err)
		}
		sm.push(idx, begin)
		sm.push(idx, end)
	case "TUPLE_SORT":
		n := sm.popInt()
		packed := make([][]byte, n)
		for i := range packed {
			// Repacking the unpacked tuple canonicalizes it; the packed form
			// orders like the tuples.
			t, err := tuple.Unpack(sm.popBytes())
			if err != nil {
				log.Panicf("unpack: %v", err)
			}
			packed[i] = mustPack(t)
		}
		slices.SortFunc(packed, bytes.Compare)
		for _, p := range packed {
			sm.push(idx, p)
		}
	case "ENCODE_FLOAT":
		b := sm.popBytes()
		sm.push(idx, math.Float32frombits(binary.BigEndian.Uint32(b)))
	case "ENCODE_DOUBLE":
		b := sm.popBytes()
		sm.push(idx, math.Float64frombits(binary.BigEndian.Uint64(b)))
	case "DECODE_FLOAT":
		v := sm.pop()
		f, ok := v.(float32)
		if !ok {
			log.Panicf("DECODE_FLOAT of %T", v)
		}
		sm.push(idx, binary.BigEndian.AppendUint32(nil, math.Float32bits(f)))
	case "DECODE_DOUBLE":
		v := sm.pop()
		f, ok := v.(float64)
		if !ok {
			log.Panicf("DECODE_DOUBLE of %T", v)
		}
		sm.push(idx, binary.BigEndian.AppendUint64(nil, math.Float64bits(f)))

	case "START_THREAD":
		child := newStackMachine(bytes.Clone(sm.popBytes()), sm.apiVersion)
		sm.threads.Go(child.Run)
	case "WAIT_EMPTY":
		r := sm.popPrefixRange()
		check(db.Transact(ctx, func(tr *fdb.Transaction) error {
			result, err := tr.GetRange(ctx, r, fdb.RangeOptions{Limit: 1})
			if err != nil {
				return err
			}
			kvs, err := result.Slice(ctx)
			if err != nil {
				return err
			}
			if len(kvs) != 0 {
				return fdb.ErrNotCommitted
			}
			return nil
		}))
		sm.push(idx, waitedForEmpty)
	case "UNIT_TESTS":
		unitTests()

	default:
		if strings.HasPrefix(op, "DIRECTORY_") {
			sm.de.processOp(sm, op[len("DIRECTORY_"):], isDB, isSnapshot, idx)
			break
		}
		log.Panicf("unhandled operation %s", inst[0])
	}
}

func toBig(v any) *big.Int {
	switch n := v.(type) {
	case int64:
		return big.NewInt(n)
	case uint64:
		return new(big.Int).SetUint64(n)
	case *big.Int:
		return new(big.Int).Set(n)
	}
	log.Panicf("SUB of %T", v)
	return nil
}

// fromBig returns n as the narrowest integer type that holds it, as the
// tuple layer decodes it.
func fromBig(n *big.Int) any {
	switch {
	case n.IsInt64():
		return n.Int64()
	case n.IsUint64():
		return n.Uint64()
	}
	return n
}

// Run reads the machine's instructions and executes them in order, then
// waits for the threads it started.
func (sm *StackMachine) Run() {
	r, err := fdb.PrefixRange(mustPack(tuple.Tuple{sm.prefix}))
	if err != nil {
		log.Fatalf("instruction range: %v", err)
	}
	kvs, err := fdb.ReadTransact(ctx, db, func(rt fdb.ReadTransaction) ([]fdb.KeyValue, error) {
		result, err := rt.GetRange(ctx, r, fdb.RangeOptions{Mode: fdb.StreamingModeWantAll})
		if err != nil {
			return nil, err
		}
		return result.Slice(ctx)
	})
	if err != nil {
		log.Fatalf("reading instructions: %v", err)
	}
	for i, kv := range kvs {
		inst, err := tuple.Unpack(kv.Value)
		if err != nil {
			log.Fatalf("instruction %d: %v", i, err)
		}
		sm.processInst(i, inst)
	}
	sm.threads.Wait()
}

func main() {
	log.SetFlags(log.Lmicroseconds)
	if len(os.Args) < 3 {
		fmt.Fprintln(os.Stderr, "usage: stacktester <prefix> <api_version> [cluster_file]")
		os.Exit(2)
	}
	prefix := []byte(os.Args[1])
	apiVersion, err := strconv.Atoi(os.Args[2])
	if err != nil {
		log.Fatalf("api version %q: %v", os.Args[2], err)
	}
	clusterFile := ""
	if len(os.Args) > 3 {
		clusterFile = os.Args[3]
	}
	db, err = fdb.Open(clusterFile)
	if err != nil {
		log.Fatalf("open %q: %v", clusterFile, err)
	}
	newStackMachine(prefix, apiVersion).Run()
	if err := db.Close(); err != nil {
		log.Fatalf("close: %v", err)
	}
}

// unitTests runs the UNIT_TESTS instruction: option setters, watches and
// locality, which the stack machine does not otherwise exercise. Failures
// that are fdb errors become the instruction's error result.
func unitTests() {
	dbo := db.Options()
	check(dbo.SetLocationCacheSize(100001))
	check(dbo.SetMaxWatches(10001))
	check(dbo.SetDatacenterID("dc_id"))
	check(dbo.SetMachineID("machine_id"))
	check(dbo.SetSnapshotRYWEnable())
	check(dbo.SetSnapshotRYWDisable())
	check(dbo.SetTransactionLoggingMaxFieldLength(1000))
	check(dbo.SetTransactionTimeout(100000))
	check(dbo.SetTransactionTimeout(0))
	check(dbo.SetTransactionMaxRetryDelay(100))
	check(dbo.SetTransactionRetryLimit(10))
	check(dbo.SetTransactionRetryLimit(-1))
	check(dbo.SetTransactionCausalReadRisky())

	check(db.Transact(ctx, func(tr *fdb.Transaction) error {
		o := tr.Options()
		for _, err := range []error{
			o.SetPrioritySystemImmediate(),
			o.SetPriorityBatch(),
			o.SetCausalReadRisky(),
			o.SetCausalWriteRisky(),
			o.SetReadYourWritesDisable(),
			o.SetReadSystemKeys(),
			o.SetAccessSystemKeys(),
			o.SetTransactionLoggingMaxFieldLength(1000),
			o.SetTimeout(60 * 1000),
			o.SetRetryLimit(50),
			o.SetMaxRetryDelay(100),
			o.SetUsedDuringCommitProtectionDisable(),
			o.SetDebugTransactionIdentifier("my_transaction"),
			o.SetLogTransaction(),
			o.SetReadLockAware(),
			o.SetLockAware(),
			o.SetIncludePortInAddress(),
		} {
			if err != nil {
				return err
			}
		}
		_, err := tr.Get(ctx, []byte("\xff"))
		return err
	}))

	testWatches()
	testLocality()
}

// checkWatches waits for the watches when expected, else checks that none
// fired yet. A watch failing with a retryable error passes it through
// OnError (as the C client would retry) and restarts the round.
func checkWatches(watches []*fdb.Watch, expected bool) bool {
	for _, w := range watches {
		if !w.IsReady() && !expected {
			continue
		}
		if err := w.Wait(ctx); err != nil {
			tr, terr := db.CreateTransaction()
			check(terr)
			check(tr.OnError(ctx, err))
		}
		if !expected {
			return false
		}
	}
	return true
}

func testWatches() {
	for {
		check(db.Transact(ctx, func(tr *fdb.Transaction) error {
			tr.Set([]byte("w0"), []byte("0"))
			tr.Set([]byte("w2"), []byte("2"))
			tr.Set([]byte("w3"), []byte("3"))
			return nil
		}))

		var watches []*fdb.Watch
		check(db.Transact(ctx, func(tr *fdb.Transaction) error {
			watches = []*fdb.Watch{
				tr.Watch([]byte("w0")),
				tr.Watch([]byte("w1")),
				tr.Watch([]byte("w2")),
				tr.Watch([]byte("w3")),
			}
			tr.Set([]byte("w0"), []byte("0"))
			tr.Clear([]byte("w1"))
			return nil
		}))

		time.Sleep(5 * time.Second)
		if !checkWatches(watches, false) {
			continue
		}

		check(db.Transact(ctx, func(tr *fdb.Transaction) error {
			tr.Set([]byte("w0"), []byte("a"))
			tr.Set([]byte("w1"), []byte("b"))
			tr.Clear([]byte("w2"))
			tr.BitXor([]byte("w3"), []byte("\xff\xff"))
			return nil
		}))
		if checkWatches(watches, true) {
			return
		}
	}
}

// testLocality checks that each shard's first and last keys are served by
// the same storage servers.
func testLocality() {
	check(db.Transact(ctx, func(tr *fdb.Transaction) error {
		if err := tr.Options().SetTimeout(60 * 1000); err != nil {
			return err
		}
		if err := tr.Options().SetReadSystemKeys(); err != nil {
			return err
		}
		boundaries, err := db.BoundaryKeys(ctx, fdb.KeyRange{Begin: []byte(""), End: []byte("\xff\xff")}, 0, 0)
		if err != nil {
			return err
		}
		for i := 0; i+1 < len(boundaries); i++ {
			start := boundaries[i]
			end, err := tr.GetKey(ctx, fdb.LastLessThan(boundaries[i+1]))
			if err != nil {
				return err
			}
			startAddrs, err := tr.AddressesForKey(ctx, start)
			if err != nil {
				return err
			}
			endAddrs, err := tr.AddressesForKey(ctx, end)
			if err != nil {
				return err
			}
			for _, a := range startAddrs {
				if !slices.Contains(endAddrs, a) {
					log.Panicf("locality not internally consistent: %q has %v, %q has %v", start, startAddrs, end, endAddrs)
				}
			}
		}
		return nil
	}))
}
