package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"math/big"
	"os"
	"testing"
	"time"

	"github.com/can1357/fdbgo/fdb"
	"github.com/can1357/fdbgo/subspace"
	"github.com/can1357/fdbgo/tuple"
)

// run executes instructions on sm and returns the resolved stack, bottom
// first.
func run(t *testing.T, sm *StackMachine, insts ...tuple.Tuple) []any {
	t.Helper()
	for i, inst := range insts {
		sm.processInst(i, inst)
	}
	out := make([]any, len(sm.stack))
	for i := len(sm.stack) - 1; i >= 0; i-- {
		out[i] = sm.pop()
	}
	return out
}

// pureMachine is a stack machine for instructions that do not touch the
// database.
func pureMachine() *StackMachine {
	return &StackMachine{apiVersion: 740, prefix: []byte("pure"), de: &directoryExtension{}}
}

func TestDirectoryInvalidOperandPreservesStack(t *testing.T) {
	for _, op := range []string{"STRIP_PREFIX", "UNPACK_KEY", "CONTAINS", "LOG_SUBSPACE"} {
		t.Run(op, func(t *testing.T) {
			sm := pureMachine()
			sm.de = &directoryExtension{list: []any{subspace.FromBytes([]byte("prefix/"))}}
			sm.push(0, []byte("sentinel"))
			sm.push(1, int64(1))
			sm.de.processOp(sm, op, true, false, 2)
			result, ok := sm.pop().([]byte)
			if !ok || !bytes.Equal(result, directoryErrorResult) {
				t.Fatalf("invalid directory operand = %v; want DIRECTORY_ERROR", result)
			}
			if value := sm.pop(); !bytes.Equal(value.([]byte), []byte("sentinel")) {
				t.Fatalf("unrelated stack entry changed: %v", value)
			}
		})
	}
}

func TestSubOverflowsToBigInt(t *testing.T) {
	got := run(t, pureMachine(),
		tuple.Tuple{"PUSH", int64(1)},
		tuple.Tuple{"PUSH", int64(math.MinInt64)},
		tuple.Tuple{"SUB"},
		tuple.Tuple{"PUSH", int64(-1)},
		tuple.Tuple{"PUSH", int64(math.MaxInt64)},
		tuple.Tuple{"SUB"},
	)
	want := new(big.Int).Sub(big.NewInt(math.MinInt64), big.NewInt(1))
	if b, ok := got[0].(*big.Int); !ok || b.Cmp(want) != 0 {
		t.Errorf("MinInt64-1 = %T %v, want %v", got[0], got[0], want)
	}
	if u, ok := got[1].(uint64); !ok || u != 1<<63 {
		t.Errorf("MaxInt64+1 = %T %v, want uint64 2^63", got[1], got[1])
	}
}

func TestFloatRoundTrip(t *testing.T) {
	f := []byte{0x7f, 0xc0, 0x00, 0x01} // a NaN payload must survive
	d := []byte{0xc0, 0x09, 0x21, 0xfb, 0x54, 0x44, 0x2d, 0x18}
	got := run(t, pureMachine(),
		tuple.Tuple{"PUSH", f},
		tuple.Tuple{"ENCODE_FLOAT"},
		tuple.Tuple{"DECODE_FLOAT"},
		tuple.Tuple{"PUSH", d},
		tuple.Tuple{"ENCODE_DOUBLE"},
		tuple.Tuple{"DECODE_DOUBLE"},
	)
	if !bytes.Equal(got[0].([]byte), f) || !bytes.Equal(got[1].([]byte), d) {
		t.Errorf("got %x %x, want %x %x", got[0], got[1], f, d)
	}
}

func TestTupleSortAndVersionstamp(t *testing.T) {
	a := mustPack(tuple.Tuple{"b"})
	b := mustPack(tuple.Tuple{int64(-5)})
	c := mustPack(tuple.Tuple{[]byte("a"), nil})
	got := run(t, pureMachine(),
		tuple.Tuple{"PUSH", a},
		tuple.Tuple{"PUSH", b},
		tuple.Tuple{"PUSH", c},
		tuple.Tuple{"PUSH", int64(3)},
		tuple.Tuple{"TUPLE_SORT"},
	)
	if len(got) != 3 || !bytes.Equal(got[0].([]byte), c) || !bytes.Equal(got[1].([]byte), a) || !bytes.Equal(got[2].([]byte), b) {
		t.Errorf("TUPLE_SORT = %q", got)
	}

	vs := tuple.IncompleteVersionstamp(7)
	got = run(t, pureMachine(),
		tuple.Tuple{"PUSH", vs},
		tuple.Tuple{"PUSH", vs},
		tuple.Tuple{"PUSH", int64(2)},
		tuple.Tuple{"PUSH", []byte("p")},
		tuple.Tuple{"TUPLE_PACK_WITH_VERSIONSTAMP"},
		tuple.Tuple{"PUSH", "x"},
		tuple.Tuple{"PUSH", int64(1)},
		tuple.Tuple{"PUSH", []byte("p")},
		tuple.Tuple{"TUPLE_PACK_WITH_VERSIONSTAMP"},
		tuple.Tuple{"PUSH", vs},
		tuple.Tuple{"PUSH", int64(1)},
		tuple.Tuple{"PUSH", []byte("p")},
		tuple.Tuple{"TUPLE_PACK_WITH_VERSIONSTAMP"},
	)
	want, err := tuple.Tuple{vs}.PackWithVersionstamp([]byte("p"))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 4 || !bytes.Equal(got[0].([]byte), errorMultiple) || !bytes.Equal(got[1].([]byte), errorNone) ||
		!bytes.Equal(got[2].([]byte), okResult) || !bytes.Equal(got[3].([]byte), want) {
		t.Errorf("TUPLE_PACK_WITH_VERSIONSTAMP = %q", got)
	}
}

func TestTupleVersionstampDeclaredAPIVersion(t *testing.T) {
	vs := tuple.IncompleteVersionstamp(7)
	prefix := []byte("prefix/")
	canonical, err := (tuple.Tuple{vs}).PackWithVersionstamp(prefix)
	if err != nil {
		t.Fatal(err)
	}
	for _, api := range []int{500, 510, 520, 740} {
		t.Run(fmt.Sprint(api), func(t *testing.T) {
			sm := pureMachine()
			sm.apiVersion = api
			got := run(t, sm,
				tuple.Tuple{"PUSH", vs},
				tuple.Tuple{"PUSH", vs},
				tuple.Tuple{"PUSH", int64(2)},
				tuple.Tuple{"PUSH", prefix},
				tuple.Tuple{"TUPLE_PACK_WITH_VERSIONSTAMP"},
				tuple.Tuple{"PUSH", "complete"},
				tuple.Tuple{"PUSH", int64(1)},
				tuple.Tuple{"PUSH", prefix},
				tuple.Tuple{"TUPLE_PACK_WITH_VERSIONSTAMP"},
				tuple.Tuple{"PUSH", vs},
				tuple.Tuple{"PUSH", int64(1)},
				tuple.Tuple{"PUSH", prefix},
				tuple.Tuple{"TUPLE_PACK_WITH_VERSIONSTAMP"},
			)
			want := canonical
			if api < 520 {
				want = want[:len(want)-2]
			}
			if len(got) != 4 || !bytes.Equal(got[0].([]byte), errorMultiple) ||
				!bytes.Equal(got[1].([]byte), errorNone) || !bytes.Equal(got[2].([]byte), okResult) ||
				!bytes.Equal(got[3].([]byte), want) {
				t.Fatalf("tuple versionstamp results = %q", got)
			}

			// Python's uint16 pack rejects overflow rather than truncating it.
			for _, offset := range []int{math.MaxUint16, math.MaxUint16 + 1} {
				packed, err := sm.packWithVersionstamp(tuple.Tuple{vs}, bytes.Repeat([]byte("p"), offset-1))
				if api < 520 && offset > math.MaxUint16 {
					if err == nil {
						t.Fatal("old tuple format accepted an overflowing offset")
					}
					continue
				}
				if err != nil {
					t.Fatal(err)
				}
				var gotOffset uint32
				if api < 520 {
					gotOffset = uint32(binary.LittleEndian.Uint16(packed[len(packed)-2:]))
				} else {
					gotOffset = binary.LittleEndian.Uint32(packed[len(packed)-4:])
				}
				if gotOffset != uint32(offset) {
					t.Fatalf("offset = %d, want %d", gotOffset, offset)
				}
			}
		})
	}
}

func TestVersionstampMutationOperands(t *testing.T) {
	for _, api := range []int{500, 510, 520, 740} {
		t.Run(fmt.Sprint(api), func(t *testing.T) {
			sm := pureMachine()
			sm.apiVersion = api
			for _, tc := range []struct {
				name       string
				mt         fdb.MutationType
				key, value []byte
			}{
				{"key", fdb.MutationTypeSetVersionstampedKey, []byte("pXXXXXXXXXX\x01\x00"), []byte("v")},
				{"key-short", fdb.MutationTypeSetVersionstampedKey, []byte{1}, []byte("v")},
				{"key-empty-body", fdb.MutationTypeSetVersionstampedKey, []byte{0, 0}, []byte("v")},
				{"key-invalid-offset", fdb.MutationTypeSetVersionstampedKey, []byte("XXXXXXXXXX\xff\xff"), []byte("v")},
				{"value", fdb.MutationTypeSetVersionstampedValue, []byte("k"), []byte("XXXXXXXXXXtail\xff\xff\xff\xff")},
				{"value-nonzero-offset", fdb.MutationTypeSetVersionstampedValue, []byte("k"), []byte("headXXXXXXXXXXtail\x04\x00\x00\x00")},
				{"value-short", fdb.MutationTypeSetVersionstampedValue, []byte("k"), []byte("XXXXXXXXX")},
				{"value-empty", fdb.MutationTypeSetVersionstampedValue, []byte("k"), nil},
				{"unrelated", fdb.MutationTypeAdd, []byte("k"), []byte{1}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					originalKey, originalValue := bytes.Clone(tc.key), bytes.Clone(tc.value)
					key, value := sm.mutationOperands(tc.mt, tc.key, tc.value)
					wantKey, wantValue := bytes.Clone(tc.key), bytes.Clone(tc.value)
					if api < 520 {
						switch tc.mt {
						case fdb.MutationTypeSetVersionstampedKey:
							if len(wantKey) >= 2 {
								wantKey = append(wantKey, 0, 0)
							}
						case fdb.MutationTypeSetVersionstampedValue:
							wantValue = append(wantValue, 0, 0, 0, 0)
						}
					}
					if !bytes.Equal(key, wantKey) || !bytes.Equal(value, wantValue) {
						t.Fatalf("operands = %x, %x; want %x, %x", key, value, wantKey, wantValue)
					}
					if !bytes.Equal(tc.key, originalKey) || !bytes.Equal(tc.value, originalValue) {
						t.Fatal("operand adaptation mutated a stack value")
					}
					if api >= 520 && (len(key) > 0 && &key[0] != &tc.key[0] ||
						len(value) > 0 && &value[0] != &tc.value[0]) {
						t.Fatal("current API operands were copied")
					}
				})
			}
		})
	}
}

// Reproduce the API500 generator's VERSIONSTAMP pair: an implicit value offset
// and a uint16 key offset, followed by GET_READ_VERSION. Previously that read
// returned client_invalid_operation even though both old operands were valid.
func TestVersionstampDeclaredAPIInstructions(t *testing.T) {
	cf := os.Getenv("FDB_CLUSTER_FILE")
	if cf == "" {
		t.Skip("FDB_CLUSTER_FILE not set")
	}
	var err error
	if db, err = fdb.Open(cf); err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	prefix := []byte("stacktester-versionstamp-formats/")
	defer func() {
		if err := db.Transact(ctx, func(tr *fdb.Transaction) error {
			tr.ClearRange(fdb.KeyRange{Begin: prefix, End: append(bytes.Clone(prefix), 0xff)})
			return nil
		}); err != nil {
			t.Error(err)
		}
	}()
	for _, api := range []int{500, 510, 520, 740} {
		t.Run(fmt.Sprint(api), func(t *testing.T) {
			keyPrefix := []byte(fmt.Sprintf("%s%d/", prefix, api))
			key := append(bytes.Clone(keyPrefix), []byte("XXXXXXXXXXtail")...)
			value := []byte("XXXXXXXXXXtail\xff\xff\xff\xff")
			if api < 520 {
				key = binary.LittleEndian.AppendUint16(key, uint16(len(keyPrefix)))
			} else {
				key = binary.LittleEndian.AppendUint32(key, uint32(len(keyPrefix)))
				// Offset zero is representable even on pre-520 servers.
				// Nonzero current offsets are covered by the pure operand test.
				value = binary.LittleEndian.AppendUint32(value, 0)
			}
			valueKey := append(bytes.Clone(keyPrefix), []byte("value")...)
			sm := newStackMachine(keyPrefix, api)
			got := run(t, sm,
				tuple.Tuple{"PUSH", value},
				tuple.Tuple{"PUSH", valueKey},
				tuple.Tuple{"PUSH", "SET_VERSIONSTAMPED_VALUE"},
				tuple.Tuple{"ATOMIC_OP"},
				tuple.Tuple{"PUSH", []byte("key-value")},
				tuple.Tuple{"PUSH", key},
				tuple.Tuple{"PUSH", "SET_VERSIONSTAMPED_KEY"},
				tuple.Tuple{"ATOMIC_OP"},
				tuple.Tuple{"GET_READ_VERSION"},
				tuple.Tuple{"COMMIT"},
				tuple.Tuple{"WAIT_FUTURE"},
			)
			if len(got) != 2 || !bytes.Equal(got[0].([]byte), gotReadVersion) ||
				!bytes.Equal(got[1].([]byte), resultNotPresent) {
				t.Fatalf("valid versionstamp pair = %q", got)
			}
			stamp, err := sm.currentTransaction().Versionstamp()
			if err != nil {
				t.Fatal(err)
			}
			stampedKey := append(bytes.Clone(keyPrefix), stamp...)
			stampedKey = append(stampedKey, []byte("tail")...)
			stampedValue := append(bytes.Clone(stamp), []byte("tail\xff\xff\xff\xff")...)
			if err := db.ReadTransact(ctx, func(tr fdb.ReadTransaction) error {
				for _, kv := range []fdb.KeyValue{{Key: stampedKey, Value: []byte("key-value")}, {Key: valueKey, Value: stampedValue}} {
					got, err := tr.Get(ctx, kv.Key)
					if err != nil {
						return err
					}
					if !bytes.Equal(got, kv.Value) {
						t.Errorf("key %x = %x, want %x", kv.Key, got, kv.Value)
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}

			for _, op := range []string{"SET_VERSIONSTAMPED_KEY", "SET_VERSIONSTAMPED_VALUE"} {
				for _, cancelled := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/cancelled=%t", op, cancelled), func(t *testing.T) {
						sm := newStackMachine(keyPrefix, api)
						if cancelled {
							sm.processInst(0, tuple.Tuple{"CANCEL"})
						}
						key, value := valueKey, []byte("XXXXXXXXX")
						if op == "SET_VERSIONSTAMPED_KEY" {
							key, value = []byte{0}, []byte("v")
						} else if api >= 520 {
							value = binary.LittleEndian.AppendUint32(value, 0)
						}
						got := run(t, sm,
							tuple.Tuple{"PUSH", value},
							tuple.Tuple{"PUSH", key},
							tuple.Tuple{"PUSH", op},
							tuple.Tuple{"ATOMIC_OP"},
							tuple.Tuple{"GET_READ_VERSION"},
						)
						// GET_READ_VERSION reports deferred malformed-operand
						// errors before cancellation, matching the C client.
						if len(got) != 1 || !bytes.Equal(got[0].([]byte), packErrorCode(2000)) {
							t.Fatalf("invalid operand result = %q, want error 2000", got)
						}
					})
				}
			}
		})
	}
}

// A registered versionstamp keeps its first outcome even after later lifecycle
// events, and duplicated stack entries can wait on it independently.
func TestVersionstampResultRetainsAttempt(t *testing.T) {
	sm := pureMachine()
	first := newVersionstampResult()
	sm.pushWait(7, first.stackValue)
	sm.processInst(8, tuple.Tuple{"DUP"})
	first.resolve(nil, fdb.ErrBrokenPromise)

	next := newVersionstampResult()
	stamp := []byte{0, 0, 0, 0, 0, 0, 0, 1, 0, 0}
	next.resolve(stamp, nil)
	first.resolve(stamp, nil)
	for range 2 {
		e := sm.waitAndPop()
		if e.idx != 7 || !bytes.Equal(e.val.([]byte), errorBytes(fdb.ErrBrokenPromise)) {
			t.Fatalf("old attempt = %+v", e)
		}
	}
	next.resolve(nil, fdb.ErrTransactionCancelled)
	if got := next.stackValue().([]byte); !bytes.Equal(got, stamp) {
		t.Fatalf("committed stamp changed after cancellation: %x", got)
	}
}

// Native commit errors invalidate a stamp, but attempt-ending errors retain
// their identity. OnError uses cancellation only for a rejected input error.
func TestVersionstampFailurePreservesAttemptEnd(t *testing.T) {
	for _, fallback := range []fdb.Error{fdb.ErrTransactionInvalidVersion, fdb.ErrTransactionCancelled} {
		for _, err := range []fdb.Error{fdb.ErrTransactionTimedOut, fdb.ErrTransactionCancelled, fdb.ErrUsedDuringCommit} {
			if got := versionstampFailure(err, fallback); got != err {
				t.Errorf("versionstampFailure(%v, %v) = %v", err, fallback, got)
			}
		}
		if got := versionstampFailure(fdb.ErrNotCommitted, fallback); got != fallback {
			t.Errorf("ordinary error with fallback %v = %v", fallback, got)
		}
	}
}

// GET_VERSIONSTAMP must register without waiting, then resolve the original
// attempt on commit, reset, cancellation or OnError rather than following retries.
func TestVersionstampProtocolLifecycle(t *testing.T) {
	cf := os.Getenv("FDB_CLUSTER_FILE")
	if cf == "" {
		t.Skip("FDB_CLUSTER_FILE not set")
	}
	var err error
	if db, err = fdb.Open(cf); err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	key := []byte("stacktester-versionstamp-test")
	defer func() {
		if err := db.Transact(ctx, func(tr *fdb.Transaction) error {
			tr.Clear(key)
			return nil
		}); err != nil {
			t.Error(err)
		}
	}()
	set := []tuple.Tuple{
		{"PUSH", []byte("value")},
		{"PUSH", key},
		{"SET"},
	}
	for _, tc := range []struct {
		name string
		end  []tuple.Tuple
		want error
	}{
		{"reset", []tuple.Tuple{{"RESET"}}, fdb.ErrBrokenPromise},
		{"cancel", []tuple.Tuple{{"CANCEL"}, {"RESET"}}, fdb.ErrTransactionCancelled},
		{"retry", []tuple.Tuple{{"PUSH", int64(1020)}, {"ON_ERROR"}, {"WAIT_FUTURE"}, {"POP"}}, fdb.ErrBrokenPromise},
		{"failed-on-error", []tuple.Tuple{{"PUSH", int64(2000)}, {"ON_ERROR"}, {"WAIT_FUTURE"}, {"POP"}, {"RESET"}}, fdb.ErrTransactionCancelled},
		{"replace", []tuple.Tuple{{"NEW_TRANSACTION"}}, fdb.ErrBrokenPromise},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sm := newStackMachine([]byte("stacktester-versionstamp-"+tc.name), 740)
			insts := []tuple.Tuple{{"GET_VERSIONSTAMP"}}
			insts = append(insts, tc.end...)
			insts = append(insts, set...)
			insts = append(insts,
				tuple.Tuple{"GET_VERSIONSTAMP"}, tuple.Tuple{"DUP"},
				tuple.Tuple{"COMMIT"}, tuple.Tuple{"WAIT_FUTURE"}, tuple.Tuple{"POP"},
				tuple.Tuple{"RESET"},
			)
			got := run(t, sm, insts...)
			if len(got) != 3 || !bytes.Equal(got[0].([]byte), errorBytes(tc.want)) {
				t.Fatalf("old attempt = %v, want %v", got, tc.want)
			}
			first, second := got[1].([]byte), got[2].([]byte)
			if len(first) != 10 || !bytes.Equal(first, second) {
				t.Fatalf("committed duplicate stamps = %x, %x", first, second)
			}
		})
	}

	for _, op := range []string{"COMMIT", "ON_ERROR", "GET_READ_VERSION"} {
		t.Run("expired-"+op, func(t *testing.T) {
			sm := newStackMachine([]byte("stacktester-versionstamp-expired-"+op), 740)
			sm.processInst(0, tuple.Tuple{"GET_VERSIONSTAMP"})
			if err := sm.currentTransaction().Options().SetTimeout(1); err != nil {
				t.Fatal(err)
			}
			// Let the attempt's timer expire before the lifecycle operation.
			time.Sleep(20 * time.Millisecond)
			if op == "ON_ERROR" {
				sm.push(1, int64(fdb.ErrNotCommitted))
			}
			sm.processInst(2, tuple.Tuple{op})
			if got := sm.pop().([]byte); !bytes.Equal(got, errorBytes(fdb.ErrTransactionTimedOut)) {
				t.Fatalf("%s = %x, want transaction_timed_out", op, got)
			}
			sm.processInst(3, tuple.Tuple{"RESET"})
			if got := sm.pop().([]byte); !bytes.Equal(got, errorBytes(fdb.ErrTransactionTimedOut)) {
				t.Fatalf("expired attempt stamp = %x, want transaction_timed_out", got)
			}
		})
	}

	t.Run("failed-commit", func(t *testing.T) {
		sm := newStackMachine([]byte("stacktester-versionstamp-conflict"), 740)
		sm.processInst(0, tuple.Tuple{"GET_VERSIONSTAMP"})
		tr := sm.currentTransaction()
		if _, err := tr.Get(ctx, key); err != nil {
			t.Fatal(err)
		}
		if err := db.Transact(ctx, func(other *fdb.Transaction) error {
			other.Set(key, []byte("conflicting write"))
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		tr.Set(key, []byte("must conflict"))
		sm.processInst(1, tuple.Tuple{"COMMIT"})
		if got := sm.pop().([]byte); !bytes.Equal(got, errorBytes(fdb.ErrNotCommitted)) {
			t.Fatalf("commit = %x, want not_committed", got)
		}
		sm.processInst(2, tuple.Tuple{"RESET"})
		if got := sm.pop().([]byte); !bytes.Equal(got, errorBytes(fdb.ErrTransactionInvalidVersion)) {
			t.Fatalf("failed commit stamp = %x, want transaction_invalid_version", got)
		}
	})

	t.Run("read-only", func(t *testing.T) {
		got := run(t, newStackMachine([]byte("stacktester-versionstamp-read-only"), 740),
			tuple.Tuple{"GET_VERSIONSTAMP"}, tuple.Tuple{"COMMIT"},
			tuple.Tuple{"WAIT_FUTURE"}, tuple.Tuple{"POP"}, tuple.Tuple{"RESET"},
		)
		if len(got) != 1 || !bytes.Equal(got[0].([]byte), errorBytes(fdb.ErrNoCommitVersion)) {
			t.Fatalf("read-only stamp = %v", got)
		}
	})

	t.Run("cancelled-registration", func(t *testing.T) {
		got := run(t, newStackMachine([]byte("stacktester-versionstamp-cancelled"), 740),
			tuple.Tuple{"CANCEL"}, tuple.Tuple{"GET_VERSIONSTAMP"},
			tuple.Tuple{"WAIT_FUTURE"}, tuple.Tuple{"RESET"},
		)
		if len(got) != 1 || !bytes.Equal(got[0].([]byte), errorBytes(fdb.ErrTransactionCancelled)) {
			t.Fatalf("cancelled registration = %v", got)
		}
	})
}

// A failed ON_ERROR leaves the transaction cancelled until RESET, and the
// next instruction on the transaction waits for ON_ERROR.
func TestOnErrorRestoresTransaction(t *testing.T) {
	cf := os.Getenv("FDB_CLUSTER_FILE")
	if cf == "" {
		t.Skip("FDB_CLUSTER_FILE not set")
	}
	var err error
	if db, err = fdb.Open(cf); err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	got := run(t, newStackMachine([]byte("stacktester-on-error-test"), 740),
		tuple.Tuple{"NEW_TRANSACTION"},
		tuple.Tuple{"PUSH", int64(1020)},
		tuple.Tuple{"ON_ERROR"},
		tuple.Tuple{"GET_READ_VERSION"},
		tuple.Tuple{"PUSH", int64(2000)},
		tuple.Tuple{"ON_ERROR"},
		tuple.Tuple{"WAIT_FUTURE"},
		tuple.Tuple{"PUSH", []byte("stacktester-on-error-test")},
		tuple.Tuple{"GET"},
		tuple.Tuple{"WAIT_FUTURE"},
		tuple.Tuple{"RESET"},
		tuple.Tuple{"GET_READ_VERSION"},
	)
	want := [][]byte{resultNotPresent, gotReadVersion, packErrorCode(2000), packErrorCode(1025), gotReadVersion}
	if len(got) != len(want) {
		t.Fatalf("stack = %q, want %q", got, want)
	}
	for i := range want {
		if !bytes.Equal(got[i].([]byte), want[i]) {
			t.Errorf("stack[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}
