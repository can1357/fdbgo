package client

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"testing"
	"time"

	"github.com/can1357/fdbgo/internal/cluster"
	"github.com/can1357/fdbgo/internal/fdberr"
	"github.com/can1357/fdbgo/internal/kv"
	"github.com/can1357/fdbgo/internal/native"
	"github.com/can1357/fdbgo/internal/options"
	"github.com/can1357/fdbgo/internal/ryw"
	"github.com/can1357/fdbgo/internal/transport"
	"github.com/can1357/fdbgo/internal/wire"
)

// localTxn has no database connection: tests must use only local operations.
func localTxn(t *testing.T) *Txn {
	t.Helper()
	db := &Db{opts: dbOptions{snapshotRYW: 1}}
	tr, err := db.CreateTransaction()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tr.Close)
	return tr
}

func TestGetManyLocalAndSpecialKeyRouting(t *testing.T) {
	tr := localTxn(t)
	tr.Set([]byte("local"), []byte("before"))
	values, wait, err := tr.GetMany([][]byte{[]byte("local"), []byte("\xff\xff"), []byte("local")}, false)
	if err != nil || wait != nil || len(values) != 3 || string(values[0]) != "before" || values[1] != nil || string(values[2]) != "before" {
		t.Fatalf("mixed local batch = %q, pending=%v, %v", values, wait != nil, err)
	}
	for _, test := range []struct {
		keys [][]byte
		want error
	}{
		{[][]byte{[]byte("local"), []byte("\xff/system"), []byte("\xff\xff/nope")}, fdberr.KeyOutsideLegalRange},
		{[][]byte{[]byte("\xff\xff/nope"), []byte("\xff/system"), []byte("local")}, fdberr.SpecialKeysNoModuleFound},
	} {
		values, wait, err := tr.GetMany(test.keys, false)
		if values != nil || wait != nil || !errors.Is(err, test.want) {
			t.Fatalf("failed mixed batch = %q, pending=%v, %v; want no values and %v", values, wait != nil, err, test.want)
		}
	}
}

func TestGetManyDisabledRYWAndSpecialWaitsSettleOnCancellation(t *testing.T) {
	db, err := OpenConnectionString("batch:test@127.0.0.1:1")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tr, err := db.CreateTransaction()
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()
	if err := tr.SetOption(options.TransactionReadYourWritesDisable, nil); err != nil {
		t.Fatal(err)
	}
	tr.SetReadVersion(1234)
	keys := [][]byte{[]byte("a"), []byte(readConflictRangeBegin + "a"), []byte("b")}
	_, wait, err := tr.GetMany(keys, false)
	if wait == nil || err != nil {
		t.Fatalf("capture = pending=%v, %v", wait != nil, err)
	}
	if pending, err := tr.ryw.PendingReads(); pending == nil || err != nil {
		t.Fatalf("reads were not all captured: pending=%v, %v", pending != nil, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	values, err := wait(ctx)
	if values != nil || !errors.Is(err, fdberr.OperationCancelled) {
		t.Fatalf("cancelled mixed batch = %q, %v; want no values and operation_cancelled", values, err)
	}
	_, commitWait, err := tr.ryw.Commit()
	if commitWait != nil || !errors.Is(err, fdberr.OperationCancelled) {
		t.Fatalf("commit after cancelled batch = pending=%v, %v", commitWait != nil, err)
	}
	// Both ordinary reads add their issue-time conflicts before the special
	// read starts waiting; its dependency must not prevent their completion.
	rows := tr.ryw.ReadConflictRanges([]byte(readConflictRangeBegin), kv.KeyAfter([]byte(readConflictRangeBegin+"z")))
	if len(rows) != 4 {
		t.Fatalf("batch lost native read conflicts: %q", rows)
	}
}

func TestSupersededTimeoutCannotAbortLiveAttempt(t *testing.T) {
	for _, replacement := range []time.Duration{0, 2 * time.Hour} {
		tr := localTxn(t)
		var param [8]byte
		binary.LittleEndian.PutUint64(param[:], uint64(time.Hour/time.Millisecond))
		if err := tr.SetOption(options.TransactionTimeout, param[:]); err != nil {
			t.Fatal(err)
		}
		old, end := tr.timer, tr.ryw.ResetSignal()
		binary.LittleEndian.PutUint64(param[:], uint64(replacement/time.Millisecond))
		if err := tr.SetOption(options.TransactionTimeout, param[:]); err != nil {
			t.Fatal(err)
		}
		// Model a callback dispatched before Stop but scheduled after replacement.
		old.expire(end)
		tr.Set([]byte("key"), []byte("live"))
		value, wait, err := tr.Get([]byte("key"), false)
		if err != nil || wait != nil || string(value) != "live" {
			t.Fatalf("replacement %v: read = %q, pending=%v, %v", replacement, value, wait != nil, err)
		}
	}
}

func TestSystemKeyOptionsAllowRawAccessWrites(t *testing.T) {
	for _, option := range []int{options.TransactionReadSystemKeys, options.TransactionAccessSystemKeys} {
		for _, size := range []int{10008, 10009} {
			tr := localTxn(t)
			if err := tr.SetOption(option, nil); err != nil {
				t.Fatal(err)
			}
			key := bytes.Repeat([]byte("k"), size)
			tr.Set(key, []byte("value"))
			value, wait, err := tr.Get(key, false)
			if size == 10009 {
				if !errors.Is(err, fdberr.KeyTooLarge) {
					t.Fatalf("option %d oversized write: %v", option, err)
				}
			} else if err != nil || wait != nil || string(value) != "value" {
				t.Fatalf("option %d raw write: %q, pending=%v, %v", option, value, wait != nil, err)
			}
		}
	}
}

func TestRetryResetBreaksVersionstampBeforeBackoff(t *testing.T) {
	tr := localTxn(t)
	_, stamp, err := tr.GetVersionstamp()
	if err != nil {
		t.Fatal(err)
	}
	_, retry, err := tr.OnError(fdberr.NotCommitted)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := stamp(ctx); !errors.Is(err, fdberr.BrokenPromise) {
		t.Fatalf("versionstamp before backoff = %v; want broken_promise", err)
	}
	if retry != nil {
		if _, err := retry(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := stamp(t.Context()); !errors.Is(err, fdberr.BrokenPromise) {
		t.Fatalf("versionstamp after backoff = %v; want broken_promise", err)
	}
}

func TestRetryCanUseCachedReadVersion(t *testing.T) {
	db, err := OpenConnectionString("cache:test@127.0.0.1:1")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tr, err := db.CreateTransaction()
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()
	for range 2 {
		if err := tr.SetOption(options.TransactionUseGRVCache, nil); err != nil {
			t.Fatal(err)
		}
		_, retry, err := tr.OnError(fdberr.NotCommitted)
		if err != nil {
			t.Fatal(err)
		}
		if retry != nil {
			if _, err := retry(t.Context()); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := tr.SetOption(options.TransactionUseGRVCache, nil); err != nil {
		t.Fatal(err)
	}
	// Keep the seeded cache fresh independently of test scheduling. No proxy
	// is available, so a request that bypasses the cache cannot succeed.
	db.ctx.UpdateCachedReadVersion(transport.Now()+60, 1234)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	version, wait, err := tr.GetReadVersion()
	if err == nil && wait != nil {
		version, err = wait(ctx)
	}
	if err != nil || version != 1234 {
		t.Fatalf("cached read version after retries = %d, %v", version, err)
	}
}

func TestElapsedTimeoutFailsSynchronously(t *testing.T) {
	tr := localTxn(t)
	tr.Set([]byte("key"), []byte("local"))
	tr.created = time.Now().Add(-time.Hour)
	var timeout [8]byte
	binary.LittleEndian.PutUint64(timeout[:], 1)
	if err := tr.SetOption(options.TransactionTimeout, timeout[:]); err != nil {
		t.Fatal(err)
	}
	value, wait, err := tr.Get([]byte("key"), false)
	if value != nil || wait != nil || !errors.Is(err, fdberr.TransactionTimedOut) {
		t.Fatalf("get after elapsed timeout = %q, pending=%v, %v; want transaction_timed_out immediately", value, wait != nil, err)
	}
	if tr.timer != nil {
		t.Fatal("elapsed deadline must not rely on a scheduled timer")
	}
}

// localProtocolTxn freezes discovery before publishing a negotiated protocol.
// Only local operations and commit-plan encoding are used.
func localProtocolTxn(t *testing.T, protocol wire.ProtocolVersion) *Txn {
	t.Helper()
	ctx := native.NewDbContext(cluster.ConnectionRecord{Conn: cluster.ConnectionString{
		Coordinators: []cluster.Coordinator{{}},
	}}, false)
	ctx.Close()
	ctx.ClusterNow().Protocol = protocol
	db := &Db{ctx: ctx, opts: dbOptions{snapshotRYW: 1}}
	tr, err := db.CreateTransaction()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tr.Close)
	return tr
}

func TestAtomicMinAndFollowConnectedClientSemantics(t *testing.T) {
	for _, version := range []struct {
		name     string
		protocol wire.ProtocolVersion
		legacy   bool
	}{
		{"unknown", 0, false},
		{"5.0", wire.Protocol50, true},
		{"5.0_flags", wire.Protocol50 | 0x8000000000000000, true},
		{"5.1", wire.Protocol51, false},
		{"5.1_flags", wire.Protocol51 | 0x8000000000000000, false},
		{"5.2", wire.Protocol52, false},
		{"7.2", wire.Protocol72, false},
		{"8.0", wire.Protocol80, false},
	} {
		t.Run(version.name, func(t *testing.T) {
			for _, op := range []struct {
				name string
				code uint8
			}{
				{"min", ryw.MutationMin},
				{"and", ryw.MutationAnd},
			} {
				t.Run(op.name, func(t *testing.T) {
					for _, empty := range []bool{false, true} {
						tr := localProtocolTxn(t, version.protocol)
						key := []byte("key")
						if empty {
							tr.Set(key, []byte{})
						} else {
							tr.Clear(key)
						}
						tr.AtomicOp(key, []byte{0x3f, 0x0f}, int(op.code))
						want := []byte{0x3f, 0x0f}
						if empty || version.legacy {
							want = []byte{0, 0}
						}
						for _, snapshot := range []bool{false, true} {
							value, wait, err := tr.Get(key, snapshot)
							if err != nil || wait != nil || value == nil || !bytes.Equal(value, want) {
								t.Fatalf("empty=%v snapshot=%v: value=%x pending=%v error=%v; want %x",
									empty, snapshot, value, wait != nil, err, want)
							}
						}
						plan, wait, err := tr.ryw.Commit()
						if err != nil || wait != nil || len(plan.Mutations) != 1 ||
							plan.Mutations[0].Kind != ryw.MutationSetValue || !bytes.Equal(plan.Mutations[0].Param2, want) ||
							len(plan.ReadConflicts) != 0 {
							t.Fatalf("empty=%v: plan=%+v pending=%v error=%v", empty, plan, wait != nil, err)
						}
					}
				})
			}
		})
	}
}

func TestAtomicMinAndWithoutDiscoveryContextRemainModern(t *testing.T) {
	for _, op := range []uint8{ryw.MutationMin, ryw.MutationAnd} {
		tr := localTxn(t)
		tr.Clear([]byte("key"))
		tr.AtomicOp([]byte("key"), []byte{7}, int(op))
		value, wait, err := tr.Get([]byte("key"), false)
		if err != nil || wait != nil || !bytes.Equal(value, []byte{7}) {
			t.Fatalf("op %d: value=%x pending=%v error=%v", op, value, wait != nil, err)
		}
	}
}

func TestAtomicConnectedCommitPreservesWireOperationsAndFeatureGates(t *testing.T) {
	for _, protocol := range []wire.ProtocolVersion{wire.Protocol50, wire.Protocol51, wire.Protocol60, wire.Protocol61} {
		for _, disabled := range []bool{false, true} {
			for _, op := range []uint8{ryw.MutationMin, ryw.MutationAnd, ryw.MutationByteMin, ryw.MutationCompareAndClear} {
				tr := localProtocolTxn(t, protocol)
				if disabled {
					if err := tr.SetOption(options.TransactionReadYourWritesDisable, nil); err != nil {
						t.Fatal(err)
					}
				}
				tr.AtomicOp([]byte("key"), []byte{7}, int(op))
				plan, wait, err := tr.ryw.Commit()
				if err != nil || wait != nil || len(plan.Mutations) != 1 {
					t.Fatalf("protocol=%x disabled=%v op=%d: plan=%+v pending=%v error=%v",
						protocol, disabled, op, plan, wait != nil, err)
				}
				wantOp := op
				if !protocol.Compatible(wire.Protocol50) {
					switch op {
					case ryw.MutationMin:
						wantOp = ryw.MutationMinV2
					case ryw.MutationAnd:
						wantOp = ryw.MutationAndV2
					}
				}
				if plan.Mutations[0].Kind != wantOp {
					t.Fatalf("protocol=%x disabled=%v op=%d: wire op=%d, want %d",
						protocol, disabled, op, plan.Mutations[0].Kind, wantOp)
				}
				var builder wire.Builder
				err = wire.EncodeLegacy(&builder, &wire.Commit{Mutations: plan.Mutations}, wire.UID{}, protocol)
				unsupported := op == ryw.MutationByteMin && protocol.Compatible(wire.Protocol50) ||
					op == ryw.MutationCompareAndClear && protocol.Normalized() < wire.Protocol61.Normalized()
				if unsupported && !errors.Is(err, fdberr.UnsupportedOperation) || !unsupported && err != nil {
					t.Fatalf("protocol=%x disabled=%v op=%d: encode error=%v, unsupported=%v",
						protocol, disabled, op, err, unsupported)
				}
			}
		}
	}
}

func TestAtomicProtocolNormalizationPreservesErrorPriority(t *testing.T) {
	for _, protocol := range []wire.ProtocolVersion{wire.Protocol50, wire.Protocol51} {
		for _, op := range []int{int(ryw.MutationMin), int(ryw.MutationAnd), -1, 262, 269} {
			tr := localProtocolTxn(t, protocol)
			tr.AtomicOp([]byte("\xff/illegal"), []byte{1}, op)
			if err := tr.ryw.DeferredError(); !errors.Is(err, fdberr.KeyOutsideLegalRange) {
				t.Fatalf("protocol=%x op=%d: illegal key error=%v", protocol, op, err)
			}
			tr.Reset()
			tr.ryw.SetDeferredError(fdberr.ValueTooLarge)
			tr.AtomicOp([]byte("\xff/illegal"), []byte{1}, op)
			if err := tr.ryw.DeferredError(); !errors.Is(err, fdberr.ValueTooLarge) {
				t.Fatalf("protocol=%x op=%d: prior deferred error=%v", protocol, op, err)
			}
			tr.Reset()
			tr.Set([]byte("key"), []byte("value"))
			if _, wait, err := tr.ryw.Commit(); err != nil || wait != nil {
				t.Fatalf("commit: pending=%v error=%v", wait != nil, err)
			}
			tr.AtomicOp([]byte("\xff/illegal"), []byte{1}, op)
			if err := tr.ryw.DeferredError(); !errors.Is(err, fdberr.UsedDuringCommit) {
				t.Fatalf("protocol=%x op=%d: during commit error=%v", protocol, op, err)
			}
			if op < 0 || op >= int(ryw.MutationMaxAtomicOp) {
				tr.Reset()
				tr.AtomicOp([]byte("key"), []byte{1}, op)
				if err := tr.ryw.DeferredError(); !errors.Is(err, fdberr.InvalidMutationType) {
					t.Fatalf("protocol=%x op=%d: invalid opcode error=%v", protocol, op, err)
				}
			}
		}
	}
}

func TestAtomicVersionstampBoundsFollowResetAttempt(t *testing.T) {
	tr := localTxn(t)
	template := binary.LittleEndian.AppendUint32(append([]byte("vs_"), make([]byte, 10)...), 3)
	tr.SetReadVersion(41)
	tr.AtomicOp(template, []byte("old"), int(ryw.MutationSetVersionstampedKey))
	plan, wait, err := tr.ryw.Commit()
	if err != nil || wait != nil || len(plan.Mutations) != 1 {
		t.Fatalf("old plan: %+v, pending=%v, %v", plan, wait != nil, err)
	}
	if got := binary.BigEndian.Uint64(plan.Mutations[0].Param1[3:11]); got != 42 {
		t.Fatalf("old placeholder = %d; want 42", got)
	}

	tr.Reset()
	tr.AtomicOp(template, []byte("new"), int(ryw.MutationSetVersionstampedKey))
	plan, wait, err = tr.ryw.Commit()
	if err != nil || wait != nil || len(plan.Mutations) != 1 {
		t.Fatalf("new plan: %+v, pending=%v, %v", plan, wait != nil, err)
	}
	if got := binary.BigEndian.Uint64(plan.Mutations[0].Param1[3:11]); got != 0 {
		t.Fatalf("new placeholder = %d; want 0 without a read version", got)
	}
}

func TestOnErrorCancellationAllowsResetFromAttemptHook(t *testing.T) {
	tr := localTxn(t)
	old := tr.ryw.ResetSignal()
	reset := make(chan struct{})
	// Cancellation hooks can reacquire the transaction lock. They must run
	// outside cancellation's critical section and reset only after it ends.
	old.OnEnd(func(error) {
		tr.Reset()
		close(reset)
	})
	_, wait, err := tr.OnError(fdberr.ClientInvalidOperation)
	if !errors.Is(err, fdberr.ClientInvalidOperation) || wait != nil {
		t.Fatalf("on error: pending=%v, %v", wait != nil, err)
	}
	<-reset
	if !errors.Is(old.Err(), fdberr.TransactionCancelled) {
		t.Fatalf("old attempt = %v; want transaction_cancelled", old.Err())
	}
	tr.Set([]byte("key"), []byte("new attempt"))
	value, read, err := tr.Get([]byte("key"), false)
	if err != nil || read != nil || string(value) != "new attempt" {
		t.Fatalf("new attempt = %q, pending=%v, %v", value, read != nil, err)
	}
}

func TestOldAttemptTimeoutDoesNotAbortReset(t *testing.T) {
	tr := localTxn(t)
	var timeout [8]byte
	binary.LittleEndian.PutUint64(timeout[:], uint64(time.Hour/time.Millisecond))
	if err := tr.SetOption(options.TransactionTimeout, timeout[:]); err != nil {
		t.Fatal(err)
	}
	old := tr.ryw.ResetSignal()
	tr.Reset()
	// A callback that was already dispatched when its timer was stopped may
	// still execute. It must retain the old attempt, not the reusable Ryw.
	old.Abort(fdberr.TransactionTimedOut)
	tr.Set([]byte("key"), []byte("new attempt"))
	value, wait, err := tr.Get([]byte("key"), false)
	if err != nil || wait != nil || string(value) != "new attempt" {
		t.Fatalf("get after old timeout = %q, pending=%v, %v", value, wait != nil, err)
	}
	if !errors.Is(old.Err(), fdberr.TransactionCancelled) {
		t.Fatalf("old attempt error = %v; want transaction_cancelled", old.Err())
	}
}
