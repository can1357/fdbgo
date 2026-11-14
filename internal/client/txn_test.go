package client

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"testing"
	"time"

	"github.com/can1357/fdbgo/internal/fdberr"
	"github.com/can1357/fdbgo/internal/kv"
	"github.com/can1357/fdbgo/internal/options"
	"github.com/can1357/fdbgo/internal/ryw"
	"github.com/can1357/fdbgo/internal/transport"
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
