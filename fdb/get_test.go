package fdb

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func TestSetGet(t *testing.T) {
	db := openTestDB(t)
	ctx := testCtx(t)
	hello := key(testPrefix(t, db), "hello")

	tr := mustTr(t, db)
	tr.Set(hello, []byte("world"))
	mustCommit(t, ctx, tr)

	tr = mustTr(t, db)
	v, err := tr.Get(ctx, hello)
	if err != nil || string(v) != "world" {
		t.Fatalf("get = %q, %v", v, err)
	}
	tr.Clear(hello)
	mustCommit(t, ctx, tr)

	tr = mustTr(t, db)
	if v, err := tr.Get(ctx, hello); err != nil || v != nil {
		t.Fatalf("get after clear = %q, %v", v, err)
	}
}

func TestGetMany(t *testing.T) {
	db := openTestDB(t)
	ctx := testCtx(t)
	p := testPrefix(t, db)
	tr := mustTr(t, db)
	keys := [][]byte{key(p, "hello"), key(p, "missing"), key(p, "world"), key(p, "hello"), key(p, "empty")}
	tr.Set(keys[0], []byte("first"))
	tr.Set(keys[2], []byte("second"))
	tr.Set(keys[4], []byte{})
	for _, persisted := range []bool{false, true} {
		if persisted {
			mustCommit(t, ctx, tr)
			tr = mustTr(t, db)
		}
		for _, reader := range []ReadTransaction{tr, tr.Snapshot()} {
			values, err := reader.GetMany(ctx, keys)
			if err != nil || len(values) != len(keys) {
				t.Fatalf("GetMany = %q, %v", values, err)
			}
			if string(values[0]) != "first" || values[1] != nil || string(values[2]) != "second" || string(values[3]) != "first" || values[4] == nil || len(values[4]) != 0 {
				t.Fatalf("GetMany lost order, missing/empty value, or duplicate key: %q", values)
			}
			empty, err := reader.GetMany(ctx, nil)
			if err != nil || len(empty) != 0 {
				t.Fatalf("empty GetMany = %q, %v", empty, err)
			}
		}
	}
}

func TestGetManyReturnsNoPartialValuesAndEarliestInputError(t *testing.T) {
	db := openParityMemoryDB(t)
	tr := mustTr(t, db)
	defer tr.Close()
	tr.Set([]byte("local"), []byte("value"))
	for _, reader := range []ReadTransaction{tr, tr.Snapshot()} {
		for _, test := range []struct {
			keys [][]byte
			want error
		}{
			{[][]byte{[]byte("local"), []byte("\xff/system"), []byte("\xff\xff/nope")}, ErrKeyOutsideLegalRange},
			{[][]byte{[]byte("\xff\xff/nope"), []byte("\xff/system"), []byte("local")}, ErrSpecialKeysNoModuleFound},
		} {
			values, err := reader.GetMany(t.Context(), test.keys)
			if values != nil || !errors.Is(err, test.want) {
				t.Fatalf("GetMany = %q, %v; want no values and %v", values, err, test.want)
			}
		}
	}
}

func TestSetConflict(t *testing.T) {
	db := openTestDB(t)
	ctx := testCtx(t)
	k := key(testPrefix(t, db), "test_set_conflict")

	tr1, tr2 := mustTr(t, db), mustTr(t, db)
	// Read the key to add a read conflict range.
	if _, err := tr2.Get(ctx, k); err != nil {
		t.Fatal(err)
	}
	// Commit the first transaction to create a conflict.
	tr1.Set(k, []byte(randStr(10)))
	mustCommit(t, ctx, tr1)
	// The second transaction now conflicts.
	tr2.Set(k, []byte(randStr(10)))
	err := tr2.Commit(ctx)
	var fe Error
	if !errors.As(err, &fe) || !errors.Is(err, ErrNotCommitted) {
		t.Fatalf("commit = %v, want not_committed", err)
	}
	if _, err := tr2.Versionstamp(); !errors.Is(err, ErrTransactionInvalidVersion) {
		t.Fatalf("versionstamp after conflict = %v; want transaction_invalid_version", err)
	}
	const msg = "Transaction not committed due to conflict with another transaction"
	if err.Error() != msg || fmt.Sprint(err) != msg {
		t.Errorf("message = %q", err.Error())
	}
	if fe.Name() != "not_committed" || fmt.Sprintf("%#v", fe) != "not_committed(1020)" {
		t.Errorf("name = %q, %#v", fe.Name(), fe)
	}
	if !fe.Retryable() || !fe.RetryableNotCommitted() || fe.MaybeCommitted() {
		t.Errorf("predicates: retryable=%v notCommitted=%v maybe=%v", fe.Retryable(), fe.RetryableNotCommitted(), fe.MaybeCommitted())
	}
}

func TestSetConflictSnapshot(t *testing.T) {
	db := openTestDB(t)
	ctx := testCtx(t)
	k := key(testPrefix(t, db), "test_set_conflict_snapshot")

	tr1, tr2 := mustTr(t, db), mustTr(t, db)
	// A snapshot read adds no conflict range, so both transactions commit.
	if _, err := tr2.Snapshot().Get(ctx, k); err != nil {
		t.Fatal(err)
	}
	tr1.Set(k, []byte(randStr(10)))
	mustCommit(t, ctx, tr1)
	tr2.Set(k, []byte(randStr(10)))
	mustCommit(t, ctx, tr2)
}

// makeDirty writes key in its own transaction, so transactions that read it
// non-snapshot before fail to commit.
func makeDirty(ctx context.Context, db *Database, k []byte) error {
	tr, err := db.CreateTransaction()
	if err != nil {
		return err
	}
	tr.Set(k, nil)
	return tr.Commit(ctx)
}

// conflictingBody reads k, conflicts with it and writes it: its commit always
// fails with not_committed.
func conflictingBody(ctx context.Context, db *Database, tr *Transaction, k []byte) error {
	if _, err := tr.Get(ctx, k); err != nil {
		return err
	}
	if err := makeDirty(ctx, db, k); err != nil {
		return err
	}
	tr.Set(k, []byte(randStr(10)))
	return nil
}

func TestTransact(t *testing.T) {
	const retryCount = 5
	db := openTestDB(t)
	ctx := testCtx(t)
	k := key(testPrefix(t, db), "test_transact")
	var tries atomic.Int32
	err := db.Transact(ctx, func(tr *Transaction) error {
		tries.Add(1)
		if err := tr.Options().SetRetryLimit(retryCount); err != nil {
			t.Fatal(err)
		}
		return conflictingBody(ctx, db, tr, k)
	})
	if !errors.Is(err, ErrNotCommitted) {
		t.Fatalf("transact = %v, want not_committed", err)
	}
	// The retry limit does not count the first try.
	if got := tries.Load(); got != retryCount+1 {
		t.Errorf("tries = %d, want %d", got, retryCount+1)
	}
}

// TestTransactDatabaseRetryLimit checks that the database's transaction_retry_limit
// default ends Transact retries with not_committed after limit+1 attempts.
func TestTransactDatabaseRetryLimit(t *testing.T) {
	db := openTestDB(t)
	ctx := testCtx(t)
	k := key(testPrefix(t, db), "test_transact_limit")
	if err := db.Options().SetTransactionRetryLimit(4); err != nil {
		t.Fatal(err)
	}
	var tries atomic.Int32
	err := db.Transact(ctx, func(tr *Transaction) error {
		tries.Add(1)
		return conflictingBody(ctx, db, tr, k)
	})
	if !errors.Is(err, ErrNotCommitted) {
		t.Fatalf("transact = %v, want not_committed", err)
	}
	if got := tries.Load(); got != 5 {
		t.Errorf("tries = %d, want 5", got)
	}
}

// TestTransactContextDeadline checks that the context deadline ends Transact retries.
func TestTransactContextDeadline(t *testing.T) {
	db := openTestDB(t)
	ctx := testCtx(t)
	k := key(testPrefix(t, db), "test_transact_timeout")
	tctx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer cancel()
	err := db.Transact(tctx, func(tr *Transaction) error {
		return conflictingBody(ctx, db, tr, k)
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("transact = %v, want deadline exceeded", err)
	}
}

// TestTransactionTimeoutOption checks the transaction timeout option fails
// the attempt with transaction_timed_out.
func TestTransactionTimeoutOption(t *testing.T) {
	db := openTestDB(t)
	ctx := testCtx(t)
	k := key(testPrefix(t, db), "timeout")
	err := db.Transact(ctx, func(tr *Transaction) error {
		if err := tr.Options().SetTimeout(250); err != nil {
			return err
		}
		return conflictingBody(ctx, db, tr, k)
	})
	if !errors.Is(err, ErrTransactionTimedOut) {
		t.Fatalf("transact = %v, want transaction_timed_out", err)
	}
}

func TestVersionstamp(t *testing.T) {
	db := openTestDB(t)
	ctx := testCtx(t)
	k := key(testPrefix(t, db), "test_versionstamp")
	tr := mustTr(t, db)
	tr.Set(k, []byte(randStr(10)))
	if _, err := tr.Versionstamp(); !errors.Is(err, ErrTransactionInvalidVersion) {
		t.Fatalf("versionstamp before commit = %v; want transaction_invalid_version", err)
	}
	mustCommit(t, ctx, tr)
	stamp, err := tr.Versionstamp()
	if err != nil || len(stamp) != 10 {
		t.Fatalf("versionstamp = %x, %v", stamp, err)
	}
	v, err := tr.CommittedVersion()
	if err != nil || v <= 0 {
		t.Fatalf("committed version = %d, %v", v, err)
	}
	var want [8]byte
	for i := range want {
		want[i] = byte(v >> (56 - 8*i))
	}
	if string(stamp[:8]) != string(want[:]) {
		t.Errorf("versionstamp %x does not start with the commit version %d", stamp, v)
	}
	again, err := tr.Versionstamp()
	if err != nil || string(again) != string(stamp) {
		t.Fatalf("repeated versionstamp = %x, %v; want %x", again, err, stamp)
	}
}

func TestVersionstampWithoutSuccessfulCommit(t *testing.T) {
	db := openTestDB(t)
	ctx := testCtx(t)
	tr := mustTr(t, db)
	defer tr.Close()
	if _, err := tr.Versionstamp(); !errors.Is(err, ErrTransactionInvalidVersion) {
		t.Fatalf("fresh versionstamp = %v; want transaction_invalid_version", err)
	}
	mustCommit(t, ctx, tr)
	if _, err := tr.Versionstamp(); !errors.Is(err, ErrNoCommitVersion) {
		t.Fatalf("read-only versionstamp = %v; want no_commit_version", err)
	}
	tr.Reset()
	tr.Cancel()
	if err := tr.Commit(ctx); !errors.Is(err, ErrTransactionCancelled) {
		t.Fatalf("cancelled commit = %v", err)
	}
	if _, err := tr.Versionstamp(); !errors.Is(err, ErrTransactionInvalidVersion) {
		t.Fatalf("failed commit versionstamp = %v; want transaction_invalid_version", err)
	}
}

func TestReadVersion(t *testing.T) {
	db := openTestDB(t)
	ctx := testCtx(t)
	v, err := mustTr(t, db).ReadVersion(ctx)
	if err != nil || v <= 0 {
		t.Fatalf("read version = %d, %v", v, err)
	}
}

func TestSetReadVersion(t *testing.T) {
	db := openTestDB(t)
	ctx := testCtx(t)
	k := key(testPrefix(t, db), "test_set_read_version")
	tr := mustTr(t, db)
	tr.SetReadVersion(0)
	if _, err := tr.Get(ctx, k); !errors.Is(err, ErrVersionInvalid) {
		t.Fatalf("get = %v, want version_invalid", err)
	}
}

func TestAddressesForKey(t *testing.T) {
	db := openTestDB(t)
	ctx := testCtx(t)
	k := key(testPrefix(t, db), "test_get_addresses_for_key")
	tr := mustTr(t, db)
	tr.Clear(k)
	mustCommit(t, ctx, tr)

	addrs, err := mustTr(t, db).AddressesForKey(ctx, k)
	if err != nil {
		t.Fatal(err)
	}
	// The dev cluster has a single storage server.
	if len(addrs) != 1 || addrs[0] == "" {
		t.Fatalf("addresses = %q", addrs)
	}
	t.Log(addrs[0])
}

// TestSetRawOption checks options set by raw code with an 8-byte
// little-endian parameter.
func TestSetRawOption(t *testing.T) {
	const retryCount = 5
	db := openTestDB(t)
	ctx := testCtx(t)
	k := key(testPrefix(t, db), "test_set_raw_option")
	var tries atomic.Int32
	err := db.Transact(ctx, func(tr *Transaction) error {
		tries.Add(1)
		if err := tr.setOption(501, int64Param(retryCount)); err != nil {
			t.Fatal(err)
		}
		return conflictingBody(ctx, db, tr, k)
	})
	if !errors.Is(err, ErrNotCommitted) {
		t.Fatalf("transact = %v, want not_committed", err)
	}
	if got := tries.Load(); got != retryCount+1 {
		t.Errorf("tries = %d, want %d", got, retryCount+1)
	}
}

func TestFailsToSetUnknownRawOption(t *testing.T) {
	db := openTestDB(t)
	if err := mustTr(t, db).setOption(0, nil); !errors.Is(err, ErrInvalidOption) {
		t.Fatalf("setOption(0) = %v, want invalid_option", err)
	}
	if err := mustTr(t, db).setOption(501, []byte{1}); !errors.Is(err, ErrInvalidOptionValue) {
		t.Fatalf("setOption(501, 1 byte) = %v, want invalid_option_value", err)
	}
}
