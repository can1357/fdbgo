package fdb

import (
	"context"
	"errors"
	"runtime"

	"github.com/can1357/fdbgo/internal/client"
	"github.com/can1357/fdbgo/internal/kv"
	"github.com/can1357/fdbgo/internal/options"
)

// ReadTransaction is the read side of a transaction, implemented by
// *Transaction and by Snapshot.
// Blocking methods return ctx.Err() when their context cancels the operation.
type ReadTransaction interface {
	// Get reads key; the value is nil if key is absent.
	Get(ctx context.Context, key []byte) ([]byte, error)
	// GetMany captures every read before waiting and batches snapshot I/O.
	// Values follow key order; a missing key has a nil value.
	GetMany(ctx context.Context, keys [][]byte) ([][]byte, error)
	// GetKey resolves sel to a key.
	GetKey(ctx context.Context, sel KeySelector) ([]byte, error)
	// GetRange reads the first batch of keys in r.
	GetRange(ctx context.Context, r Range, opts RangeOptions) (*RangeResult, error)
	// GetMappedRange reads the first batch of keys in r and, for each, the
	// secondary range mapper derives from it.
	GetMappedRange(ctx context.Context, r Range, mapper []byte, opts RangeOptions) (*MappedRangeResult, error)
	// ReadVersion returns the version reads observe.
	ReadVersion(ctx context.Context) (int64, error)
	// EstimatedRangeSizeBytes estimates the stored size of r.
	EstimatedRangeSizeBytes(ctx context.Context, r ExactRange) (int64, error)
	// RangeSplitPoints returns keys splitting r into chunks of about
	// chunkSize bytes, including r's bounds.
	RangeSplitPoints(ctx context.Context, r ExactRange, chunkSize int64) ([][]byte, error)
	// AddressesForKey returns the addresses of the storage servers holding key.
	AddressesForKey(ctx context.Context, key []byte) ([]string, error)
	// Snapshot returns the snapshot view of the transaction.
	Snapshot() Snapshot
	// Options returns the transaction's option setters.
	Options() TransactionOptions
	// Cancel cancels the transaction; pending and later operations fail
	// with ErrTransactionCancelled.
	Cancel()
	ReadTransactor
}

// Transaction is one FoundationDB transaction: reads see its own writes,
// writes are buffered until Commit. A Transaction is safe for concurrent
// use. Blocking reads may overlap in different goroutines; callers must
// synchronize reads with writes when their relative order matters.
type Transaction struct {
	t       *client.Txn
	db      *Database
	cleanup runtime.Cleanup
}

// Close releases the transaction and cancels its pending operations. Committed
// watches survive Close and remain owned by their Watch handles. Close is idempotent;
// the transaction must not be used afterwards. Unreachable transactions are
// also reclaimed, but callers should Close them when deterministic release is
// needed.
func (t *Transaction) Close() {
	t.cleanup.Stop()
	t.t.Close()
	runtime.KeepAlive(t)
}

// Snapshot performs snapshot reads: they add no read conflict ranges, and
// see the transaction's writes unless the snapshot_ryw_disable option is set.
type Snapshot struct {
	t *Transaction
}

var (
	_ ReadTransaction = (*Transaction)(nil)
	_ ReadTransaction = Snapshot{}
	_ Transactor      = (*Transaction)(nil)
	_ Transactor      = (*Database)(nil)
)

// Database returns the database the transaction belongs to.
func (t *Transaction) Database() *Database { defer runtime.KeepAlive(t); return t.db }

// Snapshot returns the snapshot view of t.
func (t *Transaction) Snapshot() Snapshot { defer runtime.KeepAlive(t); return Snapshot{t} }

// Options returns the setters of t's options.
func (t *Transaction) Options() TransactionOptions {
	defer runtime.KeepAlive(t)
	return TransactionOptions{t}
}

// Get reads key; the value is nil if key is absent. The read is added to
// the read conflict set. Cancellation returns ctx.Err() and finishes the
// tracked read; a read failure also prevents this attempt from committing.
func (t *Transaction) Get(ctx context.Context, key []byte) ([]byte, error) {
	return t.get(ctx, key, false)
}

func (t *Transaction) get(ctx context.Context, key []byte, snapshot bool) ([]byte, error) {
	defer runtime.KeepAlive(t)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	v, wait, err := t.t.Get(key, snapshot)
	if wait != nil {
		v, err = wait(ctx)
	}
	return v, operationError(ctx, err)
}

// GetMany captures every read before waiting, preserving each read's issue-time
// view and batching snapshot I/O. Values follow key order; missing keys have
// nil values. It waits for all registered reads to finish, including on
// cancellation, and returns no values if any read fails.
func (t *Transaction) GetMany(ctx context.Context, keys [][]byte) ([][]byte, error) {
	return t.getMany(ctx, keys, false)
}

func (t *Transaction) getMany(ctx context.Context, keys [][]byte, snapshot bool) ([][]byte, error) {
	defer runtime.KeepAlive(t)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	values, wait, err := t.t.GetMany(keys, snapshot)
	if wait != nil {
		values, err = wait(ctx)
	}
	return values, operationError(ctx, err)
}

// GetKey resolves sel to a key.
func (t *Transaction) GetKey(ctx context.Context, sel KeySelector) ([]byte, error) {
	return t.getKey(ctx, sel, false)
}

func (t *Transaction) getKey(ctx context.Context, sel KeySelector, snapshot bool) ([]byte, error) {
	defer runtime.KeepAlive(t)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	v, wait, err := t.t.GetKey(sel, snapshot)
	if wait != nil {
		v, err = wait(ctx)
	}
	return v, operationError(ctx, err)
}

// GetRange reads the first batch of keys in r. Later batches are requested
// only when Slice or All consumes them.
func (t *Transaction) GetRange(ctx context.Context, r Range, opts RangeOptions) (*RangeResult, error) {
	return newRangeResult(ctx, t, r, opts, false)
}

// GetMappedRange reads the first batch of keys in r and the secondary range
// mapper derives from each (fdb_transaction_get_mapped_range).
func (t *Transaction) GetMappedRange(ctx context.Context, r Range, mapper []byte, opts RangeOptions) (*MappedRangeResult, error) {
	return newMappedRangeResult(ctx, t, r, mapper, opts, false)
}

// ReadVersion returns the version t reads at, fetching it if needed.
func (t *Transaction) ReadVersion(ctx context.Context) (int64, error) {
	defer runtime.KeepAlive(t)
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	v, wait, err := t.t.GetReadVersion()
	if wait != nil {
		v, err = wait(ctx)
	}
	return v, operationError(ctx, err)
}

// SetReadVersion makes t read at version instead of fetching one.
func (t *Transaction) SetReadVersion(version int64) {
	defer runtime.KeepAlive(t)
	t.t.SetReadVersion(version)
}

// EstimatedRangeSizeBytes estimates the stored size of r.
func (t *Transaction) EstimatedRangeSizeBytes(ctx context.Context, r ExactRange) (int64, error) {
	defer runtime.KeepAlive(t)
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	b, e := r.FDBRangeKeys()
	v, wait, err := t.t.GetEstimatedRangeSizeBytes(b, e)
	if wait != nil {
		v, err = wait(ctx)
	}
	return v, operationError(ctx, err)
}

// RangeSplitPoints returns keys splitting r into chunks of about chunkSize
// bytes, including r's bounds.
func (t *Transaction) RangeSplitPoints(ctx context.Context, r ExactRange, chunkSize int64) ([][]byte, error) {
	defer runtime.KeepAlive(t)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	b, e := r.FDBRangeKeys()
	v, wait, err := t.t.GetRangeSplitPoints(b, e, chunkSize)
	if wait != nil {
		v, err = wait(ctx)
	}
	return v, operationError(ctx, err)
}

// AddressesForKey returns the addresses of the storage servers holding key.
func (t *Transaction) AddressesForKey(ctx context.Context, key []byte) ([]string, error) {
	defer runtime.KeepAlive(t)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	v, wait, err := t.t.GetAddressesForKey(key)
	if wait != nil {
		v, err = wait(ctx)
	}
	return v, operationError(ctx, err)
}

// operationError exposes context cancellation rather than the engine's
// operation_cancelled code, while preserving a result that already succeeded.
func operationError(ctx context.Context, err error) error {
	if err != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

// Set writes value to key.
func (t *Transaction) Set(key, value []byte) { defer runtime.KeepAlive(t); t.t.Set(key, value) }

// Clear removes key.
func (t *Transaction) Clear(key []byte) { defer runtime.KeepAlive(t); t.t.Clear(key) }

// ClearRange removes every key in r.
func (t *Transaction) ClearRange(r ExactRange) {
	defer runtime.KeepAlive(t)
	b, e := r.FDBRangeKeys()
	t.t.ClearRange(b, e)
}

// Atomic applies the atomic operation op with param to key. The generated
// methods (Add, BitAnd, ...) are shorthands for it.
func (t *Transaction) Atomic(op MutationType, key, param []byte) {
	defer runtime.KeepAlive(t)
	t.t.AtomicOp(key, param, int(op))
}

// AddReadConflictRange adds r to the read conflict set, as if it was read.
func (t *Transaction) AddReadConflictRange(r ExactRange) error {
	defer runtime.KeepAlive(t)
	b, e := r.FDBRangeKeys()
	return t.t.AddConflictRange(b, e, options.ConflictRangeTypeRead)
}

// AddReadConflictKey adds key to the read conflict set.
func (t *Transaction) AddReadConflictKey(key []byte) error {
	defer runtime.KeepAlive(t)
	return t.t.AddConflictRange(key, kv.KeyAfter(key), options.ConflictRangeTypeRead)
}

// AddWriteConflictRange adds r to the write conflict set, as if it was
// written.
func (t *Transaction) AddWriteConflictRange(r ExactRange) error {
	defer runtime.KeepAlive(t)
	b, e := r.FDBRangeKeys()
	return t.t.AddConflictRange(b, e, options.ConflictRangeTypeWrite)
}

// AddWriteConflictKey adds key to the write conflict set.
func (t *Transaction) AddWriteConflictKey(key []byte) error {
	defer runtime.KeepAlive(t)
	return t.t.AddConflictRange(key, kv.KeyAfter(key), options.ConflictRangeTypeWrite)
}

// Commit commits t's writes. It returns nil once they are durable. Cancelling
// ctx aborts this attempt and returns ctx.Err(); if a commit was already sent,
// its outcome may be unknown.
func (t *Transaction) Commit(ctx context.Context) error {
	defer runtime.KeepAlive(t)
	if err := ctx.Err(); err != nil {
		return err
	}
	_, wait, err := t.t.Commit()
	if wait != nil {
		_, err = wait(ctx)
	}
	return operationError(ctx, err)
}

// OnError handles an error from an operation or Commit: for a retryable
// Error it waits out a backoff, resets t and returns nil so the caller can
// retry; otherwise it returns err. Errors that are not an Error are returned
// as is.
func (t *Transaction) OnError(ctx context.Context, err error) error {
	defer runtime.KeepAlive(t)
	var fe Error
	if !errors.As(err, &fe) {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	_, wait, err := t.t.OnError(fe)
	if wait != nil {
		_, err = wait(ctx)
	}
	return operationError(ctx, err)
}

// Watch registers a watch before returning. It completes when key's value
// changes, fails if t does not commit, and outlives a successful commit.
// Changes made by t itself can complete the watch before commit.
func (t *Transaction) Watch(key []byte) *Watch {
	defer runtime.KeepAlive(t)
	_, wait, err := t.t.Watch(key)
	return newWatch(t, wait, err)
}

// CommittedVersion returns the version t committed at, or -1 for a
// read-only transaction. It is valid after Commit returns nil.
func (t *Transaction) CommittedVersion() (int64, error) {
	defer runtime.KeepAlive(t)
	return t.t.GetCommittedVersion()
}

// Versionstamp returns the 10-byte versionstamp after Commit succeeds.
// It returns ErrTransactionInvalidVersion before a successful commit and
// ErrNoCommitVersion after a read-only commit. It does not change t's state.
func (t *Transaction) Versionstamp() ([]byte, error) {
	defer runtime.KeepAlive(t)
	return t.t.CommittedVersionstamp()
}

// ApproximateSize returns the approximate size of t's commit request.
func (t *Transaction) ApproximateSize(ctx context.Context) (int64, error) {
	defer runtime.KeepAlive(t)
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	v, wait, err := t.t.GetApproximateSize()
	if wait != nil {
		v, err = wait(ctx)
	}
	return v, operationError(ctx, err)
}

// Reset returns t to its initial state, keeping options set with
// persistent semantics.
func (t *Transaction) Reset() { defer runtime.KeepAlive(t); t.t.Reset() }

// Cancel cancels t: pending and later operations fail with
// ErrTransactionCancelled until Reset.
func (t *Transaction) Cancel() { defer runtime.KeepAlive(t); t.t.Cancel() }

// Transact runs fn(t) once, without committing, so layers taking a
// Transactor compose into t.
func (t *Transaction) Transact(ctx context.Context, fn func(*Transaction) error) error {
	defer runtime.KeepAlive(t)
	return fn(t)
}

// ReadTransact runs fn(t) once.
func (t *Transaction) ReadTransact(ctx context.Context, fn func(ReadTransaction) error) error {
	defer runtime.KeepAlive(t)
	return fn(t)
}

func (t *Transaction) setOption(code int, param []byte) error {
	defer runtime.KeepAlive(t)
	return t.t.SetOption(code, param)
}

// Get reads key without adding a read conflict.
func (s Snapshot) Get(ctx context.Context, key []byte) ([]byte, error) {
	return s.t.get(ctx, key, true)
}

// GetMany is Transaction.GetMany without adding read conflicts.
func (s Snapshot) GetMany(ctx context.Context, keys [][]byte) ([][]byte, error) {
	return s.t.getMany(ctx, keys, true)
}

// GetKey resolves sel without adding a read conflict.
func (s Snapshot) GetKey(ctx context.Context, sel KeySelector) ([]byte, error) {
	return s.t.getKey(ctx, sel, true)
}

// GetRange reads the first batch of r without adding a read conflict.
func (s Snapshot) GetRange(ctx context.Context, r Range, opts RangeOptions) (*RangeResult, error) {
	return newRangeResult(ctx, s.t, r, opts, true)
}

// GetMappedRange is Transaction.GetMappedRange without read conflicts.
func (s Snapshot) GetMappedRange(ctx context.Context, r Range, mapper []byte, opts RangeOptions) (*MappedRangeResult, error) {
	return newMappedRangeResult(ctx, s.t, r, mapper, opts, true)
}

// ReadVersion is Transaction.ReadVersion.
func (s Snapshot) ReadVersion(ctx context.Context) (int64, error) { return s.t.ReadVersion(ctx) }

// EstimatedRangeSizeBytes is Transaction.EstimatedRangeSizeBytes.
func (s Snapshot) EstimatedRangeSizeBytes(ctx context.Context, r ExactRange) (int64, error) {
	return s.t.EstimatedRangeSizeBytes(ctx, r)
}

// RangeSplitPoints is Transaction.RangeSplitPoints.
func (s Snapshot) RangeSplitPoints(ctx context.Context, r ExactRange, chunkSize int64) ([][]byte, error) {
	return s.t.RangeSplitPoints(ctx, r, chunkSize)
}

// AddressesForKey is Transaction.AddressesForKey.
func (s Snapshot) AddressesForKey(ctx context.Context, key []byte) ([]string, error) {
	return s.t.AddressesForKey(ctx, key)
}

// Snapshot returns s.
func (s Snapshot) Snapshot() Snapshot { return s }

// Options is Transaction.Options.
func (s Snapshot) Options() TransactionOptions { return s.t.Options() }

// Cancel is Transaction.Cancel.
func (s Snapshot) Cancel() { s.t.Cancel() }

// ReadTransact runs fn(s) once.
func (s Snapshot) ReadTransact(ctx context.Context, fn func(ReadTransaction) error) error {
	return fn(s)
}
