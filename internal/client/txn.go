package client

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"log/slog"
	mrand "math/rand/v2"
	"slices"
	"sync"
	"time"

	"github.com/can1357/fdbgo/internal/fdberr"
	"github.com/can1357/fdbgo/internal/kv"
	"github.com/can1357/fdbgo/internal/native"
	"github.com/can1357/fdbgo/internal/options"
	"github.com/can1357/fdbgo/internal/ryw"
	"github.com/can1357/fdbgo/internal/wire"
)

// transactionSizeLimit is the upper bound of size_limit.
const transactionSizeLimit = native.TransactionSizeLimit

// MappedKeyValue is one row of a mapped range read (FDBMappedKeyValue).
type MappedKeyValue struct {
	Key, Value []byte
	// Begin and End bound the secondary range the mapper produced for this row.
	Begin, End kv.KeySelector
	// Data holds the rows of the secondary range.
	Data []kv.KeyValue
}

// MappedRangeResult is a mapped range read result.
type MappedRangeResult struct {
	Data []MappedKeyValue
	More bool
}

// Txn is a transaction handle (FDBTransaction): read-your-writes, safe for
// concurrent use.
//
// The fields below mu are the transaction state that lives outside ryw.Ryw: the native transaction, retry/timeout bookkeeping and
// persistent options.
type Txn struct {
	db *Db
	// ryw is stable for the transaction's life; resets happen in place.
	ryw *ryw.Ryw

	mu     sync.Mutex
	native *native.NativeTxn
	// persistent are the options re-applied after every reset
	// (persistentOptions, seeded with database defaults).
	persistent []txOption
	// timeout is timeoutInSeconds, measured from created.
	timeout time.Duration
	timer   *transactionTimer
	// maxRetries (-1 = unlimited) and retries.
	maxRetries, retries int64
	created             time.Time
	// debugRetryLogging is options.debugRetryLogging, cleared by every reset.
	debugRetryLogging bool
	// debugName and debugLast are transactionDebugInfo: the transaction's name and
	// when a retry was last logged; hasDebugInfo reports they are set.
	debugName    string
	debugLast    time.Time
	hasDebugInfo bool
}

// transactionTimer serializes expiration with replacement without retaining Txn.
type transactionTimer struct {
	mu      sync.Mutex
	timer   *time.Timer
	stopped bool
}

func (t *transactionTimer) stop() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.stopped = true
	t.timer.Stop()
}

func (t *transactionTimer) expire(end *ryw.AttemptEnd) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.stopped {
		end.Abort(fdberr.TransactionTimedOut)
	}
}

func (t *Txn) stopTimeout() {
	if t.timer != nil {
		t.timer.stop()
		t.timer = nil
	}
}

func newTxn(db *Db, o *dbOptions) *Txn {
	t := &Txn{
		db:         db,
		ryw:        ryw.New(ryw.NewRywOptions(o.snapshotRYW)),
		native:     native.NewNativeTxn(db.ctx, native.DefaultNativeOptions()),
		persistent: o.defaults,
		maxRetries: -1,
		created:    time.Now(),
	}
	t.native.Shared.BindAttempt(t.ryw.ResetSignal())
	// A fresh Ryw and NativeTxn already hold the attempt defaults.
	if len(t.persistent) > 0 {
		t.mu.Lock()
		t.applyPersistent()
		t.mu.Unlock()
	}
	return t
}

// Close releases the transaction like dropping an FDBTransaction: the timeout
// timer stops and pending operations fail with transaction_cancelled.
func (t *Txn) Close() {
	t.mu.Lock()
	t.stopTimeout()
	// The attempt's reset signal fires before its native reads are stopped.
	t.ryw.Cancel()
	t.native.Close()
	t.mu.Unlock()
}

// resetTimeout arms the timeout relative to the transaction's creation
// (resetTimeout). Callers hold mu.
func (t *Txn) resetTimeout() {
	t.stopTimeout()
	if t.timeout > 0 {
		end := t.ryw.ResetSignal()
		remaining := time.Until(t.created.Add(t.timeout))
		if remaining <= 0 {
			end.Abort(fdberr.TransactionTimedOut)
			return
		}
		timer := &transactionTimer{}
		timer.timer = time.AfterFunc(remaining, func() { timer.expire(end) })
		t.timer = timer
	}
}

// resetRyw is resetRyow: a new attempt with persistent options re-applied, the
// timeout last. Callers hold mu.
func (t *Txn) resetRyw(snapshotRYW int32) {
	t.ryw.SetBaseOptions(ryw.NewRywOptions(snapshotRYW))
	t.ryw.Reset()
	t.native.Shared.BindAttempt(t.ryw.ResetSignal())
	t.native.Opts = native.DefaultNativeOptions()
	t.timeout = 0
	t.maxRetries = -1
	t.debugRetryLogging = false
	t.applyPersistent()
}

// applyPersistent is applyPersistentOptions, the timeout last since it re-arms
// the timer. Callers hold mu.
func (t *Txn) applyPersistent() {
	var timeout *txOption
	for i := range t.persistent {
		o := t.persistent[i]
		if o.code == options.TransactionTimeout {
			timeout = &o
			continue
		}
		// Persistent options were valid when first set.
		_ = t.applyOption(o)
	}
	if timeout != nil {
		_ = t.applyOption(*timeout)
	} else {
		t.resetTimeout()
	}
	t.native.OptionsChanged()
}

// applyOption applies o to the read-your-writes layer and then the native
// transaction. Callers hold mu.
func (t *Txn) applyOption(o txOption) error {
	if err := validateTransactionOption(&o); err != nil {
		return err
	}
	t.ryw.SetOption(o.code)
	if err := t.ryw.DeferredError(); err != nil {
		return err
	}
	no := &t.native.Opts
	switch o.code {
	case options.TransactionAccessSystemKeys, options.TransactionReadSystemKeys, options.TransactionRawAccess:
		no.RawAccess = true
	case options.TransactionTimeout:
		t.timeout = time.Duration(o.i) * time.Millisecond
		t.resetTimeout()
	case options.TransactionRetryLimit:
		t.maxRetries = o.i
	case options.TransactionMaxRetryDelay:
		no.MaxBackoff = float64(o.i) / 1000
	case options.TransactionDebugRetryLogging:
		t.debugRetryLogging = true
		if !t.hasDebugInfo {
			t.debugName, t.debugLast, t.hasDebugInfo = string(o.s), t.created, true
		}
	case options.TransactionCausalWriteRisky:
		no.CausalWriteRisky = true
	case options.TransactionCausalReadRisky:
		no.CausalReadRisky = true
	case options.TransactionIncludePortInAddress:
		no.IncludePort = true
	case options.TransactionReadServerSideCacheEnable:
		no.ReadOpts().CacheResult = true
	case options.TransactionReadServerSideCacheDisable:
		no.ReadOpts().CacheResult = false
	case options.TransactionReadPriorityNormal:
		no.ReadOpts().ReadType = 3
	case options.TransactionReadPriorityLow:
		no.ReadOpts().ReadType = 2
	case options.TransactionReadPriorityHigh:
		no.ReadOpts().ReadType = 4
	case options.TransactionPrioritySystemImmediate:
		no.Priority = native.PriorityImmediate
	case options.TransactionPriorityBatch:
		no.Priority = native.PriorityBatch
	case options.TransactionInitializeNewDatabase:
		if err := t.native.Shared.SetInitializeVersion(); err != nil {
			return err
		}
		no.CausalWriteRisky = true
	case options.TransactionBypassStorageQuota:
		no.BypassStorageQuota = true
	case options.TransactionTransactionLoggingEnable:
		if err := setDebugIdentifier(no, string(o.s)); err != nil {
			return err
		}
		no.LogTransaction = true
	case options.TransactionDebugTransactionIdentifier:
		if err := setDebugIdentifier(no, string(o.s)); err != nil {
			return err
		}
	case options.TransactionLogTransaction:
		if no.DebugID == "" {
			return fdberr.ClientInvalidOperation
		}
		no.LogTransaction = true
	case options.TransactionTransactionLoggingMaxFieldLength:
		no.MaxLogFieldLength = o.i
	case options.TransactionServerRequestTracing:
		no.ReadOpts().DebugID = wire.UID{First: mrand.Uint64(), Second: mrand.Uint64()}
	case options.TransactionSizeLimit:
		no.SizeLimit = o.i
	case options.TransactionAutomaticIdempotency:
		if no.IdempotencyID == nil {
			id := make([]byte, 16)
			_, _ = rand.Read(id)
			no.IdempotencyID = id
		}
		no.AutomaticIdempotency = true
	case options.TransactionLockAware:
		no.ReadOpts().LockAware = true
		no.LockAware = true
		no.ReadOnly = false
	case options.TransactionReadLockAware:
		no.ReadOpts().LockAware = true
		if !no.LockAware {
			no.LockAware = true
			no.ReadOnly = true
		}
	case options.TransactionUseProvisionalProxies:
		no.UseProvisionalProxies = true
	case options.TransactionReportConflictingKeys:
		no.ReportConflictingKeys = true
	case options.TransactionTag:
		if err := addTag(&no.Tags, o.s); err != nil {
			return err
		}
	case options.TransactionAutoThrottleTag:
		if err := addTag(&no.Tags, o.s); err != nil {
			return err
		}
		if err := addTag(&no.ReadTags, o.s); err != nil {
			return err
		}
	case options.TransactionSpanParent:
		if len(o.s) != 33 {
			return fdberr.InvalidOptionValue
		}
	case options.TransactionExpensiveClearCostEstimationEnable:
		no.ExpensiveClearCostEstimation = true
	case options.TransactionUseGRVCache:
		no.UseGRVCache = true
	case options.TransactionEnableReplicaConsistencyCheck:
		no.ReplicaCheck = true
	case options.TransactionConsistencyCheckRequiredReplicas:
		if o.i < -2 {
			return fdberr.InvalidOptionValue
		}
		no.RequiredReplicas = o.i
		// Durability options are accepted and ignored by FoundationDB itself; the
		// remaining ones are interpreted by the read-your-writes layer above.
	}
	t.native.OptionsChanged()
	return nil
}

// setDebugIdentifier sets debug_transaction_identifier: 1 to 100 bytes, and
// not changed once set.
func setDebugIdentifier(no *native.NativeOptions, id string) error {
	if id == "" || len(id) > 100 {
		return fdberr.InvalidOptionValue
	}
	if no.DebugID != "" && no.DebugID != id {
		return fdberr.ClientInvalidOperation
	}
	no.DebugID = id
	return nil
}

// SetOption is fdb_transaction_set_option by raw code. An unknown code or a
// malformed parameter fails at once; as in libfdb_c, other
// failures are deferred to the next operation, and options are ignored once an
// error is deferred. Persistent options survive resets.
func (t *Txn) SetOption(code int, param []byte) error {
	o, err := decodeTxOption(code, param)
	if err != nil {
		return err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.ryw.DeferredError() != nil {
		return nil
	}
	if err := t.applyOption(o); err != nil {
		t.ryw.SetDeferredError(err)
		return nil
	}
	if o.persistent() {
		t.persistent = slices.DeleteFunc(t.persistent, func(p txOption) bool { return p.code == o.code })
		t.persistent = append(t.persistent, o)
	}
	return nil
}

// attemptWait runs a blocking operation with cancellation tied to its attempt.
func attemptWait[T any](end *ryw.AttemptEnd, f func(ctx context.Context) (T, error)) func(context.Context) (T, error) {
	return func(ctx context.Context) (T, error) {
		ctx, cancel := context.WithCancel(ctx)
		hook := end.OnEnd(func(error) { cancel() })
		defer hook.Release()
		defer cancel()
		v, err := f(ctx)
		if err != nil && end.Err() != nil {
			var zero T
			return zero, end.Err()
		}
		return v, err
	}
}

// GetReadVersion is fdb_transaction_get_read_version.
func (t *Txn) GetReadVersion() (int64, func(ctx context.Context) (int64, error), error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.ryw.DeferredError(); err != nil {
		return 0, nil, err
	}
	if err := t.ryw.ResetError(); err != nil {
		return 0, nil, err
	}
	sh, end := t.native.Shared, t.ryw.ResetSignal()
	sh.StartReadVersion()
	return 0, attemptWait(end, func(ctx context.Context) (int64, error) {
		rv, err := sh.ReadVersion(ctx)
		return rv.Version, err
	}), nil
}

// SetReadVersion is fdb_transaction_set_read_version; errors are deferred to
// the next read or commit.
func (t *Txn) SetReadVersion(version int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.native.Shared.SetReadVersion(version); err != nil {
		t.ryw.SetDeferredError(err)
	}
}

// specialSrc is the special-key data source of one transaction attempt.
type specialSrc struct {
	t          *Txn
	status     []byte
	cluster    string
	readsReady bool
	fetch      func(context.Context) error
}

// errSpecialReadPending suspends a special-key read until its source is loaded.
// The same source is reused when selector resolution resumes.
var errSpecialReadPending = errors.New("special key source not ready")

func (s *specialSrc) readConflictRows(begin, end []byte) ([]kv.KeyValue, error) {
	if !s.readsReady {
		wait, err := s.t.ryw.PendingReads()
		if err != nil {
			return nil, err
		}
		if wait != nil {
			s.fetch = func(ctx context.Context) error {
				if err := wait(ctx); err != nil {
					return err
				}
				s.readsReady = true
				return nil
			}
			return nil, errSpecialReadPending
		}
		s.readsReady = true
	}
	return s.t.ryw.ReadConflictRanges(begin, end), nil
}

func (s *specialSrc) writeConflictRows(begin, end []byte) []kv.KeyValue {
	sh := s.t.native.Shared
	var stamp []byte
	if o := sh.OutcomeNow(); o.State == native.OutcomeCommitted && o.HasVersionstamp {
		stamp = o.Versionstamp[:]
	}
	rv, ok := sh.CachedReadVersion()
	return s.t.ryw.WriteConflictRanges(begin, end, stamp, rv, ok)
}

func (s *specialSrc) conflictingRanges() ([]kv.KeyRange, bool) {
	return s.t.native.Shared.Conflicting()
}

func (s *specialSrc) connectionString() string {
	return s.t.db.ctx.ConnectionString().String()
}

func (s *specialSrc) clusterFilePath() (string, bool) {
	return s.t.db.ctx.Record().Location(), true
}

func (s *specialSrc) clusterID() (string, error) {
	if s.cluster != "" {
		return s.cluster, nil
	}
	if id := s.t.db.ctx.ClusterNow().ClusterID; id.IsValid() {
		s.cluster = id.String()
		return s.cluster, nil
	}
	s.fetch = func(ctx context.Context) error {
		id, err := s.t.db.ctx.ClusterID(ctx)
		if err == nil {
			s.cluster = id.String()
		}
		return err
	}
	return "", errSpecialReadPending
}

func (s *specialSrc) statusJSON() ([]byte, error) {
	if s.status != nil {
		return s.status, nil
	}
	s.fetch = func(ctx context.Context) error {
		var err error
		s.status, err = native.StatusJSON(ctx, s.t.db.ctx)
		return err
	}
	return nil, errSpecialReadPending
}

// specialRead loads asynchronous modules only when selector resolution or
// result collection reaches them. Callers hold mu.
func specialRead[T any](t *Txn, f func(src specialSource, relaxed bool) (T, error)) (T, func(ctx context.Context) (T, error), error) {
	relaxed := t.ryw.Options().SpecialKeySpaceRelaxed
	if err := t.ryw.DeferredError(); err != nil {
		var zero T
		return zero, nil, err
	}
	src := &specialSrc{t: t}
	v, err := f(src, relaxed)
	if err != errSpecialReadPending {
		return v, nil, err
	}
	attempt := t.ryw.ResetSignal()
	var zero T
	return zero, attemptWait(attempt, func(ctx context.Context) (T, error) {
		for {
			if err := src.fetch(ctx); err != nil {
				return zero, err
			}
			t.mu.Lock()
			if err := attempt.Err(); err != nil {
				t.mu.Unlock()
				return zero, err
			}
			v, err := f(src, relaxed)
			t.mu.Unlock()
			if err != errSpecialReadPending {
				return v, err
			}
		}
	}), nil
}

// Get is fdb_transaction_get: the value is nil when the key is absent.
func (t *Txn) Get(key []byte, snapshot bool) ([]byte, func(ctx context.Context) ([]byte, error), error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.get(key, snapshot)
}

// get captures one point read while t.mu is held.
func (t *Txn) get(key []byte, snapshot bool) ([]byte, func(ctx context.Context) ([]byte, error), error) {
	if isSpecialKey(key) {
		key = bytes.Clone(key)
		return specialRead(t, func(src specialSource, relaxed bool) ([]byte, error) { return specialGet(src, relaxed, key) })
	}
	reader := t.native.Reader()
	if !t.ryw.Options().ReadYourWritesDisabled {
		return t.ryw.Get(reader, key, snapshot)
	}
	key = bytes.Clone(key)
	return ryw.NativeRead(t.ryw, func(o ryw.RywOptions, add func([]byte, []byte)) ([]byte, func(ctx context.Context) ([]byte, error), error) {
		ok, err := ryw.ValidateGet(&o, key)
		if err != nil || !ok {
			return nil, nil, err
		}
		if !snapshot {
			add(key, kv.KeyAfter(key))
		}
		reader.StartReadVersion()
		return nil, func(ctx context.Context) ([]byte, error) { return reader.Get(ctx, key) }, nil
	})
}

// GetMany captures all reads before waiting. Ordinary RYW reads share a native
// batch; special-key and RYW-disabled reads retain their scalar routing and
// issue-time capture. Every returned wait must run even after cancellation.
func (t *Txn) GetMany(keys [][]byte, snapshot bool) ([][]byte, func(context.Context) ([][]byte, error), error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	batch := !t.ryw.Options().ReadYourWritesDisabled
	if batch {
		for _, key := range keys {
			if isSpecialKey(key) {
				batch = false
				break
			}
		}
	}
	if batch {
		return t.ryw.GetMany(t.native.Reader(), keys, snapshot)
	}

	values := make([][]byte, len(keys))
	type pendingRead struct {
		index int
		wait  func(context.Context) ([]byte, error)
	}
	var pending []pendingRead
	var firstErr error
	firstIndex := len(keys)
	for i, key := range keys {
		v, wait, err := t.get(key, snapshot)
		values[i] = v
		if err != nil && i < firstIndex {
			firstErr, firstIndex = err, i
		}
		if wait != nil {
			if pending == nil {
				pending = make([]pendingRead, 0, len(keys)-i)
			}
			pending = append(pending, pendingRead{i, wait})
		}
	}
	if len(pending) == 0 {
		if firstErr != nil {
			return nil, nil, firstErr
		}
		return values, nil, nil
	}
	return nil, func(ctx context.Context) ([][]byte, error) {
		var wg sync.WaitGroup
		var mu sync.Mutex
		run := func(p pendingRead) {
			v, err := p.wait(ctx)
			values[p.index] = v
			if err != nil {
				mu.Lock()
				if p.index < firstIndex {
					firstErr, firstIndex = err, p.index
				}
				mu.Unlock()
			}
		}
		wg.Add(len(pending) - 1)
		for _, p := range pending[:len(pending)-1] {
			go func() {
				defer wg.Done()
				run(p)
			}()
		}
		run(pending[len(pending)-1])
		wg.Wait()
		if firstErr != nil {
			return nil, firstErr
		}
		return values, nil
	}, nil
}

// GetKey is fdb_transaction_get_key: resolves a key selector.
func (t *Txn) GetKey(sel kv.KeySelector, snapshot bool) ([]byte, func(ctx context.Context) ([]byte, error), error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	sel.Normalize()
	reader := t.native.Reader()
	if !t.ryw.Options().ReadYourWritesDisabled {
		return t.ryw.GetKey(reader, sel, snapshot)
	}
	sel.Key = bytes.Clone(sel.Key)
	r, end := t.ryw, t.ryw.ResetSignal()
	return ryw.NativeRead(r, func(o ryw.RywOptions, _ func([]byte, []byte)) ([]byte, func(ctx context.Context) ([]byte, error), error) {
		if err := ryw.ValidateGetKey(&o, &sel); err != nil {
			return nil, nil, err
		}
		reader.StartReadVersion()
		return nil, func(ctx context.Context) ([]byte, error) {
			resolved, err := reader.GetKey(ctx, sel)
			if err != nil {
				return nil, err
			}
			if !snapshot {
				if c := native.GetKeyConflict(sel, resolved); bytes.Compare(c.Begin, c.End) < 0 {
					if err := r.AddNativeReadConflictForAttempt(end, c.Begin, c.End); err != nil {
						return nil, err
					}
				}
			}
			return ryw.ClipReadKey(&o, resolved), nil
		}, nil
	})
}

// GetRange is fdb_transaction_get_range, including limit translation.
func (t *Txn) GetRange(begin, end kv.KeySelector, o RangeOpts, snapshot bool) (kv.RangeResult, func(ctx context.Context) (kv.RangeResult, error), error) {
	limits, err := o.limits()
	if err != nil {
		return kv.RangeResult{}, nil, err
	}
	reverse := o.Reverse
	t.mu.Lock()
	defer t.mu.Unlock()
	begin.Normalize()
	end.Normalize()
	if isSpecialKey(begin.Key) && string(end.Key) >= specialKeysBegin && string(end.Key) <= specialKeysEnd {
		begin.Key, end.Key = bytes.Clone(begin.Key), bytes.Clone(end.Key)
		return specialRead(t, func(src specialSource, relaxed bool) (kv.RangeResult, error) {
			return specialGetRange(src, relaxed, begin, end, limits, reverse)
		})
	}
	reader := t.native.Reader()
	if !t.ryw.Options().ReadYourWritesDisabled {
		return t.ryw.GetRange(reader, begin, end, limits, snapshot, reverse)
	}
	begin.Key, end.Key = bytes.Clone(begin.Key), bytes.Clone(end.Key)
	r, attempt := t.ryw, t.ryw.ResetSignal()
	return ryw.NativeRead(r, func(opts ryw.RywOptions, _ func([]byte, []byte)) (kv.RangeResult, func(ctx context.Context) (kv.RangeResult, error), error) {
		ok, err := ryw.ValidateGetRange(&opts, &begin, &end, limits)
		if err != nil || !ok {
			return kv.RangeResult{}, nil, err
		}
		reader.StartReadVersion()
		return kv.RangeResult{}, func(ctx context.Context) (kv.RangeResult, error) {
			endSel := end
			if reverse && end.Offset > 1 {
				key, err := reader.GetKey(ctx, end)
				if err != nil {
					return kv.RangeResult{}, err
				}
				if !snapshot {
					c := native.GetKeyConflict(end, key)
					if bytes.Compare(c.Begin, c.End) < 0 {
						if err := r.AddNativeReadConflictForAttempt(attempt, c.Begin, c.End); err != nil {
							return kv.RangeResult{}, err
						}
					}
				}
				endSel = ryw.ClipReverseEnd(&opts, key)
			}
			rr, err := reader.GetRangeOut(ctx, begin, endSel, limits, reverse)
			if err != nil {
				return kv.RangeResult{}, err
			}
			if !snapshot {
				c := native.GetRangeConflict(rr.Begin, rr.End, &rr.Out, func(kv *kv.KeyValue) []byte { return kv.Key }, reverse)
				if bytes.Compare(c.Begin, c.End) < 0 {
					if err := r.AddNativeReadConflictForAttempt(attempt, c.Begin, c.End); err != nil {
						return kv.RangeResult{}, err
					}
				}
			}
			return ryw.ClipRangeResult(&opts, rr.Result(), reverse), nil
		}, nil
	})
}

// GetMappedRange is fdb_transaction_get_mapped_range.
func (t *Txn) GetMappedRange(begin, end kv.KeySelector, mapper []byte, o RangeOpts, snapshot bool) (MappedRangeResult, func(ctx context.Context) (MappedRangeResult, error), error) {
	limits, err := o.limits()
	if err != nil {
		return MappedRangeResult{}, nil, err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.ryw.DeferredError(); err != nil {
		return MappedRangeResult{}, nil, err
	}
	begin.Normalize()
	end.Normalize()
	if isSpecialKey(begin.Key) && string(end.Key) <= specialKeysEnd {
		return MappedRangeResult{}, nil, fdberr.ClientInvalidOperation
	}
	read, err := t.ryw.CheckMappedRange(&begin, &end, limits, snapshot)
	if err == nil && read && t.ryw.Options().ReadYourWritesDisabled {
		err = fdberr.UnsupportedOperation
	}
	if err != nil || !read {
		return MappedRangeResult{}, nil, err
	}
	begin.Key, end.Key, mapper = bytes.Clone(begin.Key), bytes.Clone(end.Key), bytes.Clone(mapper)
	reader, r, attempt := t.native.Reader(), t.ryw, t.ryw.ResetSignal()
	reverse := o.Reverse
	return ryw.NativeRead(r, func(opts ryw.RywOptions, _ func([]byte, []byte)) (MappedRangeResult, func(context.Context) (MappedRangeResult, error), error) {
		reader.StartReadVersion()
		return MappedRangeResult{}, func(ctx context.Context) (MappedRangeResult, error) {
			endSel := end
			if reverse && end.Offset > 1 {
				key, err := reader.GetKey(ctx, end)
				if err != nil {
					return MappedRangeResult{}, err
				}
				endSel = ryw.ClipReverseEnd(&opts, key)
			}
			out, err := reader.GetMappedRange(ctx, begin, endSel, mapper, limits, reverse)
			if err != nil {
				return MappedRangeResult{}, err
			}
			primary := ryw.RangeExtent{More: out.More, ReadToBegin: out.ReadToBegin, ReadThroughEnd: out.ReadThroughEnd}
			if n := len(out.Data); n > 0 {
				primary.HasRows, primary.First, primary.Last = true, out.Data[0].Key, out.Data[n-1].Key
			}
			secondaries := make([]ryw.MappedSecondary, len(out.Data))
			for i := range out.Data {
				row := &out.Data[i]
				if row.IsValue {
					secondaries[i] = ryw.MappedSecondary{IsValue: true, Key: row.Begin.Key}
					continue
				}
				ext := ryw.RangeExtent{More: row.More}
				if n := len(row.Data); n > 0 {
					ext.HasRows, ext.First, ext.Last = true, row.Data[0].Key, row.Data[n-1].Key
				}
				secondaries[i] = ryw.MappedSecondary{Begin: row.Begin, End: row.End, Result: ext}
			}
			if err := r.AddMappedRangeConflictsForAttempt(attempt, begin, end, reverse, primary, secondaries); err != nil {
				return MappedRangeResult{}, err
			}
			res := MappedRangeResult{Data: make([]MappedKeyValue, len(out.Data)), More: out.More}
			for i := range out.Data {
				row := &out.Data[i]
				res.Data[i] = MappedKeyValue{Key: row.Key, Value: row.Value, Begin: row.Begin, End: row.End, Data: row.Data}
			}
			return res, nil
		}, nil
	})
}

// specialWrite records a special-key write's error as deferred:
// writes need special_key_space_enable_writes,
// and no writable module is implemented.
func (t *Txn) specialWrite() {
	if t.ryw.Options().SpecialKeySpaceEnableWrites {
		t.ryw.SetDeferredError(fdberr.SpecialKeysNoWriteModuleFound)
	} else {
		t.ryw.SetDeferredError(fdberr.SpecialKeysWriteDisabled)
	}
}

// Set is fdb_transaction_set; errors are deferred to the next read or commit.
func (t *Txn) Set(key, value []byte) {
	if t.ryw.Set(key, value) {
		t.specialWrite()
	}
}

// Clear is fdb_transaction_clear; errors are deferred.
func (t *Txn) Clear(key []byte) {
	if t.ryw.Clear(key) {
		t.specialWrite()
	}
}

// ClearRange is fdb_transaction_clear_range; errors are deferred.
func (t *Txn) ClearRange(begin, end []byte) {
	if t.ryw.ClearRange(begin, end) {
		t.specialWrite()
	}
}

// AtomicOp is fdb_transaction_atomic_op with the C MutationType code; errors
// are deferred. MIN and AND follow the connected C client's API semantics:
// 5.0 uses the legacy operations, 5.1+ uses MinV2 and AndV2. Before discovery,
// retain modern local behavior.
func (t *Txn) AtomicOp(key, param []byte, op int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	// The read-version bounds and mutation must belong to the same attempt.
	rv, ok := t.native.Shared.CachedReadVersion()
	// Keep out-of-range codes invalid before narrowing, while leaving error
	// priority (deferred error, used during commit, key legality) to Ryw.
	wireOp := ryw.MutationMaxAtomicOp
	if op >= 0 && op < int(ryw.MutationMaxAtomicOp) {
		wireOp = uint8(op)
	}
	var protocol wire.ProtocolVersion
	switch wireOp {
	case ryw.MutationMin, ryw.MutationAnd, ryw.MutationAppendIfFits,
		ryw.MutationSetVersionstampedKey, ryw.MutationSetVersionstampedValue:
		if t.db.ctx != nil {
			protocol = t.db.ctx.ClusterNow().Protocol
		}
	}
	if wireOp == ryw.MutationMin || wireOp == ryw.MutationAnd {
		if protocol == 0 || protocol.Normalized() >= wire.Protocol51.Normalized() {
			if wireOp == ryw.MutationMin {
				wireOp = ryw.MutationMinV2
			} else {
				wireOp = ryw.MutationAndV2
			}
		}
	}
	legacyVersionstamps := protocol != 0 && protocol.Normalized() < wire.Protocol52.Normalized()
	if legacyVersionstamps && wireOp == ryw.MutationAppendIfFits {
		wireOp = ryw.MutationMaxAtomicOp
	}
	t.ryw.AtomicOp(key, param, wireOp, ryw.AtomicOptions{
		ReadVersion: rv, HasReadVersion: ok, LegacyVersionstamps: legacyVersionstamps,
	})
}

// AddConflictRange is fdb_transaction_add_conflict_range with the C
// ConflictRangeType code.
func (t *Txn) AddConflictRange(begin, end []byte, ty int) error {
	return t.ryw.AddConflictRange(begin, end, ty == options.ConflictRangeTypeWrite)
}

// Commit is fdb_transaction_commit.
func (t *Txn) Commit() (struct{}, func(ctx context.Context) (struct{}, error), error) {
	t.mu.Lock()
	plan, wait, err := t.ryw.Commit()
	end := t.ryw.ResetSignal()
	txn := *t.native
	txn.Opts.Tags = slices.Clone(txn.Opts.Tags)
	txn.Opts.ReadTags = slices.Clone(txn.Opts.ReadTags)
	if err == nil && wait == nil {
		txn.StartCommit(plan)
	}
	t.mu.Unlock()
	if err != nil {
		return struct{}{}, nil, err
	}
	run := func(p ryw.CommitPlan) (struct{}, error) {
		if err := end.Err(); err != nil {
			return struct{}{}, err
		}
		err := txn.Commit(p)
		if err == nil {
			t.mu.Lock()
			if end.Err() == nil {
				t.logRetry(nil)
			}
			t.mu.Unlock()
		}
		return struct{}{}, err
	}
	return struct{}{}, func(ctx context.Context) (struct{}, error) {
		if ctx.Done() != nil {
			stop := context.AfterFunc(ctx, func() { end.Abort(fdberr.TransactionCancelled) })
			defer stop()
			if err := ctx.Err(); err != nil {
				end.Abort(fdberr.TransactionCancelled)
				return struct{}{}, err
			}
		}
		if wait != nil {
			var err error
			plan, err = wait(ctx)
			if err != nil {
				return struct{}{}, err
			}
			if err = end.Err(); err != nil {
				return struct{}{}, err
			}
			txn.StartCommit(plan)
		}
		return run(plan)
	}, nil
}

// GetCommittedVersion is fdb_transaction_get_committed_version: the commit
// version, or -1 for a read-only commit.
func (t *Txn) GetCommittedVersion() (int64, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.native.Shared.CommittedVersion.Load(), nil
}

// CommittedVersionstamp returns the successful commit's versionstamp without
// registering a precommit wait or invoking the used-during-commit guard.
// Before success it returns transaction_invalid_version; read-only commits
// return no_commit_version.
func (t *Txn) CommittedVersionstamp() ([]byte, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	outcome := t.native.Shared.OutcomeNow()
	if outcome.State != native.OutcomeCommitted {
		return nil, fdberr.TransactionInvalidVersion
	}
	if !outcome.HasVersionstamp {
		return nil, fdberr.NoCommitVersion
	}
	return bytes.Clone(outcome.Versionstamp[:]), nil
}

// GetApproximateSize follows the connected C client's deferred-error behavior:
// before 7.2 size remains readable after a deferred error. Until discovery,
// local validation retains the current behavior.
func (t *Txn) GetApproximateSize() (int64, func(ctx context.Context) (int64, error), error) {
	if err := t.ryw.DeferredError(); err != nil {
		protocol := t.db.ctx.ClusterNow().Protocol
		if protocol == 0 || protocol.Normalized() >= wire.Protocol72 {
			return 0, nil, err
		}
	}
	return t.ryw.ApproximateSize(), nil, nil
}

// GetVersionstamp completes after a successful commit.
func (t *Txn) GetVersionstamp() ([]byte, func(ctx context.Context) ([]byte, error), error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.ryw.DeferredError(); err != nil {
		return nil, nil, err
	}
	if t.ryw.CheckUsedDuringCommit() {
		return nil, nil, fdberr.UsedDuringCommit
	}
	sh, end := t.native.Shared, t.ryw.ResetSignal()
	return nil, func(ctx context.Context) ([]byte, error) {
		ctx, cancel := context.WithCancelCause(ctx)
		hook := end.OnEnd(cancel)
		defer hook.Release()
		defer cancel(nil)
		// A native reset can decide broken_promise before the RYW reset.
		// Preserve that decision even when the caller waits after backoff.
		o, err := sh.Outcome(ctx)
		if err != nil {
			return nil, err
		}
		switch {
		case o.State == native.OutcomeCommitted && o.HasVersionstamp:
			return bytes.Clone(o.Versionstamp[:]), nil
		case o.State == native.OutcomeCommitted:
			return nil, fdberr.NoCommitVersion
		case o.State == native.OutcomeFailed:
			return nil, fdberr.TransactionInvalidVersion
		default:
			return nil, fdberr.BrokenPromise
		}
	}, nil
}

// Watch completes when the key changes after commit; it outlives the transaction.
func (t *Txn) Watch(key []byte) (struct{}, func(ctx context.Context) (struct{}, error), error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.ryw.Watch(t.native.Reader(), bytes.Clone(key))
}

// cancelledIfBroken maps broken_promise to transaction_cancelled, as onError does.
func cancelledIfBroken(err error) error {
	if err == fdberr.BrokenPromise {
		return fdberr.TransactionCancelled
	}
	return err
}

// OnError is fdb_transaction_on_error: it resets and waits out the backoff if err is
// retryable, otherwise fails with err.
func (t *Txn) OnError(err fdberr.Error) (struct{}, func(ctx context.Context) (struct{}, error), error) {
	snapshotRYW := t.db.snapshotRYWEnabled()
	t.mu.Lock()
	delay, derr := t.decideOnError(err)
	if derr != nil {
		// Cancel before Reset can install another attempt.
		t.ryw.Cancel()
		t.mu.Unlock()
		return struct{}{}, nil, cancelledIfBroken(derr)
	}
	end := t.ryw.ResetSignal()
	finish := func(ctx context.Context) (struct{}, error) {
		snapshotRYW := t.db.snapshotRYWEnabled()
		t.mu.Lock()
		defer t.mu.Unlock()
		if e := end.Err(); e != nil {
			return struct{}{}, cancelledIfBroken(e)
		}
		t.logRetry(err)
		t.resetRyw(snapshotRYW)
		return struct{}{}, nil
	}
	if delay <= 0 {
		if e := end.Err(); e != nil {
			t.mu.Unlock()
			return struct{}{}, nil, cancelledIfBroken(e)
		}
		t.logRetry(err)
		t.resetRyw(snapshotRYW)
		t.mu.Unlock()
		return struct{}{}, nil, nil
	}
	t.mu.Unlock()
	timer := time.NewTimer(delay)
	return struct{}{}, func(ctx context.Context) (struct{}, error) {
		defer timer.Stop()
		select {
		case <-end.Done():
			return struct{}{}, cancelledIfBroken(end.Err())
		case <-ctx.Done():
			return struct{}{}, fdberr.OperationCancelled
		case <-timer.C:
			return finish(ctx)
		}
	}, nil
}

// decideOnError counts the retry and asks the native transaction for the
// backoff. Callers hold mu.
func (t *Txn) decideOnError(err fdberr.Error) (time.Duration, error) {
	if e := t.ryw.ResetError(); e != nil {
		return 0, e
	}
	limitHit := t.maxRetries != -1 && t.retries >= t.maxRetries
	if t.retries < 1<<31-1 {
		t.retries++
	}
	if limitHit {
		return 0, err
	}
	return t.native.OnError(err)
}

// Reset is fdb_transaction_reset: a fresh transaction keeping only database
// defaults.
func (t *Txn) Reset() {
	o := t.db.snapshotOptions()
	t.mu.Lock()
	defer t.mu.Unlock()
	t.retries = 0
	t.created = time.Now()
	t.timeout = 0
	t.maxRetries = -1
	t.hasDebugInfo = false
	t.debugName = ""
	t.persistent = o.defaults
	old := t.native
	old.Shared.BreakVersionstamp()
	t.native = native.NewNativeTxn(t.db.ctx, native.DefaultNativeOptions())
	t.resetRyw(o.snapshotRYW)
	// The attempt's reset signal fires before its native reads are stopped.
	old.Close()
}

// Cancel is fdb_transaction_cancel: pending and future operations fail with
// transaction_cancelled.
func (t *Txn) Cancel() { t.ryw.Cancel() }

// guarded races a native operation against the end of the attempt, after the
// deferred-error, used-during-commit and reset checks. Callers hold mu.
func guarded[T any](t *Txn, start func() (func(context.Context) (T, error), error)) (T, func(context.Context) (T, error), error) {
	var zero T
	if err := t.ryw.DeferredError(); err != nil {
		return zero, nil, err
	}
	if t.ryw.CheckUsedDuringCommit() {
		return zero, nil, fdberr.UsedDuringCommit
	}
	if err := t.ryw.ResetError(); err != nil {
		return zero, nil, err
	}
	end := t.ryw.ResetSignal()
	f, err := start()
	if err != nil {
		return zero, nil, err
	}
	return zero, attemptWait(end, f), nil
}

// GetAddressesForKey returns addresses of the storage servers holding key.
func (t *Txn) GetAddressesForKey(key []byte) ([]string, func(ctx context.Context) ([]string, error), error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	key = bytes.Clone(key)
	return ryw.NativeRead(t.ryw, func(_ ryw.RywOptions, _ func([]byte, []byte)) ([]string, func(ctx context.Context) ([]string, error), error) {
		reader, includePort := t.native.Reader(), t.native.Opts.IncludePort
		reader.StartReadVersion()
		return nil, func(ctx context.Context) ([]string, error) {
			return reader.AddressesForKey(ctx, key, includePort)
		}, nil
	})
}

// GetEstimatedRangeSizeBytes estimates the size of the range.
func (t *Txn) GetEstimatedRangeSizeBytes(begin, end []byte) (int64, func(ctx context.Context) (int64, error), error) {
	if bytes.Compare(begin, end) > 0 {
		return 0, nil, fdberr.InvertedRange
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	begin, end = bytes.Clone(begin), bytes.Clone(end)
	return guarded(t, func() (func(context.Context) (int64, error), error) {
		reader := t.native.Reader()
		return func(ctx context.Context) (int64, error) { return reader.EstimatedRangeSize(ctx, begin, end) }, nil
	})
}

// GetRangeSplitPoints returns split points for the range.
func (t *Txn) GetRangeSplitPoints(begin, end []byte, chunkSize int64) ([][]byte, func(ctx context.Context) ([][]byte, error), error) {
	if bytes.Compare(begin, end) > 0 {
		return nil, nil, fdberr.InvertedRange
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	begin, end = bytes.Clone(begin), bytes.Clone(end)
	return guarded(t, func() (func(context.Context) ([][]byte, error), error) {
		o := t.ryw.Options()
		max := o.MaxReadKey()
		if bytes.Compare(begin, max) > 0 || bytes.Compare(end, max) > 0 {
			return nil, fdberr.KeyOutsideLegalRange
		}
		reader := t.native.Reader()
		return func(ctx context.Context) ([][]byte, error) {
			return reader.RangeSplitPoints(ctx, begin, end, chunkSize)
		}, nil
	})
}

// logRetry is debugLogRetries: warns about transactions retrying or committing
// after more than a second. err is nil for a commit. Callers hold mu.
func (t *Txn) logRetry(err error) {
	if !t.debugRetryLogging || !t.hasDebugInfo {
		return
	}
	elapsed := time.Since(t.created)
	if time.Since(t.debugLast) >= time.Second || (err == nil && elapsed > time.Second) {
		outcome := "committed"
		if err != nil {
			outcome = err.Error()
		}
		slog.Warn("fdb WARNING: long transaction", "elapsed", elapsed.Seconds(), "name", t.debugName,
			"retries", t.retries, "outcome", outcome)
		t.debugLast = time.Now()
	}
}
