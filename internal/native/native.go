package native

import (
	"bytes"
	"context"
	"encoding/binary"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/can1357/fdbgo/internal/fdberr"
	"github.com/can1357/fdbgo/internal/kv"
	"github.com/can1357/fdbgo/internal/ryw"
	"github.com/can1357/fdbgo/internal/transport"
	"github.com/can1357/fdbgo/internal/wire"
)

const (
	// wrongShardServerDelay is the pause before retrying a read that hit the wrong shard.
	wrongShardServerDelay = 10 * time.Millisecond
	// futureVersionRetryDelay is the pause, in seconds, before retrying a future_version read.
	futureVersionRetryDelay = 0.01
	// defaultBackoff is the initial retry backoff, in seconds.
	defaultBackoff = 0.01
	// DefaultMaxBackoff is the default retry backoff cap, in seconds.
	DefaultMaxBackoff = 1.0
	// resourceConstrainedMaxBackoff caps the backoff after resource-constrained errors, in seconds.
	resourceConstrainedMaxBackoff = 30.0
	// backoffGrowthRate is the factor the backoff grows by per retry.
	backoffGrowthRate = 2.0
	// TransactionSizeLimit is the largest commit size, in bytes.
	TransactionSizeLimit = 10_000_000
	// replyByteLimit caps the bytes requested from storage in one range read reply.
	replyByteLimit = 80_000
	// getRangeShardLimit caps the shards whose locations one range read fetches.
	getRangeShardLimit = 2
	// tooMany is the row or shard-location limit of reads meant to be unbounded.
	tooMany = 1_000_000
	// watchPollingTime is the pause before re-polling a watch.
	watchPollingTime = time.Second
	// maxVersionCacheLag is the oldest, in seconds, a cached read version may be.
	maxVersionCacheLag = 0.1
	// maxProxyContactLag is how long, in seconds, the GRV cache may go without contacting a proxy.
	maxProxyContactLag = 0.2
	// grvCacheRKCooldown is how long, in seconds, the GRV cache stays off after ratekeeper throttling.
	grvCacheRKCooldown = 60.0
	// maxWriteTransactionLifeVersions is how many versions (5 s) a write transaction may live.
	maxWriteTransactionLifeVersions = 5_000_000
)

var (
	// idempotencyKeysBegin is \xff\x02/idmp/: idempotency ids of recent
	// commits, keyed by big-endian commit version.
	idempotencyKeysBegin = []byte("\xff\x02/idmp/")
	// idempotencyExpiredVersionKey is \xff\x02/idmpExpiredVersion.
	idempotencyExpiredVersionKey = []byte("\xff\x02/idmpExpiredVersion")
)

// NativeOptions are the native transaction options (TransactionOptions plus
// the options TransactionState carries).
type NativeOptions struct {
	Priority         Priority
	CausalReadRisky  bool
	CausalWriteRisky bool
	LockAware        bool
	// ReadOnly is set by read_lock_aware alone: commits are refused.
	ReadOnly              bool
	FirstInBatch          bool
	IncludePort           bool
	ReportConflictingKeys bool
	UseProvisionalProxies bool
	RawAccess             bool
	BypassStorageQuota    bool
	UseGRVCache           bool
	SkipGRVCache          bool
	// Tags is the TagSet sent with commits and read version requests.
	Tags [][]byte
	// ReadTags are also sent with reads (auto_throttle_tag).
	ReadTags   [][]byte
	SizeLimit  int64
	MaxBackoff float64
	// ReadOptions are sent with reads when HasReadOptions; see ReadOpts.
	ReadOptions    wire.ReadOpts
	HasReadOptions bool
	// IdempotencyID is nil when unset.
	IdempotencyID        []byte
	AutomaticIdempotency bool
	ReplicaCheck         bool
	RequiredReplicas     int64
	// DebugID is debug_transaction_identifier / transaction_logging_enable ("" = unset).
	DebugID                      string
	LogTransaction               bool
	MaxLogFieldLength            int64
	ExpensiveClearCostEstimation bool
}

// DefaultNativeOptions returns the options of a new transaction.
func DefaultNativeOptions() NativeOptions {
	return NativeOptions{
		IncludePort: true,
		SizeLimit:   TransactionSizeLimit,
		MaxBackoff:  DefaultMaxBackoff,
	}
}

// ReadOpts returns the read options for modification, initializing them to
// the ReadOptions defaults first.
func (o *NativeOptions) ReadOpts() *wire.ReadOpts {
	if !o.HasReadOptions {
		o.ReadOptions = wire.DefaultReadOpts()
		o.HasReadOptions = true
	}
	return &o.ReadOptions
}

// grvFlags is getReadVersionFlags plus the priority bits.
func (o *NativeOptions) grvFlags() uint32 {
	f := o.Priority.GRVFlag()
	if o.CausalReadRisky {
		f |= grvCausalReadRisky
	}
	if o.UseProvisionalProxies {
		f |= grvUseProvisional
	}
	return f
}

func cloneTags(tags [][]byte) [][]byte {
	if len(tags) == 0 {
		return nil
	}
	return slices.Clone(tags)
}

func (o *NativeOptions) readParams() *readParams {
	p := &readParams{
		readTags:     cloneTags(o.ReadTags),
		provisional:  o.UseProvisionalProxies,
		replicaCheck: o.ReplicaCheck,
		required:     o.RequiredReplicas,
	}
	p.readOptions, p.hasReadOptions = o.ReadOptions, o.HasReadOptions
	return p
}

// readParams is the request context a read captures when it is issued; immutable.
type readParams struct {
	readOptions    wire.ReadOpts
	hasReadOptions bool
	readTags       [][]byte
	provisional    bool
	// replicaCheck enables consistency_check_required_replicas = required.
	replicaCheck bool
	required     int64
}

// grvParams is what a read version request captures.
type grvParams struct {
	debugID   wire.UID
	flags     uint32
	tags      [][]byte
	lockAware bool
	grvCache  bool
	priority  Priority
}

func grvParamsOf(o *NativeOptions) grvParams {
	return grvParams{
		debugID:   o.ReadOptions.DebugID,
		flags:     o.grvFlags(),
		tags:      cloneTags(o.Tags),
		lockAware: o.LockAware,
		grvCache:  o.UseGRVCache && !o.SkipGRVCache,
		priority:  o.Priority,
	}
}

// ReadVersion is what a read version request resolves to.
type ReadVersion struct {
	Version int64
	// MetadataVersion is \xff/metadataVersion as of Version when
	// MetadataVersionKnown is true (nil = absent, non-nil empty = present).
	MetadataVersion []byte
	// MetadataVersionKnown distinguishes a reported absence from unavailable
	// metadata, as with an explicit read version or a GRV-cache miss.
	MetadataVersionKnown bool
}

// OutcomeState is the state of a transaction's commit.
type OutcomeState uint8

const (
	// OutcomePending means the commit is not decided yet.
	OutcomePending OutcomeState = iota
	// OutcomeCommitted means the commit succeeded.
	OutcomeCommitted
	// OutcomeFailed means the commit failed or the attempt ended without one.
	OutcomeFailed
)

// CommitOutcome is the commit's result as versionstamp and watch futures see it.
type CommitOutcome struct {
	State OutcomeState
	// Versionstamp is the 10-byte versionstamp when HasVersionstamp;
	// read-only commits have none.
	Versionstamp    [10]byte
	HasVersionstamp bool
}

// pendingWatch is a watch registered in this attempt, armed once the commit succeeds.
type pendingWatch struct {
	arm func(version int64, err error)
}

// unknownVersion is TxnShared.knownVersion before the read version is known.
const unknownVersion = int64(-1 << 63)

// TxnShared is the state shared by one attempt and its in-flight reads.
type TxnShared struct {
	// DB is the database context.
	DB *DbContext

	ctx    context.Context
	cancel context.CancelCauseFunc

	rvMu      sync.Mutex
	rvStarted bool
	rvDone    bool
	rv        ReadVersion
	rvErr     error
	rvReady   chan struct{}
	attempt   *ryw.AttemptEnd

	// CommittedVersion is the commit version, or -1 (invalidVersion).
	CommittedVersion atomic.Int64

	outcomeMu    sync.Mutex
	outcome      CommitOutcome
	outcomeErr   error
	outcomeReady chan struct{}

	conflictMu     sync.Mutex
	conflicting    []kv.KeyRange
	hasConflicting bool

	watchMu sync.Mutex
	watches []pendingWatch

	paramsMu sync.Mutex
	grvp     grvParams

	startTime float64
	// knownVersion caches the read version once a reader saw it
	// (unknownVersion until then), so later reads skip the lock.
	knownVersion atomic.Int64
}

// newTxnShared starts an attempt whose context ends with parent. Attempts do
// not derive from the database context (that would serialize every
// transaction on its lock); database operations fail once it is closed.
func newTxnShared(parent context.Context, db *DbContext, opts *NativeOptions) *TxnShared {
	ctx, cancel := context.WithCancelCause(parent)
	s := &TxnShared{
		DB:        db,
		ctx:       ctx,
		cancel:    cancel,
		grvp:      grvParamsOf(opts),
		startTime: transport.Now(),
	}
	s.CommittedVersion.Store(invalidVersion)
	s.knownVersion.Store(unknownVersion)
	return s
}

// Context returns the attempt's context: cancelled (cause
// transaction_cancelled) when the attempt ends.
func (s *TxnShared) Context() context.Context { return s.ctx }

// end finishes the attempt: reads still running fail with
// transaction_cancelled, pending watches too. An undecided commit outcome
// stays pending, since the outcome outlives the attempt; its
// waiters race it against the read-your-writes reset signal.
func (s *TxnShared) end() { s.endWithError(fdberr.TransactionCancelled) }

func (s *TxnShared) endWithError(err error) {
	s.watchMu.Lock()
	s.cancel(err)
	ws := s.watches
	s.watches = nil
	s.watchMu.Unlock()
	for _, w := range ws {
		w.arm(0, err)
	}
}

// startReadVersionLocked marks the read version requested with extraFlags
// on first use (startTransaction) and returns the request to issue once rvMu
// is released (completion may run synchronously), or nil. Callers hold rvMu.
func (s *TxnShared) startReadVersionLocked(extraFlags uint32) func() {
	if s.rvStarted {
		return nil
	}
	s.rvStarted = true
	s.rvReady = make(chan struct{})
	s.paramsMu.Lock()
	p := s.grvp
	s.paramsMu.Unlock()
	p.flags |= extraFlags
	// Read version requests are owned by the batcher, not the attempt.
	return func() { fetchReadVersion(s.DB, s.startTime, p, s.completeReadVersion) }
}

func run(start func()) {
	if start != nil {
		start()
	}
}

// BindAttempt associates read-version publication with the read-your-writes
// cancellation signal. It is called before this attempt is exposed to readers.
func (s *TxnShared) BindAttempt(end *ryw.AttemptEnd) {
	s.rvMu.Lock()
	s.attempt = end
	s.rvMu.Unlock()
	end.OnEndImmediate(s.endWithError)
}

func (s *TxnShared) completeReadVersion(rv ReadVersion, err error) {
	s.rvMu.Lock()
	end := s.attempt
	s.rvMu.Unlock()
	publish := func(ended error) {
		s.rvMu.Lock()
		defer s.rvMu.Unlock()
		if ended != nil {
			rv, err = ReadVersion{}, ended
		} else if s.ctx != nil && s.ctx.Err() != nil {
			rv, err = ReadVersion{}, cancelErr(s.ctx)
		}
		s.rv, s.rvErr, s.rvDone = rv, err, true
		close(s.rvReady)
	}
	if end != nil {
		end.Resolve(publish)
	} else {
		publish(nil)
	}
}

// StartReadVersion starts the read version request before returning.
func (s *TxnShared) StartReadVersion() {
	s.startReadVersion(0)
}

func (s *TxnShared) startReadVersion(extraFlags uint32) {
	s.rvMu.Lock()
	start := s.startReadVersionLocked(extraFlags)
	s.rvMu.Unlock()
	run(start)
}

// readVersionWait waits for the read version, requesting it with extraFlags on first use.
func (s *TxnShared) readVersionWait(ctx context.Context, extraFlags uint32) (ReadVersion, error) {
	s.rvMu.Lock()
	if s.rvDone {
		rv, err := s.rv, s.rvErr
		s.rvMu.Unlock()
		return rv, err
	}
	start := s.startReadVersionLocked(extraFlags)
	ready := s.rvReady
	s.rvMu.Unlock()
	run(start)
	select {
	case <-ready:
		s.rvMu.Lock()
		rv, err := s.rv, s.rvErr
		s.rvMu.Unlock()
		return rv, err
	case <-ctx.Done():
		return ReadVersion{}, cancelErr(ctx)
	}
}

// ReadVersion returns the read version, requesting it on first use.
func (s *TxnShared) ReadVersion(ctx context.Context) (ReadVersion, error) {
	return s.readVersionWait(ctx, 0)
}

// CachedReadVersion returns the read version if it is already known.
func (s *TxnShared) CachedReadVersion() (int64, bool) {
	s.rvMu.Lock()
	defer s.rvMu.Unlock()
	if s.rvDone && s.rvErr == nil {
		return s.rv.Version, true
	}
	return 0, false
}

func (s *TxnShared) setFixedVersion(v int64) {
	s.rvStarted, s.rvDone = true, true
	s.rv = ReadVersion{Version: v}
	s.knownVersion.Store(v)
}

// SetReadVersion sets the transaction's read version.
func (s *TxnShared) SetReadVersion(v int64) error {
	s.rvMu.Lock()
	defer s.rvMu.Unlock()
	if s.rvStarted {
		return fdberr.ReadVersionAlreadySet
	}
	if v <= 0 {
		return fdberr.VersionInvalid
	}
	s.setFixedVersion(v)
	return nil
}

// SetInitializeVersion forces version 0 (initialize_new_database).
func (s *TxnShared) SetInitializeVersion() error {
	s.rvMu.Lock()
	defer s.rvMu.Unlock()
	if s.rvStarted {
		return fdberr.ReadVersionAlreadySet
	}
	s.setFixedVersion(0)
	return nil
}

// checkVersion is validateVersion: reads at version 0 (initialize_new_database) are illegal.
func checkVersion(v int64) error {
	if v == 0 {
		return fdberr.ClientInvalidOperation
	}
	return nil
}

// version waits for the read version for reads.
func (s *TxnShared) version(ctx context.Context) (int64, error) {
	v := s.knownVersion.Load()
	if v == unknownVersion {
		rv, err := s.readVersionWait(ctx, 0)
		if err != nil {
			return 0, err
		}
		v = rv.Version
		s.knownVersion.Store(v)
	}
	return v, checkVersion(v)
}

// OutcomeNow returns the commit outcome as of now.
func (s *TxnShared) OutcomeNow() CommitOutcome {
	s.outcomeMu.Lock()
	defer s.outcomeMu.Unlock()
	return s.outcome
}

// Outcome waits until the commit outcome is decided or ctx is cancelled.
func (s *TxnShared) Outcome(ctx context.Context) (CommitOutcome, error) {
	s.outcomeMu.Lock()
	if s.outcome.State != OutcomePending {
		o, err := s.outcome, s.outcomeErr
		s.outcomeMu.Unlock()
		return o, err
	}
	if s.outcomeReady == nil {
		s.outcomeReady = make(chan struct{})
	}
	ready := s.outcomeReady
	s.outcomeMu.Unlock()
	select {
	case <-ready:
		s.outcomeMu.Lock()
		o, err := s.outcome, s.outcomeErr
		s.outcomeMu.Unlock()
		return o, err
	case <-ctx.Done():
		s.outcomeMu.Lock()
		o, err := s.outcome, s.outcomeErr
		s.outcomeMu.Unlock()
		if o.State != OutcomePending {
			return o, err
		}
		return CommitOutcome{}, cancelErr(ctx)
	}
}

func (s *TxnShared) setOutcome(o CommitOutcome) error { return s.completeOutcome(o, nil) }

// BreakVersionstamp fails an undecided versionstamp with broken_promise,
// without ending the attempt's reads or watches. Reset calls it before
// publishing the read-your-writes attempt's cancellation.
func (s *TxnShared) BreakVersionstamp() {
	s.completeOutcome(CommitOutcome{State: OutcomeFailed}, fdberr.BrokenPromise)
}

// completeOutcome preserves the first result, including native reset errors,
// and returns the error selected by that publication.
func (s *TxnShared) completeOutcome(o CommitOutcome, err error) error {
	var selected error
	publish := func(ended error) {
		s.outcomeMu.Lock()
		defer s.outcomeMu.Unlock()
		if s.outcome.State == OutcomePending {
			if ended == nil && s.ctx != nil && s.ctx.Err() != nil {
				ended = cancelErr(s.ctx)
			}
			if ended != nil {
				o = CommitOutcome{State: OutcomeFailed}
				err = ended
			}
			s.outcome, s.outcomeErr = o, err
			if s.outcomeReady != nil {
				close(s.outcomeReady)
			}
		}
		selected = s.outcomeErr
	}
	s.rvMu.Lock()
	end := s.attempt
	s.rvMu.Unlock()
	if end != nil {
		end.Resolve(publish)
	} else {
		publish(nil)
	}
	return selected
}

// Conflicting returns the read conflict ranges the resolver reported
// (conflictingKeys) and whether it reported any.
func (s *TxnShared) Conflicting() ([]kv.KeyRange, bool) {
	s.conflictMu.Lock()
	defer s.conflictMu.Unlock()
	return s.conflicting, s.hasConflicting
}

// fetchReadVersion obtains a read version from the GRV cache or a proxy;
// done runs once and must not block.
func fetchReadVersion(c *DbContext, startTime float64, p grvParams, done func(ReadVersion, error)) {
	if p.grvCache {
		g := c.grvCacheSnapshot()
		t := transport.Now()
		var cooled bool
		switch p.priority {
		case PriorityImmediate:
			cooled = true
		case PriorityBatch:
			cooled = g.LastRKBatchThrottle == 0 || t-g.LastRKBatchThrottle > grvCacheRKCooldown
		default:
			cooled = g.LastRKDefaultThrottle == 0 || t-g.LastRKDefaultThrottle > grvCacheRKCooldown
		}
		if cooled {
			startGRVUpdater(c)
			g = c.grvCacheSnapshot()
			if transport.Now()-g.Time <= maxVersionCacheLag && g.Version != 0 {
				mv, known := c.CachedMetadataVersion(g.Version)
				done(ReadVersion{Version: g.Version, MetadataVersion: mv, MetadataVersionKnown: known}, nil)
				return
			}
		}
	}
	if len(p.tags) > 0 {
		c.throttleMu.Lock()
		err := c.throttles.CheckBeforeGRV(p.priority, p.tags)
		c.throttleMu.Unlock()
		if err != nil {
			done(ReadVersion{}, err)
			return
		}
	}
	c.ReadVersion(p.flags, p.tags, p.debugID, func(reply *wire.ReadVersionReply, err error) {
		if err != nil {
			done(ReadVersion{}, err)
			return
		}
		c.grvCacheMu.Lock()
		c.grvCache.LastProxyTime = startTime
		c.grvCacheMu.Unlock()
		if reply.Locked && !p.lockAware {
			done(ReadVersion{}, fdberr.DatabaseLocked)
			return
		}
		if len(p.tags) > 0 {
			c.throttleMu.Lock()
			err := c.throttles.CheckAfterGRV(p.priority, p.tags)
			c.throttleMu.Unlock()
			if err != nil {
				done(ReadVersion{}, err)
				return
			}
		}
		done(ReadVersion{Version: reply.Version, MetadataVersion: reply.MetadataVersion, MetadataVersionKnown: true}, nil)
	})
}

// startGRVUpdater keeps the cached read version fresh for use_grv_cache
// (backgroundGrvUpdater) until the database is closed.
func startGRVUpdater(c *DbContext) {
	c.grvCacheMu.Lock()
	if c.grvCache.UpdaterRunning || !c.addBackground() {
		c.grvCacheMu.Unlock()
		return
	}
	c.grvCache.UpdaterRunning = true
	c.grvCacheMu.Unlock()
	go func() {
		defer c.wg.Done()
		grvDelay := 0.001
		for {
			now := transport.Now()
			g := c.grvCacheSnapshot()
			if now-g.Time >= maxVersionCacheLag-grvDelay || now-g.LastProxyTime > maxProxyContactLag {
				_, err := c.ReadVersionWait(c.ctx, PriorityDefault.GRVFlag(), nil)
				if c.ctx.Err() != nil {
					return
				}
				if err == nil {
					c.grvCacheMu.Lock()
					c.grvCache.LastProxyTime = now
					c.grvCacheMu.Unlock()
					grvDelay = (grvDelay + (transport.Now() - now)) / 2
				} else if sleep(c.ctx, 100*time.Millisecond) != nil {
					return
				}
			} else {
				wait := max(min(maxProxyContactLag-(now-g.LastProxyTime), (maxVersionCacheLag-grvDelay)-(now-g.Time)), 0.001)
				if sleep(c.ctx, secs(wait)) != nil {
					return
				}
			}
		}
	}()
}

var (
	_ ryw.SnapshotReader = Reader{}
	_ ryw.WatchRegistrar = Reader{}
)

// Reader issues snapshot reads for one attempt with the options in force
// when it was created; it is a small value, cheap to copy.
type Reader struct {
	shared *TxnShared
	params *readParams
}

func (r Reader) db() *DbContext { return r.shared.DB }

func (r Reader) readCtx(version int64) wire.ReadCtx {
	return wire.ReadCtx{Version: version, Tags: r.params.readTags, Options: r.params.readOptions, HasOptions: r.params.hasReadOptions}
}

// lb load balances a storage read, comparing replicas if the transaction asked to.
func lbRead[T any](ctx context.Context, r Reader, loc Location, stream uint32, req wire.Request[wire.Lb[T]], equal func(a, b T) bool) (T, error) {
	if r.params.replicaCheck {
		return loadBalanceChecked(ctx, r.db(), loc.Servers, stream, req, r.params.required, equal)
	}
	return loadBalance(ctx, r.db(), loc.Servers, stream, req)
}

func isLocationError(err error) bool {
	return err == fdberr.WrongShardServer || err == fdberr.AllAlternativesFailed
}

// StartReadVersion starts the attempt's read version request synchronously.
func (r Reader) StartReadVersion() { r.shared.StartReadVersion() }

// BoundAttempt identifies the attempt whose cancellation every reader method
// observes, allowing RYW to avoid registering a duplicate cancellation hook.
func (r Reader) BoundAttempt() *ryw.AttemptEnd { return r.shared.attempt }

// readContext combines caller cancellation with the lifetime of the attempt.
func (r Reader) readContext(ctx context.Context) (context.Context, func()) {
	if ctx == context.Background() || ctx == context.TODO() || ctx == r.shared.ctx || r.shared.ctx.Err() != nil {
		return r.shared.ctx, func() {}
	}
	ctx, cancel := context.WithCancelCause(ctx)
	stop := context.AfterFunc(r.shared.ctx, func() { cancel(context.Cause(r.shared.ctx)) })
	return ctx, func() { stop(); cancel(nil) }
}

// Get reads key at the snapshot; nil means absent.
func (r Reader) Get(ctx context.Context, key []byte) ([]byte, error) {
	ctx, done := r.readContext(ctx)
	defer done()
	if err := ctx.Err(); err != nil {
		return nil, cancelErr(ctx)
	}
	return r.getValueWait(ctx, key)
}

// getValue is the sequential getValue loop at version.
func (r Reader) getValue(ctx context.Context, key []byte, version int64) ([]byte, error) {
	for {
		loc, err := r.db().Locate(ctx, key, false, r.params.provisional, ssGetValue)
		if err != nil {
			return nil, err
		}
		req := &wire.GetValue{Key: key, Ctx: r.readCtx(version)}
		v, err := lbRead(ctx, r, loc, ssGetValue, wire.Request[wire.Lb[[]byte]](req), equalValue)
		if isLocationError(err) {
			r.db().Locations.Invalidate(key, false)
			if err := sleep(ctx, wrongShardServerDelay); err != nil {
				return nil, err
			}
			continue
		}
		return v, err
	}
}

// getValueWait reads key at the snapshot from sequential code.
func (r Reader) getValueWait(ctx context.Context, key []byte) ([]byte, error) {
	if len(key) > ryw.MaxReadKeySize(key) {
		return nil, nil
	}
	if bytes.Equal(key, kv.MetadataVersionKey) {
		rv, err := r.shared.readVersionWait(ctx, 0)
		if err != nil {
			return nil, err
		}
		if err := checkVersion(rv.Version); err != nil {
			return nil, err
		}
		if rv.MetadataVersionKnown {
			return rv.MetadataVersion, nil
		}
		if mv, known := r.db().CachedMetadataVersion(rv.Version); known {
			return mv, nil
		}
		return r.getValue(ctx, key, rv.Version)
	}
	v, err := r.shared.version(ctx)
	if err != nil {
		return nil, err
	}
	return r.getValue(ctx, key, v)
}

func equalValue(a, b []byte) bool { return (a == nil) == (b == nil) && bytes.Equal(a, b) }

func equalSel(a, b kv.KeySelector) bool {
	return a.OrEqual == b.OrEqual && a.Offset == b.Offset && bytes.Equal(a.Key, b.Key)
}

// GetKey resolves a key selector at the snapshot.
func (r Reader) GetKey(ctx context.Context, sel kv.KeySelector) ([]byte, error) {
	ctx, done := r.readContext(ctx)
	defer done()
	if err := ctx.Err(); err != nil {
		return nil, cancelErr(ctx)
	}
	sel.Normalize()
	return r.getKey(ctx, sel)
}

// getKeyShortcut applies getKey's special cases before a storage request:
// final reports that key is the answer.
func getKeyShortcut(k kv.KeySelector) (kv.KeySelector, []byte, bool) {
	if bytes.Equal(k.Key, kv.SpecialKeysBegin) {
		if k.Offset > 0 {
			return k, slices.Clone(kv.SpecialKeysBegin), true
		}
		k.OrEqual = false
	} else if len(k.Key) == 0 && k.Offset <= 0 {
		return k, []byte{}, true
	}
	return k, nil, false
}

// getKeyResolved reports whether a storage server's reply selector is resolved.
func getKeyResolved(sel kv.KeySelector) ([]byte, bool) {
	if sel.Offset == 0 && sel.OrEqual {
		if sel.Key == nil {
			return []byte{}, true
		}
		return sel.Key, true
	}
	return nil, false
}

// getKey resolves a selector by walking shards.
func (r Reader) getKey(ctx context.Context, k kv.KeySelector) ([]byte, error) {
	version, err := r.shared.version(ctx)
	if err != nil {
		return nil, err
	}
	return r.getKeyAt(ctx, k, version)
}

// getKeyAt is the getKey loop at version.
func (r Reader) getKeyAt(ctx context.Context, k kv.KeySelector, version int64) ([]byte, error) {
	for {
		var key []byte
		var final bool
		if k, key, final = getKeyShortcut(k); final {
			return key, nil
		}
		backward := k.IsBackward()
		loc, err := r.db().Locate(ctx, k.Key, backward, r.params.provisional, ssGetKey)
		if err != nil {
			return nil, err
		}
		req := &wire.GetKey{Sel: k, Ctx: r.readCtx(version)}
		sel, err := lbRead(ctx, r, loc, ssGetKey, wire.Request[wire.Lb[kv.KeySelector]](req), equalSel)
		switch {
		case err == nil:
			if key, ok := getKeyResolved(sel); ok {
				return key, nil
			}
			k = sel
		case isLocationError(err):
			r.db().Locations.Invalidate(k.Key, backward)
			if err := sleep(ctx, wrongShardServerDelay); err != nil {
				return nil, err
			}
		default:
			return nil, err
		}
	}
}

// resolveKey is resolveKey.
func (r Reader) resolveKey(ctx context.Context, sel kv.KeySelector) ([]byte, error) {
	if sel.IsFirstGreaterOrEqual() {
		return sel.Key, nil
	}
	if sel.IsFirstGreaterThan() {
		return kv.KeyAfter(sel.Key), nil
	}
	return r.getKey(ctx, sel)
}

// GetRange reads a range at the snapshot.
func (r Reader) GetRange(ctx context.Context, begin, end kv.KeySelector, limits kv.Limits, reverse bool) (kv.RangeResult, error) {
	rr, err := r.GetRangeOut(ctx, begin, end, limits, reverse)
	if err != nil {
		return kv.RangeResult{}, err
	}
	return rr.Result(), nil
}

// RangeRead is a range read result with the selectors it actually used.
type RangeRead struct {
	Begin, End kv.KeySelector
	Out        RangeOut[kv.KeyValue]
}

// GetRangeOut reports the normalized selectors used for conflict ranges.
func (r Reader) GetRangeOut(ctx context.Context, begin, end kv.KeySelector, limits kv.Limits, reverse bool) (RangeRead, error) {
	ctx, done := r.readContext(ctx)
	defer done()
	if err := ctx.Err(); err != nil {
		return RangeRead{}, cancelErr(ctx)
	}
	return r.getRangeOut(ctx, begin, end, limits, reverse)
}

func (r Reader) getRangeOut(ctx context.Context, begin, end kv.KeySelector, limits kv.Limits, reverse bool) (RangeRead, error) {
	b, e, ok, err := normalizeRange(begin, end, limits)
	if err != nil {
		return RangeRead{}, err
	}
	if !ok {
		return RangeRead{Begin: begin, End: end}, nil
	}
	out, err := rangeRead(ctx, r, plainFamily(r), b, e, limits, reverse)
	if err != nil {
		return RangeRead{}, err
	}
	return RangeRead{Begin: b, End: e, Out: out}, nil
}

// GetMappedRange is a GetMappedKeyValues range read at the snapshot.
func (r Reader) GetMappedRange(ctx context.Context, begin, end kv.KeySelector, mapper []byte, limits kv.Limits, reverse bool) (RangeOut[wire.MappedRow], error) {
	ctx, done := r.readContext(ctx)
	defer done()
	if err := ctx.Err(); err != nil {
		return RangeOut[wire.MappedRow]{}, cancelErr(ctx)
	}
	b, e, ok, err := normalizeRange(begin, end, limits)
	if err != nil || !ok {
		return RangeOut[wire.MappedRow]{}, err
	}
	return rangeRead(ctx, r, mappedFamily(r, mapper), b, e, limits, reverse)
}

// rangeLocations returns the shards covering [begin, end), from the cache
// when it covers them (getKeyRangeLocations).
func (r Reader) rangeLocations(ctx context.Context, begin, end []byte, limit int, reverse bool, stream uint32) ([]Location, error) {
	if locs, ok := cachedLocations(r.db().Locations, begin, end, limit, reverse); ok {
		refresh := false
		for _, loc := range locs {
			if r.db().locationNeedsRefresh(loc, stream) {
				r.db().Locations.InvalidateRange(loc.Begin, loc.End)
				refresh = true
			}
		}
		if !refresh {
			return locs, nil
		}
	}
	locs, err := r.db().LocateRange(ctx, begin, end, limit, reverse, r.params.provisional)
	if err != nil {
		return nil, err
	}
	for i := range locs {
		locs[i] = clipLocation(locs[i], begin, end)
	}
	return locs, nil
}

func clipLocation(l Location, begin, end []byte) Location {
	if bytes.Compare(l.Begin, begin) < 0 {
		l.Begin = begin
	}
	if bytes.Compare(l.End, end) > 0 {
		l.End = end
	}
	return l
}

// cachedLocations returns the shards covering [begin, end) if the cache
// holds all of them, up to limit (getCachedLocations).
func cachedLocations(c *LocationCache, begin, end []byte, limit int, reverse bool) ([]Location, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var out []Location
	if reverse {
		key := end
		for {
			i := c.find(key, true)
			if i < 0 {
				return nil, false
			}
			e := &c.entries[i]
			l := Location{Begin: e.begin, End: e.end, Servers: e.servers}
			done := bytes.Compare(l.Begin, begin) <= 0
			out = append(out, clipLocation(l, begin, end))
			if done || len(out) == limit {
				return out, true
			}
			key = l.Begin
		}
	}
	key := begin
	for {
		i := c.find(key, false)
		if i < 0 {
			return nil, false
		}
		e := &c.entries[i]
		l := Location{Begin: e.begin, End: e.end, Servers: e.servers}
		done := bytes.Compare(l.End, end) >= 0
		out = append(out, clipLocation(l, begin, end))
		if done || len(out) == limit {
			return out, true
		}
		key = l.End
	}
}

// metricsReq is a WaitMetricsRequest whose reply is wrapped as a load-balanced reply.
type metricsReq struct{ *wire.WaitMetrics }

func (m metricsReq) Decode(msg []byte, t wire.Table) (wire.Lb[wire.StorageMetrics], error) {
	v, err := m.WaitMetrics.Decode(msg, t)
	return lbWrap(v), err
}

// splitReq is a SplitRangeRequest whose reply is wrapped as a load-balanced reply.
type splitReq struct{ *wire.SplitRange }

func (s splitReq) Decode(msg []byte, t wire.Table) (wire.Lb[[][]byte], error) {
	v, err := s.SplitRange.Decode(msg, t)
	return lbWrap(v), err
}

// watchReq is a WatchValueRequest whose reply is wrapped as a load-balanced reply.
type watchReq struct{ *wire.WatchValue }

func (w watchReq) Decode(msg []byte, t wire.Table) (wire.Lb[int64], error) {
	v, err := w.WatchValue.Decode(msg, t)
	return lbWrap(v), err
}

// shardJob is one shard's part of a multi-shard metrics request.
type shardJob struct {
	begin, end []byte
	servers    []wire.StorageServer
}

func shardJobs(locs []Location, begin, end []byte) []shardJob {
	jobs := make([]shardJob, len(locs))
	for i, l := range locs {
		b, e := l.Begin, l.End
		if i == 0 {
			b = begin
		}
		if i == len(locs)-1 {
			e = end
		}
		jobs[i] = shardJob{b, e, l.Servers}
	}
	return jobs
}

// fanOut runs fn for every job concurrently and returns the results in job
// order, or the first error (try_join_all).
func fanOut[T any](ctx context.Context, jobs []shardJob, fn func(context.Context, shardJob) (T, error)) ([]T, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	type res struct {
		i   int
		v   T
		err error
	}
	ch := make(chan res, len(jobs))
	for i, j := range jobs {
		go func() {
			v, err := fn(ctx, j)
			ch <- res{i, v, err}
		}()
	}
	out := make([]T, len(jobs))
	for range jobs {
		r := <-ch
		if r.err != nil {
			return nil, r.err
		}
		out[r.i] = r.v
	}
	return out, nil
}

// EstimatedRangeSize is getStorageMetricsLargeKeyRange: the summed byte estimate of [begin, end).
func (r Reader) EstimatedRangeSize(ctx context.Context, begin, end []byte) (int64, error) {
	ctx, done := r.readContext(ctx)
	defer done()
	for {
		locs, err := r.rangeLocations(ctx, begin, end, 1<<31-1, false, ssWaitMetrics)
		if err != nil {
			return 0, err
		}
		ms, err := fanOut(ctx, shardJobs(locs, begin, end), func(ctx context.Context, j shardJob) (wire.StorageMetrics, error) {
			req := metricsReq{&wire.WaitMetrics{
				Range: kv.KeyRange{Begin: j.begin, End: j.end},
				Max:   wire.StorageMetrics{Bytes: -1},
			}}
			return loadBalance(ctx, r.db(), j.servers, ssWaitMetrics, wire.Request[wire.Lb[wire.StorageMetrics]](req))
		})
		switch {
		case err == nil:
			var sum int64
			for _, m := range ms {
				sum += m.Bytes
			}
			return sum, nil
		case isLocationError(err):
			r.db().Locations.InvalidateRange(begin, end)
			if err := sleep(ctx, wrongShardServerDelay); err != nil {
				return 0, err
			}
		case err == fdberr.FutureVersion:
			if err := sleep(ctx, secs(futureVersionRetryDelay)); err != nil {
				return 0, err
			}
		default:
			return 0, err
		}
	}
}

// RangeSplitPoints is getRangeSplitPoints.
func (r Reader) RangeSplitPoints(ctx context.Context, begin, end []byte, chunk int64) ([][]byte, error) {
	ctx, done := r.readContext(ctx)
	defer done()
	for {
		locs, err := r.rangeLocations(ctx, begin, end, tooMany, false, ssGetRangeSplitPoints)
		if err != nil {
			return nil, err
		}
		replies, err := fanOut(ctx, shardJobs(locs, begin, end), func(ctx context.Context, j shardJob) ([][]byte, error) {
			req := splitReq{&wire.SplitRange{Range: kv.KeyRange{Begin: j.begin, End: j.end}, ChunkSize: chunk}}
			return loadBalance(ctx, r.db(), j.servers, ssGetRangeSplitPoints, wire.Request[wire.Lb[[][]byte]](req))
		})
		switch {
		case err == nil:
			out := [][]byte{begin}
			for i, rep := range replies {
				if i > 0 {
					out = append(out, locs[i].Begin)
				}
				out = append(out, rep...)
			}
			if !bytes.Equal(out[len(out)-1], end) {
				out = append(out, end)
			}
			return out, nil
		case isLocationError(err):
			r.db().Locations.InvalidateRange(begin, end)
			if err := sleep(ctx, wrongShardServerDelay); err != nil {
				return nil, err
			}
		default:
			return nil, err
		}
	}
}

// AddressesForKey returns the storage server addresses for key (getAddressesForKey).
func (r Reader) AddressesForKey(ctx context.Context, key []byte, includePort bool) ([]string, error) {
	ctx, done := r.readContext(ctx)
	defer done()
	if err := ctx.Err(); err != nil {
		return nil, cancelErr(ctx)
	}
	version, err := r.shared.version(ctx)
	if err != nil {
		return nil, err
	}
	// All three system-key reads must use this transaction's snapshot. The
	// current location cache only routes these reads; it cannot answer them.
	tags, err := r.getRangeOut(ctx, kv.LastLessOrEqual([]byte("\xff/serverTag/")),
		kv.FirstGreaterThan([]byte("\xff/serverTag0")), kv.NewLimits(tooMany, kv.Unlimited), false)
	if err != nil {
		return nil, err
	}
	if tags.Out.More || len(tags.Out.Data) >= tooMany {
		return nil, wire.ErrMalformed
	}
	ksKey := append([]byte("\xff/keyServers/"), key...)
	shard, err := r.getRangeOut(ctx, kv.LastLessOrEqual(ksKey), kv.FirstGreaterThan(ksKey),
		kv.NewLimits(1, kv.Unlimited), false)
	if err != nil {
		return nil, err
	}
	if len(shard.Out.Data) != 1 {
		return nil, wire.ErrMalformed
	}
	ids, err := wire.DecodeKeyServersValue(shard.Out.Data[0].Value, tags.Out.Data)
	if err != nil {
		return nil, err
	}
	out := make([]string, len(ids))
	for i, id := range ids {
		const prefix = "\xff/serverList/"
		listKey := make([]byte, len(prefix)+16)
		copy(listKey, prefix)
		binary.LittleEndian.PutUint64(listKey[len(prefix):], id.First)
		binary.LittleEndian.PutUint64(listKey[len(prefix)+8:], id.Second)
		value, err := r.getValue(ctx, listKey, version)
		if err != nil {
			return nil, err
		}
		s, err := wire.DecodeServerListValue(value)
		if err != nil {
			return nil, err
		}
		a := s.Base.Address(r.db().TLS)
		if includePort {
			out[i] = a.String()
		} else {
			out[i] = a.Addr.Addr().String()
		}
	}
	return out, nil
}

// RegisterWatch arms a watch of key whose value in this transaction's view
// is value (nil = absent): it fails with the commit's error, or watches
// storage from the commit (or read) version. The returned
// completion function synchronously releases the watch with the given result,
// cancelling only its own storage waiter. It is safe to repeat.
func (r Reader) RegisterWatch(key, value []byte) (<-chan error, func(error), error) {
	s := r.shared
	db := s.DB
	if err := db.Watches.acquire(); err != nil {
		return nil, nil, err
	}
	f := make(chan error, 1)
	ctx, cancel := context.WithCancel(db.ctx)
	var once sync.Once
	finish := func(err error) {
		once.Do(func() {
			cancel()
			db.Watches.release()
			f <- err
		})
	}
	context.AfterFunc(ctx, func() { finish(cancelErr(ctx)) })
	tags := r.params.readTags
	pw := pendingWatch{arm: func(version int64, err error) {
		if err != nil {
			finish(err)
			return
		}
		if ctx.Err() != nil {
			finish(cancelErr(ctx))
			return
		}
		if !db.addBackground() {
			finish(fdberr.OperationCancelled)
			return
		}
		go func() {
			defer db.wg.Done()
			// Storage watches spawned here are children of this registered
			// waiter, so their Add cannot race a zero-count Close wait.
			finish(db.Watches.watch(ctx, db, key, value, version, tags, r.params.readOptions.DebugID))
		}()
	}}
	s.watchMu.Lock()
	if s.ctx.Err() != nil {
		s.watchMu.Unlock()
		pw.arm(0, fdberr.TransactionCancelled)
		return f, finish, nil
	}
	s.watches = append(s.watches, pw)
	s.watchMu.Unlock()
	return f, finish, nil
}
