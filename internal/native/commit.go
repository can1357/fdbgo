package native

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	mrand "math/rand/v2"
	"slices"
	"time"

	"github.com/can1357/fdbgo/internal/fdberr"
	"github.com/can1357/fdbgo/internal/kv"
	"github.com/can1357/fdbgo/internal/ryw"
	"github.com/can1357/fdbgo/internal/transport"
	"github.com/can1357/fdbgo/internal/wire"
)

// NativeTxn is one native transaction: the current attempt's shared state,
// options and retry backoff (Transaction).
type NativeTxn struct {
	// Shared is the current attempt.
	Shared *TxnShared
	// Opts are the options; call OptionsChanged after changing them.
	Opts NativeOptions

	parent  context.Context
	params  *readParams
	reader  *Reader // reads issued now; replaced, never mutated, when the attempt or options change
	backoff float64
}

// NewNativeTxn returns a transaction on db with opts.
func NewNativeTxn(db *DbContext, opts NativeOptions) *NativeTxn {
	return newNativeTxn(context.Background(), db, opts)
}

// newNativeTxn returns a transaction whose attempts end when parent does.
func newNativeTxn(parent context.Context, db *DbContext, opts NativeOptions) *NativeTxn {
	t := &NativeTxn{Opts: opts, parent: parent, backoff: defaultBackoff}
	t.Shared = t.newShared(db)
	t.params = t.Opts.readParams()
	t.reader = &Reader{shared: t.Shared, params: t.params}
	return t
}

func (t *NativeTxn) newShared(db *DbContext) *TxnShared {
	return newTxnShared(t.parent, db, &t.Opts)
}

// OptionsChanged applies the current options to reads issued from now on.
func (t *NativeTxn) OptionsChanged() {
	t.params = t.Opts.readParams()
	t.reader = &Reader{shared: t.Shared, params: t.params}
	t.Shared.paramsMu.Lock()
	t.Shared.grvp = grvParamsOf(&t.Opts)
	t.Shared.paramsMu.Unlock()
}

// Reader returns a reader for reads issued now. It is a pointer so passing
// it as a ryw.SnapshotReader allocates nothing.
func (t *NativeTxn) Reader() *Reader { return t.reader }

// Reset ends the current attempt (its reads fail
// with transaction_cancelled) and starts a fresh one keeping options and backoff.
func (t *NativeTxn) Reset() {
	t.Shared.BreakVersionstamp()
	t.Shared.end()
	t.Shared = t.newShared(t.Shared.DB)
	t.reader = &Reader{shared: t.Shared, params: t.params}
}

// Close ends the current attempt; the transaction must not be used afterwards.
func (t *NativeTxn) Close() { t.Shared.end() }

// StartCommit starts the read version request needed by plan before returning.
// Invalid plans and read-only commits without watches do not request a version.
func (t *NativeTxn) StartCommit(plan ryw.CommitPlan) {
	wrote, err := validateCommit(&t.Opts, plan)
	if err != nil {
		return
	}
	if wrote {
		t.Shared.startReadVersion(grvCausalReadRisky)
		return
	}
	t.Shared.watchMu.Lock()
	hasWatches := len(t.Shared.watches) != 0
	t.Shared.watchMu.Unlock()
	if hasWatches {
		t.Shared.StartReadVersion()
	}
}

// Commit commits the plan and publishes its outcome and watches.
func (t *NativeTxn) Commit(plan ryw.CommitPlan) error {
	shared := t.Shared
	opts := t.Opts
	opts.Tags = cloneTags(opts.Tags)
	return commitAttempt(shared.ctx, shared, &opts, plan)
}

// commitAttempt commits plan and publishes the outcome to shared, from sequential code.
func commitAttempt(ctx context.Context, shared *TxnShared, opts *NativeOptions, plan ryw.CommitPlan) error {
	version, stamp, wrote, err := commitImpl(ctx, shared, opts, plan)
	return publishCommit(ctx, shared, version, stamp, wrote, err)
}

// publishCommit records the commit's result in shared and arms or fails its
// watches. It only blocks for a read-only commit with watches whose read
// version is not known yet.
func publishCommit(ctx context.Context, shared *TxnShared, version int64, stamp [10]byte, wrote bool, err error) error {
	out := CommitOutcome{State: OutcomeFailed}
	watchVersion, watchErr := version, err
	if err == nil {
		out.State = OutcomeCommitted
		if wrote {
			// Preserve the actual commit version even if cancellation wins
			// publication of the transaction's result.
			shared.CommittedVersion.Store(version)
			out.Versionstamp, out.HasVersionstamp = stamp, true
		} else {
			shared.watchMu.Lock()
			hasWatches := len(shared.watches) != 0
			shared.watchMu.Unlock()
			if hasWatches {
				// Resolve an attempt-owned GRV before publishing success:
				// detached watches must not depend on a later attempt end.
				var rv ReadVersion
				rv, watchErr = shared.readVersionWait(ctx, 0)
				watchVersion = rv.Version
			}
		}
	}
	// Successful publication transfers all pending watches to this commit
	// before endWithError can claim them, just as commitAndWatch/setupWatches
	// transfers them out of the upstream transaction.
	shared.watchMu.Lock()
	if selected := shared.setOutcome(out); selected != nil {
		err = selected
	}
	ws := shared.watches
	shared.watches = nil
	shared.watchMu.Unlock()
	if err != nil {
		watchErr = err
	}
	for _, w := range ws {
		w.arm(watchVersion, watchErr)
	}
	return err
}

// OnError handles err: on a retryable error it resets the
// transaction and returns the backoff to wait; otherwise it returns err.
func (t *NativeTxn) OnError(err error) (time.Duration, error) {
	e, ok := err.(fdberr.Error)
	if !ok {
		return 0, err
	}
	switch e {
	case fdberr.Success:
		return 0, fdberr.ClientInvalidOperation
	case fdberr.NotCommitted, fdberr.CommitUnknownResult, fdberr.DatabaseLocked,
		fdberr.CommitProxyMemoryLimitExceeded, fdberr.GRVProxyMemoryLimitExceeded,
		fdberr.ProcessBehind, fdberr.BatchTransactionThrottled, fdberr.TagThrottled,
		fdberr.BlobGranuleRequestFailed, fdberr.ProxyTagThrottled,
		fdberr.TransactionThrottledHotShard, fdberr.TransactionRejectedRangeLocked:
		backoff := t.nextBackoff(e)
		t.Reset()
		return secs(backoff), nil
	case fdberr.TransactionTooOld, fdberr.FutureVersion:
		delay := min(futureVersionRetryDelay, t.Opts.MaxBackoff)
		t.Reset()
		return secs(delay), nil
	}
	return 0, e
}

// nextBackoff returns the backoff to wait before the next attempt and grows it.
func (t *NativeTxn) nextBackoff(code fdberr.Error) float64 {
	returned := t.backoff
	if code == fdberr.TagThrottled {
		db := t.Shared.DB
		db.throttleMu.Lock()
		returned = db.throttles.Backoff(t.Opts.Priority, t.Opts.Tags, returned)
		db.throttleMu.Unlock()
	}
	returned *= mrand.Float64()
	limit := t.Opts.MaxBackoff
	switch code {
	case fdberr.CommitProxyMemoryLimitExceeded, fdberr.GRVProxyMemoryLimitExceeded,
		fdberr.TransactionThrottledHotShard, fdberr.TransactionRejectedRangeLocked:
		limit = resourceConstrainedMaxBackoff
	}
	t.backoff = min(t.backoff*backoffGrowthRate, limit)
	return returned
}

const (
	// sizeofMutationRef is the fixed per-mutation overhead counted by commitSize.
	sizeofMutationRef = 56
	// sizeofKeyRangeRef is the fixed per-conflict-range overhead counted by commitSize.
	sizeofKeyRangeRef = 32
)

// commitSize is the transaction size checked against the size limit: the bytes
// plus fixed overhead of the mutations and both conflict range lists.
func commitSize(mutations []wire.Mutation, reads, writes []kv.KeyRange) int64 {
	n := 0
	for _, m := range mutations {
		n += sizeofMutationRef + len(m.Param1) + len(m.Param2)
	}
	for _, r := range reads {
		n += sizeofKeyRangeRef + len(r.Begin) + len(r.End)
	}
	for _, r := range writes {
		n += sizeofKeyRangeRef + len(r.Begin) + len(r.End)
	}
	return int64(n)
}

// selfConflictingKey returns a key that makes a transaction conflict with itself: \xff/SC/ + 16 random bytes.
func selfConflictingKey() []byte {
	k := make([]byte, 5+16)
	copy(k, "\xff/SC/")
	_, _ = rand.Read(k[5:])
	return k
}

func pointRange(k []byte) kv.KeyRange { return kv.KeyRange{Begin: k, End: kv.KeyAfter(k)} }

// intersects returns a key where a range of a overlaps a range of b, if any.
// Ordered inputs are swept without copying; unordered inputs are sorted in
// private copies. Empty and inverted ranges never intersect.
func intersects(a, b []kv.KeyRange) ([]byte, bool) {
	if len(a) == 0 || len(b) == 0 {
		return nil, false
	}
	compare := func(x, y kv.KeyRange) int { return bytes.Compare(x.Begin, y.Begin) }
	sorted := func(v []kv.KeyRange) []kv.KeyRange {
		if slices.IsSortedFunc(v, compare) {
			return v
		}
		out := slices.Clone(v)
		slices.SortFunc(out, compare)
		return out
	}
	a, b = sorted(a), sorted(b)
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		x, y := a[i], b[j]
		if bytes.Compare(x.Begin, x.End) >= 0 {
			i++
			continue
		}
		if bytes.Compare(y.Begin, y.End) >= 0 {
			j++
			continue
		}
		if bytes.Compare(x.Begin, y.End) < 0 && bytes.Compare(y.Begin, x.End) < 0 {
			if bytes.Compare(x.Begin, y.Begin) >= 0 {
				return x.Begin, true
			}
			return y.Begin, true
		}
		if bytes.Compare(x.End, y.End) <= 0 {
			i++
		} else {
			j++
		}
	}
	return nil, false
}

func versionstamp(version int64, batch uint16) [10]byte {
	var s [10]byte
	binary.BigEndian.PutUint64(s[:8], uint64(version))
	binary.BigEndian.PutUint16(s[8:], batch)
	return s
}

// commitImpl is the commit body: the commit version and versionstamp with
// wrote true, or wrote false for a read-only transaction.
func commitImpl(ctx context.Context, shared *TxnShared, opts *NativeOptions, plan ryw.CommitPlan) (int64, [10]byte, bool, error) {
	req, wrote, err := prepareCommit(opts, plan)
	if err != nil || !wrote {
		return 0, [10]byte{}, false, err
	}
	// Without reads, full causal consistency of the read version is unnecessary.
	rv, err := shared.readVersionWait(ctx, grvCausalReadRisky)
	if err != nil {
		return 0, [10]byte{}, false, err
	}
	req.ReadSnapshot = rv.Version
	if err := estimateTaggedCommitCosts(ctx, shared, opts, req); err != nil {
		return 0, [10]byte{}, false, err
	}
	start := transport.Now()
	ci, proxy, err := sendCommit(ctx, shared.DB, opts.UseProvisionalProxies, req)
	if err == nil && ci.Version != invalidVersion && opts.AutomaticIdempotency {
		// Expiration is best effort and belongs to the proxy that accepted
		// this commit, not whichever proxy is published when the reply arrives.
		if conn := shared.DB.Peers.Live(proxy.Address(shared.DB.TLS)); conn != nil {
			conn.Send(proxy.Adjusted(10).Token, wire.ExpireIdempotencyID{
				Version: ci.Version, BatchIndexHighByte: uint8(ci.BatchIndex >> 8),
			})
		}
	}
	return finishCommit(ctx, shared, opts, req, start, ci, err)
}

// validateCommit determines whether a plan writes and checks its options.
func validateCommit(opts *NativeOptions, plan ryw.CommitPlan) (bool, error) {
	if len(plan.WriteConflicts) == 0 && len(plan.Mutations) == 0 {
		return false, nil
	}
	if opts.ReadOnly {
		return false, fdberr.TransactionReadOnly
	}
	if commitSize(plan.Mutations, plan.ReadConflicts, plan.WriteConflicts) > opts.SizeLimit {
		return false, fdberr.TransactionTooLarge
	}
	return true, nil
}

// prepareCommit validates plan and builds its CommitTransactionRequest (all
// but the read snapshot); wrote is false for a read-only transaction.
func prepareCommit(opts *NativeOptions, plan ryw.CommitPlan) (*wire.Commit, bool, error) {
	wrote, err := validateCommit(opts, plan)
	if err != nil || !wrote {
		return nil, false, err
	}
	mutations := plan.Mutations
	reads := slices.Clip(plan.ReadConflicts)
	writes := slices.Clip(plan.WriteConflicts)
	var conflictKey []byte
	if opts.IdempotencyID != nil {
		conflictKey = append([]byte("\xff/SC/"), opts.IdempotencyID...)
	} else if !opts.CausalWriteRisky {
		if _, ok := intersects(writes, reads); !ok {
			conflictKey = selfConflictingKey()
		}
	}
	if conflictKey != nil {
		kr := pointRange(conflictKey)
		reads = append(reads, kr)
		writes = append(writes, kr)
	}
	var flags uint32
	if opts.LockAware {
		flags |= commitIsLockAware
	}
	if opts.FirstInBatch {
		flags |= commitFirstInBatch
	}
	if opts.BypassStorageQuota {
		flags |= commitBypassStorageQuota
	}
	return &wire.Commit{
		ReadConflicts:         reads,
		WriteConflicts:        writes,
		Mutations:             mutations,
		ReportConflictingKeys: opts.ReportConflictingKeys,
		Flags:                 flags,
		Tags:                  opts.Tags,
		IdempotencyID:         opts.IdempotencyID,
		DebugID:               opts.ReadOptions.DebugID,
	}, true, nil
}

// finishCommit handles the proxy's answer to req, sent at start. Only an
// unknown result (request_maybe_delivered, commit_unknown_result) blocks.
func finishCommit(ctx context.Context, shared *TxnShared, opts *NativeOptions, req *wire.Commit, start float64, ci wire.CommitReply, err error) (int64, [10]byte, bool, error) {
	var none [10]byte
	db := shared.DB
	reads, writes := req.ReadConflicts, req.WriteConflicts
	rv := req.ReadSnapshot
	switch {
	case err == nil && ci.Version != invalidVersion:
		db.UpdateCachedReadVersion(start, ci.Version)
		return ci.Version, versionstamp(ci.Version, ci.BatchIndex), true, nil
	case err == nil:
		shared.conflictMu.Lock()
		if ci.Conflicting != nil {
			var ranges []kv.KeyRange
			seen := make(map[int32]bool)
			for _, i := range ci.Conflicting {
				if !seen[i] {
					seen[i] = true
					if i >= 0 && int(i) < len(reads) {
						ranges = append(ranges, reads[i])
					}
				}
			}
			shared.conflicting, shared.hasConflicting = ranges, true
		} else {
			shared.conflicting, shared.hasConflicting = nil, false
		}
		shared.conflictMu.Unlock()
		return 0, none, false, fdberr.NotCommitted
	case err == fdberr.RequestMaybeDelivered || err == fdberr.CommitUnknownResult:
		if ctx.Err() != nil {
			return 0, none, false, cancelErr(ctx)
		}
		if !opts.CausalWriteRisky || opts.IdempotencyID != nil {
			// Make sure the original commit is no longer in flight by committing a conflicting one.
			if k, ok := intersects(writes, reads); ok {
				if err := commitDummy(ctx, db, opts, k); err != nil {
					return 0, none, false, err
				}
			}
			if opts.IdempotencyID != nil {
				v, batch, found, err := determineCommitStatus(ctx, db, opts, rv, rv+maxWriteTransactionLifeVersions, opts.IdempotencyID)
				if err != nil {
					return 0, none, false, err
				}
				if !found {
					return 0, none, false, fdberr.TransactionTooOld
				}
				return v, versionstamp(v, batch), true, nil
			}
		}
		return 0, none, false, fdberr.CommitUnknownResult
	default:
		return 0, none, false, err
	}
}

// sendCommit sends req to a commit proxy; a proxy change while it is in
// flight makes the result unknown.
func sendCommit(ctx context.Context, db *DbContext, provisionalOK bool, req *wire.Commit) (wire.CommitReply, wire.Endpoint, error) {
	info, pctx, release, err := db.withProxies(ctx, provisionalOK)
	if err != nil {
		return wire.CommitReply{}, wire.Endpoint{}, err
	}
	defer release()
	ci, proxy, err := basicLoadBalance(pctx, db, info.Commit, 0, wire.Request[wire.CommitReply](req), true)
	if err != nil && proxiesChanged(pctx) {
		return ci, proxy, fdberr.RequestMaybeDelivered
	}
	return ci, proxy, err
}

// commitDummy commits a transaction conflicting with key so an earlier
// commit touching it can no longer land (commitDummyTransaction).
func commitDummy(ctx context.Context, db *DbContext, opts *NativeOptions, key []byte) error {
	o := *opts
	o.CausalWriteRisky = true
	o.LockAware = true
	o.ReadOnly = false
	o.RawAccess = true
	o.IdempotencyID = nil
	o.AutomaticIdempotency = false
	tr := newNativeTxn(ctx, db, o)
	defer tr.Close()
	for {
		r := pointRange(key)
		plan := ryw.CommitPlan{ReadConflicts: []kv.KeyRange{r}, WriteConflicts: []kv.KeyRange{r}}
		err := commitAttempt(tr.Shared.ctx, tr.Shared, &tr.Opts, plan)
		if err == nil {
			return nil
		}
		delay, err := tr.OnError(err)
		if err != nil {
			return err
		}
		if err := sleep(ctx, delay); err != nil {
			return err
		}
	}
}

// determineCommitStatus looks the idempotency id up in \xff\x02/idmp/ to
// learn whether a commit landed (determineCommitStatus).
func determineCommitStatus(ctx context.Context, db *DbContext, opts *NativeOptions, minVersion, maxVersion int64, id []byte) (int64, uint16, bool, error) {
	o := *opts
	o.LockAware = true
	o.ReadOnly = true
	o.IdempotencyID = nil
	tr := newNativeTxn(ctx, db, o)
	defer tr.Close()
	for {
		v, batch, found, err := func() (int64, uint16, bool, error) {
			reader := tr.Reader()
			actx := tr.Shared.ctx
			expired, err := reader.getValueWait(actx, idempotencyExpiredVersionKey)
			if err != nil {
				return 0, 0, false, err
			}
			if expired != nil && decodeExpiredVersion(expired) >= minVersion {
				return 0, 0, false, fdberr.CommitUnknownResultFatal
			}
			b := binary.BigEndian.AppendUint64(slices.Clone(idempotencyKeysBegin), uint64(minVersion))
			e := binary.BigEndian.AppendUint64(slices.Clone(idempotencyKeysBegin), uint64(maxVersion+1))
			rows, err := reader.getRangeOut(actx, kv.FirstGreaterOrEqual(b), kv.FirstGreaterOrEqual(e), kv.NewLimits(tooMany, kv.Unlimited), false)
			if err != nil {
				return 0, 0, false, err
			}
			for i := range rows.Out.Data {
				if v, batch, ok := idempotencyKVContains(&rows.Out.Data[i], id); ok {
					return v, batch, true, nil
				}
			}
			return 0, 0, false, nil
		}()
		if err == nil {
			return v, batch, found, nil
		}
		delay, err := tr.OnError(err)
		if err != nil {
			return 0, 0, false, err
		}
		if err := sleep(ctx, delay); err != nil {
			return 0, 0, false, err
		}
	}
}

// idempotencyKVContains is kvContainsIdempotencyId: the key is the prefix,
// the big-endian commit version and the high batch byte; the value is a
// versioned BinaryWriter stream: protocol version, timestamp, then
// [u8 len][id][u8 low batch byte] entries.
func idempotencyKVContains(row *kv.KeyValue, id []byte) (int64, uint16, bool) {
	if len(row.Value) < 16 {
		return 0, 0, false
	}
	v := row.Value[16:] // u64 protocol version + i64 timestamp
	for len(v) > 0 {
		n := int(v[0])
		rest := v[1:]
		if len(rest) < n+1 {
			return 0, 0, false
		}
		candidate, low := rest[:n], rest[n]
		if bytes.Equal(candidate, id) {
			key, ok := bytes.CutPrefix(row.Key, idempotencyKeysBegin)
			if !ok || len(key) < 9 {
				return 0, 0, false
			}
			version := int64(binary.BigEndian.Uint64(key[:8]))
			return version, uint16(key[8])<<8 | uint16(low), true
		}
		v = rest[n+1:]
	}
	return 0, 0, false
}
