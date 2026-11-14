package ryw

import (
	"context"

	"github.com/can1357/fdbgo/internal/fdberr"
	"github.com/can1357/fdbgo/internal/kv"
)

// checkRead is the synchronous checks every read starts with (deferred
// error, used during commit, ended attempt).
func (r *Ryw) checkRead() error {
	if r.deferredErr != nil {
		return r.deferredErr
	}
	if r.usedDuringCommit() {
		return fdberr.UsedDuringCommit
	}
	return r.reset.Err()
}

// seesWrites reports whether a read sees this transaction's writes (snapshot
// reads may not: snapshot_ryw_disable).
func (r *Ryw) seesWrites(snapshot bool) bool {
	return !(snapshot && r.options.SnapshotRywEnabled <= 0)
}

// serializable reports whether a read adds read conflict ranges.
func (r *Ryw) serializable(snapshot bool) bool { return r.seesWrites(snapshot) && !snapshot }

// startRead pins what a read observes: reads that see writes get the
// current write map version, the others none of this transaction's writes.
func (r *Ryw) startRead(snapshot, bypassUnreadable bool) (rywIter, bool) {
	writes := noWrites
	if r.seesWrites(snapshot) {
		writes = r.writes.frozen()
	}
	return newRywIter(r.cache.tree(), writes, bypassUnreadable), r.serializable(snapshot)
}

// Get returns, for keys outside the special key space, the value of
// key as this transaction sees it, nil if absent. The result must not be
// modified. Every non-nil wait must be called exactly once, even if its result
// is discarded: it runs the blocking read on its caller.
func (r *Ryw) Get(reader SnapshotReader, key []byte, snapshot bool) ([]byte, func(ctx context.Context) ([]byte, error), error) {
	r.mu.Lock()
	var plans []pointRead
	value, err := r.prepareGet(reader, key, snapshot, &plans, 1)
	r.mu.Unlock()
	if len(plans) == 0 {
		return value, nil, err
	}
	return nil, plans[0].wait, nil
}

// GetMany captures every key before starting snapshot I/O. Readers supporting
// point batches receive all unresolved keys in one call; local results never
// leave the transaction. Every non-nil wait must be called exactly once,
// including after a validation error or cancellation.
func (r *Ryw) GetMany(reader SnapshotReader, keys [][]byte, snapshot bool) ([][]byte, func(context.Context) ([][]byte, error), error) {
	values := make([][]byte, len(keys))
	var plans []pointRead
	var firstErr error
	firstIndex := len(keys)
	r.mu.Lock()
	for i, key := range keys {
		n := len(plans)
		value, err := r.prepareGet(reader, key, snapshot, &plans, len(keys)-i)
		values[i] = value
		if err != nil && i < firstIndex {
			firstErr, firstIndex = err, i
		}
		if len(plans) != n {
			plans[n].index = i
		}
	}
	r.mu.Unlock()
	if len(plans) == 0 {
		if firstErr != nil {
			return nil, nil, firstErr
		}
		return values, nil, nil
	}
	return nil, func(ctx context.Context) ([][]byte, error) {
		return waitPointReads(ctx, reader, plans, values, firstErr, firstIndex)
	}, nil
}

// prepareGet runs under the transaction lock. Only unresolved reads need plan
// storage; capacity reserves the remaining batch in one allocation.
func (r *Ryw) prepareGet(reader SnapshotReader, key []byte, snapshot bool, plans *[]pointRead, capacity int) ([]byte, error) {
	if err := r.checkRead(); err != nil {
		return nil, err
	}
	exists, err := ValidateGet(&r.options, key)
	if err != nil || !exists {
		return nil, err
	}
	key, after := dupAfter(key)
	add := r.serializable(snapshot)
	var p pendingRead
	var merge *pointReadMerge
	if !r.seesWrites(snapshot) || r.writes.pointUnmodified(key) {
		if v, known := r.cache.point(key); known {
			if add {
				r.addPointReadConflictAfter(key, after)
			}
			return v, nil
		}
		p = r.reading.begin(true)
	} else {
		it, _ := r.startRead(snapshot, r.options.BypassUnreadable)
		p = r.reading.begin(true)
		it.skip(key)
		isKV, err := it.isKV()
		var value []byte
		known := isKV
		if err == nil && isKV {
			var row kv.KeyValue
			row, _, err = it.kv()
			value = row.Value
		} else if err == nil {
			known, err = it.isEmptyRange()
		}
		if err != nil || known {
			if err == nil && add {
				err = r.updateConflictMapKey(key, it.writeIter(), false)
			}
			p.finish(err, false)
			return value, err
		}
		merge = &pointReadMerge{
			it: it,
			c: readCtx{
				r: r, reader: reader, generation: r.generation,
				end: r.reset, maxKey: r.options.MaxReadKey(),
			},
		}
	}
	if *plans == nil {
		*plans = make([]pointRead, 0, capacity)
	}
	*plans = append(*plans, pointRead{
		pending: p, reader: reader, end: r.reset,
		r: r, generation: r.generation, after: after, add: add, merge: merge,
	})
	(*plans)[len(*plans)-1].pending.hold()
	reader.StartReadVersion()
	return nil, nil
}

// GetKey resolves sel against this transaction's issue-time view.
func (r *Ryw) GetKey(reader SnapshotReader, sel kv.KeySelector, snapshot bool) ([]byte, func(ctx context.Context) ([]byte, error), error) {
	r.mu.Lock()
	if err := r.checkRead(); err != nil {
		r.mu.Unlock()
		return nil, nil, err
	}
	if err := ValidateGetKey(&r.options, &sel); err != nil {
		r.mu.Unlock()
		return nil, nil, err
	}
	sel.Key = dup(sel.Key)
	it, add := r.startRead(snapshot, false)
	p := r.reading.begin(true)
	p.hold()
	c := r.newCtx(reader)
	r.mu.Unlock()
	return launch(c, &p, it, func(c *readCtx, it rywIter) ([]byte, error) { return getKeyBody(c, it, sel, add) })
}

// GetRange reads from this transaction's issue-time view.
func (r *Ryw) GetRange(reader SnapshotReader, begin, end kv.KeySelector, limits kv.Limits, snapshot, reverse bool) (kv.RangeResult, func(ctx context.Context) (kv.RangeResult, error), error) {
	r.mu.Lock()
	if err := r.checkRead(); err != nil {
		r.mu.Unlock()
		return kv.RangeResult{}, nil, err
	}
	begin.Normalize()
	end.Normalize()
	begin.Key, end.Key = dup(begin.Key), dup(end.Key)
	read, err := ValidateGetRange(&r.options, &begin, &end, limits)
	if err != nil || !read {
		r.mu.Unlock()
		return kv.RangeResult{}, nil, err
	}
	it, add := r.startRead(snapshot, false)
	p := r.reading.begin(true)
	p.hold()
	c := r.newCtx(reader)
	r.mu.Unlock()
	return launch(c, &p, it, func(c *readCtx, it rywIter) (kv.RangeResult, error) {
		return getRangeBody(c, it, begin, end, limits, reverse, add)
	})
}

// Watch tracks setup until it is armed or the attempt ends; an armed watch
// survives the attempt. A local write can fire it at any time.
func (r *Ryw) Watch(reader WatchReader, key []byte) (struct{}, func(ctx context.Context) (struct{}, error), error) {
	r.mu.Lock()
	err := r.checkRead()
	if err == nil && r.options.ReadYourWritesDisabled {
		err = fdberr.WatchesDisabled
	}
	if err == nil {
		err = validateWatch(&r.options, key)
	}
	if err != nil {
		r.mu.Unlock()
		return struct{}{}, nil, err
	}
	key = dup(key)
	cell := &watchCell{fired: make(chan struct{}), done: r.reading.begin(false)}
	cell.done.hold()
	if r.watchMap == nil {
		r.watchMap = make(map[string][]*watchCell)
	}
	r.watchMap[string(key)] = append(r.watchMap[string(key)], cell)
	it, add := r.startRead(false, r.options.BypassUnreadable)
	c := r.newCtx(reader)
	c.survivesEnd = cell.armed.Load
	r.mu.Unlock()
	setup := func(c *readCtx, it rywIter) (<-chan error, error) {
		val, err := getBody(c, it, key, add)
		if err == nil && !cell.isFired() {
			fire := false
			err = c.with(func(*Ryw) error {
				cell.mu.Lock()
				cell.value, cell.hasValue = val, true
				fire = cell.hasSet && !optEqual(cell.set, val)
				cell.mu.Unlock()
				return nil
			})
			if err == nil && fire {
				cell.fire()
			}
		}
		if cell.isFired() {
			cell.finishSetup(cell.err, false)
			return nil, cell.err
		}
		var ch <-chan error
		if err == nil {
			err = c.with(func(*Ryw) error {
				if cell.isFired() {
					return nil
				}
				var registerErr error
				cell.mu.Lock()
				ch, cell.complete, registerErr = reader.RegisterWatch(key, val)
				if registerErr == nil {
					cell.armed.Store(true)
					if cell.isFired() {
						cell.complete(cell.err)
					}
				}
				cell.mu.Unlock()
				return registerErr
			})
		}
		cell.finishSetup(err, c.end.Err() != nil && !cell.armed.Load())
		return ch, err
	}
	ch, pending, err := launch(c, nil, it, setup)
	if err != nil {
		cell.finishSetup(err, c.end.Err() != nil)
		return struct{}{}, nil, err
	}
	if pending != nil {
		cell.done.markAdded()
	}
	wait := func(ctx context.Context) (struct{}, error) {
		if pending != nil {
			type setupResult struct {
				ch  <-chan error
				err error
			}
			done := make(chan setupResult, 1)
			go func() { v, e := pending(ctx); done <- setupResult{v, e} }()
			select {
			case <-cell.fired:
				return struct{}{}, cell.err
			case <-ctx.Done():
				cell.finish(fdberr.OperationCancelled)
				return struct{}{}, cell.err
			case <-c.end.Done():
				if cell.armed.Load() {
					res := <-done
					ch, err = res.ch, res.err
				} else {
					cell.finishSetup(c.end.Err(), true)
					return struct{}{}, c.end.Err()
				}
			case res := <-done:
				ch, err = res.ch, res.err
			}
		}
		if err != nil {
			return struct{}{}, err
		}
		select {
		case <-cell.fired:
			return struct{}{}, cell.err
		case e := <-ch:
			return struct{}{}, e
		case <-ctx.Done():
			cell.finish(fdberr.OperationCancelled)
			return struct{}{}, cell.err
		}
	}
	if cell.isFired() {
		return struct{}{}, nil, cell.err
	}
	return struct{}{}, wait, nil
}

// Commit marks the attempt committing immediately, then waits for tracked reads.
func (r *Ryw) Commit() (CommitPlan, func(ctx context.Context) (CommitPlan, error), error) {
	r.mu.Lock()
	if err := r.beginCommit(); err != nil {
		r.mu.Unlock()
		return CommitPlan{}, nil, err
	}
	end, gen := r.reset, r.generation
	pending, err := r.reading.snapshot()
	if pending == nil {
		var plan CommitPlan
		if err == nil {
			plan, err = r.buildCommitPlan()
		}
		r.mu.Unlock()
		return plan, nil, err
	}
	r.mu.Unlock()
	return CommitPlan{}, func(ctx context.Context) (CommitPlan, error) {
		if err := pending.wait(ctx, end); err != nil {
			return CommitPlan{}, err
		}
		r.mu.Lock()
		defer r.mu.Unlock()
		if r.generation != gen {
			if err := end.Err(); err != nil {
				return CommitPlan{}, err
			}
			return CommitPlan{}, fdberr.TransactionCancelled
		}
		if err := r.reset.Err(); err != nil {
			return CommitPlan{}, err
		}
		return r.buildCommitPlan()
	}, nil
}

// NativeRead issues the supplied native operation under the transaction lock.
// Every non-nil wait must be called once, even if its result is discarded.
// The wait runs directly on its caller and must honor context cancellation.
func NativeRead[T any](r *Ryw, start func(RywOptions, func([]byte, []byte)) (T, func(ctx context.Context) (T, error), error)) (T, func(context.Context) (T, error), error) {
	r.mu.Lock()
	if err := r.checkRead(); err != nil {
		r.mu.Unlock()
		return *new(T), nil, err
	}
	v, wait, err := start(r.options, r.addNativeReadConflict)
	if err != nil {
		r.mu.Unlock()
		return v, nil, err
	}
	p := r.reading.begin(true)
	if wait == nil {
		p.finish(nil, false)
		r.mu.Unlock()
		return v, nil, nil
	}
	p.hold()
	end := r.reset
	r.mu.Unlock()
	return v, func(ctx context.Context) (T, error) {
		return runRead(ctx, end, &p, nil, wait)
	}, nil
}

// PendingReads captures the tracked reads and watch setups issued so far.
// It returns no wait when they have finished or a captured read already failed.
// Otherwise the wait ends when they finish, a captured read fails, this attempt
// ends, or its context is cancelled. Later reads never extend the captured set
// or contribute errors, and resetting the transaction never retargets it.
func (r *Ryw) PendingReads() (wait func(context.Context) error, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	end := r.reset
	if err := end.Err(); err != nil {
		return nil, err
	}
	pending, err := r.reading.snapshot()
	if pending == nil {
		return nil, err
	}
	return func(ctx context.Context) error {
		return pending.wait(ctx, end)
	}, nil
}

// CheckMappedRange performs the checks of a mapped range read (after
// special-key routing); false means an empty result. Mapped reads are
// serializable only and need read-your-writes enabled. begin and end are
// normalized as by ValidateGetRange.
func (r *Ryw) CheckMappedRange(begin, end *kv.KeySelector, limits kv.Limits, snapshot bool) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.checkRead(); err != nil {
		return false, err
	}
	read, err := ValidateGetRange(&r.options, begin, end, limits)
	if err != nil || !read {
		return false, err
	}
	if snapshot || r.options.ReadYourWritesDisabled {
		return false, fdberr.UnsupportedOperation
	}
	return true, nil
}

// AddMappedRangeConflicts, after a mapped range read, adds the conflict
// ranges of the primary and secondary reads, failing with
// get_mapped_range_reads_your_writes if any of them touches this
// transaction's writes. Keys are retained.
func (r *Ryw) AddMappedRangeConflicts(begin, end kv.KeySelector, reverse bool, primary RangeExtent, secondaries []MappedSecondary) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.addMappedRangeConflicts(begin, end, reverse, primary, secondaries)
}

// AddMappedRangeConflictsForAttempt rejects results from an ended attempt.
func (r *Ryw) AddMappedRangeConflictsForAttempt(attempt *AttemptEnd, begin, end kv.KeySelector, reverse bool, primary RangeExtent, secondaries []MappedSecondary) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := attempt.Err(); err != nil {
		return err
	}
	if r.reset != attempt {
		return fdberr.TransactionCancelled
	}
	return r.addMappedRangeConflicts(begin, end, reverse, primary, secondaries)
}

func (r *Ryw) addMappedRangeConflicts(begin, end kv.KeySelector, reverse bool, primary RangeExtent, secondaries []MappedSecondary) error {
	it := r.writes.iter()
	if err := r.addRangeConflict(begin, end, reverse, &primary, &it, true); err != nil {
		return err
	}
	for i := range secondaries {
		s := &secondaries[i]
		if s.IsValue {
			it.skip(s.Key)
			if err := r.updateConflictMapKey(s.Key, &it, true); err != nil {
				return err
			}
			continue
		}
		if err := r.addRangeConflict(s.Begin, s.End, false, &s.Result, &it, true); err != nil {
			return err
		}
	}
	return nil
}
