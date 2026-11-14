package native

import (
	"context"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/can1357/fdbgo/internal/fdberr"
	"github.com/can1357/fdbgo/internal/wire"
)

// defaultMaxWatches is the default cap on outstanding watches.
const defaultMaxWatches = 10_000

// Watches is the database-wide watch bookkeeping (watchMap,
// outstandingWatches, maxOutstandingWatches).
type Watches struct {
	outstanding atomic.Int64
	max         atomic.Int64
	mu          sync.Mutex
	m           map[string]*watchMeta
}

// watchMeta is one coalesced storage watch on a key.
type watchMeta struct {
	value   []byte
	version int64
	// done is closed once err holds the outcome; guarded by Watches.mu until then.
	done   chan struct{}
	err    error
	fired  bool
	cancel context.CancelFunc
	// waiters counts subscribers; the last departing waiter cancels storage.
	waiters int
}

func newWatches() *Watches {
	w := &Watches{m: make(map[string]*watchMeta)}
	w.max.Store(defaultMaxWatches)
	return w
}

// SetMax applies the max_watches database option.
func (w *Watches) SetMax(n int64) { w.max.Store(n) }

// acquire counts a new watch (increaseWatchCounter): too_many_watches past the limit.
func (w *Watches) acquire() error {
	if w.outstanding.Add(1) > w.max.Load() {
		w.outstanding.Add(-1)
		return fdberr.TooManyWatches
	}
	return nil
}

func (w *Watches) release() { w.outstanding.Add(-1) }

// fireLocked completes m with err unless it already fired; callers hold w.mu.
func fireLocked(m *watchMeta, err error) {
	if m.fired {
		return
	}
	m.fired = true
	m.err = err
	m.cancel()
	close(m.done)
}

// waitLocked joins a coalesced watch, releasing w.mu before waiting. The
// last subscriber removes the metadata and cancels the storage long-poll.
func (w *Watches) waitLocked(ctx context.Context, key []byte, m *watchMeta) error {
	m.waiters++
	w.mu.Unlock()
	defer func() {
		w.mu.Lock()
		defer w.mu.Unlock()
		m.waiters--
		if m.waiters == 0 && !m.fired {
			if w.m[string(key)] == m {
				delete(w.m, string(key))
			}
			fireLocked(m, fdberr.OperationCancelled)
		}
	}()
	select {
	case <-m.done:
		return m.err
	case <-ctx.Done():
		return cancelErr(ctx)
	}
}

// watch waits for key to differ from value (nil = absent) after version
// (watchValueMap / getWatchFuture).
func (w *Watches) watch(ctx context.Context, db *DbContext, key, value []byte, version int64, tags [][]byte, debugID wire.UID) error {
	if ctx.Err() != nil {
		return cancelErr(ctx)
	}
	w.mu.Lock()
	m := w.m[string(key)]
	switch {
	case m != nil && equalValue(m.value, value):
		if version > m.version {
			m.version = version
		}
		return w.waitLocked(ctx, key, m)
	case m != nil && version > m.version:
		// A newer watch with a different value: the old one is satisfied, restart.
		fireLocked(m, nil)
		nm := w.spawnLocked(db, key, value, version, tags, debugID)
		return w.waitLocked(ctx, key, nm)
	case m != nil && version == m.version:
		// Same version, different value: one of them is already stale.
		w.mu.Unlock()
		return w.sameVersionDiffValue(ctx, db, key, value, version, tags, debugID)
	case m != nil:
		// An older watch with a different value has already changed.
		w.mu.Unlock()
		return nil
	default:
		nm := w.spawnLocked(db, key, value, version, tags, debugID)
		return w.waitLocked(ctx, key, nm)
	}
}

// sameVersionDiffValue reads the key to decide which watch is stale.
func (w *Watches) sameVersionDiffValue(ctx context.Context, db *DbContext, key, value []byte, version int64, tags [][]byte, debugID wire.UID) error {
	opts := DefaultNativeOptions()
	opts.RawAccess = true
	opts.ReadOpts().DebugID = debugID
	tr := newNativeTxn(ctx, db, opts)
	defer tr.Close()
	var current []byte
	for {
		v, err := tr.Reader().getValueWait(tr.Shared.ctx, key)
		if err == nil {
			current = v
			break
		}
		delay, err := tr.OnError(err)
		if err != nil {
			return err
		}
		if err := sleep(ctx, delay); err != nil {
			return err
		}
	}
	w.mu.Lock()
	if m := w.m[string(key)]; m != nil && m.version == version && !equalValue(m.value, current) {
		fireLocked(m, nil)
		delete(w.m, string(key))
	}
	if !equalValue(current, value) {
		w.mu.Unlock()
		return nil
	}
	if m := w.m[string(key)]; m != nil && equalValue(m.value, value) {
		// The current watch carries this value: share it.
		return w.waitLocked(ctx, key, m)
	}
	nm := w.spawnLocked(db, key, value, version, tags, debugID)
	return w.waitLocked(ctx, key, nm)
}

// spawnLocked starts the storage long-poll for a coalesced watch
// (watchStorageServerResp) and maps key to it; callers hold w.mu.
func (w *Watches) spawnLocked(db *DbContext, key, value []byte, version int64, tags [][]byte, debugID wire.UID) *watchMeta {
	ctx, cancel := context.WithCancel(db.ctx)
	m := &watchMeta{value: value, version: version, done: make(chan struct{}), cancel: cancel}
	w.m[string(key)] = m
	db.wg.Add(1)
	go func() {
		defer db.wg.Done()
		for {
			w.mu.Lock()
			cur := version
			if mm := w.m[string(key)]; mm != nil {
				cur = mm.version
			}
			w.mu.Unlock()
			v, err := watchValue(ctx, db, key, value, cur, tags, debugID)
			w.mu.Lock()
			if m.fired {
				w.mu.Unlock()
				return
			}
			switch {
			case db.ctx.Err() != nil:
				if w.m[string(key)] == m {
					delete(w.m, string(key))
				}
				fireLocked(m, fdberr.OperationCancelled)
			case err == nil:
				if still := w.m[string(key)]; still != nil && v < still.version {
					w.mu.Unlock()
					continue
				}
				delete(w.m, string(key))
				fireLocked(m, nil)
			case err == fdberr.FutureVersion:
				w.mu.Unlock()
				continue
			default:
				delete(w.m, string(key))
				fireLocked(m, err)
			}
			w.mu.Unlock()
			return
		}
	}()
	return m
}

// waitForCommittedVersion confirms that a storage watch's reply is committed.
// It uses immediate, non-provisional GRVs, restarting on proxy changes and
// delaying on versions still in flight or transient GRV overload errors.
func waitForCommittedVersion(ctx context.Context, db *DbContext, version int64) (int64, error) {
	// grvErrorRetryDelay is the pause after a transient GRV error.
	const grvErrorRetryDelay = 5 * time.Second
	req := &wire.GetReadVersion{Flags: PriorityImmediate.GRVFlag()}
	for {
		if ctx.Err() != nil {
			return 0, cancelErr(ctx)
		}
		info, pctx, release, err := db.withProxies(ctx, false)
		if err != nil {
			return 0, err
		}
		rv, _, err := basicLoadBalance(pctx, db, info.GRV, 0, wire.Request[wire.ReadVersionReply](req), false)
		changed := proxiesChanged(pctx)
		release()
		if changed {
			continue
		}
		delay := secs(futureVersionRetryDelay)
		switch err {
		case nil:
			if rv.Version >= version {
				return rv.Version, nil
			}
		case fdberr.BatchTransactionThrottled, fdberr.GRVProxyMemoryLimitExceeded:
			delay = time.Duration(float64(grvErrorRetryDelay) * (0.9 + 0.2*rand.Float64()))
		default:
			return 0, err
		}
		if err := sleep(ctx, delay); err != nil {
			return 0, err
		}
	}
}

// watchValue runs one storage watch until the value differs at or after
// version (watchValue).
func watchValue(ctx context.Context, db *DbContext, key, value []byte, version int64, tags [][]byte, debugID wire.UID) (int64, error) {
	ver := version
	for {
		loc, err := db.Locate(ctx, key, false, false, ssWatchValue)
		if err != nil {
			return 0, err
		}
		req := watchReq{&wire.WatchValue{Key: key, Value: value, Version: ver, Tags: tags, DebugID: debugID}}
		v, err := loadBalance(ctx, db, loc.Servers, ssWatchValue, wire.Request[wire.Lb[int64]](req))
		switch {
		case err == nil:
			// Confirm the reply's version is committed (waitForCommittedVersion).
			committed, err := waitForCommittedVersion(ctx, db, v)
			if err != nil {
				return 0, err
			}
			if gap := committed - v; gap >= 0 && gap < 50_000_000 {
				return v, nil
			}
			ver = committed
		case isLocationError(err):
			db.Locations.Invalidate(key, false)
			if err := sleep(ctx, wrongShardServerDelay); err != nil {
				return 0, err
			}
		case err == fdberr.WatchCancelled || err == fdberr.ProcessBehind:
			if err := sleep(ctx, watchPollingTime); err != nil {
				return 0, err
			}
		case err == fdberr.TimedOut:
			if err := sleep(ctx, secs(futureVersionRetryDelay)); err != nil {
				return 0, err
			}
		default:
			if err := sleep(ctx, secs(futureVersionRetryDelay)); err != nil {
				return 0, err
			}
			return 0, err
		}
	}
}
