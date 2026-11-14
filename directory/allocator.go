package directory

import (
	"context"
	"encoding/binary"
	"math/rand/v2"
	"sync"

	"github.com/can1357/fdbgo/fdb"
	"github.com/can1357/fdbgo/subspace"
	"github.com/can1357/fdbgo/tuple"
)

// allocator is the high contention allocator: it hands out small integers, unique across all
// clients of its subspace, that become directory prefixes.
//
// It works over windows of candidates. counters holds one key,
// counters:windowStart, counting the allocations made in the current window;
// recent holds recent:candidate for every candidate handed out in it. A
// client reads the current window, advancing it (clearing both subspaces
// below the new start) once it is half full, then claims a random candidate
// in it by writing recent:candidate, retrying when the candidate was taken or
// the window moved.
type allocator struct {
	counters subspace.Subspace
	recent   subspace.Subspace
}

// allocationMu serializes the read-then-write steps of allocators that may
// share a transaction and subspace. Blocking reads stay inside the critical
// section so another allocator cannot change their read-your-writes view.
var allocationMu sync.Mutex

var oneLE = []byte{1, 0, 0, 0, 0, 0, 0, 0}

func newAllocator(ss subspace.Subspace) allocator {
	return allocator{counters: sub(ss, int64(0)), recent: sub(ss, int64(1))}
}

// windowSize returns the candidate window size for windows starting at
// start: small windows keep early prefixes short, larger ones reduce
// contention once many prefixes exist.
func windowSize(start int64) int64 {
	switch {
	case start < 255:
		return 64
	case start < 65535:
		return 1024
	default:
		return 8192
	}
}

// latestWindowStart reads the start of the current window from the last
// counters key (0 if there is none).
func (a allocator) latestWindowStart(ctx context.Context, tr fdb.ReadTransaction, r fdb.Range, opts fdb.RangeOptions) (int64, error) {
	result, err := tr.GetRange(ctx, r, opts)
	if err != nil {
		return 0, err
	}
	kvs, err := result.Slice(ctx)
	if err != nil {
		return 0, err
	}
	if len(kvs) == 0 {
		return 0, nil
	}
	t, err := a.counters.Unpack(kvs[0].Key)
	if err != nil {
		return 0, err
	}
	if len(t) != 1 {
		return 0, ErrInvalidMetadata
	}
	start, ok := t[0].(int64)
	if !ok {
		return 0, ErrInvalidMetadata
	}
	return start, nil
}

// allocate returns an integer that no other allocation on the same subspace
// has returned or will return, nearly as small as possible.
func (a allocator) allocate(ctx context.Context, tr *fdb.Transaction) (int64, error) {
	begin, end := a.counters.FDBRangeKeys()
	countersRange := fdb.SelectorRange{Begin: fdb.FirstGreaterOrEqual(begin), End: fdb.FirstGreaterThan(end)}
	latestOpts := fdb.RangeOptions{Limit: 1, Reverse: true}
	for {
		start, err := a.latestWindowStart(ctx, tr.Snapshot(), countersRange, latestOpts)
		if err != nil {
			return 0, err
		}
		window, err := a.findWindow(ctx, tr, &start)
		if err != nil {
			return 0, err
		}
		candidate, ok, err := a.claim(ctx, tr, countersRange, latestOpts, start, window)
		if err != nil || ok {
			return candidate, err
		}
	}
}

// findWindow counts an allocation in the window at *start, advancing *start
// past windows that are at least half full, and returns the window's size.
func (a allocator) findWindow(ctx context.Context, tr *fdb.Transaction, start *int64) (int64, error) {
	advanced := false
	for {
		countersStart := sub(a.counters, *start).Bytes()
		allocationMu.Lock()
		var err error
		if advanced {
			tr.ClearRange(fdb.KeyRange{Begin: a.counters.Bytes(), End: countersStart})
			err = tr.Options().SetNextWriteNoWriteConflictRange()
			if err == nil {
				tr.ClearRange(fdb.KeyRange{Begin: a.recent.Bytes(), End: sub(a.recent, *start).Bytes()})
			}
		}
		var v []byte
		if err == nil {
			// Count this allocation before reading its counter.
			tr.Atomic(fdb.MutationTypeAdd, countersStart, oneLE)
			v, err = tr.Snapshot().Get(ctx, countersStart)
		}
		allocationMu.Unlock()
		if err != nil {
			return 0, err
		}
		var count int64
		if v != nil {
			if len(v) != 8 {
				return 0, ErrInvalidMetadata
			}
			count = int64(binary.LittleEndian.Uint64(v))
		}
		window := windowSize(*start)
		if count*2 < window {
			return window, nil
		}
		*start += window
		advanced = true
	}
}

// claim tries random candidates in [start, start+window) until one is free
// (ok) or the window has advanced (not ok: find the window again).
func (a allocator) claim(ctx context.Context, tr *fdb.Transaction, countersRange fdb.Range, latestOpts fdb.RangeOptions, start, window int64) (int64, bool, error) {
	for {
		// The window was less than half full in the snapshot, so this
		// should take about two tries; under contention, or when the window
		// advances, this transaction may still conflict later.
		candidate := start + rand.Int64N(window)
		key := pack(a.recent, tuple.Tuple{candidate})
		allocationMu.Lock()
		currentStart, err := a.latestWindowStart(ctx, tr.Snapshot(), countersRange, latestOpts)
		var v []byte
		if err == nil {
			v, err = tr.Get(ctx, key)
		}
		// Both reads must see the state before the candidate is marked.
		// Keep the one-shot conflict option adjacent to that write.
		if err == nil {
			err = tr.Options().SetNextWriteNoWriteConflictRange()
		}
		if err == nil {
			tr.Set(key, []byte{})
		}
		allocationMu.Unlock()
		if err != nil {
			return 0, false, err
		}
		if currentStart > start {
			return 0, false, nil
		}
		if v == nil {
			if err := tr.AddWriteConflictKey(key); err != nil {
				return 0, false, err
			}
			return candidate, true, nil
		}
	}
}
