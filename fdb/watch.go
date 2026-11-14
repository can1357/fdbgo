package fdb

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
)

// Watch observes a key registered by Transaction.Watch. It is safe for
// concurrent use and retains its transaction until cancelled or reclaimed.
// A committed watch survives Transaction.Close. Unreachable watches release
// their resources automatically unless Done transferred ownership; Cancel
// releases them deterministically.
type Watch struct {
	state   *watchState
	owner   atomic.Pointer[Transaction]
	cleanup runtime.Cleanup
}

// watchState belongs to the watch worker, not the reclaimable public handle.
// It retains the transaction only when Done hands ownership to a channel.
type watchState struct {
	mu          sync.Mutex
	done        chan struct{}
	released    chan struct{}
	err         error
	ready       bool
	cancel      context.CancelFunc
	handedOff   bool
	pinnedOwner *Transaction
}

func newWatch(t *Transaction, wait func(context.Context) (struct{}, error), err error) *Watch {
	s := &watchState{done: make(chan struct{}), released: make(chan struct{})}
	w := &Watch{state: s}
	w.owner.Store(t)
	if wait == nil {
		s.err, s.ready = err, true
		close(s.released)
		close(s.done)
		return w
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	w.cleanup = runtime.AddCleanup(w, (*watchState).abandon, s)
	go func() {
		_, err := wait(ctx)
		cancel()
		// Cancel may hold mu while waiting for reservation release.
		close(s.released)
		s.mu.Lock()
		if !s.ready {
			s.err, s.ready = err, true
			s.pinnedOwner = nil
			close(s.done)
		}
		s.mu.Unlock()
	}()
	return w
}

func (s *watchState) stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ready {
		return
	}
	s.cancel()
	<-s.released
	s.err, s.ready = ErrOperationCancelled, true
	s.pinnedOwner = nil
	close(s.done)
}

func (s *watchState) abandon() {
	s.mu.Lock()
	cancel := !s.handedOff
	s.mu.Unlock()
	if cancel {
		s.stop()
	}
}

// Wait waits for the watch to fire or fail. Cancelling ctx returns ctx.Err()
// without cancelling the watch; another Wait can still collect its result.
func (w *Watch) Wait(ctx context.Context) error {
	defer runtime.KeepAlive(w)
	select {
	case <-w.state.done:
		return w.state.err
	default:
	}
	select {
	case <-w.state.done:
		return w.state.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Done returns a channel closed when the watch fires or fails. The channel
// pins a pending watch and its transaction until completion. To abandon it
// earlier, retain the Watch and call Cancel explicitly.
func (w *Watch) Done() <-chan struct{} {
	s := w.state
	s.mu.Lock()
	if !s.ready {
		s.handedOff, s.pinnedOwner = true, w.owner.Load()
	}
	s.mu.Unlock()
	runtime.KeepAlive(w)
	return s.done
}

// Cancel completes a pending watch with ErrOperationCancelled and releases
// its reservation before returning, without cancelling the transaction or
// other watches. Completed results are unchanged. Cancel is idempotent.
func (w *Watch) Cancel() {
	w.state.stop()
	w.cleanup.Stop()
	w.owner.Store(nil)
	runtime.KeepAlive(w)
}

// IsReady reports whether the watch has fired or failed, without waiting.
func (w *Watch) IsReady() bool {
	defer runtime.KeepAlive(w)
	select {
	case <-w.state.done:
		return true
	default:
		return false
	}
}
