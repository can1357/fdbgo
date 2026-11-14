package ryw

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/can1357/fdbgo/internal/fdberr"
)

// AttemptEnd is the end of one transaction attempt: the error that ends
// it, set at most once. Operations racing an attempt select on Done, or
// register an OnEnd hook, and then fail with Err.
type AttemptEnd struct {
	code    atomic.Int64
	done    chan struct{}
	mu      sync.Mutex
	hooks   []*EndHook
	hookBuf [8]*EndHook // backs hooks for a typical attempt's reads
}

// EndHook is a callback registered with AttemptEnd.OnEnd.
type EndHook struct {
	fn        func(error)
	released  atomic.Bool
	immediate bool
}

func (h *EndHook) run(err error) { h.fn(err) }

// Release drops the hook once its operation finished. A hook already running
// is not stopped, so fn must tolerate a late call.
func (h *EndHook) Release() { h.released.Store(true) }

func newAttemptEnd() *AttemptEnd {
	a := &AttemptEnd{done: make(chan struct{})}
	a.hooks = a.hookBuf[:0]
	return a
}

// Abort ends only this attempt with err unless it already ended.
func (a *AttemptEnd) Abort(err fdberr.Error) { a.set(err) }

// Done returns a channel closed when the attempt ends.
func (a *AttemptEnd) Done() <-chan struct{} { return a.done }

// Err returns the error that ended the attempt, or nil while it runs.
func (a *AttemptEnd) Err() error {
	if c := a.code.Load(); c != 0 {
		return fdberr.Error(c)
	}
	return nil
}

// OnEnd arranges for fn to run with the ending error when the attempt ends:
// on a goroutine of its own then, or at once if it already ended. Release
// the hook when the operation it guards finishes.
func (a *AttemptEnd) OnEnd(fn func(err error)) *EndHook {
	h := &EndHook{fn: fn}
	a.register(h)
	return h
}

// OnEndImmediate registers native cleanup that neither re-enters RYW/client
// transaction methods nor waits for operations. It may run with the RYW lock
// held; general callbacks must use OnEnd.
func (a *AttemptEnd) OnEndImmediate(fn func(error)) *EndHook {
	h := &EndHook{fn: fn, immediate: true}
	a.register(h)
	return h
}

// register is OnEnd for a hook the caller allocated.
func (a *AttemptEnd) register(h *EndHook) {
	a.mu.Lock()
	if c := a.code.Load(); c != 0 {
		a.mu.Unlock()
		h.run(fdberr.Error(c))
		return
	}
	if len(a.hooks) == cap(a.hooks) {
		// Amortized cleanup of finished operations.
		kept := a.hooks[:0]
		for _, o := range a.hooks {
			if !o.released.Load() {
				kept = append(kept, o)
			}
		}
		clear(a.hooks[len(kept):])
		a.hooks = kept
	}
	a.hooks = append(a.hooks, h)
	a.mu.Unlock()
}

// Resolve serializes publication of an operation's outcome with the attempt's
// end. fn receives the ending error if cancellation won. fn must not call back
// into AttemptEnd or the transaction.
func (a *AttemptEnd) Resolve(fn func(error)) {
	a.mu.Lock()
	defer a.mu.Unlock()
	fn(a.Err())
}

// set ends the attempt with err; false if it had already ended. General hooks
// run on a new goroutine because set may hold the transaction lock. Native
// cleanup explicitly registered as nonblocking can finish on the caller.
func (a *AttemptEnd) set(err fdberr.Error) bool {
	a.mu.Lock()
	if !a.code.CompareAndSwap(0, int64(err)) {
		a.mu.Unlock()
		return false
	}
	close(a.done)
	hooks := a.hooks
	a.hooks = nil
	a.mu.Unlock()
	live := hooks[:0]
	for _, h := range hooks {
		if !h.released.Load() {
			if h.immediate {
				h.run(err)
			} else {
				live = append(live, h)
			}
		}
	}
	hooks = live
	if len(hooks) > 0 {
		go func() {
			for _, h := range hooks {
				if !h.released.Load() {
					h.run(err)
				}
			}
		}()
	}
	return true
}

// readTracker tracks the reads and watch setups of one attempt that
// commit waits for.
type readTracker struct {
	mu      sync.Mutex
	nextSeq uint64
	// added records a tracked read, even when it completed synchronously,
	// or a watch setup that outlived its synchronous part.
	added bool
	// firstErr is the failure of the earliest issued failed read, errSeq its sequence.
	errSeq   uint64
	firstErr error
	running  int
	waiters  *readWait
}

// pendingRead is one tracked operation.
type pendingRead struct {
	t      *readTracker
	seq    uint64
	record bool // its failure fails commit (false for watch setups)
	async  bool
}

// begin starts tracking an operation; record says whether its failure fails commit.
func (t *readTracker) begin(record bool) pendingRead {
	t.mu.Lock()
	t.nextSeq++
	s := t.nextSeq
	t.added = t.added || record
	t.mu.Unlock()
	return pendingRead{t: t, seq: s, record: record}
}

func (t *readTracker) wasAdded() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.added
}

// hold makes commit wait for the operation from the start. Watch setups are
// held without marking a read as added.
func (p *pendingRead) hold() {
	t := p.t
	t.mu.Lock()
	t.running++
	t.mu.Unlock()
	p.async = true
}

// markAdded records that a watch setup outlived its synchronous part.
func (p *pendingRead) markAdded() {
	p.t.mu.Lock()
	p.t.added = true
	p.t.mu.Unlock()
}

// finish records the outcome. aborted operations (ended with the attempt)
// record no error.
func (p *pendingRead) finish(err error, aborted bool) {
	recordErr := err != nil && p.record && !aborted
	if !recordErr && !p.async {
		return
	}
	t := p.t
	t.mu.Lock()
	if recordErr {
		t.added = true
		if t.firstErr == nil || p.seq < t.errSeq {
			t.firstErr, t.errSeq = err, p.seq
		}
	}
	if p.async {
		t.running--
	}
	for link := &t.waiters; *link != nil; {
		w := *link
		included := p.seq <= w.through
		if included && p.async {
			w.remaining--
		}
		if w.remaining == 0 || included && recordErr {
			*link = w.next
			w.next = nil
			close(w.done)
		} else {
			link = &w.next
		}
	}
	t.mu.Unlock()
}

// readWait captures the pending set at issue time. Later reads neither extend
// its lifetime nor contribute errors. Completion removes it from the tracker;
// failures are selected only from its captured issue sequence.
type readWait struct {
	t         *readTracker
	next      *readWait
	through   uint64
	remaining int
	done      chan struct{}
}

// snapshot returns no wait when all issued reads have finished or one failed.
// The caller holds the transaction lock so no newly issued operation can enter
// the captured set between begin and hold.
func (t *readTracker) snapshot() (*readWait, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.running == 0 || t.firstErr != nil {
		return nil, t.firstErr
	}
	w := &readWait{
		t:         t,
		next:      t.waiters,
		through:   t.nextSeq,
		remaining: t.running,
		done:      make(chan struct{}),
	}
	t.waiters = w
	return w, nil
}

func (w *readWait) wait(ctx context.Context, end *AttemptEnd) error {
	if err := end.Err(); err != nil {
		return err
	}
	select {
	case <-w.done:
		return w.result()
	default:
	}
	select {
	case <-w.done:
		return w.result()
	case <-end.done:
		return end.Err()
	case <-ctx.Done():
		return ctx.Err()
	}
}

// result preserves earliest-issued failure selection without allowing reads
// issued after the snapshot to poison a completed wait.
func (w *readWait) result() error {
	w.t.mu.Lock()
	defer w.t.mu.Unlock()
	if w.t.errSeq <= w.through {
		return w.t.firstErr
	}
	return nil
}

// runRead executes a blocking body on its wait caller. The operation must
// already be held in the tracker, and its issue-time view and read version
// must be pinned. The caller must run every issued wait exactly once, even
// after cancellation, so the body unwinds before tracking is released.
func runRead[T any](ctx context.Context, end *AttemptEnd, p *pendingRead, reader SnapshotReader, body func(context.Context) (T, error)) (T, error) {
	ctx, release := readAttemptContext(ctx, end, reader)
	if release != nil {
		defer release()
	}
	v, err := body(ctx)
	end.Resolve(func(ended error) {
		if ended != nil {
			var zero T
			v, err = zero, ended
		}
		p.finish(err, ended != nil)
	})
	return v, err
}

// readAttemptContext binds scalar-only readers to their captured attempt.
// Native readers already combine caller and bound-attempt cancellation.
func readAttemptContext(ctx context.Context, end *AttemptEnd, reader SnapshotReader) (context.Context, func()) {
	bound, ok := reader.(interface{ BoundAttempt() *AttemptEnd })
	if ok && bound.BoundAttempt() == end {
		return ctx, nil
	}
	ctx, cancel := context.WithCancel(ctx)
	hook := end.OnEnd(func(error) { cancel() })
	return ctx, func() {
		hook.Release()
		cancel()
	}
}

// readResult is sent once by a blocking read body.
type readResult[T any] struct {
	value T
	err   error
}

// launch runs the body until it either completes from the local view or first
// needs I/O. Its stack remains live while the blocking reader runs; no request
// or cache work is replayed. p must already be held before the Ryw lock is
// released, so a concurrent commit sees the operation.
func launch[T any](c *readCtx, p *pendingRead, it rywIter, body func(*readCtx, rywIter) (T, error)) (T, func(ctx context.Context) (T, error), error) {
	ready := make(chan struct{})
	result := make(chan readResult[T], 1)
	ctx, cancel := context.WithCancel(context.Background())
	c.ctx = ctx
	hook := c.end.OnEnd(func(error) { cancel() })
	var first sync.Once
	var startErr error
	c.firstIO = func() error {
		first.Do(func() {
			if startErr = c.lock(); startErr != nil {
				return
			}
			c.reader.StartReadVersion()
			c.r.mu.Unlock()
			close(ready)
		})
		if startErr != nil {
			return startErr
		}
		return c.end.Err()
	}
	go func() {
		v, err := body(c, it)
		c.end.Resolve(func(ended error) {
			aborted := ended != nil && (c.survivesEnd == nil || !c.survivesEnd())
			if aborted {
				var zero T
				v, err = zero, ended
			}
			if p != nil {
				p.finish(err, aborted)
			}
			result <- readResult[T]{v, err}
		})
		hook.Release()
		cancel()
	}()
	select {
	case res := <-result:
		return res.value, nil, res.err
	case <-ready:
		return *new(T), func(ctx context.Context) (T, error) {
			stop := context.AfterFunc(ctx, cancel)
			defer stop()
			if ctx.Err() != nil {
				cancel()
			}
			select {
			case res := <-result:
				return res.value, res.err
			case <-c.end.Done():
				select {
				case res := <-result:
					return res.value, res.err
				default:
				}
				if c.survivesEnd != nil && c.survivesEnd() {
					res := <-result
					return res.value, res.err
				}
				return *new(T), c.end.Err()
			}
		}, nil
	}
}
