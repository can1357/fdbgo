package native

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/can1357/fdbgo/internal/fdberr"
	"github.com/can1357/fdbgo/internal/transport"
	"github.com/can1357/fdbgo/internal/wire"
)

// maxLaggingRequests matches MAX_LAGGING_REQUESTS_OUTSTANDING. Exhausting a
// collection cancels it before starting a fresh collection, as upstream does.
const maxLaggingRequests = 100_000

// laggingRequests owns losing hedges until they reply or are cancelled. Its
// zero value is ready to use; close prevents registration and joins all cleanup.
type laggingRequests struct {
	mu     sync.Mutex
	ctx    context.Context
	cancel context.CancelFunc
	count  int
	closed bool
	wg     sync.WaitGroup
}

func (l *laggingRequests) add() (context.Context, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil, false
	}
	if l.ctx == nil || l.count > maxLaggingRequests {
		if l.cancel != nil {
			l.cancel()
		}
		l.ctx, l.cancel = context.WithCancel(context.Background())
		l.count = 0
	}
	l.count++
	l.wg.Add(1)
	return l.ctx, true
}

func (l *laggingRequests) done(ctx context.Context) {
	l.mu.Lock()
	if ctx == l.ctx {
		l.count--
	}
	l.mu.Unlock()
	l.wg.Done()
}

func (l *laggingRequests) close() {
	l.mu.Lock()
	l.closed = true
	if l.cancel != nil {
		l.cancel()
	}
	l.mu.Unlock()
	l.wg.Wait()
}

// lbOutcome classifies one load-balanced reply.
type lbOutcome uint8

const (
	lbDone lbOutcome = iota
	lbRetry
	lbFail
)

// lbReply is one storage reply as it arrives from the transport.
type lbReply[T any] struct {
	v   wire.Lb[T]
	err error
}

// lbAttempt is one in-flight load-balanced request: its replica, model
// ticket and reply channel.
type lbAttempt[T any] struct {
	alt      int
	ticket   transport.Ticket
	triedAll bool
	ended    bool
	ctx      context.Context
	cancel   context.CancelFunc
	ch       chan lbReply[T]
}

// abandon drops a request whose waiter went away (e.g. its context was cancelled): it
// stops counting as outstanding without recording a latency.
func (a *lbAttempt[T]) abandon() {
	if !a.ended {
		a.ended = true
		a.cancel()
		a.ticket.Release()
	}
}

// lag transfers a losing hedge to the database's bounded collection. Its
// ticket remains outstanding until the actual reply updates the model.
func (r *lbRun[T]) lag(a *lbAttempt[T]) {
	if a.ended {
		return
	}
	c := r.c
	ctx, ok := c.lagging.add()
	if !ok {
		a.abandon()
		<-a.ch
		return
	}
	go func() {
		defer c.lagging.done(ctx)
		select {
		case rep := <-a.ch:
			a.process(c, rep)
			return
		case <-ctx.Done():
		case <-a.ctx.Done():
		case <-c.ctx.Done():
		}
		a.abandon()
		// Cancellation also removes the transport waiter. Join its completion
		// before releasing this collection's ownership.
		<-a.ch
	}()
}

// lbRun is the state of one load-balanced storage request, guided by a
// queue model and safe to resend to another replica.
type lbRun[T any] struct {
	c       *DbContext
	servers []wire.StorageServer
	stream  uint32
	req     wire.Request[wire.Lb[T]]

	n, best, next, startAlt, startDistance int
	race                                   bool
	secondDelay                            time.Duration
	triedAll                               bool
	attempts                               int
	backoff                                float64
}

// init plans a run in place, for callers embedding the run in larger state.
func (r *lbRun[T]) init(c *DbContext, servers []wire.StorageServer, stream uint32, req wire.Request[wire.Lb[T]]) error {
	n := len(servers)
	if n == 0 {
		return fdberr.AllAlternativesFailed
	}
	sorted, countBest := c.byDistance(servers)
	*r = lbRun[T]{c: c, servers: sorted, stream: stream, req: req, n: n}
	plan := c.Model.Plan(n, countBest, func(i int) uint64 { return r.ep(i).Token.First }, r.healthy)
	r.best, r.next = plan.Best, plan.Next
	r.race, r.secondDelay = plan.Race, plan.SecondDelay
	r.startAlt = r.next
	r.startDistance = (r.best + n - r.startAlt) % n
	return nil
}

func (r *lbRun[T]) ep(i int) wire.Endpoint {
	ep := r.servers[i].Base
	if r.stream != 0 {
		ep = ep.Adjusted(r.stream)
	}
	return ep
}

func (r *lbRun[T]) healthy(i int) bool { return !r.c.Failures.Failed(r.ep(i), r.c.TLS) }

// choose picks the next replica to try, skipping failed ones and the one
// first is already waiting on.
func (r *lbRun[T]) choose(first *lbAttempt[T]) (int, bool) {
	n := r.n
	for range n {
		var useAlt int
		switch {
		case r.next == r.startAlt:
			useAlt = r.best
		case (r.next+n-r.startAlt)%n <= r.startDistance:
			useAlt = (r.next + n - 1) % n
		default:
			useAlt = r.next
		}
		if r.healthy(useAlt) && (first == nil || first.alt != useAlt) {
			return useAlt, true
		}
		r.next = (r.next + 1) % n
		if r.next == r.startAlt {
			r.triedAll = true
		}
	}
	return 0, false
}

// attempted counts a failed attempt and grows the backoff once every replica was tried.
func (r *lbRun[T]) attempted() {
	r.attempts++
	if r.attempts >= r.n {
		r.backoff = transport.GrowBackoff(r.backoff)
	}
}

// advance ends one round of the loop.
func (r *lbRun[T]) advance() {
	r.next = (r.next + 1) % r.n
	if r.next == r.startAlt {
		r.triedAll = true
	}
	r.race = false
}

// start issues the request to replica alt after backoff seconds.
func (r *lbRun[T]) start(ctx context.Context, alt int, backoff float64) *lbAttempt[T] {
	ep := r.ep(alt)
	ctx, cancel := context.WithCancel(ctx)
	a := &lbAttempt[T]{
		alt: alt, ticket: r.c.Model.Start(ep.Token.First), triedAll: r.triedAll,
		ctx: ctx, cancel: cancel, ch: make(chan lbReply[T], 1),
	}
	c, req := r.c, r.req
	go func() {
		if backoff > 0 {
			if err := sleep(ctx, secs(backoff)); err != nil {
				a.ch <- lbReply[T]{err: err}
				return
			}
		}
		v, err := call(ctx, c, ep, req)
		a.ch <- lbReply[T]{v, err}
	}()
	return a
}

// processLBReply classifies one reply and ends its queue-model ticket.
func processLBReply[T any](c *DbContext, rep lbReply[T], ticket *transport.Ticket, triedAll bool) (T, lbOutcome, error) {
	var zero T
	var code fdberr.Error
	penalty := -1.0
	var value T
	hasValue := false
	if rep.err == nil {
		penalty, value, code = lbSplit(rep.v)
		hasValue = code == 0
	} else {
		var fe fdberr.Error
		switch {
		case errors.As(rep.err, &fe):
			code = fe
		case isFatalRPC(rep.err):
			c.Model.End(ticket, false, false, -1)
			return zero, lbFail, rpcToFDB(rep.err)
		case errors.Is(rep.err, context.Canceled) || errors.Is(rep.err, context.DeadlineExceeded):
			c.Model.End(ticket, false, false, -1)
			return zero, lbFail, fdberr.OperationCancelled
		default:
			code = fdberr.RequestMaybeDelivered
		}
	}
	maybeDelivered := code == fdberr.BrokenPromise || code == fdberr.RequestMaybeDelivered
	received := hasValue || (!maybeDelivered && code != fdberr.ProcessBehind)
	futureVersion := code == fdberr.FutureVersion || code == fdberr.ProcessBehind
	c.Model.End(ticket, received, futureVersion, penalty)
	if code == fdberr.ServerOverloaded {
		return zero, lbRetry, nil
	}
	if hasValue {
		return value, lbDone, nil
	}
	if received {
		return zero, lbFail, code
	}
	if triedAll && code == fdberr.ProcessBehind {
		return zero, lbFail, fdberr.ProcessBehind
	}
	return zero, lbRetry, nil
}

// process consumes one attempt's reply, ending its ticket.
func (a *lbAttempt[T]) process(c *DbContext, rep lbReply[T]) (T, lbOutcome, error) {
	a.ended = true
	a.cancel()
	return processLBReply(c, rep, &a.ticket, a.triedAll)
}

// wait waits for a's reply or the end of ctx.
func (r *lbRun[T]) wait(ctx context.Context, a *lbAttempt[T]) (lbReply[T], error) {
	select {
	case rep := <-a.ch:
		return rep, nil
	case <-ctx.Done():
		a.abandon()
		return lbReply[T]{}, cancelErr(ctx)
	}
}

// run is the load-balancing loop: the value and the index (into r.servers)
// of the replica that served it. first is an outstanding request a second
// one may race.
func (r *lbRun[T]) run(ctx context.Context, first *lbAttempt[T]) (T, int, error) {
	var zero T
	c := r.c
	for {
		alt, chosen := r.choose(first)
		a := first
		first = nil
		switch {
		case !chosen && a == nil:
			// Subscribe and check under the monitor lock so recovery cannot be
			// lost between observing failure and waiting for notification.
			if err := c.Failures.WaitForRecovery(ctx, r.n, r.ep, c.TLS, c.Failures.AllAlternativesFailedDelay()); err != nil {
				if ctx.Err() != nil {
					err = cancelErr(ctx)
				}
				return zero, 0, err
			}
			r.attempts = 0
		case !chosen:
			rep, err := r.wait(ctx, a)
			if err != nil {
				return zero, 0, err
			}
			v, oc, err := a.process(c, rep)
			switch oc {
			case lbDone:
				return v, a.alt, nil
			case lbFail:
				return zero, 0, err
			}
		case a != nil:
			b := r.start(ctx, alt, r.backoff)
			firstPending := true
		race:
			for {
				var aCh chan lbReply[T]
				if firstPending {
					aCh = a.ch
				}
				select {
				case rep := <-aCh:
					firstPending = false
					v, oc, err := a.process(c, rep)
					switch oc {
					case lbDone:
						r.lag(b)
						return v, a.alt, nil
					case lbFail:
						r.lag(b)
						return zero, 0, err
					}
				case rep := <-b.ch:
					v, oc, err := b.process(c, rep)
					switch oc {
					case lbDone:
						r.lag(a)
						return v, b.alt, nil
					case lbFail:
						r.lag(a)
						return zero, 0, err
					}
					break race
				case <-ctx.Done():
					a.abandon()
					b.abandon()
					return zero, 0, cancelErr(ctx)
				}
			}
			if firstPending {
				// A retryable hedge does not invalidate the still-pending primary.
				first = a
			}
			r.attempted()
		case !r.race:
			// With no hedge, the caller can own the RPC and its queue ticket.
			ep := r.ep(alt)
			ticket := c.Model.Start(ep.Token.First)
			if r.backoff > 0 {
				if err := sleep(ctx, secs(r.backoff)); err != nil {
					ticket.Release()
					return zero, 0, err
				}
			}
			reply, callErr := call(ctx, c, ep, r.req)
			if ctx.Err() != nil {
				ticket.Release()
				return zero, 0, cancelErr(ctx)
			}
			c.Model.FirstReplied()
			v, oc, err := processLBReply(c, lbReply[T]{reply, callErr}, &ticket, r.triedAll)
			switch oc {
			case lbDone:
				return v, alt, nil
			case lbFail:
				return zero, 0, err
			}
			r.attempted()
		default:
			// Race the reply against the second-request delay.
			a := r.start(ctx, alt, r.backoff)
			t := time.NewTimer(r.secondDelay)
			var rep lbReply[T]
			got := false
			select {
			case rep = <-a.ch:
				got = true
			case <-t.C:
			case <-ctx.Done():
				t.Stop()
				a.abandon()
				return zero, 0, cancelErr(ctx)
			}
			t.Stop()
			if !got && !c.Model.TakeSecondBudget() {
				var err error
				if rep, err = r.wait(ctx, a); err != nil {
					return zero, 0, err
				}
				got = true
			}
			if got {
				c.Model.FirstReplied()
				v, oc, err := a.process(c, rep)
				switch oc {
				case lbDone:
					return v, alt, nil
				case lbFail:
					return zero, 0, err
				}
			} else {
				// Go around the loop to race a second replica against this one.
				first = a
			}
			r.attempted()
		}
		r.advance()
	}
}

// loadBalance sends a storage request to the best replica, racing a second
// one when the first is slow and moving on past failed replicas.
func loadBalance[T any](ctx context.Context, c *DbContext, servers []wire.StorageServer, stream uint32, req wire.Request[wire.Lb[T]]) (T, error) {
	var r lbRun[T]
	if err := r.init(c, servers, stream, req); err != nil {
		var zero T
		return zero, err
	}
	v, _, err := r.run(ctx, nil)
	return v, err
}

// loadBalanceChecked is loadBalance, then (for
// enable_replica_consistency_check) re-reads from other replicas and fails if
// any disagrees (replicaComparison). required is the
// consistency_check_required_replicas option: -1 all replicas, -2 best
// effort, 0 none, n that many.
func loadBalanceChecked[T any](ctx context.Context, c *DbContext, servers []wire.StorageServer, stream uint32, req wire.Request[wire.Lb[T]], required int64, equal func(a, b T) bool) (T, error) {
	const allReplicas = -1
	var zero T
	var r lbRun[T]
	if err := r.init(c, servers, stream, req); err != nil {
		return zero, err
	}
	value, src, err := r.run(ctx, nil)
	if err != nil {
		return zero, err
	}
	sorted := r.servers
	if len(sorted) <= 1 || required == 0 {
		return value, nil
	}
	var candidates []wire.Endpoint
	for i := range sorted {
		if i == src {
			continue
		}
		ep := r.ep(i)
		if !c.Failures.Failed(ep, c.TLS) {
			candidates = append(candidates, ep)
		} else if required == allReplicas {
			return zero, fdberr.UnreachableStorageReplica
		}
	}
	rand.Shuffle(len(candidates), func(i, j int) { candidates[i], candidates[j] = candidates[j], candidates[i] })
	if required > 0 && int64(len(candidates)) > required {
		candidates = candidates[:required]
	}
	type result struct {
		rep      wire.Lb[T]
		err      error
		timedOut bool
	}
	results := make([]chan result, len(candidates))
	for i, ep := range candidates {
		ch := make(chan result, 1)
		results[i] = ch
		go func() {
			tctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			rep, err := call(tctx, c, ep, req)
			ch <- result{rep, err, err != nil && tctx.Err() == context.DeadlineExceeded && ctx.Err() == nil}
		}()
	}
	var errs, mismatches, successes int64
	var code fdberr.Error
	for _, ch := range results {
		var res result
		select {
		case res = <-ch:
		case <-ctx.Done():
			return zero, cancelErr(ctx)
		}
		switch {
		case res.timedOut:
			code = fdberr.TimedOut
		case res.err != nil:
			errs++
			if fe, ok := rpcToFDB(res.err).(fdberr.Error); ok {
				code = fe
			}
		default:
			_, v, ec := lbSplit(res.rep)
			if ec != 0 {
				errs++
				code = ec
			} else {
				if !equal(v, value) {
					mismatches++
				}
				successes++
			}
		}
		if code == fdberr.WrongShardServer {
			return zero, fdberr.WrongShardServer
		}
		if successes == required {
			break
		}
	}
	timeoutsOrErrors := errs > 0 || code == fdberr.TimedOut
	if mismatches > 0 {
		return zero, fdberr.StorageReplicaComparisonError
	}
	if (timeoutsOrErrors && required == allReplicas) ||
		(required > 0 && successes != required && int64(len(candidates)) >= required) {
		if required == allReplicas {
			return zero, code
		}
		return zero, fdberr.UnreachableStorageReplica
	}
	return value, nil
}
