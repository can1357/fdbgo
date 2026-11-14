package transport

// Replica selection for storage reads and a client-side failure monitor.
//
// Each storage endpoint carries a load slot: smoothed outstanding
// requests weighted by the server's self-reported penalty, last latency, and
// a future-version backoff. A read goes to the least-loaded healthy replica;
// if it is slow relative to the second-best replica's latency, a second
// request races it, within a budget that grows with fast replies.

import (
	"context"
	"math"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/can1357/fdbgo/internal/fdberr"
	"github.com/can1357/fdbgo/internal/wire"
)

// Load-balancing parameters, matching the FoundationDB 7.4 client defaults.
const (
	// smoothingSecs is the e-folding time of the outstanding-request smoother.
	smoothingSecs = 2.0
	// futureVersionInitialBackoff is the first backoff after a future_version reply.
	futureVersionInitialBackoff = 1.0
	// futureVersionMaxBackoff caps the future_version backoff.
	futureVersionMaxBackoff = 8.0
	// futureVersionBackoffGrowth multiplies the future_version backoff per repeat.
	futureVersionBackoffGrowth = 2.0
	// instantSecondRequestMultiplier bounds how slow the best replica may be, relative to the
	// second-request delay, before the second request is sent immediately.
	instantSecondRequestMultiplier = 2.0
	// baseSecondRequestTime is added to the second-request delay.
	baseSecondRequestTime = 0.0005
	// SecondRequestMultiplierGrowth raises the second-request delay multiplier after a wasted request.
	SecondRequestMultiplierGrowth = 0.01
	// SecondRequestMultiplierDecay lowers the second-request delay multiplier after a useful request.
	SecondRequestMultiplierDecay = 0.00025
	// SecondRequestBudgetGrowth is the second-request budget earned per reply.
	SecondRequestBudgetGrowth = 0.05
	// SecondRequestMaxBudget caps the second-request budget.
	SecondRequestMaxBudget = 100.0
	// LBStartBackoff is the first backoff after all replicas failed.
	LBStartBackoff = 0.01
	// LBMaxBackoff caps the all-replicas-failed backoff.
	LBMaxBackoff = 5.0
	// LBBackoffRate multiplies the all-replicas-failed backoff per round.
	LBBackoffRate = 2.0
	// lbMaxBadOptions is how many unhealthy preferred replicas a read tolerates before
	// considering the rest.
	lbMaxBadOptions = 1
	// altFailureResetTime is the quiet period after which all-alternatives-failed delays restart.
	altFailureResetTime = 5.0
	// altFailureMinDelay is the minimum delay after all alternatives failed.
	altFailureMinDelay = 0.05
	// altFailureDelayRatio is the fast-growing delay as a fraction of time spent failing.
	altFailureDelayRatio = 0.2
	// altFailureMaxDelay caps the fast-growing delay.
	altFailureMaxDelay = 1.0
	// altFailureSlowDelayRatio is the slow-growing delay as a fraction of time spent failing.
	altFailureSlowDelayRatio = 0.04
	// altFailureSlowMaxDelay caps the slow-growing delay.
	altFailureSlowMaxDelay = 30.0
	// altFailureSkipDelay is how often an all-alternatives-failed delay may drop to the minimum.
	altFailureSkipDelay = 1.0
)

// clockStart anchors Now's process-local monotonic clock.
var clockStart = time.Now()

// Now returns seconds on a process-local monotonic clock.
func Now() float64 { return secs(time.Now()) }

// secs returns t on the Now scale.
func secs(t time.Time) float64 { return max(t.Sub(clockStart).Seconds(), 0) }

// loopTime is the latest time an I/O goroutine woke up, as float64 bits
// (non-negative, so ordered like the values).
var loopTime atomic.Uint64

// NoteTime records that an I/O goroutine woke up at t; the transport calls
// this before delivering replies.
func NoteTime(t time.Time) {
	bits := math.Float64bits(secs(t))
	for {
		cur := loopTime.Load()
		if cur >= bits || loopTime.CompareAndSwap(cur, bits) {
			return
		}
	}
}

// LoopTime returns the last time an I/O goroutine woke up, a
// per-iteration clock: free to read, and at or after the arrival of any reply
// already delivered.
func LoopTime() float64 { return math.Float64frombits(loopTime.Load()) }

// Smoother is an exponential smoother. It is not safe
// for concurrent use.
type Smoother struct {
	eFolding float64
	total    float64
	time     float64
	estimate float64
}

// NewSmoother returns a smoother folding over eFolding seconds.
func NewSmoother(eFolding float64) Smoother { return Smoother{eFolding: eFolding} }

func (s *Smoother) update(t float64) {
	elapsed := t - s.time
	if elapsed != 0 {
		s.time = t
		s.estimate += (s.total - s.estimate) * (1 - math.Exp(-elapsed/s.eFolding))
	}
}

// Reset sets the total and estimate to value.
func (s *Smoother) Reset(value float64) {
	s.time = 0
	s.total = value
	s.estimate = value
}

// AddDelta adds delta to the total.
func (s *Smoother) AddDelta(delta float64) {
	s.update(Now())
	s.total += delta
}

// SetTotal sets the total.
func (s *Smoother) SetTotal(total float64) { s.AddDelta(total - s.total) }

// SmoothTotal returns the smoothed total.
func (s *Smoother) SmoothTotal() float64 {
	s.update(Now())
	return s.estimate
}

// SmoothRate returns the smoothed rate of change of the total.
func (s *Smoother) SmoothRate() float64 {
	s.update(Now())
	return (s.total - s.estimate) / s.eFolding
}

// atomicF64 is a float64 held as bits, for lock-free model updates.
type atomicF64 struct{ v atomic.Uint64 }

func (a *atomicF64) load() float64   { return math.Float64frombits(a.v.Load()) }
func (a *atomicF64) store(v float64) { a.v.Store(math.Float64bits(v)) }

func (a *atomicF64) update(f func(float64) float64) {
	for {
		cur := a.v.Load()
		if a.v.CompareAndSwap(cur, math.Float64bits(f(math.Float64frombits(cur)))) {
			return
		}
	}
}

func (a *atomicF64) add(d float64) {
	for {
		cur := a.v.Load()
		if a.v.CompareAndSwap(cur, math.Float64bits(math.Float64frombits(cur)+d)) {
			return
		}
	}
}

// slot is one endpoint's load measurements.
//
// Requests start and end without a model-wide lock. Each change to the
// outstanding total first folds the old total into the smoother, under the
// slot's lock, so completed requests contribute even between comparisons.
type slot struct {
	// outstanding is the penalty-weighted requests in flight (the smoother's total).
	outstanding atomicF64
	latency     atomicF64
	penalty     atomicF64
	failedUntil atomicF64
	// smoothMu serializes outstanding changes and guards smoothTime and smoothEstimate.
	smoothMu       sync.Mutex
	smoothTime     float64
	smoothEstimate float64
	// estimate is the estimate as of the last fold, for comparisons that find smoothMu busy.
	estimate atomicF64
	// backoffMu guards futureVersionBackoff and increaseBackoffTime.
	backoffMu            sync.Mutex
	futureVersionBackoff float64
	increaseBackoffTime  float64
	// backoffRaised: the backoff differs from its initial state, so a clean reply must reset it.
	backoffRaised atomic.Bool
}

func newSlot() *slot {
	s := &slot{futureVersionBackoff: futureVersionInitialBackoff}
	s.latency.store(0.001)
	s.penalty.store(1)
	return s
}

// smoothed returns the smoothed outstanding count at time t, or the last
// estimate when another comparison holds smoothMu.
func (s *slot) smoothed(t float64) float64 {
	if !s.smoothMu.TryLock() {
		return s.estimate.load()
	}
	defer s.smoothMu.Unlock()
	s.fold(t)
	return s.smoothEstimate
}

// fold integrates the previous outstanding total while smoothMu is held.
func (s *slot) fold(t float64) {
	if elapsed := t - s.smoothTime; elapsed > 0 {
		total := s.outstanding.load()
		s.smoothTime = t
		s.smoothEstimate += (total - s.smoothEstimate) * (1 - math.Exp(-elapsed/smoothingSecs))
		s.estimate.store(s.smoothEstimate)
	}
}

// addOutstanding folds the old total before changing the load.
func (s *slot) addOutstanding(delta float64) {
	s.smoothMu.Lock()
	s.fold(Now())
	s.outstanding.store(s.outstanding.load() + delta)
	s.smoothMu.Unlock()
}

// QueueModel is the load model shared by all storage reads of a database.
// The zero value is not usable; call NewQueueModel.
type QueueModel struct {
	mu               sync.RWMutex
	slots            map[uint64]*slot
	secondMultiplier atomicF64
	secondBudget     atomicF64
}

// NewQueueModel returns an empty model.
func NewQueueModel() *QueueModel {
	m := &QueueModel{slots: make(map[uint64]*slot)}
	m.secondMultiplier.store(1)
	return m
}

// Ticket is a started request's claim on the queue model; it is released
// with the request's outcome by QueueModel.End, or by Release when the
// request is abandoned.
type Ticket struct {
	slot *slot
	// delta is the outstanding weight still counted; zero once released.
	delta float64
	start float64
}

// Release stops counting an abandoned request (one ended without an
// outcome) as outstanding; it is a no-op after End.
func (t *Ticket) Release() {
	if t.delta != 0 {
		t.slot.addOutstanding(-t.delta)
		t.delta = 0
	}
}

// Plan is the replicas to try for one read, in order, and when to race a second one.
type Plan struct {
	Best, Next int
	// Race is false when no second request should ever be sent.
	Race bool
	// SecondDelay is the delay before racing Next, when Race is set.
	SecondDelay time.Duration
}

func (m *QueueModel) slot(id uint64) *slot {
	m.mu.RLock()
	s := m.slots[id]
	m.mu.RUnlock()
	if s != nil {
		return s
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if s = m.slots[id]; s == nil {
		s = newSlot()
		m.slots[id] = s
	}
	return s
}

// Plan picks the best and next of n replicas like loadBalance does with a
// model. healthy(i) false excludes failed endpoints; replicas [0, countBest)
// are local; tokenOf(i) identifies replica i's endpoint in the model.
func (m *QueueModel) Plan(n, countBest int, tokenOf func(int) uint64, healthy func(int) bool) Plan {
	if n <= 1 {
		// A lone replica is always the one tried, and there is nothing to race.
		return Plan{}
	}
	best := rand.IntN(max(countBest, 1))
	next := rand.IntN(n - 1)
	if next >= best {
		next++
	}
	t := Now()
	var stack [8]*slot
	slots := stack[:0]
	if n > len(stack) {
		slots = make([]*slot, 0, n)
	}
	for i := range n {
		var s *slot
		if healthy(i) {
			s = m.slot(tokenOf(i))
		}
		slots = append(slots, s)
	}
	bestMetric, nextMetric, bestTime, nextTime := 1e9, 1e9, 1e9, 1e9
	bad := 0
	for i, qd := range slots {
		if bad < min(i, lbMaxBadOptions+1) && i == countBest {
			break
		}
		if qd == nil || t <= qd.failedUntil.load() {
			bad++
			continue
		}
		metric := qd.smoothed(t)
		if qd.penalty.load() > 1.001 {
			bad++
		}
		if metric < bestMetric {
			if i != best {
				next, nextMetric, nextTime = best, bestMetric, bestTime
			}
			best, bestMetric, bestTime = i, metric, qd.latency.load()
		} else if metric < nextMetric {
			next, nextMetric, nextTime = i, metric, qd.latency.load()
		}
	}
	if nextMetric > 1e8 {
		for i := countBest; i < n; i++ {
			qd := slots[i]
			if qd != nil && t > qd.failedUntil.load() {
				if metric := qd.smoothed(t); metric < nextMetric {
					next, nextMetric, nextTime = i, metric, qd.latency.load()
				}
			}
		}
	}
	p := Plan{Best: best, Next: next}
	if nextTime < 1e9 {
		p.Race = true
		threshold := m.secondMultiplier.load()*nextTime + baseSecondRequestTime
		if bestTime <= instantSecondRequestMultiplier*threshold {
			p.SecondDelay = time.Duration(threshold * float64(time.Second))
		}
	}
	return p
}

// Start records a request starting to endpoint id.
func (m *QueueModel) Start(id uint64) Ticket {
	s := m.slot(id)
	delta := s.penalty.load()
	s.addOutstanding(delta)
	return Ticket{slot: s, delta: delta, start: Now()}
}

// End records a request's end; penalty < 0 means
// not reported. It releases t.
func (m *QueueModel) End(t *Ticket, clean, futureVersion bool, penalty float64) {
	// A reply was delivered after the transport noted the time it arrived.
	now := max(LoopTime(), t.start)
	qd := t.slot
	delta := t.delta
	t.delta = 0
	qd.addOutstanding(-delta)
	latency := now - t.start
	if clean {
		qd.latency.store(latency)
	} else {
		qd.latency.update(func(l float64) float64 { return max(l, latency) })
	}
	if futureVersion {
		qd.backoffMu.Lock()
		if now > qd.increaseBackoffTime {
			qd.futureVersionBackoff = min(qd.futureVersionBackoff*futureVersionBackoffGrowth, futureVersionMaxBackoff)
			qd.increaseBackoffTime = now + qd.futureVersionBackoff
		}
		qd.failedUntil.store(now + qd.futureVersionBackoff)
		qd.backoffMu.Unlock()
		qd.backoffRaised.Store(true)
	} else if clean && qd.backoffRaised.Swap(false) {
		qd.backoffMu.Lock()
		qd.futureVersionBackoff, qd.increaseBackoffTime = futureVersionInitialBackoff, 0
		qd.backoffMu.Unlock()
	}
	if penalty > 0 {
		qd.penalty.store(penalty)
	}
}

// FirstReplied lowers the race tendency after a first request answered
// before the second-request delay.
func (m *QueueModel) FirstReplied() {
	// Both values saturate under steady load; skip the writes then.
	if m.secondMultiplier.load() > 1 {
		m.secondMultiplier.update(func(v float64) float64 { return max(v-SecondRequestMultiplierDecay, 1) })
	}
	if m.secondBudget.load() < SecondRequestMaxBudget {
		m.secondBudget.update(func(v float64) float64 { return min(v+SecondRequestBudgetGrowth, SecondRequestMaxBudget) })
	}
}

// TakeSecondBudget spends one unit of second-request budget, if available.
func (m *QueueModel) TakeSecondBudget() bool {
	for {
		cur := m.secondBudget.v.Load()
		b := math.Float64frombits(cur)
		if b < 1 {
			return false
		}
		if m.secondBudget.v.CompareAndSwap(cur, math.Float64bits(b-1)) {
			m.secondMultiplier.add(SecondRequestMultiplierGrowth)
			return true
		}
	}
}

// FailureMonitor is the client-side view of failed peers and endpoints.
// The zero value is ready to use.
type FailureMonitor struct {
	// marked counts entries in the failure maps; zero lets Failed skip the lock.
	marked atomic.Int64
	mu     sync.Mutex
	// changed is closed on a failure-state change; snapshots subscribe under mu.
	changed chan struct{}
	// addresses stays failed until a connection succeeds, not until a retry timer expires.
	addresses map[wire.NetAddr]struct{}
	// endpoints holds endpoints whose process reported them gone; they never come back.
	endpoints                       map[wire.UID]time.Time
	newestAlternativesFailure       float64
	oldestAlternativesFailure       float64
	lastAlternativesFailureSkipTime float64
}

func (f *FailureMonitor) count() {
	f.marked.Store(int64(len(f.addresses) + len(f.endpoints)))
}

func (f *FailureMonitor) notify() {
	if f.changed != nil {
		close(f.changed)
		f.changed = nil
	}
}

// WaitForRecovery waits until one endpoint is healthy, ctx ends, or delay
// elapses. It observes AddressOK notifications,
// returning all_alternatives_failed only if every endpoint is still failed.
func (f *FailureMonitor) WaitForRecovery(ctx context.Context, n int, endpointOf func(int) wire.Endpoint, tls bool, delay time.Duration) error {
	deadline := time.Now().Add(delay)
	timer := time.NewTimer(delay)
	defer timer.Stop()
	for {
		f.mu.Lock()
		now := time.Now()
		healthy := false
		for i := range n {
			ep := endpointOf(i)
			if _, gone := f.endpoints[ep.Token]; gone {
				continue
			}
			if _, failed := f.addresses[ep.Address(tls)]; !failed {
				healthy = true
				break
			}
		}
		var changed <-chan struct{}
		if !healthy && now.Before(deadline) {
			if f.changed == nil {
				f.changed = make(chan struct{})
			}
			changed = f.changed
		}
		f.mu.Unlock()
		if err := ctx.Err(); err != nil {
			return err
		}
		if healthy {
			return nil
		}
		if !now.Before(deadline) {
			return fdberr.AllAlternativesFailed
		}
		timer.Reset(time.Until(deadline))
		select {
		case <-changed:
		case <-timer.C:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Failed reports whether requests to ep, dialed with the given TLS preference, should be avoided.
func (f *FailureMonitor) Failed(ep wire.Endpoint, tls bool) bool {
	if f.marked.Load() == 0 {
		return false
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.endpoints[ep.Token]; ok {
		return true
	}
	_, failed := f.addresses[ep.Address(tls)]
	return failed
}

// AddressIsFailed reports whether addr is currently considered failed.
func (f *FailureMonitor) AddressIsFailed(addr wire.NetAddr) bool {
	if f.marked.Load() == 0 {
		return false
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	_, failed := f.addresses[addr]
	return failed
}

// OnlyEndpointFailed reports a retired endpoint on an otherwise healthy process.
func (f *FailureMonitor) OnlyEndpointFailed(ep wire.Endpoint, tls bool) bool {
	if f.marked.Load() == 0 {
		return false
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	_, endpointFailed := f.endpoints[ep.Token]
	_, addressFailed := f.addresses[ep.Address(tls)]
	return endpointFailed && !addressFailed
}

// AddressFailed marks addr failed after a connection error.
func (f *FailureMonitor) AddressFailed(addr wire.NetAddr) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.addresses == nil {
		f.addresses = make(map[wire.NetAddr]struct{})
	}
	f.addresses[addr] = struct{}{}
	f.count()
	f.notify()
}

// AddressOK clears addr's failure after a successful connection.
func (f *FailureMonitor) AddressOK(addr wire.NetAddr) {
	if f.marked.Load() == 0 {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, failed := f.addresses[addr]; failed {
		delete(f.addresses, addr)
		f.count()
		f.notify()
	}
}

// EndpointNotFound records that a process no longer serves ep.
func (f *FailureMonitor) EndpointNotFound(ep wire.Endpoint) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.endpoints == nil {
		f.endpoints = make(map[wire.UID]time.Time)
	}
	now := time.Now()
	f.endpoints[ep.Token] = now
	f.notify()
	// Bound memory: entries for long-gone roles are useless after their locations are refreshed.
	if len(f.endpoints) > 100_000 {
		cutoff := now.Add(-time.Hour)
		for tok, at := range f.endpoints {
			if !at.After(cutoff) {
				delete(f.endpoints, tok)
			}
		}
	}
	f.count()
}

// AllAlternativesFailedDelay returns the delay allAlternativesFailedDelay
// waits before giving up with all_alternatives_failed.
func (f *FailureMonitor) AllAlternativesFailedDelay() time.Duration {
	t := Now()
	f.mu.Lock()
	if t-f.newestAlternativesFailure > altFailureResetTime {
		f.oldestAlternativesFailure = t
	}
	delay := altFailureMinDelay
	if t-f.lastAlternativesFailureSkipTime > altFailureSkipDelay {
		f.lastAlternativesFailureSkipTime = t
	} else {
		elapsed := t - f.oldestAlternativesFailure
		delay = max(delay,
			min(elapsed*altFailureDelayRatio, altFailureMaxDelay),
			min(elapsed*altFailureSlowDelayRatio, altFailureSlowMaxDelay))
	}
	f.newestAlternativesFailure = t
	f.mu.Unlock()
	return time.Duration(delay * (0.9 + 0.2*rand.Float64()) * float64(time.Second))
}

// GrowBackoff returns the next load-balancer backoff: backoff times LBBackoffRate,
// clamped to [LBStartBackoff, LBMaxBackoff].
func GrowBackoff(backoff float64) float64 {
	return min(LBMaxBackoff, max(LBStartBackoff, backoff*LBBackoffRate))
}

// Replicas is a shared replica list; the slice identifies one location
// lookup's replicas for the requests planned over it.
type Replicas = []wire.StorageServer
