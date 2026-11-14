// Package native holds the per-database client state and the native
// transaction beneath read-your-writes: coordinator monitoring, connections,
// ClientDBInfo tracking, batched read versions, the shard location cache, load-balanced storage reads, commits,
// watches and \xff\xff/status/json.
package native

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/netip"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/can1357/fdbgo/internal/cluster"
	"github.com/can1357/fdbgo/internal/fdberr"
	"github.com/can1357/fdbgo/internal/options"
	"github.com/can1357/fdbgo/internal/transport"
	"github.com/can1357/fdbgo/internal/wire"
)

const (
	// coordinatorReconnectionDelay is the pause before reconnecting to the coordinators.
	coordinatorReconnectionDelay = time.Second
	// maxCommitProxyConnections caps how many commit proxies the client uses.
	maxCommitProxyConnections = 5
	// maxGRVProxyConnections caps how many GRV proxies the client uses.
	maxGRVProxyConnections = 3
	// maxGRVBatch caps the read version requests merged into one batch.
	maxGRVBatch = 1000
	// grvBatchTimeout is the longest a read version request waits for its batch, in seconds.
	grvBatchTimeout = 0.005
	// Match the upstream endpoint-only location refresh grace and retry windows.
	locationEndpointFailureGrace = time.Minute
	locationFailedEndpointRetry  = time.Minute
	// DefaultLocationCacheSize is the default shard location cache capacity.
	DefaultLocationCacheSize = 600_000
	// metadataVersionCacheSize is how many metadata versions are remembered.
	metadataVersionCacheSize = 1000
	// tagThrottleSmoothingWindow is the window, in seconds, of the tag rate smoother.
	tagThrottleSmoothingWindow = 2.0
	// TagThrottleRecheckInterval is how often, in seconds, a throttled tag may be retried.
	TagThrottleRecheckInterval = 5.0
)

// errProxiesChanged is the cancellation cause of a request abandoned because
// the proxy set changed under it (onProxiesChanged).
var errProxiesChanged = errors.New("proxies changed")

// ClusterInfo is the proxy set of the current cluster generation.
type ClusterInfo struct {
	ID wire.UID
	// Protocol is the coordinator's negotiated protocol, zero before discovery.
	Protocol wire.ProtocolVersion
	// GRV holds GrvProxyInterface.getConsistentReadVersion endpoints.
	GRV []wire.Endpoint
	// Commit holds CommitProxyInterface.commit endpoints (the interface base).
	Commit []wire.Endpoint
	// Provisional reports that the proxies belong to a recovering generation.
	Provisional bool
	// ClusterID identifies the cluster.
	ClusterID wire.UID
}

func (ci *ClusterInfo) usable(provisionalOK bool) bool {
	return len(ci.GRV) > 0 && len(ci.Commit) > 0 && (provisionalOK || !ci.Provisional)
}

// infoState notifies metadata readers on every publication, but cancels RPCs
// only when the proxy generation changes.
type infoState struct {
	info    *ClusterInfo
	ctx     context.Context
	cancel  context.CancelFunc
	changed chan struct{}
}

// Location is the shard containing a key and its replicas.
type Location struct {
	Begin, End []byte
	Servers    []wire.StorageServer
}

// Priority is a transaction priority.
type Priority uint8

const (
	// PriorityDefault is the default priority.
	PriorityDefault Priority = iota
	// PriorityBatch is priority_batch.
	PriorityBatch
	// PriorityImmediate is priority_system_immediate.
	PriorityImmediate
)

// GRVFlag returns the read version request priority bits.
func (p Priority) GRVFlag() uint32 {
	switch p {
	case PriorityBatch:
		return grvPriorityBatch
	case PriorityImmediate:
		return grvPriorityImmediate
	}
	return grvPriorityDefault
}

// wallNow is seconds since the Unix epoch: the clock tag throttle expirations use.
func wallNow() float64 { return float64(time.Now().UnixNano()) / 1e9 }

// tagThrottle is the client view of one throttled tag.
type tagThrottle struct {
	tpsRate        float64
	expiration     float64
	lastCheck      float64
	smoothRate     transport.Smoother
	smoothReleased transport.Smoother
}

func newTagThrottle(tpsRate, expiration float64) *tagThrottle {
	t := &tagThrottle{
		tpsRate:        tpsRate,
		expiration:     expiration,
		lastCheck:      transport.Now(),
		smoothRate:     transport.NewSmoother(tagThrottleSmoothingWindow),
		smoothReleased: transport.NewSmoother(tagThrottleSmoothingWindow),
	}
	t.smoothRate.Reset(tpsRate)
	return t
}

func (t *tagThrottle) update(tpsRate, expiration float64) {
	t.tpsRate = tpsRate
	if t.expired() {
		t.smoothRate.Reset(tpsRate)
	} else {
		t.smoothRate.SetTotal(tpsRate)
	}
	t.expiration = expiration
}

func (t *tagThrottle) expired() bool { return t.expiration <= wallNow() }

func (t *tagThrottle) canRecheck() bool {
	return t.lastCheck < transport.Now()-TagThrottleRecheckInterval
}

// throttleDuration is the seconds a transaction with this tag should wait.
func (t *tagThrottle) throttleDuration() float64 {
	now := wallNow()
	if t.expiration <= now {
		return 0
	}
	capacity := (t.smoothRate.SmoothTotal() - t.smoothReleased.SmoothRate()) * tagThrottleSmoothingWindow
	if capacity >= 1 {
		return 0
	}
	if t.tpsRate == 0 {
		return max(t.expiration-now, 0)
	}
	return min(t.expiration-now, capacity/t.tpsRate)
}

// TagThrottles are the tag throttles the proxies reported, per priority
// (throttledTags). Guard with DbContext's throttle lock.
type TagThrottles struct {
	byPriority map[Priority]map[string]*tagThrottle
}

// CheckBeforeGRV checks tags before a read version request: tag_throttled if
// a tag is throttled and was checked too recently.
func (tt *TagThrottles) CheckBeforeGRV(p Priority, tags [][]byte) error {
	m := tt.byPriority[p]
	if m == nil {
		return nil
	}
	maxDelay := 0.0
	canRecheck := false
	for _, tag := range tags {
		t, ok := m[string(tag)]
		if !ok {
			continue
		}
		if !t.expired() {
			maxDelay = max(maxDelay, t.throttleDuration())
			canRecheck = t.canRecheck()
		} else {
			delete(m, string(tag))
		}
	}
	if maxDelay > 0 && !canRecheck {
		return fdberr.TagThrottled
	}
	for _, tag := range tags {
		if t, ok := m[string(tag)]; ok {
			t.lastCheck = transport.Now()
		}
	}
	return nil
}

// update records the proxy's throttle report for the tags of a batch.
func (tt *TagThrottles) update(p Priority, tags []wire.TagCount, reply *wire.ReadVersionReply) {
	if tt.byPriority == nil {
		tt.byPriority = make(map[Priority]map[string]*tagThrottle)
	}
	m := tt.byPriority[p]
	if m == nil {
		m = make(map[string]*tagThrottle)
		tt.byPriority[p] = m
	}
	for _, tc := range tags {
		idx := slices.IndexFunc(reply.Throttled, func(th wire.ThrottledTag) bool { return bytes.Equal(th.Tag, tc.Tag) })
		if idx < 0 {
			delete(m, string(tc.Tag))
			continue
		}
		th := reply.Throttled[idx]
		if t, ok := m[string(tc.Tag)]; ok {
			t.update(th.Rate, th.Expiration)
		} else {
			m[string(tc.Tag)] = newTagThrottle(th.Rate, th.Expiration)
		}
	}
}

// CheckAfterGRV checks tags after receiving a read version (extractReadVersion).
func (tt *TagThrottles) CheckAfterGRV(p Priority, tags [][]byte) error {
	m := tt.byPriority[p]
	if m == nil {
		return nil
	}
	for _, tag := range tags {
		if t, ok := m[string(tag)]; ok {
			if t.expired() {
				delete(m, string(tag))
			} else if t.throttleDuration() > 0 {
				return fdberr.TagThrottled
			}
		}
	}
	for _, tag := range tags {
		if t, ok := m[string(tag)]; ok {
			t.smoothReleased.AddDelta(1)
		}
	}
	return nil
}

// Backoff is the extra backoff tag_throttled errors call for.
func (tt *TagThrottles) Backoff(p Priority, tags [][]byte, backoff float64) float64 {
	m := tt.byPriority[p]
	for _, tag := range tags {
		if t, ok := m[string(tag)]; ok {
			backoff = max(backoff, min(TagThrottleRecheckInterval, t.throttleDuration()))
			if backoff == TagThrottleRecheckInterval {
				break
			}
		}
	}
	return backoff
}

// locEntry is one cached shard.
type locEntry struct {
	begin, end []byte
	servers    []wire.StorageServer
}

// LocationCache is the shard location cache:
// non-overlapping ranges sorted by begin key.
type LocationCache struct {
	mu       sync.RWMutex
	entries  []locEntry
	capacity atomic.Int64
}

func newLocationCache() *LocationCache {
	c := &LocationCache{}
	c.capacity.Store(DefaultLocationCacheSize)
	return c
}

// find returns the index of the entry containing key (for reverse, the one
// containing the key just before it), or -1. Callers hold mu.
func (c *LocationCache) find(key []byte, reverse bool) int {
	var i int
	if reverse {
		i = sort.Search(len(c.entries), func(i int) bool { return bytes.Compare(c.entries[i].begin, key) >= 0 })
	} else {
		i = sort.Search(len(c.entries), func(i int) bool { return bytes.Compare(c.entries[i].begin, key) > 0 })
	}
	i--
	if i < 0 {
		return -1
	}
	cmp := bytes.Compare(key, c.entries[i].end)
	if cmp < 0 || (reverse && cmp == 0) {
		return i
	}
	return -1
}

// Get returns the cached shard containing key, or for reverse the one
// containing the key just before it.
func (c *LocationCache) Get(key []byte, reverse bool) (Location, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	i := c.find(key, reverse)
	if i < 0 {
		return Location{}, false
	}
	e := &c.entries[i]
	return Location{Begin: e.begin, End: e.end, Servers: e.servers}, true
}

// lowerBound is the index of the first entry with begin >= key. Callers hold mu.
func (c *LocationCache) lowerBound(key []byte) int {
	return sort.Search(len(c.entries), func(i int) bool { return bytes.Compare(c.entries[i].begin, key) >= 0 })
}

// Insert caches loc, trimming whatever it overlaps.
func (c *LocationCache) Insert(loc Location) {
	c.mu.Lock()
	defer c.mu.Unlock()
	i := c.lowerBound(loc.Begin)
	j := c.lowerBound(loc.End)
	if bytes.Compare(loc.Begin, loc.End) >= 0 {
		j = i
	}
	repl := []locEntry{{loc.Begin, loc.End, loc.Servers}}
	// The entry starting before begin may extend into the range: keep its head (and tail).
	if i > 0 {
		prev := &c.entries[i-1]
		if bytes.Compare(prev.end, loc.Begin) > 0 {
			if bytes.Compare(prev.end, loc.End) > 0 {
				repl = append(repl, locEntry{loc.End, prev.end, prev.servers})
			}
			prev.end = loc.Begin
		}
	}
	if j > i {
		last := c.entries[j-1]
		if bytes.Compare(last.end, loc.End) > 0 {
			repl = append(repl, locEntry{loc.End, last.end, last.servers})
		}
	}
	c.entries = slices.Replace(c.entries, i, j, repl...)
	limit := max(int(c.capacity.Load()), 1)
	for len(c.entries) > limit {
		// Sample entries, not key space, and exclude the just-inserted entry.
		// Each iteration removes one entry even for arbitrarily skewed binary keys.
		v := rand.IntN(len(c.entries) - 1)
		if v >= i {
			v++
		}
		c.entries = slices.Delete(c.entries, v, v+1)
		if v < i {
			i--
		}
	}
}

// Invalidate drops the cached shard holding key (invalidateCache).
func (c *LocationCache) Invalidate(key []byte, reverse bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if i := c.find(key, reverse); i >= 0 {
		c.entries = slices.Delete(c.entries, i, i+1)
	}
}

// InvalidateRange drops every cached shard intersecting [begin, end).
func (c *LocationCache) InvalidateRange(begin, end []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	i := c.lowerBound(begin)
	j := max(c.lowerBound(end), i)
	if i > 0 && bytes.Compare(c.entries[i-1].end, begin) > 0 {
		i--
	}
	c.entries = slices.Delete(c.entries, i, j)
}

// SetCapacity applies the location_cache_size database option.
func (c *LocationCache) SetCapacity(n int) { c.capacity.Store(int64(n)) }

// Len returns the number of cached shards.
func (c *LocationCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.entries)
}

// GRVCache holds the cached (read version, time) for use_grv_cache
// (updateCachedReadVersion) and rate keeper throttle times.
type GRVCache struct {
	Version int64
	Time    float64
	// LastProxyTime is the last time a GRV proxy was contacted (lastProxyRequestTime).
	LastProxyTime         float64
	UpdaterRunning        bool
	LastRKDefaultThrottle float64
	LastRKBatchThrottle   float64
}

// mvEntry is one (version, metadataVersion) pair; mv nil = absent.
type mvEntry struct {
	version int64
	mv      []byte
}

// mvCache is a ring of recent (version, metadataVersion) pairs (metadataVersionCache).
type mvCache struct {
	mu   sync.Mutex
	ring [metadataVersionCacheSize]mvEntry
	head int
	n    int
}

func (m *mvCache) at(i int) *mvEntry { return &m.ring[(m.head+i)%metadataVersionCacheSize] }

// Peers holds one connection per server address.
type Peers struct {
	mu         sync.RWMutex
	m          map[wire.NetAddr]*peer
	closed     bool
	stopping   chan struct{}
	closeOnce  sync.Once
	dials      sync.WaitGroup
	recoveries sync.WaitGroup
}

type peer struct {
	conn            atomic.Pointer[transport.Connection]
	dial            chan struct{}
	cancelDial      context.CancelFunc // guarded by Peers.mu
	connectFailures atomic.Uint64
	recovering      bool // guarded by Peers.mu
}

// PeerStatus is one peer's connection state for client status reports.
type PeerStatus struct {
	Addr wire.NetAddr
	// Connection is the live connection, or nil.
	Connection      *transport.Connection
	ConnectFailures uint64
}

func (p *peer) live() *transport.Connection {
	if c := p.conn.Load(); c != nil && !c.IsDead() {
		return c
	}
	return nil
}

// Live returns a live connection to addr without dialing, or nil.
func (ps *Peers) Live(addr wire.NetAddr) *transport.Connection {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	if ps.closed {
		return nil
	}
	if p := ps.m[addr]; p != nil {
		return p.live()
	}
	return nil
}

// Get returns a live connection to addr, dialing one if needed; concurrent
// callers share the dial.
func (ps *Peers) Get(ctx context.Context, addr wire.NetAddr, failures *transport.FailureMonitor) (*transport.Connection, error) {
	ps.mu.Lock()
	if ps.closed {
		ps.mu.Unlock()
		return nil, fdberr.OperationCancelled
	}
	if ps.m == nil {
		ps.m = make(map[wire.NetAddr]*peer)
		ps.stopping = make(chan struct{})
	}
	p := ps.m[addr]
	if p == nil {
		p = &peer{dial: make(chan struct{}, 1)}
		ps.m[addr] = p
	}
	if c := p.live(); c != nil {
		ps.mu.Unlock()
		return c, nil
	}
	stopping := ps.stopping
	ps.mu.Unlock()

	select {
	case p.dial <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-stopping:
		return nil, fdberr.OperationCancelled
	}
	defer func() { <-p.dial }()

	ps.mu.Lock()
	if ps.closed {
		ps.mu.Unlock()
		return nil, fdberr.OperationCancelled
	}
	if c := p.live(); c != nil {
		ps.mu.Unlock()
		return c, nil
	}
	stale := p.conn.Swap(nil)
	dctx, cancel := context.WithCancel(ctx)
	p.cancelDial = cancel
	// Registration and publication share the shutdown lock: Close can
	// cancel and join every dial, and no dial can publish once it closes.
	ps.dials.Add(1)
	ps.mu.Unlock()
	defer ps.dials.Done()

	if stale != nil {
		// A failed connection can still be draining replies. Keep ownership
		// until all its goroutines stop before replacing the peer.
		<-stale.Done()
	}
	c, err := transport.Connect(dctx, addr)
	cancel()
	ps.mu.Lock()
	p.cancelDial = nil
	if ps.closed {
		ps.mu.Unlock()
		if c != nil {
			c.Close()
			<-c.Done()
		}
		return nil, fdberr.OperationCancelled
	}
	if err == nil {
		p.conn.Store(c)
		failures.AddressOK(addr)
	} else if ctx.Err() == nil {
		p.connectFailures.Add(1)
		failures.AddressFailed(addr)
		ps.recoverLocked(addr, p, failures)
	}
	ps.mu.Unlock()
	return c, err
}

// recoverLocked keeps retrying a failed address without advertising it as
// healthy. Replica selection can therefore exhaust failed alternatives while
// a single pool-owned reconnect continues independently of request lifetimes.
func (ps *Peers) recoverLocked(addr wire.NetAddr, p *peer, failures *transport.FailureMonitor) {
	if ps.closed || p.recovering {
		return
	}
	p.recovering = true
	ps.recoveries.Add(1)
	go func() {
		defer ps.recoveries.Done()
		defer func() {
			ps.mu.Lock()
			p.recovering = false
			if failures.AddressIsFailed(addr) {
				ps.recoverLocked(addr, p, failures)
			}
			ps.mu.Unlock()
		}()
		delay := 500 * time.Millisecond
		for {
			timer := time.NewTimer(delay)
			select {
			case <-ps.stopping:
				timer.Stop()
				return
			case <-timer.C:
			}
			if !failures.AddressIsFailed(addr) {
				return
			}
			if _, err := ps.Get(context.Background(), addr, failures); err == nil {
				return
			}
			delay = min(delay*2, 5*time.Second)
		}
	}()
}

// Status returns every known peer and its live connection, if any.
func (ps *Peers) Status() []PeerStatus {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	out := make([]PeerStatus, 0, len(ps.m))
	for a, p := range ps.m {
		out = append(out, PeerStatus{Addr: a, Connection: p.live(), ConnectFailures: p.connectFailures.Load()})
	}
	return out
}

// closeAll permanently closes the pool, cancels outstanding dials and waits
// for their disposal and for established connections to drain their requests.
func (ps *Peers) closeAll() {
	ps.closeOnce.Do(func() {
		ps.mu.Lock()
		ps.closed = true
		if ps.stopping != nil {
			close(ps.stopping)
		}
		for _, p := range ps.m {
			if p.cancelDial != nil {
				p.cancelDial()
			}
			if c := p.conn.Load(); c != nil {
				c.Close()
			}
		}
		ps.mu.Unlock()

		ps.recoveries.Wait()
		ps.dials.Wait()
		// The closed map is immutable, and publication was disabled before
		// waiting, so every connection is now either here or already disposed.
		for _, p := range ps.m {
			if c := p.conn.Swap(nil); c != nil {
				<-c.Done()
			}
		}
	})
}

// locality is clientLocality: zone (machine_id) and datacenter (datacenter_id).
type locality struct {
	zone, dc []byte
}

// DbContext is the per-database state shared by all its transactions.
type DbContext struct {
	recordMu sync.Mutex
	record   cluster.ConnectionRecord

	// Peers holds the connections to cluster processes.
	Peers Peers
	// Failures is the client-side failure monitor.
	Failures *transport.FailureMonitor
	// Model is the storage load model shared by all reads.
	Model   *transport.QueueModel
	lagging laggingRequests
	// Locations is the shard location cache.
	Locations *LocationCache
	// TLS selects the TLS address of dual-stack processes.
	TLS bool
	// Watches is the watch bookkeeping of all transactions of this database.
	Watches *Watches

	info atomic.Pointer[infoState]

	verifyCausalReads atomic.Int64

	grvMu sync.Mutex
	grv   map[uint32]*grvBatcher

	throttleMu sync.Mutex
	throttles  TagThrottles

	grvCacheMu sync.Mutex
	grvCache   GRVCache

	mv mvCache

	commitCosts commitCostState

	endpointFailureMu sync.Mutex
	endpointFailures  map[wire.UID]endpointFailureInfo

	coordMu     sync.Mutex
	coordinator string

	loc atomic.Pointer[locality]

	ctx       context.Context
	cancel    context.CancelCauseFunc
	bgMu      sync.Mutex
	closing   bool
	wg        sync.WaitGroup
	closeOnce sync.Once
}

// NewDbContext creates the context and starts monitoring the coordinators.
// Close stops it.
func NewDbContext(record cluster.ConnectionRecord, tls bool) *DbContext {
	record.Init()
	ctx, cancel := context.WithCancelCause(context.Background())
	c := &DbContext{
		record:    record,
		Failures:  &transport.FailureMonitor{},
		Model:     transport.NewQueueModel(),
		Locations: newLocationCache(),
		TLS:       tls,
		Watches:   newWatches(),
		grv:       make(map[uint32]*grvBatcher),
		ctx:       ctx,
		cancel:    cancel,
	}
	ictx, icancel := context.WithCancel(ctx)
	c.info.Store(&infoState{info: &ClusterInfo{}, ctx: ictx, cancel: icancel, changed: make(chan struct{})})
	c.wg.Add(1)
	go c.monitorProxies()
	return c
}

// Close stops the coordinator monitor and every background goroutine of the
// database, fails operations in flight with operation_cancelled and closes
// its connections.
func (c *DbContext) Close() {
	c.closeOnce.Do(func() {
		c.bgMu.Lock()
		c.closing = true
		c.cancel(fdberr.OperationCancelled)
		c.bgMu.Unlock()
		c.lagging.close()
		c.Peers.closeAll()
		c.wg.Wait()
	})
}

// addBackground registers a new root background task before Close can wait.
// Children may add directly while their registered parent is still running.
func (c *DbContext) addBackground() bool {
	c.bgMu.Lock()
	defer c.bgMu.Unlock()
	if c.closing {
		return false
	}
	c.wg.Add(1)
	return true
}

// Context returns a context cancelled when the database is closed.
func (c *DbContext) Context() context.Context { return c.ctx }

// cancelErr is the error for an operation stopped by ctx: the fdberr.Error
// the context was cancelled with, else operation_cancelled.
func cancelErr(ctx context.Context) error {
	var e fdberr.Error
	if errors.As(context.Cause(ctx), &e) {
		return e
	}
	return fdberr.OperationCancelled
}

// sleep waits d or until ctx ends.
func sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		if ctx.Err() != nil {
			return cancelErr(ctx)
		}
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return cancelErr(ctx)
	}
}

func secs(s float64) time.Duration { return time.Duration(s * float64(time.Second)) }

// ClusterNow returns the current cluster info without waiting.
func (c *DbContext) ClusterNow() *ClusterInfo { return c.info.Load().info }

// ClusterID waits for a valid cluster identifier for special-key reads.
func (c *DbContext) ClusterID(ctx context.Context) (wire.UID, error) {
	for {
		if c.ctx.Err() != nil {
			return wire.UID{}, fdberr.OperationCancelled
		}
		st := c.info.Load()
		if st.info.ClusterID.IsValid() {
			return st.info.ClusterID, nil
		}
		if st.info.Protocol != 0 && st.info.Protocol.Normalized() < wire.Protocol72 {
			return wire.UID{}, fdberr.UnsupportedOperation
		}
		select {
		case <-st.changed:
		case <-ctx.Done():
			return wire.UID{}, cancelErr(ctx)
		case <-c.ctx.Done():
			return wire.UID{}, fdberr.OperationCancelled
		}
	}
}

// clusterState waits until proxies usable by a request are known.
func (c *DbContext) clusterState(ctx context.Context, provisionalOK bool) (*infoState, error) {
	for {
		if c.ctx.Err() != nil {
			return nil, fdberr.OperationCancelled
		}
		st := c.info.Load()
		if st.info.usable(provisionalOK) {
			return st, nil
		}
		select {
		case <-st.changed:
		case <-ctx.Done():
			return nil, cancelErr(ctx)
		case <-c.ctx.Done():
			return nil, fdberr.OperationCancelled
		}
	}
}

// Cluster waits until proxies usable by a request are known.
func (c *DbContext) Cluster(ctx context.Context, provisionalOK bool) (*ClusterInfo, error) {
	st, err := c.clusterState(ctx, provisionalOK)
	if err != nil {
		return nil, err
	}
	return st.info, nil
}

// withProxies waits for usable proxies and returns them with a context that
// is cancelled (cause errProxiesChanged) when the proxy set changes; call
// release when done.
func (c *DbContext) withProxies(ctx context.Context, provisionalOK bool) (*ClusterInfo, context.Context, func(), error) {
	st, err := c.clusterState(ctx, provisionalOK)
	if err != nil {
		return nil, nil, nil, err
	}
	pctx, cancel := context.WithCancelCause(ctx)
	stop := context.AfterFunc(st.ctx, func() { cancel(errProxiesChanged) })
	return st.info, pctx, func() { stop(); cancel(nil) }, nil
}

// proxiesChanged reports whether pctx (from withProxies) ended because the proxy set changed.
func proxiesChanged(pctx context.Context) bool {
	return errors.Is(context.Cause(pctx), errProxiesChanged)
}

// SetCausalReadVerification sets the database option's verification probability (0–100 percent).
func (c *DbContext) SetCausalReadVerification(percent int64) {
	c.verifyCausalReads.Store(percent)
}

// verifyOldProxies checks that a completed recovery retired the old GRV proxies.
func (c *DbContext) verifyOldProxies(old, next *ClusterInfo) {
	percent := c.verifyCausalReads.Load()
	if percent == 0 || len(old.GRV) == 0 || len(next.GRV) == 0 || next.Provisional ||
		rand.Int64N(100) >= percent {
		return
	}
	for _, ep := range old.GRV {
		if !c.addBackground() {
			return
		}
		go func() {
			defer c.wg.Done()
			req := wire.GetReadVersion{Count: 1, Flags: PriorityImmediate.GRVFlag() | grvCausalReadRisky}
			reply, err := call(c.ctx, c, ep, wire.Request[wire.ReadVersionReply](req))
			if err == nil {
				slog.Error("fdb: GotStaleReadVersion", "remote", ep.Addr.String(), "read_version", reply.Version)
			}
		}()
	}
}

func (c *DbContext) publish(info *ClusterInfo) {
	for {
		old := c.info.Load()
		changed := old.info.Provisional != info.Provisional || !old.info.Protocol.Compatible(info.Protocol) ||
			!slices.EqualFunc(old.info.GRV, info.GRV, wire.Endpoint.Equal) || !slices.EqualFunc(old.info.Commit, info.Commit, wire.Endpoint.Equal)
		next := &infoState{info: info, ctx: old.ctx, cancel: old.cancel, changed: make(chan struct{})}
		if changed {
			next.ctx, next.cancel = context.WithCancel(c.ctx)
		}
		if c.info.CompareAndSwap(old, next) {
			if changed {
				old.cancel()
				c.verifyOldProxies(old.info, info)
			}
			close(old.changed)
			return
		}
		if changed {
			next.cancel()
		}
	}
}

// Connection returns a connection to the process serving ep.
func (c *DbContext) Connection(ctx context.Context, ep wire.Endpoint) (*transport.Connection, error) {
	if c.ctx.Err() != nil {
		return nil, fdberr.OperationCancelled
	}
	addr := ep.Address(c.TLS)
	if conn := c.Peers.Live(addr); conn != nil {
		return conn, nil
	}
	return c.Peers.Get(ctx, addr, c.Failures)
}

// call sends one request to ep and waits for its reply, recording endpoint failures.
func call[R any](ctx context.Context, c *DbContext, ep wire.Endpoint, req wire.Request[R]) (R, error) {
	conn, err := c.Connection(ctx, ep)
	if err != nil {
		var zero R
		if c.ctx.Err() != nil {
			return zero, fdberr.OperationCancelled
		}
		if ctx.Err() != nil {
			return zero, cancelErr(ctx)
		}
		return zero, err
	}
	r, err := transport.Call(ctx, conn, ep.Token, req)
	return r, storageCallError(ctx, c, ep, err)
}

// storageCallError applies the endpoint and lifetime handling shared by
// individual storage calls and batch collection.
func storageCallError(ctx context.Context, c *DbContext, ep wire.Endpoint, err error) error {
	if err != nil {
		if err == transport.EndpointNotFound {
			c.Failures.EndpointNotFound(ep)
		}
		if c.ctx.Err() != nil {
			return fdberr.OperationCancelled
		}
		if ctx.Err() != nil {
			return cancelErr(ctx)
		}
	}
	return err
}

// rpcToFDB maps a transport failure to the error a client sees.
func rpcToFDB(err error) error {
	var re transport.RPCError
	if errors.As(err, &re) {
		switch re {
		case transport.Unauthorized:
			return fdberr.UnauthorizedAttempt
		case transport.Malformed:
			return fdberr.SerializationFailed
		case transport.Incompatible:
			return fdberr.IncompatibleProtocolVersion
		default:
			return fdberr.RequestMaybeDelivered
		}
	}
	var fe fdberr.Error
	if errors.As(err, &fe) {
		return fe
	}
	return fdberr.OperationCancelled
}

// isFatalRPC reports transport failures no retry can fix.
func isFatalRPC(err error) bool {
	return err == transport.Malformed || err == transport.Incompatible || err == transport.Unauthorized
}

// ---- read versions ------------------------------------------------------

// grvWaiter is one transaction waiting for a batched read version.
type grvWaiter struct {
	tags    [][]byte
	debugID wire.UID
	done    func(*wire.ReadVersionReply, error)
}

type grvBatch struct {
	waiters []grvWaiter
	sent    bool
}

// grvBatcher coalesces read version requests of one flag set (readVersionBatcher).
type grvBatcher struct {
	c         *DbContext
	flags     uint32
	mu        sync.Mutex
	cur       *grvBatch
	batchTime float64
}

// ReadVersion requests a read version from the batcher for flags
// (readVersionBatcher); done runs once with the reply, from a batch goroutine
// or synchronously when the database is closed, and must not block. Batches
// are sent when MAX_BATCH_SIZE requests are queued or the adaptive batch
// window (half the smoothed reply latency, at most GRV_BATCH_TIMEOUT) ends.
func (c *DbContext) ReadVersion(flags uint32, tags [][]byte, debugID wire.UID, done func(*wire.ReadVersionReply, error)) {
	c.grvMu.Lock()
	b := c.grv[flags]
	if b == nil {
		b = &grvBatcher{c: c, flags: flags}
		c.grv[flags] = b
	}
	c.grvMu.Unlock()
	if c.ctx.Err() != nil {
		done(nil, fdberr.OperationCancelled)
		return
	}
	b.mu.Lock()
	if b.cur == nil {
		nb := &grvBatch{}
		b.cur = nb
		if window := b.batchTime; window >= 0.001 {
			time.AfterFunc(secs(window), func() { b.flush(nb) })
		} else {
			// A sub-millisecond window is a yield: take whatever queues meanwhile.
			go b.flush(nb)
		}
	}
	nb := b.cur
	nb.waiters = append(nb.waiters, grvWaiter{tags: tags, debugID: debugID, done: done})
	if len(nb.waiters) < maxGRVBatch {
		b.mu.Unlock()
		return
	}
	nb.sent = true
	b.cur = nil
	b.mu.Unlock()
	go b.send(nb.waiters)
}

// ReadVersionWait is ReadVersion for sequential code.
func (c *DbContext) ReadVersionWait(ctx context.Context, flags uint32, tags [][]byte) (*wire.ReadVersionReply, error) {
	type res struct {
		r   *wire.ReadVersionReply
		err error
	}
	ch := make(chan res, 1)
	c.ReadVersion(flags, tags, wire.UID{}, func(r *wire.ReadVersionReply, err error) { ch <- res{r, err} })
	select {
	case r := <-ch:
		return r.r, r.err
	case <-ctx.Done():
		return nil, cancelErr(ctx)
	}
}

func (b *grvBatcher) flush(nb *grvBatch) {
	b.mu.Lock()
	if nb.sent {
		b.mu.Unlock()
		return
	}
	nb.sent = true
	if b.cur == nb {
		b.cur = nil
	}
	b.mu.Unlock()
	b.send(nb.waiters)
}

func (b *grvBatcher) send(waiters []grvWaiter) {
	c := b.c
	var tags []wire.TagCount
	if slices.ContainsFunc(waiters, func(w grvWaiter) bool { return len(w.tags) > 0 }) {
		counts := make(map[string]uint32)
		for _, w := range waiters {
			for _, t := range w.tags {
				if _, ok := counts[string(t)]; !ok {
					tags = append(tags, wire.TagCount{Tag: t})
				}
				counts[string(t)]++
			}
		}
		for i := range tags {
			tags[i].Count = counts[string(tags[i].Tag)]
		}
	}
	var debugID wire.UID
	for _, w := range waiters {
		if w.debugID.IsValid() {
			debugID = w.debugID
			break
		}
	}
	start := transport.Now()
	reply, err := c.consistentReadVersion(uint32(len(waiters)), b.flags, tags, debugID)
	latency := transport.Now() - start
	b.mu.Lock()
	b.batchTime = min(0.1*(latency*0.5)+0.9*b.batchTime, grvBatchTimeout)
	b.mu.Unlock()
	if err == nil {
		var p Priority
		switch b.flags & grvPriorityImmediate {
		case grvPriorityImmediate:
			p = PriorityImmediate
		case grvPriorityBatch:
			p = PriorityBatch
		default:
			p = PriorityDefault
		}
		if len(tags) > 0 {
			c.throttleMu.Lock()
			c.throttles.update(p, tags, reply)
			c.throttleMu.Unlock()
		}
		c.grvCacheMu.Lock()
		t := transport.Now()
		if reply.RKBatchThrottled {
			c.grvCache.LastRKBatchThrottle = t
		}
		if reply.RKDefaultThrottled {
			c.grvCache.LastRKDefaultThrottle = t
		}
		c.grvCacheMu.Unlock()
		c.UpdateCachedReadVersion(start, reply.Version)
		c.RecordMetadataVersion(reply.Version, reply.MetadataVersion)
		c.commitCosts.observe(reply.MidShardSize)
	}
	for _, w := range waiters {
		w.done(reply, err)
	}
}

// consistentReadVersion sends one GetReadVersionRequest on behalf of count
// transactions (getConsistentReadVersion). Batches belong to the database,
// so the existing proxy-generation context supplies both cancellation
// sources without allocating another context and hook per request.
func (c *DbContext) consistentReadVersion(count, flags uint32, tags []wire.TagCount, debugID wire.UID) (*wire.ReadVersionReply, error) {
	provisionalOK := flags&grvUseProvisional != 0
	req := &wire.GetReadVersion{Count: count, Flags: flags, Tags: tags, DebugID: debugID}
	for {
		st, err := c.clusterState(c.ctx, provisionalOK)
		if err != nil {
			return nil, err
		}
		r, _, err := basicLoadBalance(st.ctx, c, st.info.GRV, 0, wire.Request[wire.ReadVersionReply](req), false)
		if err != nil && st.ctx.Err() != nil && c.ctx.Err() == nil {
			continue
		}
		if err != nil {
			return nil, err
		}
		return &r, nil
	}
}

// basicLoadBalance sends to one of alternatives (basicLoadBalance), each
// adjusted to stream: transport failures and broken_promise move on to the
// next alternative, unless atMostOnce, in which case they become
// request_maybe_delivered.
func basicLoadBalance[R any](ctx context.Context, c *DbContext, alternatives []wire.Endpoint, stream uint32, req wire.Request[R], atMostOnce bool) (R, wire.Endpoint, error) {
	var zero R
	n := len(alternatives)
	next := rand.IntN(n)
	attempts := 0
	backoff := 0.0
	for {
		var ep wire.Endpoint
		chosen := false
		for range n {
			ep = alternatives[next]
			if stream != 0 {
				ep = ep.Adjusted(stream)
			}
			next = (next + 1) % n
			if !c.Failures.Failed(ep, c.TLS) {
				chosen = true
				break
			}
		}
		if !chosen {
			// Everything is down: wait for an alternative to recover.
			if err := sleep(ctx, c.Failures.AllAlternativesFailedDelay()); err != nil {
				return zero, wire.Endpoint{}, err
			}
			attempts = 0
			continue
		}
		if backoff > 0 {
			if err := sleep(ctx, secs(backoff)); err != nil {
				return zero, wire.Endpoint{}, err
			}
		}
		r, err := call(ctx, c, ep, req)
		if err == nil {
			return r, ep, nil
		}
		if ctx.Err() != nil {
			return zero, wire.Endpoint{}, cancelErr(ctx)
		}
		var code fdberr.Error
		switch {
		case err == transport.NotDelivered:
		case errors.As(err, &code) && code != fdberr.BrokenPromise && code != fdberr.RequestMaybeDelivered:
			return zero, wire.Endpoint{}, code
		case isFatalRPC(err):
			return zero, wire.Endpoint{}, rpcToFDB(err)
		case atMostOnce:
			return zero, wire.Endpoint{}, fdberr.RequestMaybeDelivered
		}
		attempts++
		if attempts >= n {
			backoff = transport.GrowBackoff(backoff)
		}
	}
}

// UpdateCachedReadVersion records a read or commit version observed at time
// (updateCachedReadVersion).
func (c *DbContext) UpdateCachedReadVersion(t float64, version int64) {
	c.grvCacheMu.Lock()
	if version >= c.grvCache.Version {
		c.grvCache.Version = version
		c.grvCache.Time = max(c.grvCache.Time, t)
	}
	c.grvCacheMu.Unlock()
}

// RecordMetadataVersion records (version, metadataVersion) (metadataVersionCache).
func (c *DbContext) RecordMetadataVersion(version int64, mv []byte) {
	m := &c.mv
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.n > 0 && version <= m.at(m.n-1).version {
		return
	}
	if m.n == metadataVersionCacheSize {
		m.head = (m.head + 1) % metadataVersionCacheSize
		m.n--
	}
	*m.at(m.n) = mvEntry{version, mv}
	m.n++
}

// CachedMetadataVersion returns the metadata version cached for exactly
// version (nil = absent) and whether one is cached.
func (c *DbContext) CachedMetadataVersion(version int64) ([]byte, bool) {
	m := &c.mv
	m.mu.Lock()
	defer m.mu.Unlock()
	i := sort.Search(m.n, func(i int) bool { return m.at(i).version >= version })
	if i < m.n && m.at(i).version == version {
		return m.at(i).mv, true
	}
	return nil, false
}

// grvCacheSnapshot returns a copy of the GRV cache state.
func (c *DbContext) grvCacheSnapshot() GRVCache {
	c.grvCacheMu.Lock()
	defer c.grvCacheMu.Unlock()
	return c.grvCache
}

// ---- locations ----------------------------------------------------------

type endpointFailureInfo struct {
	start, refreshed time.Time
}

// locationNeedsRefresh applies checkOnlyEndpointFailed to every replica, even
// when a healthy replica can serve the request. Refreshes are eager during the
// first minute, then limited to once a minute for each retired endpoint.
func (c *DbContext) locationNeedsRefresh(loc Location, stream uint32) bool {
	now := time.Now()
	c.endpointFailureMu.Lock()
	defer c.endpointFailureMu.Unlock()
	refresh := false
	for _, server := range loc.Servers {
		ep := server.Base.Adjusted(stream)
		if !c.Failures.OnlyEndpointFailed(ep, c.TLS) {
			delete(c.endpointFailures, ep.Token)
			continue
		}
		info, ok := c.endpointFailures[ep.Token]
		if !ok {
			info.start = now
		}
		if now.Sub(info.start) < locationEndpointFailureGrace || now.Sub(info.refreshed) > locationFailedEndpointRetry {
			info.refreshed = now
			refresh = true
		}
		if c.endpointFailures == nil {
			c.endpointFailures = make(map[wire.UID]endpointFailureInfo)
		}
		c.endpointFailures[ep.Token] = info
	}
	return refresh
}

// Locate returns the shard containing key (or the key before it when
// reverse) and its replicas (getKeyLocation).
func (c *DbContext) Locate(ctx context.Context, key []byte, reverse, provisionalOK bool, stream uint32) (Location, error) {
	if loc, ok := c.Locations.Get(key, reverse); ok {
		if !c.locationNeedsRefresh(loc, stream) {
			return loc, nil
		}
		c.Locations.Invalidate(key, reverse)
	}
	req := &wire.GetKeyServerLocations{Begin: key, Limit: 100, Reverse: reverse}
	shards, err := c.fetchLocations(ctx, req, provisionalOK)
	if err != nil {
		return Location{}, err
	}
	first := shards[0]
	loc := Location{Begin: first.Range.Begin, End: first.Range.End, Servers: first.Servers}
	c.Locations.Insert(loc)
	return loc, nil
}

// LocateRange returns up to limit shards covering [begin, end)
// (getKeyRangeLocations) and caches them.
func (c *DbContext) LocateRange(ctx context.Context, begin, end []byte, limit int, reverse, provisionalOK bool) ([]Location, error) {
	if end == nil {
		end = []byte{}
	}
	req := &wire.GetKeyServerLocations{Begin: begin, End: end, Limit: int32(min(limit, 1<<31-1)), Reverse: reverse}
	shards, err := c.fetchLocations(ctx, req, provisionalOK)
	if err != nil {
		return nil, err
	}
	locs := make([]Location, len(shards))
	for i, s := range shards {
		locs[i] = Location{Begin: s.Range.Begin, End: s.Range.End, Servers: s.Servers}
		c.Locations.Insert(locs[i])
	}
	return locs, nil
}

func (c *DbContext) fetchLocations(ctx context.Context, req *wire.GetKeyServerLocations, provisionalOK bool) ([]wire.ShardLocation, error) {
	backoff := 0.0
	for {
		info, pctx, release, err := c.withProxies(ctx, provisionalOK)
		if err != nil {
			return nil, err
		}
		var shards []wire.ShardLocation
		if info.Protocol.Compatible(wire.Protocol50) {
			shards, err = c.fetchLocations50(pctx, info, req)
		} else {
			shards, _, err = basicLoadBalance(pctx, c, info.Commit, cpGetKeyServersLocations, wire.Request[[]wire.ShardLocation](req), false)
		}
		changed := proxiesChanged(pctx)
		release()
		switch {
		case err != nil && changed:
			continue
		case err == nil && len(shards) > 0:
			return shards, nil
		case err == nil:
			return nil, fdberr.InternalError
		case err == fdberr.CommitProxyMemoryLimitExceeded:
			// Eaten like getKeyLocation_internal does, with growing backoff.
			backoff = transport.GrowBackoff(backoff)
			if err := sleep(ctx, secs(backoff)); err != nil {
				return nil, err
			}
		default:
			return nil, err
		}
	}
}

// SetLocality applies the machine_id (options.DatabaseMachineID) or
// datacenter_id (options.DatabaseDatacenterID) database option (clientLocality).
func (c *DbContext) SetLocality(code int, id []byte) {
	for {
		old := c.loc.Load()
		var l locality
		if old != nil {
			l = *old
		}
		switch code {
		case options.DatabaseMachineID:
			l.zone = slices.Clone(id)
		case options.DatabaseDatacenterID:
			l.dc = slices.Clone(id)
		default:
			return
		}
		if c.loc.CompareAndSwap(old, &l) {
			return
		}
	}
}

// byDistance orders replicas by distance from this client (same zone, same
// datacenter, distant) and counts the closest ones.
func (c *DbContext) byDistance(servers []wire.StorageServer) ([]wire.StorageServer, int) {
	l := c.loc.Load()
	if l == nil || (l.zone == nil && l.dc == nil) {
		return servers, len(servers)
	}
	distance := func(s *wire.StorageServer) int {
		if l.zone != nil && s.Zone != nil && bytes.Equal(s.Zone, l.zone) {
			return 0
		}
		if l.dc != nil && s.DC != nil && bytes.Equal(s.DC, l.dc) {
			return 1
		}
		return 2
	}
	sorted := slices.Clone(servers)
	slices.SortStableFunc(sorted, func(a, b wire.StorageServer) int { return distance(&a) - distance(&b) })
	best := distance(&sorted[0])
	count := 0
	for i := range sorted {
		if distance(&sorted[i]) != best {
			break
		}
		count++
	}
	return sorted, count
}

// KnownStorageServers returns every storage server in the location cache (server_interf).
func (c *DbContext) KnownStorageServers() []wire.StorageServer {
	c.Locations.mu.RLock()
	defer c.Locations.mu.RUnlock()
	seen := make(map[wire.UID]bool)
	var out []wire.StorageServer
	for _, e := range c.Locations.entries {
		for _, s := range e.servers {
			if !seen[s.ID] {
				seen[s.ID] = true
				out = append(out, s)
			}
		}
	}
	return out
}

// ConnectionString returns the connection string currently in use.
func (c *DbContext) ConnectionString() cluster.ConnectionString {
	c.recordMu.Lock()
	defer c.recordMu.Unlock()
	return c.record.Conn
}

// Record returns a copy of the connection record (cluster file path and connection string).
func (c *DbContext) Record() cluster.ConnectionRecord {
	c.recordMu.Lock()
	defer c.recordMu.Unlock()
	return c.record
}

// Coordinator returns the coordinator last contacted successfully, if any.
func (c *DbContext) Coordinator() (string, bool) {
	c.coordMu.Lock()
	defer c.coordMu.Unlock()
	return c.coordinator, c.coordinator != ""
}

// ---- coordinator monitor ------------------------------------------------

// monitorProxies keeps ClientDBInfo current by long-polling the coordinators
// (monitorProxies / monitorProxiesOneGeneration).
func (c *DbContext) monitorProxies() {
	defer c.wg.Done()
	var lastGRV, lastCommit []wire.UID
	persisted := c.ConnectionString()
	forwarding := false
generation:
	for {
		conn := c.ConnectionString()
		coords := slices.Clone(conn.Coordinators)
		rand.Shuffle(len(coords), func(i, j int) { coords[i], coords[j] = coords[j], coords[i] })
		var addresses []wire.NetAddr
		var hostnames []wire.Hostname
		for _, co := range conn.Coordinators {
			if co.IsHostname() {
				hostnames = append(hostnames, wire.Hostname{Host: co.Host, Port: co.Port, TLS: co.TLS})
			} else {
				addresses = append(addresses, wire.NetAddr{Addr: co.Addr, TLS: co.TLS})
			}
		}
		index, successIndex := 0, 0
		for {
			if c.ctx.Err() != nil {
				return
			}
			coord := coords[index]
			req := &wire.OpenDatabase{
				KnownID:      c.ClusterNow().ID,
				ClusterKey:   []byte(conn.Key),
				Coordinators: addresses,
				Hostnames:    hostnames,
			}
			info, protocol, err := c.openDatabase(coord, req)
			if c.ctx.Err() != nil {
				return
			}
			if err == nil {
				if info.Forward != nil {
					next, perr := cluster.ParseConnectionString(string(info.Forward))
					if perr == nil {
						slog.Info("fdb: coordinators moved; following forward", "new", next.String())
						c.recordMu.Lock()
						c.record.Conn = next
						forwarding = true
						c.recordMu.Unlock()
						c.Locations.InvalidateRange(nil, []byte("\xff\xff\xff"))
						continue generation
					}
					slog.Warn("fdb: ignoring malformed forwarded connection string")
				} else {
					if forwarding {
						c.recordMu.Lock()
						c.record.Persist(conn)
						c.recordMu.Unlock()
						persisted, forwarding = conn, false
					}
					grv := make([]wire.Endpoint, len(info.GRV))
					for i, p := range info.GRV {
						grv[i] = p.Endpoint
					}
					commit := make([]wire.Endpoint, len(info.Commit))
					for i, p := range info.Commit {
						commit[i] = p.Endpoint
					}
					grv, lastGRV = shrink(grv, lastGRV, maxGRVProxyConnections)
					commit, lastCommit = shrink(commit, lastCommit, maxCommitProxyConnections)
					provisional := len(info.Commit) > 0 && info.Commit[0].Provisional
					c.coordMu.Lock()
					c.coordinator = coord.String()
					c.coordMu.Unlock()
					c.publish(&ClusterInfo{ID: info.ID, Protocol: protocol, GRV: grv, Commit: commit, Provisional: provisional, ClusterID: info.ClusterID})
					successIndex = index
				}
				continue
			}
			slog.Debug("fdb: coordinator request failed", "coordinator", coord.String(), "error", err)
			index = (index + 1) % len(coords)
			if index == successIndex {
				// Every coordinator failed: adopt an externally updated cluster file, else wait.
				c.recordMu.Lock()
				stored, ok, serr := c.record.Stored()
				if ok && serr == nil && stored.String() != persisted.String() {
					c.record.Conn = stored
					persisted, forwarding = stored, false
					c.recordMu.Unlock()
					continue generation
				}
				c.recordMu.Unlock()
				if sleep(c.ctx, coordinatorReconnectionDelay) != nil {
					return
				}
			}
		}
	}
}

// shrink keeps at most max proxies, preferring the ones chosen last time
// (shrinkProxyList); it returns the kept endpoints and their tokens.
func shrink(eps []wire.Endpoint, last []wire.UID, limit int) ([]wire.Endpoint, []wire.UID) {
	if len(eps) > limit {
		var kept []wire.Endpoint
		for _, e := range eps {
			if len(kept) < limit && slices.Contains(last, e.Token) {
				kept = append(kept, e)
			}
		}
		if len(kept) < limit {
			var rest []wire.Endpoint
			for _, e := range eps {
				if !slices.ContainsFunc(kept, e.Equal) {
					rest = append(rest, e)
				}
			}
			rand.Shuffle(len(rest), func(i, j int) { rest[i], rest[j] = rest[j], rest[i] })
			kept = append(kept, rest[:min(len(rest), limit-len(kept))]...)
		}
		eps = kept
	}
	tokens := make([]wire.UID, len(eps))
	for i, e := range eps {
		tokens[i] = e.Token
	}
	return eps, tokens
}

// resolve returns the address of a coordinator, resolving hostnames.
func resolve(ctx context.Context, co cluster.Coordinator) (wire.NetAddr, error) {
	if co.Host == "" {
		return wire.NetAddr{Addr: co.Addr, TLS: co.TLS}, nil
	}
	addrs, err := net.DefaultResolver.LookupNetIP(ctx, "ip", co.Host)
	if err != nil || len(addrs) == 0 {
		return wire.NetAddr{}, transport.NotDelivered
	}
	return wire.NetAddr{Addr: netip.AddrPortFrom(pickCoordinatorAddress(addrs, rand.IntN), co.Port), TLS: co.TLS}, nil
}

// pickCoordinatorAddress matches pickOneAddress's default IPv6 preference,
// choosing afresh on every resolve even when the DNS answers have not changed.
// choose is injectable so every alternative can be exercised deterministically.
func pickCoordinatorAddress(addrs []netip.Addr, choose func(int) int) netip.Addr {
	ipv6 := 0
	for _, addr := range addrs {
		if addr.Unmap().Is6() {
			ipv6++
		}
	}
	if ipv6 == 0 {
		return addrs[choose(len(addrs))].Unmap()
	}
	index := choose(ipv6)
	for _, addr := range addrs {
		if addr.Unmap().Is6() {
			if index == 0 {
				return addr
			}
			index--
		}
	}
	panic("invalid coordinator address selection")
}

// openDatabase monitors proxies through the coordinator, or through its elected
// controller on releases that predate OpenDatabaseCoordRequest.
func (c *DbContext) openDatabase(co cluster.Coordinator, req *wire.OpenDatabase) (wire.ClientDBInfo, wire.ProtocolVersion, error) {
	addr, err := resolve(c.ctx, co)
	if err != nil {
		return wire.ClientDBInfo{}, 0, err
	}
	ep := wire.Endpoint{Addr: addr, Token: wire.WellKnown(wire.TokenOpenDatabase)}
	conn, err := c.Connection(c.ctx, ep)
	if err != nil {
		return wire.ClientDBInfo{}, 0, err
	}
	protocol := conn.ProtocolVersion()
	if protocol.Normalized() < wire.Protocol62.Normalized() {
		info, err := c.openControllerDatabase(addr, req)
		return info, protocol, err
	}
	info, err := transport.Call(c.ctx, conn, ep.Token, wire.Request[wire.ClientDBInfo](req))
	return info, protocol, storageCallError(c.ctx, c, ep, err)
}
