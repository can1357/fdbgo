package native

import (
	"bytes"
	"context"
	"sync"

	"github.com/can1357/fdbgo/internal/kv"
	"github.com/can1357/fdbgo/internal/ryw"
	"github.com/can1357/fdbgo/internal/transport"
	"github.com/can1357/fdbgo/internal/wire"
)

// valueBatchRead owns one cached point read and its first queue-model ticket.
// The larger retry state is constructed only when the first attempt fails.
type valueBatchRead struct {
	servers []wire.StorageServer
	req     wire.GetValue
	ticket  transport.Ticket
	ep      wire.Endpoint
	batch   int
	err     error
	outcome lbOutcome
	started bool
}

// GetMany reads keys at this reader's snapshot and returns one outcome per
// input, in order. Cached, ordinary single-replica reads share synchronous
// dispatch and collection; other reads retain Get's full routing semantics.
func (r Reader) GetMany(ctx context.Context, keys [][]byte) []kv.ValueResult {
	results := make([]kv.ValueResult, len(keys))
	if len(keys) == 0 {
		return results
	}
	ctx, done := r.readContext(ctx)
	defer done()
	if ctx.Err() != nil {
		for i := range results {
			results[i].Err = cancelErr(ctx)
		}
		return results
	}
	if r.params.replicaCheck || r.params.provisional || r.params.hasReadOptions || len(r.params.readTags) != 0 {
		r.getManyFallback(ctx, keys, results)
		return results
	}
	c := r.db()
	reads := make([]valueBatchRead, len(keys))
	for i, key := range keys {
		if len(key) > ryw.MaxReadKeySize(key) || bytes.Equal(key, kv.MetadataVersionKey) {
			r.getManyFallback(ctx, keys, results)
			return results
		}
		loc, ok := c.Locations.Get(key, false)
		if ok && c.locationNeedsRefresh(loc, ssGetValue) {
			// The refresh check updates its throttle; invalidate now so the
			// fallback cannot mistake that update for a reusable location.
			c.Locations.Invalidate(key, false)
			ok = false
		}
		if !ok || len(loc.Servers) != 1 || c.Failures.Failed(loc.Servers[0].Base.Adjusted(ssGetValue), c.TLS) {
			r.getManyFallback(ctx, keys, results)
			return results
		}
		reads[i].servers = loc.Servers
	}
	version, err := r.shared.version(ctx)
	if err != nil {
		for i := range results {
			results[i].Err = err
		}
		return results
	}
	calls := make([]transport.BatchCall[wire.Lb[[]byte]], 0, len(keys))
	for i := range reads {
		read := &reads[i]
		read.batch = -1
		read.req = wire.GetValue{Key: keys[i], Ctx: r.readCtx(version)}
		read.ep = read.servers[0].Base.Adjusted(ssGetValue)
		if c.Failures.Failed(read.ep, c.TLS) {
			// A replica can fail between the cache check and dispatch. Let
			// the existing loop handle recovery and its all-failed delay.
			continue
		}
		read.ticket = c.Model.Start(read.ep.Token.First)
		read.started = true
		conn, err := c.Connection(ctx, read.ep)
		if err != nil {
			read.err = err
			continue
		}
		read.batch = len(calls)
		calls = append(calls, transport.BatchCall[wire.Lb[[]byte]]{
			Connection: conn, Destination: read.ep.Token, Request: &read.req,
		})
	}
	replies := transport.CallBatch(ctx, calls)
	// End every first-attempt ticket before entering a potentially slow retry.
	// CallBatch has already released every transport slot, including on cancel.
	for i := range reads {
		read := &reads[i]
		if !read.started {
			continue
		}
		rep := lbReply[[]byte]{err: read.err}
		if read.batch >= 0 {
			reply := replies[read.batch]
			rep = lbReply[[]byte]{v: reply.Value, err: reply.Err}
		}
		rep.err = storageCallError(ctx, c, read.ep, rep.err)
		if ctx.Err() != nil {
			read.ticket.Release()
			read.outcome = lbFail
			results[i].Err = cancelErr(ctx)
			continue
		}
		c.Model.FirstReplied()
		results[i].Value, read.outcome, results[i].Err = processLBReply(c, rep, &read.ticket, false)
	}
	for i := range reads {
		read := &reads[i]
		if !read.started || read.outcome == lbRetry {
			// A singleton plan has zero-valued ordering and hedge state.
			// Failed selection has already exhausted its only alternative.
			run := lbRun[[]byte]{
				c: c, servers: read.servers, stream: ssGetValue, req: &read.req,
				n: 1, triedAll: !read.started,
			}
			if read.started {
				run.attempted()
				run.advance()
			}
			results[i].Value, _, results[i].Err = run.run(ctx, nil)
		}
		if isLocationError(results[i].Err) {
			c.Locations.Invalidate(keys[i], false)
			if err := sleep(ctx, wrongShardServerDelay); err != nil {
				results[i].Err = err
				continue
			}
			results[i].Value, results[i].Err = r.getValue(ctx, keys[i], version)
		}
	}
	return results
}

// getManyFallback retains the independent Get paths for metadata keys, read
// options, cache misses and replicated shards. All reads are joined on errors.
func (r Reader) getManyFallback(ctx context.Context, keys [][]byte, results []kv.ValueResult) {
	var wg sync.WaitGroup
	get := func(i int) {
		if ctx.Err() != nil {
			results[i].Err = cancelErr(ctx)
			return
		}
		results[i].Value, results[i].Err = r.getValueWait(ctx, keys[i])
	}
	for i := 1; i < len(keys); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			get(i)
		}()
	}
	get(0)
	wg.Wait()
}
