package native

import (
	"context"
	"math"
	"math/rand/v2"
	"sync"

	"github.com/can1357/fdbgo/internal/transport"
	"github.com/can1357/fdbgo/internal/wire"
)

const commitSampleCost = 100

// commitCostState follows DatabaseContext::smoothMidShardSize (5-second
// exponential smoothing), including its 10 MB initial estimate.
type commitCostState struct {
	mu                    sync.Mutex
	initialized           bool
	total, estimate, last float64
}

func (s *commitCostState) update(now float64) {
	if !s.initialized {
		s.initialized = true
		s.total, s.estimate = 10000000, 10000000
	}
	s.estimate += (s.total - s.estimate) * -math.Expm1(-(now-s.last)/5)
	s.last = now
}

func (s *commitCostState) observe(bytes int64) {
	if bytes <= 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.update(transport.Now())
	s.total = float64(bytes)
}

func (s *commitCostState) midShardBytes() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.update(transport.Now())
	return uint64(s.estimate)
}

// writeOperationCost is getWriteOperationCost: page-rounded bytes times the
// read/write fungibility ratio, with at least one 4096-byte page.
func writeOperationCost(bytes uint64) uint64 {
	if bytes == 0 {
		return 4 * 4096
	}
	return 4 * 4096 * ((bytes-1)/4096 + 1)
}

func estimateTaggedCommitCosts(ctx context.Context, shared *TxnShared, opts *NativeOptions, req *wire.Commit) error {
	if len(req.Tags) == 0 || opts.Priority == PriorityImmediate {
		return nil
	}
	reader := Reader{shared: shared, params: opts.readParams()}
	clearBytes := func(ctx context.Context, begin, end []byte) (uint64, error) {
		if opts.ExpensiveClearCostEstimation {
			n, err := reader.EstimatedRangeSize(ctx, begin, end)
			return uint64(n), err
		}
		locs, err := reader.rangeLocations(ctx, begin, end, 1<<31-1, false, wire.SSStreamGetShardState)
		if err != nil {
			return 0, err
		}
		var mid uint64
		if len(locs) > 2 {
			mid = shared.DB.commitCosts.midShardBytes()
		}
		return incompleteClearBytes(len(locs), mid), nil
	}
	cost, err := estimateCommitCosts(ctx, req.Mutations, opts.ExpensiveClearCostEstimation, clearBytes, rand.Float64)
	req.CostEstimation = cost
	return err
}

func incompleteClearBytes(shards int, midShardBytes uint64) uint64 {
	if shards == 0 {
		return 0
	}
	if shards == 1 {
		return 4096
	}
	return 4096*2 + uint64(shards-2)*midShardBytes
}

// estimateCommitCosts keeps original mutation indices: the proxy consumes clear
// costs while splitting mutations among their storage servers. Transaction and
// clear-operation sampling/scaling match NativeAPI::estimateCommitCosts.
func estimateCommitCosts(ctx context.Context, mutations []wire.Mutation, expensive bool, clearBytes func(context.Context, []byte, []byte) (uint64, error), random func() float64) (*wire.CommitCostEstimation, error) {
	var cost wire.CommitCostEstimation
	for i, m := range mutations {
		switch m.Kind {
		case 0, 2, 6, 7, 8, 9, 12, 13, 14, 15, 16, 17, 18, 19, 20:
			cost.OpsCount++
			cost.WriteCosts += writeOperationCost(uint64(len(m.Param1) + len(m.Param2)))
		case 1:
			cost.OpsCount++
			n, err := clearBytes(ctx, m.Param1, m.Param2)
			if err != nil {
				return nil, err
			}
			// The inexpensive path skips ranges for which there are no locations.
			if n == 0 && !expensive {
				continue
			}
			c := writeOperationCost(n)
			cost.ClearCosts = append(cost.ClearCosts, wire.ClearCost{MutationIndex: int32(i), Cost: c})
			cost.WriteCosts += c
			if expensive {
				cost.ExpensiveCostEstCount++
			}
		}
	}
	if cost.WriteCosts == 0 || random() > float64(cost.WriteCosts)/commitSampleCost {
		return nil, nil
	}
	sampled := cost.ClearCosts[:0]
	denominator := min(float64(cost.WriteCosts), float64(commitSampleCost))
	for _, clear := range cost.ClearCosts {
		if random() < float64(clear.Cost)/denominator {
			clear.Cost = max(clear.Cost, uint64(commitSampleCost))
			sampled = append(sampled, clear)
		}
	}
	cost.ClearCosts = sampled
	return &cost, nil
}
