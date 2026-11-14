package native

import (
	"context"
	"errors"
	"math"
	"testing"

	"github.com/can1357/fdbgo/internal/fdberr"
	"github.com/can1357/fdbgo/internal/wire"
)

func TestCommitCostEstimation(t *testing.T) {
	mutations := []wire.Mutation{
		{Kind: 0, Param1: []byte("k"), Param2: make([]byte, 4096)},
		{Kind: 5}, // NoOp is not charged.
		{Kind: 1, Param1: []byte("a"), Param2: []byte("z")},
		{Kind: 20, Param1: []byte("atomic")},
	}
	for _, expensive := range []bool{false, true} {
		calls := 0
		cost, err := estimateCommitCosts(context.Background(), mutations, expensive, func(_ context.Context, begin, end []byte) (uint64, error) {
			calls++
			if string(begin) != "a" || string(end) != "z" {
				t.Fatal("incorrect clear range")
			}
			return 8193, nil
		}, func() float64 { return 0.999 })
		if err != nil || cost == nil {
			t.Fatal(cost, err)
		}
		if calls != 1 || cost.OpsCount != 3 || cost.WriteCosts != 6*16384 {
			t.Fatalf("cost=%+v calls=%d", cost, calls)
		}
		if len(cost.ClearCosts) != 1 || cost.ClearCosts[0] != (wire.ClearCost{MutationIndex: 2, Cost: 3 * 16384}) {
			t.Fatalf("clear costs: %+v", cost.ClearCosts)
		}
		want := uint32(0)
		if expensive {
			want = 1
		}
		if cost.ExpensiveCostEstCount != want {
			t.Fatal(cost.ExpensiveCostEstCount)
		}
	}
}

func TestCommitCostEmptyAndFailures(t *testing.T) {
	clear := []wire.Mutation{{Kind: 1}}
	zero := func(context.Context, []byte, []byte) (uint64, error) { return 0, nil }
	random := func() float64 { return 0 }
	cost, err := estimateCommitCosts(context.Background(), clear, false, zero, random)
	if cost != nil || err != nil {
		t.Fatal(cost, err)
	}
	cost, err = estimateCommitCosts(context.Background(), clear, true, zero, random)
	if err != nil || cost.WriteCosts != 16384 || cost.ExpensiveCostEstCount != 1 {
		t.Fatal(cost, err)
	}
	_, err = estimateCommitCosts(context.Background(), clear, true, func(context.Context, []byte, []byte) (uint64, error) { return 0, fdberr.OperationCancelled }, random)
	if !errors.Is(err, fdberr.OperationCancelled) {
		t.Fatal(err)
	}
}

func TestCommitCostTagAndPriorityGate(t *testing.T) {
	// Nil shared state makes accidental location/metrics I/O fail immediately.
	for _, tc := range []struct {
		tags     [][]byte
		priority Priority
	}{
		{nil, PriorityDefault},
		{[][]byte{[]byte("tag")}, PriorityImmediate},
	} {
		req := &wire.Commit{Tags: tc.tags, Mutations: []wire.Mutation{{Kind: 1}}}
		if err := estimateTaggedCommitCosts(context.Background(), nil, &NativeOptions{Priority: tc.priority}, req); err != nil || req.CostEstimation != nil {
			t.Fatal(req, err)
		}
	}
	req := &wire.Commit{Tags: [][]byte{[]byte("tag")}, Mutations: []wire.Mutation{{Kind: 0, Param1: []byte("key")}}}
	if err := estimateTaggedCommitCosts(context.Background(), nil, &NativeOptions{}, req); err != nil || req.CostEstimation == nil {
		t.Fatal(req, err)
	}
}

func TestCommitCostIncompleteShards(t *testing.T) {
	for _, tc := range []struct {
		shards int
		want   uint64
	}{
		{0, 0}, {1, 4096}, {2, 8192}, {3, 10008192}, {5, 30008192},
	} {
		if got := incompleteClearBytes(tc.shards, 10000000); got != tc.want {
			t.Fatal(tc.shards, got, tc.want)
		}
	}
	for _, tc := range []struct{ bytes, want uint64 }{
		{0, 16384}, {1, 16384}, {4096, 16384}, {4097, 32768},
	} {
		if got := writeOperationCost(tc.bytes); got != tc.want {
			t.Fatal(tc.bytes, got, tc.want)
		}
	}
}

func TestCommitCostShardSmoothing(t *testing.T) {
	var s commitCostState
	s.update(100)
	if s.estimate != 10000000 {
		t.Fatal(s.estimate)
	}
	s.total = 20000000
	s.update(105)
	want := 20000000 - 10000000/math.E
	if math.Abs(s.estimate-want) > 1e-6 {
		t.Fatal(s.estimate, want)
	}
	s.observe(0)
	if s.total != 20000000 {
		t.Fatal(s.total)
	}
}
