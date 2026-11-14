package wire

import (
	"errors"
	"testing"
)

func TestLocationCompactKeyRange(t *testing.T) {
	for _, tc := range []struct {
		name, begin, end, wantBegin, wantEnd string
		invalid                              bool
	}{
		{"compact", "key\x00", "", "key", "key\x00", false},
		{"empty-key", "\x00", "", "", "\x00", false},
		{"ordinary", "a", "z", "a", "z", false},
		{"empty", "", "", "", "", false},
		{"missing-nul", "key", "", "", "", true},
		{"inverted", "z", "a", "", "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			msg := reply((GetKeyServerLocations{}).ReplyFileID(), func(b *Builder) Off {
				kr := keyRange(b, []byte(tc.begin), []byte(tc.end))
				b.StartTable(2)
				b.AddOffset(0, kr)
				pair := b.EndTable()
				mark := b.Mark()
				b.Push(pair)
				pairs := b.Offsets(mark)
				b.StartTable(1)
				b.AddOffset(0, pairs)
				return b.EndTable()
			})
			locs, err := DecodeReply[[]ShardLocation](GetKeyServerLocations{}, msg)
			if tc.invalid {
				if !errors.Is(err, ErrMalformed) {
					t.Fatal(locs, err)
				}
				return
			}
			if err != nil || len(locs) != 1 {
				t.Fatal(locs, err)
			}
			if string(locs[0].Range.Begin) != tc.wantBegin || string(locs[0].Range.End) != tc.wantEnd {
				t.Fatal(locs[0].Range)
			}
		})
	}
}

func TestCommitCostWireSchema(t *testing.T) {
	req := &Commit{Tags: [][]byte{[]byte("tag")}, CostEstimation: &CommitCostEstimation{
		OpsCount: 3, WriteCosts: 1 << 40, ExpensiveCostEstCount: 1,
		ClearCosts: []ClearCost{{MutationIndex: 2, Cost: 1 << 39}},
	}}
	tab := requestTable(t, encode[CommitReply](req))
	tag, err := tab.U8(5)
	if err != nil || tag != 1 {
		t.Fatal(tag, err)
	}
	c, err := tab.mustTable(6)
	if err != nil {
		t.Fatal(err)
	}
	ops, err := c.I32(0)
	if err != nil || ops != 3 {
		t.Fatal(ops, err)
	}
	cost, err := c.U64(1)
	if err != nil || cost != 1<<40 {
		t.Fatal(cost, err)
	}
	expensive, err := c.U32(3)
	if err != nil || expensive != 1 {
		t.Fatal(expensive, err)
	}
	clears, err := c.Vector(2)
	if err != nil || clears.Len() != 1 {
		t.Fatal(clears, err)
	}
	clear, err := clears.Table(0)
	if err != nil {
		t.Fatal(err)
	}
	idx, err := clear.I32(0)
	if err != nil || idx != 2 {
		t.Fatal(idx, err)
	}
	cost, err = clear.U64(1)
	if err != nil || cost != 1<<39 {
		t.Fatal(cost, err)
	}
	tag, err = tab.U8(7)
	if err != nil || tag != 1 {
		t.Fatal(tag, err)
	}
	absent := requestTable(t, encode[CommitReply](&Commit{}))
	tag, err = absent.U8(5)
	if err != nil || tag != 0 {
		t.Fatal(tag, err)
	}
}

func TestReadVersionMidShardSize(t *testing.T) {
	msg := reply((GetReadVersion{}).ReplyFileID(), func(b *Builder) Off {
		b.StartTable(7)
		b.AddI64(6, 12345678)
		return b.EndTable()
	})
	r, err := DecodeReply[ReadVersionReply](GetReadVersion{}, msg)
	if err != nil || r.MidShardSize != 12345678 {
		t.Fatal(r, err)
	}
}
