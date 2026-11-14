package wire

import (
	"bytes"
	"testing"

	"github.com/can1357/fdbgo/internal/kv"
)

func version80RequestTable(t *testing.T, req Message) Table {
	t.Helper()
	b := Builder{Protocol: Protocol80}
	b.Reset()
	off := req.Encode(&b, UID{7, 8})
	return requestTable(t, b.FinishRequest(off, req.FileID()))
}

func version80UnionUID(t *testing.T, field Field) UID {
	t.Helper()
	at, err := deref(field.msg, field.pos)
	if err != nil {
		t.Fatal(err)
	}
	first, err := le64(field.msg, at)
	if err != nil {
		t.Fatal(err)
	}
	second, err := le64(field.msg, at+8)
	if err != nil {
		t.Fatal(err)
	}
	return UID{first, second}
}

// 8.0.0 StorageServerInterface.h removes tenantInfo before options. Exercise
// populated unions: omitted read options would hide the incompatible slot shift.
func TestVersion80ReadOptionsSlots(t *testing.T) {
	id := UID{1, 2}
	opts := DefaultReadOpts()
	opts.DebugID = id
	opts.LockAware = true
	ctx := ReadCtx{Version: 42, Tags: [][]byte{[]byte("tag")}, Options: opts, HasOptions: true}
	for _, tc := range []struct {
		name                 string
		req                  Message
		tags, options, reply int
	}{
		{"value", &GetValue{Key: []byte("k"), Ctx: ctx}, 2, 6, 4},
		{"key", &GetKey{Sel: kv.KeySelector{Key: []byte("k"), Offset: 1}, Ctx: ctx}, 2, 6, 4},
		{"range", &GetKeyValues{Ctx: ctx, Limit: 10, LimitBytes: 100}, 5, 9, 7},
		{"mapped", &GetMappedKeyValues{Ctx: ctx, Mapper: []byte("mapper"), Limit: 10, LimitBytes: 100}, 6, 10, 8},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tab := version80RequestTable(t, tc.req)
			tag, f, err := tab.Union(tc.tags)
			if err != nil || tag != 1 {
				t.Fatal(tag, err)
			}
			blob, err := f.Bytes()
			if err != nil || !bytes.Equal(blob, []byte{3, 't', 'a', 'g'}) {
				t.Fatal(blob, err)
			}
			tag, f, err = tab.Union(tc.options)
			if err != nil || tag != 1 {
				t.Fatal(tag, err)
			}
			options, err := f.Table()
			if err != nil {
				t.Fatal(err)
			}
			readType, err := options.I32(0)
			if err != nil || readType != 3 {
				t.Fatal(readType, err)
			}
			lockAware, err := options.Bool(6)
			if err != nil || !lockAware {
				t.Fatal(lockAware, err)
			}
			tag, f, err = options.Union(2)
			if err != nil || tag != 1 {
				t.Fatal(tag, err)
			}
			if debugID := version80UnionUID(t, f); debugID != id {
				t.Fatal(debugID)
			}
			rp, err := tab.mustTable(tc.reply)
			if err != nil {
				t.Fatal(err)
			}
			token, err := rp.UID(0)
			if err != nil || token != (UID{7, 8}) {
				t.Fatal(token, err)
			}
		})
	}
}

// SplitRange.limit is not optional: Flow's LoadMember zero-initializes absent
// scalar fields, overriding the C++ declaration's -1 initializer.
func TestVersion80ScalarTenantReplacements(t *testing.T) {
	split := version80RequestTable(t, &SplitRange{Range: kv.KeyRange{Begin: []byte("a"), End: []byte("z")}, ChunkSize: 4096})
	limit, err := split.I32(3)
	if err != nil || limit != -1 {
		t.Fatal(limit, err)
	}
	metrics := version80RequestTable(t, &WaitMetrics{})
	legacy, err := metrics.I64(4)
	if err != nil || legacy != 0 {
		t.Fatal(legacy, err)
	}
	locations := version80RequestTable(t, GetKeyServerLocations{Begin: []byte("a"), End: []byte{}, Limit: 10, Reverse: true})
	legacy, err = locations.I64(7)
	if err != nil || legacy != LatestVersion {
		t.Fatal(legacy, err)
	}
	tag, f, err := locations.Union(1)
	if err != nil || tag != 1 {
		t.Fatal(tag, err)
	}
	end, err := f.Bytes()
	if err != nil || len(end) != 0 {
		t.Fatal(end, err)
	}
}

func TestVersion80WatchOptionalValues(t *testing.T) {
	for _, value := range [][]byte{nil, {}, []byte("value")} {
		tab := version80RequestTable(t, &WatchValue{Key: []byte("key"), Value: value, Version: 42, DebugID: UID{1, 2}})
		tag, field, err := tab.Union(1)
		if err != nil {
			t.Fatal(err)
		}
		if value == nil {
			if tag != 0 {
				t.Fatal(tag)
			}
		} else {
			if tag != 1 {
				t.Fatal(tag)
			}
			got, err := field.Bytes()
			if err != nil || !bytes.Equal(got, value) {
				t.Fatal(got, err)
			}
		}
		version, err := tab.I64(3)
		if err != nil || version != 42 {
			t.Fatal(version, err)
		}
		tag, field, err = tab.Union(6)
		if err != nil || tag != 1 {
			t.Fatal(tag, err)
		}
		if debugID := version80UnionUID(t, field); debugID != (UID{1, 2}) {
			t.Fatal(debugID)
		}
	}
}

func TestVersion80CommitIdempotencySlotAndUnflaggedMutation(t *testing.T) {
	id := []byte("0123456789abcdef")
	tab := version80RequestTable(t, &Commit{
		ReadSnapshot: 42, IdempotencyID: id, Tags: [][]byte{[]byte("tag")},
		Mutations:      []Mutation{{Kind: 0, Param1: []byte("key"), Param2: []byte("value")}},
		CostEstimation: &CommitCostEstimation{OpsCount: 1, WriteCosts: 1 << 40, ClearCosts: []ClearCost{{MutationIndex: 0, Cost: 1 << 39}}},
	})
	got, err := tab.Bytes(10)
	if err != nil || !bytes.Equal(got, id) {
		t.Fatal(got, err)
	}
	tx, err := tab.mustTable(0)
	if err != nil {
		t.Fatal(err)
	}
	mutations, err := tx.Vector(2)
	if err != nil || mutations.Len() != 1 {
		t.Fatal(mutations, err)
	}
	mutation, err := mutations.Table(0)
	if err != nil {
		t.Fatal(err)
	}
	kind, err := mutation.U8(0)
	if err != nil || kind != 0 {
		t.Fatal(kind, err)
	}
	value, err := mutation.Bytes(2)
	if err != nil || string(value) != "value" {
		t.Fatal(value, err)
	}
	tag, field, err := tab.Union(5)
	if err != nil || tag != 1 {
		t.Fatal(tag, err)
	}
	cost, err := field.Table()
	if err != nil {
		t.Fatal(err)
	}
	writeCost, err := cost.U64(1)
	if err != nil || writeCost != 1<<40 {
		t.Fatal(writeCost, err)
	}
	expire := version80RequestTable(t, ExpireIdempotencyID{Version: 42, BatchIndexHighByte: 255})
	version, err := expire.I64(0)
	if err != nil || version != 42 {
		t.Fatal(version, err)
	}
	batch, err := expire.U8(1)
	if err != nil || batch != 255 {
		t.Fatal(batch, err)
	}
}

func TestVersion80ClientDBInfoClusterID(t *testing.T) {
	id := UID{11, 22}
	msg := reply((OpenDatabase{}).ReplyFileID(), func(b *Builder) Off {
		b.StartTable(12)
		b.AddUID(6, id)
		b.AddBool(8, true) // nativeCdcEnabled, not a legacy encryption key proxy.
		b.AddI32(9, 8)     // nativeCdcTagCount, not the legacy cluster ID.
		return b.EndTable()
	})
	info, err := DecodeReply[ClientDBInfo](OpenDatabase{}, msg, Protocol80)
	if err != nil || info.ClusterID != id {
		t.Fatal(info, err)
	}
}
