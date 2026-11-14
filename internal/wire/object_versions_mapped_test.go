package wire

import (
	"net/netip"
	"testing"
)

func TestObjectMappedNestedRangeUsesDefaultTableVector(t *testing.T) {
	// FDBTypes.h RangeResultRef casts to VectorRef<KeyValueRef>, whose default
	// strategy is FlatBuffers. Unlike GetKeyValuesReply it is NOT String.
	for _, p := range []ProtocolVersion{object71, object72, Protocol73, Protocol74} {
		msg := reply((&GetMappedKeyValues{}).ReplyFileID(), func(b *Builder) Off {
			key, value := b.String("mapped"), b.String("answer")
			b.StartTable(2)
			b.AddOffset(0, key)
			b.AddOffset(1, value)
			b.Push(b.EndTable())
			rows := b.Offsets(0)
			b.StartTable(6)
			b.AddOffset(0, rows)
			b.AddBool(1, true)
			rangeResult := b.EndTable()
			selectorKey := b.String("mapped")
			b.StartTable(3)
			b.AddOffset(0, selectorKey)
			b.AddI32(2, 1)
			selector := b.EndTable()
			b.StartTable(3)
			b.AddOffset(0, selector)
			b.AddOffset(1, selector)
			b.AddOffset(2, rangeResult)
			mappedRange := b.EndTable()
			originalKey, originalValue := b.String("original"), b.String("pointer")
			b.StartTable(2)
			b.AddOffset(0, originalKey)
			b.AddOffset(1, originalValue)
			original := b.EndTable()
			b.StartTable(3)
			b.AddOffset(0, original)
			b.AddUnion(1, 2, mappedRange)
			b.Push(b.EndTable())
			mappedRows := b.Offsets(0)
			b.StartTable(7)
			b.AddF64(0, 1)
			b.AddOffset(3, mappedRows)
			return b.EndTable()
		})
		r, err := DecodeReply[Lb[MappedPage]](&GetMappedKeyValues{}, msg, p)
		if err != nil || len(r.Value.Data) != 1 || len(r.Value.Data[0].Data) != 1 {
			t.Fatalf("%x: %+v %v", p, r, err)
		}
		row := r.Value.Data[0]
		if string(row.Key) != "original" || string(row.Data[0].Key) != "mapped" || string(row.Data[0].Value) != "answer" || !row.More {
			t.Fatalf("%x: %+v", p, row)
		}
	}
}

func TestObject62StorageIndependentStreams(t *testing.T) {
	var b Builder
	b.Reset()
	var streams [14]Off
	for _, slot := range []int{3, 4, 5, 6, 7, 13} {
		streams[slot] = writeEndpoint(&b, Endpoint{Addr: NetAddr{Addr: netip.MustParseAddrPort("127.0.0.1:4500")}, Token: UID{uint64(slot * 17), uint64(slot)}})
	}
	b.StartTable(14)
	b.AddUID(0, UID{1, 2})
	for _, slot := range []int{3, 4, 5, 6, 7, 13} {
		b.AddOffset(slot, streams[slot])
	}
	ssi := b.EndTable()
	root, err := Root(b.FinishRequest(ssi, 15302073), object62)
	if err != nil {
		t.Fatal(err)
	}
	table, err := root.mustTable(0)
	if err != nil {
		t.Fatal(err)
	}
	s, err := storageServer(table)
	if err != nil || s.ID != (UID{1, 2}) {
		t.Fatal(s, err)
	}
	for _, pair := range [][2]int{{0, 3}, {1, 4}, {2, 5}, {3, 6}, {4, 7}, {10, 13}} {
		if token := s.Base.Adjusted(uint32(pair[0])).Token; token != (UID{uint64(pair[1] * 17), uint64(pair[1])}) {
			t.Fatal(pair, token)
		}
	}
}
