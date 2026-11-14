package wire

import (
	"encoding/binary"
	"math"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/can1357/fdbgo/internal/kv"
)

func TestReadVersionThrottleDurations(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for _, duration := range []float64{60, 0, -10, math.MaxFloat64} {
			msg := reply((GetReadVersion{}).ReplyFileID(), func(b *Builder) Off {
				tag := b.Bytes([]byte("hot"))
				b.StartTable(2)
				b.AddF64(0, 12)
				b.AddF64(1, duration)
				limits := b.EndTable()
				b.StartTable(2)
				b.AddOffset(0, tag)
				b.AddOffset(1, limits)
				pair := b.EndTable()
				mark := b.Mark()
				b.Push(pair)
				tags := b.Offsets(mark)
				b.StartTable(9)
				b.AddI64(1, 42)
				b.AddOffset(5, tags)
				return b.EndTable()
			})
			decoded, err := DecodeReply[ReadVersionReply](GetReadVersion{}, msg)
			if err != nil || len(decoded.Throttled) != 1 {
				t.Fatalf("duration %g: %+v, %v", duration, decoded, err)
			}
			want := duration
			if duration != math.MaxFloat64 {
				want += float64(time.Now().UnixNano()) / 1e9
			}
			got := decoded.Throttled[0]
			if got.Expiration != want || got.Rate != 12 || string(got.Tag) != "hot" {
				t.Fatalf("duration %g: %+v; want expiration %g", duration, got, want)
			}
			time.Sleep(time.Second)
			if decoded.Throttled[0].Expiration != want {
				t.Fatal("decoded deadline changed after delivery")
			}
		}
	})
}

func TestDecodeKeyServersValue(t *testing.T) {
	uidBytes := func(ids ...UID) []byte {
		var out []byte
		for _, id := range ids {
			out = binary.LittleEndian.AppendUint64(out, id.First)
			out = binary.LittleEndian.AppendUint64(out, id.Second)
		}
		return out
	}
	encode := func(version uint64, size int, src, dest []byte, shardIDs ...UID) []byte {
		out := binary.LittleEndian.AppendUint64(nil, version)
		out = binary.LittleEndian.AppendUint32(out, uint32(len(src)/size))
		out = append(out, src...)
		out = binary.LittleEndian.AppendUint32(out, uint32(len(dest)/size))
		out = append(out, dest...)
		return append(out, uidBytes(shardIDs...)...)
	}
	first, second, dest := UID{1, 2}, UID{256, 3}, UID{99, 4}
	tagRow := func(id UID, tag []byte) kv.KeyValue {
		return kv.KeyValue{
			Key:   append([]byte("\xff/serverTag/"), uidBytes(id)...),
			Value: append(binary.LittleEndian.AppendUint64(nil, 0x0fdb00b062010001), tag...),
		}
	}
	// Both tag bytes and UID bytewise key order differ from numeric UID order.
	firstTag, secondTag, destTag := []byte{0xff, 0x34, 0x12}, []byte{2, 0x34, 0x12}, []byte{3, 1, 0}
	tags := []kv.KeyValue{tagRow(second, secondTag), tagRow(dest, destTag), tagRow(first, firstTag)}
	tagged := encode(0x0fdb00b063010000, 3, append(slices.Clone(secondTag), firstTag...), destTag)
	for _, tc := range []struct {
		name  string
		value []byte
		want  []UID
	}{
		{"empty", nil, nil},
		{"uids", encode(0x0fdb00b062010001, 16, uidBytes(second, first), uidBytes(dest)), []UID{second, first}},
		{"tags", tagged, []UID{first, second}},
		{"shard-metadata", encode(0x0fdb00b072000000, 16, uidBytes(second, first), uidBytes(dest), UID{7, 8}, UID{9, 10}), []UID{second, first}},
		{"stationary-shard", encode(0x0fdb00b072000000, 16, uidBytes(first), nil, UID{7, 8}), []UID{first}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := DecodeKeyServersValue(tc.value, tags)
			if err != nil || !slices.Equal(got, tc.want) {
				t.Fatalf("got %v, %v; want %v", got, err, tc.want)
			}
			for n := 1; n < len(tc.value); n++ {
				if _, err := DecodeKeyServersValue(tc.value[:n], tags); err != ErrMalformed {
					t.Fatalf("truncated to %d bytes: %v", n, err)
				}
			}
		})
	}
	for _, tc := range []struct {
		name string
		tags []kv.KeyValue
	}{
		{"missing-source", tags[:2]},
		{"missing-destination", []kv.KeyValue{tags[0], tags[2]}},
		{"bad-tag-value", []kv.KeyValue{{Key: tags[0].Key, Value: []byte{1}}}},
		{"bad-tag-key", []kv.KeyValue{{Key: []byte("wrong-prefix"), Value: tags[0].Value}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := DecodeKeyServersValue(tagged, tc.tags); err != ErrMalformed {
				t.Fatalf("got %v; want malformed system data", err)
			}
		})
	}
}
