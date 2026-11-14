package native

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"net/netip"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/can1357/fdbgo/internal/fdberr"
	"github.com/can1357/fdbgo/internal/kv"
	"github.com/can1357/fdbgo/internal/wire"
)

func addressServerListValue(id wire.UID, plain, tls wire.NetAddr) []byte {
	var b wire.Builder
	b.Reset()
	address := func(a wire.NetAddr) wire.Off {
		var ip wire.Off
		if a.Addr.Addr().Is4() {
			v := a.Addr.Addr().As4()
			ip = b.ScalarU32(binary.BigEndian.Uint32(v[:]))
			b.StartTable(2)
			b.AddUnion(0, 1, ip)
		} else {
			v := a.Addr.Addr().As16()
			ip = b.Bytes(v[:])
			b.StartTable(2)
			b.AddUnion(0, 2, ip)
		}
		ip = b.EndTable()
		b.StartTable(4)
		b.AddOffset(0, ip)
		b.AddU16(1, a.Addr.Port())
		if a.TLS {
			b.AddU16(2, 2)
		}
		return b.EndTable()
	}
	primary, secondary := address(plain), address(tls)
	b.StartTable(3)
	b.AddOffset(0, primary)
	b.AddUnion(1, 1, secondary)
	addresses := b.EndTable()
	b.StartTable(2)
	b.AddOffset(0, addresses)
	b.AddUID(1, wire.UID{First: 88, Second: 99})
	ep := b.EndTable()
	b.StartTable(1)
	b.AddOffset(0, ep)
	stream := b.EndTable()
	b.StartTable(6)
	b.AddUID(0, id)
	b.AddOffset(2, stream)
	b.AddBool(5, true)
	ssi := b.EndTable()
	return append(binary.LittleEndian.AppendUint64(nil, 0x1fdb00b074000000), b.FinishRequest(ssi, 15302073)...)
}

func addressRangeReply(rows []kv.KeyValue) func(*wire.Builder) []byte {
	return func(b *wire.Builder) []byte {
		blob := binary.LittleEndian.AppendUint32(nil, uint32(len(rows)))
		for _, row := range rows {
			blob = binary.LittleEndian.AppendUint32(blob, uint32(len(row.Key)))
			blob = append(blob, row.Key...)
			blob = binary.LittleEndian.AppendUint32(blob, uint32(len(row.Value)))
			blob = append(blob, row.Value...)
		}
		data := b.Bytes(blob)
		b.StartTable(7)
		b.AddOffset(3, data)
		return lifecycleReply(b, b.EndTable(), (&wire.GetKeyValues{}).ReplyFileID())
	}
}

// addressSnapshotReader serves system metadata only. The cached location is
// deliberately this routing server, not any of the snapshot's returned hosts.
func addressSnapshotReader(t *testing.T, version int64, tls bool, failure string) (*Reader, *[3]atomic.Int64) {
	t.Helper()
	var calls [3]atomic.Int64
	var ep wire.Endpoint
	uidKey := func(prefix string, id wire.UID) []byte {
		key := binary.LittleEndian.AppendUint64([]byte(prefix), id.First)
		return binary.LittleEndian.AppendUint64(key, id.Second)
	}
	server := newNativeTestServer(t, func(c *nativeTestConn, dest wire.UID, msg []byte) {
		request, err := lifecycleRequestTable(msg)
		if err != nil {
			t.Error(err)
			return
		}
		stage, versionSlot, replySlot := 2, 1, 4
		var begin []byte
		if dest == ep.Adjusted(ssGetKeyValues).Token {
			versionSlot, replySlot = 2, 7
			sel, ok, err := request.Table(0)
			if err != nil || !ok {
				t.Errorf("range selector: %v, %v", ok, err)
				return
			}
			begin, err = sel.Bytes(0)
			if err != nil {
				t.Error(err)
				return
			}
			switch {
			case bytes.HasPrefix(begin, []byte("\xff/serverTag/")):
				stage = 0
			case bytes.HasPrefix(begin, []byte("\xff/keyServers/")):
				stage = 1
			default:
				t.Errorf("unexpected system range: %q", begin)
				return
			}
			offset, _ := sel.I32(2)
			if offset != 0 {
				t.Errorf("range begin must select the preceding boundary: offset %d", offset)
			}
			end, ok, err := request.Table(1)
			if err != nil || !ok {
				t.Errorf("range end selector: %v, %v", ok, err)
				return
			}
			endKey, _ := end.Bytes(0)
			endOffset, _ := end.I32(2)
			wantEnd := begin
			if stage == 0 {
				wantEnd = []byte("\xff/serverTag0\x00")
				if string(begin) != "\xff/serverTag/\x00" {
					t.Errorf("serverTag begin = %q", begin)
				}
			}
			if endOffset != 1 || !bytes.Equal(endKey, wantEnd) {
				t.Errorf("range end = %q offset %d; want %q offset 1", endKey, endOffset, wantEnd)
			}
			if limit, _ := request.I32(3); stage == 1 && limit != 1 {
				t.Errorf("keyServers row limit = %d; want 1", limit)
			}
		} else if dest != ep.Token {
			t.Errorf("unexpected endpoint: %v", dest)
			return
		}
		calls[stage].Add(1)
		v, err := request.I64(versionSlot)
		if err != nil || v != version {
			t.Errorf("system read version = %d, %v; want %d", v, err, version)
		}
		token, err := nativeRequestReplyToken(msg, replySlot)
		if err != nil {
			t.Error(err)
			return
		}
		if version == 1 || failure == []string{"tag-read", "keyservers-read", "serverlist-read"}[stage] {
			fileID := (&wire.GetValue{}).ReplyFileID()
			if dest == ep.Adjusted(ssGetKeyValues).Token {
				fileID = (&wire.GetKeyValues{}).ReplyFileID()
			}
			if err := c.send(token, lifecycleErrorReply(fdberr.TransactionTooOld, fileID)); err != nil {
				t.Error(err)
			}
			return
		}
		// The same source tag belongs to different servers at the two snapshots.
		source, destination := wire.UID{First: uint64(version), Second: 123}, wire.UID{First: 99, Second: 456}
		var response func(*wire.Builder) []byte
		switch stage {
		case 0:
			tags := []kv.KeyValue{
				{Key: uidKey("\xff/serverTag/", source), Value: append(binary.LittleEndian.AppendUint64(nil, 0x0fdb00b062010001), 0xff, 0x34, 0x12)},
				{Key: uidKey("\xff/serverTag/", destination), Value: append(binary.LittleEndian.AppendUint64(nil, 0x0fdb00b062010001), 2, 0x34, 0x12)},
			}
			if failure == "missing-tag" {
				tags = tags[1:]
			}
			response = addressRangeReply(tags)
		case 1:
			value := binary.LittleEndian.AppendUint64(nil, 0x0fdb00b063010000)
			value = append(binary.LittleEndian.AppendUint32(value, 1), 0xff, 0x34, 0x12)
			value = append(binary.LittleEndian.AppendUint32(value, 1), 2, 0x34, 0x12)
			key := []byte("\xff/keyServers/a")
			if bytes.Compare(begin, []byte("\xff/keyServers/\xff\xff")) >= 0 {
				key, value = []byte("\xff/keyServers/\xff\xff"), nil
			}
			rows := []kv.KeyValue{{Key: key, Value: value}}
			if failure == "missing-shard" {
				rows = nil
			}
			response = addressRangeReply(rows)
		case 2:
			key, err := request.Bytes(0)
			if err != nil || !bytes.Equal(key, uidKey("\xff/serverList/", source)) {
				t.Errorf("serverList key = %q, %v; want source, not relocation destination", key, err)
			}
			plain := wire.NetAddr{Addr: netip.MustParseAddrPort(fmt.Sprintf("192.0.2.%d:4500", version))}
			tls := wire.NetAddr{Addr: netip.MustParseAddrPort(fmt.Sprintf("[2001:db8::%d]:4501", version)), TLS: true}
			value := addressServerListValue(source, plain, tls)
			if failure == "malformed-interface" {
				value = value[:12]
			}
			response = func(b *wire.Builder) []byte {
				stored := b.Bytes(value)
				b.StartTable(6)
				if failure != "missing-interface" {
					b.AddUnion(3, 1, stored)
				}
				return lifecycleReply(b, b.EndTable(), (&wire.GetValue{}).ReplyFileID())
			}
		}
		if err := c.send(token, response); err != nil {
			t.Errorf("system metadata reply: %v", err)
		}
	})
	ep = wire.Endpoint{Addr: server.addr(), Token: wire.UID{First: 987, Second: 100}}
	db := newLifecycleTestDB(t, ep)
	db.TLS = tls
	tr := NewNativeTxn(db, DefaultNativeOptions())
	t.Cleanup(tr.Close)
	if err := tr.Shared.SetReadVersion(version); err != nil {
		t.Fatal(err)
	}
	return tr.Reader(), &calls
}

func TestAddressesForKeySnapshot(t *testing.T) {
	for _, version := range []int64{42, 43} {
		for _, tls := range []bool{false, true} {
			for _, port := range []bool{false, true} {
				t.Run(fmt.Sprintf("version=%d/tls=%v/port=%v", version, tls, port), func(t *testing.T) {
					r, calls := addressSnapshotReader(t, version, tls, "")
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					got, err := r.AddressesForKey(ctx, []byte("hello"), port)
					want := fmt.Sprintf("192.0.2.%d", version)
					if tls {
						want = fmt.Sprintf("2001:db8::%d", version)
					}
					if port {
						if tls {
							want = "[" + want + "]:4501:tls"
						} else {
							want += ":4500"
						}
					}
					if err != nil || !slices.Equal(got, []string{want}) {
						t.Fatalf("addresses = %v, %v; want %s", got, err, want)
					}
					for stage := range calls {
						if calls[stage].Load() != 1 {
							t.Fatalf("stage %d calls = %d; want 1", stage, calls[stage].Load())
						}
					}
				})
			}
		}
	}
}

func TestAddressesForKeyErrors(t *testing.T) {
	for _, failure := range []string{"version-one", "tag-read", "keyservers-read", "serverlist-read", "missing-tag", "missing-shard", "missing-interface", "malformed-interface"} {
		t.Run(failure, func(t *testing.T) {
			version := int64(42)
			var want error = wire.ErrMalformed
			switch failure {
			case "version-one":
				version, want = 1, fdberr.TransactionTooOld
			case "tag-read", "keyservers-read", "serverlist-read":
				want = fdberr.TransactionTooOld
			}
			r, _ := addressSnapshotReader(t, version, false, failure)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if got, err := r.AddressesForKey(ctx, []byte("hello"), true); err != want || got != nil {
				t.Fatalf("addresses = %v, %v; want %v", got, err, want)
			}
		})
	}
}

func TestAddressesForKeyOutsideKeyspace(t *testing.T) {
	for _, key := range []string{"\xff\xff", "\xff\xff/status/json"} {
		t.Run(fmt.Sprintf("%x", key), func(t *testing.T) {
			r, calls := addressSnapshotReader(t, 42, false, "")
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if got, err := r.AddressesForKey(ctx, []byte(key), true); err != nil || len(got) != 0 {
				t.Fatalf("addresses = %v, %v; want empty", got, err)
			}
			if calls[0].Load() != 1 || calls[1].Load() != 1 || calls[2].Load() != 0 {
				t.Fatal("out-of-keyspace lookup did not read the snapshot's empty boundary")
			}
		})
	}
}

func TestAddressesForKeyCancellationAndInvalidVersion(t *testing.T) {
	db := newLifecycleTestDB(t, wire.Endpoint{})
	tr := NewNativeTxn(db, DefaultNativeOptions())
	defer tr.Close()
	if err := tr.Shared.SetReadVersion(42); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := tr.Reader().AddressesForKey(ctx, []byte("key"), true); err != fdberr.OperationCancelled {
		t.Fatalf("cancelled address read: %v", err)
	}
	tr.Close()
	if _, err := tr.Reader().AddressesForKey(context.Background(), []byte("key"), true); err != fdberr.TransactionCancelled {
		t.Fatalf("ended attempt address read: %v", err)
	}
	tr = NewNativeTxn(db, DefaultNativeOptions())
	defer tr.Close()
	if err := tr.Shared.SetInitializeVersion(); err != nil {
		t.Fatal(err)
	}
	if _, err := tr.Reader().AddressesForKey(context.Background(), []byte("key"), true); err != fdberr.ClientInvalidOperation {
		t.Fatalf("initialization-version address read: %v", err)
	}
}
