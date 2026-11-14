package native

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/can1357/fdbgo/internal/fdberr"
	"github.com/can1357/fdbgo/internal/kv"
	"github.com/can1357/fdbgo/internal/transport"
	"github.com/can1357/fdbgo/internal/wire"
)

func lifecycleRequestTable(msg []byte) (wire.Table, error) {
	root, err := wire.Root(msg)
	if err != nil {
		return wire.Table{}, err
	}
	table, ok, err := root.Table(0)
	if err == nil && !ok {
		err = wire.ErrMalformed
	}
	return table, err
}

func lifecycleReply(b *wire.Builder, value wire.Off, fileID uint32) []byte {
	b.StartTable(2)
	b.AddUnion(0, 2, value)
	return b.Finish(b.EndTable(), fileID)
}

func lifecycleErrorReply(code fdberr.Error, fileID uint32) func(*wire.Builder) []byte {
	return func(b *wire.Builder) []byte {
		b.StartTable(1)
		b.AddU16(0, uint16(code))
		err := b.EndTable()
		b.StartTable(2)
		b.AddUnion(0, 1, err)
		return b.Finish(b.EndTable(), fileID)
	}
}

func lifecycleGRVReply(version int64, metadata []byte) func(*wire.Builder) []byte {
	return func(b *wire.Builder) []byte {
		var mv wire.Off
		if metadata != nil {
			mv = b.Bytes(metadata)
		}
		b.StartTable(9)
		b.AddI64(1, version)
		if metadata != nil {
			b.AddUnion(3, 1, mv)
		}
		return lifecycleReply(b, b.EndTable(), (wire.GetReadVersion{}).ReplyFileID())
	}
}

func TestMetadataReadVersionSources(t *testing.T) {
	for _, source := range []string{"explicit-hit", "explicit-miss", "grv-cache-hit", "grv-cache-miss", "proxy"} {
		for _, value := range []struct {
			name string
			data []byte
		}{
			{"absent", nil},
			{"empty", []byte{}},
			{"present", []byte("metadata-version")},
		} {
			t.Run(source+"/"+value.name, func(t *testing.T) {
				const version = int64(42)
				var storageReads atomic.Int64
				var ep wire.Endpoint
				server := newNativeTestServer(t, func(c *nativeTestConn, dest wire.UID, msg []byte) {
					if dest == ep.Adjusted(1).Token {
						token, err := nativeRequestReplyToken(msg, 5)
						if err != nil {
							t.Errorf("GRV request: %v", err)
							return
						}
						if err := c.send(token, lifecycleGRVReply(version, value.data)); err != nil {
							t.Errorf("GRV reply: %v", err)
						}
						return
					}
					if dest != ep.Adjusted(ssGetValue).Token {
						t.Errorf("unexpected endpoint: %v", dest)
						return
					}
					request, err := lifecycleRequestTable(msg)
					if err != nil {
						t.Error(err)
						return
					}
					key, keyErr := request.Bytes(0)
					v, versionErr := request.I64(1)
					if keyErr != nil || versionErr != nil || string(key) != string(kv.MetadataVersionKey) || v != version {
						t.Errorf("storage read key %q version %d errors %v, %v", key, v, keyErr, versionErr)
					}
					storageReads.Add(1)
					token, err := nativeRequestReplyToken(msg, 4)
					if err != nil {
						t.Error(err)
						return
					}
					if err := c.send(token, func(b *wire.Builder) []byte {
						var stored wire.Off
						if value.data != nil {
							stored = b.Bytes(value.data)
						}
						b.StartTable(6)
						if value.data != nil {
							b.AddUnion(3, 1, stored)
						}
						return lifecycleReply(b, b.EndTable(), (&wire.GetValue{}).ReplyFileID())
					}); err != nil {
						t.Errorf("storage reply: %v", err)
					}
				})
				ep = wire.Endpoint{Addr: server.addr(), Token: wire.UID{First: 123, Second: 100}}
				db := newLifecycleTestDB(t, ep)
				db.publish(&ClusterInfo{GRV: []wire.Endpoint{ep.Adjusted(1)}, Commit: []wire.Endpoint{ep}})
				db.RecordMetadataVersion(version-1, []byte("wrong older metadata"))
				cached := source == "explicit-hit" || source == "grv-cache-hit"
				if cached {
					db.RecordMetadataVersion(version, value.data)
				}
				db.RecordMetadataVersion(version+1, []byte("wrong newer metadata"))
				opts := DefaultNativeOptions()
				if source == "grv-cache-hit" || source == "grv-cache-miss" {
					opts.UseGRVCache = true
					// Isolate the foreground cache path from the background updater.
					db.grvCache = GRVCache{Version: version, Time: transport.Now(), UpdaterRunning: true}
				}
				tr := NewNativeTxn(db, opts)
				defer tr.Close()
				if source == "explicit-hit" || source == "explicit-miss" {
					if err := tr.Shared.SetReadVersion(version); err != nil {
						t.Fatal(err)
					}
				}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				rv, err := tr.Shared.ReadVersion(ctx)
				if err != nil || rv.Version != version {
					t.Fatalf("read version = %+v, %v", rv, err)
				}
				if source == "proxy" {
					// A proxy-provided absence remains known even after cache eviction.
					for i := int64(2); i <= metadataVersionCacheSize+2; i++ {
						db.RecordMetadataVersion(version+i, []byte("later metadata"))
					}
				}
				got, err := tr.Reader().Get(ctx, kv.MetadataVersionKey)
				if err != nil || !equalValue(got, value.data) {
					t.Fatalf("metadata = %#v, %v; want %#v", got, err, value.data)
				}
				wantReads := int64(0)
				if source == "explicit-miss" || source == "grv-cache-miss" {
					wantReads = 1
				}
				if got := storageReads.Load(); got != wantReads {
					t.Fatalf("storage reads = %d; want %d", got, wantReads)
				}
			})
		}
	}
}

func TestMetadataReadCancellationAndInvalidVersion(t *testing.T) {
	db := newLifecycleTestDB(t, wire.Endpoint{})
	db.RecordMetadataVersion(42, []byte("cached"))
	tr := NewNativeTxn(db, DefaultNativeOptions())
	defer tr.Close()
	if err := tr.Shared.SetReadVersion(42); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := tr.Reader().Get(ctx, kv.MetadataVersionKey); err != fdberr.OperationCancelled {
		t.Fatalf("cancelled cached metadata read: %v", err)
	}
	tr.Close()
	if _, err := tr.Reader().Get(context.Background(), kv.MetadataVersionKey); err != fdberr.TransactionCancelled {
		t.Fatalf("ended attempt metadata read: %v", err)
	}
	tr = NewNativeTxn(db, DefaultNativeOptions())
	defer tr.Close()
	if err := tr.Shared.SetInitializeVersion(); err != nil {
		t.Fatal(err)
	}
	if _, err := tr.Reader().Get(context.Background(), kv.MetadataVersionKey); err != fdberr.ClientInvalidOperation {
		t.Fatalf("initialization-version metadata read: %v", err)
	}
}
