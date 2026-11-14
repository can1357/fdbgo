package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/netip"
	"testing"

	"github.com/can1357/fdbgo/internal/cluster"
	"github.com/can1357/fdbgo/internal/fdberr"
	"github.com/can1357/fdbgo/internal/native"
	"github.com/can1357/fdbgo/internal/options"
	"github.com/can1357/fdbgo/internal/wire"
)

func TestDatabaseLocalityOptionsAcceptOpaqueValues(t *testing.T) {
	db := &Db{ctx: &native.DbContext{}}
	for _, code := range []int{options.DatabaseMachineID, options.DatabaseDatacenterID} {
		for _, value := range [][]byte{nil, {}, []byte("machine"), {0xff, 0xfe, 0, 'x'}} {
			param := bytes.Clone(value)
			decoded, err := decodeDbOption(code, param)
			if err != nil || !bytes.Equal(decoded.s, value) || decoded.s == nil {
				t.Fatalf("locality %d value %x decoded as %x, %v", code, value, decoded.s, err)
			}
			if err := db.SetOption(code, param); err != nil {
				t.Fatalf("locality %d rejected value %x: %v", code, value, err)
			}
			if len(param) > 0 {
				param[0] ^= 0xff
				if !bytes.Equal(decoded.s, value) {
					t.Fatal("decoded locality aliases the caller's parameter")
				}
			}
		}
	}
}

func TestLocalityChangePreservesUnrelatedOptionValidation(t *testing.T) {
	if _, err := decodeTxOption(options.TransactionDebugTransactionIdentifier, []byte{0xff}); !errors.Is(err, fdberr.InvalidOptionValue) {
		t.Fatalf("invalid UTF-8 transaction identifier = %v", err)
	}
	db := &Db{ctx: &native.DbContext{}}
	for _, code := range []int{options.DatabaseLocationCacheSize, options.DatabaseMaxWatches} {
		if err := db.SetOption(code, []byte{1}); !errors.Is(err, fdberr.InvalidOptionValue) {
			t.Fatalf("malformed integer %d = %v", code, err)
		}
	}
	if err := db.SetOption(options.DatabaseTestCausalReadRisky, []byte{101, 0, 0, 0, 0, 0, 0, 0}); !errors.Is(err, fdberr.InvalidOptionValue) {
		t.Fatalf("out-of-range percentage = %v", err)
	}
}

func TestClientStatusUsesCurrentTopologyNotPeerHistory(t *testing.T) {
	addr := func(s string, tls bool) wire.NetAddr {
		return wire.NetAddr{Addr: netip.MustParseAddrPort(s), TLS: tls}
	}
	coord := addr("127.0.0.1:4510", false)
	secureCoord := addr("127.0.0.1:4510", true)
	proxy := addr("127.0.0.1:4511", true)
	storage := addr("127.0.0.1:4512", false)
	retired := addr("127.0.0.1:4513", false)

	// Freeze the native snapshot before filling it: an invalid literal cannot
	// contact a server even if the monitor runs before Close. Status reporting
	// itself is synchronous and does not require a live database connection.
	coordinators := make([]cluster.Coordinator, 3)
	ctx := native.NewDbContext(cluster.ConnectionRecord{Conn: cluster.ConnectionString{
		Key: "status:test", Coordinators: coordinators,
	}}, true)
	ctx.Close()
	copy(coordinators, []cluster.Coordinator{
		{Addr: coord.Addr},
		{Addr: secureCoord.Addr, TLS: true},
		{Host: "unused.invalid", Port: 4514},
	})
	info := ctx.ClusterNow()
	info.GRV = []wire.Endpoint{{Addr: coord}}
	info.Commit = []wire.Endpoint{{Addr: addr("127.0.0.1:9999", false), Alt: proxy}}
	ctx.Locations.Insert(native.Location{
		Begin: []byte{}, End: []byte("z"),
		Servers: []wire.StorageServer{{ID: wire.UID{First: 1}, Base: wire.Endpoint{Addr: storage}}},
	})

	// Install only canceled dial attempts, so no real network is required.
	// One peer remains in topology and one is historical; neither is connected.
	ctx.Peers = native.Peers{}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	for _, a := range []wire.NetAddr{coord, retired} {
		if _, err := ctx.Peers.Get(canceled, a, ctx.Failures); err == nil {
			t.Fatal("canceled dial unexpectedly succeeded")
		}
		ctx.Failures.AddressOK(a)
	}
	ctx.Failures.AddressFailed(storage)
	db := &Db{ctx: ctx}
	read := func() map[string]map[string]any {
		t.Helper()
		text, wait, err := db.ClientStatus()
		if err != nil || wait != nil {
			t.Fatalf("ClientStatus = pending %v, %v", wait != nil, err)
		}
		var doc struct {
			Connections          []map[string]any
			NumConnectionsFailed int
		}
		if err := json.Unmarshal(text, &doc); err != nil {
			t.Fatal(err)
		}
		got := make(map[string]map[string]any)
		for _, c := range doc.Connections {
			name := c["Address"].(string)
			if _, duplicate := got[name]; duplicate {
				t.Fatalf("duplicate connection address %q", name)
			}
			got[name] = c
		}
		failed := 0
		for _, c := range got {
			if c["Status"] == "failed" {
				failed++
			}
		}
		if doc.NumConnectionsFailed != failed {
			t.Fatalf("NumConnectionsFailed = %d, actual %d", doc.NumConnectionsFailed, failed)
		}
		return got
	}
	got := read()
	if len(got) != 4 {
		t.Fatalf("topology inventory = %v; want four distinct literal addresses", got)
	}
	for _, a := range []wire.NetAddr{coord, secureCoord, proxy, storage} {
		if _, ok := got[a.String()]; !ok {
			t.Fatalf("uncontacted topology address %s is missing: %v", a, got)
		}
	}
	if _, ok := got[retired.String()]; ok {
		t.Fatal("retired peer leaked into current connection inventory")
	}
	if got[coord.String()]["Status"] != "connecting" || got[storage.String()]["Status"] != "failed" {
		t.Fatalf("optional peer/failure state not applied: %v", got)
	}
	for _, a := range []wire.NetAddr{secureCoord, proxy} {
		c := got[a.String()]
		if c["Status"] != "disconnected" {
			t.Fatalf("never-contacted address status = %v", c)
		}
		for _, field := range []string{"ConnectFailedCount", "Compatible", "LastConnectTime", "BytesReceived", "BytesSent", "ProtocolVersion"} {
			if _, ok := c[field]; ok {
				t.Fatalf("never-contacted address invented %s: %v", field, c)
			}
		}
	}
	if _, ok := got[coord.String()]["ConnectFailedCount"]; !ok {
		t.Fatal("known peer lost its available connection failure count")
	}

	// Replacing topology must remove its addresses despite retained peer state.
	info.GRV = nil
	info.Commit = nil
	ctx.Locations.InvalidateRange(nil, []byte("z"))
	coordinators[0] = cluster.Coordinator{Addr: secureCoord.Addr, TLS: true}
	got = read()
	if len(got) != 1 || got[secureCoord.String()] == nil {
		t.Fatalf("removed topology addresses remain: %v", got)
	}
}
