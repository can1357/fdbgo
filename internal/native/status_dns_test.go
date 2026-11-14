package native

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"testing"

	"github.com/can1357/fdbgo/internal/cluster"
	"github.com/can1357/fdbgo/internal/wire"
)

func TestStatusJSONRetriesCoordinatorDNS(t *testing.T) {
	// The first lookup receives NXDOMAIN for both address families. Subsequent
	// lookups resolve the real local status server without external DNS traffic.
	var mu sync.Mutex
	seen := make(map[uint16]bool)
	resolver := &net.Resolver{PreferGo: true, Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
		client, server := net.Pipe()
		go func() {
			defer server.Close()
			var length [2]byte
			if _, err := io.ReadFull(server, length[:]); err != nil {
				return
			}
			query := make([]byte, binary.BigEndian.Uint16(length[:]))
			if _, err := io.ReadFull(server, query); err != nil || len(query) < 16 {
				return
			}
			end := 12
			for end < len(query) && query[end] != 0 {
				end += int(query[end]) + 1
			}
			end += 5 // zero label, query type, query class
			if end > len(query) {
				return
			}
			kind := binary.BigEndian.Uint16(query[end-4:])
			mu.Lock()
			recovered := seen[kind]
			seen[kind] = true
			mu.Unlock()
			reply := append([]byte(nil), query[:end]...)
			binary.BigEndian.PutUint16(reply[2:4], 0x8180)
			clear(reply[6:12])
			if !recovered {
				reply[3] = 0x83 // NXDOMAIN, rather than a resolver-level retry
			} else if kind == 1 {
				binary.BigEndian.PutUint16(reply[6:8], 1)
				reply = append(reply, 0xc0, 0x0c, 0, 1, 0, 1, 0, 0, 0, 60, 0, 4, 127, 0, 0, 1)
			}
			binary.BigEndian.PutUint16(length[:], uint16(len(reply)))
			if _, err := server.Write(append(length[:], reply...)); err != nil {
				return
			}
		}()
		return client, nil
	}}
	old := net.DefaultResolver
	net.DefaultResolver = resolver
	t.Cleanup(func() { net.DefaultResolver = old })

	controller := wire.UID{First: 87, Second: 91}
	var info []byte
	server := newNativeTestServer(t, statusTestHandler(t, func() leader { return leader{info: info} }, true, controller))
	info = statusTestControllerInfo(wire.Endpoint{Addr: server.addr(), Token: controller})
	db := newLifecycleTestDB(t, wire.Endpoint{})
	db.record = cluster.ConnectionRecord{Conn: cluster.ConnectionString{
		Key: "status:dns", Coordinators: []cluster.Coordinator{{Host: "coordinator.test", Port: server.addr().Addr.Port()}},
	}}
	doc := statusTestDocument(t, db)
	if reachable, _ := jget(doc, "client.coordinators.quorum_reachable"); reachable != true {
		t.Fatalf("DNS recovery did not restore coordinator quorum: %v", doc)
	}
	if _, ok := doc["cluster"]; !ok {
		t.Fatalf("DNS recovery did not fetch cluster status: %v", doc)
	}
}
