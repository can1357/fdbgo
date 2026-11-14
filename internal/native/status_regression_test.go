package native

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/can1357/fdbgo/internal/cluster"
	"github.com/can1357/fdbgo/internal/fdberr"
	"github.com/can1357/fdbgo/internal/wire"
)

func statusTestConnection(servers ...*nativeTestServer) cluster.ConnectionString {
	conn := cluster.ConnectionString{Key: "status:test"}
	for _, s := range servers {
		conn.Coordinators = append(conn.Coordinators, cluster.Coordinator{Addr: s.addr().Addr})
	}
	return conn
}

func statusTestLeaderReply(l leader) func(*wire.Builder) []byte {
	return func(b *wire.Builder) []byte {
		info := b.Bytes(l.info)
		b.StartTable(4)
		b.AddUID(0, l.changeID)
		b.AddOffset(1, info)
		b.AddBool(2, l.forward)
		li := b.EndTable()
		b.StartTable(2)
		b.AddUnion(0, 1, li)
		return lifecycleReply(b, b.EndTable(), (getLeaderReq{}).ReplyFileID())
	}
}

func statusTestControllerInfo(ep wire.Endpoint) []byte {
	var b wire.Builder
	b.Reset()
	ip := ep.Addr.Addr.Addr().As4()
	v := b.ScalarU32(binary.BigEndian.Uint32(ip[:]))
	b.StartTable(2)
	b.AddUnion(0, 1, v)
	ipTable := b.EndTable()
	b.StartTable(4)
	b.AddOffset(0, ipTable)
	b.AddU16(1, ep.Addr.Addr.Port())
	addr := b.EndTable()
	b.StartTable(3)
	b.AddOffset(0, addr)
	addresses := b.EndTable()
	b.StartTable(2)
	b.AddOffset(0, addresses)
	b.AddUID(1, ep.Token)
	endpoint := b.EndTable()
	b.StartTable(1)
	b.AddOffset(0, endpoint)
	stream := b.EndTable()
	b.StartTable(3)
	b.AddOffset(2, stream)
	ci := b.EndTable()
	b.StartTable(1)
	b.AddOffset(0, ci)
	return append(binary.LittleEndian.AppendUint64(nil, 0x1fdb00b074000000), b.FinishRequest(b.EndTable(), 1)...)
}

// statusTestHandler speaks the real status RPCs. Nil replies deliberately leave
// that endpoint pending, without blocking requests to the other endpoints.
func statusTestHandler(t *testing.T, nominee func() leader, protocol bool, controller wire.UID) func(*nativeTestConn, wire.UID, []byte) {
	t.Helper()
	return func(c *nativeTestConn, dest wire.UID, msg []byte) {
		slot := 0
		if dest == wire.WellKnown(tokenGetLeader) {
			slot = 2
		}
		token, err := nativeRequestReplyToken(msg, slot)
		if err != nil {
			t.Errorf("status request: %v", err)
			return
		}
		var reply func(*wire.Builder) []byte
		switch dest {
		case wire.WellKnown(tokenGetLeader):
			if nominee != nil {
				reply = statusTestLeaderReply(nominee())
			}
		case wire.WellKnown(tokenProtocolInfo):
			if protocol {
				reply = func(b *wire.Builder) []byte {
					b.StartTable(1)
					b.AddI64(0, 0x0fdb00b074000000)
					return lifecycleReply(b, b.EndTable(), (protocolInfoReq{}).ReplyFileID())
				}
			}
		case controller:
			reply = func(b *wire.Builder) []byte {
				text := b.Bytes([]byte(`{"from_controller":true}`))
				b.StartTable(1)
				b.AddOffset(0, text)
				return lifecycleReply(b, b.EndTable(), (statusReq{}).ReplyFileID())
			}
		default:
			t.Errorf("unexpected status endpoint: %v", dest)
		}
		if reply != nil {
			if err := c.send(token, reply); err != nil {
				t.Errorf("status reply: %v", err)
			}
		}
	}
}

func statusTestDocument(t *testing.T, db *DbContext) map[string]any {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 6*time.Second)
	defer cancel()
	text, err := StatusJSON(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err := json.Unmarshal(text, &doc); err != nil {
		t.Fatal(err)
	}
	return doc
}

func TestStatusJSONIndependentCoordinatorReplies(t *testing.T) {
	for _, slow := range []string{"protocol", "leader"} {
		t.Run(slow, func(t *testing.T) {
			controller := wire.UID{First: 9, Second: 500}
			var info []byte
			var nominee func() leader
			if slow != "leader" {
				nominee = func() leader { return leader{info: info} }
			}
			server := newNativeTestServer(t, statusTestHandler(t, nominee, slow != "protocol", controller))
			info = statusTestControllerInfo(wire.Endpoint{Addr: server.addr(), Token: controller})
			db := newLifecycleTestDB(t, wire.Endpoint{})
			db.record = cluster.ConnectionRecord{Conn: statusTestConnection(server)}
			doc := statusTestDocument(t, db)
			q, _ := jget(doc, "client.coordinators.quorum_reachable")
			if q != (slow != "leader") {
				t.Fatalf("quorum_reachable = %v for slow %s", q, slow)
			}
			coords, _ := jget(doc, "client.coordinators.coordinators")
			co := coords.([]any)[0].(map[string]any)
			if co["reachable"] != (slow != "leader") {
				t.Fatalf("coordinator = %v", co)
			}
			_, protocol := co["protocol"]
			if protocol != (slow != "protocol") {
				t.Fatalf("protocol presence = %v for slow %s", protocol, slow)
			}
			if slow == "protocol" {
				if value, _ := jget(doc, "cluster.from_controller"); value != true {
					t.Fatalf("successful leader was lost: %v", doc)
				}
			}
		})
	}
}

func TestStatusJSONRetriesBrokenCoordinatorPromises(t *testing.T) {
	for _, endpoint := range []string{"leader", "protocol"} {
		t.Run(endpoint, func(t *testing.T) {
			controller := wire.UID{First: 9, Second: 700}
			var info []byte
			handler := statusTestHandler(t, func() leader { return leader{info: info} }, true, controller)
			dest, slot, fileID := wire.WellKnown(tokenGetLeader), 2, (getLeaderReq{}).ReplyFileID()
			if endpoint == "protocol" {
				dest, slot, fileID = wire.WellKnown(tokenProtocolInfo), 0, (protocolInfoReq{}).ReplyFileID()
			}
			var calls atomic.Int32
			server := newNativeTestServer(t, func(c *nativeTestConn, target wire.UID, msg []byte) {
				if target == dest && calls.Add(1) == 1 {
					token, err := nativeRequestReplyToken(msg, slot)
					if err != nil {
						t.Error(err)
						return
					}
					if err := c.send(token, func(b *wire.Builder) []byte {
						b.StartTable(1)
						b.AddU16(0, uint16(fdberr.BrokenPromise))
						failure := b.EndTable()
						b.StartTable(2)
						b.AddUnion(0, 1, failure)
						return b.Finish(b.EndTable(), fileID)
					}); err != nil {
						t.Error(err)
					}
					return
				}
				handler(c, target, msg)
			})
			info = statusTestControllerInfo(wire.Endpoint{Addr: server.addr(), Token: controller})
			db := newLifecycleTestDB(t, wire.Endpoint{})
			db.record = cluster.ConnectionRecord{Conn: statusTestConnection(server)}
			doc := statusTestDocument(t, db)
			if q, _ := jget(doc, "client.coordinators.quorum_reachable"); q != true {
				t.Fatalf("transient error lost quorum: %v", doc)
			}
			if value, _ := jget(doc, "cluster.from_controller"); value != true {
				t.Fatalf("controller status missing after retry: %v", doc)
			}
			coords, _ := jget(doc, "client.coordinators.coordinators")
			if _, ok := coords.([]any)[0].(map[string]any)["protocol"]; !ok {
				t.Fatalf("protocol missing after retry: %v", doc)
			}
		})
	}
}

func TestStatusJSONFollowsCoordinatorForwarding(t *testing.T) {
	controller := wire.UID{First: 9, Second: 600}
	var info []byte
	final := newNativeTestServer(t, statusTestHandler(t, func() leader { return leader{info: info} }, true, controller))
	info = statusTestControllerInfo(wire.Endpoint{Addr: final.addr(), Token: controller})
	middle := newNativeTestServer(t, statusTestHandler(t, func() leader {
		return leader{forward: true, info: []byte(statusTestConnection(final).String())}
	}, true, wire.UID{}))
	old := newNativeTestServer(t, statusTestHandler(t, func() leader {
		return leader{forward: true, info: []byte(statusTestConnection(middle).String())}
	}, true, wire.UID{}))
	db := newLifecycleTestDB(t, wire.Endpoint{})
	original := statusTestConnection(old)
	db.record = cluster.ConnectionRecord{Conn: original}
	doc := statusTestDocument(t, db)
	if value, _ := jget(doc, "cluster.from_controller"); value != true {
		t.Fatalf("forwarded controller was not queried: %v", doc)
	}
	if db.ConnectionString().String() != original.String() {
		t.Fatal("status discovery changed the captured database connection record")
	}
}

func TestStatusLeaderMasksFitnessAndOrdersTies(t *testing.T) {
	candidate := func(first, second uint64, info string) *probe {
		return &probe{reachable: true, leader: leaderReply{ok: true, leader: leader{changeID: wire.UID{First: first, Second: second}, info: []byte(info)}}}
	}
	a := candidate(1<<63|5, 8, "leader-a-original")
	results := []*probe{candidate(2, 9, "leader-b"), a, candidate(3<<57|5, 8, "leader-a-other-fitness")}
	got, ok := statusLeader(results)
	if !ok || string(got.info) != "leader-a-original" || got.changeID != a.leader.leader.changeID {
		t.Fatalf("masked vote selected %+v, %v", got, ok)
	}
	got, ok = statusLeader([]*probe{a, results[0]})
	if !ok || string(got.info) != "leader-b" {
		t.Fatalf("tied votes did not choose the lowest masked UID: %+v", got)
	}
	forward := candidate(100, 100, "status:forward@127.0.0.1:4000")
	forward.leader.leader.forward = true
	got, ok = statusLeader(append(results, forward))
	if !ok || !got.forward {
		t.Fatal("a single forwarding answer must override normal nominees")
	}
}

func TestStatusJSONCancelsPendingProbes(t *testing.T) {
	requested := make(chan struct{}, 2)
	server := newNativeTestServer(t, func(*nativeTestConn, wire.UID, []byte) { requested <- struct{}{} })
	db := newLifecycleTestDB(t, wire.Endpoint{})
	db.record = cluster.ConnectionRecord{Conn: statusTestConnection(server)}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := StatusJSON(ctx, db); done <- err }()
	for range 2 {
		select {
		case <-requested:
		case <-time.After(3 * time.Second):
			t.Fatal("status probes did not start")
		}
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, fdberr.OperationCancelled) {
			t.Fatalf("status cancellation = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("status ignored cancellation")
	}
}

func TestStatusForwardingCycleAndDeadline(t *testing.T) {
	for _, cycle := range []bool{false, true} {
		t.Run(map[bool]string{false: "deadline", true: "cycle"}[cycle], func(t *testing.T) {
			var conn cluster.ConnectionString
			var nominee func() leader
			if cycle {
				nominee = func() leader { return leader{forward: true, info: []byte(conn.String())} }
			}
			server := newNativeTestServer(t, statusTestHandler(t, nominee, false, wire.UID{}))
			conn = statusTestConnection(server)
			db := newLifecycleTestDB(t, wire.Endpoint{})
			initial := statusTestConnection()
			initial.Key = "status:old"
			results := []*probe{{reachable: true, leader: leaderReply{ok: true, leader: leader{forward: true, info: []byte(conn.String())}}}}
			timeout := 100 * time.Millisecond
			if cycle {
				timeout = 3 * time.Second
			}
			ctx, cancel := context.WithTimeout(t.Context(), timeout)
			defer cancel()
			_, err := statusLeaderEndpoint(ctx, db, initial, results)
			if err == nil {
				t.Fatal("forwarding did not terminate with an error")
			}
			if cycle && ctx.Err() != nil {
				t.Fatal("forwarding cycle was not detected before its deadline")
			}
			if !cycle && ctx.Err() != context.DeadlineExceeded {
				t.Fatalf("pending forwarded endpoint ignored deadline: %v", err)
			}
		})
	}
}
