package native

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/can1357/fdbgo/internal/cluster"
	"github.com/can1357/fdbgo/internal/fdberr"
	"github.com/can1357/fdbgo/internal/transport"
	"github.com/can1357/fdbgo/internal/wire"
)

// \xff\xff/status/json: the cluster
// controller's status document plus the client's own view of the
// coordinators and cluster file. The cluster controller is found through the
// coordinators' getLeader endpoint, whose LeaderInfo.serializedInfo is an
// object-serialized ClusterControllerClientInterface prefixed by its
// protocol version.

const (
	// tokenGetLeader is the well-known endpoint token of the coordinators' getLeader.
	tokenGetLeader = 3
	// tokenProtocolInfo is the well-known endpoint token of protocol info requests.
	tokenProtocolInfo = 10
	// statusTimeout bounds building the status document.
	statusTimeout = 30 * time.Second
)

// leader is the cluster controller a coordinator nominates (LeaderInfo:
// changeID, serializedInfo, forward).
type leader struct {
	changeID wire.UID
	info     []byte
	forward  bool
	// The version prefix does not identify the serializer; 6.2 supports both.
	object bool
}

// leaderReply is Optional<LeaderInfo>; ok false = none.
type leaderReply struct {
	leader leader
	ok     bool
}

// getLeaderReq is GetLeaderRequest: key, knownLeader, reply.
type getLeaderReq struct {
	key   []byte
	known wire.UID
}

func (getLeaderReq) FileID() uint32 { return 214727 }

func (getLeaderReq) ReplyFileID() uint32 { return 2<<28 | 4<<24 | 8338794 }

// ReplyFileIDProtocol preserves the pre-7.0 OR-composed ErrorOr<Optional<LeaderInfo>> identifier.
func (r getLeaderReq) ReplyFileIDProtocol(protocol wire.ProtocolVersion) uint32 {
	if protocol != 0 && protocol.Normalized() < wire.Protocol70.Normalized() {
		return 0x11<<24 | 8338794
	}
	return r.ReplyFileID()
}

func (r getLeaderReq) Encode(b *wire.Builder, reply wire.UID) wire.Off {
	k := b.Bytes(r.key)
	b.StartTable(1)
	b.AddUID(0, reply)
	rp := b.EndTable()
	b.StartTable(3)
	b.AddOffset(0, k)
	b.AddUID(1, r.known)
	b.AddOffset(2, rp)
	return b.EndTable()
}

// Decode reads Optional<LeaderInfo>, wrapped in a table by EnsureTable.
func (getLeaderReq) Decode(msg []byte, t wire.Table) (leaderReply, error) {
	tag, f, err := t.Union(0)
	if err != nil {
		return leaderReply{}, err
	}
	switch tag {
	case 0:
		return leaderReply{}, nil
	case 1:
		li, err := f.Table()
		if err != nil {
			return leaderReply{}, err
		}
		id, err := li.UID(0)
		if err != nil {
			return leaderReply{}, err
		}
		info, err := li.Bytes(1)
		if err != nil {
			return leaderReply{}, err
		}
		fwd, err := li.Bool(2)
		if err != nil {
			return leaderReply{}, err
		}
		return leaderReply{leader{changeID: id, info: info, forward: fwd, object: true}, true}, nil
	}
	return leaderReply{}, wire.ErrMalformed
}

// protocolInfoReq is ProtocolInfoRequest: reply. Its reply carries the peer's ProtocolVersion.
type protocolInfoReq struct{}

func (protocolInfoReq) FileID() uint32 { return 13261233 }

func (protocolInfoReq) ReplyFileID() uint32 { return wire.FIDErrorOr | 7784298 }

func (protocolInfoReq) Encode(b *wire.Builder, reply wire.UID) wire.Off {
	b.StartTable(1)
	b.AddUID(0, reply)
	rp := b.EndTable()
	b.StartTable(1)
	b.AddOffset(0, rp)
	return b.EndTable()
}

func (protocolInfoReq) Decode(msg []byte, t wire.Table) (uint64, error) { return t.U64(0) }

// statusReq is StatusRequest: reply, statusField. Its reply is StatusReply{statusStr}.
type statusReq struct{}

func (statusReq) FileID() uint32 { return 14419140 }

func (statusReq) ReplyFileID() uint32 { return wire.FIDErrorOr | 9980504 }

func (statusReq) Encode(b *wire.Builder, reply wire.UID) wire.Off {
	b.StartTable(1)
	b.AddUID(0, reply)
	rp := b.EndTable()
	b.StartTable(2)
	b.AddOffset(0, rp)
	return b.EndTable()
}

func (statusReq) Decode(msg []byte, t wire.Table) ([]byte, error) { return t.Bytes(0) }

// statusEndpoint returns ClusterInterface.databaseStatus from a serialized
// ClusterControllerClientInterface.
func statusEndpoint(info []byte, object bool) (wire.Endpoint, error) {
	if len(info) < 8 {
		return wire.Endpoint{}, wire.ErrMalformed
	}
	protocol := wire.ProtocolVersion(binary.LittleEndian.Uint64(info[:8]))
	if !object {
		_, status, err := wire.DecodeControllerEndpointsLegacy(info)
		return status, err
	}
	root, err := wire.Root(info[8:])
	if err != nil {
		return wire.Endpoint{}, err
	}
	root.Protocol = protocol
	t := root
	for _, slot := range []int{0, 0, 2} {
		next, ok, err := t.Table(slot)
		if err != nil {
			return wire.Endpoint{}, err
		}
		if !ok {
			return wire.Endpoint{}, wire.ErrMalformed
		}
		t = next
	}
	return wire.RequestStreamEndpoint(t)
}

// message is makeMessage.
func message(name, description string) map[string]any {
	return map[string]any{"name": name, "description": description}
}

// jget follows a dotted path through nested objects.
func jget(v any, path string) (any, bool) {
	for k := range strings.SplitSeq(path, ".") {
		m, ok := v.(map[string]any)
		if !ok {
			return nil, false
		}
		if v, ok = m[k]; !ok {
			return nil, false
		}
	}
	return v, true
}

func jlen(v any, ok bool) int {
	if a, isArr := v.([]any); ok && isArr {
		return len(a)
	}
	return 0
}

// databaseStatus is getClientDatabaseStatus: availability and health derived from both sections.
func databaseStatus(client map[string]any, cluster any) map[string]any {
	available, healthy := false, false
	recovery, rok := jget(cluster, "recovery_state.name")
	recoveryName, rsok := recovery.(string)
	quorum, qok := jget(client, "coordinators.quorum_reachable")
	quorumB, qbok := quorum.(bool)
	dbAvail, dok := jget(cluster, "database_available")
	dbAvailB, dbok := dbAvail.(bool)
	if rok && rsok && qok && qbok && dok && dbok {
		switch recoveryName {
		case "accepting_commits", "all_logs_recruited", "storage_recovered", "fully_recovered":
			available = quorumB && dbAvailB
		}
		if procs, ok := jget(cluster, "processes"); available && ok {
			if procMap, ok := procs.(map[string]any); ok {
				procMessages := false
				for _, p := range procMap {
					if jlen(jget(p, "messages")) > 0 {
						procMessages = true
					}
				}
				_, dataStatePresent := jget(cluster, "data.state")
				dh, dhok := jget(cluster, "data.state.healthy")
				dataUnhealthy := dataStatePresent && dhok && dh == false
				clusterMessages := jlen(jget(cluster, "messages"))
				configMessages := jlen(jget(client, "configuration.messages"))
				utd, uok := jget(client, "cluster_file.up_to_date")
				upToDate := uok && utd == true
				healthy = !(clusterMessages > 0 || configMessages > 0 || procMessages || dataUnhealthy || !dataStatePresent || !upToDate)
			}
		}
	}
	return map[string]any{"healthy": healthy, "available": available}
}

// probe is one coordinator's answers: getLeader (reachable false when the
// call failed) and its protocol version.
type probe struct {
	reachable bool
	leader    leaderReply
	protoOK   bool
	proto     uint64
}

// coordinatorAnswer carries just one endpoint's result. The buffered result
// channels allow outstanding calls to finish after their collection is canceled.
type coordinatorAnswer struct {
	i        int
	protocol bool
	p        probe
}

// resolveCoordinatorWithRetry keeps transient DNS failures within the caller's
// collection deadline, using the client's hostname-resolution backoff.
func resolveCoordinatorWithRetry(ctx context.Context, co cluster.Coordinator) (wire.NetAddr, error) {
	for delay := 50 * time.Millisecond; ; delay = min(2*delay, time.Second) {
		addr, err := resolve(ctx, co)
		if err == nil {
			return addr, nil
		}
		if err := sleep(ctx, delay); err != nil {
			return wire.NetAddr{}, err
		}
	}
}

// Coordinator endpoints survive process restarts; retry broken promises until
// the status collection deadline, as retryBrokenPromise does in the C client.
func coordinatorCall[R any](ctx context.Context, db *DbContext, ep wire.Endpoint, req wire.Request[R]) (R, error) {
	for {
		reply, err := call(ctx, db, ep, req)
		if !errors.Is(err, fdberr.BrokenPromise) {
			return reply, err
		}
		if err := sleep(ctx, 10*time.Millisecond); err != nil {
			return reply, err
		}
	}
}

func coordinatorRequests(ctx context.Context, db *DbContext, conn cluster.ConnectionString, protocols bool) <-chan coordinatorAnswer {
	count := len(conn.Coordinators)
	if protocols {
		count *= 2
	}
	ch := make(chan coordinatorAnswer, count)
	for i, co := range conn.Coordinators {
		go func() {
			addr, err := resolveCoordinatorWithRetry(ctx, co)
			if err != nil {
				ch <- coordinatorAnswer{i: i}
				if protocols {
					ch <- coordinatorAnswer{i: i, protocol: true}
				}
				return
			}
			if protocols {
				go func() {
					ep := wire.Endpoint{Addr: addr, Token: wire.WellKnown(tokenProtocolInfo)}
					v, err := coordinatorProtocol(ctx, db, ep)
					ch <- coordinatorAnswer{i: i, protocol: true, p: probe{protoOK: err == nil, proto: v}}
				}()
			}
			ep := wire.Endpoint{Addr: addr, Token: wire.WellKnown(tokenGetLeader)}
			l, err := coordinatorCall(ctx, db, ep, wire.Request[leaderReply](getLeaderReq{key: []byte(conn.Key)}))
			ch <- coordinatorAnswer{i: i, p: probe{reachable: err == nil, leader: l}}
		}()
	}
	return ch
}

func recordCoordinatorAnswer(results []*probe, r coordinatorAnswer) {
	if results[r.i] == nil {
		results[r.i] = &probe{}
	}
	p := results[r.i]
	if r.protocol {
		p.protoOK, p.proto = r.p.protoOK, r.p.proto
	} else {
		p.reachable, p.leader = r.p.reachable, r.p.leader
	}
}

func probeCoordinators(ctx context.Context, db *DbContext, conn cluster.ConnectionString) ([]*probe, error) {
	pctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	ch := coordinatorRequests(pctx, db, conn, true)
	results := make([]*probe, len(conn.Coordinators))
	majority := len(results)/2 + 1
	var settle <-chan time.Time
	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	for received := 0; received < 2*len(results); received++ {
		select {
		case r := <-ch:
			recordCoordinatorAnswer(results, r)
		case <-settle:
			return results, nil
		case <-pctx.Done():
			if ctx.Err() != nil {
				return nil, cancelErr(ctx)
			}
			return results, nil
		}
		leaders, protos := 0, 0
		for _, p := range results {
			if p != nil && p.reachable {
				leaders++
			}
			if p != nil && p.protoOK {
				protos++
			}
		}
		if timer == nil && leaders >= majority && protos >= majority {
			timer = time.NewTimer(1500 * time.Millisecond)
			settle = timer.C
		}
	}
	if ctx.Err() != nil {
		return nil, cancelErr(ctx)
	}
	return results, nil
}

func maskedLeaderID(id wire.UID) wire.UID {
	return wire.UID{First: id.First & ((1 << 57) - 1), Second: id.Second}
}

func leaderIDLess(a, b wire.UID) bool {
	return a.First < b.First || a.First == b.First && a.Second < b.Second
}

// statusLeader uses the monitorLeader vote identity and tie order, retaining
// the chosen nominee's actual serialized interface. One forwarding answer is
// sufficient to move to the next coordinator generation.
func statusLeader(results []*probe) (leader, bool) {
	type vote struct {
		l     leader
		count int
	}
	votes := make(map[wire.UID]vote)
	var best wire.UID
	bestCount := 0
	for _, p := range results {
		if p == nil || !p.reachable || !p.leader.ok {
			continue
		}
		l := p.leader.leader
		if l.forward {
			return l, true
		}
		id := maskedLeaderID(l.changeID)
		v := votes[id]
		if v.count == 0 {
			v.l = l
		}
		v.count++
		votes[id] = v
		if v.count > bestCount || v.count == bestCount && leaderIDLess(id, best) {
			best, bestCount = id, v.count
		}
	}
	if bestCount == 0 {
		return leader{}, false
	}
	return votes[best].l, true
}

// statusLeaderEndpoint follows coordinator migrations locally for this status
// invocation. A single deadline bounds the entire chain, including DNS and RPC.
func statusLeaderEndpoint(ctx context.Context, db *DbContext, conn cluster.ConnectionString, results []*probe) (wire.Endpoint, error) {
	seen := map[string]bool{conn.String(): true}
	for {
		if ctx.Err() != nil {
			return wire.Endpoint{}, cancelErr(ctx)
		}
		l, ok := statusLeader(results)
		if !ok {
			return wire.Endpoint{}, errors.New("no leader")
		}
		if !l.forward {
			return statusEndpoint(l.info, l.object)
		}
		next, err := cluster.ParseConnectionString(string(l.info))
		if err != nil {
			return wire.Endpoint{}, err
		}
		if seen[next.String()] {
			return wire.Endpoint{}, errors.New("coordinator forwarding cycle")
		}
		seen[next.String()] = true
		conn = next
		gctx, cancel := context.WithCancel(ctx)
		ch := coordinatorRequests(gctx, db, conn, false)
		results = make([]*probe, len(conn.Coordinators))
		for received := 0; received < len(results); received++ {
			select {
			case r := <-ch:
				recordCoordinatorAnswer(results, r)
				// monitorLeader publishes a nominee as soon as one is known;
				// it does not require a majority to locate the controller.
				if _, ok := statusLeader(results); ok {
					received = len(results)
				}
			case <-ctx.Done():
				cancel()
				return wire.Endpoint{}, cancelErr(ctx)
			}
		}
		cancel()
	}
}

// StatusJSON builds the \xff\xff/status/json document.
func StatusJSON(ctx context.Context, db *DbContext) ([]byte, error) {
	conn := db.ConnectionString()
	rec := db.Record()
	stored, storedOK, storedErr := rec.Stored()
	messages := []any{}
	client := map[string]any{}

	// Leader reachability and protocol information complete independently.
	// Neither endpoint may hide a successful answer from the other.
	n := len(conn.Coordinators)
	majority := n/2 + 1
	results, err := probeCoordinators(ctx, db, conn)
	if err != nil {
		return nil, err
	}
	reachable := 0
	coords := make([]any, n)
	for i, co := range conn.Coordinators {
		p := results[i]
		o := map[string]any{"address": co.String(), "reachable": p != nil && p.reachable}
		if p != nil && p.reachable {
			reachable++
		}
		if p != nil && p.protoOK {
			o["protocol"] = fmt.Sprintf("%016x", p.proto)
		}
		coords[i] = o
	}
	quorumReachable := reachable >= majority
	coordinatorsFaultTolerance := int64((n-1)/2 - (n - reachable))
	client["coordinators"] = map[string]any{"quorum_reachable": quorumReachable, "coordinators": coords}
	if !quorumReachable {
		messages = append(messages, message("quorum_not_reachable", "Unable to reach a quorum of coordinators."))
	}
	upToDate := true
	if storedOK {
		upToDate = storedErr == nil && stored.String() == conn.String()
	}
	client["cluster_file"] = map[string]any{"path": rec.Location(), "up_to_date": upToDate}
	if !upToDate && storedOK && storedErr == nil {
		messages = append(messages, message("incorrect_cluster_file_contents",
			"Cluster file contents do not match current cluster connection string.\nThe file contains the connection string: "+
				stored.String()+"\nThe current connection string is: "+conn.String()+
				"\nVerify the cluster file and its parent directory are writable and that the cluster file has not been overwritten externally. To change coordinators without manual intervention, the cluster file and its containing folder must be writable by all servers and clients. If a majority of the coordinators referenced by the old connection string are lost, the database will stop working until the correct cluster file is distributed to all processes."))
	}
	client["timestamp"] = time.Now().Unix()

	doc := map[string]any{}
	var cluster any = map[string]any{}
	if quorumReachable {
		lctx, lcancel := context.WithTimeout(ctx, 2*time.Second)
		ep, epErr := statusLeaderEndpoint(lctx, db, conn, results)
		lcancel()
		if ctx.Err() != nil {
			return nil, cancelErr(ctx)
		}
		if epErr != nil {
			messages = append(messages, message("no_cluster_controller",
				"Unable to locate a cluster controller within 2 seconds.  Check that there are server processes running."))
		} else {
			tctx, tcancel := context.WithTimeout(ctx, statusTimeout)
			s, err := call(tctx, db, ep, wire.Request[[]byte](statusReq{}))
			timedOut := tctx.Err() == context.DeadlineExceeded && ctx.Err() == nil
			tcancel()
			var fe fdberr.Error
			switch {
			case err == nil:
				cluster = parseClusterStatus(s, coordinatorsFaultTolerance)
			case ctx.Err() != nil:
				return nil, cancelErr(ctx)
			case timedOut:
				messages = append(messages, message("status_incomplete_timeout", "Timed out fetching cluster status."))
			case errors.As(err, &fe) && fe == fdberr.ServerOverloaded:
				messages = append(messages, message("server_overloaded",
					"The cluster controller is currently processing too many status requests and is unable to respond"))
			case errors.As(err, &fe) || err == transport.Malformed || errors.Is(err, wire.ErrMalformed):
				messages = append(messages, message("status_incomplete_error", "Cluster encountered an error fetching status."))
			default:
				messages = append(messages, message("unreachable_cluster_controller",
					"Unable to communicate with the cluster controller at "+ep.Addr.String()+" to get status."))
			}
		}
		doc["cluster"] = cluster
	}
	client["messages"] = messages
	client["database_status"] = databaseStatus(client, cluster)
	doc["client"] = client
	// Every document has cluster.layers._valid.
	c, ok := doc["cluster"].(map[string]any)
	if !ok {
		if _, present := doc["cluster"]; !present {
			c = map[string]any{}
			doc["cluster"] = c
		}
	}
	if c != nil {
		layers, ok := c["layers"].(map[string]any)
		if !ok {
			if _, present := c["layers"]; !present {
				layers = map[string]any{}
				c["layers"] = layers
			}
		}
		if layers != nil {
			if _, present := layers["_valid"]; !present {
				layers["_valid"] = false
			}
		}
	}
	if ctx.Err() != nil {
		return nil, cancelErr(ctx)
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(doc); err != nil {
		return nil, fdberr.InternalError
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}

// parseClusterStatus parses the cluster controller's document and caps its
// fault tolerance by the coordinators'.
func parseClusterStatus(s []byte, coordinatorsFaultTolerance int64) any {
	dec := json.NewDecoder(bytes.NewReader(s))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return map[string]any{}
	}
	if ft, ok := jget(v, "fault_tolerance"); ok {
		if m, ok := ft.(map[string]any); ok {
			data, dok := asInt(m["max_zone_failures_without_losing_data"])
			avail, aok := asInt(m["max_zone_failures_without_losing_availability"])
			if dok && aok {
				m["max_zone_failures_without_losing_data"] = min(data, coordinatorsFaultTolerance)
				m["max_zone_failures_without_losing_availability"] = min(avail, coordinatorsFaultTolerance)
			}
		}
	}
	return v
}

func asInt(v any) (int64, bool) {
	n, ok := v.(json.Number)
	if !ok {
		return 0, false
	}
	i, err := n.Int64()
	return i, err == nil
}
