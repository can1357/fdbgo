package native

import (
	"context"
	"sync"

	"github.com/can1357/fdbgo/internal/fdberr"
	"github.com/can1357/fdbgo/internal/transport"
	"github.com/can1357/fdbgo/internal/wire"
)

// EncodeLegacy writes GetLeaderRequest for the binary transport generations.
func (r getLeaderReq) EncodeLegacy(w *wire.BinaryWriter, reply wire.UID) error {
	w.Bytes(r.key)
	w.UID(r.known)
	w.ReplyPromise(reply)
	return nil
}

// DecodeLegacy reads Optional<LeaderInfo> without ObjectSerializer tables.
func (getLeaderReq) DecodeLegacy(r *wire.BinaryReader) (leaderReply, error) {
	var result leaderReply
	result.ok = r.Bool()
	if result.ok {
		result.leader = leader{changeID: r.UID(), info: r.Bytes(), forward: r.Bool()}
	}
	return result, r.Err()
}

// EncodeLegacy writes the status reply promise used by pre-6.2 controllers.
func (statusReq) EncodeLegacy(w *wire.BinaryWriter, reply wire.UID) error {
	w.ReplyPromise(reply)
	return nil
}

// DecodeLegacy reads StatusObject's length-prefixed JSON representation.
func (statusReq) DecodeLegacy(r *wire.BinaryReader) ([]byte, error) {
	v := r.Bytes()
	return v, r.Err()
}

// DecodeLegacy preserves the native load-balancer adapter for binary metrics.
func (m metricsReq) DecodeLegacy(r *wire.BinaryReader) (wire.Lb[wire.StorageMetrics], error) {
	v, err := m.WaitMetrics.DecodeLegacy(r)
	return lbWrap(v), err
}

// DecodeLegacy preserves the native load-balancer adapter for binary watches.
func (w watchReq) DecodeLegacy(r *wire.BinaryReader) (wire.Lb[int64], error) {
	v, err := w.WatchValue.DecodeLegacy(r)
	return lbWrap(v), err
}

// coordinatorProtocol uses the handshake on releases predating ProtocolInfoRequest.
func coordinatorProtocol(ctx context.Context, db *DbContext, ep wire.Endpoint) (uint64, error) {
	conn, err := db.Connection(ctx, ep)
	if err != nil {
		return 0, err
	}
	if conn.ProtocolVersion().Normalized() < wire.Protocol70.Normalized() {
		return uint64(conn.ProtocolVersion()), nil
	}
	return coordinatorCall(ctx, db, ep, wire.Request[uint64](protocolInfoReq{}))
}

// openControllerDatabase follows the elected controller on releases before 6.2.
// A controller long-poll must not hide a simultaneous coordinator leader change.
func (c *DbContext) openControllerDatabase(addr wire.NetAddr, req *wire.OpenDatabase) (wire.ClientDBInfo, error) {
	coordinator := wire.Endpoint{Addr: addr, Token: wire.WellKnown(tokenGetLeader)}
	leaderRequest := getLeaderReq{key: req.ClusterKey}
	current, err := coordinatorCall(c.ctx, c, coordinator, wire.Request[leaderReply](leaderRequest))
	if err != nil {
		return wire.ClientDBInfo{}, err
	}
	if !current.ok {
		return wire.ClientDBInfo{}, transport.NotDelivered
	}
	if current.leader.forward {
		return wire.ClientDBInfo{Forward: current.leader.info}, nil
	}
	controller, _, err := wire.DecodeControllerEndpointsLegacy(current.leader.info)
	if err != nil {
		return wire.ClientDBInfo{}, err
	}
	ctx, cancel := context.WithCancel(c.ctx)
	var workers sync.WaitGroup
	defer func() { cancel(); workers.Wait() }()
	type result struct {
		info wire.ClientDBInfo
		err  error
	}
	results := make(chan result, 2)
	workers.Go(func() {
		request := wire.OpenDatabaseController{KnownID: req.KnownID}
		info, err := call(ctx, c, controller, wire.Request[wire.ClientDBInfo](request))
		results <- result{info, err}
	})
	workers.Go(func() {
		leaderRequest.known = current.leader.changeID
		for {
			next, err := coordinatorCall(ctx, c, coordinator, wire.Request[leaderReply](leaderRequest))
			if err != nil {
				results <- result{err: err}
				return
			}
			if next.ok && next.leader.forward {
				results <- result{info: wire.ClientDBInfo{Forward: next.leader.info}}
				return
			}
			if !next.ok || next.leader.changeID != current.leader.changeID {
				results <- result{err: fdberr.BrokenPromise}
				return
			}
		}
	})
	select {
	case result := <-results:
		return result.info, result.err
	case <-ctx.Done():
		return wire.ClientDBInfo{}, cancelErr(ctx)
	}
}
