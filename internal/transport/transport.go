// Package transport is the client side of FoundationDB's FlowTransport
// protocol: connections to cluster processes, replica load balancing and a
// client-side failure monitor for storage reads, process-wide network
// options, and TLS.
//
// After a versioned ConnectPacket each way, every packet is
//
//	[u32 len][optional checksum][16-byte destination token][message]
//
// Checksums are CRC32C through 6.3 and XXH3-64 from 7.0. TLS omits the
// checksum except in 5.0. The negotiated connection selects binary (5.x–6.2)
// or object (6.2 onward) serialization independently of other connections.
//
// Replies return on the connection that carried the request, addressed to the
// token minted for its ReplyPromise, so one connection per server process
// multiplexes all traffic. Senders frame whole packets on their own goroutine
// and append them to the connection's output buffer; a writer goroutine
// swaps buffers and writes everything queued in one write. A monitor
// goroutine pings the peer like connectionMonitor and fails the connection
// when it stops answering; failure wakes every waiter.
package transport

import (
	"context"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"log/slog"
	"math"
	"math/rand/v2"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/zeebo/xxh3"

	"github.com/can1357/fdbgo/internal/fdberr"
	"github.com/can1357/fdbgo/internal/wire"
)

const (
	// objectSerializerFlag is set on the wire when messages use the object serializer.
	objectSerializerFlag uint64 = 0x1000000000000000
	// versionMask strips transport flags while preserving the peer's patch bits.
	versionMask uint64 = 0x0FFFFFFFFFFFFFFF
	// connectPacketLen is the ConnectPacket length, excluding the length field itself.
	connectPacketLen = 40
	// packetLimit is the largest packet accepted from a peer, matching the server limit.
	packetLimit = 100 << 20
	// monitorLoop is the interval between connection monitor pings.
	monitorLoop = time.Second
	// monitorTimeout is how long a ping may go unanswered before the connection fails.
	monitorTimeout = 2 * time.Second
	// idleTimeout is how long idle client connections are closed after this long.
	idleTimeout = 180 * time.Second
	// connectTimeout bounds establishing a connection, handshake included.
	connectTimeout = 5 * time.Second
	// pendingShards shards the outstanding-request map, so senders and the reader rarely share a lock.
	pendingShards = 16
	// readChunk is the size of the reader's receive buffers; replies alias them.
	readChunk = 256 << 10
	// minRead is the smallest free tail worth reading into before moving to a fresh buffer.
	minRead = 16 << 10
	// maxSpare caps the write buffer kept for reuse after a burst.
	maxSpare = 4 << 20
)

// RPCError says why an RPC produced no reply. Remote ErrorOr errors are
// returned as fdberr.Error instead.
type RPCError int

// RPC failure kinds.
const (
	// NotDelivered: the request never left this process (no connection could be made).
	NotDelivered RPCError = iota + 1
	// MaybeDelivered: the connection failed while the request was outstanding.
	MaybeDelivered
	// EndpointNotFound: the peer has no receiver for the destination token (its role is gone).
	EndpointNotFound
	// Unauthorized: the peer refused to serve a private endpoint to us.
	Unauthorized
	// Malformed: the reply did not parse.
	Malformed
	// Incompatible: the peer speaks an incompatible protocol version.
	Incompatible
)

// Error describes the failure kind.
func (e RPCError) Error() string {
	switch e {
	case NotDelivered:
		return "transport: request not delivered"
	case MaybeDelivered:
		return "transport: connection failed with request outstanding"
	case EndpointNotFound:
		return "transport: endpoint not found"
	case Unauthorized:
		return "transport: unauthorized endpoint"
	case Malformed:
		return "transport: malformed reply"
	case Incompatible:
		return "transport: incompatible protocol version"
	}
	return "transport: unknown error"
}

// Busyness accounting: nanoseconds the I/O goroutines of all connections spent
// working (parsing, dispatching, writing), and the busy fraction of the last
// completed one-second window as float64 bits.
var (
	busyNanos   atomic.Uint64
	busyness    atomic.Uint64
	samplerOnce sync.Once
)

// Busyness returns the fraction of the last second the client's network work
// kept a goroutine busy (networkInfo.metrics.lastRunLoopBusyness); 0 until a
// full window elapsed.
func Busyness() float64 { return math.Float64frombits(busyness.Load()) }

// StartBusynessSampler starts the once-per-process busyness sampler, which
// lives as long as the process (like the network thread it models).
func StartBusynessSampler() {
	samplerOnce.Do(func() {
		go func() {
			last := busyNanos.Load()
			at := time.Now()
			t := time.NewTicker(time.Second)
			for range t.C {
				now := busyNanos.Load()
				elapsed := max(float64(time.Since(at).Nanoseconds()), 1)
				busyness.Store(math.Float64bits(min(float64(now-last)/elapsed, 1)))
				last = now
				at = time.Now()
			}
		}()
	})
}

// recordBusy adds the time since start to the busy counter.
func recordBusy(start time.Time) {
	busyNanos.Add(uint64(time.Since(start).Nanoseconds()))
}

// replier receives the outcome of one request: the reply message (aliasing a
// receive buffer) or a failure kind. It runs on the reader goroutine (or the
// goroutine draining a failed connection) and must not block.
type replier interface {
	reply(msg []byte, err RPCError)
}

// pending is an outstanding request awaiting its reply.
type pending struct {
	r replier
	// dest is the destination token, matched against endpoint-not-found notices.
	dest wire.UID
}

// shard is one lock-protected slice of the outstanding-request map, padded to
// its own cache line.
type shard struct {
	mu sync.Mutex
	m  map[uint64]pending
	_  [48]byte
}

// Connection is one FlowTransport connection to a server process. It is safe
// for concurrent use; a failed connection never recovers.
type Connection struct {
	addr     wire.NetAddr
	protocol wire.ProtocolVersion
	// raw is the TCP socket, closed to fail the connection; rw carries the
	// FlowTransport stream (raw, or TLS on top of it).
	raw net.Conn
	rw  net.Conn
	// tokenHi is the first half of every reply token minted here; its low
	// bit (the stream flag) is clear.
	tokenHi  uint64
	nextID   atomic.Uint64
	checksum bool
	// object and checksumBytes are immutable after negotiation.
	object        bool
	checksumBytes int
	pending       [pendingShards]shard

	// dead is set before the pending map is drained on failure.
	dead atomic.Bool
	// closed is closed when the connection fails; done when its goroutines
	// have stopped and every outstanding request has been drained.
	closed   chan struct{}
	done     chan struct{}
	workers  atomic.Int32
	failOnce sync.Once

	// wmu guards out, the framed packets waiting for the writer; wake
	// (capacity 1) tells the writer there is something to write.
	wmu  sync.Mutex
	out  []byte
	wake chan struct{}

	bytesReceived atomic.Uint64
	bytesSent     atomic.Uint64
	created       time.Time
	// lastSendNanos is the time after created of the last request other than
	// a monitor ping.
	lastSendNanos atomic.Int64
}

// builders pools message builders for request encoding and framing.
var builders = sync.Pool{New: func() any { return new(wire.Builder) }}

// Connect dials addr (over TLS for :tls addresses), negotiates a supported
// protocol and starts the connection's goroutines. It initially advertises
// 7.4; a supported mismatch closes that socket and redials with the peer's
// protocol before any requests can be sent. A peer closing before its hello
// gets one fresh 5.0 probe: that release does not reply on a mismatch.
// All attempts share a 5s timeout.
// Failures are NotDelivered (no connection, TLS failure, timeout), Incompatible
// or Malformed (bad handshake); if ctx ends first its error is returned.
func Connect(ctx context.Context, addr wire.NetAddr) (*Connection, error) {
	if !addr.TLS && PlaintextDisabled() {
		slog.Debug("PlainTextConnectionDisabled", "addr", addr.String())
		return nil, NotDelivered
	}
	dctx, cancel := context.WithTimeout(ctx, connectTimeout)
	defer cancel()
	c, err := dial(dctx, addr)
	if err != nil {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		// The socket deadline can fire just before ctx's own timer.
		if dl, ok := ctx.Deadline(); ok && !time.Now().Before(dl) {
			return nil, context.DeadlineExceeded
		}
	}
	return c, err
}

func dial(ctx context.Context, addr wire.NetAddr) (*Connection, error) {
	var d net.Dialer
	advertised := wire.ProtocolVersion(uint64(wire.Protocol74) | objectSerializerFlag)
	tried := map[wire.ProtocolVersion]bool{advertised: true}
	for {
		raw, err := d.DialContext(ctx, "tcp", addr.Addr.String())
		if err != nil {
			return nil, NotDelivered
		}
		if tc, ok := raw.(*net.TCPConn); ok {
			_ = tc.SetNoDelay(true)
		}
		// All attempts use the same deadline, including TLS and protocol discovery.
		if dl, ok := ctx.Deadline(); ok {
			_ = raw.SetDeadline(dl)
		}
		stop := context.AfterFunc(ctx, func() { _ = raw.SetDeadline(time.Unix(1, 0)) })
		rw := raw
		if addr.TLS {
			tc, err := tlsConnect(ctx, raw)
			if err != nil {
				stop()
				raw.Close()
				return nil, NotDelivered
			}
			rw = tc
		}
		peer, err := handshake(rw, advertised)
		if !stop() {
			raw.Close()
			return nil, NotDelivered
		}
		if err != nil {
			raw.Close()
			if err == errNoHello {
				// 5.0 rejects mismatched versions before sending its ConnectPacket
				// (FlowTransport.actor.cpp:connectionReader/connectionIncoming).
				// Probe it once on a fresh socket, never with application bytes.
				next := wire.Protocol50
				if !tried[next] && ctx.Err() == nil {
					tried[next] = true
					advertised = next
					continue
				}
				return nil, NotDelivered
			}
			next := peer
			if err == Incompatible && validCodec(peer) && !tried[next] {
				// Ordinary RPC receivers require the exact protocol, including
				// patch bits, even when the handshake considers them compatible.
				// The initial ConnectPacket also fixes 6.2's serializer choice.
				// Never send RPCs on this socket.
				tried[next] = true
				advertised = next
				continue
			}
			return nil, err
		}
		_ = raw.SetDeadline(time.Time{})
		checksum := !addr.TLS || peer.Normalized() < wire.ProtocolVersion(0x0FDB00A551040000)
		return start(addr, raw, rw, checksum, peer), nil
	}
}

// handshake returns the peer's protocol after exchanging ConnectPackets.
// A supported mismatch returns the peer version, including its codec flag,
// with Incompatible so dial can retry on a new socket. An empty-header close
// returns errNoHello for the bounded 5.0 probe; no application bytes are sent.
func handshake(rw net.Conn, advertised wire.ProtocolVersion) (wire.ProtocolVersion, error) {
	var pkt [4 + connectPacketLen]byte
	size := connectPacketSize(advertised)
	binary.LittleEndian.PutUint32(pkt[0:], uint32(size))
	binary.LittleEndian.PutUint64(pkt[4:], uint64(advertised))
	// canonicalRemotePort, connectionId, ip4, flags and ip6 stay zero: we are a client.
	if n, err := rw.Write(pkt[:4+size]); err != nil || n != 4+size {
		return 0, NotDelivered
	}
	var hdr [12]byte
	if n, err := readFull(rw, hdr[:]); err != nil {
		if n == 0 && (errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, syscall.ECONNRESET)) {
			return 0, errNoHello
		}
		return 0, NotDelivered
	}
	n := binary.LittleEndian.Uint32(hdr[0:])
	version := binary.LittleEndian.Uint64(hdr[4:])
	if n < 22 || n > 1024 {
		return 0, Malformed
	}
	peer := wire.ProtocolVersion(version)
	if !validCodec(peer) {
		return 0, Incompatible
	}
	if int(n) < connectPacketSize(peer) {
		return 0, Malformed
	}
	for remaining := int(n) - 8; remaining > 0; {
		count := min(remaining, len(pkt))
		if _, err := readFull(rw, pkt[:count]); err != nil {
			return 0, NotDelivered
		}
		remaining -= count
	}
	if peer != advertised {
		return peer, Incompatible
	}
	return peer, nil
}

// ConnectPacket gained IPv6 fields in 6.1 (FlowTransport::ConnectPacket).
func connectPacketSize(protocol wire.ProtocolVersion) int {
	if protocol.HasIPv6() {
		return connectPacketLen
	}
	return 22
}

// 6.2 introduced a serializer knob; 6.3 removed binary transport entirely.
func validCodec(protocol wire.ProtocolVersion) bool {
	if uint64(protocol)&^(versionMask|objectSerializerFlag) != 0 || !protocol.Supported() {
		return false
	}
	object := uint64(protocol)&objectSerializerFlag != 0
	v := protocol.Normalized()
	if v < wire.ProtocolVersion(0x0FDB00B062010000) {
		return !object
	}
	return v == wire.ProtocolVersion(0x0FDB00B062010000) || object
}

var (
	crc32cTable = crc32.MakeTable(crc32.Castagnoli)
	errNoHello  = errors.New("transport: peer closed before ConnectPacket")
)

func readFull(r net.Conn, b []byte) (int, error) {
	n := 0
	for n < len(b) {
		m, err := r.Read(b[n:])
		n += m
		if err != nil {
			return n, err
		}
		if m == 0 {
			return n, io.ErrNoProgress
		}
	}
	return n, nil
}

// start wraps an established stream (raw, or TLS over raw) and starts the
// reader, writer and monitor goroutines; all stop when the connection fails.
func start(addr wire.NetAddr, raw, rw net.Conn, checksum bool, protocol wire.ProtocolVersion) *Connection {
	c := &Connection{
		addr:     addr,
		protocol: wire.ProtocolVersion(uint64(protocol) & versionMask),
		raw:      raw,
		rw:       rw,
		tokenHi:  rand.Uint64() &^ 1,
		checksum: checksum,
		closed:   make(chan struct{}),
		done:     make(chan struct{}),
		wake:     make(chan struct{}, 1),
		created:  time.Now(),
	}
	c.object = uint64(protocol)&objectSerializerFlag != 0 || protocol.UsesObjectSerializer()
	// A negotiated 6.2 peer may explicitly use binary instead of its default.
	if protocol.Normalized() == wire.ProtocolVersion(0x0FDB00B062010000) {
		c.object = uint64(protocol)&objectSerializerFlag != 0
	}
	if checksum {
		c.checksumBytes = 8
		if protocol.Normalized() < wire.ProtocolVersion(0x0FDB00B070010000) {
			c.checksumBytes = 4
		}
	}
	c.nextID.Store(1)
	for i := range c.pending {
		c.pending[i].m = make(map[uint64]pending)
	}
	c.workers.Store(3)
	go c.readLoop()
	go c.writeLoop()
	go c.monitor()
	return c
}

// Addr returns the address the connection was dialed to.
func (c *Connection) Addr() wire.NetAddr { return c.addr }

// ProtocolVersion returns the negotiated peer protocol without transport flags.
// Patch bits are preserved. The value never changes: reconnecting after a server
// upgrade creates a new Connection rather than reusing or replaying this one's RPCs.
func (c *Connection) ProtocolVersion() wire.ProtocolVersion { return c.protocol }

// UsesObjectSerializer reports the codec selected by the peer's ConnectPacket.
// Unlike ProtocolVersion.UsesObjectSerializer, it accounts for 6.2's knob.
func (c *Connection) UsesObjectSerializer() bool { return c.object }

// Stats returns the bytes received and sent so far and the time since the
// connection was established.
func (c *Connection) Stats() (received, sent uint64, age time.Duration) {
	return c.bytesReceived.Load(), c.bytesSent.Load(), time.Since(c.created)
}

// IsDead reports whether the connection has failed; a failed connection never recovers.
func (c *Connection) IsDead() bool { return c.dead.Load() }

// Close fails the connection: outstanding requests complete with
// MaybeDelivered (on the reader goroutine, shortly after Close returns).
func (c *Connection) Close() { c.fail() }

// Done returns a channel closed once the connection's goroutines have stopped
// and every outstanding request has been completed.
func (c *Connection) Done() <-chan struct{} { return c.done }

func (c *Connection) shard(id uint64) *shard { return &c.pending[id%pendingShards] }

// idle reports whether no request is outstanding.
func (c *Connection) idle() bool {
	for i := range c.pending {
		s := &c.pending[i]
		s.mu.Lock()
		n := len(s.m)
		s.mu.Unlock()
		if n != 0 {
			return false
		}
	}
	return true
}

// register records an outstanding request to dest and returns its reply id.
// ok is false when the connection is dead; then r has been or will be told
// exactly when removed is false (the drain took it).
func (c *Connection) register(dest wire.UID, r replier) (id uint64, ok, removed bool) {
	id = c.nextID.Add(1)
	s := c.shard(id)
	s.mu.Lock()
	s.m[id] = pending{r: r, dest: dest}
	s.mu.Unlock()
	// Pairs with fail: either the drain (which runs after dead is set) sees
	// our entry, or we see dead and withdraw it ourselves.
	if c.dead.Load() {
		return id, false, c.remove(id)
	}
	return id, true, false
}

// remove withdraws request id; false means its outcome is already being delivered.
func (c *Connection) remove(id uint64) bool {
	s := c.shard(id)
	s.mu.Lock()
	_, ok := s.m[id]
	if ok {
		delete(s.m, id)
	}
	s.mu.Unlock()
	return ok
}

// request encodes req with reply token id and queues it for dest.
func (c *Connection) request(dest wire.UID, req wire.Message, id uint64) error {
	if err := c.queueRequest(dest, req, id); err != nil {
		return err
	}
	c.wakeWriter()
	return nil
}

// queueRequest encodes and enqueues a request without waking the writer.
func (c *Connection) queueRequest(dest wire.UID, req wire.Message, id uint64) error {
	if dest != wire.WellKnown(wire.TokenPing) {
		c.lastSendNanos.Store(int64(time.Since(c.created)))
	}
	b := builders.Get().(*wire.Builder)
	b.Reset()
	b.Protocol = c.protocol
	reply := wire.UID{First: c.tokenHi, Second: id}
	if c.object {
		off := req.Encode(b, reply)
		b.FinishRequest(off, wire.MessageFileID(req, c.protocol))
	} else if err := wire.EncodeLegacy(b, req, reply, c.protocol); err != nil {
		builders.Put(b)
		return err
	}
	c.enqueue(b, dest)
	builders.Put(b)
	return nil
}

// destination resolves the client's current well-known tokens for this peer.
// 7.2 inserted UnauthorizedEndpoint before the coordinator endpoints, shifting
// GetLeader and OpenDatabase from 2/3 to 3/4. ProtocolInfo remains 10.
// Resolve only outgoing requests, once before registration and queueing: reply
// tokens and tokens named by failure notices are already peer-native.
func (c *Connection) destination(dest wire.UID) wire.UID {
	if c.protocol.Normalized() < wire.Protocol72 {
		switch dest {
		case wire.WellKnown(wire.TokenOpenDatabase - 1), wire.WellKnown(wire.TokenOpenDatabase):
			dest.Second--
		}
	}
	return dest
}

// Send queues a best-effort one-way notification without allocating a reply slot.
func (c *Connection) Send(dest wire.UID, msg wire.Message) {
	if wire.ValidateRequest(msg, c.protocol) == nil {
		_ = c.request(c.destination(dest), msg, 0)
	}
}

// enqueue frames the finished message in b for dest and queues it for the writer.
func (c *Connection) enqueue(b *wire.Builder, dest wire.UID) {
	tok := b.Raw(16)
	binary.LittleEndian.PutUint64(tok[0:], dest.First)
	binary.LittleEndian.PutUint64(tok[8:], dest.Second)
	n := len(b.Finished())
	if c.checksumBytes == 8 {
		sum := xxh3.Hash(b.Finished())
		hdr := b.Raw(12)
		binary.LittleEndian.PutUint32(hdr[0:], uint32(n))
		binary.LittleEndian.PutUint64(hdr[4:], sum)
	} else if c.checksumBytes == 4 {
		sum := crc32.Checksum(b.Finished(), crc32cTable)
		hdr := b.Raw(8)
		binary.LittleEndian.PutUint32(hdr[0:], uint32(n))
		binary.LittleEndian.PutUint32(hdr[4:], sum)
	} else {
		binary.LittleEndian.PutUint32(b.Raw(4), uint32(n))
	}
	c.wmu.Lock()
	if !c.dead.Load() {
		c.out = append(c.out, b.Finished()...)
	}
	c.wmu.Unlock()
}

func (c *Connection) wakeWriter() {
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

// waiter is a blocked Call's reply slot. It is allocated per call rather
// than pooled: a channel belongs to the synctest bubble that created it.
type waiter chan result

type result struct {
	msg []byte
	err RPCError
}

func (w waiter) reply(msg []byte, err RPCError) { w <- result{msg, err} }

// Call sends req to the stream dest and waits for its decoded reply. Errors
// are RPCError kinds, fdberr.Error for remote errors, or ctx's error when ctx
// ends first (the request's slot is released; a late reply is dropped).
func Call[R any](ctx context.Context, c *Connection, dest wire.UID, req wire.Request[R]) (R, error) {
	if err := wire.ValidateRequest(req, c.protocol); err != nil {
		var zero R
		return zero, err
	}
	dest = c.destination(dest)
	slot, err := registerCall(c, dest)
	if err != nil {
		var zero R
		return zero, err
	}
	if err := c.request(dest, req, slot.id); err != nil {
		if !c.remove(slot.id) {
			<-slot.w
		}
		var zero R
		return zero, err
	}
	return collectCall(ctx, c, req, slot)
}

// callSlot owns a registered request until collectCall releases it.
type callSlot struct {
	id uint64
	w  waiter
}

func registerCall(c *Connection, dest wire.UID) (callSlot, error) {
	slot := callSlot{w: make(waiter, 1)}
	id, ok, removed := c.register(dest, slot.w)
	slot.id = id
	if !ok {
		if !removed {
			<-slot.w
		}
		return callSlot{}, NotDelivered
	}
	return slot, nil
}

func collectCall[R any](ctx context.Context, c *Connection, req wire.Request[R], slot callSlot) (R, error) {
	var zero R
	var res result
	select {
	case res = <-slot.w:
	case <-ctx.Done():
		if c.remove(slot.id) {
			return zero, ctx.Err()
		}
		// The reader already took the slot; its send is imminent.
		res = <-slot.w
	}
	return decodeResult(c, req, res)
}

func decodeResult[R any](c *Connection, req wire.Request[R], res result) (R, error) {
	if res.err != 0 {
		var zero R
		return zero, res.err
	}
	return decode(c, req, res.msg)
}

// BatchCall is one request in a blocking batch, possibly sharing a connection
// with other requests. Connection must be non-nil.
type BatchCall[R any] struct {
	Connection  *Connection
	Destination wire.UID
	Request     wire.Request[R]
}

// BatchReply is the outcome at the corresponding input position.
type BatchReply[R any] struct {
	Value R
	Err   error
}

// batchCompletion wakes the collector once, after every slot has settled.
type batchCompletion struct {
	remaining atomic.Int64
	done      chan struct{}
}

// batchSlot is completed by either the connection's reply owner or cancellation
// after remove succeeds. Its result is published by batchCompletion.done.
type batchSlot struct {
	id         uint64
	completion *batchCompletion
	res        result
	err        error
	failed     bool
}

func (s *batchSlot) reply(msg []byte, err RPCError) {
	s.res = result{msg, err}
	s.complete()
}

func (s *batchSlot) complete() {
	if s.completion.remaining.Add(-1) == 0 {
		close(s.completion.done)
	}
}

// CallBatch registers and queues all requests before waking each connection
// once. It collects every slot, even after an error or cancellation, and uses
// the same reply routing and cancellation arbitration as Call.
func CallBatch[R any](ctx context.Context, calls []BatchCall[R]) []BatchReply[R] {
	replies := make([]BatchReply[R], len(calls))
	if len(calls) == 0 {
		return replies
	}
	slots := make([]batchSlot, len(calls))
	completion := &batchCompletion{done: make(chan struct{})}
	completion.remaining.Store(int64(len(calls)))
	// Most batches share one connection. Keep that case allocation-free.
	var first *Connection
	var others map[*Connection]struct{}
	for i, call := range calls {
		slot := &slots[i]
		slot.completion = completion
		if err := wire.ValidateRequest(call.Request, call.Connection.protocol); err != nil {
			slot.err = err
			slot.complete()
			continue
		}
		dest := call.Connection.destination(call.Destination)
		id, ok, removed := call.Connection.register(dest, slot)
		slot.id = id
		if !ok {
			slot.failed = true
			if removed {
				slot.complete()
			}
			continue
		}
		if err := call.Connection.queueRequest(dest, call.Request, id); err != nil {
			slot.err = err
			if call.Connection.remove(id) {
				slot.complete()
			}
			continue
		}
		if first == nil {
			first = call.Connection
		} else if call.Connection != first {
			if others == nil {
				others = make(map[*Connection]struct{})
			}
			others[call.Connection] = struct{}{}
		}
	}
	if first != nil {
		first.wakeWriter()
	}
	for c := range others {
		c.wakeWriter()
	}
	select {
	case <-completion.done:
	case <-ctx.Done():
		for i := range slots {
			slot := &slots[i]
			if slot.id != 0 && calls[i].Connection.remove(slot.id) {
				slot.err = ctx.Err()
				slot.complete()
			}
		}
		// Removed slots belong to their reader or failure drain until reply
		// publication. Join them before reading any result or releasing state.
		<-completion.done
	}
	for i := range slots {
		slot := &slots[i]
		switch {
		case slot.failed:
			replies[i].Err = NotDelivered
		case slot.err != nil:
			replies[i].Err = slot.err
		default:
			replies[i].Value, replies[i].Err = decodeResult(calls[i].Connection, calls[i].Request, slot.res)
		}
	}
	return replies
}

// decode unwraps a reply message, mapping parse failures to Malformed.
func decode[R any](c *Connection, req wire.Request[R], msg []byte) (R, error) {
	var r R
	var err error
	if c.object {
		r, err = wire.DecodeReply(req, msg, c.protocol)
	} else {
		r, err = wire.DecodeLegacy(req, msg, c.protocol)
	}
	if err != nil {
		var code fdberr.Error
		if errors.As(err, &code) {
			return r, code
		}
		var zero R
		return zero, Malformed
	}
	return r, nil
}

// fail marks the connection failed and closes the socket; the reader then
// drains every outstanding request with MaybeDelivered.
func (c *Connection) fail() {
	c.failOnce.Do(func() {
		c.dead.Store(true)
		close(c.closed)
		_ = c.raw.Close()
		c.wmu.Lock()
		c.out = nil
		c.wmu.Unlock()
	})
}

// drain completes every outstanding request with MaybeDelivered.
func (c *Connection) drain() {
	for i := range c.pending {
		s := &c.pending[i]
		s.mu.Lock()
		m := s.m
		s.m = make(map[uint64]pending)
		s.mu.Unlock()
		for _, p := range m {
			p.r.reply(nil, MaybeDelivered)
		}
	}
}

func (c *Connection) workerDone() {
	if c.workers.Add(-1) == 0 {
		close(c.done)
	}
}

// writeLoop double-buffers c.out: senders append to one buffer while the
// other is on the wire. Small growing batches give runnable senders bounded
// scheduling turns to join the batch, without waiting for a timer or more work.
// The written buffer always becomes the next spare, even when empty, or the
// two would alias.
func (c *Connection) writeLoop() {
	defer c.workerDone()
	var spare []byte
	for {
		select {
		case <-c.wake:
		case <-c.closed:
			return
		}
		c.wmu.Lock()
		// Stop as soon as a scheduling turn adds no work. The bound prevents
		// sustained producers from postponing a flush indefinitely.
		for turns := 0; turns < 4 && len(c.out) > 0 && len(c.out) < 8<<10; turns++ {
			n := len(c.out)
			c.wmu.Unlock()
			runtime.Gosched()
			c.wmu.Lock()
			if len(c.out) == n {
				break
			}
		}
		buf := c.out
		c.out = spare[:0]
		c.wmu.Unlock()
		if len(buf) > 0 {
			start := time.Now()
			NoteTime(start)
			if n, err := c.rw.Write(buf); err != nil || n != len(buf) {
				c.fail()
				return
			}
			recordBusy(start)
			c.bytesSent.Add(uint64(len(buf)))
		}
		if cap(buf) > maxSpare {
			buf = nil
		}
		spare = buf
	}
}

// readLoop parses packets into replies that alias its receive buffers: a
// buffer that handed out a reply is never written again, and a fresh one
// takes over when it fills up. On exit it fails the connection and drains
// the outstanding requests.
func (c *Connection) readLoop() {
	defer c.workerDone()
	defer c.drain()
	defer c.fail()
	hdrLen := 4 + c.checksumBytes
	buf := make([]byte, readChunk)
	r, w := 0, 0
	// shared is set once a reply aliasing buf was delivered.
	shared := false
	for {
		start := time.Now()
		NoteTime(start)
		for w-r >= hdrLen {
			n := int(binary.LittleEndian.Uint32(buf[r:]))
			if n < 16 || n > packetLimit {
				return
			}
			if w-r < hdrLen+n {
				break
			}
			pkt := buf[r+hdrLen : r+hdrLen+n : r+hdrLen+n]
			if c.checksumBytes == 8 && xxh3.Hash(pkt) != binary.LittleEndian.Uint64(buf[r+4:]) {
				return
			}
			if c.checksumBytes == 4 && crc32.Checksum(pkt, crc32cTable) != binary.LittleEndian.Uint32(buf[r+4:]) {
				return
			}
			r += hdrLen + n
			if c.deliver(pkt) {
				shared = true
			}
		}
		recordBusy(start)
		if r == w && !shared {
			r, w = 0, 0
		}
		// Make room for the rest of a partial packet, or at least minRead bytes.
		need := minRead
		if w-r >= hdrLen {
			need = max(need, hdrLen+int(binary.LittleEndian.Uint32(buf[r:]))-(w-r))
		}
		if len(buf)-w < need {
			size := max(readChunk, (w-r)+need)
			if shared || size > len(buf) {
				next := make([]byte, size)
				copy(next, buf[r:w])
				buf = next
				shared = false
			} else {
				copy(buf, buf[r:w])
			}
			w -= r
			r = 0
		}
		m, err := c.rw.Read(buf[w:])
		if m > 0 {
			w += m
			c.bytesReceived.Add(uint64(m))
		}
		if err != nil {
			return
		}
	}
}

// deliver routes one received packet ([token][message]); it reports whether
// the message was handed to a request (and so must not be overwritten).
func (c *Connection) deliver(pkt []byte) bool {
	tok := wire.UID{First: binary.LittleEndian.Uint64(pkt[0:]), Second: binary.LittleEndian.Uint64(pkt[8:])}
	msg := pkt[16:]
	if tok.First == c.tokenHi {
		s := c.shard(tok.Second)
		s.mu.Lock()
		p, ok := s.m[tok.Second]
		if ok {
			delete(s.m, tok.Second)
		}
		s.mu.Unlock()
		if ok {
			p.r.reply(msg, 0)
		}
		return ok
	}
	switch tok {
	case wire.WellKnown(wire.TokenPing):
		// Servers ping clients that go quiet; answer like the PingReceiver does.
		var reply wire.UID
		var err error
		if c.object {
			reply, err = wire.PingReplyToken(msg, c.protocol)
		} else {
			reply, err = wire.PingReplyTokenLegacy(msg, c.protocol)
		}
		if err == nil {
			b := builders.Get().(*wire.Builder)
			b.Reset()
			b.Protocol = c.protocol
			if c.object {
				wire.EncodeVoidReply(b)
			} else {
				wire.EncodeVoidReplyLegacy(b, c.protocol)
			}
			c.enqueue(b, reply)
			c.wakeWriter()
			builders.Put(b)
		}
	case wire.WellKnown(wire.TokenEndpointNotFound):
		if gone, err := c.noticeToken(msg); err == nil {
			c.failDest(gone, EndpointNotFound)
		}
	case wire.WellKnown(wire.TokenUnauthorizedEndpoint):
		if c.protocol.Normalized() < wire.Protocol72 {
			break
		}
		if denied, err := c.noticeToken(msg); err == nil {
			c.failDest(denied, Unauthorized)
		}
	}
	return false
}

func (c *Connection) noticeToken(msg []byte) (wire.UID, error) {
	if c.object {
		return wire.NoticeToken(msg, c.protocol)
	}
	return wire.NoticeTokenLegacy(msg, c.protocol)
}

// failDest fails every request sent to dest.
func (c *Connection) failDest(dest wire.UID, err RPCError) {
	var failed []replier
	for i := range c.pending {
		s := &c.pending[i]
		s.mu.Lock()
		for id, p := range s.m {
			if p.dest == dest {
				failed = append(failed, p.r)
				delete(s.m, id)
			}
		}
		s.mu.Unlock()
	}
	for _, r := range failed {
		r.reply(nil, err)
	}
}

// monitor pings the peer every second and fails the connection when it goes
// silent; it closes the connection after
// idleTimeout without traffic or outstanding requests.
func (c *Connection) monitor() {
	defer c.workerDone()
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	ping := wire.WellKnown(wire.TokenPing)
	w := make(waiter, 1)
	for {
		timer.Reset(monitorLoop + rand.N(200*time.Millisecond))
		select {
		case <-timer.C:
		case <-c.closed:
			return
		}
		sinceSend := time.Since(c.created) - time.Duration(c.lastSendNanos.Load())
		if sinceSend > idleTimeout && c.idle() {
			c.fail()
			return
		}
		id, ok, removed := c.register(ping, w)
		if !ok {
			if !removed {
				<-w
			}
			return
		}
		if err := c.request(ping, wire.Ping{}, id); err != nil {
			c.fail()
			<-w
			return
		}
		seen := c.bytesReceived.Load()
	wait:
		for {
			timer.Reset(monitorTimeout)
			select {
			case <-w:
				break wait
			case <-timer.C:
				now := c.bytesReceived.Load()
				if now == seen {
					slog.Debug("connection timed out", "peer", c.addr.String())
					c.fail()
					<-w // the drain completes the ping
					return
				}
				seen = now
			}
		}
	}
}
