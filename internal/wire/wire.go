package wire

// FDB 7.3/7.4 RPC messages: encoders for the requests a client sends and decoders
// for the replies it receives. Each layout follows the serializer() field
// order of the same-named FoundationDB message, with slot numbers assigned
// per flat.go: unions take two slots, Arena members none. Fields a client
// never sets are omitted; Flow's reader default-constructs them.
//
// The client-facing ObjectSerializer layouts and file identifiers are identical
// in upstream 7.3.81 and 7.4.8: CoordinationInterface.h (OpenDatabase),
// CommitProxyInterface.h (ClientDBInfo, GRV, commit, locations, idempotency
// expiration), StorageServerInterface.h (reads, watches, metrics, splits), and
// FlowTransport.actor.cpp (ping and notices). Their nested FDBTypes.h,
// TagThrottle.actor.h, IdempotencyId.actor.h, TenantInfo.h and network types also
// share layouts. There is deliberately no protocol branch in these codecs.
// 7.4 only appends streams beyond the interface offsets we use. Its optional
// MutationRef checksums do not change the unflagged mutations emitted here.

import (
	"bytes"
	"encoding/binary"
	"math"
	"net/netip"
	"slices"
	"strconv"
	"time"

	"github.com/can1357/fdbgo/internal/fdberr"
	"github.com/can1357/fdbgo/internal/kv"
)

// Well-known tokens.
const (
	// TokenEndpointNotFound is WLTOKEN_ENDPOINT_NOT_FOUND: a peer has no receiver for a token we sent to.
	TokenEndpointNotFound uint64 = 0
	// TokenPing is WLTOKEN_PING_PACKET: connection liveness pings.
	TokenPing uint64 = 1
	// TokenUnauthorizedEndpoint is WLTOKEN_UNAUTHORIZED_ENDPOINT: a peer refused a private endpoint.
	TokenUnauthorizedEndpoint uint64 = 2
	// TokenOpenDatabase is WLTOKEN_CLIENTLEADERREG_OPENDATABASE: coordinator endpoint serving ClientDBInfo.
	TokenOpenDatabase uint64 = 4
)

const (
	// InvalidVersion is invalidVersion.
	InvalidVersion int64 = -1
	// LatestVersion is latestVersion.
	LatestVersion int64 = -2
)

const (
	// FIDErrorOr is the file identifier of ErrorOr<T>'s composed wrapper (high byte 2 marks ErrorOr).
	FIDErrorOr uint32 = 2 << 24
	// FIDVoid is the file identifier of Void.
	FIDVoid uint32 = 2010442
)

// WellKnown returns the well-known token id.
func WellKnown(id uint64) UID { return UID{^uint64(0), id} }

// NetAddr is one public address of a process (NetworkAddress).
type NetAddr struct {
	Addr netip.AddrPort
	TLS  bool
}

// String renders "ip:port[:tls]".
func (a NetAddr) String() string {
	s := a.Addr.String()
	if a.TLS {
		s += ":tls"
	}
	return s
}

// Endpoint addresses one request stream on one process.
type Endpoint struct {
	Addr NetAddr
	// Alt is the other-protocol address of a process listening on both TLS and
	// plaintext; Alt.Addr invalid means none.
	Alt   NetAddr
	Token UID
}

// Adjusted returns the endpoint of stream i: interfaces serialize one
// base endpoint and derive their other streams by index.
func (e Endpoint) Adjusted(i uint32) Endpoint {
	idx := uint32(e.Token.Second) + i
	e.Token = UID{e.Token.First + uint64(i)<<32, e.Token.Second&0xffff_ffff_0000_0000 | uint64(idx)}
	return e
}

// Address returns the address to dial, preferring one whose TLS-ness matches
// ours.
func (e Endpoint) Address(tls bool) NetAddr {
	if e.Alt.Addr.IsValid() && e.Addr.TLS != tls && e.Alt.TLS == tls {
		return e.Alt
	}
	return e.Addr
}

// ReadOpts are values a transaction may attach to a read request (ReadOptions).
type ReadOpts struct {
	// ReadType: EAGER=0, FETCH=1, LOW=2, NORMAL=3, HIGH=4.
	ReadType    int32
	CacheResult bool
	LockAware   bool
	// DebugID enables server-side request tracing when nonzero.
	DebugID UID
}

// DefaultReadOpts returns ReadOptions' defaults.
func DefaultReadOpts() ReadOpts { return ReadOpts{ReadType: 3, CacheResult: true} }

// ReadCtx is the per-request transaction context shared by storage reads.
type ReadCtx struct {
	Version int64
	// Tags are transaction tags (TagSet) sent for throttling and sampling.
	Tags [][]byte
	// Options is sent only when HasOptions (otherwise storage servers use defaults).
	Options    ReadOpts
	HasOptions bool
}

// ---- shared encoders -------------------------------------------------------

// replyPromise writes ReplyPromise<T>: a table holding the reply token.
func replyPromise(b *Builder, token UID) Off {
	b.StartTable(1)
	b.AddUID(0, token)
	return b.EndTable()
}

// noTenant writes TenantInfo{tenantId=-1}. Requests must carry it explicitly:
// an absent TenantInfo deserializes as unauthorized and fails verify().
func noTenant(b *Builder) Off {
	b.StartTable(3)
	b.AddI64(0, -1)
	return b.EndTable()
}

// tagSet writes TagSet (dynamic size): [u8 len][tag bytes] per tag.
func tagSet(b *Builder, tags [][]byte) Off {
	n := 0
	for _, t := range tags {
		n += 1 + len(t)
	}
	off, out := b.Blob(n)
	p := 0
	for _, t := range tags {
		out[p] = uint8(len(t))
		copy(out[p+1:], t)
		p += 1 + len(t)
	}
	return off
}

// readOptions writes ReadOptions: type, cacheResult, debugID(2), consistencyCheckStartVersion(2), lockAware.
func readOptions(b *Builder, o ReadOpts) Off {
	debug := optionalUID(b, o.DebugID)
	b.StartTable(7)
	b.AddI32(0, o.ReadType)
	b.AddBool(1, o.CacheResult)
	if debug != 0 {
		b.AddUnion(2, 1, debug)
	}
	b.AddBool(6, o.LockAware)
	return b.EndTable()
}

func optionalUID(b *Builder, id UID) Off {
	if !id.IsValid() {
		return 0
	}
	return b.ScalarUID(id)
}

// keySelector writes KeySelectorRef: key, orEqual, offset.
func keySelector(b *Builder, s *kv.KeySelector) Off {
	k := b.Bytes(s.Key)
	b.StartTable(3)
	b.AddOffset(0, k)
	b.AddBool(1, s.OrEqual)
	b.AddI32(2, int32(s.Offset))
	return b.EndTable()
}

// decodeKeyRange restores KeyRangeRef's compact single-key representation.
func decodeKeyRange(t Table) (kv.KeyRange, error) {
	var r kv.KeyRange
	var err error
	if r.Begin, err = t.bytesOrEmpty(0); err != nil {
		return r, err
	}
	if r.End, err = t.bytesOrEmpty(1); err != nil {
		return r, err
	}
	if len(r.Begin) > 0 && len(r.End) == 0 {
		if r.Begin[len(r.Begin)-1] != 0 {
			return r, ErrMalformed
		}
		r.End = r.Begin
		r.Begin = r.End[:len(r.End)-1]
	}
	if bytes.Compare(r.Begin, r.End) > 0 {
		return r, ErrMalformed
	}
	return r, nil
}

// keyRange writes KeyRangeRef: begin, end.
func keyRange(b *Builder, begin, end []byte) Off {
	bo := b.Bytes(begin)
	eo := b.Bytes(end)
	b.StartTable(2)
	b.AddOffset(0, bo)
	b.AddOffset(1, eo)
	return b.EndTable()
}

// networkAddress writes IPAddress (variant<uint32_t, array<uint8_t,16>>)
// inside NetworkAddress{ip, port, flags, fromHostname}.
func networkAddress(b *Builder, a NetAddr) Off {
	ip := a.Addr.Addr()
	var ipo Off
	if ip.Is4() {
		o := ip.As4()
		v := b.ScalarU32(uint32(o[0])<<24 | uint32(o[1])<<16 | uint32(o[2])<<8 | uint32(o[3]))
		b.StartTable(2)
		b.AddUnion(0, 1, v)
		ipo = b.EndTable()
	} else {
		o := ip.As16()
		v := b.Bytes(o[:])
		b.StartTable(2)
		b.AddUnion(0, 2, v)
		ipo = b.EndTable()
	}
	b.StartTable(4)
	b.AddOffset(0, ipo)
	b.AddU16(1, a.Addr.Port())
	if a.TLS {
		b.AddU16(2, 2)
	}
	return b.EndTable()
}

// ctxOffs holds the storage-read context fields shared by GetValue/GetKey/GetKeyValues.
type ctxOffs struct {
	tags, options, tenant Off
}

func writeCtx(b *Builder, c *ReadCtx) ctxOffs {
	var o ctxOffs
	if len(c.Tags) > 0 {
		o.tags = tagSet(b, c.Tags)
	}
	if c.HasOptions {
		o.options = readOptions(b, c.Options)
	}
	o.tenant = noTenant(b)
	return o
}

// ---- shared decoders -------------------------------------------------------

func netAddr(na Table) (NetAddr, error) {
	ipt, err := na.mustTable(0)
	if err != nil {
		return NetAddr{}, err
	}
	tag, f, err := ipt.Union(0)
	if err != nil {
		return NetAddr{}, err
	}
	var ip netip.Addr
	switch tag {
	case 1:
		v, err := f.U32()
		if err != nil {
			return NetAddr{}, err
		}
		ip = netip.AddrFrom4([4]byte{byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)})
	case 2:
		vec, err := f.Vector()
		if err != nil {
			return NetAddr{}, err
		}
		o, err := vec.AsBytes()
		if err != nil {
			return NetAddr{}, err
		}
		if len(o) != 16 {
			return NetAddr{}, ErrMalformed
		}
		ip = netip.AddrFrom16([16]byte(o))
	default:
		return NetAddr{}, ErrMalformed
	}
	port, err := na.U16(1)
	if err != nil {
		return NetAddr{}, err
	}
	flags, err := na.U16(2)
	if err != nil {
		return NetAddr{}, err
	}
	return NetAddr{netip.AddrPortFrom(ip, port), flags&2 != 0}, nil
}

// endpoint decodes Endpoint{addresses: NetworkAddressList{address, secondaryAddress(2)}, token}.
func endpoint(ep Table) (Endpoint, error) {
	list, err := ep.mustTable(0)
	if err != nil {
		return Endpoint{}, err
	}
	at, err := list.mustTable(0)
	if err != nil {
		return Endpoint{}, err
	}
	var e Endpoint
	if e.Addr, err = netAddr(at); err != nil {
		return Endpoint{}, err
	}
	tag, f, err := list.Union(1)
	if err != nil {
		return Endpoint{}, err
	}
	if tag == 1 {
		t, err := f.Table()
		if err != nil {
			return Endpoint{}, err
		}
		if e.Alt, err = netAddr(t); err != nil {
			return Endpoint{}, err
		}
	}
	e.Token, err = ep.UID(1)
	return e, err
}

// RequestStreamEndpoint decodes RequestStream<T>: a table holding its Endpoint.
func RequestStreamEndpoint(t Table) (Endpoint, error) {
	ep, err := t.mustTable(0)
	if err != nil {
		return Endpoint{}, err
	}
	return endpoint(ep)
}

// lbError decodes the Optional<Error> of a LoadBalancedReply as an error code (0 = none).
func lbError(t Table, slot int) (fdberr.Error, error) {
	tag, f, err := t.Union(slot)
	switch {
	case err != nil:
		return 0, err
	case tag == 0:
		return 0, nil
	case tag != 1:
		return 0, ErrMalformed
	}
	et, err := f.Table()
	if err != nil {
		return 0, err
	}
	c, err := et.U16(0)
	return fdberr.Error(c), err
}

// optionalBytes decodes Optional<Value> at slot (nil when absent).
func optionalBytes(t Table, slot int) ([]byte, error) {
	tag, f, err := t.Union(slot)
	switch {
	case err != nil:
		return nil, err
	case tag == 0:
		return nil, nil
	case tag != 1:
		return nil, ErrMalformed
	}
	return f.Bytes()
}

func selOf(t Table) (kv.KeySelector, error) {
	k, err := t.bytesOrEmpty(0)
	if err != nil {
		return kv.KeySelector{}, err
	}
	oe, err := t.Bool(1)
	if err != nil {
		return kv.KeySelector{}, err
	}
	off, err := t.I32(2)
	return kv.KeySelector{Key: k, OrEqual: oe, Offset: int(off)}, err
}

// ---- framing ---------------------------------------------------------------

// Message is the encoding contract for RPC requests and one-way notifications.
type Message interface {
	// FileID is the message type's file_identifier.
	FileID() uint32
	// Encode writes the message table; one-way messages ignore reply.
	Encode(b *Builder, reply UID) Off
}

// Request is an RPC request whose reply decodes to R.
type Request[R any] interface {
	Message
	// ReplyFileID identifies the complete ErrorOr<EnsureTable<T>> reply.
	ReplyFileID() uint32
	// Decode decodes reply table t; msg is the whole reply buffer and decoded slices alias it.
	Decode(msg []byte, t Table) (R, error)
}

// ReplyTable unwraps a reply message's ErrorOr<EnsureTable<T>> into T's
// table. A remote error is returned as fdberr.Error; a parse failure as ErrMalformed.
func ReplyTable(msg []byte) (Table, error) {
	root, err := Root(msg)
	if err != nil {
		return Table{}, err
	}
	tag, f, err := root.Union(0)
	if err != nil {
		return Table{}, err
	}
	switch tag {
	case 1:
		et, err := f.Table()
		if err != nil {
			return Table{}, err
		}
		c, err := et.U16(0)
		if err != nil {
			return Table{}, err
		}
		return Table{}, fdberr.Error(c)
	case 2:
		return f.Table()
	}
	return Table{}, ErrMalformed
}

// DecodeReply unwraps msg with ReplyTable and decodes it as req's reply.
func DecodeReply[R any](req Request[R], msg []byte) (R, error) {
	if len(msg) < 8 || binary.LittleEndian.Uint32(msg[4:8]) != req.ReplyFileID() {
		var zero R
		return zero, ErrMalformed
	}
	t, err := ReplyTable(msg)
	if err != nil {
		var zero R
		return zero, err
	}
	return req.Decode(msg, t)
}

// EncodeRequest resets b and writes req as a complete message with reply token reply.
func EncodeRequest[R any](b *Builder, req Request[R], reply UID) []byte {
	b.Reset()
	off := req.Encode(b, reply)
	return b.FinishRequest(off, req.FileID())
}

// ---- messages --------------------------------------------------------------

// ClientDBInfo is the cluster's proxies, as served by coordinators.
type ClientDBInfo struct {
	ID UID
	// GRV holds GrvProxyInterface.getConsistentReadVersion of every proxy.
	GRV []ProxyEndpoint
	// Commit holds CommitProxyInterface.commit (base endpoint of the interface).
	Commit []ProxyEndpoint
	// Forward is the new connection string when the coordinators moved (nil = none).
	Forward []byte
	// ClusterID identifies the cluster (stable across recoveries).
	ClusterID UID
}

// ProxyEndpoint is one proxy's base endpoint and whether it belongs to a recovering generation.
type ProxyEndpoint struct {
	Endpoint    Endpoint
	Provisional bool
}

// Hostname is one hostname coordinator entry (host, port, tls).
type Hostname struct {
	Host string
	Port uint16
	TLS  bool
}

// OpenDatabase is OpenDatabaseCoordRequest: issues, supportedVersions,
// traceLogGroup, knownClientInfoID, clusterKey, coordinators, reply,
// hostnames, internal.
type OpenDatabase struct {
	KnownID      UID
	ClusterKey   []byte
	Coordinators []NetAddr
	Hostnames    []Hostname
}

// FileID implements Request.
func (OpenDatabase) FileID() uint32 { return 214728 }

// ReplyFileID identifies an ErrorOr<ClientDBInfo> reply.
func (OpenDatabase) ReplyFileID() uint32 { return FIDErrorOr | 5355080 }

// Encode implements Request.
func (r OpenDatabase) Encode(b *Builder, reply UID) Off {
	key := b.Bytes(r.ClusterKey)
	m := b.Mark()
	for _, c := range r.Coordinators {
		b.Push(networkAddress(b, c))
	}
	coords := b.Offsets(m)
	var hosts Off
	if len(r.Hostnames) > 0 {
		m = b.Mark()
		for _, h := range r.Hostnames {
			ho := b.String(h.Host)
			var pb [8]byte
			so := b.Bytes(strconv.AppendUint(pb[:0], uint64(h.Port), 10))
			b.StartTable(3)
			b.AddOffset(0, ho)
			b.AddOffset(1, so)
			b.AddBool(2, h.TLS)
			b.Push(b.EndTable())
		}
		hosts = b.Offsets(m)
	}
	rp := replyPromise(b, reply)
	b.StartTable(9)
	if r.KnownID.IsValid() {
		b.AddUID(3, r.KnownID)
	}
	b.AddOffset(4, key)
	b.AddOffset(5, coords)
	b.AddOffset(6, rp)
	if hosts != 0 {
		b.AddOffset(7, hosts)
	}
	b.AddBool(8, true)
	return b.EndTable()
}

// Decode implements Request: ClientDBInfo is grvProxies, commitProxies, id,
// forward(2), history, tenantMode, encryptKeyProxy(2), clusterId, clusterType,
// metaclusterName(2). Both proxy interfaces serialize processId(2),
// provisional, then their base stream.
func (OpenDatabase) Decode(msg []byte, t Table) (ClientDBInfo, error) {
	proxies := func(slot int) ([]ProxyEndpoint, error) {
		v, err := t.Vector(slot)
		if err != nil {
			return nil, err
		}
		out := make([]ProxyEndpoint, 0, v.capHint())
		for i := range v.Len() {
			p, err := v.Table(i)
			if err != nil {
				return nil, err
			}
			st, err := p.mustTable(3)
			if err != nil {
				return nil, err
			}
			ep, err := RequestStreamEndpoint(st)
			if err != nil {
				return nil, err
			}
			prov, err := p.Bool(2)
			if err != nil {
				return nil, err
			}
			out = append(out, ProxyEndpoint{ep, prov})
		}
		return out, nil
	}
	var info ClientDBInfo
	var err error
	if info.GRV, err = proxies(0); err != nil {
		return ClientDBInfo{}, err
	}
	if info.Commit, err = proxies(1); err != nil {
		return ClientDBInfo{}, err
	}
	if info.ID, err = t.UID(2); err != nil {
		return ClientDBInfo{}, err
	}
	if info.Forward, err = optionalBytes(t, 3); err != nil {
		return ClientDBInfo{}, err
	}
	if info.ClusterID, err = t.UID(9); err != nil {
		return ClientDBInfo{}, err
	}
	return info, nil
}

// GetReadVersionRequest priority and flag bits.
const (
	GRVPrioritySystemImmediate uint32 = 15 << 24
	GRVPriorityDefault         uint32 = 8 << 24
	GRVPriorityBatch           uint32 = 1 << 24
	GRVUseProvisionalProxies   uint32 = 2
	GRVCausalReadRisky         uint32 = 1
)

// TagCount is one entry of a TransactionTagMap<uint32_t>.
type TagCount struct {
	Tag   []byte
	Count uint32
}

// GetReadVersion is GetReadVersionRequest: transactionCount, flags, tags,
// debugID(2), reply, spanContext, maxVersion.
type GetReadVersion struct {
	Count uint32
	Flags uint32
	// Tags are per-tag transaction counts.
	Tags []TagCount
	// DebugID enables server-side request tracing when nonzero.
	DebugID UID
}

// ThrottledTag is a tag throttled for this priority.
type ThrottledTag struct {
	Tag []byte
	// Rate is the tps rate; Expiration is in seconds since the epoch.
	Rate, Expiration float64
}

// ReadVersionReply is GetReadVersionReply: processBusyTime, version, locked,
// metadataVersion(2), tagThrottleInfo, midShardSize, rkDefaultThrottled,
// rkBatchThrottled, ssVersionVectorDelta, proxyId, proxyTagThrottledDuration.
type ReadVersionReply struct {
	Version            int64
	Locked             bool
	MetadataVersion    []byte
	Throttled          []ThrottledTag
	MidShardSize       int64
	RKDefaultThrottled bool
	RKBatchThrottled   bool
}

// FileID implements Request.
func (GetReadVersion) FileID() uint32 { return 838566 }

// ReplyFileID identifies an ErrorOr<GetReadVersionReply> reply.
func (GetReadVersion) ReplyFileID() uint32 { return FIDErrorOr | 15709388 }

// Encode implements Request.
func (r GetReadVersion) Encode(b *Builder, reply UID) Off {
	debug := optionalUID(b, r.DebugID)
	var tags Off
	if len(r.Tags) > 0 {
		m := b.Mark()
		for _, tc := range r.Tags {
			t := b.Bytes(tc.Tag)
			b.StartTable(2)
			b.AddOffset(0, t)
			b.AddU32(1, tc.Count)
			b.Push(b.EndTable())
		}
		tags = b.Offsets(m)
	}
	rp := replyPromise(b, reply)
	b.StartTable(8)
	b.AddU32(0, r.Count)
	b.AddU32(1, r.Flags)
	if tags != 0 {
		b.AddOffset(2, tags)
	}
	if debug != 0 {
		b.AddUnion(3, 1, debug)
	}
	b.AddOffset(5, rp)
	b.AddI64(7, InvalidVersion)
	return b.EndTable()
}

// Decode implements Request.
func (GetReadVersion) Decode(msg []byte, t Table) (ReadVersionReply, error) {
	var r ReadVersionReply
	v, err := t.Vector(5)
	if err != nil {
		return r, err
	}
	var now float64
	if v.Len() != 0 {
		now = float64(time.Now().UnixNano()) / 1e9
	}
	for i := range v.Len() {
		pair, err := v.Table(i)
		if err != nil {
			return r, err
		}
		lim, err := pair.mustTable(1)
		if err != nil {
			return r, err
		}
		var tt ThrottledTag
		if tt.Tag, err = pair.bytesOrEmpty(0); err != nil {
			return r, err
		}
		if tt.Rate, err = lim.F64(0); err != nil {
			return r, err
		}
		duration, err := lim.F64(1)
		if err != nil {
			return r, err
		}
		// ClientTagThrottleLimits serializes a duration to avoid clock skew.
		// The consumer uses Unix seconds, not Flow's process-relative clock.
		tt.Expiration = duration
		if duration != math.MaxFloat64 {
			tt.Expiration += now
		}
		r.Throttled = append(r.Throttled, tt)
	}
	if r.Version, err = t.I64(1); err != nil {
		return r, err
	}
	if r.Locked, err = t.Bool(2); err != nil {
		return r, err
	}
	if r.MetadataVersion, err = optionalBytes(t, 3); err != nil {
		return r, err
	}
	if r.MidShardSize, err = t.I64(6); err != nil {
		return r, err
	}
	if r.RKDefaultThrottled, err = t.Bool(7); err != nil {
		return r, err
	}
	r.RKBatchThrottled, err = t.Bool(8)
	return r, err
}

// StorageServer is a storage server as returned by location lookups.
type StorageServer struct {
	ID UID
	// Base is StorageServerInterface.getValue, the base of its adjusted endpoints.
	Base Endpoint
	// Zone is the LocalityData zone id (the machine, for client locality); nil = absent.
	Zone []byte
	// DC is the LocalityData datacenter id; nil = absent.
	DC []byte
}

func storageServer(ssi Table) (StorageServer, error) {
	var s StorageServer
	lt, ok, err := ssi.Table(1)
	if err != nil {
		return s, err
	}
	if s.Zone, s.DC, err = locality(lt, ok); err != nil {
		return s, err
	}
	if s.ID, err = ssi.UID(0); err != nil {
		return s, err
	}
	st, err := ssi.mustTable(2)
	if err != nil {
		return s, err
	}
	s.Base, err = RequestStreamEndpoint(st)
	return s, err
}

// DecodeServerListValue decodes the versioned StorageServerInterface stored
// under \xff/serverList/. Returned locality slices alias value.
func DecodeServerListValue(value []byte) (StorageServer, error) {
	if len(value) < 16 {
		return StorageServer{}, ErrMalformed
	}
	version := binary.LittleEndian.Uint64(value)
	if version&0x0fff_ffff_ffff_ffff < 0x0fdb_00b0_7101_0000 || version&0x1000_0000_0000_0000 == 0 ||
		binary.LittleEndian.Uint32(value[12:]) != 15302073 {
		return StorageServer{}, ErrMalformed
	}
	root, err := Root(value[8:])
	if err != nil {
		return StorageServer{}, err
	}
	ssi, err := root.mustTable(0)
	if err != nil {
		return StorageServer{}, err
	}
	return storageServer(ssi)
}

// systemVector splits one BinaryWriter vector, whose fixed-size entries follow
// a signed 32-bit count. Bounds checking also rejects negative counts.
func systemVector(value []byte, size int) (entries, rest []byte, err error) {
	if len(value) < 4 {
		return nil, nil, ErrMalformed
	}
	count := uint64(binary.LittleEndian.Uint32(value))
	if count > uint64((len(value)-4)/size) {
		return nil, nil, ErrMalformed
	}
	end := 4 + int(count)*size
	return value[4:end], value[end:], nil
}

func containsServerTag(tags, tag []byte) bool {
	for len(tags) >= 3 {
		if bytes.Equal(tags[:3], tag) {
			return true
		}
		tags = tags[3:]
	}
	return false
}

// DecodeKeyServersValue returns the canonical source server IDs from a
// \xff/keyServers/ value, resolving tag-encoded locations against serverTags.
// Relocation destinations are validated but never returned.
func DecodeKeyServersValue(value []byte, serverTags []kv.KeyValue) ([]UID, error) {
	if len(value) == 0 {
		return nil, nil
	}
	if len(value) < 8 {
		return nil, ErrMalformed
	}
	version := binary.LittleEndian.Uint64(value) & 0x0fff_ffff_ffff_ffff
	if version < 0x0fdb_00b0_6201_0001 {
		return nil, ErrMalformed
	}
	tagEncoded := version >= 0x0fdb_00b0_6301_0000 && version < 0x0fdb_00b0_7200_0000
	size := 16
	if tagEncoded {
		size = 3
	}
	src, rest, err := systemVector(value[8:], size)
	if err != nil {
		return nil, err
	}
	dest, rest, err := systemVector(rest, size)
	if err != nil {
		return nil, err
	}
	if version >= 0x0fdb_00b0_7200_0000 {
		if len(rest) != 16 && len(rest) != 32 || len(dest) != 0 && len(rest) != 32 {
			return nil, ErrMalformed
		}
	} else if len(rest) != 0 {
		return nil, ErrMalformed
	}
	out := make([]UID, 0, len(src)/size)
	if !tagEncoded {
		for len(src) != 0 {
			out = append(out, UID{binary.LittleEndian.Uint64(src), binary.LittleEndian.Uint64(src[8:])})
			src = src[16:]
		}
		return out, nil
	}
	const prefix = "\xff/serverTag/"
	destCount := 0
	for _, row := range serverTags {
		if len(row.Key) != len(prefix)+16 || !bytes.HasPrefix(row.Key, []byte(prefix)) ||
			len(row.Value) != 11 || binary.LittleEndian.Uint64(row.Value)&0x0fff_ffff_ffff_ffff < 0x0fdb_00a5_6001_0000 {
			return nil, ErrMalformed
		}
		if containsServerTag(src, row.Value[8:]) {
			id := row.Key[len(prefix):]
			out = append(out, UID{binary.LittleEndian.Uint64(id), binary.LittleEndian.Uint64(id[8:])})
		}
		if containsServerTag(dest, row.Value[8:]) {
			destCount++
		}
	}
	if len(out) != len(src)/3 || destCount != len(dest)/3 {
		return nil, ErrMalformed
	}
	slices.SortFunc(out, func(a, b UID) int {
		if a.First < b.First || a.First == b.First && a.Second < b.Second {
			return -1
		}
		if a != b {
			return 1
		}
		return 0
	})
	return out, nil
}

// locality reads zoneid and dcid from a LocalityData table (_data: a map of
// key to Optional<Value>, i.e. a vector of pair tables).
func locality(t Table, ok bool) (zone, dc []byte, err error) {
	if !ok {
		return nil, nil, nil
	}
	v, err := t.Vector(0)
	if err != nil {
		return nil, nil, err
	}
	for i := range v.Len() {
		pair, err := v.Table(i)
		if err != nil {
			return nil, nil, err
		}
		key, err := pair.Bytes(0)
		if err != nil {
			return nil, nil, err
		}
		val, err := optionalBytes(pair, 1)
		if err != nil {
			return nil, nil, err
		}
		switch string(key) {
		case "zoneid":
			zone = val
		case "dcid":
			dc = val
		}
	}
	return zone, dc, nil
}

// Indices of StorageServerInterface streams relative to getValue.
const (
	SSStreamGetKey              uint32 = 1
	SSStreamGetKeyValues        uint32 = 2
	SSStreamGetShardState       uint32 = 3
	SSStreamWaitMetrics         uint32 = 4
	SSStreamWatchValue          uint32 = 10
	SSStreamGetRangeSplitPoints uint32 = 12
	SSStreamGetMappedKeyValues  uint32 = 14
)

// CPStreamGetKeyServersLocations is the CommitProxyInterface stream index of
// getKeyServersLocations relative to commit.
const CPStreamGetKeyServersLocations uint32 = 2

// ShardLocation is one shard: a key range and the servers holding it.
type ShardLocation struct {
	Range   kv.KeyRange
	Servers []StorageServer
}

// GetKeyServerLocations is GetKeyServerLocationsRequest: begin, end(2), limit,
// reverse, reply, spanContext, tenant, minTenantVersion, arena.
type GetKeyServerLocations struct {
	Begin []byte
	// End is nil when absent.
	End     []byte
	Limit   int32
	Reverse bool
}

// FileID implements Request.
func (GetKeyServerLocations) FileID() uint32 { return 9144680 }

// ReplyFileID identifies an ErrorOr<GetKeyServerLocationsReply> reply.
func (GetKeyServerLocations) ReplyFileID() uint32 { return FIDErrorOr | 10636023 }

// Encode implements Request.
func (r GetKeyServerLocations) Encode(b *Builder, reply UID) Off {
	bo := b.Bytes(r.Begin)
	var eo Off
	if r.End != nil {
		eo = b.Bytes(r.End)
	}
	rp := replyPromise(b, reply)
	tn := noTenant(b)
	b.StartTable(9)
	b.AddOffset(0, bo)
	if r.End != nil {
		b.AddUnion(1, 1, eo)
	}
	b.AddI32(3, r.Limit)
	b.AddBool(4, r.Reverse)
	b.AddOffset(5, rp)
	b.AddOffset(7, tn)
	b.AddI64(8, LatestVersion)
	return b.EndTable()
}

// Decode implements Request: GetKeyServerLocationsReply.results is pairs of
// KeyRangeRef and vector<StorageServerInterface>; the interface serializes
// uniqueID, locality, getValue, tssPairID(2), acceptingRequests.
func (GetKeyServerLocations) Decode(msg []byte, t Table) ([]ShardLocation, error) {
	results, err := t.Vector(0)
	if err != nil {
		return nil, err
	}
	out := make([]ShardLocation, 0, results.capHint())
	for i := range results.Len() {
		pair, err := results.Table(i)
		if err != nil {
			return nil, err
		}
		kr, err := pair.mustTable(0)
		if err != nil {
			return nil, err
		}
		sv, err := pair.Vector(1)
		if err != nil {
			return nil, err
		}
		var loc ShardLocation
		loc.Servers = make([]StorageServer, 0, sv.capHint())
		for j := range sv.Len() {
			ssi, err := sv.Table(j)
			if err != nil {
				return nil, err
			}
			s, err := storageServer(ssi)
			if err != nil {
				return nil, err
			}
			loc.Servers = append(loc.Servers, s)
		}
		if loc.Range, err = decodeKeyRange(kr); err != nil {
			return nil, err
		}
		out = append(out, loc)
	}
	return out, nil
}

// Lb is a LoadBalancedReply: the server's load penalty and its value or own error code.
type Lb[T any] struct {
	// Penalty is the cost multiplier the server asks clients to apply to requests it is sent.
	Penalty float64
	// Err is the server's error (0 = none, Value valid).
	Err   fdberr.Error
	Value T
}

// lbHead decodes the penalty and error of a LoadBalancedReply.
func lbHead(t Table) (float64, fdberr.Error, error) {
	p, err := t.F64(0)
	if err != nil {
		return 0, 0, err
	}
	e, err := lbError(t, 1)
	return p, e, err
}

// GetValue is GetValueRequest: key, version, tags(2), reply, spanContext,
// tenantInfo, options(2), ssLatestCommitVersions.
type GetValue struct {
	Key []byte
	Ctx ReadCtx
}

// FileID implements Request.
func (*GetValue) FileID() uint32 { return 8454530 }

// ReplyFileID identifies an ErrorOr<GetValueReply> reply.
func (*GetValue) ReplyFileID() uint32 { return FIDErrorOr | 1378929 }

// Encode implements Request.
func (r *GetValue) Encode(b *Builder, reply UID) Off {
	ko := b.Bytes(r.Key)
	c := writeCtx(b, &r.Ctx)
	rp := replyPromise(b, reply)
	b.StartTable(10)
	b.AddOffset(0, ko)
	b.AddI64(1, r.Ctx.Version)
	if c.tags != 0 {
		b.AddUnion(2, 1, c.tags)
	}
	b.AddOffset(4, rp)
	b.AddOffset(6, c.tenant)
	if c.options != 0 {
		b.AddUnion(7, 1, c.options)
	}
	return b.EndTable()
}

// Decode implements Request: GetValueReply is penalty, error(2), value(2), cached.
// Value is nil when the key is absent.
func (*GetValue) Decode(msg []byte, t Table) (Lb[[]byte], error) {
	var r Lb[[]byte]
	var err error
	if r.Penalty, r.Err, err = lbHead(t); err != nil || r.Err != 0 {
		return r, err
	}
	r.Value, err = optionalBytes(t, 3)
	return r, err
}

// GetKey is GetKeyRequest: sel, version, tags(2), reply, spanContext,
// tenantInfo, options(2), ssLatestCommitVersions, arena.
type GetKey struct {
	Sel kv.KeySelector
	Ctx ReadCtx
}

// FileID implements Request.
func (*GetKey) FileID() uint32 { return 10457870 }

// ReplyFileID identifies an ErrorOr<GetKeyReply> reply.
func (*GetKey) ReplyFileID() uint32 { return FIDErrorOr | 11226513 }

// Encode implements Request.
func (r *GetKey) Encode(b *Builder, reply UID) Off {
	so := keySelector(b, &r.Sel)
	c := writeCtx(b, &r.Ctx)
	rp := replyPromise(b, reply)
	b.StartTable(10)
	b.AddOffset(0, so)
	b.AddI64(1, r.Ctx.Version)
	if c.tags != 0 {
		b.AddUnion(2, 1, c.tags)
	}
	b.AddOffset(4, rp)
	b.AddOffset(6, c.tenant)
	if c.options != 0 {
		b.AddUnion(7, 1, c.options)
	}
	return b.EndTable()
}

// Decode implements Request: GetKeyReply is penalty, error(2), sel, cached.
func (*GetKey) Decode(msg []byte, t Table) (Lb[kv.KeySelector], error) {
	var r Lb[kv.KeySelector]
	var err error
	if r.Penalty, r.Err, err = lbHead(t); err != nil || r.Err != 0 {
		return r, err
	}
	st, err := t.mustTable(3)
	if err != nil {
		return r, err
	}
	r.Value, err = selOf(st)
	return r, err
}

// RangePage is a storage range-read reply page.
type RangePage struct {
	Data []kv.KeyValue
	More bool
}

// GetKeyValues is GetKeyValuesRequest: begin, end, version, limit, limitBytes,
// tags(2), reply, spanContext, tenantInfo, options(2), ssLatestCommitVersions, arena.
type GetKeyValues struct {
	Begin, End kv.KeySelector
	Limit      int32
	LimitBytes int32
	Ctx        ReadCtx
}

// FileID implements Request.
func (*GetKeyValues) FileID() uint32 { return 6795746 }

// ReplyFileID identifies an ErrorOr<GetKeyValuesReply> reply.
func (*GetKeyValues) ReplyFileID() uint32 { return FIDErrorOr | 1783066 }

// Encode implements Request.
func (r *GetKeyValues) Encode(b *Builder, reply UID) Off {
	bo := keySelector(b, &r.Begin)
	eo := keySelector(b, &r.End)
	c := writeCtx(b, &r.Ctx)
	rp := replyPromise(b, reply)
	b.StartTable(13)
	b.AddOffset(0, bo)
	b.AddOffset(1, eo)
	b.AddI64(2, r.Ctx.Version)
	b.AddI32(3, r.Limit)
	b.AddI32(4, r.LimitBytes)
	if c.tags != 0 {
		b.AddUnion(5, 1, c.tags)
	}
	b.AddOffset(7, rp)
	b.AddOffset(9, c.tenant)
	if c.options != 0 {
		b.AddUnion(10, 1, c.options)
	}
	return b.EndTable()
}

// Decode implements Request: GetKeyValuesReply is penalty, error(2), data,
// version, more, cached, arena. data is serialized as one string blob of
// [u32 count] then [u32 klen][key][u32 vlen][value] per row.
func (*GetKeyValues) Decode(msg []byte, t Table) (Lb[RangePage], error) {
	var r Lb[RangePage]
	var err error
	if r.Penalty, r.Err, err = lbHead(t); err != nil || r.Err != 0 {
		return r, err
	}
	blob, err := t.Bytes(3)
	if err != nil {
		return r, err
	}
	if r.Value.Data, err = stringKVs(blob); err != nil {
		return r, err
	}
	r.Value.More, err = t.Bool(5)
	return r, err
}

// stringKVs decodes a string-serialized KeyValueRef vector blob, aliasing it.
func stringKVs(blob []byte) ([]kv.KeyValue, error) {
	if len(blob) < 4 {
		return nil, nil
	}
	count := int(le32u(blob))
	rest := blob[4:]
	out := make([]kv.KeyValue, 0, min(count, len(rest)/8))
	take := func() ([]byte, error) {
		if len(rest) < 4 {
			return nil, ErrMalformed
		}
		n := int(le32u(rest))
		if n > len(rest)-4 {
			return nil, ErrMalformed
		}
		s := rest[4 : 4+n : 4+n]
		rest = rest[4+n:]
		return s, nil
	}
	for range count {
		k, err := take()
		if err != nil {
			return nil, err
		}
		v, err := take()
		if err != nil {
			return nil, err
		}
		out = append(out, kv.KeyValue{Key: k, Value: v})
	}
	if len(rest) != 0 {
		return nil, ErrMalformed
	}
	return out, nil
}

func le32u(b []byte) uint32 {
	return uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
}

// MappedRow is one row of a mapped range read: the scanned pair and the secondary range it mapped to.
type MappedRow struct {
	Key, Value []byte
	// IsValue is set when the mapper produced a point read (GetValueReqAndResultRef) rather than a range.
	IsValue    bool
	Begin, End kv.KeySelector
	Data       []kv.KeyValue
	More       bool
}

// GetMappedKeyValues is GetMappedKeyValuesRequest: begin, end, mapper,
// version, limit, limitBytes, tags(2), reply, spanContext, tenantInfo,
// options(2), ssLatestCommitVersions, arena.
type GetMappedKeyValues struct {
	Begin, End kv.KeySelector
	Mapper     []byte
	Limit      int32
	LimitBytes int32
	Ctx        ReadCtx
}

// MappedPage is a mapped range-read reply page.
type MappedPage struct {
	Data []MappedRow
	More bool
}

// FileID implements Request.
func (*GetMappedKeyValues) FileID() uint32 { return 6795747 }

// ReplyFileID identifies an ErrorOr<GetMappedKeyValuesReply> reply.
func (*GetMappedKeyValues) ReplyFileID() uint32 { return FIDErrorOr | 1783067 }

// Encode implements Request.
func (r *GetMappedKeyValues) Encode(b *Builder, reply UID) Off {
	bo := keySelector(b, &r.Begin)
	eo := keySelector(b, &r.End)
	mo := b.Bytes(r.Mapper)
	c := writeCtx(b, &r.Ctx)
	rp := replyPromise(b, reply)
	b.StartTable(14)
	b.AddOffset(0, bo)
	b.AddOffset(1, eo)
	b.AddOffset(2, mo)
	b.AddI64(3, r.Ctx.Version)
	b.AddI32(4, r.Limit)
	b.AddI32(5, r.LimitBytes)
	if c.tags != 0 {
		b.AddUnion(6, 1, c.tags)
	}
	b.AddOffset(8, rp)
	b.AddOffset(10, c.tenant)
	if c.options != 0 {
		b.AddUnion(11, 1, c.options)
	}
	return b.EndTable()
}

// Decode implements Request: GetMappedKeyValuesReply is penalty, error(2),
// data, version, more, cached, arena. MappedKeyValueRef is (KeyValueRef,
// reqAndResult(2)) where the variant is GetValueReqAndResultRef{key,
// result(2)} or GetRangeReqAndResultRef{begin, end, result: RangeResultRef},
// and RangeResultRef is (data, more, readThrough(2), readToBegin, readThroughEnd).
func (*GetMappedKeyValues) Decode(msg []byte, t Table) (Lb[MappedPage], error) {
	var r Lb[MappedPage]
	var err error
	if r.Penalty, r.Err, err = lbHead(t); err != nil || r.Err != 0 {
		return r, err
	}
	r.Value, err = mappedPage(t)
	return r, err
}

func kvOf(t Table) (kv.KeyValue, error) {
	k, err := t.bytesOrEmpty(0)
	if err != nil {
		return kv.KeyValue{}, err
	}
	v, err := t.bytesOrEmpty(1)
	return kv.KeyValue{Key: k, Value: v}, err
}

func mappedRow(row Table) (MappedRow, error) {
	var out MappedRow
	kt, err := row.mustTable(0)
	if err != nil {
		return out, err
	}
	pair, err := kvOf(kt)
	if err != nil {
		return out, err
	}
	out.Key, out.Value = pair.Key, pair.Value
	out.Begin = kv.FirstGreaterOrEqual(nil)
	out.End = out.Begin
	tag, f, err := row.Union(1)
	if err != nil {
		return out, err
	}
	switch tag {
	case 0:
	case 1:
		// GetValueReqAndResultRef: the mapped key and its value, if present.
		gv, err := f.Table()
		if err != nil {
			return out, err
		}
		out.IsValue = true
		key, err := gv.bytesOrEmpty(0)
		if err != nil {
			return out, err
		}
		out.Begin = kv.FirstGreaterOrEqual(key)
		out.End = kv.FirstGreaterOrEqual(kv.KeyAfter(key))
		v, err := optionalBytes(gv, 1)
		if err != nil {
			return out, err
		}
		if v != nil {
			out.Data = append(out.Data, kv.KeyValue{Key: key, Value: v})
		}
	case 2:
		gr, err := f.Table()
		if err != nil {
			return out, err
		}
		bt, err := gr.mustTable(0)
		if err != nil {
			return out, err
		}
		if out.Begin, err = selOf(bt); err != nil {
			return out, err
		}
		et, err := gr.mustTable(1)
		if err != nil {
			return out, err
		}
		if out.End, err = selOf(et); err != nil {
			return out, err
		}
		rr, ok, err := gr.Table(2)
		if err != nil {
			return out, err
		}
		if ok {
			v, err := rr.Vector(0)
			if err != nil {
				return out, err
			}
			out.Data = make([]kv.KeyValue, 0, v.capHint())
			for i := range v.Len() {
				kt, err := v.Table(i)
				if err != nil {
					return out, err
				}
				p, err := kvOf(kt)
				if err != nil {
					return out, err
				}
				out.Data = append(out.Data, p)
			}
			if out.More, err = rr.Bool(1); err != nil {
				return out, err
			}
		}
	default:
		return out, ErrMalformed
	}
	return out, nil
}

func mappedPage(t Table) (MappedPage, error) {
	var p MappedPage
	v, err := t.Vector(3)
	if err != nil {
		return p, err
	}
	p.Data = make([]MappedRow, 0, v.capHint())
	for i := range v.Len() {
		row, err := v.Table(i)
		if err != nil {
			return p, err
		}
		m, err := mappedRow(row)
		if err != nil {
			return p, err
		}
		p.Data = append(p.Data, m)
	}
	p.More, err = t.Bool(5)
	return p, err
}

// WatchValue is WatchValueRequest: key, value(2), version, tags(2),
// debugID(2), reply, spanContext, tenantInfo.
type WatchValue struct {
	Key []byte
	// Value is nil when the key is absent.
	Value   []byte
	Version int64
	Tags    [][]byte
	// DebugID enables server-side request tracing when nonzero.
	DebugID UID
}

// FileID implements Request.
func (*WatchValue) FileID() uint32 { return 14747733 }

// ReplyFileID identifies an ErrorOr<WatchValueReply> reply.
func (*WatchValue) ReplyFileID() uint32 { return FIDErrorOr | 3 }

// Encode implements Request.
func (r *WatchValue) Encode(b *Builder, reply UID) Off {
	debug := optionalUID(b, r.DebugID)
	ko := b.Bytes(r.Key)
	var vo, tags Off
	if r.Value != nil {
		vo = b.Bytes(r.Value)
	}
	if len(r.Tags) > 0 {
		tags = tagSet(b, r.Tags)
	}
	rp := replyPromise(b, reply)
	tn := noTenant(b)
	b.StartTable(11)
	b.AddOffset(0, ko)
	if r.Value != nil {
		b.AddUnion(1, 1, vo)
	}
	b.AddI64(3, r.Version)
	if tags != 0 {
		b.AddUnion(4, 1, tags)
	}
	if debug != 0 {
		b.AddUnion(6, 1, debug)
	}
	b.AddOffset(8, rp)
	b.AddOffset(10, tn)
	return b.EndTable()
}

// Decode implements Request: WatchValueReply is version, cached.
func (*WatchValue) Decode(msg []byte, t Table) (int64, error) { return t.I64(0) }

// Mutation is a mutation as committed (MutationRef: type, param1, param2).
type Mutation struct {
	Kind           uint8
	Param1, Param2 []byte
}

// CommitTransactionRequest flag bits.
const (
	CommitFlagIsLockAware        uint32 = 1
	CommitFlagFirstInBatch       uint32 = 2
	CommitFlagBypassStorageQuota uint32 = 4
)

// CommitCostEstimation is ClientTrCommitCostEstimation. ClearCosts contains
// sampled, scaled costs indexed by the original transaction mutation position.
type CommitCostEstimation struct {
	OpsCount              int32
	WriteCosts            uint64
	ClearCosts            []ClearCost
	ExpensiveCostEstCount uint32
}

type ClearCost struct {
	MutationIndex int32
	Cost          uint64
}

// Commit is CommitTransactionRequest: transaction, reply, flags, debugID(2),
// commitCostEstimation(2), tagSet(2), spanContext, tenantInfo, idempotencyId,
// arena. CommitTransactionRef is read_conflict_ranges, write_conflict_ranges,
// mutations, read_snapshot, report_conflicting_keys, lock_aware,
// spanContext(2), tenantIds(2).
type Commit struct {
	ReadConflicts         []kv.KeyRange
	WriteConflicts        []kv.KeyRange
	Mutations             []Mutation
	ReadSnapshot          int64
	ReportConflictingKeys bool
	LockAware             bool
	Flags                 uint32
	Tags                  [][]byte
	CostEstimation        *CommitCostEstimation
	// IdempotencyID is the IdempotencyIdRef (16..255 bytes) for automatic idempotency; nil = none.
	IdempotencyID []byte
	// DebugID enables server-side request tracing when nonzero.
	DebugID UID
}

// ExpireIdempotencyID acknowledges a committed automatic idempotency ID to its proxy.
type ExpireIdempotencyID struct {
	Version            int64
	BatchIndexHighByte uint8
}

// FileID implements Message.
func (ExpireIdempotencyID) FileID() uint32 { return 1900933 }

// Encode implements Message; expiration is a one-way notification.
func (r ExpireIdempotencyID) Encode(b *Builder, _ UID) Off {
	tenant := noTenant(b)
	b.StartTable(3)
	b.AddI64(0, r.Version)
	b.AddU8(1, r.BatchIndexHighByte)
	b.AddOffset(2, tenant)
	return b.EndTable()
}

// CommitReply is CommitID: version, txnBatchId, metadataVersion(2), conflictingKRIndices(2).
type CommitReply struct {
	Version    int64
	BatchIndex uint16
	// Conflicting holds the indices of conflicting read ranges; nil = not reported.
	Conflicting []int32
}

// FileID implements Request.
func (*Commit) FileID() uint32 { return 93948 }

// ReplyFileID identifies an ErrorOr<CommitID> reply.
func (*Commit) ReplyFileID() uint32 { return FIDErrorOr | 14254927 }

// Encode implements Request.
func (r *Commit) Encode(b *Builder, reply UID) Off {
	debug := optionalUID(b, r.DebugID)
	m := b.Mark()
	for i := range r.ReadConflicts {
		b.Push(keyRange(b, r.ReadConflicts[i].Begin, r.ReadConflicts[i].End))
	}
	rcr := b.Offsets(m)
	for i := range r.WriteConflicts {
		b.Push(keyRange(b, r.WriteConflicts[i].Begin, r.WriteConflicts[i].End))
	}
	wcr := b.Offsets(m)
	for i := range r.Mutations {
		mu := &r.Mutations[i]
		p1 := b.Bytes(mu.Param1)
		p2 := b.Bytes(mu.Param2)
		b.StartTable(3)
		b.AddU8(0, mu.Kind)
		b.AddOffset(1, p1)
		b.AddOffset(2, p2)
		b.Push(b.EndTable())
	}
	muts := b.Offsets(m)
	b.StartTable(10)
	b.AddOffset(0, rcr)
	b.AddOffset(1, wcr)
	b.AddOffset(2, muts)
	b.AddI64(3, r.ReadSnapshot)
	b.AddBool(4, r.ReportConflictingKeys)
	b.AddBool(5, r.LockAware)
	tx := b.EndTable()

	var tags, idmp, cost Off
	if c := r.CostEstimation; c != nil {
		for _, clear := range c.ClearCosts {
			b.StartTable(2)
			b.AddI32(0, clear.MutationIndex)
			b.AddI64(1, int64(clear.Cost))
			b.Push(b.EndTable())
		}
		clears := b.Offsets(m)
		b.StartTable(4)
		b.AddI32(0, c.OpsCount)
		b.AddI64(1, int64(c.WriteCosts))
		b.AddOffset(2, clears)
		b.AddU32(3, c.ExpensiveCostEstCount)
		cost = b.EndTable()
	}
	if len(r.Tags) > 0 {
		tags = tagSet(b, r.Tags)
	}
	if r.IdempotencyID != nil {
		// IdempotencyIdRef uses dynamic_size_traits: its raw id bytes.
		idmp = b.Bytes(r.IdempotencyID)
	}
	rp := replyPromise(b, reply)
	tn := noTenant(b)
	b.StartTable(12)
	b.AddOffset(0, tx)
	b.AddOffset(1, rp)
	b.AddU32(2, r.Flags)
	if debug != 0 {
		b.AddUnion(3, 1, debug)
	}
	if cost != 0 {
		b.AddUnion(5, 1, cost)
	}
	if tags != 0 {
		b.AddUnion(7, 1, tags)
	}
	b.AddOffset(10, tn)
	if r.IdempotencyID != nil {
		b.AddOffset(11, idmp)
	}
	return b.EndTable()
}

// Decode implements Request.
func (*Commit) Decode(msg []byte, t Table) (CommitReply, error) {
	var r CommitReply
	tag, f, err := t.Union(4)
	if err != nil {
		return r, err
	}
	switch tag {
	case 0:
	case 1:
		v, err := f.Vector()
		if err != nil {
			return r, err
		}
		r.Conflicting = make([]int32, 0, v.capHint())
		for i := range v.Len() {
			x, err := v.I32(i)
			if err != nil {
				return r, err
			}
			r.Conflicting = append(r.Conflicting, x)
		}
	default:
		return r, ErrMalformed
	}
	if r.Version, err = t.I64(0); err != nil {
		return r, err
	}
	r.BatchIndex, err = t.U16(1)
	return r, err
}

// StorageMetrics is StorageMetrics: bytes, bytesWrittenPerKSecond,
// iosPerKSecond, bytesReadPerKSecond, opsReadPerKSecond.
type StorageMetrics struct {
	Bytes                  int64
	BytesWrittenPerKSecond int64
	IOsPerKSecond          int64
	BytesReadPerKSecond    int64
	OpsReadPerKSecond      int64
}

func storageMetrics(b *Builder, m *StorageMetrics) Off {
	b.StartTable(5)
	b.AddI64(0, m.Bytes)
	b.AddI64(1, m.BytesWrittenPerKSecond)
	b.AddI64(2, m.IOsPerKSecond)
	b.AddI64(3, m.BytesReadPerKSecond)
	b.AddI64(4, m.OpsReadPerKSecond)
	return b.EndTable()
}

// WaitMetrics is WaitMetricsRequest: keys, min, max, reply, tenantInfo,
// minVersion, arena. A reversed min/max asks for an immediate report.
type WaitMetrics struct {
	Range    kv.KeyRange
	Min, Max StorageMetrics
}

// FileID implements Request.
func (*WaitMetrics) FileID() uint32 { return 1795961 }

// ReplyFileID identifies an ErrorOr<StorageMetrics> reply.
func (*WaitMetrics) ReplyFileID() uint32 { return FIDErrorOr | 13622226 }

// Encode implements Request.
func (r *WaitMetrics) Encode(b *Builder, reply UID) Off {
	kr := keyRange(b, r.Range.Begin, r.Range.End)
	mn := storageMetrics(b, &r.Min)
	mx := storageMetrics(b, &r.Max)
	rp := replyPromise(b, reply)
	tn := noTenant(b)
	b.StartTable(6)
	b.AddOffset(0, kr)
	b.AddOffset(1, mn)
	b.AddOffset(2, mx)
	b.AddOffset(3, rp)
	b.AddOffset(4, tn)
	return b.EndTable()
}

// Decode implements Request.
func (*WaitMetrics) Decode(msg []byte, t Table) (StorageMetrics, error) {
	var m StorageMetrics
	for i, p := range [...]*int64{&m.Bytes, &m.BytesWrittenPerKSecond, &m.IOsPerKSecond, &m.BytesReadPerKSecond, &m.OpsReadPerKSecond} {
		v, err := t.I64(i)
		if err != nil {
			return StorageMetrics{}, err
		}
		*p = v
	}
	return m, nil
}

// SplitRange is SplitRangeRequest: keys, chunkSize, reply, tenantInfo, arena.
type SplitRange struct {
	Range     kv.KeyRange
	ChunkSize int64
}

// FileID implements Request.
func (*SplitRange) FileID() uint32 { return 10725174 }

// ReplyFileID identifies an ErrorOr<SplitRangeReply> reply.
func (*SplitRange) ReplyFileID() uint32 { return FIDErrorOr | 11813134 }

// Encode implements Request.
func (r *SplitRange) Encode(b *Builder, reply UID) Off {
	kr := keyRange(b, r.Range.Begin, r.Range.End)
	rp := replyPromise(b, reply)
	tn := noTenant(b)
	b.StartTable(4)
	b.AddOffset(0, kr)
	b.AddI64(1, r.ChunkSize)
	b.AddOffset(2, rp)
	b.AddOffset(3, tn)
	return b.EndTable()
}

// Decode implements Request: SplitRangeReply is splitPoints.
func (*SplitRange) Decode(msg []byte, t Table) ([][]byte, error) {
	v, err := t.Vector(0)
	if err != nil {
		return nil, err
	}
	out := make([][]byte, 0, v.capHint())
	for i := range v.Len() {
		s, err := v.Bytes(i)
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, nil
}

// Ping is PingRequest: reply.
type Ping struct{}

// FileID implements Request.
func (Ping) FileID() uint32 { return 4707015 }

// ReplyFileID identifies an ErrorOr<Void> reply.
func (Ping) ReplyFileID() uint32 { return FIDErrorOr | FIDVoid }

// Encode implements Request.
func (Ping) Encode(b *Builder, reply UID) Off {
	rp := replyPromise(b, reply)
	b.StartTable(1)
	b.AddOffset(0, rp)
	return b.EndTable()
}

// Decode implements Request.
func (Ping) Decode(msg []byte, t Table) (struct{}, error) { return struct{}{}, nil }

// PingReplyToken extracts the reply token of a peer's PingRequest.
func PingReplyToken(msg []byte) (UID, error) {
	root, err := Root(msg)
	if err != nil {
		return UID{}, err
	}
	req, err := root.mustTable(0)
	if err != nil {
		return UID{}, err
	}
	rp, err := req.mustTable(0)
	if err != nil {
		return UID{}, err
	}
	return rp.UID(0)
}

// EncodeVoidReply writes ErrorOr<EnsureTable<Void>> carrying Void, the reply to a ping.
func EncodeVoidReply(b *Builder) []byte {
	v := b.EmptyTable()
	b.StartTable(2)
	b.AddUnion(0, 2, v)
	root := b.EndTable()
	return b.Finish(root, FIDErrorOr|FIDVoid)
}

// NoticeToken extracts the token of an EndpointNotFound/UnauthorizedEndpoint notice (a serialized UID).
func NoticeToken(msg []byte) (UID, error) {
	root, err := Root(msg)
	if err != nil {
		return UID{}, err
	}
	return root.UID(0)
}
