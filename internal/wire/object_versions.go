package wire

import (
	"github.com/can1357/fdbgo/internal/fdberr"
	"github.com/can1357/fdbgo/internal/kv"
)

// Evidence (upstream release archives under target/versions/<release>/src):
//
// Message / structure            6.2.30       6.3.25       7.0.0        7.1.67       7.2.9
// Coordination OpenDatabase      7 fields     7 fields     7 fields     +hosts/internal unchanged
// ClientDBInfo                   combined     combined     split        +tenantMode  +encryption/cluster
// Proxy/StorageServerInterface   full streams small streams small streams +readiness  unchanged
// GetValue/GetKey                no tags      +tags/debug  +span        +tenant      options replace debug
// GetKeyValues                   fetch/debug  +tags        +span        +tenant      options replace fetch/debug
// GetMappedKeyValues             absent       absent       incompatible present      options replace fetch/debug
// GetReadVersion request/reply   no tags/busy  +tags/busy   +span/mid    +maxVersion  unchanged prefix
// CommitTransactionRef           4 fields     +conflicts   unchanged    +lock/span   unchanged
// CommitTransactionRequest       debug        debug        +cost/tags   +tenant      +idempotency
// GetKeyServerLocations          6 fields     unchanged    +span        optional name TenantInfo
// WatchValue                     no tags      +tags        +span        +tenant      unchanged
// StorageMetrics                 3 fields     +read bytes  unchanged    unchanged    unchanged
// WaitMetrics                    4 fields     unchanged    unchanged    unchanged    unchanged
// SplitRange                     absent       absent       present      +tenant      unchanged
// Idempotency expiration         absent       absent       absent       absent       absent (starts7.3)
// Ping is a direct ReplyPromise<Void> before7.0; notices carry Endpoint rather
// than UID before7.0 (fdbrpc/FlowTransport.actor.cpp Ping/EndpointNotFoundReceiver).
// ErrorOr's composed FID prefix is 1 before 7.0, then 2; ReplyPromise uses
// prefix 2 before 7.0, then 1 (flow/flow.h and fdbrpc/fdbrpc.h).
// Endpoint/LoadBalancedReply and all other used FIDs are unchanged. In 6.2,
// Void's serializer writes no table: its success union points to the buffer end.
//
// Sources: fdbclient/{CoordinationInterface,MasterProxyInterface,CommitProxyInterface,
// GrvProxyInterface,StorageServerInterface,CommitTransaction}.h (7.2 uses include/fdbclient),
// fdbrpc/FlowTransport.h, fdbrpc/include/fdbrpc/TenantInfo.h (7.2), and
// fdbclient/SystemData.cpp. Object serializers skip Arena and expand Optional into
// two slots. 6.2 SSI includes getVersion before getValue; 6.3 SmallEndpoints removes it.
const (
	object63 ProtocolVersion = 0x0fdb00b063010000
	object70 ProtocolVersion = 0x0fdb00b070010000
	object71 ProtocolVersion = 0x0fdb00b071010000
	object72 ProtocolVersion = 0x0fdb00b072000000
)

// MessageFileID handles the pre-PingRequest transport object type.
func MessageFileID(req Message, protocol ProtocolVersion) uint32 {
	if p, ok := req.(interface{ FileIDProtocol(ProtocolVersion) uint32 }); ok {
		return p.FileIDProtocol(protocol)
	}
	return req.FileID()
}

func (Ping) FileIDProtocol(protocol ProtocolVersion) uint32 {
	if objectProtocol(protocol) < object70 {
		// The old request is ReplyPromise<Void>, not ErrorOr<Void>.
		return 2<<24 | FIDVoid
	}
	return (Ping{}).FileID()
}

// replyFileIDProtocol selects ErrorOr's composed identifier for an unwrapped
// payload type. Requests with nested composed payloads implement
// ReplyFileIDProtocol, because pre-7.0 identifiers OR their high bytes instead
// of composing nibbles (for example, ErrorOr<Optional<T>> uses 0x11, not 0x24).
func replyFileIDProtocol(fileID uint32, protocol ProtocolVersion) uint32 {
	if objectProtocol(protocol) < object70 {
		return fileID&0x00ff_ffff | 1<<24
	}
	return fileID
}

func objectProtocol(p ProtocolVersion) ProtocolVersion {
	if p == 0 {
		return Protocol74
	}
	return p.Normalized()
}

// ValidateRequest rejects fields which the destination cannot represent.
// Wrapper requests inherit ValidateProtocol from their embedded wire request.
func ValidateRequest(req Message, protocol ProtocolVersion) error {
	if v, ok := req.(interface{ ValidateProtocol(ProtocolVersion) error }); ok {
		return v.ValidateProtocol(protocol)
	}
	return nil
}

func validateRead(c *ReadCtx, protocol ProtocolVersion) error {
	p := objectProtocol(protocol)
	if p < object63 && len(c.Tags) != 0 {
		return fdberr.UnsupportedOperation
	}
	if c.HasOptions {
		if p < object72 && (!c.Options.CacheResult || c.Options.ReadType != 3 && c.Options.ReadType != 1) {
			return fdberr.UnsupportedOperation
		}
	}
	return nil
}

func (r OpenDatabase) ValidateProtocol(protocol ProtocolVersion) error {
	if objectProtocol(protocol) < object71 && len(r.Hostnames) != 0 {
		return fdberr.UnsupportedOperation
	}
	return nil
}

func (r *GetValue) ValidateProtocol(p ProtocolVersion) error {
	if objectProtocol(p) < object72 && r.Ctx.HasOptions && r.Ctx.Options.ReadType == 1 {
		return fdberr.UnsupportedOperation
	}
	return validateRead(&r.Ctx, p)
}

func (r *GetKey) ValidateProtocol(p ProtocolVersion) error {
	if r.Ctx.HasOptions && (objectProtocol(p) < object72 && r.Ctx.Options.ReadType == 1 || objectProtocol(p) < object63 && r.Ctx.Options.DebugID.IsValid()) {
		return fdberr.UnsupportedOperation
	}
	return validateRead(&r.Ctx, p)
}
func (r *GetKeyValues) ValidateProtocol(p ProtocolVersion) error { return validateRead(&r.Ctx, p) }

func (r *GetMappedKeyValues) ValidateProtocol(protocol ProtocolVersion) error {
	if objectProtocol(protocol) < object71 {
		return fdberr.UnsupportedOperation
	}
	return validateRead(&r.Ctx, protocol)
}

func (r *WatchValue) ValidateProtocol(protocol ProtocolVersion) error {
	if objectProtocol(protocol) < object63 && len(r.Tags) != 0 {
		return fdberr.UnsupportedOperation
	}
	return nil
}

func (r GetReadVersion) ValidateProtocol(protocol ProtocolVersion) error {
	if objectProtocol(protocol) < object63 && len(r.Tags) != 0 {
		return fdberr.UnsupportedOperation
	}
	return nil
}

func (r *Commit) ValidateProtocol(protocol ProtocolVersion) error {
	p := objectProtocol(protocol)
	if p < object63 && r.ReportConflictingKeys || p < object70 && (len(r.Tags) != 0 || r.CostEstimation != nil) || p < object72 && r.IdempotencyID != nil || p < Protocol73 && r.Flags&CommitFlagBypassStorageQuota != 0 {
		return fdberr.UnsupportedOperation
	}
	return nil
}

func (ExpireIdempotencyID) ValidateProtocol(protocol ProtocolVersion) error {
	if objectProtocol(protocol) < Protocol73 {
		return fdberr.UnsupportedOperation
	}
	return nil
}

func (*SplitRange) ValidateProtocol(protocol ProtocolVersion) error {
	if objectProtocol(protocol) < object70 {
		return fdberr.UnsupportedOperation
	}
	return nil
}

func encodeReadVersion62(b *Builder, r GetReadVersion, reply UID) Off {
	debug := optionalUID(b, r.DebugID)
	rp := replyPromise(b, reply)
	b.StartTable(5)
	b.AddU32(0, r.Count)
	b.AddU32(1, r.Flags)
	if debug != 0 {
		b.AddUnion(2, 1, debug)
	}
	b.AddOffset(4, rp)
	return b.EndTable()
}

func decodeReadVersion62(t Table) (ReadVersionReply, error) {
	var r ReadVersionReply
	var err error
	if r.Version, err = t.I64(0); err != nil {
		return r, err
	}
	if r.Locked, err = t.Bool(1); err != nil {
		return r, err
	}
	r.MetadataVersion, err = optionalBytes(t, 2)
	return r, err
}

func decodeCombinedProxies(t Table) (ClientDBInfo, error) {
	var r ClientDBInfo
	v, err := t.Vector(0)
	if err != nil {
		return r, err
	}
	r.Commit = make([]ProxyEndpoint, 0, v.capHint())
	r.GRV = make([]ProxyEndpoint, 0, v.capHint())
	for i := range v.Len() {
		pt, err := v.Table(i)
		if err != nil {
			return r, err
		}
		baseSlot, provisionalSlot := 3, 2
		if objectProtocol(t.Protocol) < object63 {
			baseSlot, provisionalSlot = 2, 1
		}
		st, err := pt.mustTable(baseSlot)
		if err != nil {
			return r, err
		}
		ep, err := RequestStreamEndpoint(st)
		if err != nil {
			return r, err
		}
		prov, err := pt.Bool(provisionalSlot)
		if err != nil {
			return r, err
		}
		grv := ep.Adjusted(1)
		if objectProtocol(t.Protocol) < object63 {
			gs, err := pt.mustTable(3)
			if err != nil {
				return r, err
			}
			grv, err = RequestStreamEndpoint(gs)
			if err != nil {
				return r, err
			}
			ls, err := pt.mustTable(4)
			if err != nil {
				return r, err
			}
			location, err := RequestStreamEndpoint(ls)
			if err != nil {
				return r, err
			}
			ep.Streams = &EndpointStreams{Present: 1 | 1<<2}
			ep.Streams.Tokens[0], ep.Streams.Tokens[2] = ep.Token, location.Token
		}
		r.Commit = append(r.Commit, ProxyEndpoint{ep, prov})
		r.GRV = append(r.GRV, ProxyEndpoint{grv, prov})
	}
	if r.ID, err = t.UID(1); err != nil {
		return r, err
	}
	r.Forward, err = optionalBytes(t, 4)
	return r, err
}

func decodeStorageServer62(t Table) (StorageServer, error) {
	var r StorageServer
	var err error
	if r.ID, err = t.UID(0); err != nil {
		return r, err
	}
	lt, ok, err := t.Table(1)
	if err != nil {
		return r, err
	}
	if r.Zone, r.DC, err = locality(lt, ok); err != nil {
		return r, err
	}
	streams := &EndpointStreams{}
	for _, pair := range [...]struct{ slot, index int }{{3, 0}, {4, 1}, {5, 2}, {6, 3}, {7, 4}, {13, 10}} {
		st, err := t.mustTable(pair.slot)
		if err != nil {
			return r, err
		}
		ep, err := RequestStreamEndpoint(st)
		if err != nil {
			return r, err
		}
		if pair.index == 0 {
			r.Base = ep
		}
		streams.Tokens[pair.index] = ep.Token
		streams.Present |= 1 << pair.index
	}
	r.Base.Streams = streams
	return r, nil
}

// encodeReadOld covers point, selector, and range reads before ReadOptions.
func encodeReadOld(b *Builder, key []byte, begin, end *kv.KeySelector, limit, limitBytes int32, c *ReadCtx, reply UID) Off {
	p := objectProtocol(b.Protocol)
	var first, last Off
	if begin == nil {
		first = b.Bytes(key)
	} else {
		first = keySelector(b, begin)
	}
	if end != nil {
		last = keySelector(b, end)
	}
	var tags Off
	if p >= object63 && len(c.Tags) != 0 {
		tags = tagSet(b, c.Tags)
	}
	var debug Off
	if c.HasOptions {
		debug = optionalUID(b, c.Options.DebugID)
	}
	rp := replyPromise(b, reply)
	var tn Off
	if p >= object71 {
		tn = noTenant(b)
	}
	// 6.2 GetKey lacks debugID; range reads insert isFetchKeys before tags/debug.
	debugSlot, replySlot, tenantSlot := 4, 6, 8
	if p < object63 {
		debugSlot, replySlot = 2, 4
		if begin != nil && end == nil {
			replySlot = 2
			debug = 0
		}
	}
	if end != nil {
		debugSlot, replySlot, tenantSlot = 8, 10, 12
		if p < object63 {
			debugSlot, replySlot = 6, 8
		}
	}
	b.StartTable(14)
	b.AddOffset(0, first)
	if end == nil {
		b.AddI64(1, c.Version)
		if tags != 0 {
			b.AddUnion(2, 1, tags)
		}
	} else {
		b.AddOffset(1, last)
		b.AddI64(2, c.Version)
		b.AddI32(3, limit)
		b.AddI32(4, limitBytes)
		b.AddBool(5, c.HasOptions && c.Options.ReadType == 1)
		if tags != 0 {
			b.AddUnion(6, 1, tags)
		}
	}
	if debug != 0 {
		b.AddUnion(debugSlot, 1, debug)
	}
	b.AddOffset(replySlot, rp)
	if tn != 0 {
		b.AddOffset(tenantSlot, tn)
	}
	return b.EndTable()
}

func encodeMapped71(b *Builder, r *GetMappedKeyValues, reply UID) Off {
	bo, eo := keySelector(b, &r.Begin), keySelector(b, &r.End)
	mo := b.Bytes(r.Mapper)
	var tags Off
	if len(r.Ctx.Tags) != 0 {
		tags = tagSet(b, r.Ctx.Tags)
	}
	var debug Off
	if r.Ctx.HasOptions {
		debug = optionalUID(b, r.Ctx.Options.DebugID)
	}
	rp, tn := replyPromise(b, reply), noTenant(b)
	b.StartTable(15)
	b.AddOffset(0, bo)
	b.AddOffset(1, eo)
	b.AddOffset(2, mo)
	b.AddI64(3, r.Ctx.Version)
	b.AddI32(4, r.Limit)
	b.AddI32(5, r.LimitBytes)
	b.AddBool(6, r.Ctx.HasOptions && r.Ctx.Options.ReadType == 1)
	if tags != 0 {
		b.AddUnion(7, 1, tags)
	}
	if debug != 0 {
		b.AddUnion(9, 1, debug)
	}
	b.AddOffset(11, rp)
	b.AddOffset(13, tn)
	return b.EndTable()
}

func encodeWatch62(b *Builder, r *WatchValue, reply UID) Off {
	key := b.Bytes(r.Key)
	var value Off
	if r.Value != nil {
		value = b.Bytes(r.Value)
	}
	debug := optionalUID(b, r.DebugID)
	rp := replyPromise(b, reply)
	b.StartTable(7)
	b.AddOffset(0, key)
	if r.Value != nil {
		b.AddUnion(1, 1, value)
	}
	b.AddI64(3, r.Version)
	if debug != 0 {
		b.AddUnion(4, 1, debug)
	}
	b.AddOffset(6, rp)
	return b.EndTable()
}

func encodeLocationsOld(b *Builder, r GetKeyServerLocations, reply UID) Off {
	begin := b.Bytes(r.Begin)
	var end Off
	if r.End != nil {
		end = b.Bytes(r.End)
	}
	rp := replyPromise(b, reply)
	b.StartTable(10)
	b.AddOffset(0, begin)
	if r.End != nil {
		b.AddUnion(1, 1, end)
	}
	b.AddI32(3, r.Limit)
	b.AddBool(4, r.Reverse)
	b.AddOffset(5, rp)
	// 7.1's optional tenant name occupies slots7/8. Starting in 7.2 this
	// becomes TenantInfo, whose explicit deserialization establishes trust.
	if objectProtocol(b.Protocol) >= object71 {
		b.AddI64(9, LatestVersion)
	}
	return b.EndTable()
}
