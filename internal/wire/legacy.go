package wire

import (
	"github.com/can1357/fdbgo/internal/fdberr"
	"github.com/can1357/fdbgo/internal/kv"
)

// LegacyMessage is the binary serializer contract for native custom RPCs.
type LegacyMessage interface {
	EncodeLegacy(*BinaryWriter, UID) error
}

// LegacyRequest retains the concrete result type throughout binary decoding.
type LegacyRequest[R any] interface {
	Request[R]
	DecodeLegacy(*BinaryReader) (R, error)
}

func legacyProtocol(v ProtocolVersion) bool {
	switch v.Normalized() {
	case legacy50.Normalized(), legacy51.Normalized(), legacy52.Normalized(), legacy60.Normalized(), legacy61.Normalized(), legacy62.Normalized():
		return true
	}
	return false
}

func binaryBuilder(b *Builder, protocol ProtocolVersion) BinaryWriter {
	b.Reset()
	if cap(b.buf) < 512 {
		b.buf = make([]byte, 512)
	}
	return BinaryWriter{Protocol: protocol & 0x0fffffffffffffff, buf: b.buf[:32]}
}
func finishBinary(b *Builder, w *BinaryWriter) []byte {
	b.buf = w.buf
	b.head = 32
	return b.Finished()
}

// EncodeLegacy writes a complete binary request into b without a separate
// payload allocation or copy. The prefix is reserved for transport framing.
// RPC fields follow the corresponding upstream serializer() order; Arena is
// zero-size and ReplyPromise follows the version-specific fdbrpc.h encoding.
func EncodeLegacy(b *Builder, req Message, reply UID, protocol ProtocolVersion) error {
	if !legacyProtocol(protocol) {
		return fdberr.UnsupportedOperation
	}
	r, ok := req.(LegacyMessage)
	if !ok {
		return fdberr.UnsupportedOperation
	}
	w := binaryBuilder(b, protocol)
	if err := r.EncodeLegacy(&w, reply); err != nil {
		b.Reset()
		return err
	}
	if err := w.Err(); err != nil {
		b.Reset()
		return err
	}
	finishBinary(b, &w)
	return nil
}

// DecodeLegacy unwraps the networksender.actor.h SerializeBoolAnd reply. Error
// is a uint16 (flow/Error.h), independent of the client's error integer width.
func DecodeLegacy[R any](req Request[R], msg []byte, protocol ProtocolVersion) (R, error) {
	var zero R
	if !legacyProtocol(protocol) {
		return zero, fdberr.UnsupportedOperation
	}
	r := NewBinaryReader(msg, protocol)
	if !r.Bool() {
		code := r.U16()
		if err := r.Done(); err != nil {
			return zero, err
		}
		if code == 0 || code == 0xffff {
			return zero, ErrMalformed
		}
		return zero, fdberr.Error(code)
	}
	if r.Err() != nil {
		return zero, r.Err()
	}
	decoder, ok := req.(LegacyRequest[R])
	if !ok {
		return zero, fdberr.UnsupportedOperation
	}
	out, err := decoder.DecodeLegacy(r)
	if err != nil {
		return zero, err
	}
	if err = r.Done(); err != nil {
		return zero, err
	}
	return out, nil
}

// OpenDatabaseController is the pre-6.2 OpenDatabaseRequest addressed to the
// ClusterInterface obtained from coordinator getLeader, not well-known token4.
// The historical database name is always "DB" (NativeAPI.actor.cpp).
type OpenDatabaseController struct{ KnownID UID }

// ValidateProtocol restricts controller discovery to its pre-6.2 protocol family.
func (OpenDatabaseController) ValidateProtocol(protocol ProtocolVersion) error {
	if !legacyProtocol(protocol) || protocol.Normalized() >= legacy62 {
		return fdberr.UnsupportedOperation
	}
	return nil
}
func (OpenDatabaseController) FileID() uint32      { return 2799502 }
func (OpenDatabaseController) ReplyFileID() uint32 { return FIDErrorOr | 5355080 }

// The object form is the 6.2 controller-side aggregation request. Pre-6.2
// transport always calls EncodeLegacy instead.
func (r OpenDatabaseController) Encode(b *Builder, reply UID) Off {
	rp := replyPromise(b, reply)
	b.StartTable(6)
	b.AddI32(0, 1)
	b.AddUID(4, r.KnownID)
	b.AddOffset(5, rp)
	return b.EndTable()
}
func (OpenDatabaseController) Decode(msg []byte, t Table) (ClientDBInfo, error) {
	return (OpenDatabase{}).Decode(msg, t)
}
func (r OpenDatabaseController) EncodeLegacy(w *BinaryWriter, reply UID) error {
	if w.Protocol >= legacy62 {
		return fdberr.UnsupportedOperation
	}
	if w.Protocol < legacy61 {
		w.Bytes([]byte("DB"))
		w.Bytes(nil)
		w.U32(0)
		if w.Protocol >= legacy51 {
			w.Bytes(nil)
		}
	} else {
		w.U32(0)
		w.U32(0)
		w.I32(0)
		w.Bytes(nil)
	}
	w.UID(r.KnownID)
	w.ReplyPromise(reply)
	return nil
}
func (OpenDatabaseController) DecodeLegacy(r *BinaryReader) (ClientDBInfo, error) {
	return r.ClientDBInfo(), r.Err()
}

func (r OpenDatabase) EncodeLegacy(w *BinaryWriter, reply UID) error {
	if w.Protocol < legacy62 || len(r.Hostnames) != 0 {
		return fdberr.UnsupportedOperation
	}
	w.U32(0)
	w.U32(0)
	w.Bytes(nil)
	w.UID(r.KnownID)
	w.Bytes(r.ClusterKey)
	w.U32(uint32(len(r.Coordinators)))
	for _, a := range r.Coordinators {
		w.NetAddr(a)
	}
	w.ReplyPromise(reply)
	return nil
}
func (OpenDatabase) DecodeLegacy(r *BinaryReader) (ClientDBInfo, error) {
	return r.ClientDBInfo(), r.Err()
}

func (r GetReadVersion) EncodeLegacy(w *BinaryWriter, reply UID) error {
	if len(r.Tags) != 0 {
		return fdberr.UnsupportedOperation
	}
	w.U32(r.Count)
	w.U32(r.Flags)
	w.OptionalUID(r.DebugID)
	w.ReplyPromise(reply)
	return nil
}
func (GetReadVersion) DecodeLegacy(r *BinaryReader) (ReadVersionReply, error) {
	out := ReadVersionReply{Version: r.I64(), Locked: r.Bool()}
	if r.Protocol >= legacy61 {
		out.MetadataVersion = r.OptionalBytes()
	}
	return out, r.Err()
}
func (v GetKeyServerLocations) EncodeLegacy(w *BinaryWriter, reply UID) error {
	if w.Protocol < legacy51 {
		return fdberr.UnsupportedOperation
	}
	w.Bytes(v.Begin)
	w.OptionalBytes(v.End)
	w.I32(v.Limit)
	w.Bool(v.Reverse)
	w.ReplyPromise(reply)
	return nil
}
func (GetKeyServerLocations) DecodeLegacy(r *BinaryReader) ([]ShardLocation, error) {
	if r.Protocol < legacy51 {
		return nil, fdberr.UnsupportedOperation
	}
	n := r.Count(12)
	out := make([]ShardLocation, 0, n)
	for range n {
		loc := ShardLocation{Range: r.KeyRange()}
		count := r.Count(16 + 8 + 12*24)
		loc.Servers = make([]StorageServer, 0, count)
		for range count {
			loc.Servers = append(loc.Servers, r.StorageServer())
		}
		out = append(out, loc)
	}
	return out, r.Err()
}

// GetLegacyMetadataServers is the 5.0 proxy's unkeyed location bootstrap RPC.
// Its endpoint is CPStreamGetKeyServersLocations, but its request is only a
// ReplyPromise<vector<StorageServerInterface>>, not a keyed location query.
// The returned servers cover [keyServersPrefix, allKeys.end); callers resolve
// ordinary shards transactionally through keyServers/serverList (5.0 NativeAPI).
type GetLegacyMetadataServers struct{}

// ValidateProtocol restricts unkeyed metadata-server discovery to FoundationDB 5.0.
func (GetLegacyMetadataServers) ValidateProtocol(protocol ProtocolVersion) error {
	if !protocol.Compatible(legacy50) {
		return fdberr.UnsupportedOperation
	}
	return nil
}
func (GetLegacyMetadataServers) FileID() uint32                   { return 0x12<<24 | 15302073 }
func (GetLegacyMetadataServers) ReplyFileID() uint32              { return 0x12<<24 | 15302073 }
func (GetLegacyMetadataServers) Encode(b *Builder, reply UID) Off { return replyPromise(b, reply) }
func (GetLegacyMetadataServers) Decode(_ []byte, t Table) ([]StorageServer, error) {
	v, err := t.Vector(0)
	if err != nil {
		return nil, err
	}
	out := make([]StorageServer, 0, v.capHint())
	for i := range v.Len() {
		st, err := v.Table(i)
		if err != nil {
			return nil, err
		}
		s, err := storageServer(st)
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, nil
}
func (GetLegacyMetadataServers) EncodeLegacy(w *BinaryWriter, reply UID) error {
	if w.Protocol >= legacy51 {
		return fdberr.UnsupportedOperation
	}
	w.ReplyPromise(reply)
	return nil
}
func (GetLegacyMetadataServers) DecodeLegacy(r *BinaryReader) ([]StorageServer, error) {
	if r.Protocol >= legacy51 {
		return nil, fdberr.UnsupportedOperation
	}
	n := r.Count(16 + 8 + 12*24)
	out := make([]StorageServer, 0, n)
	for range n {
		out = append(out, r.StorageServer())
	}
	return out, r.Err()
}

func legacyReadContext(c ReadCtx) error {
	if len(c.Tags) != 0 {
		return fdberr.UnsupportedOperation
	}
	// Legacy storage servers have no per-request ReadOptions. Default options
	// are semantically identical; silently dropping nondefaults is not.
	options := c.Options
	options.DebugID = UID{}
	// Database lock awareness is enforced by the GRV client on these releases;
	// storage range locks, and their separate read flag, did not exist yet.
	options.LockAware = false
	if c.HasOptions && options != DefaultReadOpts() {
		return fdberr.UnsupportedOperation
	}
	return nil
}
func (v *GetValue) EncodeLegacy(w *BinaryWriter, reply UID) error {
	if err := legacyReadContext(v.Ctx); err != nil {
		return err
	}
	w.Bytes(v.Key)
	w.I64(v.Ctx.Version)
	w.OptionalUID(v.Ctx.Options.DebugID)
	w.ReplyPromise(reply)
	return nil
}
func (v *GetKey) EncodeLegacy(w *BinaryWriter, reply UID) error {
	if err := legacyReadContext(v.Ctx); err != nil {
		return err
	}
	if v.Ctx.Options.DebugID.IsValid() {
		return fdberr.UnsupportedOperation
	}
	w.KeySelector(v.Sel)
	w.I64(v.Ctx.Version)
	w.ReplyPromise(reply)
	return nil
}
func (v *GetKeyValues) EncodeLegacy(w *BinaryWriter, reply UID) error {
	if err := legacyReadContext(v.Ctx); err != nil {
		return err
	}
	w.KeySelector(v.Begin)
	w.KeySelector(v.End)
	w.I64(v.Ctx.Version)
	w.I32(v.Limit)
	w.I32(v.LimitBytes)
	if w.Protocol >= legacy62 {
		w.Bool(false)
	}
	w.OptionalUID(v.Ctx.Options.DebugID)
	w.ReplyPromise(reply)
	return nil
}
func legacyLB(r *BinaryReader) (float64, fdberr.Error) {
	penalty := r.F64()
	var code fdberr.Error
	if r.Protocol >= legacy62 && r.Bool() {
		code = fdberr.Error(r.U16())
		if code == 0 || code == 0xffff {
			r.err = ErrMalformed
		}
	}
	return penalty, code
}
func (*GetValue) DecodeLegacy(r *BinaryReader) (Lb[[]byte], error) {
	var out Lb[[]byte]
	out.Penalty, out.Err = legacyLB(r)
	out.Value = r.OptionalBytes()
	return out, r.Err()
}
func (*GetKey) DecodeLegacy(r *BinaryReader) (Lb[kv.KeySelector], error) {
	var out Lb[kv.KeySelector]
	out.Penalty, out.Err = legacyLB(r)
	out.Value = r.KeySelector()
	return out, r.Err()
}
func (*GetKeyValues) DecodeLegacy(r *BinaryReader) (Lb[RangePage], error) {
	var out Lb[RangePage]
	out.Penalty, out.Err = legacyLB(r)
	// VectorRef's String strategy affects ObjectSerializer only. BinaryWriter
	// writes the count and KeyValueRef entries directly (flow/Arena.h:976).
	n := r.Count(8)
	out.Value.Data = make([]kv.KeyValue, 0, n)
	for range n {
		out.Value.Data = append(out.Value.Data, kv.KeyValue{Key: r.Bytes(), Value: r.Bytes()})
	}
	r.I64()
	out.Value.More = r.Bool()
	return out, r.Err()
}
func (v *WatchValue) EncodeLegacy(w *BinaryWriter, reply UID) error {
	if len(v.Tags) != 0 {
		return fdberr.UnsupportedOperation
	}
	w.Bytes(v.Key)
	w.OptionalBytes(v.Value)
	w.I64(v.Version)
	w.OptionalUID(v.DebugID)
	w.ReplyPromise(reply)
	return nil
}
func (*WatchValue) DecodeLegacy(r *BinaryReader) (int64, error) { v := r.I64(); return v, r.Err() }

func (v *Commit) EncodeLegacy(w *BinaryWriter, reply UID) error {
	if v.ReportConflictingKeys || len(v.Tags) != 0 || v.CostEstimation != nil || v.IdempotencyID != nil || v.Flags & ^uint32(3) != 0 {
		return fdberr.UnsupportedOperation
	}
	if w.Protocol < legacy60 && v.Flags & ^uint32(CommitFlagIsLockAware) != 0 {
		return fdberr.UnsupportedOperation
	}
	w.U32(uint32(len(v.ReadConflicts)))
	for _, kr := range v.ReadConflicts {
		w.KeyRange(kr)
	}
	w.U32(uint32(len(v.WriteConflicts)))
	for _, kr := range v.WriteConflicts {
		w.KeyRange(kr)
	}
	w.U32(uint32(len(v.Mutations)))
	for _, m := range v.Mutations {
		if err := legacyMutation(w, m); err != nil {
			return err
		}
	}
	w.I64(v.ReadSnapshot)
	w.ReplyPromise(reply)
	flags := v.Flags
	if v.LockAware {
		flags |= CommitFlagIsLockAware
	}
	if w.Protocol < legacy60 {
		w.OptionalUID(v.DebugID)
		w.Bool(flags&CommitFlagIsLockAware != 0)
	} else {
		w.U32(flags)
		w.OptionalUID(v.DebugID)
	}
	return nil
}
func (*Commit) DecodeLegacy(r *BinaryReader) (CommitReply, error) {
	out := CommitReply{Version: r.I64(), BatchIndex: r.U16()}
	if r.Protocol >= legacy61 {
		r.OptionalBytes()
	}
	return out, r.Err()
}
func legacyMetrics(w *BinaryWriter, m StorageMetrics) {
	w.I64(m.Bytes)
	w.I64(m.BytesWrittenPerKSecond)
	w.I64(m.IOsPerKSecond)
}
func (v *WaitMetrics) EncodeLegacy(w *BinaryWriter, reply UID) error {
	if v.Min.BytesReadPerKSecond != 0 || v.Min.OpsReadPerKSecond != 0 || v.Max.BytesReadPerKSecond != 0 || v.Max.OpsReadPerKSecond != 0 {
		return fdberr.UnsupportedOperation
	}
	w.KeyRange(v.Range)
	legacyMetrics(w, v.Min)
	legacyMetrics(w, v.Max)
	w.ReplyPromise(reply)
	return nil
}
func (*WaitMetrics) DecodeLegacy(r *BinaryReader) (StorageMetrics, error) {
	out := StorageMetrics{Bytes: r.I64(), BytesWrittenPerKSecond: r.I64(), IOsPerKSecond: r.I64()}
	return out, r.Err()
}
func (Ping) EncodeLegacy(w *BinaryWriter, reply UID) error  { w.ReplyPromise(reply); return nil }
func (Ping) DecodeLegacy(r *BinaryReader) (struct{}, error) { return struct{}{}, r.Err() }

func PingReplyTokenLegacy(msg []byte, protocol ProtocolVersion) (UID, error) {
	r := NewBinaryReader(msg, protocol)
	if r.Protocol < legacy61 {
		ep := r.Endpoint()
		return ep.Token, r.Done()
	}
	v := r.UID()
	return v, r.Done()
}
func NoticeTokenLegacy(msg []byte, protocol ProtocolVersion) (UID, error) {
	r := NewBinaryReader(msg, protocol)
	ep := r.Endpoint()
	return ep.Token, r.Done()
}
func EncodeVoidReplyLegacy(b *Builder, protocol ProtocolVersion) []byte {
	w := binaryBuilder(b, protocol)
	w.Bool(true)
	return finishBinary(b, &w)
}

// DecodeControllerEndpointsLegacy decodes the client-visible prefix of a
// versioned ClusterControllerFullInterface. Its server-only suffix is allowed:
// upstream deliberately deserializes this interface by slicing its prefix.
func DecodeControllerEndpointsLegacy(info []byte) (open, status Endpoint, err error) {
	r := NewBinaryReader(info, 0)
	r.Protocol = ProtocolVersion(r.U64()) & 0x0fffffffffffffff
	if !legacyProtocol(r.Protocol) {
		return open, status, fdberr.UnsupportedOperation
	}
	open = r.Endpoint()
	r.Endpoint()
	status = r.Endpoint()
	return open, status, r.Err()
}
