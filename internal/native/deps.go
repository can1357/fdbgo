package native

import (
	"github.com/can1357/fdbgo/internal/fdberr"
	"github.com/can1357/fdbgo/internal/wire"
)

// Wire-protocol constants this package uses.
const (
	grvPriorityBatch     = wire.GRVPriorityBatch
	grvPriorityDefault   = wire.GRVPriorityDefault
	grvPriorityImmediate = wire.GRVPrioritySystemImmediate
	grvUseProvisional    = wire.GRVUseProvisionalProxies
	grvCausalReadRisky   = wire.GRVCausalReadRisky

	commitIsLockAware        = wire.CommitFlagIsLockAware
	commitFirstInBatch       = wire.CommitFlagFirstInBatch
	commitBypassStorageQuota = wire.CommitFlagBypassStorageQuota

	ssGetValue            uint32 = 0
	ssGetKey                     = wire.SSStreamGetKey
	ssGetKeyValues               = wire.SSStreamGetKeyValues
	ssWaitMetrics                = wire.SSStreamWaitMetrics
	ssWatchValue                 = wire.SSStreamWatchValue
	ssGetRangeSplitPoints        = wire.SSStreamGetRangeSplitPoints
	ssGetMappedKeyValues         = wire.SSStreamGetMappedKeyValues

	cpGetKeyServersLocations = wire.CPStreamGetKeyServersLocations

	invalidVersion = wire.InvalidVersion
)

// lbSplit returns a load-balanced reply's penalty, value and server error (0 = none).
func lbSplit[T any](l wire.Lb[T]) (float64, T, fdberr.Error) { return l.Penalty, l.Value, l.Err }

// lbWrap wraps a plain reply as a load-balanced one without a penalty report.
func lbWrap[T any](v T) wire.Lb[T] { return wire.Lb[T]{Penalty: -1, Value: v} }

// decodeExpiredVersion reads IdempotencyIdsExpiredVersion, which is
// ObjectWriter-serialized: root table, then {expired: i64}; 0 if malformed.
func decodeExpiredVersion(v []byte) int64 {
	root, err := wire.Root(v)
	if err != nil {
		return 0
	}
	t, ok, err := root.Table(0)
	if err != nil || !ok {
		return 0
	}
	n, err := t.I64(0)
	if err != nil {
		return 0
	}
	return n
}
