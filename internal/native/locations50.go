package native

import (
	"bytes"
	"context"
	"encoding/binary"

	"github.com/can1357/fdbgo/internal/fdberr"
	"github.com/can1357/fdbgo/internal/kv"
	"github.com/can1357/fdbgo/internal/wire"
)

var legacyMetadataBegin = []byte("\xff/keyServers/")
var legacyMetadataEnd = []byte("\xff\xff")

// fetchLocations50 follows 5.0 NativeAPI's getKeyLocation: the proxy supplies
// metadata servers, then keyServers and serverList resolve ordinary shards.
func (c *DbContext) fetchLocations50(ctx context.Context, info *ClusterInfo, req *wire.GetKeyServerLocations) ([]wire.ShardLocation, error) {
	key := req.Begin
	if req.End != nil && req.Reverse {
		key = req.End
	}
	limit := int(req.Limit)
	if req.End == nil {
		limit = 1
	}
	var shards []wire.ShardLocation
	for len(shards) < limit {
		loc, err := c.locate50(ctx, info, key, req.Reverse)
		if err != nil {
			return nil, err
		}
		shards = append(shards, wire.ShardLocation{Range: kv.KeyRange{Begin: loc.Begin, End: loc.End}, Servers: loc.Servers})
		if req.End == nil {
			break
		}
		if req.Reverse {
			if bytes.Compare(loc.Begin, req.Begin) <= 0 {
				break
			}
			key = loc.Begin
		} else {
			if bytes.Compare(loc.End, req.End) >= 0 {
				break
			}
			key = loc.End
		}
	}
	return shards, nil
}

func (c *DbContext) locate50(ctx context.Context, info *ClusterInfo, key []byte, reverse bool) (Location, error) {
	// In 5.0 the metadata suffix is never split: both keyServers and serverList
	// live on the interfaces supplied by MasterProxyInterface.
	if cmp := bytes.Compare(key, legacyMetadataBegin); cmp > 0 || cmp == 0 && !reverse {
		servers, _, err := basicLoadBalance(ctx, c, info.Commit, cpGetKeyServersLocations,
			wire.Request[[]wire.StorageServer](wire.GetLegacyMetadataServers{}), false)
		if err != nil {
			return Location{}, err
		}
		if len(servers) == 0 {
			return Location{}, fdberr.AllAlternativesFailed
		}
		return Location{Begin: legacyMetadataBegin, End: legacyMetadataEnd, Servers: servers}, nil
	}

	opts := DefaultNativeOptions()
	opts.LockAware = true
	transaction := newNativeTxn(ctx, c, opts)
	defer transaction.Close()
	// Location metadata follows latestVersion, not the user's snapshot.
	transaction.Shared.setFixedVersion(wire.LatestVersion)
	reader := transaction.Reader()
	lookup := append(bytes.Clone(legacyMetadataBegin), key...)
	begin, end := kv.LastLessOrEqual(lookup), kv.FirstGreaterThan(lookup).Add(1)
	if reverse {
		begin, end = kv.LastLessThan(lookup), kv.FirstGreaterOrEqual(lookup).Add(1)
	}
	for {
		rows, err := reader.getRangeOut(ctx, begin, end, kv.NewLimits(2, kv.Unlimited), false)
		if err != nil {
			return Location{}, err
		}
		if len(rows.Out.Data) != 2 {
			return Location{}, fdberr.SerializationFailed
		}
		first, last := rows.Out.Data[0], rows.Out.Data[1]
		lo, ok := bytes.CutPrefix(first.Key, legacyMetadataBegin)
		if !ok {
			return Location{}, fdberr.SerializationFailed
		}
		hi, ok := bytes.CutPrefix(last.Key, legacyMetadataBegin)
		if !ok || bytes.Compare(lo, hi) >= 0 {
			return Location{}, fdberr.SerializationFailed
		}
		ids, err := wire.DecodeKeyServersValue(first.Value, nil)
		if err != nil {
			return Location{}, err
		}
		if len(ids) == 0 {
			return Location{}, fdberr.AllAlternativesFailed
		}
		servers := make([]wire.StorageServer, 0, len(ids))
		for _, id := range ids {
			serverKey := binary.LittleEndian.AppendUint64([]byte("\xff/serverList/"), id.First)
			serverKey = binary.LittleEndian.AppendUint64(serverKey, id.Second)
			value, err := reader.getValue(ctx, serverKey, wire.LatestVersion)
			if err != nil {
				return Location{}, err
			}
			if value == nil {
				break // serverList changed after keyServers; resolve the shard again.
			}
			server, err := wire.DecodeServerListValue(value)
			if err != nil {
				return Location{}, err
			}
			servers = append(servers, server)
		}
		if len(servers) == len(ids) {
			return Location{Begin: lo, End: hi, Servers: servers}, nil
		}
		if err := sleep(ctx, wrongShardServerDelay); err != nil {
			return Location{}, err
		}
	}
}
