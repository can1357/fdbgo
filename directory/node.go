package directory

import (
	"bytes"
	"context"

	"github.com/can1357/fdbgo/fdb"
	"github.com/can1357/fdbgo/subspace"
	"github.com/can1357/fdbgo/tuple"
)

// node is the metadata of a directory found by directoryLayer.find:
// currentPath is how far the walk got (it stops at a partition), targetPath
// the path that was looked up.
type node struct {
	dl          *directoryLayer
	subspace    subspace.Subspace
	currentPath []string
	targetPath  []string
	layer       []byte
}

// loadLayer reads the layer recorded at a metadata node (empty if none).
func loadLayer(ctx context.Context, rtr fdb.ReadTransaction, nodeSS subspace.Subspace) ([]byte, error) {
	layer, err := rtr.Get(ctx, pack(nodeSS, tuple.Tuple{layerKey}))
	if err != nil {
		return nil, err
	}
	if layer == nil {
		layer = []byte{}
	}
	return layer, nil
}

// partitionSubpath returns the rest of the target path below the partition the walk stopped at.
func (n *node) partitionSubpath() []string {
	return concatPath(n.targetPath[len(n.currentPath):], nil)
}

// isInPartition reports whether the walk stopped at a partition, with part
// of the target path left unless includeEmptySubpath.
func (n *node) isInPartition(includeEmptySubpath bool) bool {
	return bytes.Equal(n.layer, partitionLayer) && (includeEmptySubpath || len(n.targetPath) > len(n.currentPath))
}

// contents opens the directory the node describes.
func (n *node) contents() (DirectorySubspace, error) {
	return n.dl.contentsOfNode(n.subspace, n.currentPath, n.layer)
}

// listSubdirs returns the names of the node's subdirectories.
func (n *node) listSubdirs(ctx context.Context, rtr fdb.ReadTransaction) ([]string, error) {
	result, err := rtr.GetRange(ctx, sub(n.subspace, subDirs), fdb.RangeOptions{})
	if err != nil {
		return nil, err
	}
	kvs, err := result.Slice(ctx)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(kvs))
	for _, kv := range kvs {
		t, err := n.subspace.Unpack(kv.Key)
		if err != nil {
			return nil, err
		}
		if len(t) != 2 {
			return nil, ErrInvalidMetadata
		}
		name, ok := t[1].(string)
		if !ok {
			return nil, ErrInvalidMetadata
		}
		names = append(names, name)
	}
	return names, nil
}
