package directory

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"slices"

	"github.com/can1357/fdbgo/fdb"
	"github.com/can1357/fdbgo/subspace"
	"github.com/can1357/fdbgo/tuple"
)

// Directory layer metadata layout and version.
const (
	subDirs      = int64(0)
	majorVersion = 1
	minorVersion = 0
	patchVersion = 0
)

var (
	hcaKey         = []byte("hca")
	versionKey     = []byte("version")
	layerKey       = []byte("layer")
	partitionLayer = []byte("partition")
)

// directoryLayer is a root directory: node metadata lives in nodeSS, contents
// are allocated in contentSS. path is the absolute path of the partition the
// layer manages (empty for a top-level layer).
type directoryLayer struct {
	nodeSS              subspace.Subspace
	contentSS           subspace.Subspace
	rootNode            subspace.Subspace
	allocator           allocator
	allowManualPrefixes bool
	path                []string
}

// NewDirectoryLayer returns a directory layer storing its metadata in nodeSS
// and its directories' contents in contentSS. allowManualPrefixes enables
// CreatePrefix.
func NewDirectoryLayer(nodeSS, contentSS subspace.Subspace, allowManualPrefixes bool) Directory {
	return newDirectoryLayer(nodeSS, contentSS, allowManualPrefixes, nil)
}

func newDirectoryLayer(nodeSS, contentSS subspace.Subspace, allowManualPrefixes bool, path []string) *directoryLayer {
	rootNode := sub(nodeSS, nodeSS.Bytes())
	return &directoryLayer{
		nodeSS:              nodeSS,
		contentSS:           contentSS,
		rootNode:            rootNode,
		allocator:           newAllocator(sub(rootNode, hcaKey)),
		allowManualPrefixes: allowManualPrefixes,
		path:                path,
	}
}

// sub extends s with elements that always pack ([]byte, string, int64):
// the error Subspace.Sub reports only for unsupported types cannot occur.
func sub(s subspace.Subspace, elems ...any) subspace.Subspace {
	out, _ := s.Sub(elems...)
	return out
}

// pack is Subspace.Pack for tuples of elements that always pack.
func pack(s subspace.Subspace, t tuple.Tuple) []byte {
	out, _ := s.Pack(t)
	return out
}

// nodeWithPrefix returns the metadata node of the directory with content prefix.
func (dl *directoryLayer) nodeWithPrefix(prefix []byte) subspace.Subspace {
	return sub(dl.nodeSS, prefix)
}

// Layer returns nil: a directory layer has no layer of its own.
func (dl *directoryLayer) Layer() []byte { return nil }

// Path returns the absolute path of the partition the layer manages.
func (dl *directoryLayer) Path() []string { return dl.path }

// CreateOrOpen implements Directory.
func (dl *directoryLayer) CreateOrOpen(ctx context.Context, t fdb.Transactor, path []string, layer []byte) (DirectorySubspace, error) {
	return fdb.Transact(ctx, t, func(tr *fdb.Transaction) (DirectorySubspace, error) {
		return dl.createOrOpen(ctx, tr, tr, path, nil, layer, true, true)
	})
}

// Create implements Directory.
func (dl *directoryLayer) Create(ctx context.Context, t fdb.Transactor, path []string, layer []byte) (DirectorySubspace, error) {
	return dl.CreatePrefix(ctx, t, path, layer, nil)
}

// CreatePrefix implements Directory.
func (dl *directoryLayer) CreatePrefix(ctx context.Context, t fdb.Transactor, path []string, layer, prefix []byte) (DirectorySubspace, error) {
	return fdb.Transact(ctx, t, func(tr *fdb.Transaction) (DirectorySubspace, error) {
		return dl.createOrOpen(ctx, tr, tr, path, prefix, layer, true, false)
	})
}

// Open implements Directory.
func (dl *directoryLayer) Open(ctx context.Context, rt fdb.ReadTransactor, path []string, layer []byte) (DirectorySubspace, error) {
	return fdb.ReadTransact(ctx, rt, func(rtr fdb.ReadTransaction) (DirectorySubspace, error) {
		return dl.createOrOpen(ctx, rtr, nil, path, nil, layer, false, true)
	})
}

// Exists implements Directory.
func (dl *directoryLayer) Exists(ctx context.Context, rt fdb.ReadTransactor, path []string) (bool, error) {
	return fdb.ReadTransact(ctx, rt, func(rtr fdb.ReadTransaction) (bool, error) {
		return dl.exists(ctx, rtr, path)
	})
}

// List implements Directory.
func (dl *directoryLayer) List(ctx context.Context, rt fdb.ReadTransactor, path []string) ([]string, error) {
	return fdb.ReadTransact(ctx, rt, func(rtr fdb.ReadTransaction) ([]string, error) {
		return dl.list(ctx, rtr, path)
	})
}

// Move implements Directory.
func (dl *directoryLayer) Move(ctx context.Context, t fdb.Transactor, oldPath, newPath []string) (DirectorySubspace, error) {
	return fdb.Transact(ctx, t, func(tr *fdb.Transaction) (DirectorySubspace, error) {
		return dl.move(ctx, tr, oldPath, newPath)
	})
}

// MoveTo fails with ErrCannotMoveRoot: a directory layer is a root.
func (dl *directoryLayer) MoveTo(ctx context.Context, t fdb.Transactor, newAbsolutePath []string) (DirectorySubspace, error) {
	return nil, ErrCannotMoveRoot
}

// Remove implements Directory.
func (dl *directoryLayer) Remove(ctx context.Context, t fdb.Transactor, path []string) (bool, error) {
	return fdb.Transact(ctx, t, func(tr *fdb.Transaction) (bool, error) {
		return dl.remove(ctx, tr, path)
	})
}

// find walks path from the root node, stopping early at a partition. It
// returns nil if a component does not exist.
func (dl *directoryLayer) find(ctx context.Context, rtr fdb.ReadTransaction, path []string) (*node, error) {
	n := &node{dl: dl, subspace: dl.rootNode, targetPath: path}
	for i, name := range path {
		n.currentPath = path[:i+1]
		prefix, err := rtr.Get(ctx, pack(n.subspace, tuple.Tuple{subDirs, name}))
		if err != nil {
			return nil, err
		}
		if prefix == nil {
			return nil, nil
		}
		n.subspace = dl.nodeWithPrefix(prefix)
		if n.layer, err = loadLayer(ctx, rtr, n.subspace); err != nil {
			return nil, err
		}
		if bytes.Equal(n.layer, partitionLayer) {
			return n, nil
		}
	}
	if len(path) == 0 {
		var err error
		if n.layer, err = loadLayer(ctx, rtr, n.subspace); err != nil {
			return nil, err
		}
	}
	return n, nil
}

// absolutePath returns path below the layer's own path.
func (dl *directoryLayer) absolutePath(path []string) []string {
	return concatPath(dl.path, path)
}

// contentsOfNode opens the directory stored at node nodeSS.
func (dl *directoryLayer) contentsOfNode(nodeSS subspace.Subspace, path []string, layer []byte) (DirectorySubspace, error) {
	prefix, err := dl.prefixOfNode(nodeSS)
	if err != nil {
		return nil, err
	}
	if bytes.Equal(layer, partitionLayer) {
		return newDirectoryPartition(dl.absolutePath(path), prefix, dl), nil
	}
	return directorySubspace{
		dl:    dl,
		ss:    subspace.FromBytes(prefix),
		path:  dl.absolutePath(path),
		layer: slices.Clone(layer),
	}, nil
}

// prefixOfNode returns the content prefix a metadata node is keyed by.
func (dl *directoryLayer) prefixOfNode(nodeSS subspace.Subspace) ([]byte, error) {
	t, err := dl.nodeSS.Unpack(nodeSS.Bytes())
	if err != nil {
		return nil, err
	}
	if len(t) != 1 {
		return nil, ErrInvalidMetadata
	}
	prefix, ok := t[0].([]byte)
	if !ok {
		return nil, ErrInvalidMetadata
	}
	return prefix, nil
}

// partitionOf returns the directory layer of the partition stored at n.
func (dl *directoryLayer) partitionOf(n *node) (*directoryLayer, error) {
	prefix, err := dl.prefixOfNode(n.subspace)
	if err != nil {
		return nil, err
	}
	return newPartitionLayer(dl.absolutePath(n.currentPath), prefix), nil
}

// createOrOpen opens and/or creates the directory at path. tr is nil for
// read-only calls (allowCreate false).
func (dl *directoryLayer) createOrOpen(ctx context.Context, rtr fdb.ReadTransaction, tr *fdb.Transaction, path []string, prefix, layer []byte, allowCreate, allowOpen bool) (DirectorySubspace, error) {
	if err := dl.checkVersion(ctx, rtr, nil); err != nil {
		return nil, err
	}
	if prefix != nil && !dl.allowManualPrefixes {
		if len(dl.path) == 0 {
			return nil, ErrManualPrefixesNotAllowed
		}
		return nil, ErrPrefixInPartition
	}
	if len(path) == 0 {
		return nil, ErrNoPath
	}
	n, err := dl.find(ctx, rtr, path)
	if err != nil {
		return nil, err
	}
	if n == nil {
		return dl.create(ctx, tr, path, layer, prefix, allowCreate)
	}
	if n.isInPartition(false) {
		p, err := dl.partitionOf(n)
		if err != nil {
			return nil, err
		}
		return p.createOrOpen(ctx, rtr, tr, n.partitionSubpath(), prefix, layer, allowCreate, allowOpen)
	}
	if !allowOpen {
		return nil, ErrDirAlreadyExists
	}
	if len(layer) > 0 && !bytes.Equal(layer, n.layer) {
		return nil, ErrLayerMismatch
	}
	return n.contents()
}

func (dl *directoryLayer) create(ctx context.Context, tr *fdb.Transaction, path []string, layer, prefix []byte, allowCreate bool) (DirectorySubspace, error) {
	if !allowCreate {
		return nil, ErrDirNotExists
	}
	// Validate manual prefixes before checkVersion can write metadata.
	if prefix != nil {
		if len(prefix) == 0 {
			return nil, ErrPrefixInUse
		}
		if _, err := fdb.Strinc(prefix); err != nil {
			return nil, err
		}
	}
	if err := dl.checkVersion(ctx, tr, tr); err != nil {
		return nil, err
	}
	newPrefix, err := dl.newPrefix(ctx, tr, prefix)
	if err != nil {
		return nil, err
	}
	free, err := dl.isPrefixFree(ctx, tr, newPrefix, prefix == nil)
	if err != nil {
		return nil, err
	}
	if !free {
		return nil, ErrPrefixInUse
	}
	parent, err := dl.parentNode(ctx, tr, path)
	if err != nil {
		return nil, err
	}
	if prefix != nil && len(path) > 1 {
		// Creating missing ancestors can allocate a prefix beneath this manual
		// prefix. Recheck their writes before registering the child, retaining
		// non-snapshot reads to conflict with concurrent prefix registrations.
		free, err := dl.isPrefixFree(ctx, tr, newPrefix, false)
		if err != nil {
			return nil, err
		}
		if !free {
			return nil, ErrPrefixInUse
		}
	}
	nodeSS := dl.nodeWithPrefix(newPrefix)
	tr.Set(pack(parent, tuple.Tuple{subDirs, path[len(path)-1]}), newPrefix)
	if layer == nil {
		layer = []byte{}
	}
	tr.Set(pack(nodeSS, tuple.Tuple{layerKey}), layer)
	return dl.contentsOfNode(nodeSS, path, layer)
}

// parentNode returns the metadata node of path's parent, creating missing ancestors.
func (dl *directoryLayer) parentNode(ctx context.Context, tr *fdb.Transaction, path []string) (subspace.Subspace, error) {
	if len(path) <= 1 {
		return dl.rootNode, nil
	}
	parent, err := dl.createOrOpen(ctx, tr, tr, path[:len(path)-1], nil, nil, true, true)
	if err != nil {
		return subspace.Subspace{}, err
	}
	prefix, err := parent.Bytes()
	if err != nil {
		return subspace.Subspace{}, err
	}
	return dl.nodeWithPrefix(prefix), nil
}

// newPrefix returns the manual prefix, or allocates one in the content
// subspace and checks that no keys use it.
func (dl *directoryLayer) newPrefix(ctx context.Context, tr *fdb.Transaction, prefix []byte) ([]byte, error) {
	if prefix != nil {
		return slices.Clone(prefix), nil
	}
	n, err := dl.allocator.allocate(ctx, tr)
	if err != nil {
		return nil, err
	}
	allocated := sub(dl.contentSS, n).Bytes()
	// A directory owns every key with its raw prefix, including the prefix
	// itself and 0xff suffixes excluded by tuple ranges.
	end, err := fdb.Strinc(allocated)
	if err != nil {
		return nil, err
	}
	result, err := tr.GetRange(ctx, fdb.KeyRange{Begin: allocated, End: end}, fdb.RangeOptions{Limit: 1})
	if err != nil {
		return nil, err
	}
	kvs, err := result.Slice(ctx)
	if err != nil {
		return nil, err
	}
	if len(kvs) > 0 {
		return nil, ErrPrefixNotEmpty
	}
	return allocated, nil
}

// isPrefixFree reports whether no directory and no metadata uses prefix.
func (dl *directoryLayer) isPrefixFree(ctx context.Context, rtr fdb.ReadTransaction, prefix []byte, snapshot bool) (bool, error) {
	if len(prefix) == 0 {
		return false, nil
	}
	end, err := fdb.Strinc(prefix)
	if err != nil {
		return false, err
	}
	if snapshot {
		rtr = rtr.Snapshot()
	}
	n, err := dl.nodeContainingKey(ctx, rtr, prefix)
	if err != nil || n != nil {
		return false, err
	}
	r := fdb.KeyRange{
		Begin: pack(dl.nodeSS, tuple.Tuple{prefix}),
		End:   pack(dl.nodeSS, tuple.Tuple{end}),
	}
	result, err := rtr.GetRange(ctx, r, fdb.RangeOptions{Limit: 1})
	if err != nil {
		return false, err
	}
	kvs, err := result.Slice(ctx)
	if err != nil {
		return false, err
	}
	return len(kvs) == 0, nil
}

// nodeContainingKey returns the node of the directory whose prefix starts
// key, the root node for keys in the metadata, or nil.
func (dl *directoryLayer) nodeContainingKey(ctx context.Context, rtr fdb.ReadTransaction, key []byte) (*subspace.Subspace, error) {
	if bytes.HasPrefix(key, dl.nodeSS.Bytes()) {
		return &dl.rootNode, nil
	}
	begin, _ := dl.nodeSS.FDBRangeKeys()
	end := pack(dl.nodeSS, tuple.Tuple{append(slices.Clone(key), 0x00)})
	result, err := rtr.GetRange(ctx, fdb.KeyRange{Begin: begin, End: end}, fdb.RangeOptions{Limit: 1, Reverse: true})
	if err != nil {
		return nil, err
	}
	kvs, err := result.Slice(ctx)
	if err != nil {
		return nil, err
	}
	if len(kvs) == 0 {
		return nil, nil
	}
	t, err := dl.nodeSS.Unpack(kvs[0].Key)
	if err != nil {
		return nil, err
	}
	if len(t) > 0 {
		if prev, ok := t[0].([]byte); ok && bytes.HasPrefix(key, prev) {
			n := dl.nodeWithPrefix(prev)
			return &n, nil
		}
	}
	return nil, nil
}

// checkVersion validates the stored layer version. With tr non-nil the
// caller is about to write: a missing version is initialized and a newer
// minor version is rejected.
func (dl *directoryLayer) checkVersion(ctx context.Context, rtr fdb.ReadTransaction, tr *fdb.Transaction) error {
	key := sub(dl.rootNode, versionKey).Bytes()
	v, err := rtr.Get(ctx, key)
	if err != nil {
		return err
	}
	if v == nil {
		if tr != nil {
			var value [12]byte
			binary.LittleEndian.PutUint32(value[0:], majorVersion)
			binary.LittleEndian.PutUint32(value[4:], minorVersion)
			binary.LittleEndian.PutUint32(value[8:], patchVersion)
			tr.Set(key, value[:])
		}
		return nil
	}
	if len(v) < 12 {
		return fmt.Errorf("%w: incorrect version length", ErrIncompatibleVersion)
	}
	major := binary.LittleEndian.Uint32(v[0:])
	minor := binary.LittleEndian.Uint32(v[4:])
	patch := binary.LittleEndian.Uint32(v[8:])
	if major > majorVersion {
		return fmt.Errorf("%w: cannot load directory with version %d.%d.%d using directory layer %d.%d.%d",
			ErrIncompatibleVersion, major, minor, patch, majorVersion, minorVersion, patchVersion)
	}
	if minor > minorVersion && tr != nil {
		return fmt.Errorf("%w: directory with version %d.%d.%d is read-only when opened using directory layer %d.%d.%d",
			ErrIncompatibleVersion, major, minor, patch, majorVersion, minorVersion, patchVersion)
	}
	return nil
}

func (dl *directoryLayer) exists(ctx context.Context, rtr fdb.ReadTransaction, path []string) (bool, error) {
	if err := dl.checkVersion(ctx, rtr, nil); err != nil {
		return false, err
	}
	n, err := dl.find(ctx, rtr, path)
	if err != nil || n == nil {
		return false, err
	}
	if n.isInPartition(false) {
		p, err := dl.partitionOf(n)
		if err != nil {
			return false, err
		}
		return p.exists(ctx, rtr, n.partitionSubpath())
	}
	return true, nil
}

func (dl *directoryLayer) list(ctx context.Context, rtr fdb.ReadTransaction, path []string) ([]string, error) {
	if err := dl.checkVersion(ctx, rtr, nil); err != nil {
		return nil, err
	}
	n, err := dl.find(ctx, rtr, path)
	if err != nil {
		return nil, err
	}
	if n == nil {
		return nil, ErrDirNotExists
	}
	if n.isInPartition(true) {
		p, err := dl.partitionOf(n)
		if err != nil {
			return nil, err
		}
		return p.list(ctx, rtr, n.partitionSubpath())
	}
	return n.listSubdirs(ctx, rtr)
}

func (dl *directoryLayer) move(ctx context.Context, tr *fdb.Transaction, oldPath, newPath []string) (DirectorySubspace, error) {
	if err := dl.checkVersion(ctx, tr, tr); err != nil {
		return nil, err
	}
	if len(oldPath) <= len(newPath) && slices.Equal(oldPath, newPath[:len(oldPath)]) {
		return nil, ErrMoveIntoSubdirectory
	}
	oldNode, err := dl.find(ctx, tr, oldPath)
	if err != nil {
		return nil, err
	}
	newNode, err := dl.find(ctx, tr, newPath)
	if err != nil {
		return nil, err
	}
	if oldNode == nil {
		return nil, ErrDirNotExists
	}
	oldInPartition := oldNode.isInPartition(false)
	if newNode == nil {
		if oldInPartition {
			return nil, ErrCannotMoveBetweenPartitions
		}
	} else {
		newInPartition := newNode.isInPartition(false)
		if !oldInPartition && !newInPartition {
			return nil, ErrDirAlreadyExists
		}
		if !oldInPartition || !newInPartition || !slices.Equal(oldNode.currentPath, newNode.currentPath) {
			return nil, ErrCannotMoveBetweenPartitions
		}
		p, err := dl.partitionOf(newNode)
		if err != nil {
			return nil, err
		}
		return p.move(ctx, tr, oldNode.partitionSubpath(), newNode.partitionSubpath())
	}
	if len(newPath) == 0 {
		return nil, ErrDirAlreadyExists
	}
	parent, err := dl.find(ctx, tr, newPath[:len(newPath)-1])
	if err != nil {
		return nil, err
	}
	if parent == nil {
		return nil, ErrParentDirDoesNotExist
	}
	prefix, err := dl.prefixOfNode(oldNode.subspace)
	if err != nil {
		return nil, err
	}
	tr.Set(pack(parent.subspace, tuple.Tuple{subDirs, newPath[len(newPath)-1]}), prefix)
	if err := dl.removeFromParent(ctx, tr, oldPath); err != nil {
		return nil, err
	}
	return dl.contentsOfNode(oldNode.subspace, newPath, oldNode.layer)
}

func (dl *directoryLayer) removeFromParent(ctx context.Context, tr *fdb.Transaction, path []string) error {
	if len(path) == 0 {
		return ErrCannotModifyRoot
	}
	parent, err := dl.find(ctx, tr, path[:len(path)-1])
	if err != nil {
		return err
	}
	if parent != nil {
		tr.Clear(pack(parent.subspace, tuple.Tuple{subDirs, path[len(path)-1]}))
	}
	return nil
}

// remove removes the directory at path; it reports false if none exists.
func (dl *directoryLayer) remove(ctx context.Context, tr *fdb.Transaction, path []string) (bool, error) {
	if err := dl.checkVersion(ctx, tr, tr); err != nil {
		return false, err
	}
	if len(path) == 0 {
		return false, ErrCannotModifyRoot
	}
	n, err := dl.find(ctx, tr, path)
	if err != nil || n == nil {
		return false, err
	}
	if n.isInPartition(false) {
		p, err := dl.partitionOf(n)
		if err != nil {
			return false, err
		}
		return p.remove(ctx, tr, n.partitionSubpath())
	}
	if err := dl.removeRecursive(ctx, tr, n.subspace); err != nil {
		return false, err
	}
	if err := dl.removeFromParent(ctx, tr, path); err != nil {
		return false, err
	}
	return true, nil
}

// removeRecursive clears the contents and metadata of the directory at
// nodeSS and of all its subdirectories.
func (dl *directoryLayer) removeRecursive(ctx context.Context, tr *fdb.Transaction, nodeSS subspace.Subspace) error {
	result, err := tr.GetRange(ctx, sub(nodeSS, subDirs), fdb.RangeOptions{})
	if err != nil {
		return err
	}
	for kv, err := range result.All(ctx) {
		if err != nil {
			return err
		}
		if err := dl.removeRecursive(ctx, tr, dl.nodeWithPrefix(kv.Value)); err != nil {
			return err
		}
	}
	prefix, err := dl.prefixOfNode(nodeSS)
	if err != nil {
		return err
	}
	end, err := fdb.Strinc(prefix)
	if err != nil {
		return err
	}
	tr.ClearRange(fdb.KeyRange{Begin: prefix, End: end})
	tr.ClearRange(nodeSS)
	return nil
}
