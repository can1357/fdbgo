package directory

import (
	"context"
	"fmt"

	"github.com/can1357/fdbgo/fdb"
	"github.com/can1357/fdbgo/subspace"
	"github.com/can1357/fdbgo/tuple"
)

// directorySubspace is an opened directory: its content subspace, the
// absolute path it was opened at and the directory layer managing it.
type directorySubspace struct {
	dl    *directoryLayer
	ss    subspace.Subspace
	path  []string
	layer []byte
}

var (
	_ DirectorySubspace = directorySubspace{}
	_ DirectorySubspace = directoryPartition{}
	_ Directory         = (*directoryLayer)(nil)
)

func (d directorySubspace) subpath(path []string) ([]string, error) {
	return partitionSubpath(d.dl, d.path, path)
}

// CreateOrOpen implements Directory.
func (d directorySubspace) CreateOrOpen(ctx context.Context, t fdb.Transactor, path []string, layer []byte) (DirectorySubspace, error) {
	p, err := d.subpath(path)
	if err != nil {
		return nil, err
	}
	return d.dl.CreateOrOpen(ctx, t, p, layer)
}

// Create implements Directory.
func (d directorySubspace) Create(ctx context.Context, t fdb.Transactor, path []string, layer []byte) (DirectorySubspace, error) {
	return d.CreatePrefix(ctx, t, path, layer, nil)
}

// CreatePrefix implements Directory.
func (d directorySubspace) CreatePrefix(ctx context.Context, t fdb.Transactor, path []string, layer, prefix []byte) (DirectorySubspace, error) {
	p, err := d.subpath(path)
	if err != nil {
		return nil, err
	}
	return d.dl.CreatePrefix(ctx, t, p, layer, prefix)
}

// Open implements Directory.
func (d directorySubspace) Open(ctx context.Context, rt fdb.ReadTransactor, path []string, layer []byte) (DirectorySubspace, error) {
	p, err := d.subpath(path)
	if err != nil {
		return nil, err
	}
	return d.dl.Open(ctx, rt, p, layer)
}

// Move implements Directory.
func (d directorySubspace) Move(ctx context.Context, t fdb.Transactor, oldPath, newPath []string) (DirectorySubspace, error) {
	o, err := d.subpath(oldPath)
	if err != nil {
		return nil, err
	}
	n, err := d.subpath(newPath)
	if err != nil {
		return nil, err
	}
	return d.dl.Move(ctx, t, o, n)
}

// MoveTo implements Directory.
func (d directorySubspace) MoveTo(ctx context.Context, t fdb.Transactor, newAbsolutePath []string) (DirectorySubspace, error) {
	return moveDirectory(ctx, t, d.dl, d.path, newAbsolutePath)
}

// Remove implements Directory.
func (d directorySubspace) Remove(ctx context.Context, t fdb.Transactor, path []string) (bool, error) {
	p, err := d.subpath(path)
	if err != nil {
		return false, err
	}
	return d.dl.Remove(ctx, t, p)
}

// Exists implements Directory.
func (d directorySubspace) Exists(ctx context.Context, rt fdb.ReadTransactor, path []string) (bool, error) {
	p, err := d.subpath(path)
	if err != nil {
		return false, err
	}
	return d.dl.Exists(ctx, rt, p)
}

// List implements Directory.
func (d directorySubspace) List(ctx context.Context, rt fdb.ReadTransactor, path []string) ([]string, error) {
	p, err := d.subpath(path)
	if err != nil {
		return nil, err
	}
	return d.dl.List(ctx, rt, p)
}

// Layer implements Directory.
func (d directorySubspace) Layer() []byte { return d.layer }

// Path implements Directory.
func (d directorySubspace) Path() []string { return d.path }

// Subspace implements DirectorySubspace.
func (d directorySubspace) Subspace() (subspace.Subspace, error) { return d.ss, nil }

// Sub implements DirectorySubspace.
func (d directorySubspace) Sub(elems ...any) (subspace.Subspace, error) { return d.ss.Sub(elems...) }

// Bytes implements DirectorySubspace.
func (d directorySubspace) Bytes() ([]byte, error) { return d.ss.Bytes(), nil }

// Pack implements DirectorySubspace.
func (d directorySubspace) Pack(t tuple.Tuple) ([]byte, error) { return d.ss.Pack(t) }

// PackWithVersionstamp implements DirectorySubspace.
func (d directorySubspace) PackWithVersionstamp(t tuple.Tuple) ([]byte, error) {
	return d.ss.PackWithVersionstamp(t)
}

// Unpack implements DirectorySubspace.
func (d directorySubspace) Unpack(key []byte) (tuple.Tuple, error) { return d.ss.Unpack(key) }

// Contains implements DirectorySubspace.
func (d directorySubspace) Contains(key []byte) bool { return d.ss.Contains(key) }

// FDBRangeKeys implements fdb.ExactRange.
func (d directorySubspace) FDBRangeKeys() (begin, end []byte) { return d.ss.FDBRangeKeys() }

// FDBRangeKeySelectors implements fdb.Range.
func (d directorySubspace) FDBRangeKeySelectors() (begin, end fdb.KeySelector) {
	return d.ss.FDBRangeKeySelectors()
}

// String implements fmt.Stringer.
func (d directorySubspace) String() string {
	return fmt.Sprintf("DirectorySubspace(%q, %s)", d.path, d.ss)
}

// directoryPartition is a directory with layer "partition": a directory
// layer of its own, nested in its parent's, whose prefix starts every
// descendant's prefix. Its root holds no keys.
type directoryPartition struct {
	directorySubspace
	parent *directoryLayer
}

// newPartitionLayer returns the directory layer of the partition at absolute
// path with content prefix: metadata under prefix+0xFE, contents under prefix.
func newPartitionLayer(path []string, prefix []byte) *directoryLayer {
	nodePrefix := append(append(make([]byte, 0, len(prefix)+1), prefix...), 0xfe)
	return newDirectoryLayer(subspace.FromBytes(nodePrefix), subspace.FromBytes(prefix), false, path)
}

func newDirectoryPartition(path []string, prefix []byte, parent *directoryLayer) directoryPartition {
	return directoryPartition{
		directorySubspace: directorySubspace{
			dl:    newPartitionLayer(path, prefix),
			ss:    subspace.FromBytes(prefix),
			path:  path,
			layer: partitionLayer,
		},
		parent: parent,
	}
}

// Layer implements Directory. Each call returns an independent partition label.
func (p directoryPartition) Layer() []byte { return []byte("partition") }

// layerForPath returns the parent layer for the partition itself, its own
// layer for paths inside it.
func (p directoryPartition) layerForPath(path []string) *directoryLayer {
	if len(path) == 0 {
		return p.parent
	}
	return p.dl
}

// Exists implements Directory.
func (p directoryPartition) Exists(ctx context.Context, rt fdb.ReadTransactor, path []string) (bool, error) {
	dl := p.layerForPath(path)
	sp, err := partitionSubpath(dl, p.path, path)
	if err != nil {
		return false, err
	}
	return dl.Exists(ctx, rt, sp)
}

// Remove implements Directory.
func (p directoryPartition) Remove(ctx context.Context, t fdb.Transactor, path []string) (bool, error) {
	dl := p.layerForPath(path)
	sp, err := partitionSubpath(dl, p.path, path)
	if err != nil {
		return false, err
	}
	return dl.Remove(ctx, t, sp)
}

// MoveTo implements Directory: the partition moves within its parent layer.
func (p directoryPartition) MoveTo(ctx context.Context, t fdb.Transactor, newAbsolutePath []string) (DirectorySubspace, error) {
	return moveDirectory(ctx, t, p.parent, p.path, newAbsolutePath)
}

// Subspace fails with ErrPartitionRoot.
func (p directoryPartition) Subspace() (subspace.Subspace, error) {
	return subspace.Subspace{}, ErrPartitionRoot
}

// Sub fails with ErrPartitionRoot.
func (p directoryPartition) Sub(elems ...any) (subspace.Subspace, error) {
	return subspace.Subspace{}, ErrPartitionRoot
}

// Bytes fails with ErrPartitionRoot.
func (p directoryPartition) Bytes() ([]byte, error) { return nil, ErrPartitionRoot }

// Pack fails with ErrPartitionRoot.
func (p directoryPartition) Pack(t tuple.Tuple) ([]byte, error) { return nil, ErrPartitionRoot }

// PackWithVersionstamp fails with ErrPartitionRoot.
func (p directoryPartition) PackWithVersionstamp(t tuple.Tuple) ([]byte, error) {
	return nil, ErrPartitionRoot
}

// Unpack fails with ErrPartitionRoot.
func (p directoryPartition) Unpack(key []byte) (tuple.Tuple, error) { return nil, ErrPartitionRoot }

// Contains reports false: no key belongs to the root of a partition.
func (p directoryPartition) Contains(key []byte) bool { return false }

// FDBRangeKeys returns the inverted range ["\xff", ""), which clears and
// conflict ranges reject and reads find empty.
func (p directoryPartition) FDBRangeKeys() (begin, end []byte) { return []byte{0xff}, []byte{} }

// FDBRangeKeySelectors returns the selectors of FDBRangeKeys.
func (p directoryPartition) FDBRangeKeySelectors() (begin, end fdb.KeySelector) {
	b, e := p.FDBRangeKeys()
	return fdb.FirstGreaterOrEqual(b), fdb.FirstGreaterOrEqual(e)
}

// String implements fmt.Stringer.
func (p directoryPartition) String() string {
	return fmt.Sprintf("DirectoryPartition(%q, %s)", p.path, p.ss)
}
