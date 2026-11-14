// Package directory is the FoundationDB directory layer: it maps
// hierarchical paths (like file system paths) to short, automatically
// allocated key prefixes, so applications can name subspaces without long
// keys. The API follows the official Go bindings' directory package, and the
// key layout is shared with every official binding.
//
// Directory operations are transactional: they take an fdb.Transactor (a
// Database, which retries and commits, or a Transaction, which composes the
// operation into a larger transaction) or an fdb.ReadTransactor for reads.
// Creating several directories concurrently inside one transaction may race;
// issue them one after another or use separate transactions.
package directory

import (
	"context"
	"errors"
	"slices"

	"github.com/can1357/fdbgo/fdb"
	"github.com/can1357/fdbgo/subspace"
	"github.com/can1357/fdbgo/tuple"
)

// Errors returned by directory operations. Version errors wrap
// ErrIncompatibleVersion with the stored version.
var (
	// ErrDirAlreadyExists is returned when creating a directory that exists, or moving onto one.
	ErrDirAlreadyExists = errors.New("directory: the directory already exists")
	// ErrDirNotExists is returned when opening, listing or moving a directory that does not exist.
	ErrDirNotExists = errors.New("directory: the directory does not exist")
	// ErrParentDirDoesNotExist is returned when moving a directory under a parent that does not exist.
	ErrParentDirDoesNotExist = errors.New("directory: the parent directory does not exist")
	// ErrNoPath is returned when creating or opening the empty path.
	ErrNoPath = errors.New("directory: no path provided")
	// ErrLayerMismatch is returned when opening a directory with a layer other than the one it was created with.
	ErrLayerMismatch = errors.New("directory: the directory was created with an incompatible layer")
	// ErrIncompatibleVersion is returned when the stored directory layer version cannot be used by this implementation.
	ErrIncompatibleVersion = errors.New("directory: incompatible directory layer version")
	// ErrManualPrefixesNotAllowed is returned by CreatePrefix on a directory layer without manual prefixes.
	ErrManualPrefixesNotAllowed = errors.New("directory: cannot specify a prefix unless manual prefixes are enabled")
	// ErrPrefixInPartition is returned by CreatePrefix inside a directory partition.
	ErrPrefixInPartition = errors.New("directory: cannot specify a prefix in a partition")
	// ErrPrefixInUse is returned when the requested prefix overlaps another directory or the directory metadata.
	ErrPrefixInUse = errors.New("directory: the given prefix is already in use")
	// ErrPrefixNotEmpty is returned when a newly allocated prefix already holds keys.
	ErrPrefixNotEmpty = errors.New("directory: the database has keys stored at the prefix chosen by the automatic prefix allocator")
	// ErrCannotModifyRoot is returned when removing the root directory.
	ErrCannotModifyRoot = errors.New("directory: the root directory cannot be modified")
	// ErrCannotMoveRoot is returned by MoveTo on a directory layer.
	ErrCannotMoveRoot = errors.New("directory: the root directory cannot be moved")
	// ErrCannotMoveBetweenPartitions is returned when a move would cross a partition boundary.
	ErrCannotMoveBetweenPartitions = errors.New("directory: cannot move between partitions")
	// ErrMoveIntoSubdirectory is returned when the destination of a move is inside the moved directory.
	ErrMoveIntoSubdirectory = errors.New("directory: the destination directory cannot be a subdirectory of the source directory")
	// ErrCannotCreateSubpath is returned when a directory's path is shorter than its directory layer's path.
	ErrCannotCreateSubpath = errors.New("directory: cannot compute the subpath of the directory")
	// ErrPartitionRoot is returned by the subspace methods of a directory partition, whose root cannot hold keys.
	ErrPartitionRoot = errors.New("directory: the root of a directory partition cannot be used as a subspace")
	// ErrInvalidMetadata is returned when the directory metadata or allocator state is malformed.
	ErrInvalidMetadata = errors.New("directory: invalid directory layer metadata")
)

// Directory is a subspace of keys identified by a hierarchical path. A
// directory layer, a DirectorySubspace and a directory partition are
// Directories; paths given to their methods are relative to them.
type Directory interface {
	// CreateOrOpen opens the directory at path, creating it and missing
	// parents if needed. A non-empty layer is recorded on creation and must
	// match the recorded layer when the directory exists.
	CreateOrOpen(ctx context.Context, t fdb.Transactor, path []string, layer []byte) (DirectorySubspace, error)
	// Open opens the directory at path, failing with ErrDirNotExists if it
	// does not exist. A non-empty layer must match the recorded layer.
	Open(ctx context.Context, rt fdb.ReadTransactor, path []string, layer []byte) (DirectorySubspace, error)
	// Create creates the directory at path and missing parents, failing with
	// ErrDirAlreadyExists if it exists.
	Create(ctx context.Context, t fdb.Transactor, path []string, layer []byte) (DirectorySubspace, error)
	// CreatePrefix is Create with a manually chosen content prefix; a nil
	// prefix allocates one. It needs a directory layer that allows manual
	// prefixes.
	CreatePrefix(ctx context.Context, t fdb.Transactor, path []string, layer, prefix []byte) (DirectorySubspace, error)
	// Move moves the directory at oldPath to newPath. The directory keeps its
	// prefix and contents; clients that have it open are unaffected.
	Move(ctx context.Context, t fdb.Transactor, oldPath, newPath []string) (DirectorySubspace, error)
	// MoveTo moves this directory to newAbsolutePath, relative to the root of
	// its directory layer (which must not change).
	MoveTo(ctx context.Context, t fdb.Transactor, newAbsolutePath []string) (DirectorySubspace, error)
	// Remove removes the directory at path, its contents and all its
	// subdirectories. It returns false, without error, if no directory
	// exists at path.
	Remove(ctx context.Context, t fdb.Transactor, path []string) (bool, error)
	// Exists reports whether a directory exists at path.
	Exists(ctx context.Context, rt fdb.ReadTransactor, path []string) (bool, error)
	// List returns the names of the immediate subdirectories of path.
	List(ctx context.Context, rt fdb.ReadTransactor, path []string) ([]string, error)
	// Layer returns the layer the directory was created with.
	Layer() []byte
	// Path returns the absolute path the directory was opened with.
	Path() []string
}

// DirectorySubspace is an opened directory: a Directory that is also the
// subspace holding the directory's contents. The root of a directory
// partition (layer "partition") cannot hold keys: its subspace methods fail
// with ErrPartitionRoot, Contains reports false and its range is the
// inverted range ["\xff", "") that clears and conflict ranges reject.
type DirectorySubspace interface {
	Directory
	// Subspace returns the content subspace of the directory.
	Subspace() (subspace.Subspace, error)
	// Sub returns the subspace of the contents extended with elems.
	Sub(elems ...any) (subspace.Subspace, error)
	// Bytes returns the content prefix. It must not be modified.
	Bytes() ([]byte, error)
	// Pack returns the key of t in the contents.
	Pack(t tuple.Tuple) ([]byte, error)
	// PackWithVersionstamp returns the key of t in the contents for SetVersionstampedKey.
	PackWithVersionstamp(t tuple.Tuple) ([]byte, error)
	// Unpack decodes a key of the contents.
	Unpack(key []byte) (tuple.Tuple, error)
	// Contains reports whether key is in the contents.
	Contains(key []byte) bool
	// FDBRangeKeys implements fdb.ExactRange over the contents.
	FDBRangeKeys() (begin, end []byte)
	// FDBRangeKeySelectors implements fdb.Range over the contents.
	FDBRangeKeySelectors() (begin, end fdb.KeySelector)
	// String describes the directory for humans.
	String() string
}

var root = newDirectoryLayer(subspace.FromBytes([]byte{0xfe}), subspace.AllKeys(), false, nil)

// Root returns the default directory layer. It stores metadata in keys
// beginning with 0xFE and allocates content prefixes in the rest of the
// keyspace, which suits otherwise empty databases; use NewDirectoryLayer to
// place metadata and contents elsewhere. The root cannot be moved or removed.
func Root() Directory { return root }

// CreateOrOpen is Root().CreateOrOpen.
func CreateOrOpen(ctx context.Context, t fdb.Transactor, path []string, layer []byte) (DirectorySubspace, error) {
	return root.CreateOrOpen(ctx, t, path, layer)
}

// Open is Root().Open.
func Open(ctx context.Context, rt fdb.ReadTransactor, path []string, layer []byte) (DirectorySubspace, error) {
	return root.Open(ctx, rt, path, layer)
}

// Create is Root().Create.
func Create(ctx context.Context, t fdb.Transactor, path []string, layer []byte) (DirectorySubspace, error) {
	return root.Create(ctx, t, path, layer)
}

// Move is Root().Move.
func Move(ctx context.Context, t fdb.Transactor, oldPath, newPath []string) (DirectorySubspace, error) {
	return root.Move(ctx, t, oldPath, newPath)
}

// Exists is Root().Exists.
func Exists(ctx context.Context, rt fdb.ReadTransactor, path []string) (bool, error) {
	return root.Exists(ctx, rt, path)
}

// List is Root().List.
func List(ctx context.Context, rt fdb.ReadTransactor, path []string) ([]string, error) {
	return root.List(ctx, rt, path)
}

// concatPath returns a fresh slice holding a then b.
func concatPath(a, b []string) []string {
	out := make([]string, 0, len(a)+len(b))
	return append(append(out, a...), b...)
}

// partitionSubpath returns path relative to directory layer dl for a
// directory opened at dirPath: dirPath without dl's path, then path.
func partitionSubpath(dl *directoryLayer, dirPath, path []string) ([]string, error) {
	if len(dl.path) > len(dirPath) {
		return nil, ErrCannotCreateSubpath
	}
	return concatPath(dirPath[len(dl.path):], path), nil
}

// moveDirectory moves the directory opened at dirPath, managed by dl, to
// newAbsolutePath, which must stay under dl's path.
func moveDirectory(ctx context.Context, t fdb.Transactor, dl *directoryLayer, dirPath, newAbsolutePath []string) (DirectorySubspace, error) {
	if len(dl.path) > len(newAbsolutePath) || !slices.Equal(dl.path, newAbsolutePath[:len(dl.path)]) {
		return nil, ErrCannotMoveBetweenPartitions
	}
	oldPath, err := partitionSubpath(dl, dirPath, nil)
	if err != nil {
		return nil, err
	}
	return dl.Move(ctx, t, oldPath, slices.Clone(newAbsolutePath[len(dl.path):]))
}
