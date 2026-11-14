package directory

import (
	"bytes"
	"context"
	"errors"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/can1357/fdbgo/fdb"
	"github.com/can1357/fdbgo/subspace"
	"github.com/can1357/fdbgo/tuple"
)

// openDB opens the dev cluster named by FDB_CLUSTER_FILE, skipping the test
// when it is unset.
func openDB(t *testing.T) *fdb.Database {
	t.Helper()
	cf := os.Getenv("FDB_CLUSTER_FILE")
	if cf == "" {
		t.Skip("FDB_CLUSTER_FILE not set")
	}
	db, err := fdb.Open(cf)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func testContext(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	return ctx
}

// clearRaw clears every key starting with prefix.
func clearRaw(ctx context.Context, t *testing.T, db *fdb.Database, prefix []byte) {
	t.Helper()
	r, err := fdb.PrefixRange(prefix)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Transact(ctx, func(tr *fdb.Transaction) error {
		tr.ClearRange(r)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// testLayer returns a directory layer confined to a cleared, test-specific subspace.
func testLayer(ctx context.Context, t *testing.T, db *fdb.Database, name string, manual bool) (Directory, subspace.Subspace) {
	t.Helper()
	testRoot, err := subspace.Sub(name)
	if err != nil {
		t.Fatal(err)
	}
	clearRaw(ctx, t, db, testRoot.Bytes())
	return NewDirectoryLayer(sub(testRoot, "node"), sub(testRoot, "content"), manual), testRoot
}

func TestIntegrationCreateThenOpen(t *testing.T) {
	db := openDB(t)
	ctx := testContext(t)
	dl, _ := testLayer(ctx, t, db, "test-directory-go", false)
	for _, path := range [][]string{{"application"}, {"1", "2"}} {
		created, err := dl.Create(ctx, db, path, nil)
		if err != nil {
			t.Fatalf("Create(%v): %v", path, err)
		}
		opened, err := dl.Open(ctx, db, path, nil)
		if err != nil {
			t.Fatalf("Open(%v): %v", path, err)
		}
		cb, _ := created.Bytes()
		ob, _ := opened.Bytes()
		if !bytes.Equal(cb, ob) || !slices.Equal(opened.Path(), path) {
			t.Errorf("%v: created %q at %v, opened %q at %v", path, cb, created.Path(), ob, opened.Path())
		}
	}
}

func TestIntegrationRejectsOccupiedRawPrefixes(t *testing.T) {
	db := openDB(t)
	ctx := testContext(t)
	dl, testRoot := testLayer(ctx, t, db, "test-directory-go-raw-prefixes", false)
	content := sub(testRoot, "content")
	for _, suffix := range [][]byte{{}, {0xff}} {
		tr, err := db.CreateTransaction()
		if err != nil {
			t.Fatal(err)
		}
		tr.ClearRange(testRoot)
		// Cover every candidate of the first window so either boundary is hit
		// whatever the allocator picks.
		for candidate := range int64(64) {
			tr.Set(append(pack(content, tuple.Tuple{candidate}), suffix...), []byte("existing application data"))
		}
		if _, err := dl.Create(ctx, tr, []string{"occupied"}, nil); !errors.Is(err, ErrPrefixNotEmpty) {
			t.Errorf("suffix %q: Create error = %v, want ErrPrefixNotEmpty", suffix, err)
		}
		tr.Cancel()
	}
}

func TestIntegrationRejectsInvalidManualPrefixWithoutWrites(t *testing.T) {
	db := openDB(t)
	ctx := testContext(t)
	dl, testRoot := testLayer(ctx, t, db, "test-directory-go-invalid-manual-prefix", true)
	tr, err := db.CreateTransaction()
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Cancel()
	for _, prefix := range [][]byte{{0xff}, {0xff, 0xff}} {
		_, want := fdb.Strinc(prefix)
		if _, err := dl.CreatePrefix(ctx, tr, []string{"invalid"}, nil, prefix); !errors.Is(err, want) {
			t.Fatalf("CreatePrefix(%x) = %v, want %v", prefix, err, want)
		}
		result, err := tr.GetRange(ctx, testRoot, fdb.RangeOptions{})
		if err != nil {
			t.Fatal(err)
		}
		kvs, err := result.Slice(ctx)
		if err != nil || len(kvs) != 0 {
			t.Fatalf("rejected prefix wrote metadata or poisoned transaction: %v, %v", kvs, err)
		}
	}
	// The same transaction must still support a valid creation and commit.
	validPrefix := sub(testRoot, "manual").Bytes()
	if _, err := dl.CreatePrefix(ctx, tr, []string{"valid"}, nil, validPrefix); err != nil {
		t.Fatal(err)
	}
	if err := tr.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if exists, err := dl.Exists(ctx, db, []string{"valid"}); err != nil || !exists {
		t.Fatalf("valid directory after commit: %v, %v", exists, err)
	}
}

func TestIntegrationRejectsManualPrefixOverlappingNewParent(t *testing.T) {
	db := openDB(t)
	ctx := testContext(t)
	dl, testRoot := testLayer(ctx, t, db, "test-directory-go-new-parent-prefix", true)
	content := sub(testRoot, "content").Bytes()
	tr, err := db.CreateTransaction()
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Cancel()

	// Every automatically allocated parent prefix is beneath content,
	// regardless of which candidate the allocator chooses.
	childPath := []string{"parent", "child"}
	if _, err := dl.CreatePrefix(ctx, tr, childPath, nil, content); !errors.Is(err, ErrPrefixInUse) {
		t.Errorf("CreatePrefix overlapping a newly created parent: %v, want ErrPrefixInUse", err)
	}
	parent, err := dl.Open(ctx, tr, []string{"parent"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if prefix := mustBytes(t, parent); !bytes.HasPrefix(prefix, content) {
		t.Fatalf("parent prefix %q is not beneath manual prefix %q", prefix, content)
	}
	key, err := parent.Pack(tuple.Tuple{"row"})
	if err != nil {
		t.Fatal(err)
	}
	value := []byte("parent data")
	tr.Set(key, value)
	// A caller can commit the ancestor and other work after handling the
	// rejected child creation. No overlapping child may be left registered.
	if err := tr.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	if removed, err := dl.Remove(ctx, db, childPath); err != nil || removed {
		t.Errorf("Remove rejected child = %v, %v, want false, nil", removed, err)
	}
	if exists, err := dl.Exists(ctx, db, []string{"parent"}); err != nil || !exists {
		t.Errorf("parent after removing rejected child = %v, %v", exists, err)
	}
	got, err := fdb.Transact(ctx, db, func(tr *fdb.Transaction) ([]byte, error) {
		return tr.Get(ctx, key)
	})
	if err != nil || !bytes.Equal(got, value) {
		t.Errorf("parent data after removing rejected child = %q, %v, want %q", got, err, value)
	}
}

func TestIntegrationOperations(t *testing.T) {
	db := openDB(t)
	ctx := testContext(t)
	dl, _ := testLayer(ctx, t, db, "test-directory-go-ops", false)

	a, err := dl.CreateOrOpen(ctx, db, []string{"a"}, []byte("layer-a"))
	if err != nil {
		t.Fatal(err)
	}
	if again, err := dl.CreateOrOpen(ctx, db, []string{"a"}, nil); err != nil || again.String() != a.String() {
		t.Errorf("CreateOrOpen existing = %v, %v", again, err)
	}
	if _, err := dl.Open(ctx, db, []string{"a"}, []byte("other")); !errors.Is(err, ErrLayerMismatch) {
		t.Errorf("Open with another layer: %v", err)
	}
	if _, err := dl.Create(ctx, db, []string{"a"}, nil); !errors.Is(err, ErrDirAlreadyExists) {
		t.Errorf("Create existing: %v", err)
	}
	if _, err := dl.Open(ctx, db, []string{"missing"}, nil); !errors.Is(err, ErrDirNotExists) {
		t.Errorf("Open missing: %v", err)
	}
	if _, err := dl.CreateOrOpen(ctx, db, nil, nil); !errors.Is(err, ErrNoPath) {
		t.Errorf("CreateOrOpen of the root: %v", err)
	}
	if _, err := dl.CreatePrefix(ctx, db, []string{"manual"}, nil, []byte("m")); !errors.Is(err, ErrManualPrefixesNotAllowed) {
		t.Errorf("CreatePrefix without manual prefixes: %v", err)
	}

	// Nested creation creates parents; relative operations on a DirectorySubspace.
	if _, err := dl.Create(ctx, db, []string{"a", "b", "c"}, nil); err != nil {
		t.Fatal(err)
	}
	if names, err := a.List(ctx, db, nil); err != nil || !slices.Equal(names, []string{"b"}) {
		t.Errorf("a.List = %v, %v", names, err)
	}
	if ok, err := a.Exists(ctx, db, []string{"b", "c"}); err != nil || !ok {
		t.Errorf("a.Exists(b/c) = %v, %v", ok, err)
	}
	if _, err := dl.List(ctx, db, []string{"nope"}); !errors.Is(err, ErrDirNotExists) {
		t.Errorf("List missing: %v", err)
	}

	// Contents are keyed under the directory prefix.
	key, err := a.Pack(tuple.Tuple{"row"})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Transact(ctx, func(tr *fdb.Transaction) error { tr.Set(key, []byte("v")); return nil }); err != nil {
		t.Fatal(err)
	}

	// Moves.
	if _, err := dl.Move(ctx, db, []string{"a"}, []string{"a", "x"}); !errors.Is(err, ErrMoveIntoSubdirectory) {
		t.Errorf("Move into itself: %v", err)
	}
	if _, err := dl.Move(ctx, db, []string{"a"}, []string{"no", "parent"}); !errors.Is(err, ErrParentDirDoesNotExist) {
		t.Errorf("Move under a missing parent: %v", err)
	}
	moved, err := a.MoveTo(ctx, db, []string{"renamed"})
	if err != nil {
		t.Fatal(err)
	}
	mb, _ := moved.Bytes()
	ab, _ := a.Bytes()
	if !bytes.Equal(mb, ab) || !slices.Equal(moved.Path(), []string{"renamed"}) || string(moved.Layer()) != "layer-a" {
		t.Errorf("moved = %v", moved)
	}
	if names, err := dl.List(ctx, db, nil); err != nil || !slices.Equal(names, []string{"renamed"}) {
		t.Errorf("root List after move = %v, %v", names, err)
	}

	// Partitions.
	part, err := dl.Create(ctx, db, []string{"part"}, []byte("partition"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := part.Bytes(); !errors.Is(err, ErrPartitionRoot) {
		t.Errorf("partition Bytes: %v", err)
	}
	inner, err := part.Create(ctx, db, []string{"inner"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	pb, ib := part.(directoryPartition).ss.Bytes(), mustBytes(t, inner)
	if !bytes.HasPrefix(ib, pb) || !slices.Equal(inner.Path(), []string{"part", "inner"}) {
		t.Errorf("partition child prefix %q not under %q, path %v", ib, pb, inner.Path())
	}
	if viaRoot, err := dl.Open(ctx, db, []string{"part", "inner"}, nil); err != nil || !bytes.Equal(mustBytes(t, viaRoot), ib) {
		t.Errorf("open through the partition = %v, %v", viaRoot, err)
	}
	if names, err := dl.List(ctx, db, []string{"part"}); err != nil || !slices.Equal(names, []string{"inner"}) {
		t.Errorf("List(part) = %v, %v", names, err)
	}
	if _, err := dl.Move(ctx, db, []string{"part", "inner"}, []string{"outside"}); !errors.Is(err, ErrCannotMoveBetweenPartitions) {
		t.Errorf("Move out of a partition: %v", err)
	}
	if _, err := part.CreatePrefix(ctx, db, []string{"m"}, nil, []byte("x")); !errors.Is(err, ErrPrefixInPartition) {
		t.Errorf("CreatePrefix in a partition: %v", err)
	}
	if moved, err := dl.Move(ctx, db, []string{"part", "inner"}, []string{"part", "inner2"}); err != nil || !bytes.Equal(mustBytes(t, moved), ib) {
		t.Errorf("Move inside a partition = %v, %v", moved, err)
	}

	// Removal clears contents and subdirectories.
	if ok, err := dl.Remove(ctx, db, []string{"renamed"}); err != nil || !ok {
		t.Fatalf("Remove = %v, %v", ok, err)
	}
	if ok, err := dl.Remove(ctx, db, []string{"renamed"}); err != nil || ok {
		t.Errorf("Remove again = %v, %v", ok, err)
	}
	if _, err := dl.Remove(ctx, db, nil); !errors.Is(err, ErrCannotModifyRoot) {
		t.Errorf("Remove root: %v", err)
	}
	v, err := fdb.Transact(ctx, db, func(tr *fdb.Transaction) ([]byte, error) { return tr.Get(ctx, key) })
	if err != nil || v != nil {
		t.Errorf("content after Remove = %q, %v", v, err)
	}
	if ok, err := dl.Exists(ctx, db, []string{"renamed", "b", "c"}); err != nil || ok {
		t.Errorf("subdirectory exists after Remove = %v, %v", ok, err)
	}
	if ok, err := part.Remove(ctx, db, nil); err != nil || !ok {
		t.Errorf("partition removing itself = %v, %v", ok, err)
	}
	if ok, err := dl.Exists(ctx, db, []string{"part"}); err != nil || ok {
		t.Errorf("partition exists after Remove = %v, %v", ok, err)
	}
}

func TestIntegrationManualPrefixesAndVersion(t *testing.T) {
	db := openDB(t)
	ctx := testContext(t)
	dl, testRoot := testLayer(ctx, t, db, "test-directory-go-manual", true)
	content := sub(testRoot, "content").Bytes()
	manual := append(slices.Clone(content), 'm')
	d, err := dl.CreatePrefix(ctx, db, []string{"m"}, nil, manual)
	if err != nil {
		t.Fatal(err)
	}
	if b := mustBytes(t, d); !bytes.Equal(b, manual) {
		t.Errorf("manual prefix = %q", b)
	}
	for _, p := range [][]byte{manual, append(slices.Clone(manual), 'x'), {}} {
		if _, err := dl.CreatePrefix(ctx, db, []string{"other"}, nil, p); !errors.Is(err, ErrPrefixInUse) {
			t.Errorf("CreatePrefix(%q) error = %v, want ErrPrefixInUse", p, err)
		}
	}

	// A newer stored version is rejected: majors always, minors on writes.
	versionKey := sub(dl.(*directoryLayer).rootNode, versionKey).Bytes()
	setVersion := func(major, minor uint32) {
		v := []byte{byte(major), 0, 0, 0, byte(minor), 0, 0, 0, 0, 0, 0, 0}
		if err := db.Transact(ctx, func(tr *fdb.Transaction) error { tr.Set(versionKey, v); return nil }); err != nil {
			t.Fatal(err)
		}
	}
	setVersion(1, 1)
	if _, err := dl.Open(ctx, db, []string{"m"}, nil); err != nil {
		t.Errorf("Open with a newer minor version: %v", err)
	}
	if _, err := dl.Create(ctx, db, []string{"n"}, nil); !errors.Is(err, ErrIncompatibleVersion) {
		t.Errorf("Create with a newer minor version: %v", err)
	}
	setVersion(2, 0)
	if _, err := dl.Open(ctx, db, []string{"m"}, nil); !errors.Is(err, ErrIncompatibleVersion) {
		t.Errorf("Open with a newer major version: %v", err)
	}
}

func mustBytes(t *testing.T, d DirectorySubspace) []byte {
	t.Helper()
	b, err := d.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func checkUnique(t *testing.T, ints []int64) {
	t.Helper()
	seen := make(map[int64]bool, len(ints))
	for _, n := range ints {
		if seen[n] {
			t.Fatalf("allocator returned %d twice", n)
		}
		seen[n] = true
	}
}

func TestIntegrationHCASequential(t *testing.T) {
	db := openDB(t)
	ctx := testContext(t)
	key := []byte("test-go-hca-seq")
	clearRaw(ctx, t, db, key)
	a := newAllocator(subspace.FromBytes(key))
	ints := make([]int64, 0, 1000)
	for range 1000 {
		n, err := fdb.Transact(ctx, db, func(tr *fdb.Transaction) (int64, error) { return a.allocate(ctx, tr) })
		if err != nil {
			t.Fatal(err)
		}
		ints = append(ints, n)
	}
	checkUnique(t, ints)
}

func TestIntegrationHCAConcurrent(t *testing.T) {
	db := openDB(t)
	ctx := testContext(t)
	key := []byte("test-go-hca-conc")
	clearRaw(ctx, t, db, key)
	a := newAllocator(subspace.FromBytes(key))
	ints := make([]int64, 1000)
	errs := make([]error, len(ints))
	var wg sync.WaitGroup
	for i := range ints {
		wg.Go(func() {
			ints[i], errs[i] = fdb.Transact(ctx, db, func(tr *fdb.Transaction) (int64, error) { return a.allocate(ctx, tr) })
		})
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		t.Fatal(err)
	}
	checkUnique(t, ints)
}

// TestIntegrationHCASharedTransaction runs independent allocators on one
// transaction from several goroutines.
func TestIntegrationHCASharedTransaction(t *testing.T) {
	db := openDB(t)
	ctx := testContext(t)
	ss := subspace.FromBytes([]byte("test-go-hca-shared-trx"))
	tr, err := db.CreateTransaction()
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Cancel()
	tr.ClearRange(ss)
	const workers, each = 8, 64
	results := make([][]int64, workers)
	errs := make([]error, workers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for w := range workers {
		wg.Go(func() {
			a := newAllocator(ss)
			<-start
			for range each {
				n, err := a.allocate(ctx, tr)
				if err != nil {
					errs[w] = err
					return
				}
				results[w] = append(results[w], n)
			}
		})
	}
	close(start)
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		t.Fatal(err)
	}
	checkUnique(t, slices.Concat(results...))
}
