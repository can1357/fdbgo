package directory

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/can1357/fdbgo/subspace"
	"github.com/can1357/fdbgo/tuple"
)

// TestKeyLayout pins the metadata keys shared with every binding.
func TestKeyLayout(t *testing.T) {
	dl := root
	if got := string(dl.rootNode.Bytes()); got != "\xfe\x01\xfe\x00" {
		t.Errorf("root node = %q", got)
	}
	if got := string(sub(dl.rootNode, versionKey).Bytes()); got != "\xfe\x01\xfe\x00\x01version\x00" {
		t.Errorf("version key = %q", got)
	}
	if got := string(dl.allocator.counters.Bytes()); got != "\xfe\x01\xfe\x00\x01hca\x00\x14" {
		t.Errorf("allocator counters = %q", got)
	}
	if got := string(dl.allocator.recent.Bytes()); got != "\xfe\x01\xfe\x00\x01hca\x00\x15\x01" {
		t.Errorf("allocator recent = %q", got)
	}
	// Child entry of "app" under the root and the layer key of the
	// directory with prefix 0x15 0x07.
	if got := string(pack(dl.rootNode, tuple.Tuple{subDirs, "app"})); got != "\xfe\x01\xfe\x00\x14\x02app\x00" {
		t.Errorf("subdirectory key = %q", got)
	}
	n := dl.nodeWithPrefix([]byte{0x15, 0x07})
	if got := string(pack(n, tuple.Tuple{layerKey})); got != "\xfe\x01\x15\x07\x00\x01layer\x00" {
		t.Errorf("layer key = %q", got)
	}
	if p, err := dl.prefixOfNode(n); err != nil || !bytes.Equal(p, []byte{0x15, 0x07}) {
		t.Errorf("prefixOfNode = %q, %v", p, err)
	}

	custom := NewDirectoryLayer(subspace.FromBytes([]byte("N")), subspace.FromBytes([]byte("C")), true).(*directoryLayer)
	if got := string(custom.rootNode.Bytes()); got != "N\x01N\x00" {
		t.Errorf("custom root node = %q", got)
	}
}

func TestPartitionLayer(t *testing.T) {
	prefix := []byte{0x15, 0x05}
	p := newDirectoryPartition([]string{"a", "part"}, prefix, root)
	if got := string(p.dl.nodeSS.Bytes()); got != "\x15\x05\xfe" {
		t.Errorf("partition node subspace = %q", got)
	}
	if got := string(p.dl.contentSS.Bytes()); got != "\x15\x05" {
		t.Errorf("partition content subspace = %q", got)
	}
	if got := string(p.dl.rootNode.Bytes()); got != "\x15\x05\xfe\x01\x15\x05\xfe\x00" {
		t.Errorf("partition root node = %q", got)
	}
	if !slices.Equal(p.dl.path, []string{"a", "part"}) || p.dl.allowManualPrefixes {
		t.Errorf("partition layer path %v, manual prefixes %v", p.dl.path, p.dl.allowManualPrefixes)
	}
	if string(p.Layer()) != "partition" || !slices.Equal(p.Path(), []string{"a", "part"}) {
		t.Errorf("partition Layer %q Path %v", p.Layer(), p.Path())
	}

	// The root of a partition is not a subspace.
	if _, err := p.Subspace(); !errors.Is(err, ErrPartitionRoot) {
		t.Errorf("Subspace: %v", err)
	}
	if _, err := p.Sub(1); !errors.Is(err, ErrPartitionRoot) {
		t.Errorf("Sub: %v", err)
	}
	if _, err := p.Bytes(); !errors.Is(err, ErrPartitionRoot) {
		t.Errorf("Bytes: %v", err)
	}
	if _, err := p.Pack(tuple.Tuple{1}); !errors.Is(err, ErrPartitionRoot) {
		t.Errorf("Pack: %v", err)
	}
	if _, err := p.PackWithVersionstamp(tuple.Tuple{tuple.IncompleteVersionstamp(0)}); !errors.Is(err, ErrPartitionRoot) {
		t.Errorf("PackWithVersionstamp: %v", err)
	}
	if _, err := p.Unpack([]byte{0x15, 0x05, 0x15, 0x01}); !errors.Is(err, ErrPartitionRoot) {
		t.Errorf("Unpack: %v", err)
	}
	if p.Contains([]byte{0x15, 0x05, 0x15, 0x01}) {
		t.Error("partition root contains a key")
	}
	if b, e := p.FDBRangeKeys(); bytes.Compare(b, e) <= 0 {
		t.Errorf("partition range [%q, %q) is not inverted", b, e)
	}
	if p.layerForPath(nil) != root || p.layerForPath([]string{"x"}) != p.dl {
		t.Error("layerForPath")
	}
}

func TestDirectorySubspace(t *testing.T) {
	d := directorySubspace{dl: root, ss: subspace.FromBytes([]byte{0x15, 0x09}), path: []string{"app"}, layer: []byte("L")}
	empty, err := d.Pack(nil)
	if err != nil {
		t.Fatal(err)
	}
	empty[0] ^= 0xff
	again, err := d.Pack(tuple.Tuple{})
	if err != nil || !bytes.Equal(again, []byte{0x15, 0x09}) {
		t.Fatalf("empty Pack aliases directory prefix: %q, %v", again, err)
	}
	key, err := d.Pack(tuple.Tuple{"k"})
	if err != nil || string(key) != "\x15\x09\x02k\x00" {
		t.Errorf("Pack = %q, %v", key, err)
	}
	if got, err := d.Unpack(key); err != nil || len(got) != 1 || got[0] != "k" {
		t.Errorf("Unpack = %v, %v", got, err)
	}
	if !d.Contains(key) || d.Contains([]byte{0x15, 0x0a}) {
		t.Error("Contains")
	}
	if b, err := d.Bytes(); err != nil || string(b) != "\x15\x09" {
		t.Errorf("Bytes = %q, %v", b, err)
	}
	if s, err := d.Sub("x"); err != nil || string(s.Bytes()) != "\x15\x09\x02x\x00" {
		t.Errorf("Sub = %v, %v", s, err)
	}
	if b, e := d.FDBRangeKeys(); string(b) != "\x15\x09\x00" || string(e) != "\x15\x09\xff" {
		t.Errorf("FDBRangeKeys = %q, %q", b, e)
	}
	if string(d.Layer()) != "L" || !slices.Equal(d.Path(), []string{"app"}) {
		t.Error("Layer/Path")
	}
}

func TestPartitionSubpath(t *testing.T) {
	part := newPartitionLayer([]string{"p"}, []byte{0x15, 0x01})
	got, err := partitionSubpath(part, []string{"p", "a"}, []string{"b"})
	if err != nil || !slices.Equal(got, []string{"a", "b"}) {
		t.Errorf("partitionSubpath = %v, %v", got, err)
	}
	if _, err := partitionSubpath(part, nil, []string{"b"}); !errors.Is(err, ErrCannotCreateSubpath) {
		t.Errorf("short directory path: %v", err)
	}
}

// TestMoveChecks covers the moves rejected before any transaction is used.
func TestMoveChecks(t *testing.T) {
	ctx := context.Background()
	if _, err := Root().MoveTo(ctx, nil, []string{"x"}); !errors.Is(err, ErrCannotMoveRoot) {
		t.Errorf("Root().MoveTo: %v", err)
	}
	part := newPartitionLayer([]string{"p"}, []byte{0x15, 0x01})
	d := directorySubspace{dl: part, ss: subspace.FromBytes([]byte{0x15, 0x01, 0x15, 0x02}), path: []string{"p", "a"}}
	for _, newPath := range [][]string{{"q", "a"}, {}, {"q"}} {
		if _, err := d.MoveTo(ctx, nil, newPath); !errors.Is(err, ErrCannotMoveBetweenPartitions) {
			t.Errorf("MoveTo(%v): %v", newPath, err)
		}
	}
	p := newDirectoryPartition([]string{"outer", "p"}, []byte{0x15, 0x03}, newPartitionLayer([]string{"outer"}, []byte{0x15, 0x04}))
	if _, err := p.MoveTo(ctx, nil, []string{"elsewhere", "p"}); !errors.Is(err, ErrCannotMoveBetweenPartitions) {
		t.Errorf("partition MoveTo across its parent partition: %v", err)
	}
}

func TestWindowSize(t *testing.T) {
	for start, want := range map[int64]int64{0: 64, 254: 64, 255: 1024, 65534: 1024, 65535: 8192, 1 << 40: 8192} {
		if got := windowSize(start); got != want {
			t.Errorf("windowSize(%d) = %d, want %d", start, got, want)
		}
	}
}
