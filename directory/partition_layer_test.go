package directory

import (
	"bytes"
	"testing"
)

func TestPartitionLayerLabelDoesNotAliasRoutingDiscriminator(t *testing.T) {
	p := newDirectoryPartition([]string{"partition"}, []byte("prefix"), nil)
	label := p.Layer()
	label[0] = 'X'
	if string(p.Layer()) != "partition" || string(partitionLayer) != "partition" {
		t.Fatal("mutating a partition label changed partition classification")
	}
}

func TestIntegrationPartitionLabelMutationPreservesChildRouting(t *testing.T) {
	db := openDB(t)
	ctx := testContext(t)
	dl, root := testLayer(ctx, t, db, "fdbgo-"+t.Name(), false)
	t.Cleanup(func() { clearRaw(ctx, t, db, root.Bytes()) })
	p, err := dl.Create(ctx, db, []string{"partition"}, []byte("partition"))
	if err != nil {
		t.Fatal(err)
	}
	child, err := p.Create(ctx, db, []string{"child"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	label := p.Layer()
	label[0] = 'X'
	opened, err := dl.Open(ctx, db, []string{"partition", "child"}, nil)
	if err != nil {
		t.Fatalf("Open child after label mutation: %v", err)
	}
	before, err := child.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	after, err := opened.Bytes()
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("child prefix after mutation = %x, %v; want %x", after, err, before)
	}
}
