package fdb

import (
	"fmt"
	"testing"
)

// BenchmarkReadTransact10 measures one read-only transaction of 10 point
// reads against the cluster in FDB_CLUSTER_FILE: allocations per transaction
// are the client's whole hot path (GRV, RYW, load balancing, transport).
func BenchmarkReadTransact10(b *testing.B) {
	db := openTestDB(b)
	ctx := testCtx(b)
	prefix := testPrefix(b, db)
	keys := make([][]byte, 10)
	for i := range keys {
		keys[i] = key(prefix, fmt.Sprintf("k%02d", i))
	}
	if err := db.Transact(ctx, func(tr *Transaction) error {
		for _, k := range keys {
			tr.Set(k, []byte("value"))
		}
		return nil
	}); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		err := db.ReadTransact(ctx, func(tr ReadTransaction) error {
			_, err := tr.GetMany(ctx, keys)
			return err
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}
