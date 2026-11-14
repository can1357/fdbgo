package native

import (
	"fmt"
	"testing"

	"github.com/can1357/fdbgo/internal/kv"
	"github.com/can1357/fdbgo/internal/ryw"
	"github.com/can1357/fdbgo/internal/wire"
)

// BenchmarkPrepareCommit measures validation and self-conflict preparation
// for ten writes, with and without a prior read of those keys.
func BenchmarkPrepareCommit(b *testing.B) {
	var plan ryw.CommitPlan
	for i := range 10 {
		key := fmt.Appendf(nil, "bench/%02d", i)
		plan.Mutations = append(plan.Mutations, wire.Mutation{Kind: mutSetValue, Param1: key, Param2: []byte("value")})
		plan.WriteConflicts = append(plan.WriteConflicts, pointRange(key))
	}
	opts := DefaultNativeOptions()
	for _, tc := range []struct {
		name  string
		reads []kv.KeyRange
	}{
		{"BlindWrites", nil},
		{"ReadModifyWrite", plan.WriteConflicts},
	} {
		b.Run(tc.name, func(b *testing.B) {
			p := plan
			p.ReadConflicts = tc.reads
			b.ReportAllocs()
			for b.Loop() {
				if _, _, err := prepareCommit(&opts, p); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
