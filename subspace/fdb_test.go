package subspace_test

import (
	"github.com/can1357/fdbgo/internal/kv"
	"github.com/can1357/fdbgo/subspace"
	"github.com/can1357/fdbgo/tuple"
)

// exactRange has the method set of fdb.ExactRange (fdb.KeySelector is an
// alias of kv.KeySelector); it is restated here so these packages test
// without building fdb.
type exactRange interface {
	FDBRangeKeys() (begin, end []byte)
	FDBRangeKeySelectors() (begin, end kv.KeySelector)
}

// Subspaces and tuples are exact ranges for clears, conflict ranges and reads.
var (
	_ exactRange = subspace.Subspace{}
	_ exactRange = tuple.Tuple{}
)
