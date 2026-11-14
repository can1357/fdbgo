package client

import (
	"errors"
	"testing"

	"github.com/can1357/fdbgo/internal/fdberr"
	"github.com/can1357/fdbgo/internal/options"
)

func TestDuplicateTagAtCapacityFailsTransaction(t *testing.T) {
	for _, code := range []int{options.TransactionTag, options.TransactionAutoThrottleTag} {
		for _, count := range []int{4, 5} {
			tr := localTxn(t)
			tr.Set([]byte("key"), []byte("value"))
			for i := range count {
				if err := tr.SetOption(code, []byte{byte('a' + i)}); err != nil {
					t.Fatal(err)
				}
			}
			if err := tr.SetOption(code, []byte("a")); err != nil {
				t.Fatal(err)
			}
			value, pending, err := tr.Get([]byte("key"), false)
			if count == 5 {
				if !errors.Is(err, fdberr.TooManyTags) {
					t.Fatalf("option %d at capacity: read error = %v", code, err)
				}
			} else if err != nil || pending != nil || string(value) != "value" {
				t.Fatalf("option %d below capacity: read = %q, pending=%v, error=%v", code, value, pending != nil, err)
			}
		}
	}
}
