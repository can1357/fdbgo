package native

import (
	"bytes"
	"testing"

	"github.com/can1357/fdbgo/internal/kv"
)

func TestNormalizeOversizedRangeSelectors(t *testing.T) {
	for _, prefix := range [][]byte{[]byte("a"), []byte("\xff/a")} {
		limit := kv.MaxReadKeySize(prefix)
		key := bytes.Repeat([]byte{'m'}, limit+100)
		copy(key, prefix)
		for _, equal := range []bool{false, true} {
			b, e, ok, err := normalizeRange(kv.KeySelector{Key: key, OrEqual: equal, Offset: 1}, kv.FirstGreaterOrEqual(kv.SpecialKeysBegin), kv.DefaultLimits())
			if err != nil || !ok {
				t.Fatalf("normalizeRange: ok=%v err=%v", ok, err)
			}
			if len(b.Key) != limit+1 || b.OrEqual || !bytes.Equal(e.Key, kv.SpecialKeysBegin) {
				t.Fatal("oversized range bound reached native path")
			}
		}
		// Different oversized keys sharing the first limit+1 bytes select
		// the same anchor. Normalization must recognize the empty range.
		end := bytes.Clone(key)
		end[len(end)-1]++
		_, _, ok, err := normalizeRange(kv.FirstGreaterThan(key), kv.FirstGreaterOrEqual(end), kv.DefaultLimits())
		if err != nil || ok {
			t.Fatalf("equivalent oversized bounds: ok=%v err=%v", ok, err)
		}
	}
}
