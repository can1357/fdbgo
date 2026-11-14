package ryw

import (
	"bytes"
	"testing"

	"github.com/can1357/fdbgo/internal/kv"
)

func TestValidateOversizedSelectors(t *testing.T) {
	for _, prefix := range [][]byte{[]byte("a"), []byte("\xff/a")} {
		limit := kv.MaxReadKeySize(prefix)
		key := bytes.Repeat([]byte{'m'}, limit+100)
		copy(key, prefix)
		o := NewRywOptions(1)
		o.ReadSystemKeys = true
		for _, equal := range []bool{false, true} {
			sel := kv.KeySelector{Key: key, OrEqual: equal, Offset: -3}
			if err := ValidateGetKey(&o, &sel); err != nil {
				t.Fatal(err)
			}
			if len(sel.Key) != limit+1 || sel.OrEqual != equal || sel.Offset != -3 {
				t.Fatal("GetKey selector was not normalized")
			}
			begin := kv.KeySelector{Key: key, OrEqual: equal, Offset: 1}
			end := kv.FirstGreaterOrEqual(kv.SpecialKeysBegin)
			if ok, err := ValidateGetRange(&o, &begin, &end, kv.DefaultLimits()); err != nil || !ok {
				t.Fatalf("range validation: ok=%v err=%v", ok, err)
			}
			if len(begin.Key) != limit+1 || begin.OrEqual {
				t.Fatal("range selector was not normalized")
			}
		}
	}
}

func TestOversizedSelectorsRetainIssueTimeKeys(t *testing.T) {
	for _, equal := range []bool{false, true} {
		bound := bytes.Repeat([]byte{'m'}, 10010)
		prefix := bytes.Clone(bound[:10008])
		db := fakeFromMap(map[string][]byte{string(prefix): []byte("prefix"), "z": []byte("last")}, false)
		gated, release := db.gated(t)
		r := newRyw()
		keyRead := capture(r.GetKey(gated, kv.KeySelector{Key: bound, OrEqual: equal, Offset: 0}, false))
		rangeRead := capture(r.GetRange(gated, kv.KeySelector{Key: bound, OrEqual: equal, Offset: 1}, fgoe("zz"), kv.DefaultLimits(), false, false))
		// Public reads must retain their issue-time bounds even though Normalize
		// itself only borrows a slice of the caller's storage.
		bound[0] = 'a'
		release()
		if got := must(t, keyRead); !bytes.Equal(got, prefix) {
			t.Fatalf("GetKey resolved mutated/incorrect bound: length=%d", len(got))
		}
		eqKeys(t, must(t, rangeRead).Data, "z")
	}
}
