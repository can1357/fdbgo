package kv

import (
	"bytes"
	"fmt"
	"testing"
)

func TestSelectorNormalizationBoundaries(t *testing.T) {
	for _, space := range []struct {
		name   string
		prefix []byte
		limit  int
	}{
		{"raw", []byte("a"), 10008},
		{"system", []byte("\xff/a"), 30000},
		{"special", []byte("\xff\xff/a"), 30000},
	} {
		for _, size := range []int{space.limit - 1, space.limit, space.limit + 1, space.limit + 2, space.limit + 100} {
			t.Run(fmt.Sprintf("%s/%d", space.name, size), func(t *testing.T) {
				key := bytes.Repeat([]byte{'m'}, size)
				copy(key, space.prefix)
				original := bytes.Clone(key)
				for _, orEqual := range []bool{false, true} {
					s := KeySelector{Key: key, OrEqual: orEqual, Offset: -2}
					s.Normalize()
					if len(s.Key) != min(size, space.limit+1) || s.OrEqual != orEqual || s.Offset != -2 {
						t.Fatalf("normalization changed selector incorrectly: length=%d equal=%v offset=%d", len(s.Key), s.OrEqual, s.Offset)
					}
					if &s.Key[0] != &key[0] {
						t.Fatal("normalization unnecessarily copied key")
					}
					// Compare the selector's anchor position around the longest
					// legal prefix and its lexicographic neighbors.
					prefix := bytes.Clone(key[:min(size, space.limit)])
					lower, upper := bytes.Clone(prefix), bytes.Clone(prefix)
					lower[len(lower)-1]--
					upper[len(upper)-1]++
					candidates := [][]byte{lower, prefix, upper}
					before := func(k, bound []byte, equal bool) bool {
						c := bytes.Compare(k, bound)
						return c < 0 || (equal && c == 0)
					}
					for _, candidate := range candidates {
						if before(candidate, key, orEqual) != before(candidate, s.Key, s.OrEqual) {
							t.Fatal("normalization changed anchor position")
						}
					}
					s.RemoveOrEqual()
					if s.OrEqual || len(s.Key) > space.limit+1 {
						t.Fatal("OrEqual removal exceeded selector bound")
					}
					for _, candidate := range candidates {
						if before(candidate, key, orEqual) != before(candidate, s.Key, false) {
							t.Fatal("OrEqual removal changed anchor position")
						}
					}
				}
				if !bytes.Equal(key, original) {
					t.Fatal("normalization modified caller storage")
				}
			})
		}
	}
}
