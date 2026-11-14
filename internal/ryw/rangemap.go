package ryw

import (
	"bytes"
	"slices"

	"github.com/can1357/fdbgo/internal/kv"
)

// readConflictSet is the union of this attempt's read conflicts. Unlike the
// cache and write map, it has no persistent readers: only commit and special
// key reporting need the canonical union. Keep insertion cheap and coalesce
// on demand, or before growing the buffer so repeated reads cannot retain an
// unbounded history of duplicate ranges. Endpoints are already owned.
type readConflictSet struct {
	ranges []kv.KeyRange
	dirty  bool
}

func (c *readConflictSet) add(begin, end []byte) {
	if bytes.Compare(begin, end) >= 0 {
		return
	}
	if len(c.ranges) == cap(c.ranges) {
		c.union()
	}
	if n := len(c.ranges); n > 0 {
		last := &c.ranges[n-1]
		if bytes.Compare(begin, last.End) <= 0 && bytes.Compare(end, last.Begin) >= 0 {
			if bytes.Compare(begin, last.Begin) < 0 {
				last.Begin = begin
				c.dirty = true
			}
			if bytes.Compare(end, last.End) > 0 {
				last.End = end
			}
			return
		}
		if bytes.Compare(begin, last.Begin) < 0 {
			c.dirty = true
		}
	}
	c.ranges = append(c.ranges, kv.KeyRange{Begin: begin, End: end})
}

// union returns sorted, disjoint ranges, merging touching endpoints too.
// The result is borrowed until the next insertion; callers hold Ryw.mu.
func (c *readConflictSet) union() []kv.KeyRange {
	if !c.dirty {
		return c.ranges
	}
	slices.SortFunc(c.ranges, func(a, b kv.KeyRange) int {
		return bytes.Compare(a.Begin, b.Begin)
	})
	n := 0
	for _, kr := range c.ranges {
		if n > 0 && bytes.Compare(kr.Begin, c.ranges[n-1].End) <= 0 {
			if bytes.Compare(kr.End, c.ranges[n-1].End) > 0 {
				c.ranges[n-1].End = kr.End
			}
		} else {
			c.ranges[n] = kr
			n++
		}
	}
	clear(c.ranges[n:])
	c.ranges = c.ranges[:n]
	c.dirty = false
	return c.ranges
}

// boundaries visits conflict starts and ends in [begin, end), in key order.
func (c *readConflictSet) boundaries(begin, end []byte, f func(k []byte, v bool)) {
	if bytes.Compare(begin, end) >= 0 {
		return
	}
	ranges := c.union()
	first, _ := slices.BinarySearchFunc(ranges, begin, func(kr kv.KeyRange, key []byte) int {
		return bytes.Compare(kr.End, key)
	})
	for _, kr := range ranges[first:] {
		if bytes.Compare(kr.Begin, end) >= 0 {
			break
		}
		if bytes.Compare(kr.Begin, begin) >= 0 {
			f(kr.Begin, true)
		}
		if bytes.Compare(kr.End, begin) >= 0 && bytes.Compare(kr.End, end) < 0 {
			f(kr.End, false)
		}
	}
}

// coalescedMap maps every key in ["", end) mapped to a value, stored as
// range boundaries with adjacent equal ranges merged. Boundary k -> v means
// [k, next boundary) has v; the end boundary holds the zero value.
type coalescedMap[V comparable] struct {
	m   pmap[V]
	end []byte
}

// newCoalescedMap maps everything to v.
func newCoalescedMap[V comparable](v V, end []byte) *coalescedMap[V] {
	c := &coalescedMap[V]{m: newPmap[V](nil), end: end}
	var zero V
	c.m.set([]byte{}, v)
	c.m.set(end, zero)
	return c
}

// lowerBound is the first boundary >= key (the end boundary for keys <= end).
func (c *coalescedMap[V]) lowerBound(key []byte) *pnode[V] {
	if n := c.m.view().ceil(key); n != nil {
		return n
	}
	return c.m.view().find(c.end)
}

// insert maps [begin, end) to value, merging with equal neighbours.
func (c *coalescedMap[V]) insert(begin, end []byte, value V) {
	if bytes.Compare(begin, end) >= 0 {
		return
	}
	var endVal V
	insertEnd := false
	endEntry := c.lowerBound(end)
	eraseIncl := false
	if !bytes.Equal(end, c.end) {
		if !bytes.Equal(endEntry.key, end) {
			if n := c.m.view().lower(end); n != nil && n.val != value {
				endVal, insertEnd = n.val, true
			}
		}
		if !insertEnd && endEntry.val == value && !bytes.Equal(endEntry.key, c.end) {
			eraseIncl = true
		}
	}
	insertBegin := len(begin) == 0
	if !insertBegin {
		n := c.m.view().lower(begin)
		insertBegin = n == nil || n.val != value
	}
	c.m.deleteRange(begin, false, endEntry.key, eraseIncl)
	if insertEnd {
		c.m.set(end, endVal)
	}
	if insertBegin {
		c.m.set(begin, value)
	}
}

// ranges calls f on every range [b, e) below the end boundary, in key order.
func (c *coalescedMap[V]) ranges(f func(b, e []byte, v V)) {
	var prev *pnode[V]
	c.m.view().ascend(nil, func(n *pnode[V]) bool {
		if prev != nil {
			f(prev.key, n.key, prev.val)
		}
		prev = n
		return true
	})
}

// boundaries calls f on every boundary b with begin <= b < end and the value
// of the range starting there.
func (c *coalescedMap[V]) boundaries(begin, end []byte, f func(k []byte, v V)) {
	if bytes.Compare(begin, end) >= 0 {
		return
	}
	c.m.view().ascend(begin, func(n *pnode[V]) bool {
		if bytes.Compare(n.key, end) >= 0 {
			return false
		}
		f(n.key, n.val)
		return true
	})
}
