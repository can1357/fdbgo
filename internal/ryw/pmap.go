package ryw

import (
	"bytes"
	"math/rand/v2"
	"sync/atomic"
)

// The write map and the snapshot cache are persistent ordered maps: a
// read pins the version it observed
// in O(1) while the transaction keeps writing. pmap is a treap with path
// copying. Each node records the generation of the map version that created
// it; nodes of the current, not yet frozen generation are updated in place,
// so a run of writes with no read in between copies each path only once.

// genCounter hands out generations; 0 is never used, so nodes of the shared
// empty trees are always copied before being changed.
var genCounter atomic.Uint64

func nextGen() uint64 { return genCounter.Add(1) }

type pnode[V any] struct {
	key         []byte
	val         V
	left, right *pnode[V]
	prio        uint32
	gen         uint64
}

// ptree is an immutable version of a pmap.
type ptree[V any] struct{ root *pnode[V] }

// find returns the node with key, or nil.
func (t ptree[V]) find(key []byte) *pnode[V] {
	for n := t.root; n != nil; {
		c := bytes.Compare(key, n.key)
		if c == 0 {
			return n
		}
		if c < 0 {
			n = n.left
		} else {
			n = n.right
		}
	}
	return nil
}

// floor returns the node with the greatest key <= key, or nil.
func (t ptree[V]) floor(key []byte) *pnode[V] {
	var best *pnode[V]
	for n := t.root; n != nil; {
		c := bytes.Compare(n.key, key)
		if c == 0 {
			return n
		}
		if c < 0 {
			best, n = n, n.right
		} else {
			n = n.left
		}
	}
	return best
}

// lower returns the node with the greatest key < key, or nil.
func (t ptree[V]) lower(key []byte) *pnode[V] {
	var best *pnode[V]
	for n := t.root; n != nil; {
		if bytes.Compare(n.key, key) < 0 {
			best, n = n, n.right
		} else {
			n = n.left
		}
	}
	return best
}

// ceil returns the node with the least key >= key, or nil.
func (t ptree[V]) ceil(key []byte) *pnode[V] {
	var best *pnode[V]
	for n := t.root; n != nil; {
		c := bytes.Compare(n.key, key)
		if c == 0 {
			return n
		}
		if c > 0 {
			best, n = n, n.left
		} else {
			n = n.right
		}
	}
	return best
}

// higher returns the node with the least key > key, or nil.
func (t ptree[V]) higher(key []byte) *pnode[V] {
	var best *pnode[V]
	for n := t.root; n != nil; {
		if bytes.Compare(n.key, key) > 0 {
			best, n = n, n.left
		} else {
			n = n.right
		}
	}
	return best
}

// first returns the node with the least key, or nil.
func (t ptree[V]) first() *pnode[V] {
	n := t.root
	if n == nil {
		return nil
	}
	for n.left != nil {
		n = n.left
	}
	return n
}

// last returns the node with the greatest key, or nil.
func (t ptree[V]) last() *pnode[V] {
	n := t.root
	if n == nil {
		return nil
	}
	for n.right != nil {
		n = n.right
	}
	return n
}

// ascend calls f on every node with key >= lo in key order until f returns false.
func (t ptree[V]) ascend(lo []byte, f func(*pnode[V]) bool) {
	ascendFrom(t.root, lo, f)
}

func ascendFrom[V any](n *pnode[V], lo []byte, f func(*pnode[V]) bool) bool {
	for n != nil {
		if bytes.Compare(n.key, lo) >= 0 {
			if !ascendFrom(n.left, lo, f) || !f(n) {
				return false
			}
		}
		n = n.right
	}
	return true
}

// pmap is the mutable head of a persistent map. The zero value is not
// usable; create one with newPmap.
//
// New nodes come from slab, so a transaction's few dozen inserts cost a
// handful of allocations. A pmap must therefore not be copied once it has
// allocated: both copies would hand out the same nodes.
type pmap[V any] struct {
	root *pnode[V]
	gen  uint64
	// dirty: some node carries gen, so freeze must start a new generation.
	dirty bool
	slab  []pnode[V]
	slabs int // number of slabs allocated, to grow them geometrically
}

// node returns a fresh node from the slab.
func (m *pmap[V]) node() *pnode[V] {
	if len(m.slab) == 0 {
		m.slab = make([]pnode[V], 8<<min(m.slabs, 4))
		m.slabs++
	}
	n := &m.slab[0]
	m.slab = m.slab[1:]
	return n
}

// newPmap returns a map whose contents are the immutable version root.
func newPmap[V any](root *pnode[V]) pmap[V] { return pmap[V]{root: root, gen: nextGen()} }

// view returns the current contents for lookups; it must not be used after
// the next update.
func (m *pmap[V]) view() ptree[V] { return ptree[V]{m.root} }

// freeze returns the current contents as an immutable version: later updates
// copy instead of changing nodes in place.
func (m *pmap[V]) freeze() ptree[V] {
	if m.dirty {
		m.gen = nextGen()
		m.dirty = false
	}
	return ptree[V]{m.root}
}

// mut returns n itself if it belongs to the current generation, else a copy that does.
func (m *pmap[V]) mut(n *pnode[V]) *pnode[V] {
	if n.gen == m.gen {
		return n
	}
	c := m.node()
	*c = *n
	c.gen = m.gen
	m.dirty = true
	return c
}

// set maps key to val. key must not be modified afterwards.
func (m *pmap[V]) set(key []byte, val V) {
	if m.view().find(key) != nil {
		m.root = m.replace(m.root, key, val)
		return
	}
	m.dirty = true
	x := m.node()
	*x = pnode[V]{key: key, val: val, prio: rand.Uint32(), gen: m.gen}
	m.root = m.insert(m.root, x)
}

func (m *pmap[V]) replace(n *pnode[V], key []byte, val V) *pnode[V] {
	n = m.mut(n)
	switch c := bytes.Compare(key, n.key); {
	case c == 0:
		n.val = val
	case c < 0:
		n.left = m.replace(n.left, key, val)
	default:
		n.right = m.replace(n.right, key, val)
	}
	return n
}

func (m *pmap[V]) insert(n, x *pnode[V]) *pnode[V] {
	if n == nil {
		return x
	}
	if x.prio > n.prio {
		x.left, x.right = m.split(n, x.key, false)
		return x
	}
	n = m.mut(n)
	if bytes.Compare(x.key, n.key) < 0 {
		n.left = m.insert(n.left, x)
	} else {
		n.right = m.insert(n.right, x)
	}
	return n
}

// split divides n into keys < key (<= key if incl) and the rest.
func (m *pmap[V]) split(n *pnode[V], key []byte, incl bool) (l, r *pnode[V]) {
	if n == nil {
		return nil, nil
	}
	c := bytes.Compare(n.key, key)
	n = m.mut(n)
	if c < 0 || (incl && c == 0) {
		n.right, r = m.split(n.right, key, incl)
		return n, r
	}
	l, n.left = m.split(n.left, key, incl)
	return l, n
}

// merge joins a and b, every key of a being less than every key of b.
func (m *pmap[V]) merge(a, b *pnode[V]) *pnode[V] {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	if a.prio > b.prio {
		a = m.mut(a)
		a.right = m.merge(a.right, b)
		return a
	}
	b = m.mut(b)
	b.left = m.merge(a, b.left)
	return b
}

// delete removes key if present.
func (m *pmap[V]) delete(key []byte) {
	if m.view().find(key) == nil {
		return
	}
	m.root = m.del(m.root, key)
}

func (m *pmap[V]) del(n *pnode[V], key []byte) *pnode[V] {
	c := bytes.Compare(key, n.key)
	if c == 0 {
		return m.merge(n.left, n.right)
	}
	n = m.mut(n)
	if c < 0 {
		n.left = m.del(n.left, key)
	} else {
		n.right = m.del(n.right, key)
	}
	return n
}

// deleteRange removes the keys between lo and hi; loExcl excludes lo itself,
// hiIncl includes hi itself.
func (m *pmap[V]) deleteRange(lo []byte, loExcl bool, hi []byte, hiIncl bool) {
	var n *pnode[V]
	if loExcl {
		n = m.view().higher(lo)
	} else {
		n = m.view().ceil(lo)
	}
	if n == nil {
		return
	}
	if c := bytes.Compare(n.key, hi); c > 0 || (c == 0 && !hiIncl) {
		return
	}
	l, rest := m.split(m.root, lo, loExcl)
	_, r := m.split(rest, hi, hiIncl)
	m.root = m.merge(l, r)
}
