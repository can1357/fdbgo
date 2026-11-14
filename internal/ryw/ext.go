package ryw

import "bytes"

// ext is a key followed by zeros
// implicit zero bytes, ordered like the materialized string, so keyAfter
// chains need no allocation.
type ext struct {
	base  []byte
	zeros int
}

func extKey(k []byte) ext { return ext{base: k} }

// len is the length of the materialized key.
func (e ext) len() int { return len(e.base) + e.zeros }

// toBytes returns the materialized key.
func (e ext) toBytes() []byte {
	if e.zeros == 0 {
		return e.base
	}
	v := make([]byte, e.len())
	copy(v, e.base)
	return v
}

// keyAfter adds one trailing zero byte.
func (e ext) keyAfter() ext { return ext{e.base, e.zeros + 1} }

func allZero(b []byte) bool {
	for _, c := range b {
		if c != 0 {
			return false
		}
	}
	return true
}

func (e ext) compare(r ext) int {
	cbl := min(len(e.base), len(r.base))
	if c := bytes.Compare(e.base[:cbl], r.base[:cbl]); c != 0 {
		return c
	}
	if !allZero(e.base[cbl:]) {
		return 1
	}
	if !allZero(r.base[cbl:]) {
		return -1
	}
	return cmpInt(e.len(), r.len())
}

// cmpKey compares with a plain key.
func (e ext) cmpKey(k []byte) int {
	cbl := min(len(e.base), len(k))
	if c := bytes.Compare(e.base[:cbl], k[:cbl]); c != 0 {
		return c
	}
	if !allZero(e.base[cbl:]) {
		return 1
	}
	if !allZero(k[cbl:]) {
		return -1
	}
	return cmpInt(e.len(), len(k))
}

// eqKey reports whether the materialized key equals k.
func (e ext) eqKey(k []byte) bool { return e.cmpKey(k) == 0 }

func (e ext) startsWith(s ext) bool {
	if e.len() < s.len() {
		return false
	}
	cbl := min(len(e.base), len(s.base))
	upto := min(s.len(), len(e.base))
	if cbl < upto && !allZero(e.base[cbl:upto]) {
		return false
	}
	if !allZero(s.base[cbl:]) {
		return false
	}
	return bytes.Equal(e.base[:cbl], s.base[:cbl])
}

// isKeyAfter reports whether e == keyAfter(s).
func (e ext) isKeyAfter(s ext) bool {
	if e.len() != s.len()+1 {
		return false
	}
	if e.zeros == 0 && e.base[len(e.base)-1] != 0 {
		return false
	}
	return e.startsWith(s)
}

func extMax(a, b ext) ext {
	if a.compare(b) >= 0 {
		return a
	}
	return b
}

func extMin(a, b ext) ext {
	if a.compare(b) <= 0 {
		return a
	}
	return b
}

func cmpInt(a, b int) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}
