package main

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/can1357/fdbgo/directory"
	"github.com/can1357/fdbgo/fdb"
	"github.com/can1357/fdbgo/subspace"
	"github.com/can1357/fdbgo/tuple"
)

// directoryExtension is the directory layer tester state: the directory
// list (directory layers, directory subspaces, partitions, raw subspaces
// and nil for failed creations), the current index and the error index.
type directoryExtension struct {
	list       []any
	index      int
	errorIndex int
}

func newDirectoryExtension() *directoryExtension {
	return &directoryExtension{list: []any{directory.Root()}}
}

// createOps are the operations that append to the directory list, so their
// failure appends nil.
var createOps = map[string]bool{
	"CREATE_SUBSPACE": true,
	"CREATE_LAYER":    true,
	"CREATE_OR_OPEN":  true,
	"CREATE":          true,
	"OPEN":            true,
	"MOVE":            true,
	"MOVE_TO":         true,
	"OPEN_SUBSPACE":   true,
}

// errDirectory fails a directory operation for a reason the reference
// tester reports as DIRECTORY_ERROR without an fdb error behind it.
type errDirectory string

func (e errDirectory) Error() string { return string(e) }

func (de *directoryExtension) current() any {
	if de.index < 0 || de.index >= len(de.list) {
		return nil
	}
	return de.list[de.index]
}

func (de *directoryExtension) cwd() (directory.Directory, error) {
	d, ok := de.current().(directory.Directory)
	if !ok {
		return nil, errDirectory(fmt.Sprintf("item %d is %T, not a directory", de.index, de.current()))
	}
	return d, nil
}

// css returns the subspace of the current item: a raw subspace or the
// contents of a directory subspace. Directory layers and partition roots
// have none, so their subspace operations fail.
func (de *directoryExtension) css() (subspace.Subspace, error) {
	return asSubspace(de.current())
}

// asSubspace returns the subspace an item of the directory list denotes.
func asSubspace(v any) (subspace.Subspace, error) {
	switch s := v.(type) {
	case subspace.Subspace:
		return s, nil
	case directory.DirectorySubspace:
		return s.Subspace()
	}
	return subspace.Subspace{}, errDirectory(fmt.Sprintf("%T is not a subspace", v))
}

// popTuple pops a count and that many items as a tuple.
func (sm *StackMachine) popCountedTuple() tuple.Tuple {
	return sm.popTuple(sm.popInt())
}

// popPath pops a counted tuple of strings as a directory path.
func (sm *StackMachine) popPath() ([]string, error) {
	t := sm.popCountedTuple()
	path := make([]string, len(t))
	for i, el := range t {
		s, ok := el.(string)
		if !ok {
			return nil, errDirectory(fmt.Sprintf("path element %T is not a string", el))
		}
		path[i] = s
	}
	return path, nil
}

// popMaybePath pops a count (0 or 1) and, for 1, a path.
func (sm *StackMachine) popMaybePath() ([]string, error) {
	if sm.popInt() == 0 {
		return nil, nil
	}
	return sm.popPath()
}

func packStrings(s []string) []byte {
	t := make(tuple.Tuple, len(s))
	for i, el := range s {
		t[i] = el
	}
	return mustPack(t)
}

func (de *directoryExtension) processOp(sm *StackMachine, op string, isDB, isSnapshot bool, idx int) {
	if err := de.run(sm, op, isDB, isSnapshot, idx); err != nil {
		if sm.verbose {
			fmt.Printf("%q %d. DIRECTORY_%s: %v\n", sm.prefix, idx, op, err)
		}
		sm.push(idx, directoryErrorResult)
		if createOps[op] {
			de.list = append(de.list, nil)
		}
	}
}

// run executes one directory instruction. Every error, fdb errors included,
// is reported as DIRECTORY_ERROR by processOp.
func (de *directoryExtension) run(sm *StackMachine, op string, isDB, isSnapshot bool, idx int) (err error) {
	// t is the transactor of operations that write: the database retries
	// and commits, the current transaction runs once. rt reads, through the
	// snapshot view for _SNAPSHOT.
	var t fdb.Transactor
	var rt fdb.ReadTransactor
	if isDB {
		t, rt = db, db
	} else {
		e := sm.currentEntry()
		e.mu.Lock()
		tr, stamp := e.tr, e.stamp
		e.mu.Unlock()
		defer func() {
			if endsVersionstampAttempt(err) {
				e.mu.Lock()
				stamp.resolve(nil, err)
				e.mu.Unlock()
			}
		}()
		t, rt = tr, tr
		if isSnapshot {
			rt = tr.Snapshot()
		}
	}

	switch op {
	case "CREATE_SUBSPACE":
		path := sm.popCountedTuple()
		raw := sm.popBytes()
		s, err := subspace.FromBytes(raw).Sub(path...)
		if err != nil {
			return err
		}
		de.list = append(de.list, s)
	case "CREATE_LAYER":
		i1, i2 := int(sm.popInt()), int(sm.popInt())
		manual := sm.popInt() == 1
		if i1 < 0 || i1 >= len(de.list) || i2 < 0 || i2 >= len(de.list) {
			return errDirectory("CREATE_LAYER index out of range")
		}
		if de.list[i1] == nil || de.list[i2] == nil {
			de.list = append(de.list, nil)
			return nil
		}
		node, err := asSubspace(de.list[i1])
		if err != nil {
			return err
		}
		content, err := asSubspace(de.list[i2])
		if err != nil {
			return err
		}
		de.list = append(de.list, directory.NewDirectoryLayer(node, content, manual))
	case "CHANGE":
		de.index = int(sm.popInt())
		if de.current() == nil {
			de.index = de.errorIndex
		}
	case "SET_ERROR_INDEX":
		de.errorIndex = int(sm.popInt())
	case "CREATE_OR_OPEN":
		path, err := sm.popPath()
		layer := sm.popBytes()
		if err != nil {
			return err
		}
		d, err := de.cwd()
		if err != nil {
			return err
		}
		ds, err := d.CreateOrOpen(ctx, t, path, layer)
		if err != nil {
			return err
		}
		de.list = append(de.list, ds)
	case "CREATE":
		path, err := sm.popPath()
		layer := sm.popBytes()
		prefix := sm.pop()
		if err != nil {
			return err
		}
		d, err := de.cwd()
		if err != nil {
			return err
		}
		var ds directory.DirectorySubspace
		if prefix == nil {
			ds, err = d.Create(ctx, t, path, layer)
		} else {
			p, ok := prefix.([]byte)
			if !ok {
				return errDirectory(fmt.Sprintf("prefix is %T", prefix))
			}
			ds, err = d.CreatePrefix(ctx, t, path, layer, p)
		}
		if err != nil {
			return err
		}
		de.list = append(de.list, ds)
	case "OPEN":
		path, err := sm.popPath()
		layer := sm.popBytes()
		if err != nil {
			return err
		}
		d, err := de.cwd()
		if err != nil {
			return err
		}
		ds, err := d.Open(ctx, rt, path, layer)
		if err != nil {
			return err
		}
		de.list = append(de.list, ds)
	case "MOVE":
		oldPath, err1 := sm.popPath()
		newPath, err2 := sm.popPath()
		if err := errors.Join(err1, err2); err != nil {
			return err
		}
		d, err := de.cwd()
		if err != nil {
			return err
		}
		ds, err := d.Move(ctx, t, oldPath, newPath)
		if err != nil {
			return err
		}
		de.list = append(de.list, ds)
	case "MOVE_TO":
		path, err := sm.popPath()
		if err != nil {
			return err
		}
		d, err := de.cwd()
		if err != nil {
			return err
		}
		ds, err := d.MoveTo(ctx, t, path)
		if err != nil {
			return err
		}
		de.list = append(de.list, ds)
	case "REMOVE", "REMOVE_IF_EXISTS":
		path, err := sm.popMaybePath()
		if err != nil {
			return err
		}
		d, err := de.cwd()
		if err != nil {
			return err
		}
		// Run inside Transact so a failed REMOVE on the database aborts
		// instead of committing what the attempt wrote.
		return t.Transact(ctx, func(tr *fdb.Transaction) error {
			removed, err := d.Remove(ctx, tr, path)
			if err != nil {
				return err
			}
			if !removed && op == "REMOVE" {
				return errDirectory("directory does not exist")
			}
			return nil
		})
	case "LIST":
		path, err := sm.popMaybePath()
		if err != nil {
			return err
		}
		d, err := de.cwd()
		if err != nil {
			return err
		}
		children, err := d.List(ctx, rt, path)
		if err != nil {
			return err
		}
		sm.push(idx, packStrings(children))
	case "EXISTS":
		path, err := sm.popMaybePath()
		if err != nil {
			return err
		}
		d, err := de.cwd()
		if err != nil {
			return err
		}
		ok, err := d.Exists(ctx, rt, path)
		if err != nil {
			return err
		}
		sm.push(idx, boolInt(ok))
	case "PACK_KEY":
		tup := sm.popCountedTuple()
		s, err := de.css()
		if err != nil {
			return err
		}
		k, err := s.Pack(tup)
		if err != nil {
			return err
		}
		sm.push(idx, k)
	case "UNPACK_KEY":
		key := sm.popBytes()
		s, err := de.css()
		if err != nil {
			return err
		}
		tup, err := s.Unpack(key)
		if err != nil {
			return err
		}
		for _, el := range tup {
			sm.push(idx, el)
		}
	case "RANGE":
		tup := sm.popCountedTuple()
		s, err := de.css()
		if err != nil {
			return err
		}
		sub, err := s.Sub(tup...)
		if err != nil {
			return err
		}
		begin, end := sub.FDBRangeKeys()
		sm.push(idx, begin)
		sm.push(idx, end)
	case "CONTAINS":
		key := sm.popBytes()
		s, err := de.css()
		if err != nil {
			return err
		}
		sm.push(idx, boolInt(s.Contains(key)))
	case "OPEN_SUBSPACE":
		tup := sm.popCountedTuple()
		s, err := de.css()
		if err != nil {
			return err
		}
		sub, err := s.Sub(tup...)
		if err != nil {
			return err
		}
		de.list = append(de.list, sub)
	case "LOG_SUBSPACE":
		prefix := sm.popBytes()
		s, err := de.css()
		if err != nil {
			return err
		}
		key := append(bytes.Clone(prefix), mustPack(tuple.Tuple{int64(de.index)})...)
		value := s.Bytes()
		return t.Transact(ctx, func(tr *fdb.Transaction) error {
			tr.Set(key, value)
			return nil
		})
	case "LOG_DIRECTORY":
		prefix := sm.popBytes()
		d, err := de.cwd()
		if err != nil {
			return err
		}
		ls, err := subspace.FromBytes(prefix).Sub(int64(de.index))
		if err != nil {
			return err
		}
		exists, err := d.Exists(ctx, rt, nil)
		if err != nil {
			return err
		}
		var children []string
		if exists {
			if children, err = d.List(ctx, rt, nil); err != nil {
				return err
			}
		}
		kvs := [][2][]byte{
			{mustSubPack(ls, "path"), packStrings(d.Path())},
			{mustSubPack(ls, "layer"), mustPack(tuple.Tuple{bytesOrEmpty(d.Layer())})},
			{mustSubPack(ls, "exists"), mustPack(tuple.Tuple{boolInt(exists)})},
			{mustSubPack(ls, "children"), packStrings(children)},
		}
		return t.Transact(ctx, func(tr *fdb.Transaction) error {
			for _, kv := range kvs {
				tr.Set(kv[0], kv[1])
			}
			return nil
		})
	case "STRIP_PREFIX":
		b := sm.popBytes()
		s, err := de.css()
		if err != nil {
			return err
		}
		p := s.Bytes()
		if !bytes.HasPrefix(b, p) {
			return errDirectory(fmt.Sprintf("%q does not start with raw prefix %q", b, p))
		}
		sm.push(idx, b[len(p):])
	default:
		return errDirectory("unknown directory operation " + op)
	}
	return nil
}

func mustSubPack(s subspace.Subspace, name string) []byte {
	k, err := s.Pack(tuple.Tuple{name})
	if err != nil {
		panic(err)
	}
	return k
}

func bytesOrEmpty(b []byte) []byte {
	if b == nil {
		return []byte{}
	}
	return b
}

func boolInt(b bool) int64 {
	if b {
		return 1
	}
	return 0
}
