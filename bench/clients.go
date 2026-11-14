package main

// #define FDB_API_VERSION 730
// #include <foundationdb/fdb_c.h>
import "C"

import (
	"context"

	cfdb "github.com/apple/foundationdb/bindings/go/src/fdb"

	"github.com/can1357/fdbgo/fdb"
)

// kv is the client under test. txn reads rkeys in parallel, then writes val to
// every wkey, retrying until it commits; it returns the attempts made.
type kv interface {
	txn(rkeys, wkeys [][]byte, val []byte) (attempts int, err error)
}

func openKV(c config) (kv, error) {
	if c.client == "fdbgo" {
		db, err := fdb.Open(c.cluster)
		return fdbgoKV{db}, err
	}
	// build.sh pairs the Go wrapper with the installed SDK; use the highest
	// runtime API supported by that SDK, capped at the versions we exercise.
	if err := cfdb.APIVersion(min(740, int(C.fdb_get_max_api_version()))); err != nil {
		return nil, err
	}
	db, err := cfdb.OpenDatabase(c.cluster)
	return cgoKV{db}, err
}

// cgoKV drives libfdb_c through the official Go bindings.
type cgoKV struct{ db cfdb.Database }

func (k cgoKV) txn(rkeys, wkeys [][]byte, val []byte) (int, error) {
	attempts := 0
	read := func(rt cfdb.ReadTransaction) error {
		futs := make([]cfdb.FutureByteSlice, len(rkeys))
		for i, key := range rkeys {
			futs[i] = rt.Get(cfdb.Key(key))
		}
		for _, f := range futs {
			if _, err := f.Get(); err != nil {
				return err
			}
		}
		return nil
	}
	var err error
	if len(wkeys) == 0 {
		_, err = k.db.ReadTransact(func(rt cfdb.ReadTransaction) (any, error) {
			attempts++
			return nil, read(rt)
		})
	} else {
		_, err = k.db.Transact(func(tr cfdb.Transaction) (any, error) {
			attempts++
			if err := read(tr); err != nil {
				return nil, err
			}
			for _, key := range wkeys {
				tr.Set(cfdb.Key(key), val)
			}
			return nil, nil
		})
	}
	return attempts, err
}

// fdbgoKV drives the pure-Go client in fdbgo/fdb.
type fdbgoKV struct{ db *fdb.Database }

func (k fdbgoKV) txn(rkeys, wkeys [][]byte, val []byte) (int, error) {
	ctx := context.Background()
	attempts := 0
	read := func(rt fdb.ReadTransaction) error {
		_, err := rt.GetMany(ctx, rkeys)
		return err
	}
	if len(wkeys) == 0 {
		err := k.db.ReadTransact(ctx, func(rt fdb.ReadTransaction) error {
			attempts++
			return read(rt)
		})
		return attempts, err
	}
	err := k.db.Transact(ctx, func(tr *fdb.Transaction) error {
		attempts++
		if err := read(tr); err != nil {
			return err
		}
		for _, key := range wkeys {
			tr.Set(key, val)
		}
		return nil
	})
	return attempts, err
}
