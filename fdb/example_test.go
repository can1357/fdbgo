package fdb_test

import (
	"context"
	"fmt"
	"log"

	"github.com/can1357/fdbgo/fdb"
)

func ExampleOpen() {
	// "" resolves FDB_CLUSTER_FILE, ./fdb.cluster, then fdb.DefaultClusterFile().
	db, err := fdb.Open("")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
}

func ExampleDatabase_Transact() {
	ctx := context.Background()
	db, err := fdb.Open("")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// fn may run several times: it must only touch the transaction.
	err = db.Transact(ctx, func(tr *fdb.Transaction) error {
		v, err := tr.Get(ctx, []byte("counter"))
		if err != nil {
			return err
		}
		tr.Set([]byte("counter"), append(v, '+'))
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	// fdb.Transact returns a value from the transaction.
	n, err := fdb.ReadTransact(ctx, db, func(tr fdb.ReadTransaction) (int, error) {
		v, err := tr.Get(ctx, []byte("counter"))
		return len(v), err
	})
	fmt.Println(n, err)
}

func ExampleTransaction_GetRange() {
	ctx := context.Background()
	db, err := fdb.Open("")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	r, err := fdb.PrefixRange([]byte("users/"))
	if err != nil {
		log.Fatal(err)
	}
	err = db.ReadTransact(ctx, func(tr fdb.ReadTransaction) error {
		result, err := tr.GetRange(ctx, r, fdb.RangeOptions{Limit: 100})
		if err != nil {
			return err
		}
		// All fetches subsequent batches as the loop consumes them.
		for kv, err := range result.All(ctx) {
			if err != nil {
				return err
			}
			fmt.Printf("%s = %s\n", fdb.Printable(kv.Key), fdb.Printable(kv.Value))
		}
		// Slice reads everything at once.
		result, err = tr.GetRange(ctx, r, fdb.RangeOptions{Limit: 10, Reverse: true})
		if err != nil {
			return err
		}
		last, err := result.Slice(ctx)
		fmt.Println(len(last))
		return err
	})
	if err != nil {
		log.Fatal(err)
	}
}

func ExampleTransaction_Watch() {
	ctx := context.Background()
	db, err := fdb.Open("")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// The watch outlives its transaction: it is armed when the transaction
	// commits and completes when the value changes afterwards.
	w, err := fdb.Transact(ctx, db, func(tr *fdb.Transaction) (*fdb.Watch, error) {
		return tr.Watch([]byte("config")), nil
	})
	if err != nil {
		log.Fatal(err)
	}
	if err := w.Wait(ctx); err != nil {
		log.Fatal(err)
	}
	fmt.Println("config changed")
}
