package fdb

import (
	"context"
	"errors"
	"runtime"

	"github.com/can1357/fdbgo/internal/client"
	"github.com/can1357/fdbgo/internal/cluster"
	"github.com/can1357/fdbgo/internal/transport"
)

// Database is a handle to one cluster, shared by all its transactions and
// safe for concurrent use. Close it to stop its background goroutines.
type Database struct {
	db *client.Db
}

// Open opens the cluster described by a cluster file. An empty path resolves
// FDB_CLUSTER_FILE, then ./fdb.cluster, then DefaultClusterFile. Open does
// not wait for the cluster; the first operation does.
func Open(clusterFile string) (*Database, error) {
	db, err := client.Open(clusterFile)
	if err != nil {
		return nil, err
	}
	return &Database{db}, nil
}

// OpenConnectionString opens a cluster from a connection string
// ("description:id@host:port,...") without a cluster file.
func OpenConnectionString(connectionString string) (*Database, error) {
	db, err := client.OpenConnectionString(connectionString)
	if err != nil {
		return nil, err
	}
	return &Database{db}, nil
}

// DefaultClusterFile returns the platform's default cluster file path.
func DefaultClusterFile() string { return cluster.DefaultClusterFile() }

// Close stops monitoring the cluster and closes its connections. Operations
// still in flight fail.
func (d *Database) Close() error {
	d.db.Close()
	return nil
}

// CreateTransaction returns a new transaction. Most callers want Transact.
func (d *Database) CreateTransaction() (*Transaction, error) {
	t, err := d.db.CreateTransaction()
	if err != nil {
		return nil, err
	}
	tr := &Transaction{t: t, db: d}
	tr.cleanup = runtime.AddCleanup(tr, (*client.Txn).Close, t)
	return tr, nil
}

// Options returns the setters of this database's options.
func (d *Database) Options() DatabaseOptions { return DatabaseOptions{d} }

// Transact runs fn in a new transaction and commits it, retrying fn when fn
// or the commit fails with a retryable Error (Transaction.OnError decides
// and backs off). Errors that are not an Error abort without retry. fn may
// run more than once, so it must not have side effects outside the
// transaction. If ctx ends, the transaction is cancelled and ctx.Err() is
// returned.
func (d *Database) Transact(ctx context.Context, fn func(*Transaction) error) error {
	return d.retry(ctx, func(tr *Transaction) error {
		if err := fn(tr); err != nil {
			return err
		}
		return tr.Commit(ctx)
	})
}

// ReadTransact runs fn in a new transaction without committing, retrying
// like Transact.
func (d *Database) ReadTransact(ctx context.Context, fn func(ReadTransaction) error) error {
	return d.retry(ctx, func(tr *Transaction) error { return fn(tr) })
}

// retry is the retry loop of Transact and ReadTransact: attempt, then OnError
// until attempt succeeds or fails with an error OnError does not retry. The
// transaction is released at the end, like a dropped FDBTransaction.
func (d *Database) retry(ctx context.Context, attempt func(*Transaction) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	tr, err := d.CreateTransaction()
	if err != nil {
		return err
	}
	defer tr.Close()
	if ctx.Done() != nil {
		stop := context.AfterFunc(ctx, tr.t.Cancel)
		defer stop()
	}
	for {
		err := attempt(tr)
		if err == nil {
			return nil
		}
		if cerr := ctx.Err(); cerr != nil {
			return cerr
		}
		var fe Error
		if !errors.As(err, &fe) {
			return err
		}
		if err := tr.OnError(ctx, fe); err != nil {
			if cerr := ctx.Err(); cerr != nil {
				return cerr
			}
			return err
		}
	}
}

// ClientStatus returns the client's status document as JSON
// (fdb_database_get_client_status).
func (d *Database) ClientStatus(ctx context.Context) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	v, wait, err := d.db.ClientStatus()
	if wait != nil {
		v, err = wait(ctx)
	}
	return v, operationError(ctx, err)
}

// MainThreadBusyness reports how busy the client's network loop has been,
// from 0 to 1 (fdb_database_get_main_thread_busyness).
func (d *Database) MainThreadBusyness() float64 { return d.db.MainThreadBusyness() }

// keyServersPrefix is the system key prefix of the shard-to-storage-server map;
// its keys are the shard boundaries.
const keyServersPrefix = "\xff/keyServers/"

// BoundaryKeys returns the shard boundaries inside r as of readVersion (0
// means the latest), at most limit of them (0 means no limit).
func (d *Database) BoundaryKeys(ctx context.Context, r ExactRange, limit int, readVersion int64) ([][]byte, error) {
	tr, err := d.CreateTransaction()
	if err != nil {
		return nil, err
	}
	defer tr.Close()
	if readVersion != 0 {
		tr.SetReadVersion(readVersion)
	}
	if err := tr.Options().SetReadSystemKeys(); err != nil {
		return nil, err
	}
	if err := tr.Options().SetLockAware(); err != nil {
		return nil, err
	}
	b, e := r.FDBRangeKeys()
	kr := KeyRange{
		Begin: append([]byte(keyServersPrefix), b...),
		End:   append([]byte(keyServersPrefix), e...),
	}
	result, err := tr.Snapshot().GetRange(ctx, kr, RangeOptions{Limit: limit})
	if err != nil {
		return nil, err
	}
	kvs, err := result.Slice(ctx)
	if err != nil {
		return nil, err
	}
	if limit != 0 && limit < len(kvs) {
		kvs = kvs[:limit]
	}
	out := make([][]byte, len(kvs))
	for i := range kvs {
		out[i] = kvs[i].Key[len(keyServersPrefix):]
	}
	return out, nil
}

func (d *Database) setOption(code int, param []byte) error { return d.db.SetOption(code, param) }

// Transactor runs functions in a transaction: a Database retries and
// commits, a Transaction runs fn once inside itself. Layers take a
// Transactor so callers can compose them into a larger transaction.
type Transactor interface {
	Transact(ctx context.Context, fn func(*Transaction) error) error
	ReadTransactor
}

// ReadTransactor runs read-only functions in a transaction.
type ReadTransactor interface {
	ReadTransact(ctx context.Context, fn func(ReadTransaction) error) error
}

// Transact is Transactor.Transact returning a value.
func Transact[T any](ctx context.Context, t Transactor, fn func(*Transaction) (T, error)) (T, error) {
	var out T
	err := t.Transact(ctx, func(tr *Transaction) error {
		v, err := fn(tr)
		out = v
		return err
	})
	return out, err
}

// setNetworkOption applies a process-wide option (fdb_network_set_option).
func setNetworkOption(code int, param []byte) error { return transport.SetNetworkOption(code, param) }

// int64Param encodes an Int option parameter as the C API expects: 8 bytes,
// little-endian.
func int64Param(v int64) []byte {
	return []byte{byte(v), byte(v >> 8), byte(v >> 16), byte(v >> 24), byte(v >> 32), byte(v >> 40), byte(v >> 48), byte(v >> 56)}
}

// ReadTransact is ReadTransactor.ReadTransact returning a value.
func ReadTransact[T any](ctx context.Context, t ReadTransactor, fn func(ReadTransaction) (T, error)) (T, error) {
	var out T
	err := t.ReadTransact(ctx, func(tr ReadTransaction) error {
		v, err := fn(tr)
		out = v
		return err
	})
	return out, err
}
