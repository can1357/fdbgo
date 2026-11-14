// Package client is the engine behind the public fdb package, with the
// semantics of the FDB C API: read-your-writes transactions over native
// transactions, plus the special key space.
//
// Db behaves like FDBDatabase and Txn like FDBTransaction: every fdb_database_*
// and fdb_transaction_* call has a method here with the same behavior, errors
// included, so the public layer is a thin typed wrapper. Operations are issued
// at the call site; callers start any returned wait closure immediately.
//
// A Txn owns one attempt at a time: the read-your-writes state (ryw.Ryw,
// which also holds the attempt's reset signal, deferred error and pending-read
// tracking) and the native transaction (native.NativeTxn).
package client

import (
	"context"
	"encoding/json"
	"math"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/can1357/fdbgo/internal/cluster"
	"github.com/can1357/fdbgo/internal/fdberr"
	"github.com/can1357/fdbgo/internal/native"
	"github.com/can1357/fdbgo/internal/options"
	"github.com/can1357/fdbgo/internal/transport"
	"github.com/can1357/fdbgo/internal/wire"
)

// absoluteMaxWatches is the largest accepted max_watches setting.
const absoluteMaxWatches = 1_000_000

// Db is a database handle (FDBDatabase), shared by all its transactions.
type Db struct {
	ctx *native.DbContext

	mu   sync.Mutex
	opts dbOptions
}

// dbOptions are the database options that shape new transactions.
type dbOptions struct {
	// defaults is transactionDefaults: options every new transaction starts with
	// (persisting across resets).
	defaults []txOption
	// snapshotRYW is snapshotRywEnabled: positive when snapshot reads see the
	// transaction's writes.
	snapshotRYW int32
	// useConfigDatabase sends transactions to the configuration database.
	useConfigDatabase bool
}

// Open is fdb_create_database: opens the cluster file at path, or the default
// one (FDB_CLUSTER_FILE, ./fdb.cluster, then the platform default path) when
// path is empty. It connects lazily in the background.
//
// Errors: no_cluster_file_found (1515), connection_string_invalid (2104),
// platform_error (1500) on I/O failures.
func Open(path string) (*Db, error) {
	record, err := cluster.Open(path)
	if err != nil {
		return nil, err
	}
	return withRecord(*record), nil
}

// OpenConnectionString opens a database from a connection string
// (desc:id@addr,...) rather than a cluster file.
func OpenConnectionString(conn string) (*Db, error) {
	cs, err := cluster.ParseConnectionString(conn)
	if err != nil {
		return nil, err
	}
	return withRecord(cluster.ConnectionRecord{Conn: cs}), nil
}

func withRecord(record cluster.ConnectionRecord) *Db {
	transport.StartBusynessSampler()
	return &Db{
		ctx:  native.NewDbContext(record, transport.TLSEnabled()),
		opts: dbOptions{snapshotRYW: 1},
	}
}

// Close stops the database's background work, fails operations in flight and
// closes its connections.
func (d *Db) Close() { d.ctx.Close() }

// SetOption is fdb_database_set_option by raw code.
func (d *Db) SetOption(code int, param []byte) error {
	o, err := decodeDbOption(code, param)
	if err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if tcode, ok := dbDefaultFor[code]; ok {
		topt, err := transactionDefault(o, tcode)
		if err != nil {
			return err
		}
		d.opts.defaults = slices.DeleteFunc(d.opts.defaults, func(x txOption) bool { return x.code == tcode })
		d.opts.defaults = append(d.opts.defaults, topt)
		return nil
	}
	switch code {
	case options.DatabaseLocationCacheSize:
		if o.i < 0 || o.i > math.MaxInt32 {
			return fdberr.InvalidOptionValue
		}
		d.ctx.Locations.SetCapacity(int(o.i))
	case options.DatabaseMaxWatches:
		if o.i < 0 || o.i > absoluteMaxWatches {
			return fdberr.InvalidOptionValue
		}
		d.ctx.Watches.SetMax(o.i)
	case options.DatabaseMachineID, options.DatabaseDatacenterID:
		d.ctx.SetLocality(code, o.s)
	case options.DatabaseSnapshotRYWEnable:
		d.opts.snapshotRYW++
	case options.DatabaseSnapshotRYWDisable:
		d.opts.snapshotRYW--
	case options.DatabaseUseConfigDatabase:
		d.opts.useConfigDatabase = true
	case options.DatabaseTestCausalReadRisky:
		if o.i < 0 || o.i > 100 {
			return fdberr.InvalidOptionValue
		}
		d.ctx.SetCausalReadVerification(o.i)
	default:
		return fdberr.InvalidOption
	}
	return nil
}

// snapshotOptions returns a copy of the options new transactions start from.
func (d *Db) snapshotOptions() dbOptions {
	d.mu.Lock()
	defer d.mu.Unlock()
	o := d.opts
	o.defaults = slices.Clone(o.defaults)
	return o
}

// snapshotRYWEnabled reads the current counter without copying transaction
// defaults; OnError preserves its existing persistent options.
func (d *Db) snapshotRYWEnabled() int32 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.opts.snapshotRYW
}

// CreateTransaction is fdb_database_create_transaction.
func (d *Db) CreateTransaction() (*Txn, error) {
	o := d.snapshotOptions()
	if o.useConfigDatabase {
		// Configuration-database transactions use a different protocol.
		return nil, fdberr.UnsupportedOperation
	}
	return newTxn(d, &o), nil
}

// MainThreadBusyness is fdb_database_get_main_thread_busyness: the fraction of
// the last second the client's network work kept a goroutine busy, in [0, 1].
func (d *Db) MainThreadBusyness() float64 { return transport.Busyness() }

// ClientStatus is fdb_database_get_client_status: JSON describing the client's
// view of the cluster.
func (d *Db) ClientStatus() ([]byte, func(context.Context) ([]byte, error), error) {
	v, err := clientStatusJSON(d.ctx)
	return v, nil, err
}

// clientStatusJSON builds the client status document with sorted keys.
func clientStatusJSON(ctx *native.DbContext) ([]byte, error) {
	obj := map[string]any{}
	conn := ctx.ConnectionString()
	coordinators := make([]string, 0, len(conn.Coordinators))
	addresses := make(map[wire.NetAddr]struct{})
	for _, c := range conn.Coordinators {
		coordinators = append(coordinators, c.String())
		if !c.IsHostname() {
			addresses[wire.NetAddr{Addr: c.Addr, TLS: c.TLS}] = struct{}{}
		}
	}
	healthy := len(coordinators) > 0
	obj["Coordinators"] = coordinators
	if c, ok := ctx.Coordinator(); ok {
		obj["CurrentCoordinator"] = c
	} else {
		healthy = false
	}
	info := ctx.ClusterNow()
	obj["ClusterID"] = info.ClusterID.String()
	tls := ctx.TLS
	grv := make([]string, 0, len(info.GRV))
	for _, e := range info.GRV {
		addr := e.Address(tls)
		grv = append(grv, addr.String())
		addresses[addr] = struct{}{}
	}
	commit := make([]string, 0, len(info.Commit))
	for _, e := range info.Commit {
		addr := e.Address(tls)
		commit = append(commit, addr.String())
		addresses[addr] = struct{}{}
	}
	if len(grv) == 0 || len(commit) == 0 {
		healthy = false
	}
	obj["GrvProxies"] = grv
	obj["CommitProxies"] = commit
	servers := map[string]string{}
	for _, s := range ctx.KnownStorageServers() {
		addr := s.Base.Address(tls)
		servers[s.ID.String()] = addr.String()
		addresses[addr] = struct{}{}
	}
	ids := make([]string, 0, len(servers))
	for id := range servers {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	storage := make([]map[string]string, 0, len(ids))
	for _, id := range ids {
		storage = append(storage, map[string]string{"SSID": id, "Address": servers[id]})
	}
	obj["StorageServers"] = storage
	failed := 0
	connections := connectionStatus(ctx, addresses)
	for _, c := range connections {
		if c["Status"] == "failed" {
			failed++
		}
	}
	obj["Connections"] = connections
	obj["NumConnectionsFailed"] = failed
	if failed > 0 {
		healthy = false
	}
	obj["Healthy"] = healthy
	return json.Marshal(obj)
}

// connectionStatus reports only current topology addresses. Peer state is
// optional: an address need not have been contacted yet, and an old peer need
// not still belong to the cluster.
func connectionStatus(ctx *native.DbContext, addresses map[wire.NetAddr]struct{}) []map[string]any {
	peers := make(map[wire.NetAddr]native.PeerStatus)
	for _, p := range ctx.Peers.Status() {
		peers[p.Addr] = p
	}
	ordered := make([]wire.NetAddr, 0, len(addresses))
	for addr := range addresses {
		ordered = append(ordered, addr)
	}
	slices.SortFunc(ordered, func(a, b wire.NetAddr) int {
		return strings.Compare(a.String(), b.String())
	})
	connections := make([]map[string]any, 0, len(ordered))
	for _, addr := range ordered {
		p, known := peers[addr]
		c := map[string]any{"Address": addr.String()}
		switch {
		case ctx.Failures.AddressIsFailed(addr):
			c["Status"] = "failed"
		case p.Connection != nil:
			c["Status"] = "connected"
		case known:
			c["Status"] = "connecting"
		default:
			c["Status"] = "disconnected"
		}
		if known {
			c["ConnectFailedCount"] = p.ConnectFailures
		}
		if p.Connection != nil {
			rx, tx, age := p.Connection.Stats()
			c["Compatible"] = true
			c["LastConnectTime"] = age.Seconds()
			c["BytesReceived"] = rx
			c["BytesSent"] = tx
			c["ProtocolVersion"] = strconv.FormatUint(transport.ProtocolVersion, 16)
		}
		connections = append(connections, c)
	}
	return connections
}
