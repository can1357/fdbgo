# fdbgo

[![CI](https://github.com/can1357/fdbgo/actions/workflows/ci.yml/badge.svg)](https://github.com/can1357/fdbgo/actions/workflows/ci.yml)

A pure-Go client for FoundationDB 7.4. Connects directly to the cluster—no cgo, `libfdb_c`, or network-thread setup required.

Includes transactions, read-your-writes, snapshot reads, range scans, atomic mutations, watches, TLS, and the tuple, subspace, and directory layers.

## Install

Requires **Go 1.27+** and a **FoundationDB 7.4** cluster. The client implements API version 740.

```sh
go get github.com/can1357/fdbgo@v0.1.1
```

## Quick start

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/can1357/fdbgo/fdb"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	db, err := fdb.Open("")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	value, err := fdb.Transact(ctx, db, func(tr *fdb.Transaction) ([]byte, error) {
		tr.Set([]byte("hello"), []byte("world"))
		return tr.Get(ctx, []byte("hello"))
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(value))
}
```

`Open("")` looks for `FDB_CLUSTER_FILE`, then `./fdb.cluster`, then the platform default. Pass a cluster-file path explicitly or use `OpenConnectionString` instead.

`Transact` commits and retries retryable failures. Its callback may run more than once: keep external side effects outside it. `ReadTransact` provides the same retry handling without committing.

## API essentials

- **Blocking reads:** calls take a `context.Context` and return `(value, error)`. Keys and values are `[]byte`; `nil` means a missing value, while an empty slice represents a present empty value. Returned slices must not be modified.
- **Batch reads:** `tr.GetMany(ctx, keys)` returns values in input order, including duplicate keys. It captures all reads before waiting and returns no partial results on error.
- **Range scans:** `tr.GetRange(ctx, r, opts)` fetches the first batch. Use `Slice(ctx)` to collect rows or `All(ctx)` to iterate, fetching further batches on demand.
- **Snapshots:** `tr.Snapshot()` reads without adding read conflicts and sees the transaction's writes unless snapshot read-your-writes is disabled.
- **Watches:** register with `tr.Watch(key)` before committing, then call `Wait(ctx)` or select on `Done()`. A committed watch survives transaction closure. Call `Cancel()` when abandoning it; cancelling a wait's context does not cancel the watch.
- **Versionstamps:** `tr.Versionstamp()` returns the stamp after a successful commit. Read-only commits have no versionstamp.
- **Errors:** use `errors.Is(err, fdb.ErrNotCommitted)` or `errors.As` with `fdb.Error`.

Close manually created transactions with `Transaction.Close()`. Transaction helpers handle cleanup automatically. A failed or cancelled tracked read prevents that attempt from committing; reset or retry before reusing it. Cancelling a commit cannot undo a request already sent to the cluster.

Transactions support concurrent calls, but callers must synchronize reads and writes when their relative order matters. Range results are not safe for concurrent use.

## Packages

| Package | Purpose |
| --- | --- |
| [`fdb`](https://pkg.go.dev/github.com/can1357/fdbgo/fdb) | Database and transaction APIs, reads, mutations, watches, and options |
| [`tuple`](https://pkg.go.dev/github.com/can1357/fdbgo/tuple) | FoundationDB tuple encoding and versionstamped tuples |
| [`subspace`](https://pkg.go.dev/github.com/can1357/fdbgo/subspace) | Tuple-based key prefixes and ranges |
| [`directory`](https://pkg.go.dev/github.com/can1357/fdbgo/directory) | Directory layer, partitions, and high-contention allocation |

## Performance

Comparison with `libfdb_c` through the official Go bindings on macOS arm64: ten random point reads per transaction, 1 million keys with 100-byte values, and an 8-process single-replica SSD cluster. Both clients used `GOMAXPROCS=4` per process.

The table shows medians across five runs with alternating client order, two seconds of warmup, and twelve seconds of measurement. Concurrency is the total across all client processes.

| Client processes | Concurrency | `libfdb_c` tx/s | fdbgo tx/s | Throughput gain | `libfdb_c` p99 | fdbgo p99 |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 4 | 128 | 40,153 | 43,314 | 7.9% | 6.600 ms | 4.304 ms |
| 4 | 512 | 38,087 | 40,390 | 6.0% | 17.344 ms | 15.344 ms |

fdbgo had higher throughput in all five paired runs at each point. These results describe this workload and configuration, not a universal advantage. The optimized batch path uses cached shard locations and single-replica reads; other cases retain normal routing, retry, and replica-check behavior.

The [`bench`](bench) module and [`scripts/bench.sh`](scripts/bench.sh) provide the comparison harness.

## Limitations

Only FoundationDB 7.4 and API version 740 are supported. Unsupported features include tenants, configuration-database transactions, special-key writes and management modules, blob granules, client trace files, knobs, and the multi-version client.

## Development

```sh
go test -race ./...
go vet ./...
go generate ./fdb

# Include integration tests against a running cluster.
FDB_CLUSTER_FILE=/path/to/fdb.cluster go test -race ./...
```

Integration tests skip when `FDB_CLUSTER_FILE` is unset. CI runs race-enabled tests and vet on Linux and macOS, plus live-cluster tests and comparisons with the official Python binding on Linux.

- [`scripts/fetch-fdb.sh`](scripts/fetch-fdb.sh) downloads FoundationDB 7.4.8 binaries and sources for macOS arm64.
- [`scripts/cluster.sh`](scripts/cluster.sh) manages a local test cluster.
- [`scripts/bindingtester.sh quick`](scripts/bindingtester.sh) runs the reference-binding checks and requires `uv`. It uses a private cluster by default. **These tests clear the database; never use a shared cluster.**

Scripts default to `target/fdb`. Set `FDB_BIN`, `FDB_SOURCE_DIR`, `FDB_CLIENT_LIB`, or `FDB_ROOT` to use an existing installation.

## License

[MIT](LICENSE). Vendored FoundationDB generator inputs in `cmd/fdbgo-gen/` retain their [Apache 2.0 license](cmd/fdbgo-gen/LICENSE.foundationdb) and upstream copyright notices.
