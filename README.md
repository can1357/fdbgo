# fdbgo

[![CI](https://github.com/can1357/fdbgo/actions/workflows/ci.yml/badge.svg)](https://github.com/can1357/fdbgo/actions/workflows/ci.yml)

A pure-Go client for FoundationDB 5.0–8.0. Connects directly to the cluster—no cgo, `libfdb_c`, or network-thread setup required.

Includes transactions, read-your-writes, snapshot reads, range scans, atomic mutations, watches, TLS, and the tuple, subspace, and directory layers.

## Install

Requires **Go 1.27+** and a cluster using one of the [supported protocols](#compatibility). Protocol selection is automatic and independent for each connection; one process can use different server releases. No version-selection call is needed. Once connected, version-dependent behavior follows that release's C client; operations unavailable on older servers return `UnsupportedOperation`.

```sh
go get github.com/can1357/fdbgo
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

## Compatibility

The release matrix covers **5.0.8, 5.1.7, 5.2.8, 6.0.18, 6.1.13, 6.2.30, 6.3.25, 7.0.0, 7.1.67, 7.2.9, 7.3.81, 7.4.8, and 8.0.0**. Other patch releases must advertise a matching wire protocol; unknown protocol revisions are rejected.

Each connection selects the matching binary or object codec and packet framing. Releases before 6.2 discover proxies through the cluster controller; 5.0 resolves shards through metadata servers. No C libraries are loaded. The 6.2 binary-serializer configuration is supported alongside its default object serializer.

Historical behavior can change during an upgrade. For example, `ApproximateSize` ignores deferred transaction errors before 7.2 and reports them on newer servers; it never clears the error for a later commit. Before protocol discovery, local validation uses current-client behavior.

Server capabilities still matter:

| Feature | Minimum server release |
|---|---|
| Conflicting-key reporting | 6.3 |
| Range split points | 7.0 |
| Mapped range reads | 7.1 |
| Idempotent commits, cluster ID | 7.2 |
| Full read-options encoding | 7.2 |
| Automatic idempotency expiration | 7.3 |

Versionstamp operands keep the Go API's 32-bit offset trailer. The wire codec translates for 5.0/5.1, where key offsets must fit 16 bits and values must put the stamp at offset zero; historical maximum operand sizes remain usable. MIN and AND follow the connected release's semantics. Unavailable atomic mutations and unrepresentable options fail explicitly rather than silently changing semantics.

Connections renegotiate after server restarts and supported upgrades. Transaction retry and commit-uncertainty rules still apply: use `Transact` and keep callbacks safe to repeat. Follow FoundationDB's server upgrade path; client compatibility does not make skipped-version storage upgrades safe.

The [release manifest](scripts/fdb-releases.tsv) pins binaries and source archives. The original 5.0.8 binary-build source is unavailable upstream; its codec audit uses the published release-tag source, and the manifest records that provenance limitation.

Unsupported features include tenants, configuration-database transactions, special-key writes and management modules, blob granules, client trace files, and knobs. 8.0 is an upstream prerelease.

## Development

```sh
go test -race ./...
go vet ./...
go generate ./fdb

# Include integration tests against a running cluster.
FDB_CLUSTER_FILE=/path/to/fdb.cluster go test -race ./...
```

Integration tests skip when `FDB_CLUSTER_FILE` is unset. CI runs race-enabled tests and vet on Linux and macOS, plus live-cluster tests, matching C-wrapper builds, reference-binding comparisons, and adjacent-release upgrades across the Linux release matrix. Tests for unavailable features verify `UnsupportedOperation`; core transaction tests remain enabled.

Run the complete matrix, or selected releases, with `scripts/test-versions.sh [versions...]`. It needs Linux amd64, Go, a C compiler, `make`, `pkg-config`, libffi/zlib development headers, `xz`, `curl`, `dpkg-deb`, `jq`, and `uv`. Historical reference bindings build a checksum-pinned Python 2.7.18 interpreter; newer references use Python 3.10. These are test dependencies only. Each release gets an isolated cluster. An individual persisted-data upgrade test covers concurrent transactions, watches, and snapshot conflicts while retaining the database handle:

```sh
FDB_UPGRADE_FROM=/path/to/fdb-7.3 FDB_UPGRADE_TO=/path/to/fdb-7.4 \
  go test -race ./fdb -run '^TestMultiVersionUpgrade$'
```

Each upgrade path is an SDK root containing `bin/fdbserver` and `bin/fdbcli`; the test creates its own clusters.

- [`scripts/fetch-fdb.sh`](scripts/fetch-fdb.sh) downloads checksum-pinned binaries and sources (default 7.4.8). Linux amd64 supports the entire matrix; older releases have no native macOS arm64 SDK. Use `FDB_ROOT=target/versions/6.3.25 scripts/fetch-fdb.sh 6.3.25` for an isolated installation.
- [`scripts/cluster.sh`](scripts/cluster.sh) manages a local test cluster.
- [`scripts/bindingtester.sh quick`](scripts/bindingtester.sh) runs the reference-binding checks and requires `uv`. It uses a private cluster by default. **These tests clear the database; never use a shared cluster.**

SDK scripts default to `target/fdb`; the matrix runner uses `target/versions/<release>`. Set `FDB_ROOT` for fetching, reference tests, and benchmark builds; set `FDB_BIN=$FDB_ROOT/bin` for `cluster.sh`. `FDB_SOURCE_DIR` and `FDB_CLIENT_LIB` can override the matching reference sources and library. Reference tests and C benchmarks select the installed SDK's supported API version automatically.

## License

[MIT](LICENSE). Vendored FoundationDB generator inputs in `cmd/fdbgo-gen/` retain their [Apache 2.0 license](cmd/fdbgo-gen/LICENSE.foundationdb) and upstream copyright notices.
