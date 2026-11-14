#!/usr/bin/env bash
# Builds fdbbench against libfdb_c (for -client c) and the fdbgo module in ../.
#
#   [FDB_ROOT=<dir with include/ and lib/>] bench/build.sh   -> bench/fdbbench
#
# FDB_ROOT defaults to target/fdb (scripts/fetch-fdb.sh).
# FDB_SOURCE_DIR defaults to $FDB_ROOT/src and must match the installed SDK.
# The official Go wrapper hardcodes its header API version, so build with the
# matching source tree rather than requiring a 7.4 SDK for every benchmark.
set -euo pipefail
R=${FDB_ROOT:-$(cd "$(dirname "$0")/.." && pwd)/target/fdb}
R=$(cd "$R" && pwd)
S=${FDB_SOURCE_DIR:-$R/src}
S=$(cd "$S" && pwd)
cd "$(dirname "$0")"
moddir=$(mktemp -d "$PWD/.fdbbench.XXXXXX")
modfile=$moddir/go.mod
trap 'rm -rf "$moddir"' EXIT
cp go.mod "$modfile"
cp go.sum "${modfile%.mod}.sum"
go mod edit -modfile "$modfile" "-replace=github.com/apple/foundationdb/bindings/go=$S/bindings/go"
CGO_CFLAGS="-I$R/include" CGO_LDFLAGS="-L$R/lib -Wl,-rpath,$R/lib" \
	go build -modfile "$modfile" -o fdbbench . 2> >(grep -v 'duplicate -rpath' >&2)
