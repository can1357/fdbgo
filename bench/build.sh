#!/usr/bin/env bash
# Builds fdbbench against libfdb_c (for -client c) and the fdbgo module in ../.
#
#   [FDB_ROOT=<dir with include/ and lib/>] bench/build.sh   -> bench/fdbbench
#
# FDB_ROOT defaults to target/fdb (scripts/fetch-fdb.sh).
set -euo pipefail
cd "$(dirname "$0")"
R=${FDB_ROOT:-$(cd .. && pwd)/target/fdb}
CGO_CFLAGS="-I$R/include" CGO_LDFLAGS="-L$R/lib -Wl,-rpath,$R/lib" go build -o fdbbench . 2> >(grep -v 'duplicate -rpath' >&2)
