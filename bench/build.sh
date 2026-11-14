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
reference=$S/bindings/go
if [[ ! -f $reference/go.mod || ! -f $reference/src/fdb/generated.go ]]; then
	# 5.0–6.0 predate modules. 5.0 also generates its Go options at build time
	# (bindings/go/include.mk). Adapt only the temporary build input, not the
	# pinned source tree or the checked-in bench module.
	mkdir -p "$moddir/reference/src/fdb"
	cp "$reference/src/fdb/"*.go "$moddir/reference/src/fdb/"
	if [[ ! -f $reference/src/fdb/generated.go ]]; then
		go run "$reference/src/_util/translate_fdb_options.go" \
			< "$S/fdbclient/vexillographer/fdb.options" > "$moddir/reference/src/fdb/generated.go"
	fi
	reference=$moddir/reference
	printf 'module github.com/apple/foundationdb/bindings/go\n\ngo 1.27\n' > "$reference/go.mod"
fi
go mod edit -modfile "$modfile" "-replace=github.com/apple/foundationdb/bindings/go=$reference"
CGO_CFLAGS="-I$R/include" CGO_LDFLAGS="-L$R/lib -Wl,-rpath,$R/lib" \
	go build -mod=mod -modfile "$modfile" -o fdbbench . 2> >(grep -v 'duplicate -rpath' >&2)
