#!/usr/bin/env bash
# Fetches the FoundationDB release the scripts run against into target/fdb (macOS arm64):
#
#   target/fdb/bin/{fdbserver,fdbcli}   cluster.sh
#   target/fdb/lib/libfdb_c.dylib       bench (-client c), bindingtester's python reference
#   target/fdb/include/foundationdb/    bench cgo build
#   target/fdb/src                      FoundationDB sources at the same tag (bindingtester)
#
#   scripts/fetch-fdb.sh [version=7.4.8]
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
V=${1:-7.4.8}
OUT=$ROOT/target/fdb
DL=$OUT/dl
mkdir -p "$DL" "$OUT/bin" "$OUT/lib" "$OUT/include"
# A go.mod makes target/ its own module, so ./... skips the Go files in the FoundationDB sources.
[[ -f $ROOT/target/go.mod ]] || printf 'module ignored\n' >"$ROOT/target/go.mod"

pkg=FoundationDB-${V}_arm64.pkg
url=https://github.com/apple/foundationdb/releases/download/$V/$pkg
if [[ ! -f $DL/$pkg ]]; then
	curl -fsSL -o "$DL/$pkg.part" "$url"
	want=$(curl -fsSL "$url.sha256" | awk '{print $1}')
	got=$(shasum -a 256 "$DL/$pkg.part" | awk '{print $1}')
	[[ $want == "$got" ]] || { echo "checksum mismatch for $pkg" >&2; exit 1; }
	mv "$DL/$pkg.part" "$DL/$pkg"
fi
rm -rf "$DL/x"
pkgutil --expand-full "$DL/$pkg" "$DL/x"
cp "$DL/x/FoundationDB-server.pkg/Payload/usr/local/libexec/fdbserver" "$OUT/bin/"
cp "$DL/x/FoundationDB-clients.pkg/Payload/usr/local/bin/fdbcli" "$OUT/bin/"
cp "$DL/x/FoundationDB-clients.pkg/Payload/usr/local/lib/libfdb_c.dylib" "$OUT/lib/"
rm -rf "$OUT/include/foundationdb"
cp -R "$DL/x/FoundationDB-clients.pkg/Payload/usr/local/include/foundationdb" "$OUT/include/"
rm -rf "$DL/x"

if [[ ! -d $OUT/src/.git ]]; then
	rm -rf "$OUT/src"
	git clone -q --depth 1 --branch "$V" https://github.com/apple/foundationdb.git "$OUT/src"
fi
echo "FoundationDB $V in $OUT"
