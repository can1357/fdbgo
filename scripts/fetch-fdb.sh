#!/usr/bin/env bash
# Fetches a FoundationDB release into FDB_ROOT (default target/fdb; macOS arm64):
#
#   $FDB_ROOT/bin/{fdbserver,fdbcli}   cluster.sh (set FDB_BIN=$FDB_ROOT/bin)
#   $FDB_ROOT/lib/libfdb_c.dylib      bench (-client c), bindingtester's python reference
#   $FDB_ROOT/include/foundationdb/  bench cgo build
#   $FDB_ROOT/src                    matching source archive (bindings)
#
#   FDB_ROOT=target/fdb-7.3.81 scripts/fetch-fdb.sh 7.3.81
#   scripts/fetch-fdb.sh [version=7.4.8]
# Use distinct roots for releases installed side by side.
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
V=${1:-7.4.8}
OUT=${FDB_ROOT:-$ROOT/target/fdb}
mkdir -p "$OUT"
OUT=$(cd "$OUT" && pwd)
DL=$OUT/dl
mkdir -p "$DL" "$OUT/bin" "$OUT/lib" "$OUT/include" "$ROOT/target"
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
# Replacing a signed Mach-O in place can invalidate macOS's cached signature,
# including for processes still mapping it. Publish a fresh inode instead.
install_binary() {
	local staging
	staging=$(mktemp "$2.XXXXXX")
	cp -p "$1" "$staging"
	mv -f "$staging" "$2"
}
install_binary "$DL/x/FoundationDB-server.pkg/Payload/usr/local/libexec/fdbserver" "$OUT/bin/fdbserver"
install_binary "$DL/x/FoundationDB-clients.pkg/Payload/usr/local/bin/fdbcli" "$OUT/bin/fdbcli"
install_binary "$DL/x/FoundationDB-clients.pkg/Payload/usr/local/lib/libfdb_c.dylib" "$OUT/lib/libfdb_c.dylib"
rm -rf "$OUT/include/foundationdb"
cp -R "$DL/x/FoundationDB-clients.pkg/Payload/usr/local/include/foundationdb" "$OUT/include/"
rm -rf "$DL/x"

# Commit pins resolved from the annotated release tags through GitHub's API.
case "$V" in
	7.3.81) source_ref=d40f65d20e4ac102c788e9bdec4160c1c1306dd3 ;;
	7.4.8) source_ref=3ce230b696166af33e55aae95c54a185a761981e ;;
	*) source_ref=refs/tags/$V ;;
esac
archive=$DL/foundationdb-$V.tar.gz
if [[ ! -f $archive ]]; then
	curl -fsSL -o "$archive.part" "https://codeload.github.com/apple/foundationdb/tar.gz/$source_ref"
	mv "$archive.part" "$archive"
fi
# Never retain sources from a different release previously installed at this root.
source_tmp=$(mktemp -d "$OUT/src.XXXXXX")
trap 'rm -rf "$source_tmp"' EXIT
tar -xzf "$archive" --strip-components=1 -C "$source_tmp"
rm -rf "$OUT/src"
mv "$source_tmp" "$OUT/src"
echo "FoundationDB $V in $OUT"
