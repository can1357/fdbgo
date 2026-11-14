#!/usr/bin/env bash
# Fetch checksum-pinned SDK, server and available upstream source from fdb-releases.tsv.
# The manifest explicitly records 5.0.8's unavailable binary build-source exception.
#   FDB_ROOT=target/fdb-5.0.8 scripts/fetch-fdb.sh 5.0.8
#   scripts/fetch-fdb.sh [version=7.4.8]
# Linux x86_64 supports every pinned release. macOS arm64 starts at 7.1;
# run older versions in a Linux amd64 Docker container, not as native arm64 binaries.
# FDB_FETCH_SOURCE_ONLY=1 fetches pinned sources without host binaries.
# FDB_FETCH_BINARIES_ONLY=1 preserves an existing, already pinned source tree.
# Output: bin/{fdbserver,fdbcli}, lib/libfdb_c.{so,dylib}, include/foundationdb, src, release.env.
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
source "$ROOT/scripts/fdb-release.sh"
fdb_release "${1:-7.4.8}"
if [[ $release_source_kind != build-commit ]]; then
	echo "WARNING: FoundationDB $release_version binary build source $release_linux_build_commit is unavailable upstream; using published release-tag source $release_commit. Exact source equivalence is unverified." >&2
fi
if [[ ${FDB_FETCH_SOURCE_ONLY:-0} == 1 && ${FDB_FETCH_BINARIES_ONLY:-0} == 1 ]]; then
	echo 'FDB_FETCH_SOURCE_ONLY and FDB_FETCH_BINARIES_ONLY are mutually exclusive.' >&2
	exit 2
fi
OUT=${FDB_ROOT:-$ROOT/target/fdb}
platform="$(uname -s)-$(uname -m)"
if [[ ${FDB_FETCH_SOURCE_ONLY:-0} != 1 ]]; then
	case "$platform" in
		Linux-x86_64) command -v dpkg-deb >/dev/null || { echo 'dpkg-deb is required (install dpkg).' >&2; exit 1; } ;;
		Darwin-arm64)
			[[ $release_mac_arm64_sha != - ]] || {
				echo "FoundationDB $release_version has no native macOS arm64 SDK; use Docker --platform linux/amd64." >&2
				exit 1
			} ;;
		Darwin-x86_64) ;;
		*) echo "Unsupported host $platform; use Docker --platform linux/amd64." >&2; exit 1 ;;
	esac
fi
mkdir -p "$OUT" "$ROOT/target"
OUT=$(cd "$OUT" && pwd)
DL=$OUT/dl
mkdir -p "$DL" "$OUT/bin" "$OUT/lib" "$OUT/include"
# Keep upstream Go source trees out of the repository's ./... package expansion.
[[ -f $ROOT/target/go.mod ]] || printf 'module ignored\n' >"$ROOT/target/go.mod"
base=https://github.com/apple/foundationdb/releases/download/$release_version

if [[ ${FDB_FETCH_SOURCE_ONLY:-0} != 1 ]]; then
	case "$platform" in
		Linux-x86_64)
			clients=foundationdb-clients_${release_version}-1_amd64.deb
			fdb_download "$base/fdbserver.x86_64" "$DL/fdbserver-$release_version.x86_64" "$release_server_sha"
			fdb_download "$base/$clients" "$DL/$clients" "$release_clients_sha"
			sdk=$(mktemp -d "$OUT/sdk.XXXXXX")
			trap 'rm -rf "$sdk"' EXIT
			dpkg-deb --extract "$DL/$clients" "$sdk"
			fdb_install "$DL/fdbserver-$release_version.x86_64" "$OUT/bin/fdbserver"
			fdb_install "$sdk/usr/bin/fdbcli" "$OUT/bin/fdbcli"
			fdb_install "$sdk/usr/lib/libfdb_c.so" "$OUT/lib/libfdb_c.so"
			rm -rf "$OUT/include/foundationdb"
			cp -R "$sdk/usr/include/foundationdb" "$OUT/include/"
			rm -rf "$sdk"
			trap - EXIT ;;
		Darwin-*)
			if [[ $platform == Darwin-arm64 ]]; then
				pkg=FoundationDB-${release_version}_arm64.pkg
				checksum=$release_mac_arm64_sha
			elif ((release_api >= 710)); then
				pkg=FoundationDB-${release_version}_x86_64.pkg
				checksum=$release_mac_x64_sha
			else
				pkg=FoundationDB-${release_version}.pkg
				checksum=$release_mac_x64_sha
			fi
			fdb_download "$base/$pkg" "$DL/$pkg" "$checksum"
			x=$(mktemp -d "$OUT/pkg.XXXXXX")
			rmdir "$x" # pkgutil requires a destination that does not exist.
			trap 'rm -rf "$x"' EXIT
			pkgutil --expand-full "$DL/$pkg" "$x"
			fdb_install "$x/FoundationDB-server.pkg/Payload/usr/local/libexec/fdbserver" "$OUT/bin/fdbserver"
			fdb_install "$x/FoundationDB-clients.pkg/Payload/usr/local/bin/fdbcli" "$OUT/bin/fdbcli"
			fdb_install "$x/FoundationDB-clients.pkg/Payload/usr/local/lib/libfdb_c.dylib" "$OUT/lib/libfdb_c.dylib"
			rm -rf "$OUT/include/foundationdb"
			cp -R "$x/FoundationDB-clients.pkg/Payload/usr/local/include/foundationdb" "$OUT/include/"
			rm -rf "$x"
			trap - EXIT ;;
	esac
fi

if [[ ${FDB_FETCH_BINARIES_ONLY:-0} != 1 ]]; then
	archive=$DL/foundationdb-$release_commit.tar.gz
	fdb_download "https://codeload.github.com/apple/foundationdb/tar.gz/$release_commit" "$archive" "$release_source_sha"
	source_tmp=$(mktemp -d "$OUT/src.XXXXXX")
	trap 'rm -rf "$source_tmp"' EXIT
	tar -xzf "$archive" --strip-components=1 -C "$source_tmp"
	rm -rf "$OUT/src"
	mv "$source_tmp" "$OUT/src"
fi
if [[ ${FDB_FETCH_SOURCE_ONLY:-0} != 1 ]]; then
	printf 'FDB_RELEASE_VERSION=%s\nFDB_RELEASE_API=%s\nFDB_RELEASE_COMMIT=%s\nFDB_RELEASE_SOURCE_KIND=%s\nFDB_RELEASE_LINUX_BUILD_COMMIT=%s\n' \
		"$release_version" "$release_api" "$release_commit" "$release_source_kind" "$release_linux_build_commit" >"$OUT/release.env"
fi
echo "FoundationDB $release_version (API $release_api, $release_source_kind source $release_commit) in $OUT"
