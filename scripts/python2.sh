#!/usr/bin/env bash
# Build the historical reference interpreter, not a translated Python 3 substitute.
# Prints only the absolute interpreter path on stdout; build diagnostics go to stderr.
# Requires a C compiler, make, pkg-config, xz and libffi development headers; no pip2 or OpenSSL.
# FDB_TOOL_ROOT defaults to target/tools; FDB_PYTHON2_JOBS defaults to 2.
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
source "$ROOT/scripts/fdb-release.sh"
TOOLS=${FDB_TOOL_ROOT:-$ROOT/target/tools}
mkdir -p "$TOOLS/dl"
TOOLS=$(cd "$TOOLS" && pwd)
prefix=$TOOLS/python-2.7.18-$(uname -s)-$(uname -m)
checksum=b62c0e7937551d0cc02b8fd5cb0f544f9405bafc9a54d3808ed4594812edef43
archive=$TOOLS/dl/Python-2.7.18.tar.xz
build=
lock_dir=
cleanup() {
	[[ -z $build ]] || rm -rf "$build"
	[[ -z $lock_dir ]] || rmdir "$lock_dir"
}
trap cleanup EXIT
# Linux reference runs may prepare multiple release SDKs concurrently. flock is
# supplied by util-linux on CI/Docker; the directory lock supports macOS hosts.
if command -v flock >/dev/null 2>&1; then
	exec 9>"$prefix.lock"
	flock 9
else
	for ((attempt = 0; attempt < 900; attempt++)); do
		if mkdir "$prefix.lock.d" 2>/dev/null; then lock_dir=$prefix.lock.d; break; fi
		sleep 1
	done
	[[ -n $lock_dir ]] || { echo "timed out waiting for Python 2 build lock: $prefix.lock.d" >&2; exit 1; }
fi
if [[ -x $prefix/bin/python2.7 && -f $prefix/.complete ]] && [[ $(cat "$prefix/.complete") == "$checksum" ]]; then
	printf '%s\n' "$prefix/bin/python2.7"
	exit 0
fi
fdb_download "https://www.python.org/ftp/python/2.7.18/Python-2.7.18.tar.xz" "$archive" "$checksum"
build=$(mktemp -d "$TOOLS/python2-build.XXXXXX")
tar -xJf "$archive" --strip-components=1 -C "$build"
log=$prefix.build.log
echo "==> building Python 2.7.18 in $prefix (log: $log)" >&2
if ! (
	cd "$build" &&
	./configure --prefix="$prefix" --disable-shared --without-ensurepip --with-system-ffi &&
	make -j "${FDB_PYTHON2_JOBS:-2}" &&
	make install &&
	# Optional extension failures do not fail CPython's make target. ctypes is
	# mandatory for the real C reference binding, so never publish a broken cache.
	"$prefix/bin/python2.7" -c 'import ctypes, json; ctypes.CDLL(None)'
) >"$log" 2>&1; then
	cat "$log" >&2
	exit 1
fi
printf '%s\n' "$checksum" >"$prefix/.complete"
printf '%s\n' "$prefix/bin/python2.7"
