#!/usr/bin/env bash
# Run the complete Linux amd64 release matrix, or selected versions:
#   scripts/test-versions.sh                 # every pinned release + adjacent upgrade
#   scripts/test-versions.sh 5.0.8 8.0.0    # selected releases; upgrades still use adjacent series
# Requires Go (go.mod), a C compiler, make, pkg-config, libffi/zlib development
# headers, xz, curl, dpkg-deb, jq and uv. Historical references build pinned Python
# 2.7.18 once in target/tools; modern references use uv Python 3.10.
# On arm64 use Docker Linux amd64 for legacy assets; native macOS arm64 starts at 7.1.
# AVX-enabled releases must execute successfully; emulator /proc/cpuinfo flags
# are not authoritative and must not prevent trying the actual release binaries.
# These tests destroy only the private FDB_RUN directory (default run/ci-<version>).
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
source "$ROOT/scripts/fdb-release.sh"
cd "$ROOT"
if (($# == 0)); then
	# The manifest contains only validated version strings, not shell input.
	set -- $(awk '!/^#/ && NF { print $1 }' scripts/fdb-releases.tsv)
fi

run_version() (
	fdb_release "$1"
	local upgrade=$release_upgrade
	export FDB_VERSION=$release_version FDB_API_VERSION=$release_api FDB_FETCH_SOURCE_ONLY=0 FDB_FETCH_BINARIES_ONLY=0
	export FDB_ROOT=${FDB_ROOT:-$ROOT/target/versions/$release_version}
	export FDB_BIN=$FDB_ROOT/bin FDB_SOURCE_DIR=$FDB_ROOT/src
	export FDB_RUN=${FDB_RUN:-$ROOT/run/ci-$release_version}
	export FDB_CLUSTER_FILE=$FDB_RUN/fdb.cluster
	scripts/fetch-fdb.sh "$release_version"
	trap 'scripts/cluster.sh down' EXIT
	scripts/cluster.sh up 1 memory
	# No core transactions are filtered out. Optional feature gates live in tests,
	# keyed by the actual negotiated server protocol, not by skipping test packages.
	go test -race -count=1 -timeout=5m ./...
	bench/build.sh
	scripts/bindingtester.sh full 1
	if ((release_api == 620)); then
		scripts/cluster.sh down
		FDB_OBJECT_SERIALIZER=0 scripts/cluster.sh up 1 memory
		FDB_OBJECT_SERIALIZER=0 go test -race -count=1 -timeout=5m ./...
	fi
	if [[ $upgrade != - ]]; then
		local next=$ROOT/target/versions/$upgrade
		FDB_ROOT=$next scripts/fetch-fdb.sh "$upgrade"
		FDB_UPGRADE_FROM=$FDB_ROOT FDB_UPGRADE_TO=$next \
			go test -race -count=1 -timeout=5m ./fdb -run '^TestMultiVersionUpgrade$'
	fi
)

for version in "$@"; do run_version "$version"; done
