#!/usr/bin/env bash
# Shared, pinned release metadata for fetch-fdb.sh and local/CI verification.
# Source this file; fdb_release VERSION populates release_* variables.
fdb_release() {
	local manifest row
	manifest=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/fdb-releases.tsv
	row=$(awk -v version="$1" '$1 == version { print; found=1; exit } END { if (!found) exit 1 }' "$manifest") || {
		echo "FoundationDB $1 is not pinned in $manifest" >&2
		return 1
	}
	read -r release_version release_api release_commit release_source_sha release_server_sha \
		release_clients_sha release_mac_x64_sha release_mac_arm64_sha release_upgrade release_linux_build_commit <<< "$row"
	release_linux_build_commit=${release_linux_build_commit:-$release_commit}
	release_source_kind=build-commit
	[[ $release_commit == "$release_linux_build_commit" ]] || release_source_kind=release-tag-unavailable-build-source
}

fdb_sha256() {
	if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | awk '{print $1}'
	else shasum -a 256 "$1" | awk '{print $1}'; fi
}

# Cached downloads are checked as well: interrupted/stale cache entries never count as SDKs.
fdb_download() {
	local url=$1 destination=$2 checksum=$3
	if [[ -f $destination ]]; then
		[[ $(fdb_sha256 "$destination") == "$checksum" ]] && return 0
		echo "checksum mismatch for cached $destination; remove it and retry" >&2
		return 1
	fi
	curl --fail --location --silent --show-error --retry 3 -o "$destination.part" "$url"
	[[ $(fdb_sha256 "$destination.part") == "$checksum" ]] || {
		echo "checksum mismatch for $url" >&2
		rm -f "$destination.part"
		return 1
	}
	mv "$destination.part" "$destination"
}

# Replacing a mapped, signed Mach-O in place can invalidate macOS's cached signature.
fdb_install() {
	local staging
	staging=$(mktemp "$2.XXXXXX")
	cp -p "$1" "$staging"
	chmod 755 "$staging"
	mv -f "$staging" "$2"
}
