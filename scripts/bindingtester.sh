#!/usr/bin/env bash
# Runs the FoundationDB bindingtester against the fdbgo stack machine (cmd/stacktester).
#
# Usage:
#   scripts/bindingtester.sh setup          matching Python runtime + patched tester sources only
#   scripts/bindingtester.sh [quick]        scripted, api/directory vs python, directory_hca
#   scripts/bindingtester.sh full [N]       known-faulty seeds, then N random iterations (default 1)
#   scripts/bindingtester.sh <args...>      passes <args...> to bindingtester.py
#
# bindingtester.py clears the whole database before every test, so the default is a private
# cluster (run/bt-<client-version>, a per-API port) started on demand
# with scripts/cluster.sh; never point FDB_CLUSTER_FILE at a shared cluster.
#
# Environment:
#   FDB_ROOT          installation root (default: <repo>/target/fdb); also holds tester work files
#   FDB_CLUSTER_FILE  cluster file (default: <repo>/run/bt-<client-version>/fdb.cluster)
#   FDB_API_VERSION   reference API version (default: matching pinned SDK's maximum)
#   FDB_BT_PORT       private cluster port (default: 4000 + maximum API version)
#   FDB_BT_PYTHON     modern/tooling uv Python version (default: 3.10.19)
#   FDB_TOOL_ROOT     cached Python 2.7.18 build root (default: target/tools; API <=610)
#   FDB_PYTHON2_JOBS  historical interpreter build parallelism (default: 2)
#   FDB_SOURCE_DIR    matching FoundationDB source tree (default: $FDB_ROOT/src; read-only, copied)
#   FDB_CLIENT_LIB    libfdb_c used by the python reference binding
#   FDB_CLI           fdbcli used to probe the cluster and enable tenant_mode
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
fdb_root=${FDB_ROOT:-$root/target/fdb}
fdb_root=$(cd "$fdb_root" && pwd)
fdb_source_dir=${FDB_SOURCE_DIR:-$fdb_root/src}
work_dir=$fdb_root/bindingtester
venv=$work_dir/venv
runtime_bin=$work_dir/runtime-bin
src=$work_dir/src
tester_bin=$work_dir/stacktester
# A go.mod makes target/ its own module, so ./... skips the Go files of the copied bindings.
mkdir -p "$root/target"
[[ -f $root/target/go.mod ]] || printf 'module ignored\n' >"$root/target/go.mod"
api_version=${FDB_API_VERSION:-}
source "$root/scripts/fdb-release.sh"
directory_flags=()

case "$(uname -s)" in
  Darwin) capi_name=libfdb_c.dylib ;;
  *) capi_name=libfdb_c.so ;;
esac
fdb_client_lib=${FDB_CLIENT_LIB:-$fdb_root/lib/$capi_name}
fdb_cli=${FDB_CLI:-$fdb_root/bin/fdbcli}

log() { printf '==> %s\n' "$*" >&2; }

# Python 3.10 handles SDK discovery and options generation without importing the
# historical binding. API <=610 must actually execute on Python 2.7: those
# upstream sources contain Python-2-only syntax, not merely deprecated APIs.
# API >=620 stays on 3.10 (inspect.getargspec/asyncio.coroutine disappear in 3.11).
setup_python() {
  [[ -f "$fdb_client_lib" ]] || { log "libfdb_c not found at $fdb_client_lib"; exit 1; }
  log "creating venv $venv"
  uv venv --quiet --clear --python "${FDB_BT_PYTHON:-3.10.19}" "$venv"

  local client_info
  client_info=$("$venv/bin/python" - "$fdb_client_lib" "$api_version" <<'EOF'
import ctypes, sys
lib = ctypes.CDLL(sys.argv[1])
maximum = lib.fdb_get_max_api_version()
api = int(sys.argv[2]) if sys.argv[2] else maximum
if not 23 <= api <= maximum:
    sys.exit(f"FDB_API_VERSION must be between 23 and {maximum}, got {api}")
# fdb_get_client_version dereferences the client API, which exists only after API selection.
if lib.fdb_select_api_version_impl(api, api) != 0:
    sys.exit(f"fdb_select_api_version failed for API {api}")
lib.fdb_get_client_version.restype = ctypes.c_char_p
print(lib.fdb_get_client_version().decode().split(",")[0], api, maximum)
EOF
)
  read -r fdb_version api_version max_api_version <<< "$client_info"
  fdb_release "$fdb_version"
  [[ $max_api_version == "$release_api" ]] || { log "SDK API $max_api_version does not match pinned API $release_api"; exit 1; }
  local source_api
  source_api=$(awk '/define FDB_API_VERSION / { print $NF; exit }' "$fdb_source_dir/bindings/go/src/fdb/fdb.go")
  [[ $source_api == "$release_api" ]] || { log "source API $source_api does not match SDK API $release_api"; exit 1; }
  runtime_python=$venv/bin/python
  if ((max_api_version <= 620)); then
    runtime_python=$("$root/scripts/python2.sh")
  fi
  # known_testers.py invokes `python` for the reference subprocess. Its runtime
  # must match the bindingtester driver, never whichever Python is on the host.
  mkdir -p "$runtime_bin"
  ln -sf "$runtime_python" "$runtime_bin/python"
  log "libfdb_c version $fdb_version, API $api_version, reference $runtime_python"
}

# Fresh copy of bindings/{bindingtester,python} with the generated python options, the local
# libfdb_c and the fdbgo tester registered.
setup_sources() {
  log "copying bindingtester sources to $src"
  rm -rf "$src"
  mkdir -p "$src/bindings"
  cp -R "$fdb_source_dir/bindings/bindingtester" "$fdb_source_dir/bindings/python" "$src/bindings/"
  chmod -R u+w "$src"

  local fdb_pkg=$src/bindings/python/fdb
  # Same scope/option tuples and hidden-option filter as upstream
  # fdbclient/vexillographer/python.cs, for every pinned release.
  "$venv/bin/python" - "$fdb_source_dir/fdbclient/vexillographer/fdb.options" "$fdb_pkg/fdboptions.py" \
    "$fdb_source_dir/fdbclient/vexillographer/vexillographer.cs" <<'EOF'
import re
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
types = {None: "type(None)", "Int": "type(0)", "String": "type('')", "Bytes": "type(b'')"}
scopes = {scope.attrib["name"]: scope.findall("Option")
          for scope in ET.parse(sys.argv[1]).getroot().findall("Scope")}
# python.cs iterates the Scope enum, not only populated XML elements.
enum = re.search(r"public enum Scope\s*\{([^}]*)\}", Path(sys.argv[3]).read_text())
if enum is None:
    raise SystemExit("cannot locate the pinned generator's Scope enum")
with open(sys.argv[2], "w") as out:
    out.write("# -*- coding: utf-8 -*-\n")
    for name in enum.group(1).split(","):
        name = name.strip()
        out.write(name + " = {\n")
        for option in scopes.get(name, []):
            a = option.attrib
            if a.get("hidden", "false").lower() == "true":
                continue
            out.write("    %r: (%s, %r, %s, %r),\n" % (
                a["name"], a["code"], a.get("description", ""),
                types[a.get("paramType")], a.get("paramDescription")))
        out.write("}\n\n")
EOF
  printf 'LATEST_API_VERSION = %s\nFDB_VERSION = "%s"\n' "$max_api_version" "$fdb_version" \
    > "$fdb_pkg/apiversion.py"
  # fdb/impl.py loads the C library from `<capi_name>.pth` next to the module when present.
  printf '%s\n' "$fdb_client_lib" > "$fdb_pkg/$capi_name.pth"

  # Tenants are not supported by this binding (tenants_enabled defaults to False).
  # 5.0 has neither ALL_TYPES nor Tester's types keyword; 5.1 adds both.
  echo "testers['fdbgo'] = Tester('fdbgo', '$tester_bin', 2040, 23, $api_version, **({'types': ALL_TYPES} if 'ALL_TYPES' in globals() else {}))" \
    >> "$src/bindings/bindingtester/known_testers.py"
  # The snapshot-operation switch first appears in the 5.2 tester.
  if grep -q -- '--no-directory-snapshot-ops' "$src/bindings/bindingtester/bindingtester.py"; then
    directory_flags=(--no-directory-snapshot-ops)
  fi
}

cli() { "$fdb_cli" -C "$cluster_file" --timeout 30 --exec "$*"; }

# Starts the private cluster when the default cluster file is used and it is not answering.
# On 7.2–7.4, bindingtester clears the tenant map even when tenant tests are disabled.
setup_cluster() {
  if [[ "$cluster_file" == "$default_cluster_file" ]] &&
    ! { [[ -f "$cluster_file" ]] && "$fdb_cli" -C "$cluster_file" --timeout 5 --exec "status minimal" 2>/dev/null |
      grep -q "available"; }; then
    log "starting private cluster in $bt_run (port $bt_port)"
    FDB_BIN="$fdb_root/bin" FDB_PORT=$bt_port FDB_RUN=$bt_run "$root/scripts/cluster.sh" up 1 memory
  fi
  # Only the 7.2–7.4 testers clear the management tenant map. Older releases
  # have no tenant_mode; 8.0 removes tenants and aborts in the tenant C stubs.
  ((max_api_version >= 720 && max_api_version < 800)) || return 0
  local tenant_mode
  tenant_mode=$(cli "status json" |
    "$venv/bin/python" -c 'import json, sys; print(json.load(sys.stdin)["cluster"]["configuration"]["tenant_mode"])')
  if [[ "$tenant_mode" == disabled ]]; then
    log "enabling tenant_mode=optional_experimental on $cluster_file"
    cli "configure tenant_mode=optional_experimental"
  fi
}

build_tester() {
  log "building $tester_bin"
  (cd "$root" && go build -o "$tester_bin" ./cmd/stacktester)
}

# Both the driver and `python` reference subprocess use the matching interpreter.
# PYTHONPATH selects only the release's copied Python binding and generated options.
bt() {
  log "bindingtester.py $*"
  (
    cd "$src"
    PATH="$runtime_bin:$venv/bin:$PATH" PYTHONPATH="$src/bindings/python" \
      "$runtime_python" bindings/bindingtester/bindingtester.py \
      --cluster-file "$cluster_file" "$@"
  )
}

run_quick() {
  bt --api-version "$api_version" --test-name scripted fdbgo
  bt --num-ops 1000 --api-version $api_version --test-name api --compare python fdbgo
  bt --num-ops 1000 --api-version $api_version --test-name directory --concurrency 1 fdbgo --compare python
  bt --num-ops 100 --api-version $api_version --test-name directory_hca --concurrency 1 fdbgo "${directory_flags[@]}"
}

run_faulty_seeds() {
  # Seeds that exposed past regressions, with the flags they were found with.
  bt --num-ops 1000 --api-version $api_version --test-name api --compare python fdbgo --seed 3534790651
  bt --num-ops 1000 --api-version $api_version --test-name api --compare python fdbgo --seed 3864917676
  bt --num-ops 1000 --api-version $api_version --test-name api --concurrency 5 fdbgo --seed 3153055325
  local seed
  for seed in 2095856910 2483220251 241550211 1508488514 4203526853 \
    4142326254 4275610547 1951034301 1700644945 584458794; do
    bt --num-ops 1000 --api-version $api_version --test-name directory --concurrency 1 fdbgo "${directory_flags[@]}" --compare python --seed "$seed"
  done
  # Database-level directory operations must retry concurrent commit conflicts.
  local hca_api=$api_version
  ((hca_api <= 610)) || hca_api=610
  bt --num-ops 100 --api-version "$hca_api" --test-name directory_hca --concurrency 5 fdbgo --seed 3179331380
}

run_full() {
  local iterations=${1:-1} i
  run_faulty_seeds
  for ((i = 1; i <= iterations; i++)); do
    log "iteration $i/$iterations"
    bt --api-version "$api_version" --test-name scripted fdbgo
    bt --num-ops 1000 --api-version $api_version --test-name api --compare python fdbgo
    bt --num-ops 1000 --api-version $api_version --test-name api --concurrency 5 fdbgo
    bt --num-ops 1000 --api-version $api_version --test-name directory --concurrency 1 fdbgo --compare python
    bt --num-ops 100 --api-version $api_version --test-name directory_hca --concurrency 1 fdbgo "${directory_flags[@]}"
  done
}

mode=${1:-quick}
setup_python
setup_sources
[[ "$mode" == setup ]] && exit 0
bt_run=$root/run/bt-$fdb_version
bt_port=${FDB_BT_PORT:-$((4000 + max_api_version))}
default_cluster_file=$bt_run/fdb.cluster
cluster_file=${FDB_CLUSTER_FILE:-$default_cluster_file}
setup_cluster
build_tester
case "$mode" in
  quick) run_quick ;;
  full) run_full "${2:-1}" ;;
  *) bt "$@" ;;
esac
