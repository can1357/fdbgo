#!/usr/bin/env bash
# Runs the FoundationDB bindingtester against the fdbgo stack machine (cmd/stacktester).
#
# Usage:
#   scripts/bindingtester.sh setup          python venv + patched bindingtester sources only
#   scripts/bindingtester.sh [quick]        scripted, api/directory vs python, directory_hca
#   scripts/bindingtester.sh full [N]       known-faulty seeds, then N random iterations (default 1)
#   scripts/bindingtester.sh <args...>      passes <args...> to bindingtester.py
#
# bindingtester.py clears the whole database before every test, so the default is a private
# cluster (run/bt, port 4800) that this script starts with scripts/cluster.sh when it is not
# running; never point FDB_CLUSTER_FILE at a shared cluster.
#
# Environment:
#   FDB_CLUSTER_FILE  cluster file (default: <repo>/run/bt/fdb.cluster, started on demand)
#   FDB_SOURCE_DIR    FoundationDB source tree providing bindings/ (read-only, copied)
#   FDB_CLIENT_LIB    libfdb_c used by the python reference binding
#   FDB_CLI           fdbcli used to probe the cluster and enable tenant_mode
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
fdb_source_dir=${FDB_SOURCE_DIR:-$root/target/fdb/src}
bt_run=$root/run/bt
bt_port=4800
default_cluster_file=$bt_run/fdb.cluster
cluster_file=${FDB_CLUSTER_FILE:-$default_cluster_file}
work_dir=$root/target/bindingtester
venv=$work_dir/venv
pypkg=$work_dir/pypkg
src=$work_dir/src
tester_bin=$work_dir/stacktester
# A go.mod makes target/ its own module, so ./... skips the Go files of the copied bindings.
mkdir -p "$root/target"
[[ -f $root/target/go.mod ]] || printf 'module ignored\n' >"$root/target/go.mod"
api_version=740

case "$(uname -s)" in
  Darwin) capi_name=libfdb_c.dylib ;;
  *) capi_name=libfdb_c.so ;;
esac
fdb_client_lib=${FDB_CLIENT_LIB:-$root/target/fdb/lib/$capi_name}
fdb_cli=${FDB_CLI:-$root/target/fdb/bin/fdbcli}

log() { printf '==> %s\n' "$*" >&2; }

# Python venv for bindingtester.py, plus the PyPI `foundationdb` release matching the local libfdb_c
# unpacked into $pypkg (outside sys.path) as the source of the generated fdboptions.py.
setup_python() {
  [[ -f "$fdb_client_lib" ]] || { log "libfdb_c not found at $fdb_client_lib"; exit 1; }
  if [[ ! -x "$venv/bin/python" ]]; then
    log "creating venv $venv"
    uv venv --quiet "$venv"
  fi

  fdb_version=$("$venv/bin/python" - "$fdb_client_lib" "$api_version" <<'EOF'
import ctypes, sys
lib = ctypes.CDLL(sys.argv[1])
# fdb_get_client_version dereferences the client API, which exists only after API selection.
if lib.fdb_select_api_version_impl(int(sys.argv[2]), int(sys.argv[2])) != 0:
    sys.exit("fdb_select_api_version failed")
lib.fdb_get_client_version.restype = ctypes.c_char_p
print(lib.fdb_get_client_version().decode().split(",")[0])
EOF
)
  log "libfdb_c version $fdb_version"
  if [[ ! -d "$pypkg/foundationdb-$fdb_version.dist-info" ]]; then
    rm -rf "$pypkg"
    uv pip install --quiet --python "$venv/bin/python" --target "$pypkg" "foundationdb==$fdb_version"
  fi
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
  cp "$pypkg/fdb/fdboptions.py" "$fdb_pkg/fdboptions.py"
  printf 'LATEST_API_VERSION = %s\nFDB_VERSION = "%s"\n' "$api_version" "$fdb_version" \
    > "$fdb_pkg/apiversion.py"
  # fdb/impl.py loads the C library from `<capi_name>.pth` next to the module when present.
  printf '%s\n' "$fdb_client_lib" > "$fdb_pkg/$capi_name.pth"

  # Tenants are not supported by this binding (tenants_enabled defaults to False).
  echo "testers['fdbgo'] = Tester('fdbgo', '$tester_bin', 2040, 23, $api_version, types=ALL_TYPES)" \
    >> "$src/bindings/bindingtester/known_testers.py"
}

cli() { "$fdb_cli" -C "$cluster_file" --timeout 30 --exec "$*"; }

# Starts the private cluster when the default cluster file is used and it is not answering.
# bindingtester.py clears \xff\xff/management/tenant/map/ before every test, which fails with
# tenants_disabled (2136) unless tenant_mode is enabled, so enable it.
setup_cluster() {
  if [[ "$cluster_file" == "$default_cluster_file" ]] &&
    ! { [[ -f "$cluster_file" ]] && "$fdb_cli" -C "$cluster_file" --timeout 5 --exec "status minimal" 2>/dev/null |
      grep -q "available"; }; then
    log "starting private cluster in $bt_run (port $bt_port)"
    FDB_PORT=$bt_port FDB_RUN=$bt_run "$root/scripts/cluster.sh" up 1 memory
  fi
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

# Runs bindingtester.py from the patched tree. bindingtester's util.py imports fdb before the tree
# adjusts sys.path, so PYTHONPATH selects the patched binding; `python3` (python tester) is the venv.
bt() {
  log "bindingtester.py $*"
  (
    cd "$src"
    PATH="$venv/bin:$PATH" PYTHONPATH="$src/bindings/python" \
      "$venv/bin/python" bindings/bindingtester/bindingtester.py \
      --cluster-file "$cluster_file" "$@"
  )
}

run_quick() {
  bt --test-name scripted fdbgo
  bt --num-ops 1000 --api-version $api_version --test-name api --compare python fdbgo
  bt --num-ops 1000 --api-version $api_version --test-name directory --concurrency 1 fdbgo --compare python
  bt --num-ops 100 --api-version $api_version --test-name directory_hca --concurrency 1 fdbgo --no-directory-snapshot-ops
}

run_faulty_seeds() {
  # Seeds that exposed past regressions, with the flags they were found with.
  bt --num-ops 1000 --api-version $api_version --test-name api --compare python fdbgo --seed 3534790651
  bt --num-ops 1000 --api-version $api_version --test-name api --compare python fdbgo --seed 3864917676
  bt --num-ops 1000 --api-version $api_version --test-name api --concurrency 5 fdbgo --seed 3153055325
  local seed
  for seed in 2095856910 2483220251 241550211 1508488514 4203526853 \
    4142326254 4275610547 1951034301 1700644945 584458794; do
    bt --num-ops 1000 --api-version $api_version --test-name directory --concurrency 1 fdbgo --no-directory-snapshot-ops --compare python --seed "$seed"
  done
  # Database-level directory operations must retry concurrent commit conflicts.
  bt --num-ops 100 --api-version 610 --test-name directory_hca --concurrency 5 fdbgo --seed 3179331380
}

run_full() {
  local iterations=${1:-1} i
  run_faulty_seeds
  for ((i = 1; i <= iterations; i++)); do
    log "iteration $i/$iterations"
    bt --test-name scripted fdbgo
    bt --num-ops 1000 --api-version $api_version --test-name api --compare python fdbgo
    bt --num-ops 1000 --api-version $api_version --test-name api --concurrency 5 fdbgo
    bt --num-ops 1000 --api-version $api_version --test-name directory --concurrency 1 fdbgo --compare python
    bt --num-ops 100 --api-version $api_version --test-name directory_hca --concurrency 1 fdbgo --no-directory-snapshot-ops
  done
}

mode=${1:-quick}
setup_python
setup_sources
[[ "$mode" == setup ]] && exit 0
setup_cluster
build_tester
case "$mode" in
  quick) run_quick ;;
  full) run_full "${2:-1}" ;;
  *) bt "$@" ;;
esac
