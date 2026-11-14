#!/usr/bin/env bash
# A/B throughput sweep: fdbgo against libfdb_c on one fresh cluster.
#
#   [CLIENTS="c fdbgo"] [GRID="1:512 4:32 4:128 4:512"] [WORKLOADS="read write mixed"] \
#     scripts/bench.sh <label> <nprocs> [engine=ssd] [measure_secs=15]
#
# Brings the cluster up (scripts/cluster.sh), loads 1M keys, then runs every workload ×
# (client procs:concurrency) point once per client, interleaved so each client sees the same
# cluster state. Before each run it waits for storage/log queues to drain and data movement to
# stop; mid-run it snapshots `status json` for the busiest process and ratekeeper's limiter.
# One JSON line per run is appended to bench/results.jsonl, tagged with <label>.
#
# Clients: c (libfdb_c via the official bindings) and fdbgo, both run by bench/fdbbench (built here).
#
# The sweep cluster is separate from the dev cluster: FDB_RUN defaults to run/bench, FDB_PORT to 4900.
#
# Env: FDB_BIN (default target/fdb/bin), FDB_PORT, FDB_RUN.
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
export FDB_RUN=${FDB_RUN:-$ROOT/run/bench} FDB_PORT=${FDB_PORT:-4900}
BENCH=$ROOT/bench/fdbbench
FDB_BIN=${FDB_BIN:-$ROOT/target/fdb/bin}
CF=$FDB_RUN/fdb.cluster
OUT=$ROOT/bench/results.jsonl
label=${1:?label}
nprocs=${2:?nprocs}
engine=${3:-ssd}
secs=${4:-15}
warm=5
KEYS=1000000
CLIENTS=${CLIENTS:-c fdbgo}
GRID=${GRID:-1:512 4:32 4:128 4:512}
WORKLOADS=${WORKLOADS:-read write mixed}

status() { "$FDB_BIN/fdbcli" -C "$CF" --timeout 30 --exec 'status json'; }

settle() {
	for _ in $(seq 300); do
		status | jq -e '.cluster.qos.performance_limited_by.name == "workload"
			and .cluster.qos.worst_queue_bytes_storage_server < 10e6
			and .cluster.qos.worst_queue_bytes_log_server < 10e6
			and .cluster.data.state.healthy
			and .cluster.data.moving_data.in_flight_bytes + .cluster.data.moving_data.in_queue_bytes == 0' >/dev/null && return
		sleep 1
	done
	echo "warning: cluster did not settle in 300s" >&2
}

# bench <wl> <client> <procs> <c> <bench args...>
bench() {
	local wl=$1 client=$2 procs=$3 c=$4 out srv
	shift 4
	out=$(mktemp)
	"$BENCH" -C "$CF" -client "$client" -c "$c" -procs "$procs" -json "$@" >"$out" &
	srv='{}'
	if [[ $wl != load ]]; then
		sleep $((warm + secs / 2))
		srv=$(status | jq -c '{
			top_cpu: ([.cluster.processes[] | {cores: .cpu.usage_cores, roles: ([.roles[].role] | join(","))}] | max_by(.cores)),
			srv_cores: ([.cluster.processes[].cpu.usage_cores] | add),
			limited_by: .cluster.qos.performance_limited_by.name}')
	fi
	wait $!
	jq -c --arg label "$label" --arg wl "$wl" --arg client "$client" --argjson srv "$srv" '. + {label: $label, wl: $wl, client: $client} + $srv' "$out" |
		tee -a "$OUT" |
		jq -r '"\(.label) \(.client) \(.wl) procs=\(.procs) c=\(.c): tx/s=\(.tx_s|floor) r/s=\(.reads_s|floor) w/s=\(.writes_s|floor) p50=\(.p50_ms) p99=\(.p99_ms)ms busiest=\((.top_cpu.cores // 0)*100|floor)% [\(.top_cpu.roles)] limited_by=\(.limited_by)"'
	rm -f "$out"
}

"$ROOT/bench/build.sh"
mkdir -p "$(dirname "$OUT")"
"$ROOT/scripts/cluster.sh" up "$nprocs" "$engine"
trap '"$ROOT/scripts/cluster.sh" down' EXIT

bench load fdbgo 4 64 --mode load --keys $KEYS --value 100 --batch 100
for wl in $WORKLOADS; do
	case $wl in
	read) ops=(--reads 10 --writes 0) ;;
	write) ops=(--reads 0 --writes 10) ;;
	mixed) ops=(--reads 8 --writes 2) ;;
	esac
	for pc in $GRID; do
		for client in $CLIENTS; do
			settle
			bench "$wl" "$client" "${pc%:*}" "${pc#*:}" --keys $KEYS --value 100 "${ops[@]}" -d "${secs}s" --warmup "${warm}s"
		done
	done
done
