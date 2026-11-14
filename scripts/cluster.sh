#!/usr/bin/env bash
# Throwaway local FoundationDB cluster on 127.0.0.1 for tests and benchmarks.
#
#   scripts/cluster.sh up <nprocs> [engine]   wipe run/, start nprocs fdbservers, configure, wait healthy
#   scripts/cluster.sh down                   stop all fdbservers started from run/
#   scripts/cluster.sh status                 fdbcli status
#   scripts/cluster.sh cli <cmd...>           run one fdbcli command
#
# nprocs=1 runs every role in one process. nprocs>=4 pins classes so roles don't share
# a process: 2 stateless (CC/master/proxies/resolver), ~1/3 transaction (tlogs), rest storage.
#
# Env: FDB_BIN (dir holding fdbserver/fdbcli, default target/fdb/bin from scripts/fetch-fdb.sh), FDB_PORT (first port, default 4700),
# FDB_RUN (state dir, default <repo>/run). The cluster file lands in $FDB_RUN/fdb.cluster.
# FDB_OBJECT_SERIALIZER=0|1 selects the 6.2 serializer for binary/object compatibility tests.
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
BIN=${FDB_BIN:-$ROOT/target/fdb/bin}
RUN=${FDB_RUN:-$ROOT/run}
CF=$RUN/fdb.cluster
PORT=${FDB_PORT:-4700}
knobs=()
if [[ -n ${FDB_OBJECT_SERIALIZER:-} ]]; then
	[[ $FDB_OBJECT_SERIALIZER == 0 || $FDB_OBJECT_SERIALIZER == 1 ]] || {
		echo "FDB_OBJECT_SERIALIZER must be 0 or 1" >&2; exit 2;
	}
	knobs=(--knob_use_object_serializer "$FDB_OBJECT_SERIALIZER")
fi

cli() { "$BIN/fdbcli" "${knobs[@]}" -C "$CF" --timeout 30 --exec "$*"; }

down() {
	[[ -f $RUN/pids ]] || return 0
	while read -r pid; do kill "$pid" 2>/dev/null || true; done <"$RUN/pids"
	while read -r pid; do while kill -0 "$pid" 2>/dev/null; do sleep 0.1; done; done <"$RUN/pids"
	rm -f "$RUN/pids"
}

up() {
	local n=$1 engine=${2:-ssd} logs=0 extra="" api version
	# GRV and commit proxies split in 7.0. Older releases only accept proxies=N.
	if [[ -f $BIN/../release.env ]]; then
		source "$BIN/../release.env"
		api=$FDB_RELEASE_API
	else
		version=$("$BIN/fdbserver" --version)
		[[ $version =~ ([0-9]+)\.([0-9]+)\.[0-9]+ ]] || {
			echo "cannot determine fdbserver version: $version" >&2; exit 1;
		}
		api=$((BASH_REMATCH[1] * 100 + BASH_REMATCH[2] * 10))
	fi
	((n == 1 || n >= 4)) || { echo "nprocs must be 1 or >=4" >&2; exit 2; }
	down
	rm -rf "$RUN"
	mkdir -p "$RUN/logs"
	echo "test:test@127.0.0.1:$PORT" >"$CF"
	if ((n > 1)); then
		logs=$(((n - 2) / 3))
		((logs >= 1)) || logs=1
		if ((api >= 700)); then
			extra="logs=$logs commit_proxies=2 grv_proxies=1 resolvers=1"
		else
			extra="logs=$logs proxies=2 resolvers=1"
		fi
	fi
	for ((i = 0; i < n; i++)); do
		local class=unset
		if ((n > 1)); then
			if ((i < 2)); then class=stateless
			elif ((i < 2 + logs)); then class=transaction
			else class=storage; fi
		fi
		mkdir -p "$RUN/data/$i"
		"$BIN/fdbserver" -p "127.0.0.1:$((PORT + i))" -C "$CF" -d "$RUN/data/$i" -L "$RUN/logs" \
			-c "$class" "${knobs[@]}" >"$RUN/logs/proc$i.out" 2>&1 &
		echo $! >>"$RUN/pids"
	done
	cli "configure new single $engine $extra"
	echo "waiting for healthy cluster..."
	for _ in $(seq 120); do
		if cli "status json" 2>/dev/null | jq -e '.client.database_status.available and .cluster.data.state.healthy' >/dev/null; then
			echo "up: $n procs, engine=$engine ${extra:-} cluster file $CF"
			return 0
		fi
		sleep 1
	done
	echo "cluster not healthy after 120s" >&2
	cli status >&2
	exit 1
}

case ${1:-} in
up) up "${2:?nprocs}" "${3:-ssd}" ;;
down) down ;;
status) cli status ;;
cli) shift; cli "$@" ;;
*) sed -n '2,13p' "$0" >&2; exit 2 ;;
esac
