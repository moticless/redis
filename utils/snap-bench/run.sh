#!/bin/bash
# Start each binary with identical flags (pinned to cores 0-7), run bench.py, stop it.
#   run.sh <csv> [rates] [datasets] [scale] [reps]
# CFGS env: space-separated subset of "base hybrid"
cd "$(dirname "$0")"
CSV=${1:-res.csv}; RATES=${2:-0,20000}; DATASETS=${3:-small,mixed,big}; SCALE=${4:-1.0}; REPS=${5:-1}
PORT=7600; D=${BENCH_DIR:-/dev/shm/snap-bench-runs}; mkdir -p "$D"   # tmpfs: save time must not measure disk writeback
BASE=/tmp/redis-base/src/redis-server
HYB=/home/moti/git/redis-bgsave/src/redis-server
CLI=/home/moti/git/redis-bgsave/src/redis-cli

start() {  # start <name> <binary> [flags...]
  local name=$1 bin=$2; shift 2
  taskset -c 0-7 "$bin" --port $PORT --bind 127.0.0.1 --save "" --appendonly no --activedefrag no --disable-thp yes \
    --io-threads 1 --latency-monitor-threshold 5 --jemalloc-bg-thread yes --daemonize yes --enable-debug-command yes \
    --dir "$D" --dbfilename "$name.rdb" --logfile "$D/$name.log" "$@" || return 1
  for i in $(seq 1 50); do $CLI -p $PORT ping 2>/dev/null | grep -q PONG && return 0; sleep 0.2; done
  return 1
}

for cfg in ${CFGS:-base hybrid}; do
  $CLI -p $PORT shutdown nosave >/dev/null 2>&1; sleep 1; rm -f "$D/$cfg.rdb"
  case $cfg in
    base)   start base "$BASE" ;;
    hybrid) start hybrid "$HYB" --bgsave-hybrid yes ;;
  esac || { echo "$cfg: server start failed (see $D/$cfg.log)"; continue; }
  echo "=== $(date +%T) $cfg $($CLI -p $PORT info server | grep -E '^redis_git_sha1' | tr -d '\r')"
  taskset -c 8-15 python3 bench.py run --name $cfg --port $PORT --csv "$CSV" --rates "$RATES" --datasets "$DATASETS" --scale "$SCALE" --reps "$REPS"
  $CLI -p $PORT shutdown nosave >/dev/null 2>&1; sleep 2
done
echo "=== $(date +%T) DONE -> $CSV"
