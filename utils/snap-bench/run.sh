#!/bin/bash
# Start each binary with identical flags (pinned to cores 0-7), run bench.py (snapload.py traffic), stop it.
#   run.sh <csv> [rates] [datasets] [scale] [reps]
# CFGS env: space-separated subset of "base up hybrid hybrid-before hybrid2 valkey-fork valkey-forkless yinon-fork yinon-hybrid yinon-forkless"
# Parity: small-value cap pinned to 4096 B on both POCs (ours save-small-max-bytes, theirs forkless-string-max);
# retained-memory budgets left at each POC's default (ours 256 MB hard mark, theirs unlimited) and recorded in the CSV.
cd "$(dirname "$0")"
CSV=${1:-res.csv}; RATES=${2:-0,20000}; DATASETS=${3:-small,mixed,big}; SCALE=${4:-1.0}; REPS=${5:-1}
PORT=7600; D=${BENCH_DIR:-/dev/shm/snap-bench-runs}; mkdir -p "$D"   # tmpfs: save time must not measure disk writeback
SERVER_CPUS=${SERVER_CPUS:-0-7}   # server, its threads and its fork child (affinity is inherited); e.g. SERVER_CPUS=0 for one core
LOAD_CPUS=${LOAD_CPUS:-8-15}      # snapload.py, bench.py and its probes
HOT=${HOT:-1.0}                   # fraction of each class's keys the writes hit (bench.py --hot)
COOL=${COOL:-0}                   # thermal gate: wait before each cell until the package is below this many C (0 = off)
BASE=${BASE:-/tmp/redis-base/src/redis-server}        # stock 0c1c74706 (M1 baseline)
UP=${UP:-/tmp/redis-up/src/redis-server}              # stock upstream 0d6266f2d (current base)
HYB=${HYB:-/home/moti/git/redis-bgsave/src/redis-server}
HYB_BEFORE=${HYB_BEFORE:-/tmp/redis-hyb-before/src/redis-server}   # our branch before the change under test (A/B runs)
HYB2=${HYB2:-/tmp/redis-hyb2/src/redis-server}                     # a second build of our branch (A/B/C runs)
VALKEY=${VALKEY:-/tmp/valkey-bench/src/valkey-server} # valkey unstable (fork and forkless saves)
YINON=${YINON:-/home/moti/git/tmp/redis-yinon/src/redis-server} # teammate POC 1204d0970 (bgsave-mode fork|hybrid|forkless)
CLI=${CLI:-$(dirname "$HYB")/redis-cli}                 # any redis-cli

start() {  # start <name> <binary> [flags...]
  local name=$1 bin=$2; shift 2
  taskset -c "$SERVER_CPUS" "$bin" --port $PORT --bind 127.0.0.1 --save "" --appendonly no --activedefrag no --disable-thp yes \
    --io-threads 1 --latency-monitor-threshold 5 --jemalloc-bg-thread yes --daemonize yes --enable-debug-command yes \
    --dir "$D" --dbfilename "$name.rdb" --logfile "$D/$name.log" "$@" || return 1
  for i in $(seq 1 50); do $CLI -p $PORT ping 2>/dev/null | grep -q PONG && return 0; sleep 0.2; done
  return 1
}

for cfg in ${CFGS:-base hybrid}; do
  $CLI -p $PORT shutdown nosave >/dev/null 2>&1; sleep 1; rm -f "$D/$cfg.rdb"
  case $cfg in
    base)            start base "$BASE" ;;
    up)              start up "$UP" ;;
    hybrid)          start hybrid "$HYB" --bgsave-hybrid yes ;;
    hybrid-before)   start hybrid-before "$HYB_BEFORE" --bgsave-hybrid yes ;;
    hybrid2)         start hybrid2 "$HYB2" --bgsave-hybrid yes ;;
    valkey-fork)     start valkey-fork "$VALKEY" ;;
    valkey-forkless) start valkey-forkless "$VALKEY" --forkless-infrastructure-enabled yes --bgsave-default-method forkless ;;
    yinon-fork)      start yinon-fork "$YINON" --bgsave-mode fork ;;
    yinon-hybrid)    start yinon-hybrid "$YINON" --bgsave-mode hybrid --forkless-string-max 4096 ;;
    yinon-forkless)  start yinon-forkless "$YINON" --bgsave-mode forkless --forkless-string-max 4096 ;;
  esac || { echo "$cfg: server start failed (see $D/$cfg.log)"; continue; }
  echo "=== $(date +%T) $cfg $($CLI -p $PORT info server | grep -E '^redis_git_sha1|^redis_version|^valkey_version' | tr -d '\r' | tr '\n' ' ')"
  taskset -c "$LOAD_CPUS" python3 bench.py run --name $cfg --port $PORT --csv "$CSV" --rates "$RATES" --datasets "$DATASETS" --scale "$SCALE" --reps "$REPS" --load-cpus "$LOAD_CPUS" --server-cpus "$SERVER_CPUS" --hot "$HOT" --cool-below "$COOL"
  $CLI -p $PORT shutdown nosave >/dev/null 2>&1; sleep 2
done
echo "=== $(date +%T) DONE -> $CSV"
