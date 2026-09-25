#!/bin/bash
# Steady-state cost of the hybrid feature with NO save running: pinned redis-benchmark SET/GET/DEL,
# feature off vs on, same binary. Use a PLAIN build (REDIS_CFLAGS empty): debug assertions add
# hot-path checks that are not part of the feature.
#   microbench.sh [binary] [runs]      -> prints ops/s per command and mode, medians over runs
#   MODES="on off" CMDS="set get" SERVER_CPU=2 BENCH_CPUS=4-7 to change the order, commands and pinning
BIN=${1:-/tmp/redis-hyb/src/redis-server}; RUNS=${2:-3}
BENCH=$(dirname "$BIN")/redis-benchmark; CLI=/home/moti/git/redis-bgsave/src/redis-cli; PORT=7601
SERVER_CPU=${SERVER_CPU:-2}; BENCH_CPUS=${BENCH_CPUS:-4-7}
N=${N:-2000000}; P=${P:-16}; C=${C:-50}; D=${D:-100}; R=${R:-1000000}
median() { sort -n | awk '{a[NR]=$1} END {print (NR%2 ? a[(NR+1)/2] : (a[NR/2]+a[NR/2+1])/2)}'; }
for mode in ${MODES:-off on}; do
  $CLI -p $PORT shutdown nosave >/dev/null 2>&1; sleep 0.5
  flag=""; [ $mode = on ] && flag="--bgsave-hybrid yes"
  taskset -c $SERVER_CPU "$BIN" --port $PORT --bind 127.0.0.1 --save "" --appendonly no --daemonize yes --logfile /dev/null \
    --dir /dev/shm --disable-thp yes --io-threads 1 $flag || exit 1
  for i in $(seq 1 30); do $CLI -p $PORT ping 2>/dev/null | grep -q PONG && break; sleep 0.2; done
  for cmd in ${CMDS:-set get}; do
    res=()
    for r in $(seq 1 $RUNS); do
      $CLI -p $PORT flushall >/dev/null; [ $cmd != set ] && taskset -c $BENCH_CPUS $BENCH -p $PORT -q -t set -n $N -P $P -c $C -d $D -r $R >/dev/null
      res+=("$(taskset -c $BENCH_CPUS $BENCH -p $PORT -q -t $cmd -n $N -P $P -c $C -d $D -r $R --csv | tail -1 | cut -d, -f2 | tr -d '"')")
    done
    printf "%s %-3s %s ops/s  (runs: %s)\n" "$mode" "$cmd" "$(printf '%s\n' "${res[@]}" | median)" "${res[*]}"
  done
  echo "$mode: $($CLI -p $PORT info server | grep -E '^redis_git_sha1' | tr -d '\r') $($CLI -p $PORT config get bgsave-hybrid | tail -1)"
done
$CLI -p $PORT shutdown nosave >/dev/null 2>&1
