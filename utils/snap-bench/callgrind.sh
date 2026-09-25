#!/bin/bash
# Deterministic steady-state cost of the feature: instructions executed inside processCommand()
# for 200k SET (fill) and 200k SET + 200k GET, feature off vs on, same PLAIN binary, under callgrind.
# Instruction counts do not depend on CPU frequency or on other load, unlike ops/s.
# LIMITATION (2026-09-25): under valgrind the SNAP_NOFORK arena reports unavailable
# (snap_nofork_arena_available:0 in the INFO line printed per run), so "on" measures the feature's
# code path with values in the default allocator, not the arena mallocx path. For the arena cost
# use perf stat -e instructions on the server pid (needs kernel.perf_event_paranoid <= 1 or root).
#   callgrind.sh [binary]     -> Ir per mode and phase, plus the on/off ratio
BIN=${1:-/tmp/redis-hyb/src/redis-server}; BENCH=$(dirname "$BIN")/redis-benchmark
CLI=/home/moti/git/redis-bgsave/src/redis-cli; PORT=7602; OUT=${OUT:-/dev/shm/callgrind}; mkdir -p $OUT
N=${N:-200000}; P=16; C=20; D=100; R=100000
run() {  # run <mode> <phases: set | setget>
  local mode=$1 phases=$2 flag="" f=$OUT/cg-$mode-$phases.out
  [ $mode = on ] && flag="--bgsave-hybrid yes"
  rm -f $f
  valgrind --tool=callgrind --callgrind-out-file=$f --collect-atstart=no --toggle-collect=processCommand \
    "$BIN" --port $PORT --bind 127.0.0.1 --save "" --appendonly no --logfile /dev/null --dir /dev/shm --disable-thp yes $flag \
    > $OUT/cg-$mode-$phases.log 2>&1 &
  local vp=$!
  for i in $(seq 1 300); do $CLI -p $PORT ping 2>/dev/null | grep -q PONG && break; sleep 0.2; done
  $BENCH -p $PORT -q -t set -n $N -P $P -c $C -d $D -r $R > /dev/null
  [ $phases = setget ] && $BENCH -p $PORT -q -t get -n $N -P $P -c $C -d $D -r $R > /dev/null
  $CLI -p $PORT info | grep -E "^snap_nofork_arena_available|^snap_nofork_va_used|^used_memory:" | tr -d '\r' | tr '\n' ' ' >&2; echo >&2
  $CLI -p $PORT shutdown nosave > /dev/null 2>&1; wait $vp
  grep "^summary:" $f | awk '{print $2}'
}
declare -A ir
for mode in off on; do for ph in set setget; do ir[$mode-$ph]=$(run $mode $ph); echo "$mode $ph: Ir=${ir[$mode-$ph]}"; done; done
awk -v so=${ir[off-set]} -v sn=${ir[on-set]} -v go=${ir[off-setget]} -v gn=${ir[on-setget]} 'BEGIN {
  printf "SET: off %.1f Ir/cmd, on %.1f Ir/cmd, on/off %+.2f%%\n", so/'$N', sn/'$N', (sn/so-1)*100;
  printf "GET: off %.1f Ir/cmd, on %.1f Ir/cmd, on/off %+.2f%%\n", (go-so)/'$N', (gn-sn)/'$N', ((gn-sn)/(go-so)-1)*100 }'
