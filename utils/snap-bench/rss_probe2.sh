#!/bin/bash
# Cross-class reuse: populate small (arena-heavy), FLUSHALL, wait, populate big
# (forkable-heavy). Does memory freed in one arena stay resident?  rss_probe2.sh <base|hybrid>
cd "$(dirname "$0")"
CFG=$1; PORT=7700; D=/dev/shm/snap-bench-runs; mkdir -p $D
BASE=/tmp/redis-base/src/redis-server; HYB=/home/moti/git/redis-bgsave/src/redis-server
CLI=/home/moti/git/redis-bgsave/src/redis-cli
FLAGS="--port $PORT --bind 127.0.0.1 --save '' --appendonly no --activedefrag no --disable-thp yes --io-threads 1 --jemalloc-bg-thread yes --daemonize yes --dir $D --dbfilename probe.rdb --logfile $D/probe2-$CFG.log"
$CLI -p $PORT shutdown nosave >/dev/null 2>&1; sleep 0.5
if [ "$CFG" = base ]; then eval taskset -c 0-7 $BASE $FLAGS; else eval taskset -c 0-7 $HYB $FLAGS --bgsave-hybrid yes; fi
for i in $(seq 1 50); do $CLI -p $PORT ping 2>/dev/null | grep -q PONG && break; sleep 0.2; done
PID=$($CLI -p $PORT info server | grep process_id | tr -d '\r' | cut -d: -f2)
dump() {
  printf "%-32s used %5s MB  RssAnon %6s kB  LazyFree %6s kB  allocator_resident %5s MB\n" "$1" \
    $(( $($CLI -p $PORT info memory | grep "^used_memory:" | tr -d '\r' | cut -d: -f2) / 1048576 )) \
    $(grep RssAnon /proc/$PID/status | awk '{print $2}') \
    $(grep LazyFree /proc/$PID/smaps_rollup | awk '{print $2}') \
    $(( $($CLI -p $PORT info memory | grep "^allocator_resident:" | tr -d '\r' | cut -d: -f2) / 1048576 ))
}
pop() { python3 -c "import sys; sys.path.insert(0,'.'); import bench; bench.populate($PORT, '$1', 1.0)"; }
pop small; sleep 3; dump "small populated"
$CLI -p $PORT flushall >/dev/null; sleep 3; dump "FLUSHALL +3s"
sleep 15; dump "FLUSHALL +18s"
sleep 15; dump "FLUSHALL +33s"
pop big; sleep 3; dump "big populated (after small)"
sleep 20; dump "big +23s"
$CLI -p $PORT memory purge >/dev/null; sleep 1; dump "after MEMORY PURGE"
$CLI -p $PORT shutdown nosave >/dev/null 2>&1
