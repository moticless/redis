#!/bin/bash
# Why does the hybrid server have a larger RSS at rest on the big/mixed datasets?
# Start one server (binary + flags as run.sh), populate a dataset, dump allocator
# stats, MEMORY PURGE, dump again.   rss_probe.sh <base|hybrid> <dataset>
cd "$(dirname "$0")"
CFG=$1; DS=${2:-big}; PORT=7700; D=/tmp/snap-bench-runs
BASE=/tmp/redis-base/src/redis-server; HYB=/home/moti/git/redis-bgsave/src/redis-server
CLI=/home/moti/git/redis-bgsave/src/redis-cli
FLAGS="--port $PORT --bind 127.0.0.1 --save '' --appendonly no --activedefrag no --disable-thp yes --io-threads 1 --jemalloc-bg-thread yes --daemonize yes --enable-debug-command yes --dir $D --dbfilename probe.rdb --logfile $D/probe-$CFG.log"
$CLI -p $PORT shutdown nosave >/dev/null 2>&1; sleep 0.5
if [ "$CFG" = base ]; then eval taskset -c 0-7 $BASE $FLAGS; else eval taskset -c 0-7 $HYB $FLAGS --bgsave-hybrid yes; fi
for i in $(seq 1 50); do $CLI -p $PORT ping 2>/dev/null | grep -q PONG && break; sleep 0.2; done
python3 -c "
import sys; sys.path.insert(0,'.'); import bench
bench.populate($PORT, '$DS', 1.0)"
PID=$($CLI -p $PORT info server | grep process_id | tr -d '\r' | cut -d: -f2)
dump() {
  echo "--- $1"
  $CLI -p $PORT info memory | grep -E "^(used_memory|used_memory_rss|allocator_allocated|allocator_active|allocator_resident|allocator_frag_bytes|allocator_rss_bytes|rss_overhead_bytes|mem_fragmentation_ratio|allocator_allocated_nofork|allocator_active_nofork|allocator_resident_nofork|snap_nofork_arena_va_used):" | tr -d '\r' | tr '\n' ' '; echo
  grep -E "^(RssAnon|RssFile|VmSwap)" /proc/$PID/status | tr '\n' ' '; echo
  $CLI -p $PORT memory malloc-stats | grep -E "^Allocated|^arenas\[[0-9]+\]:$|^(dirty|muzzy|retained|allocated|active|mapped|resident):" | head -40
}
sleep 3; dump "after populate + 3s"
$CLI -p $PORT bgsave >/dev/null
while $CLI -p $PORT info persistence | grep -qE "rdb_bgsave_in_progress:1|snap_save_in_progress:1"; do sleep 0.2; done
sleep 2; dump "after one BGSAVE"
sleep 12; dump "after 15s more (decay window passed)"
$CLI -p $PORT memory purge >/dev/null; sleep 1; dump "after MEMORY PURGE"
$CLI -p $PORT shutdown nosave >/dev/null 2>&1
