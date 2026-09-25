#!/bin/bash
# TASK-1.22 AC5: copy-on-write paid by a FALLBACK child (BGREWRITEAOF) while the hybrid feature is on
# (arena flipped forkable for the child) versus the same rewrite with the feature off. Same binary.
#   aof-cow.sh [binary] [writes/s]   -> aof_last_cow_size, fork time, rewrite duration per mode
BIN=${1:-/tmp/redis-hyb/src/redis-server}; RATE=${2:-20000}; PORT=7604
CLI=/home/moti/git/redis-bgsave/src/redis-cli; BENCH=$(dirname "$BIN")/redis-benchmark; D=/dev/shm/aof-cow; mkdir -p $D
for mode in on off; do
  $CLI -p $PORT shutdown nosave >/dev/null 2>&1; sleep 0.5; rm -rf $D/$mode; mkdir -p $D/$mode
  taskset -c 0-7 "$BIN" --port $PORT --bind 127.0.0.1 --save "" --appendonly no --appenddirname aof --dir $D/$mode \
    --daemonize yes --logfile $D/$mode/log --disable-thp yes --bgsave-hybrid $([ $mode = on ] && echo yes || echo no) || exit 1
  for i in $(seq 1 50); do $CLI -p $PORT ping 2>/dev/null | grep -q PONG && break; sleep 0.2; done
  taskset -c 8-15 $BENCH -p $PORT -q -t set -n 1200000 -r 1200000 -d 100 -P 32 -c 20 --sequential >/dev/null 2>&1 || \
  taskset -c 8-15 $BENCH -p $PORT -q -t set -n 1200000 -r 1200000 -d 100 -P 32 -c 20 >/dev/null
  used=$($CLI -p $PORT info memory | grep "^used_memory_human" | tr -d '\r')
  # same-size overwrites at RATE for 15 s, rewrite starts 2 s in
  taskset -c 8-15 memtier_benchmark -s 127.0.0.1 -p $PORT --protocol redis -t 2 -c 4 --test-time 15 --rate-limiting $((RATE/8)) \
    --command="SET __key__ __data__" --command-key-pattern=R --key-prefix "key:" --key-minimum 1 --key-maximum 1200000 -d 100 \
    --hide-histogram > $D/$mode/memtier.out 2>&1 &
  sleep 2; t0=$(date +%s.%N); $CLI -p $PORT bgrewriteaof >/dev/null
  while [ "$($CLI -p $PORT info persistence | grep -c 'aof_rewrite_in_progress:1')" = 1 ]; do sleep 0.05; done
  t1=$(date +%s.%N); wait
  info=$($CLI -p $PORT info | tr -d '\r')
  echo "bgsave-hybrid $mode: $used; rewrite $(echo "$t1 - $t0" | bc) s; aof_last_cow_size=$(echo "$info" | grep '^aof_last_cow_size' | cut -d: -f2) B; latest_fork_usec=$(echo "$info" | grep '^latest_fork_usec' | cut -d: -f2); arena_forkable_now=$(echo "$info" | grep '^snap_nofork_arena_forkable' | cut -d: -f2); status=$(echo "$info" | grep '^aof_last_bgrewrite_status' | cut -d: -f2)"
  grep -c "arena made forkable" $D/$mode/log | sed 's/^/  arena flips logged: /'
done
$CLI -p $PORT shutdown nosave >/dev/null 2>&1
