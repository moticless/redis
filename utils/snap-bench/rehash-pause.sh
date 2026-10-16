#!/bin/bash
# Insert-heavy writes during a hybrid save: chain length, overlay counters, latency.
# usage: rehash-pause.sh MODE   (MODE: hybrid | classic | nosave)
MODE=$1
BIN=/home/moti/git/redis-bgsave/src
PORT=7790
DIR=/tmp/claude-1000/rp-$MODE
NKEYS=${NKEYS:-1040000}
NINS=${NINS:-3000000}
rm -rf $DIR; mkdir -p $DIR; cd $DIR
HYB=yes; [ "$MODE" != hybrid ] && HYB=no
$BIN/redis-server --port $PORT --save "" --appendonly no --dir $DIR --hybrid-snapshot $HYB \
  --rdb-key-save-delay ${DELAY:-8} --logfile $DIR/log.txt --daemonize no --enable-debug-command yes > $DIR/out.txt 2>&1 &
SP=$!
for i in $(seq 50); do $BIN/redis-cli -p $PORT ping 2>/dev/null | grep -q PONG && break; sleep 0.1; done
C="$BIN/redis-cli -p $PORT"
$C debug populate $NKEYS key 16 > /dev/null
# let the populate's own rehash finish
for i in $(seq 100); do $C debug htstats 0 full | grep -q "rehashing target" || break; sleep 0.1; done
echo "== before: $($C dbsize) keys"; $C debug htstats 0 full | grep -E "table size|number of elements|max chain" | tr '\n' ' '; echo
if [ "$MODE" != nosave ]; then $C bgsave > /dev/null; sleep 0.5; fi
$BIN/redis-benchmark -p $PORT -t set -n $NINS -r 1000000000 -d 16 -P 8 -c 20 --threads 2 -q 2>&1 | tr '\r' '\n' | tail -n 2
echo "== after inserts (save running: $($C info persistence | grep -E '^(rdb_bgsave_in_progress)' | tr -d '\r'))"
$C debug htstats 0 full | grep -E "table size|number of elements|max chain|avg chain|chain length" | tr '\n' ' '; echo
$C info persistence | grep -E "snap_save_overlay|snap_save_side_entries|snap_save_main_buckets" | tr -d '\r' | tr '\n' ' '; echo
$C latency histogram set 2>/dev/null | grep -E "p99|p99.9|max" | head -3
# wait for the save to end, then report its length and the final layout
for i in $(seq 600); do [ "$($C info persistence | grep '^rdb_bgsave_in_progress' | tr -d '\r')" = "rdb_bgsave_in_progress:0" ] && break; sleep 0.5; done
$C info persistence | grep -E "rdb_last_bgsave_time_sec|rdb_last_bgsave_status|snap_save_overlay" | tr -d '\r' | tr '\n' ' '; echo
$C debug htstats 0 full | grep -E "table size|number of elements|max chain|avg chain|different slots|chain length|rehashing" | tr '\n' ' '; echo
$C shutdown nosave 2>/dev/null; wait $SP 2>/dev/null
