#!/bin/bash
# Insert-heavy writes during a save: the keyspace dict's layout (table size,
# keys, chains) sampled while the save runs, and the cost of lookups on it.
# usage: rehash-pause.sh MODE   (MODE: hybrid | classic | nosave)
# env: BIN (dir with redis-server/cli/benchmark), NKEYS, NINS, DELAY, PORT
MODE=$1
BIN=${BIN:-/home/moti/git/redis-bgsave/src}
PORT=${PORT:-7790}
DIR=${TMPDIR:-/tmp}/rp-$MODE
NKEYS=${NKEYS:-1040000}
NINS=${NINS:-6000000}
rm -rf $DIR; mkdir -p $DIR; cd $DIR
HYB=yes; [ "$MODE" != hybrid ] && HYB=no
$BIN/redis-server --port $PORT --save "" --appendonly no --dir $DIR --hybrid-snapshot $HYB \
  --rdb-key-save-delay ${DELAY:-8} --logfile $DIR/log.txt --daemonize no --enable-debug-command yes > $DIR/out.txt 2>&1 &
SP=$!
for i in $(seq 50); do $BIN/redis-cli -p $PORT ping 2>/dev/null | grep -q PONG && break; sleep 0.1; done
C="$BIN/redis-cli -p $PORT"
insave() { $C info persistence | grep '^rdb_bgsave_in_progress' | tr -d '\r' | cut -d: -f2; }
# Table 0 (and 1 while rehashing) of the keyspace dict: size, keys, max and average chain.
layout() {
  $C debug htstats 0 full | awk '/^\[Expires/{exit} /^Hash table [01] stats/{t=$3}
    /table size/{printf " t%s.size=%s",t,$NF} /number of elements/{printf " t%s.keys=%s",t,$NF}
    /max chain/{printf " t%s.maxchain=%s",t,$NF} /avg chain length \(counted\)/{printf " t%s.avgchain=%s",t,$NF}'
  echo
}
# Lookups, mostly misses (a miss walks the whole chain): client rate and
# latency, and the server's own time per GET (commandstats delta).
getstat() { $C info commandstats | grep '^cmdstat_get:' | tr -d '\r' | sed 's/.*calls=\([0-9]*\),usec=\([0-9]*\),.*/\1 \2/'; }
probe() {
  local a b
  a=($(getstat)); [ ${#a[@]} -eq 0 ] && a=(0 0)
  $BIN/redis-benchmark -p $PORT -t get -n ${NGET:-2000000} -r 1000000000 -P 8 -c 20 --threads 2 -q 2>&1 |
    tr '\r' '\n' | grep -E '^GET: .*per second' | tail -n 1 | tr -d '\n'
  b=($(getstat))
  awk -v c=$(( b[0] - a[0] )) -v u=$(( b[1] - a[1] )) 'BEGIN{printf ", server %.3f usec/GET\n", u / c}'
}
rehashed() { for i in $(seq 3000); do $C debug htstats 0 | grep -q "rehashing target" || return; sleep 0.1; done; }
$C debug populate $NKEYS key 16 > /dev/null
rehashed   # the populate's own rehash
echo "== before: $($C dbsize) keys:$(layout)"
echo "== lookups before: $(probe)"
T0=$(date +%s%N)
if [ "$MODE" != nosave ]; then $C bgsave > /dev/null; sleep 0.5; fi
( while :; do echo "   t=$(( ($(date +%s%N) - T0) / 1000000 ))ms save=$(insave)$(layout)"; sleep 2; done ) &
SAMPLER=$!
$BIN/redis-benchmark -p $PORT -t set -n $NINS -r 1000000000 -d 16 -P 8 -c 20 --threads 2 -q 2>&1 | tr '\r' '\n' |
  grep -E '^SET: .*per second' | tail -n 1 | sed 's/^/== inserts: /'
kill $SAMPLER; wait $SAMPLER 2>/dev/null
echo "== after inserts, save=$(insave): $($C dbsize) keys:$(layout)"
echo "== lookups after inserts, save=$(insave) at start: $(probe) (save=$(insave) at end)"
$C info persistence | grep -E "snap_save_(overlay|main_keys|inplace)" | tr -d '\r' | tr '\n' ' '; echo
# wait for the save to end and the dict to finish growing, then look again
for i in $(seq 1200); do [ "$(insave)" = 0 ] && break; sleep 0.5; done
echo "== save done: $(( ($(date +%s%N) - T0) / 1000000 ))ms, $($C info persistence | grep -E '^(rdb_last_bgsave_status|snap_last_save_inplace_writes)' | tr -d '\r' | tr '\n' ' ')"
# the cron starts the resize the save held back: let it, then time the rehash
sleep 1
R0=$(date +%s%N); rehashed
echo "== rehashed after $(( ($(date +%s%N) - R0) / 1000000 ))ms:$(layout)"
echo "== lookups rehashed: $(probe)"
$C shutdown nosave 2>/dev/null; wait $SP 2>/dev/null
