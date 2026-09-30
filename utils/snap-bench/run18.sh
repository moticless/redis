#!/bin/bash
# run18.sh: diskless full sync to a fresh replica, three-way on one host and day:
#   up      = upstream 0d6266f2d (default rdb-channel diskless sync)
#   hybrid  = our branch with the hybrid child streaming the replica (~/redis-after6)
#   valkey-fork = Valkey 06d35aca6, fork-based diskless sync (dual channel off, its default)
# The replica runs the same binary as its primary, on the load CPUs, and is recreated per cell.
set -u
cd ~/redis-after6/utils/snap-bench || exit 1
export SERVER_CPUS="0-3,8-11" LOAD_CPUS="4-7,12-15"
export HYB=~/redis-after6/src/redis-server UP=~/redis-up/src/redis-server VALKEY=~/valkey/src/valkey-server CLI=~/redis-after6/src/redis-cli
export BENCH_DIR=/dev/shm/snap-bench-runs CFGS="up hybrid valkey-fork" SYNC=1
export SYNC_REPLICA_ARGS="--repl-diskless-load on-empty-db"   # the replica loads the stream directly: no 3 GB file per sync
CSV=~/run18.csv
LOG=~/run18.log
run_ds() {  # <dataset> <scale> <rates> <reps> <hot>
  echo "=== $(date +%T) dataset $1 scale $2 rates $3 reps $4 hot $5" >> "$LOG"
  HOT=$5 ./run-interleaved.sh "$CSV" "$3" "$1" "$2" "$4" >> "$LOG" 2>&1
  rm -f /dev/shm/snap-bench-runs/cache/* /dev/shm/snap-bench-runs/*.rdb /dev/shm/snap-bench-runs/replica-*/*.rdb
}
run_ds mixed1m 16   0,20000 2 1.0
run_ds small   13.5 0,20000 2 1.0
run_ds mixed   16   0,20000 2 1.0
echo "=== $(date +%T) RUN18_DONE" >> "$LOG"
