#!/bin/bash
# run38.sh: run 18 (diskless full sync to a fresh replica, rdb channel) with the primary on one vCPU.
#   up = upstream 0d6266f2d, hybrid = 44a1bb220, valkey-fork = Valkey 06d35aca6 (fork-based diskless sync)
# Primary, scan thread and fork child on vCPU 0; replica and load on 4-7,12-15.
set -u
cd ~/redis-hyb/utils/snap-bench || exit 1
export SERVER_CPUS="0" LOAD_CPUS="4-7,12-15"
export HYB=~/redis-hyb/src/redis-server UP=~/redis-up/src/redis-server VALKEY=~/valkey/src/valkey-server CLI=~/redis-hyb/src/redis-cli
export BENCH_DIR=/dev/shm/snap-bench-runs CFGS="up hybrid valkey-fork" SYNC=1
export SYNC_REPLICA_ARGS="--repl-diskless-load on-empty-db"
CSV=~/run38.csv; LOG=~/run38.log
run_ds() {  # <dataset> <scale> <rates> <reps> <hot>
  echo "=== $(date +%T) dataset $1 scale $2 rates $3 reps $4 hot $5" >> "$LOG"
  HOT=$5 ./run-interleaved.sh "$CSV" "$3" "$1" "$2" "$4" >> "$LOG" 2>&1
  rm -f /dev/shm/snap-bench-runs/cache/* /dev/shm/snap-bench-runs/*.rdb /dev/shm/snap-bench-runs/replica-*/*.rdb
}
run_ds mixed   16 0,20000,50000 2 1.0
run_ds mixed1m 16 0,20000       2 1.0
echo "=== $(date +%T) RUN38_DONE" >> "$LOG"
