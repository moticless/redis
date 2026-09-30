#!/bin/bash
# run18b.sh: Valkey's forkless full sync (PR 4797 branch, ~/valkey-pr) as an experimental fourth configuration
# of run 18, appended to ~/run18.csv. Same cells. Falls out of the table if the sync does not complete cleanly.
set -u
cd ~/redis-after6/utils/snap-bench || exit 1
export SERVER_CPUS="0-3,8-11" LOAD_CPUS="4-7,12-15"
export HYB=~/redis-after6/src/redis-server VALKEY=~/valkey-pr/src/valkey-server CLI=~/redis-after6/src/redis-cli
export BENCH_DIR=/dev/shm/snap-bench-runs CFGS="valkey-forkless" SYNC=1
export SYNC_REPLICA_ARGS="--forkless-infrastructure-enabled yes --repl-diskless-load on-empty-db"
CSV=~/run18.csv
LOG=~/run18b.log
run_ds() {  # <dataset> <scale> <rates> <reps> <hot>
  echo "=== $(date +%T) dataset $1 scale $2 rates $3 reps $4 hot $5" >> "$LOG"
  HOT=$5 ./run-interleaved.sh "$CSV" "$3" "$1" "$2" "$4" >> "$LOG" 2>&1
  rm -f /dev/shm/snap-bench-runs/cache/* /dev/shm/snap-bench-runs/*.rdb /dev/shm/snap-bench-runs/replica-*/*.rdb
}
run_ds mixed1m 16   0,20000 2 1.0
run_ds small   13.5 0,20000 2 1.0
run_ds mixed   16   0,20000 2 1.0
echo "=== $(date +%T) RUN18B_DONE" >> "$LOG"
