#!/bin/bash
# run16.sh: A/B of the scan thread's outbound queue (TASK-1.27) on a c7i.4xlarge.
# hybrid-before = 444feb8d4 (blocking pipe writes), hybrid = the same tree with the queue.
# Server on vCPUs 0-3,8-11, load on 4-7,12-15, dumps and dataset cache on tmpfs (remounted to 24G).
set -u
cd ~/redis-after/utils/snap-bench || exit 1
export SERVER_CPUS="0-3,8-11" LOAD_CPUS="4-7,12-15"
export HYB=~/redis-after/src/redis-server HYB_BEFORE=~/redis-before/src/redis-server
export BENCH_DIR=/dev/shm/snap-bench-runs CFGS="hybrid-before hybrid"
CSV=~/run16.csv
LOG=~/run16.log
run_ds() {  # <dataset> <scale> <rates> <reps> <hot>
  echo "=== $(date +%T) dataset $1 scale $2 rates $3 reps $4 hot $5" >> "$LOG"
  HOT=$5 ./run-interleaved.sh "$CSV" "$3" "$1" "$2" "$4" >> "$LOG" 2>&1
  rm -f /dev/shm/snap-bench-runs/cache/*      # one 3 GB dataset cache at a time
  rm -f /dev/shm/snap-bench-runs/*.rdb /dev/shm/snap-bench-runs/temp-*.rdb
}
run_ds mixed1m 16   0,20000        2 1.0     # AC1: the sum-vs-max cell of run 13
run_ds small   13.5 0,20000,50000  1 1.0     # AC3: run 12 cells, hybrid before/after
run_ds mixed   16   0,20000,50000  1 1.0
echo "=== $(date +%T) RUN16_DONE" >> "$LOG"
