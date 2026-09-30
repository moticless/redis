#!/bin/bash
# run19.sh: the scan thread's queue bound, 128 MB (default) vs 16 MB vs 2 MB, BGSAVE on mixed1m (0, 20k) and small (0, 20k),
# interleaved, 2 reps. Answers whether a small queue keeps the mixed1m overlap.
set -u
cd ~/redis-after6/utils/snap-bench || exit 1
export SERVER_CPUS="0-3,8-11" LOAD_CPUS="4-7,12-15"
export HYB=~/redis-after6/src/redis-server CLI=~/redis-after6/src/redis-cli
export BENCH_DIR=/dev/shm/snap-bench-runs CFGS="hybrid hybrid-q16 hybrid-q2"
CSV=~/run19.csv
LOG=~/run19.log
run_ds() {  # <dataset> <scale> <rates> <reps> <hot>
  echo "=== $(date +%T) dataset $1 scale $2 rates $3 reps $4 hot $5" >> "$LOG"
  HOT=$5 ./run-interleaved.sh "$CSV" "$3" "$1" "$2" "$4" >> "$LOG" 2>&1
  rm -f /dev/shm/snap-bench-runs/cache/* /dev/shm/snap-bench-runs/*.rdb
}
run_ds mixed1m 16   0,20000 2 1.0
run_ds small   13.5 0,20000 2 1.0
echo "=== $(date +%T) RUN19_DONE" >> "$LOG"
