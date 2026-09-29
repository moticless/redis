#!/bin/bash
# run16d.sh: repeat the cells where run 16 pass 3 (hybrid3, the final build) differed from 'before' by
# more than a few percent, interleaved before/after with 3 reps each, to separate signal from noise.
set -u
cd ~/redis-after3/utils/snap-bench || exit 1
export SERVER_CPUS="0-3,8-11" LOAD_CPUS="4-7,12-15"
export HYB_BEFORE=~/redis-before/src/redis-server HYB3=~/redis-after3/src/redis-server CLI=~/redis-after3/src/redis-cli
export BENCH_DIR=/dev/shm/snap-bench-runs CFGS="hybrid-before hybrid3"
CSV=~/run16.csv
LOG=~/run16d.log
run_ds() {  # <dataset> <scale> <rates> <reps> <hot>
  echo "=== $(date +%T) dataset $1 scale $2 rates $3 reps $4 hot $5" >> "$LOG"
  HOT=$5 ./run-interleaved.sh "$CSV" "$3" "$1" "$2" "$4" >> "$LOG" 2>&1
  rm -f /dev/shm/snap-bench-runs/cache/*
  rm -f /dev/shm/snap-bench-runs/*.rdb /dev/shm/snap-bench-runs/temp-*.rdb
}
run_ds mixed   16   20000,50000 3 1.0
run_ds small   13.5 20000       3 1.0
echo "=== $(date +%T) RUN16D_DONE" >> "$LOG"
