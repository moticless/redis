#!/bin/bash
# run16b.sh: second pass of run 16 with the memory-fixed build (hybrid3 = ~/redis-after3),
# appended to ~/run16.csv so the three configurations share one file. Same cells as run16.sh.
set -u
cd ~/redis-after3/utils/snap-bench || exit 1
export SERVER_CPUS="0-3,8-11" LOAD_CPUS="4-7,12-15"
export HYB3=~/redis-after3/src/redis-server CLI=~/redis-after3/src/redis-cli
export BENCH_DIR=/dev/shm/snap-bench-runs CFGS="hybrid3"
CSV=~/run16.csv
LOG=~/run16c.log
run_ds() {  # <dataset> <scale> <rates> <reps> <hot>
  echo "=== $(date +%T) dataset $1 scale $2 rates $3 reps $4 hot $5" >> "$LOG"
  HOT=$5 ./run-interleaved.sh "$CSV" "$3" "$1" "$2" "$4" >> "$LOG" 2>&1
  rm -f /dev/shm/snap-bench-runs/cache/*
  rm -f /dev/shm/snap-bench-runs/*.rdb /dev/shm/snap-bench-runs/temp-*.rdb
}
run_ds mixed1m 16   0,20000        2 1.0
run_ds small   13.5 0,20000,50000  1 1.0
run_ds mixed   16   0,20000,50000  1 1.0
echo "=== $(date +%T) RUN16C_DONE" >> "$LOG"
