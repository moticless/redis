#!/bin/bash
# run17.sh: mixed1m three-way on one host and day: upstream 0d6266f2d (up), our hybrid with the queue
# (hybrid = ~/redis-after4, 128 MB queue bound), Valkey forkless 06d35aca6. Interleaved, 2 reps.
set -u
cd ~/redis-after4/utils/snap-bench || exit 1
export SERVER_CPUS="0-3,8-11" LOAD_CPUS="4-7,12-15"
export HYB=~/redis-after4/src/redis-server UP=~/redis-up/src/redis-server VALKEY=~/valkey/src/valkey-server CLI=~/redis-after4/src/redis-cli
export BENCH_DIR=/dev/shm/snap-bench-runs CFGS="up hybrid valkey-forkless"
CSV=~/run17.csv
LOG=~/run17.log
echo "=== $(date +%T) mixed1m scale 16 rates 0,20000,50000 reps 2 hot 1.0" >> "$LOG"
HOT=1.0 ./run-interleaved.sh "$CSV" 0,20000,50000 mixed1m 16 2 >> "$LOG" 2>&1
rm -f /dev/shm/snap-bench-runs/cache/* /dev/shm/snap-bench-runs/*.rdb
echo "=== $(date +%T) RUN17_DONE" >> "$LOG"
