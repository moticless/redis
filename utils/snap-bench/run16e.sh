#!/bin/bash
# run16e.sh: queue bound 128 MB (hybrid = ~/redis-after4, bgsave-hybrid-max-queued-bytes default) vs
# 256 MB (hybrid3 = ~/redis-after3, queue bounded by the retained limit), mixed1m idle and 20k, interleaved, 2 reps.
set -u
cd ~/redis-after4/utils/snap-bench || exit 1
export SERVER_CPUS="0-3,8-11" LOAD_CPUS="4-7,12-15"
export HYB=~/redis-after4/src/redis-server HYB3=~/redis-after3/src/redis-server CLI=~/redis-after4/src/redis-cli
export BENCH_DIR=/dev/shm/snap-bench-runs CFGS="hybrid3 hybrid"
CSV=~/run16e.csv
LOG=~/run16e.log
echo "=== $(date +%T) mixed1m scale 16 rates 0,20000 reps 2 hot 1.0" >> "$LOG"
HOT=1.0 ./run-interleaved.sh "$CSV" 0,20000 mixed1m 16 2 >> "$LOG" 2>&1
rm -f /dev/shm/snap-bench-runs/cache/* /dev/shm/snap-bench-runs/*.rdb
echo "=== $(date +%T) RUN16E_DONE" >> "$LOG"
