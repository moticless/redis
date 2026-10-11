#!/bin/bash
# Run 39: atomic slot migration on the source node, stock (hybrid off) vs hybrid, 4 GB datasets, slices of 5% and 25% of the slots.
cd ~/redis-hyb/utils/snap-bench
export HYB=~/redis-hyb/src/redis-server CLI=~/redis-hyb/src/redis-cli
export SERVER_CPUS="0-3,8-11" LOAD_CPUS="4-7,12-15" BENCH_DIR=/dev/shm/snap-bench-runs ASM=1 HOT=1.0 CFGS="hybrid-off hybrid"
run() { echo "=== $(date +%T) START $1 slice $2 $4 scale $5"; ASM_SLICE=$2 ./run-interleaved.sh "$1" "$3" "$4" "$5" 2; }
rm -rf $BENCH_DIR/cache
run ~/run39s5.csv  0.05 0,20000,50000 small 13.5
run ~/run39s25.csv 0.25 0,20000,50000 small 13.5
rm -rf $BENCH_DIR/cache
run ~/run39m5.csv  0.05 0,20000,50000 mixed 16
run ~/run39m25.csv 0.25 0,20000,50000 mixed 16
echo RUN39_DONE
