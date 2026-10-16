#!/bin/bash
# Run 40: stock upstream vs the committed side-table hybrid (before) vs the bucket-overlay hybrid (after), 4 GB datasets, 8 vCPUs for the server.
cd ~/redis-after/utils/snap-bench
export UP=~/redis-up/src/redis-server HYB=~/redis-before/src/redis-server HYB2=~/redis-after/src/redis-server CLI=~/redis-after/src/redis-cli
export SERVER_CPUS="0-3,8-11" LOAD_CPUS="4-7,12-15" CFGS="up hybrid hybrid2" BENCH_DIR=/dev/shm/snap-bench-runs
mkdir -p $BENCH_DIR
nohup ~/dump-gc.sh >/dev/null 2>&1 &
for ds in "mixed1m 16" "mixed 16" "small 13.5"; do
  set -- $ds
  rm -rf $BENCH_DIR/cache
  HOT=1.0 ./run-interleaved.sh ~/run40-$1.csv 0 $1 $2 1
  HOT=1.0 ./run-interleaved.sh ~/run40-$1.csv 20000,50000 $1 $2 2
  [ "$1" = mixed ] && HOT=0.2 ./run-interleaved.sh ~/run40-mixed-hot.csv 50000 mixed 16 2
done
pkill -f dump-gc.sh
echo RUN40_DONE
