#!/bin/bash
# Like run.sh, but interleaves the configurations per cell: for every (dataset, rate, rep) each
# binary is started, loads the dataset from the RDB cache, runs ONE cell and stops. Spreads clock
# drift, thermal state and background noise evenly across configurations. Same env knobs as run.sh.
#   run-interleaved.sh <csv> [rates] [datasets] [scale] [reps]
cd "$(dirname "$0")"
CSV=${1:-res.csv}; RATES=${2:-0,20000}; DATASETS=${3:-small,mixed,big}; SCALE=${4:-1.0}; REPS=${5:-1}
for ds in ${DATASETS//,/ }; do
  for rate in ${RATES//,/ }; do
    for rep in $(seq 1 $REPS); do
      echo "=== $(date +%T) cell $ds r$rate #$rep"
      CFGS="${CFGS:-base hybrid}" ./run.sh "$CSV" "$rate" "$ds" "$SCALE" 1 | grep -v "^=== .*DONE"
    done
  done
done
echo "=== $(date +%T) DONE -> $CSV"
