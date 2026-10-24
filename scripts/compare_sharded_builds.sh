#!/bin/sh
# Paired throughput of two ShardedCache builds of bounded_cache_probe,
# alternating which runs first. Writes "base" and "head" rows to OUT, each
# after the requested entry count, as compare_sharded_quick.sh does.
# usage: sh scripts/compare_sharded_builds.sh BASE_PROBE HEAD_PROBE PAIRS "WORKERS" ENTRIES OUT
set -eu
base=$1
head=$2
pairs=${3:-5}
workers_list=${4:-"1 4"}
entries=${5:-50000}
out=${6:-target/compare-sharded-builds.csv}
: > "$out"
for pair in $(seq 1 "$pairs"); do
    for workers in $workers_list; do
        for workload in resident-hit read-miss turnover cold-fill replace delete-reinsert update; do
            case $workload in
                resident-hit | read-miss) operations=${READ_OPS:-8000000} ;;
                *) operations=${WRITE_OPS:-4000000} ;;
            esac
            if [ $((pair % 2)) -eq 0 ]; then
                order='head base'
            else
                order='base head'
            fi
            for build in $order; do
                if [ "$build" = base ]; then probe=$base; else probe=$head; fi
                printf '%s,' "$entries" >> "$out"
                "$probe" sharded "$workload" "$entries" "$operations" "$workers" 64 "$pair" 0 \
                    | tail -n 1 | sed "s/^sharded,/$build,/" >> "$out"
            done
        done
    done
done
