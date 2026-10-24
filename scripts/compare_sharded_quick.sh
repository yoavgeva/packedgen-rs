#!/bin/sh
# Paired throughput of ShardedCache against quick_cache 0.7. Each pair runs
# both caches in separate processes, alternating which runs first, and
# writes the probe's CSV rows to OUT, each after the requested entry count.
# usage: sh scripts/compare_sharded_quick.sh PAIRS "WORKERS" ENTRIES OUT
set -eu
pairs=${1:-3}
workers_list=${2:-"1 4"}
entries=${3:-50000}
out=${4:-target/compare-sharded-quick.csv}
cargo build --locked --release --example bounded_cache_probe
probe=${CARGO_TARGET_DIR:-target}/release/examples/bounded_cache_probe
: > "$out"
for pair in $(seq 1 "$pairs"); do
    for workers in $workers_list; do
        for workload in resident-hit read-miss turnover cold-fill replace delete-reinsert update read-through; do
            case $workload in
                resident-hit | read-miss) operations=${READ_OPS:-8000000} ;;
                *) operations=${WRITE_OPS:-4000000} ;;
            esac
            if [ $((pair % 2)) -eq 0 ]; then
                order='quickcache sharded'
            else
                order='sharded quickcache'
            fi
            for backend in $order; do
                printf '%s,' "$entries" >> "$out"
                "$probe" "$backend" "$workload" "$entries" "$operations" "$workers" 64 "$pair" 0 \
                    | tail -n 1 >> "$out"
            done
        done
    done
done
