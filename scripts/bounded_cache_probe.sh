#!/bin/sh
# Run each cache in a separate process; keep the source/binary identity with
# every CSV. Output directories are immutable once created.
set -eu

mode=${1:-smoke}
output=${2:-target/bounded-cache-2026-10-03}
case "$mode" in
    smoke) pairs=1; sizes=64; workers_list='1 2' ;;
    full) pairs=6; sizes='64 65536'; workers_list='1 2' ;;
    screen) pairs=6; sizes='64 65536'; workers_list='1 2' ;;
    *) printf '%s\n' 'usage: sh scripts/bounded_cache_probe.sh smoke|full|screen [new-output-directory]' >&2; exit 2 ;;
esac
if [ -e "$output" ]; then
    printf '%s\n' "refusing to replace existing results: $output" >&2
    exit 2
fi
mkdir -p "$output"
cargo build --locked --release --example bounded_cache_probe
binary=${CARGO_TARGET_DIR:-target}/release/examples/bounded_cache_probe
{
    rustc -vV
    cargo --version
    uname -a
    case "$(uname -s)" in
        Darwin) sysctl hw.logicalcpu hw.physicalcpu hw.memsize ;;
        Linux) getconf _NPROCESSORS_ONLN ;;
    esac
    git rev-parse HEAD
    git status --short
    printf 'mode=%s\npairs=%s\n' "$mode" "$pairs"
    printf '%s\n' 'features=default (empty); system allocator; randomized default hashers'
    printf 'RUSTFLAGS=%s\nCARGO_ENCODED_RUSTFLAGS=%s\n' "${RUSTFLAGS:-}" "${CARGO_ENCODED_RUSTFLAGS:-}"
} > "$output/environment.txt"
shasum -a 256 "$binary" Cargo.toml Cargo.lock src/cache.rs src/cache_arena.rs \
    examples/bounded_cache_probe.rs scripts/bounded_cache_probe.sh > "$output/hashes.txt"
git diff --binary > "$output/tracked-diff.patch"

for value_bytes in $sizes; do
    for workers in $workers_list; do
        for workload in resident-hit read-miss cold-fill turnover replace delete-reinsert; do
            if [ "$mode" = smoke ]; then
                entries=512
                operations=20000
            elif [ "$mode" = screen ]; then
                if [ "$value_bytes" -eq 64 ]; then
                    entries=50000
                    operations=500000
                else
                    entries=1024
                    operations=100000
                fi
                case "$workload" in
                    resident-hit|read-miss) operations=2000000 ;;
                esac
            elif [ "$value_bytes" -eq 64 ]; then
                entries=50000
                case "$workload" in
                    resident-hit|read-miss) operations=10000000 ;;
                    *) operations=1000000 ;;
                esac
            else
                entries=1024
                case "$workload" in
                    resident-hit|read-miss) operations=5000000 ;;
                    *) operations=200000 ;;
                esac
            fi
            pair=1
            while [ "$pair" -le "$pairs" ]; do
                case $((pair % 3)) in
                    1) backends='direct-owned quickcache direct-borrowed' ;;
                    2) backends='quickcache direct-borrowed direct-owned' ;;
                    0) backends='direct-borrowed direct-owned quickcache' ;;
                esac
                for backend in $backends; do
                    name="${workload}_v${value_bytes}_t${workers}_p${pair}_${backend}"
                    uptime > "$output/$name.host-before.txt"
                    "$binary" "$backend" "$workload" "$entries" "$operations" \
                        "$workers" "$value_bytes" "$pair" 0 \
                        > "$output/$name.csv" 2> "$output/$name.stderr"
                    uptime > "$output/$name.host-after.txt"
                    tail -n 1 "$output/$name.csv"
                done
                pair=$((pair + 1))
            done
        done
    done
done
