#!/bin/sh
# Paired old/new native processes for the bounded cache probe. This is a
# throughput regression screen, not an isolated-host or tail-latency proof.
set -eu

baseline=${1:?usage: sh scripts/bounded_cache_regression.sh BASELINE_BINARY NEW_OUTPUT_DIRECTORY [direct-owned|direct-borrowed] [full|reads|large-reads]}
output=${2:?usage: sh scripts/bounded_cache_regression.sh BASELINE_BINARY NEW_OUTPUT_DIRECTORY [direct-owned|direct-borrowed] [full|reads|large-reads]}
backend=${3:-direct-owned}
scope=${4:-full}
case "$backend" in
    direct-owned|direct-borrowed) ;;
    *) printf '%s\n' 'backend must be direct-owned or direct-borrowed' >&2; exit 2 ;;
esac
case "$scope" in
    full) workloads='resident-hit read-miss cold-fill turnover replace delete-reinsert'; sizes='64 65536'; worker_counts='1 2' ;;
    reads) workloads='resident-hit read-miss'; sizes='64 65536'; worker_counts='1 2' ;;
    large-reads) workloads='resident-hit read-miss'; sizes=65536; worker_counts=1 ;;
    *) printf '%s\n' 'scope must be full, reads, or large-reads' >&2; exit 2 ;;
esac
pairs=${PACKEDGEN_REGRESSION_PAIRS:-6}
read_operations=${PACKEDGEN_REGRESSION_READ_OPERATIONS:-2000000}
case "$pairs" in
    ''|*[!0-9]*|0) printf '%s\n' 'pairs must be a positive integer' >&2; exit 2 ;;
esac
case "$read_operations" in
    ''|*[!0-9]*|0) printf '%s\n' 'read operations must be a positive integer' >&2; exit 2 ;;
esac
test -x "$baseline"
if [ -e "$output" ]; then
    printf '%s\n' "refusing to replace existing results: $output" >&2
    exit 2
fi
mkdir -p "$output"
cargo build --locked --release --example bounded_cache_probe
cp "$baseline" "$output/baseline-probe"
cp "${CARGO_TARGET_DIR:-target}/release/examples/bounded_cache_probe" "$output/candidate-probe"
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
    printf 'pairs=%s\n' "$pairs"
    printf 'backend=%s\nscope=%s\nread_operations=%s\n' "$backend" "$scope" "$read_operations"
    printf '%s\n' 'features=default (empty); system allocator; randomized default hashers'
    printf 'RUSTFLAGS=%s\nCARGO_ENCODED_RUSTFLAGS=%s\n' "${RUSTFLAGS:-}" "${CARGO_ENCODED_RUSTFLAGS:-}"
} > "$output/environment.txt"
shasum -a 256 "$output/baseline-probe" "$output/candidate-probe" Cargo.toml Cargo.lock \
    src/cache.rs src/cache_arena.rs examples/bounded_cache_probe.rs \
    scripts/bounded_cache_regression.sh > "$output/hashes.txt"
git diff --binary > "$output/tracked-diff.patch"

for value_bytes in $sizes; do
    for workers in $worker_counts; do
        for workload in $workloads; do
            if [ "$value_bytes" -eq 64 ]; then
                entries=50000
                case "$workload" in
                    resident-hit|read-miss) operations=$read_operations ;;
                    *) operations=500000 ;;
                esac
            else
                entries=1024
                case "$workload" in
                    resident-hit|read-miss) operations=$read_operations ;;
                    *) operations=100000 ;;
                esac
            fi
            pair=1
            while [ "$pair" -le "$pairs" ]; do
                case $((pair % 2)) in
                    1) versions='baseline candidate' ;;
                    0) versions='candidate baseline' ;;
                esac
                for version in $versions; do
                    name="${workload}_v${value_bytes}_t${workers}_p${pair}_${version}"
                    uptime > "$output/$name.host-before.txt"
                    "$output/$version-probe" "$backend" "$workload" "$entries" \
                        "$operations" "$workers" "$value_bytes" "$pair" 0 \
                        > "$output/$name.csv" 2> "$output/$name.stderr"
                    uptime > "$output/$name.host-after.txt"
                    tail -n 1 "$output/$name.csv"
                done
                pair=$((pair + 1))
            done
        done
    done
done
