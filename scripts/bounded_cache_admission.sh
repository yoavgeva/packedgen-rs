#!/bin/sh
# Unique-key cold fill only: cache-level upsert versus guarded conditional
# admission, with QuickCache in every cell. Synchronous limits stay unchanged.
set -eu

output=${1:?usage: sh scripts/bounded_cache_admission.sh NEW_OUTPUT_DIRECTORY}
pairs=${PACKEDGEN_ADMISSION_PAIRS:-6}
case "$pairs" in
    ''|*[!0-9]*|0) printf '%s\n' 'pairs must be a positive integer' >&2; exit 2 ;;
esac
if [ -e "$output" ]; then
    printf '%s\n' "refusing to replace existing results: $output" >&2
    exit 2
fi
mkdir -p "$output"
cargo build --locked --release --example bounded_cache_probe
cp "${CARGO_TARGET_DIR:-target}/release/examples/bounded_cache_probe" "$output/probe"
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
    printf '%s\n' 'features=default (empty); system allocator; randomized default hashers'
    printf '%s\n' 'unique cold-fill keys; synchronous default; same entry/weight limits'
    printf 'RUSTFLAGS=%s\nCARGO_ENCODED_RUSTFLAGS=%s\n' "${RUSTFLAGS:-}" "${CARGO_ENCODED_RUSTFLAGS:-}"
} > "$output/environment.txt"
shasum -a 256 "$output/probe" Cargo.toml Cargo.lock src/cache.rs src/cache_arena.rs \
    examples/bounded_cache_probe.rs scripts/bounded_cache_admission.sh \
    scripts/summarize_bounded_cache.awk > "$output/hashes.txt"
git diff --binary > "$output/tracked-diff.patch"

for value_bytes in 64 65536; do
    if [ "$value_bytes" -eq 64 ]; then
        entries=50000
        operations=500000
    else
        entries=1024
        operations=100000
    fi
    for workers in 1 2; do
        pair=1
        while [ "$pair" -le "$pairs" ]; do
            case $((pair % 3)) in
                1) backends='direct-owned quickcache direct-guarded-owned' ;;
                2) backends='quickcache direct-guarded-owned direct-owned' ;;
                0) backends='direct-guarded-owned direct-owned quickcache' ;;
            esac
            for backend in $backends; do
                name="cold-fill_v${value_bytes}_t${workers}_p${pair}_${backend}"
                uptime > "$output/$name.host-before.txt"
                "$output/probe" "$backend" cold-fill "$entries" "$operations" \
                    "$workers" "$value_bytes" "$pair" 0 \
                    > "$output/$name.csv" 2> "$output/$name.stderr"
                uptime > "$output/$name.host-after.txt"
                tail -n 1 "$output/$name.csv"
            done
            pair=$((pair + 1))
        done
    done
done
awk -v alternate_backend=direct-guarded-owned -v expected_repetitions="$pairs" \
    -v expected_configurations=4 -f scripts/summarize_bounded_cache.awk \
    "$output"/*.csv > "$output/summary.tsv"
