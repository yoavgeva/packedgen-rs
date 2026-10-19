#!/bin/sh
# Replays ARC paper traces through mokabench, the moka project's cache load
# generator, with a ShardedCache driver added by scripts/mokabench.diff. It
# clones mokabench at a fixed commit into target/mokabench, downloads the
# traces from moka's cache-trace repository into target/arc-traces, and
# appends mokabench's CSV rows to OUT, each after the trace and repetition.
# MOKABENCH_CACHES picks the caches: packedgen, quick_cache, moka,
# moka-async, moka-seg8; the default is packedgen,quick_cache,moka.
# usage: sh scripts/mokabench.sh REPS CLIENTS "TRACES" OUT
#   e.g. sh scripts/mokabench.sh 3 1,4,8 "s3 ds1 oltp" target/mokabench.csv
set -eu
reps=${1:-1}
clients=${2:-1,4,8}
traces=${3:-"s3 ds1 p3 p8 s1 oltp"}
out=${4:-target/mokabench.csv}
mokabench_commit=a0d61f64d007057a72b328c5d17cafc27c2905a1
trace_commit=ef0a9de8cf0202a1f2fee186a0af497774b0f0a9
root=$(cd "$(dirname "$0")/.." && pwd)
dir=$root/target/mokabench
traces_dir=$root/target/arc-traces

if [ ! -d "$dir/.git" ]; then
    git clone --quiet https://github.com/moka-rs/mokabench.git "$dir"
fi
git -C "$dir" checkout --quiet --force "$mokabench_commit"
rm -f "$dir/src/cache/packedgen.rs"
git -C "$dir" apply "$root/scripts/mokabench.diff"
cargo build --quiet --release --manifest-path "$dir/Cargo.toml" \
    --target-dir "$dir/target" --features packedgen,quick_cache

mkdir -p "$traces_dir"
for trace in $traces; do
    case $trace in
        concat) file=ConCat.lis ;;
        merge-p) file=MergeP.lis ;;
        merge-s) file=MergeS.lis ;;
        *) file=$(echo "$trace" | tr '[:lower:]' '[:upper:]').lis ;;
    esac
    if [ ! -f "$traces_dir/$file" ]; then
        curl --fail --silent --show-error --location --retry 3 \
            --output "$traces_dir/$file.zst" \
            "https://raw.githubusercontent.com/moka-rs/cache-trace/$trace_commit/arc/$file.zst"
        zstd --decompress --quiet --rm "$traces_dir/$file.zst"
    fi
done
ln -sfn "$traces_dir" "$dir/cache-trace/arc"

export MOKABENCH_CACHES="${MOKABENCH_CACHES:-packedgen,quick_cache,moka}"
for rep in $(seq 1 "$reps"); do
    for trace in $traces; do
        (cd "$dir" && ./target/release/mokabench --trace-file "$trace" --num-clients "$clients") \
            > "$dir/run.out"
        grep -E '^[A-Z][^{]*, [0-9]+, [0-9]+, ' "$dir/run.out" | sed "s/^/$trace,$rep,/" >> "$out"
    done
done
