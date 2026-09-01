#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

mode="${1:---quick}"
result_root="${PACKEDGEN_PROOF_RESULTS:-target/cache-proof}"
run_id="${PACKEDGEN_PROOF_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
result_dir="$result_root/$run_id"

quick() {
  cargo fmt --all -- --check
  cargo test --test direct_cache_differential
  cargo test --test direct_packed_cache
  cargo test --test cache_concurrency_model
  cargo clippy --all-targets --all-features -- -D warnings
}

msrv() {
  cargo +1.88.0 check --all-targets --all-features
  cargo +1.88.0 check -p packedgen-opthash --all-targets
}

append_csv() {
  local destination="$1"
  shift
  local temporary
  temporary="$(mktemp)"
  "$@" >"$temporary"
  if [[ ! -s "$destination" ]]; then
    cp "$temporary" "$destination"
  else
    tail -n +2 "$temporary" >>"$destination"
  fi
  cat "$temporary"
  rm "$temporary"
}

bench() {
  local entries="${PACKEDGEN_PROOF_ENTRIES:-200000}"
  local operations="${PACKEDGEN_PROOF_OPERATIONS:-2000000}"
  local write_operations="${PACKEDGEN_PROOF_WRITE_OPERATIONS:-$((operations / 5))}"
  local samples="${PACKEDGEN_PROOF_SAMPLES:-9}"
  local threads="${PACKEDGEN_PROOF_THREADS:-1 2 4 8 16}"
  local workloads="${PACKEDGEN_PROOF_WORKLOADS:-read_hit read_miss replace_hit insert_miss insert_miss_batch_32 remove_hit remove_miss touch_hit cache_mix_95 cache_mix_95_pressure cache_mix_95_hot_pressure}"
  local memory_entries="${PACKEDGEN_PROOF_MEMORY_ENTRIES:-100000 1000000}"
  local features="${PACKEDGEN_PROOF_FEATURES:-}"

  mkdir -p "$result_dir"
  : >"$result_dir/throughput.csv"
  : >"$result_dir/memory.csv"
  : >"$result_dir/latency.csv"

  for workload in $workloads; do
    for thread_count in $threads; do
      append_csv "$result_dir/throughput.csv" \
        cargo run --quiet --release --features "$features" --example cache_probe -- \
        "$entries" "$operations" "$thread_count" "$samples" "$workload"
    done
  done

  for count in $memory_entries; do
    append_csv "$result_dir/memory.csv" \
      cargo run --quiet --release --features "$features" --example memory_probe -- \
      direct-packed-cache-mixed-value64 "$count"
    append_csv "$result_dir/memory.csv" \
      cargo run --quiet --release --features "$features" --example memory_probe -- \
      direct-packed-cache-batch-admission-mixed-value64 "$count"
    append_csv "$result_dir/memory.csv" \
      cargo run --quiet --release --features "$features" --example memory_probe -- \
      papaya-cache-compact-mixed-value64 "$count"
    append_csv "$result_dir/memory.csv" \
      cargo run --quiet --release --features "$features" --example memory_probe -- \
      papaya-cache-inline-compact-mixed-value64 "$count"
  done

  append_csv "$result_dir/latency.csv" \
    cargo run --quiet --release --features "$features" --example cache_latency_probe -- \
    "$entries" "$operations" "$write_operations" 8 2 "$samples"

  printf 'PackedGen cache proof results: %s\n' "$result_dir"
}

soak() {
  PACKEDGEN_SOAK_OPERATIONS="${PACKEDGEN_SOAK_OPERATIONS:-2000000}" \
    cargo test --release --test direct_cache_differential \
    concurrent_reclamation_and_rebuild_soak -- --ignored --nocapture
}

miri() {
  cargo +nightly miri setup
  MIRIFLAGS="${MIRIFLAGS:--Zmiri-permissive-provenance}" \
    cargo +nightly miri test --lib \
    cache_arena::tests::stale_handle_cannot_read_reused_slot -- --exact
  MIRIFLAGS="${MIRIFLAGS:--Zmiri-permissive-provenance}" \
    cargo +nightly miri test --lib \
    cache_arena::tests::partial_direct_reservation_releases_only_unused_slots -- --exact
  MIRIFLAGS="${MIRIFLAGS:--Zmiri-permissive-provenance}" \
    cargo +nightly miri test --lib \
    cache_arena::tests::retired_value_drop_can_activate_first_mutation_on_the_same_arena -- --exact
  MIRIFLAGS="${MIRIFLAGS:--Zmiri-permissive-provenance}" \
    cargo +nightly miri test --lib \
    generation_map::tests::mutable_only_guarded_insert_automatically_uses_route_hash_and_preserves_length -- --exact
  MIRIFLAGS="${MIRIFLAGS:--Zmiri-permissive-provenance}" \
    cargo +nightly miri test --test direct_packed_cache \
    protected_value_survives_replacement_and_cache_drop -- --exact
  MIRIFLAGS="${MIRIFLAGS:--Zmiri-permissive-provenance}" \
    cargo +nightly miri test --test direct_packed_cache \
    miri_conditional_block_and_boxed_reclaimers_coexist -- --exact
  MIRIFLAGS="${MIRIFLAGS:--Zmiri-permissive-provenance}" \
    cargo +nightly miri test --test direct_packed_cache \
    miri_concurrent_replacement_and_reclamation_smoke -- --exact
  MIRIFLAGS="${MIRIFLAGS:--Zmiri-permissive-provenance}" \
    cargo +nightly miri test --test direct_packed_cache \
    guarded_admission_batches_keep_public_accounting_and_rebuild_exact -- --exact
  MIRIFLAGS="${MIRIFLAGS:--Zmiri-permissive-provenance}" \
    cargo +nightly miri test --features prepared-keys --test direct_packed_cache \
    prepared_replacement_pipeline_recycles_and_drops_values_exactly_once -- --exact
  MIRIFLAGS="${MIRIFLAGS:--Zmiri-permissive-provenance}" \
    cargo +nightly miri test --test direct_packed_cache \
    unbounded_admission_batch_publishes_length_immediately -- --exact
}

sanitizers() {
  local target
  target="${PACKEDGEN_SANITIZER_TARGET:-$(rustc +nightly -vV | awk '/^host:/ { print $2 }')}"
  RUSTFLAGS="-Zsanitizer=address" RUSTDOCFLAGS="-Zsanitizer=address" \
    cargo +nightly test --target "$target" --test direct_packed_cache -- --test-threads=1
  RUSTFLAGS="-Zsanitizer=thread" RUSTDOCFLAGS="-Zsanitizer=thread" \
    cargo +nightly test -Zbuild-std --target "$target" \
    --test direct_packed_cache concurrent_replacement_and_pinned_reads_stay_valid -- --exact
  RUSTFLAGS="-Zsanitizer=thread" RUSTDOCFLAGS="-Zsanitizer=thread" \
    cargo +nightly test -Zbuild-std --target "$target" --features prepared-keys \
    --test direct_packed_cache \
    concurrent_prepared_replacement_batches_recycle_without_lost_values -- --exact
}

memory_soak() {
  local entries="${PACKEDGEN_SOAK_ENTRIES:-200000}"
  local cycles="${PACKEDGEN_SOAK_CYCLES:-50}"
  local threads="${PACKEDGEN_SOAK_THREADS:-8}"
  local features="${PACKEDGEN_SOAK_FEATURES:-gxhash}"
  mkdir -p "$result_dir"
  : >"$result_dir/memory-soak.csv"
  append_csv "$result_dir/memory-soak.csv" \
    cargo run --quiet --release --features "$features" --example cache_memory_soak -- \
    direct "$entries" "$cycles" "$threads"
  append_csv "$result_dir/memory-soak.csv" \
    cargo run --quiet --release --features "$features" --example cache_memory_soak -- \
    papaya-inline "$entries" "$cycles" "$threads"
  printf 'PackedGen memory-soak results: %s\n' "$result_dir/memory-soak.csv"
}

endurance() {
  local entries="${PACKEDGEN_ENDURANCE_ENTRIES:-200000}"
  local seconds="${PACKEDGEN_ENDURANCE_SECONDS:-86400}"
  local threads="${PACKEDGEN_ENDURANCE_THREADS:-16}"
  local maximum_growth_bps="${PACKEDGEN_ENDURANCE_MAX_GROWTH_BPS:-2500}"
  local maximum_trend_bps="${PACKEDGEN_ENDURANCE_MAX_TREND_BPS:-500}"
  local report_every="${PACKEDGEN_ENDURANCE_REPORT_EVERY:-1000}"
  local features="${PACKEDGEN_ENDURANCE_FEATURES:-gxhash,jemalloc-probe}"
  local cpuset="${PACKEDGEN_ENDURANCE_CPUSET:-}"
  local -a command=(
    cargo run --quiet --release --features "$features" --example cache_memory_soak --
    direct "$entries" 0 "$threads" "$seconds"
  )
  if [[ -n "$cpuset" ]]; then
    if [[ "$(uname -s)" != "Linux" ]] || ! command -v taskset >/dev/null 2>&1; then
      printf 'PACKEDGEN_ENDURANCE_CPUSET requires Linux taskset\n' >&2
      return 2
    fi
    command=(taskset --cpu-list "$cpuset" "${command[@]}")
  fi
  mkdir -p "$result_dir"
  : >"$result_dir/endurance.csv"
  {
    uname -a
    rustc -vV
    printf 'entries=%s\nseconds=%s\nthreads=%s\nmaximum_growth_bps=%s\nmaximum_trend_bps=%s\nreport_every=%s\nfeatures=%s\ncpuset=%s\n' \
      "$entries" "$seconds" "$threads" "$maximum_growth_bps" "$maximum_trend_bps" "$report_every" "$features" "$cpuset"
    if command -v lscpu >/dev/null 2>&1; then
      lscpu
    fi
  } >"$result_dir/endurance-environment.txt"
  PACKEDGEN_SOAK_MAX_GROWTH_BPS="$maximum_growth_bps" \
    PACKEDGEN_SOAK_REPORT_EVERY="$report_every" \
    _RJEM_MALLOC_CONF="${_RJEM_MALLOC_CONF:-narenas:1,dirty_decay_ms:0,muzzy_decay_ms:0,tcache:false}" \
    append_csv "$result_dir/endurance.csv" \
    "${command[@]}"
  awk -F, -v expected_seconds="$seconds" -v maximum_trend_bps="$maximum_trend_bps" '
    NR > 2 {
      live[++samples] = $9
      final_elapsed = $3
    }
    END {
      if (samples < 2) {
        print "endurance analysis needs at least two post-baseline samples" > "/dev/stderr"
        exit 1
      }
      if (final_elapsed + 0.001 < expected_seconds) {
        printf "endurance stopped early: %.3f < %.3f seconds\n", final_elapsed, expected_seconds > "/dev/stderr"
        exit 1
      }
      first = int(samples / 10) + 1
      middle = int((first + samples) / 2)
      for (row = first; row <= middle; row++) {
        early_sum += live[row]
        early_count++
      }
      for (row = middle + 1; row <= samples; row++) {
        late_sum += live[row]
        late_count++
      }
      early = early_sum / early_count
      late = late_sum / late_count
      printf "endurance live-byte trend: early=%.0f late=%.0f limit=+%.2f%%\n", early, late, maximum_trend_bps / 100.0
      if (late * 10000 > early * (10000 + maximum_trend_bps)) {
        print "endurance live-memory trend exceeded the configured bound" > "/dev/stderr"
        exit 1
      }
    }
  ' "$result_dir/endurance.csv"
  printf 'PackedGen endurance results: %s\n' "$result_dir/endurance.csv"
}

case "$mode" in
  --quick)
    quick
    ;;
  --msrv)
    msrv
    ;;
  --bench)
    bench
    ;;
  --soak)
    soak
    ;;
  --miri)
    miri
    ;;
  --sanitizers)
    sanitizers
    ;;
  --memory-soak)
    memory_soak
    ;;
  --endurance)
    endurance
    ;;
  --all)
    quick
    msrv
    bench
    soak
    ;;
  *)
    printf 'usage: %s [--quick|--msrv|--bench|--soak|--miri|--sanitizers|--memory-soak|--endurance|--all]\n' "$0" >&2
    exit 2
    ;;
esac
