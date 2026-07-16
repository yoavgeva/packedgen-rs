# PackedGen

PackedGen is an experimental Rust library for memory-dense concurrent maps
built from immutable packed generations, lock-free mutable overlays, atomic
value slots, and exact online rebuild publication.

The primary backend is not an implementation of the Elastic Hashing paper. It
uses PtrHash-indexed frozen generations plus lock-free overlays. The repository
retains an auditable, attributed `opthash` workspace crate implementing the
paper-derived Elastic and Funnel maps as explicit research and comparison
backends. The exact boundary is documented in
[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

## Status

This repository is **not production-ready**. Its primary
`AtomicPackedGenMap` has lock-free readers and point
writers, but still needs Loom/Miri/sanitizer verification, Linux-pinned p99
measurements, traversal/eviction APIs, and a stable public API.

`ConcurrentSwissMap` now provides a separate practical multi-reader,
multi-writer backend. Power-of-two shards independently own a read/write lock,
SegmentedSwiss table, packed-key arena, and live counter. Point reads and writes
never acquire a global lock; atomic insert-new, update, upsert, conditional
remove, and shard-by-shard key compaction cover the first ETS-like service
surface. It remains lock-based and does not yet implement ETS traversal, match
specifications, ownership, or bags.

The lock-free implementation has four variants. `LockFreeBinaryMap` is a fully
dynamic Papaya control. `LockFreeHybridMap` combines a dense immutable packed
base with a lock-free mutable overlay. `LockFreeGenerationMap` publishes those
generations through ArcSwap using stable generic value cells.
`AtomicPackedGenMap` (an alias for `LockFreeAtomicU64GenerationMap`) additionally
stores `NonMaxU64` values directly
in atomic frozen slots, so existing-key update/delete/reinsert operations need
no overlay allocation. New 32-byte keys are stored inline in lock-free overlay
nodes, avoiding the separate boxed-key allocation. Workloads with exact 8, 16,
24, 31, 40, 48, 56, or 64-byte keys can explicitly preallocate the matching
inline class; unexpected widths use the fully dynamic fallback. Miss-heavy
workloads can embed a nine-bit digest fingerprint into otherwise unused packed
reference bits for frozen keys up to 255 bytes. This adds no retained bytes and
rejects most misses before fetching member key bytes; longer-key generations
fall back to exact references. The ordinary hit-heavy policy remains
exact-only. Readers and point writers acquire no library-owned lock,
including during rebuilds. A striped atomic handoff keeps equal-key writes on
one generation until their predecessor stripe closes, then safely activates
the new overlay or direct atomic base. Per-stripe shadow state also lets clean
atomic-base hits bypass an unnecessary overlay lookup. Rebuild packs the stable
predecessor without changed-key replay. Whole-table maintenance can still
starve under nonstop writes, so this remains experimental rather than an ETS
replacement.

The opt-in `prepared-keys` feature adds 24-byte exact handles for explicitly
identified hot frozen keys. Preparation pays routing and PtrHash once;
subsequent reads, updates, replacements, and removals use a generation-checked
dense slot while it remains valid. Callers still provide the original key
bytes, and stale, wrong-key, cross-map, overlay, or rebuilt handles fall back
to the ordinary exact path. Allocation-free `prepare_key_batch` and
`get_prepared_batch` APIs amortize generation pinning for caller-owned request
batches and cheaply refresh a hot set after rebuild.

Write-heavy callers can additionally enable `prepared-batch-gate`. It adds 16
cache-line-separated writer gates—1,040 measured requested bytes per live
generation—to amortize update/replacement writer pinning once per batch. The
ordinary `prepared-keys` path keeps zero additional persistent batch state.

The current direct DashMap comparison is summarized in
[`docs/ATOMIC_VS_DASHMAP.md`](docs/ATOMIC_VS_DASHMAP.md). In the local warm
probe, the atomic generation uses 42.2% less frozen RAM and wins eight-thread
read-heavy cache mixes, while DashMap wins single-thread latency, new-key
insertion, and one-hot-key contention.

The broader current Rust concurrent-map comparison is in
[`docs/CONCURRENT_MAP_COMPARISON.md`](docs/CONCURRENT_MAP_COMPARISON.md). It
adds Papaya, `scc`, Flurry, and `RwLock<HashBrown>`, measures 1/2/4/8/12/16
worker scaling, and reports one-million-entry RAM. PackedGen is the density
winner and the prepared batch path leads several measured multicore read and
existing-key write workloads. Competing maps still win important scalar,
new-key, and contended-key cases.

`PackedBinaryMap` is the first database-oriented layout. It stores eight-byte
references in the table and immutable key bytes in a segmented arena. Its raw
lookup path hashes caller bytes once. Deletes cannot make the core re-hash a
reference as if it were the original key: tombstone cleanup is deferred and a
bounded, byte-aware routing rebuild preserves survivors.

Routing memory is explicit through `RouteCacheBudget`: the default adaptive
policy keeps one packed `u32` route slot per configured entry using four-way,
two-choice buckets; `Compact` disables direct routes, and `ReadOptimized`
reserves two physical route slots per entry. Capacity-aware location encoding
and a four-node relocation bound currently retain about 96.74% of adaptive
routes without creating a near-capacity insertion cliff.
Delete maintenance is synchronous by default. Single-writer services can select
`MaintenanceMode::Deferred`, observe `maintenance_due()`, and call `maintain()`
at a controlled boundary so table rebuild and arena compaction do not land on
the request that crosses the delete threshold. `PackedMapStats` exposes
maintenance runs, compaction-staging failures, and reclaimed arena capacity.
`try_begin_maintenance` plus `prepare_maintenance_step` can copy compacted key
bytes in bounded owner-selected slices before `finish_maintenance` performs the
remaining table cutover.
Structural mutation marks a staged plan stale; the writer can detect this and
restart allocation-safely. Replacing only a value keeps the plan valid because
packed key references do not move.
Every successful maintenance cutover advances an observable `PackedGeneration`;
exhaustive short writer-trace tests verify that a stale staged plan cannot
resurrect a removed key or discard an inserted key.
Deferred mode emits its soft maintenance signal at 25% deleted entries and
forces maintenance at 50% by default, bounding ignored tombstones and dead
arena bytes. `with_maintenance_threshold_percents` can select a stricter policy;
construction validates it and precomputes the exact entry counts exposed in
`PackedMapStats`.

The packed generation prototype now implements the core of the intended
production architecture:

1. one table per application shard;
2. a single ordered writer per shard;
3. lock-free readers protected by generation/epoch reclamation;
4. immutable entry records, atomically replaced on update;
5. background rebuild into a new table generation;
6. atomic generation cutover while the active writer overlay remains published;
7. explicit metrics for probes, rebuilds, tombstones, and bytes.

## Example

```rust
use packedgen::{ElasticConfig, FixedElasticMap, InsertOutcome};

let config = ElasticConfig::new(1_000_000)
    .with_reserve_exponent(6) // delta = 1/64, target occupancy ~98.4%
    .unwrap();
let mut map = FixedElasticMap::new(config);

assert_eq!(map.try_insert(b"key".to_vec(), 42), Ok(InsertOutcome::Inserted));
assert_eq!(map.get(b"key".as_slice()), Some(&42));
```

Services should prefer `PackedBinaryMap::try_new(config)`, which reports core
geometry, capacity, and every eager auxiliary allocation as `PackedBuildError`
instead of panicking.
`PackedBinaryMap::try_from_entries` atomically loads an iterator: duplicate keys
replace earlier values, while failure reports the input index and never exposes
the partial map.

The concurrent binary-key API uses owned lookup results by default:

```rust
use packedgen::ConcurrentSwissMap;

let map = ConcurrentSwissMap::with_capacity(1_000_000);
map.try_insert(b"key", 41_u64).unwrap();
map.update(b"key", |value| *value += 1).unwrap();
assert_eq!(map.get_cloned(b"key"), Some(42));
assert!(!map.try_insert_new(b"key", 99).unwrap());
```

The allocation-free atomic generation uses a checked value domain:

```rust
use packedgen::{AtomicPackedGenMap, NonMaxU64};

let map = AtomicPackedGenMap::try_from_entries(
    [(b"counter".as_slice(), NonMaxU64::new(0).unwrap())],
    128,
)
.unwrap();
assert_eq!(
    map.update(b"counter", |value| NonMaxU64::new(value.get() + 1).unwrap()),
    Some(NonMaxU64::new(1).unwrap())
);
assert_eq!(map.get(b"counter").map(NonMaxU64::get), Some(1));
```

With `--features prepared-keys`, repeated hot-key access can prepare a handle:

```rust
use packedgen::{AtomicPackedGenMap, NonMaxU64};

let map = AtomicPackedGenMap::try_from_entries(
    [(b"hot".as_slice(), NonMaxU64::new(7).unwrap())],
    0,
)
.unwrap();
let hot = map.prepare_key(b"hot");
assert_eq!(map.get_prepared(b"hot", &hot).map(NonMaxU64::get), Some(7));

let keys = [b"hot".as_slice()];
let handles = [hot];
let mut values = [None];
map.get_prepared_batch(&keys, &handles, &mut values);
assert_eq!(values[0].map(NonMaxU64::get), Some(7));
```

## What we must prove

The project will not claim a performance or RAM win from load factor alone.
Benchmarks must separate:

- slot-array savings from packed-key/value-layout savings;
- successful lookup from missing-key lookup;
- uniform from skewed and adversarial keys;
- steady fixed epochs from rebuild periods;
- payload bytes from index overhead;
- single-thread throughput from concurrent tail latency.

See [`docs/ROADMAP.md`](docs/ROADMAP.md) and
[`docs/FERRICSTORE.md`](docs/FERRICSTORE.md). The first deliberately unflattering
performance result is recorded in [`docs/BASELINE.md`](docs/BASELINE.md).
The concise pass/fail matrix is in [`docs/STATUS.md`](docs/STATUS.md).
Every successful and failed layout experiment is summarized in
[`docs/EXPERIMENT_MATRIX.md`](docs/EXPERIMENT_MATRIX.md).
The concurrent ETS-like experiment and its current limits are recorded in
[`docs/EXPERIMENT_CONCURRENT_ETS.md`](docs/EXPERIMENT_CONCURRENT_ETS.md).
The lock-free reader design, RAM/operation matrix, exact rebuild protocol, and
high-churn failure are recorded in
[`docs/EXPERIMENT_LOCKFREE_GENERATIONS.md`](docs/EXPERIMENT_LOCKFREE_GENERATIONS.md).

At one million 32-byte binary keys, the default adaptive packed `1/64` layout
uses about 54.991 requested bytes per entry versus HashBrown's 84.429 (34.9%
less); the read-optimized two-slot policy is about 59.022 B/entry (30.1% less).
It also removes the million per-key allocations. At one million keys, fixed
batches of 32 reduce Elastic lookup from roughly 66 ns to 54 ns/key, versus
~28 ns/key for batched HashBrown.
PackedGen is therefore not yet "better than SwissTable" overall; the release
gates require the remaining latency work and a RAM-limited system win.

`FrozenPackedMap` explores a second, immutable backend built on PtrHash 2.0.1.
It retains exact semantics by checking the original packed key after perfect
indexing. At one million 32-byte keys it used 49.004 requested bytes per entry,
42.0% less than HashBrown. With the opt-in `gxhash` hardware-AES feature, its
measured successful lookup was ~14.5 ns/key versus HashBrown's ~27.1 ns/key on
the same million-key corpus. Frozen construction is slower (~1.78 ms versus
~0.48 ms for 16K entries), so this backend deliberately trades build and
mutation support for density and read speed. The portable XXH3-128 path remains
the default; enable `gxhash` only on supported AES-capable targets.

The same-requested-RAM probe exposes allocation cliffs that a single
bytes-per-entry point hides. With a 64 MiB requested-allocation budget, packed
Elastic held 1,032,192 records versus HashBrown's 917,504 (**12.5% more**), but
its five-sample median successful lookup was 149–159 ns/key across two processes
versus 35–37 ns/key. This fails the project's 25%-more-records density gate and
the lookup gate; it is evidence of a useful capacity direction, not an overall
win. The RAM probe also includes segmented SwissTable and immutable PtrHash
backends as comparison baselines; they explore different tradeoffs and do not
claim to implement the paper's elastic placement schedule.

At one million 32-byte binary keys, the 64-shard concurrent SegmentedSwiss map
used 54.309 requested B/entry versus DashMap with boxed keys at 84.438, a 35.7%
reduction with about 4,557 allocations instead of one million. In the current
eight-thread smoke run, pure read hits were near DashMap (83.57 versus 79.72
million operations/s), while DashMap still led the cache-like mixed workload
(77.30 versus 39.31 million operations/s). This is a real concurrent RAM win,
not yet a general concurrent speed win.

The atomic packed generation used 48.761 requested B/entry both before and
after updating 1% of one million existing keys, versus DashMap at 84.438. Warm
existing-key updates allocate zero bytes. One million newly inserted 32-byte
keys used 58.951 requested B/entry with inline keys versus 82.931 for boxed-key
Papaya. Configured exact 16/31/40/64-byte inline classes reduced requested RAM
by 20.6–35.4% versus their boxed controls and halved allocation counts. Its
latest broad eight-thread smoke reached
110.802 M read hits/s and 90.610 Mops/s on the 90/5/3/1/1 cache mix, versus
DashMap at 68.060 and 71.301 respectively. Distributed update is close but
noisy (41.877–64.544 Mops/s in recent runs), while a single contended hot key
remains a clear CAS-retry loss. In million-entry generic rebuild probes,
median striped handoff was about 4–28 microseconds from no writers through a
four-writer whole-table sweep, and base replacement was about 2–16 microseconds.
The full ~178–196 ms packed build ran while point operations continued. There
is no changed-key replay cliff; mutation throughput and pinned p99 latency remain
open release gates.

## Development

```text
cargo fmt --all -- --check
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo run --release --example memory_probe -- all 1000000
cargo run --release --example ram_budget_probe -- 64 1000000
cargo run --release --example fixed32_experiment -- 1000000
cargo run --release --features gxhash --example hybrid_probe -- 1000000 1 2000000
cargo run --release --features gxhash --example operation_matrix -- 1000000 1000000 10000
cargo run --release --example simd_key_probe -- 100000 2000000 31 all
cargo run --release --example concurrent_matrix -- 200000 500000 8 64
cargo run --release --example concurrent_map_scaling_probe -- 100000 3000000 16 7 64
cargo run --release --example memory_probe -- concurrent-swiss-binary 1000000
cargo run --release --features gxhash --example memory_probe -- lockfree-generation-churn-binary 1000000
cargo run --release --features gxhash --example generation_rebuild_probe -- 1000000 1 4 10000
cargo run --release --features gxhash --example atomic_overlay_probe -- 100000 300000 8 21 32 insert_miss
cargo bench --features gxhash --bench mixed_workloads -- successful_lookup_binary_32
cargo bench
# or run the complete local release audit
scripts/audit.sh
```

## Acknowledgements

- Martín Farach-Colton, Andrew Krapivin, and William Kuszmaul, authors of the
  elastic/funnel hashing paper.
- Aaron Ang and the `opthash-rs` contributors, whose Apache-2.0 implementation
  is retained as the owned paper-derived comparison backend.
