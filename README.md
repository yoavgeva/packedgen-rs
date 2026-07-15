# ElasticHash

ElasticHash is an experimental Rust project for turning elastic hashing into a
database-usable, observable hash index. It is aimed at large, pre-sized,
binary-keyed indexes that need to operate above 95% occupancy.

The mathematical basis is *Optimal Bounds for Open Addressing Without
Reordering* by Farach-Colton, Krapivin, and Kuszmaul. The repository owns an
auditable, attributed subtree of the paper-fidelity `opthash-rs` core and adds
the service contract that a storage engine needs: explicit capacity epochs,
packed binary keys, capacity errors, lifecycle statistics, differential tests,
and workload benchmarks. We preserve the upstream history and Apache-2.0
attribution rather than presenting that foundation as original work.

## Status

This repository is **not production-ready**. Version 0.1 is a single-writer,
fixed-epoch map. It deliberately rejects an absent insertion at its configured
live-entry limit instead of silently resizing. Updates to existing keys remain
valid at the limit. A one-byte-per-entry stable negative filter avoids running
the expensive exact query schedule for most missing keys.

`PackedBinaryMap` is the first database-oriented layout. It stores eight-byte
references in the table and immutable key bytes in a segmented arena. Its raw
lookup path hashes caller bytes once. Deletes cannot make the core re-hash a
reference as if it were the original key: tombstone cleanup is deferred and a
bounded, byte-aware routing rebuild preserves survivors.

Routing memory is explicit through `RouteCacheBudget`: the default adaptive
policy keeps small-map overhead within the measured memory ceiling, `Compact`
disables direct routes, and `ReadOptimized` reserves two route slots per entry.
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

The intended production architecture is:

1. one table per application shard;
2. a single ordered writer per shard;
3. lock-free readers protected by generation/epoch reclamation;
4. immutable entry records, atomically replaced on update;
5. background rebuild into a new table generation;
6. atomic generation cutover after replaying the writer delta;
7. explicit metrics for probes, rebuilds, tombstones, and bytes.

## Example

```rust
use elastichash::{ElasticConfig, FixedElasticMap, InsertOutcome};

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

At one million 32-byte binary keys, the accelerated packed `1/64` layout uses
62.533 requested bytes per entry versus HashBrown's 84.429: **25.9% less**. It
also removes the million per-key allocations. The two-way routing accelerator
reduced the small-fixture successful lookup from roughly 70 ns to 28 ns, versus
HashBrown around 7 ns. At one million keys, fixed batches of 32 reduce Elastic
lookup from ~66.2 to ~54.0 ns/key, versus ~27.9 ns/key for batched HashBrown.
ElasticHash is therefore not yet "better than SwissTable" overall; the release
gates require the remaining latency work and a RAM-limited system win.

The same-requested-RAM probe exposes allocation cliffs that a single
bytes-per-entry point hides. With a 64 MiB requested-allocation budget, packed
Elastic held 1,032,192 records versus HashBrown's 917,504 (**12.5% more**), but
its five-sample median successful lookup was 149–159 ns/key across two processes
versus 35–37 ns/key. This fails the project's 25%-more-records density gate and
the lookup gate; it is evidence of a useful capacity direction, not an overall
win.

## Development

```text
cargo fmt --all -- --check
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo run --release --example memory_probe -- all 1000000
cargo run --release --example ram_budget_probe -- 64 1000000
cargo bench
# or run the complete local release audit
scripts/audit.sh
```

## Acknowledgements

- Martín Farach-Colton, Andrew Krapivin, and William Kuszmaul, authors of the
  elastic/funnel hashing paper.
- Aaron Ang and the `opthash-rs` contributors, whose Apache-2.0 implementation
  is used as the pinned algorithmic core during the audit phase.
