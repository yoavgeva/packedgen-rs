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

At one million 32-byte binary keys, the accelerated packed `1/64` layout uses
62.533 requested bytes per entry versus HashBrown's 84.429: **25.9% less**. It
also removes the million per-key allocations. The two-way routing accelerator
reduced successful lookup from roughly 70 ns to 28 ns, but HashBrown remains
around 7 ns on this development machine. ElasticHash is therefore not yet
"better than SwissTable" overall; the release gates require the remaining
latency work and a RAM-limited system win.

## Development

```text
cargo fmt --all -- --check
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo run --release --example memory_probe -- all 1000000
cargo bench
```

## Acknowledgements

- Martín Farach-Colton, Andrew Krapivin, and William Kuszmaul, authors of the
  elastic/funnel hashing paper.
- Aaron Ang and the `opthash-rs` contributors, whose Apache-2.0 implementation
  is used as the pinned algorithmic core during the audit phase.
