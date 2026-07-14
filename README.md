# ElasticHash

ElasticHash is an experimental Rust project for turning elastic hashing into a
database-usable, observable hash index. It is aimed at large, pre-sized,
binary-keyed indexes that need to operate above 95% occupancy.

The mathematical basis is *Optimal Bounds for Open Addressing Without
Reordering* by Farach-Colton, Krapivin, and Kuszmaul. The current core is pinned
to the paper-fidelity implementation in `opthash-rs`; this repository adds the
service contract that a storage engine needs: explicit capacity epochs,
capacity errors, lifecycle statistics, differential tests, and workload
benchmarks. We credit and preserve the upstream implementation rather than
presenting it as original work.

## Status

This repository is **not production-ready**. Version 0.1 is a single-writer,
fixed-epoch map. It deliberately rejects an absent insertion at its configured
live-entry limit instead of silently resizing. Updates to existing keys remain
valid at the limit. A one-byte-per-entry stable negative filter avoids running
the expensive exact query schedule for most missing keys.

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

The first measured result at one million entries is a 44% requested-byte saving
for `u64 -> u64` and a 29% saving for 32-byte boxed keys at `1/64` reserve versus
HashBrown 0.17.1. HashBrown remains much faster for in-cache operations; see the
baseline for the complete tradeoff.

## Development

```text
cargo fmt --all -- --check
cargo test --all-targets
cargo clippy --all-targets --all-features -- -D warnings
cargo run --release --example memory_probe -- all 1000000
cargo bench
```

## Acknowledgements

- Martín Farach-Colton, Andrew Krapivin, and William Kuszmaul, authors of the
  elastic/funnel hashing paper.
- Aaron Ang and the `opthash-rs` contributors, whose Apache-2.0 implementation
  is used as the pinned algorithmic core during the audit phase.
