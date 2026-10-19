# PackedGen

PackedGen's main component is `ShardedCache`, a bounded concurrent cache for
any `K: Hash + Eq`. Reads are lock-free, per-shard tables are sized once and
never rebuilt, and eviction follows S3-FIFO. Its overhead beyond the key and
value is about 30 requested bytes per entry.

The repository also holds research data structures: packed generational
maps, lock-free and lock-based concurrent maps, the byte-keyed
`DirectPackedCache`, and paper-derived Elastic and Funnel hashing backends.
They are experimental; see [Research components](#research-components).

PackedGen currently supports 64-bit targets only. The caches store native
pointers in compact 64-bit handles, and unsupported pointer widths fail with
an explicit compile-time error rather than relying on incidental layout or
shift failures.

## ShardedCache

Writers take per-shard locks; readers take none. The shards share one table
of six-slot buckets of tags and handles to epoch-reclaimed entries, and every
key lives in one of two buckets. Overhead beyond the key and value is 29.1 to
33.3 requested bytes per entry, against 78 to 210 for quick_cache 0.7.

Against quick_cache, ShardedCache leads concurrent reads. With eight threads
on a 16-core machine it ran 2.4 to 3.8 times quick_cache's resident-hit
throughput and 6.5 to 7.3 times its miss throughput, and it was 42 to 46%
ahead on a 95%-read turnover mix. At eight threads it was also 27 to 29%
ahead on deleting and reinserting keys, between 14% behind and 22% ahead on
replacing them, and 12 to 22% behind on inserting new keys into a full cache.
On one thread, resident hits ranged from 12% behind to even, and writes from
5% ahead to 21% behind; much of that write gap is epoch reclamation's
bookkeeping, queuing, tagging, and later freeing each removed entry. With
1,000,000 entries, one thread trailed by about 14% on hits and 59% on
misses, as its bucket table outgrew the caches. In
process memory, 1,000,000 entries took 46 to 136 bytes each, against 83 to
332 for quick_cache. Timing every operation at
1,000,000 entries, its p99.999 latency was 1.1 to 22 times shorter than
quick_cache's, and 2.3 to 14 times shorter from p99.9 at eight threads; on
one thread its p99 trailed by up to two 42 ns timer ticks.

Each shard sizes its probationary queue to the keys it sees return. On the
ARC paper traces, its summed hit rate over 18 cells was 12.5 points above
single-shard quick_cache and 9.5 above quick_cache's sharded cache.

Design, correctness evidence, tables, and rejected alternatives are in
[`docs/SHARDED_CACHE_2026-10-17.md`](docs/SHARDED_CACHE_2026-10-17.md).

It offers:

- `get_or_insert_with` and `get_or_try_insert_with`, which load a missing key
  once however many callers miss it together;
- per-entry and default times to live;
- hit, miss, eviction, and expiration counts;
- a removal listener told why each entry left;
- `peek`, `contains_key`, `clear`, and `maintain`, which drops removed
  values promptly after a burst of writes;
- weighted limits and borrowed reads through `pin()`.

```rust
use std::time::Duration;

use packedgen::{RemovalCause, ShardedCache, ShardedCacheBuilder};

let cache: ShardedCache<u64, String> = ShardedCache::new(10_000);
cache.insert(1, "one".to_owned());
assert_eq!(cache.get(&1).as_deref(), Some("one"));

// Weighted limits, and borrowed reads that pin once for many lookups.
let weighted = ShardedCacheBuilder::new(10_000)
    .max_weight(1 << 20)
    .weighter(|_key: &u64, value: &String| value.len() as u64)
    .build();
weighted.insert(2, "two".to_owned());
let guard = weighted.pin();
assert_eq!(guard.get(&2).map(String::as_str), Some("two"));

// Expiring entries loaded once per miss, with counts and a removal listener.
let sessions: ShardedCache<u64, String, _, _, _> = ShardedCacheBuilder::new(10_000)
    .time_to_live(Duration::from_secs(60))
    .record_stats()
    .removal_listener(|key: &u64, _value: &String, cause: RemovalCause| {
        if cause == RemovalCause::Size {
            println!("evicted session {key}");
        }
    })
    .build();
let name = sessions.get_or_insert_with(&3, || "three".to_owned());
assert_eq!(name, "three");
assert_eq!(sessions.stats().misses, 1);
```

### Maturity

`ShardedCache` is new. Unit tests, an 8-thread race that counts live values,
Miri, and the address and thread sanitizers cover it (`scripts/cache_proof.sh`),
but it has no production deployments yet and its API may change before 1.0.
Its benchmarks so far ran on one 16-core Apple Silicon machine and on a Linux
VM hosted there.

## Research components

These components are research and comparison code, not recommended for
production use. Their status, measurements, examples, and the claims they
must still prove are in [`docs/RESEARCH_STATUS.md`](docs/RESEARCH_STATUS.md),
and their design is in [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

- **Packed generational maps.** `PackedGenMap`, `AtomicPackedGenMap`, and the
  `LockFree*` maps combine immutable PtrHash-indexed generations with
  lock-free mutable overlays and exact online rebuild publication.
- **Lock-based maps.** `ConcurrentSwissMap` shards a SegmentedSwiss table
  with packed keys behind per-shard read-write locks.
- **Byte-keyed caches.** `DirectPackedCache` caches byte-string keys over the
  packed index, with times to live and admission policies; the segment
  caches store values in packed segments.
- **Paper-derived hashing.** The `opthash` workspace crate implements Elastic
  and Funnel hashing as comparison backends. The primary maps do not use
  them.

## Development

```text
cargo fmt --all -- --check
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
scripts/cache_proof.sh --miri          # interpreter checks, ShardedCache included
scripts/cache_proof.sh --sanitizers    # address and thread sanitizers
cargo run --release --example cache_density_compare
cargo run --release --example bounded_cache_probe -- sharded turnover 50000 8000000 8 64 1
scripts/audit.sh                       # the complete local release audit
```

The research components' probes are listed in
[`docs/RESEARCH_STATUS.md`](docs/RESEARCH_STATUS.md#research-probes).

## Acknowledgements

- Martín Farach-Colton, Andrew Krapivin, and William Kuszmaul, authors of the
  elastic/funnel hashing paper.
- Aaron Ang and the `opthash-rs` contributors, whose Apache-2.0 implementation
  is retained as the owned paper-derived comparison backend.
