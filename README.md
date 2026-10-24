# PackedGen

PackedGen's main component is `ShardedCache`, a bounded concurrent cache for
any `K: Hash + Eq`. Reads are lock-free, per-shard tables are sized once and
never rebuilt, and eviction follows S3-FIFO. Its overhead beyond the key and
value is about 24 requested bytes per entry at 1,000,000 entries.

The repository also holds research data structures: packed generational
maps, lock-free and lock-based concurrent maps, the byte-keyed
`DirectPackedCache`, and paper-derived Elastic and Funnel hashing backends.
They are experimental; see [Research components](#research-components).

PackedGen currently supports 64-bit targets only. The research caches store
native pointers in compact 64-bit handles, and unsupported pointer widths
fail with an explicit compile-time error rather than relying on incidental
layout or shift failures.

## ShardedCache

Writers take per-shard locks; readers take none. The shards share one table
of 64-byte buckets, each with ten tag bytes and ten four-byte handles to
epoch-reclaimed entries in one slab region per cache, and every key lives in
one of two buckets. Overhead beyond the key and value is
23.7 to 24.1 requested bytes per entry at 1,000,000 entries and 26.9 to 31.0
at 100,000, against 78 to 210 for quick_cache 0.7.

Against quick_cache, ShardedCache leads concurrent reads. With eight threads
on a busy 16-core machine at 50,000 entries, it ran 2.3 to 3.2 times
quick_cache's resident-hit throughput and 4.7 to 5.9 times its miss
throughput, and it was 51 to 68% ahead on a 95%-read turnover mix, 37 to 44%
ahead on deleting and reinserting keys, 5 to 22% ahead on replacing them,
and 7 to 15% behind on inserting new keys into a full cache. Upserting
resident keys into an exactly full cache, it was 10% ahead with eight
threads and 11% ahead on one, and 56 to 58% ahead at 1,000,000 entries.
Reading through `get_or_insert_with`, it was 32% ahead with eight threads
and level on one (−3%), and at 1,000,000 entries 61% and 13% ahead. On one
thread,
resident hits were level (−1 to −4%) and misses 79 to 87% faster; writes
ranged from 11% ahead to 9% behind on Apple Silicon and trailed by 16 to 23%
on Linux. Much of the single-thread write gap is epoch reclamation's
bookkeeping: queuing, tagging, and later freeing each removed entry. Those
runs use 32-byte keys and 64-byte `Arc` payloads, which cost both caches
hashing and reference counts. With `u64` keys and values, one thread's
upserts into a full cache ran at 0.4 to 0.5 times quick_cache's throughput
on macOS and Linux, as each writes a new entry and retires the old one
where quick_cache overwrites in place. At eight threads those upserts ranged
from 15% behind to 31% ahead, and hits ran 7 to 9 times quick_cache's. With
1,000,000 entries on Apple Silicon, one thread was level on hits (+2%),
ahead on writes by 12 to 47%, and 50% behind on misses, whose tag words
share cache lines with the handles; eight threads led on every workload, by
4 to 109%. On a 4-vCPU x86_64 runner, four threads led on reads and turnover
at 50,000 entries, and one thread trailed by 21 to 36% on writes. In
process memory, before four-byte handles, 1,000,000 entries took 46 to 136
bytes each, against 83 to 332 for quick_cache. Timing every operation at
1,000,000 entries, its tail from p99.9 on was the shorter one in every cell:
up to 16 times shorter at p99.999, and 3 to 17 times shorter from p99.9 at
eight threads; on one thread its p99 was level or one 42 ns timer tick
behind.

Each shard sizes its probationary queue to the keys it sees return. On the
ARC paper traces, its summed hit rate over 18 cells was 12.5 points above
single-shard quick_cache and 9.5 above quick_cache's sharded cache.

On [mokabench](https://github.com/moka-rs/mokabench), moka's trace-replay
load generator, over 16 cells of six ARC traces, ShardedCache's throughput
against quick_cache had a geometric mean of −1% at one client, +22% at
eight, and +51% at sixteen on Apple Silicon; −5% at one client to +35% at
sixteen on Linux; and −5% at one client to +21% at four on a 4-vCPU x86_64
runner. It lost mostly where reads miss and most operations insert and
evict: by up to 30% at one client, and on Linux by up to 29% with more.
moka's sync cache ran 2.4 to 22 times slower.

Design, correctness evidence, tables, and rejected alternatives are in
[`docs/SHARDED_CACHE_2026-10-17.md`](docs/SHARDED_CACHE_2026-10-17.md).

It offers:

- `get_or_insert_with` and `get_or_try_insert_with`, which load a missing key
  once however many callers miss it together, and `get_or_insert_async` and
  `get_or_try_insert_async`, which wait for that load without blocking, on
  any executor; `get_value_or_guard`, with a timeout, and
  `get_value_or_guard_async` hand the caller a guard to insert through
  instead; a write to a key while it loads supersedes the load, so a value
  read from its source before an invalidation is never cached after it;
- per-entry and default times to live, or a time to idle that reads extend;
- hit, miss, eviction, and expiration counts;
- a removal listener told why each entry left;
- `iter` over cloned entries, or borrowed ones through `pin()`, and
  `retain`, whose predicate may write to the cache;
- `remove_if`, which decides and removes under one lock, and `replace`,
  which writes only a key that holds a live value;
- `drain`, and `memory_used`, which reports the table, entry slots,
  policy state, and bookkeeping in bytes;
- `peek`, `contains_key`, `clear`, and `maintain`, which drops removed
  values promptly after a burst of writes;
- weighted limits, `set_capacity` and `set_max_weight` to resize at run
  time, and borrowed reads through `pin()`.

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
and CI runs its tests on Linux, macOS, and Windows. It has no production
deployments yet, and its API may change before 1.0.
Its benchmarks so far ran on one 16-core Apple Silicon machine, on a Linux
VM hosted there, and on GitHub's x86_64 runners.

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
