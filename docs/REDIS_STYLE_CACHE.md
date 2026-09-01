# Redis-style embedded cache

Date: 2026-07-22

`DirectPackedCache<V>` can now serve as an embedded, Redis-style hot cache for
exact binary keys and arbitrary Rust values. It is not a Redis server: there is
no RESP endpoint, persistence, replication, clustering, or Redis data-type
layer. The useful comparison is the common `GET`/`SET`/`SETNX`/`DEL`/TTL and
bounded-eviction workload.

## Production-shaped API

The bulk constructor restores a snapshot directly into the compact frozen
generation. It avoids loading a full writable overlay and then retaining a
second generation during warmup.

```rust
use std::sync::Arc;
use std::time::Duration;
use packedgen::{CacheConfig, DirectPackedCache};

let records = (0_u64..1_000_000).map(|id| {
    let key = id.to_le_bytes();
    let value = [0_u8; 64];
    (key, value, 72, Some(Duration::from_secs(3_600)))
});

let cache = Arc::new(DirectPackedCache::try_from_entries_with_options(
    CacheConfig::new(u64::MAX)
        .with_max_entries(1_000_000)
        .with_overlay_capacity(65_536)
        .with_async_eviction(10_100),
    records,
)?);
let maintenance = cache.spawn_maintenance(Duration::from_secs(1));

// Redis-like operations:
let hit = cache.get(&42_u64.to_le_bytes());
cache.insert_discard_with_options(
    &7_u64.to_le_bytes(),
    [1_u8; 64],
    72,
    Some(Duration::from_secs(3_600)),
)?;
cache.remove_discard(&9_u64.to_le_bytes());

maintenance.shutdown();
# Ok::<(), Box<dyn std::error::Error>>(())
```

The input iterator must have an exact length. Duplicate keys, an invalid item
weight, or a preload exceeding the configured limits returns `CacheBuildError`;
already allocated values are reclaimed. Initial records do not inflate runtime
operation counters.

For heap-owning values, the supplied weight must include the key, the shallow
value, and owned heap data. `max_weight` is caller-accounted logical weight, not
a promise about allocator RSS. A fixed-size cache can use `max_entries`; a
variable-size cache should primarily use `max_weight`.

## Equal-live-memory Redis comparison

Machine: Apple M4 Max, 16 logical cores, macOS, Rust release build. Redis is
Homebrew Redis 8.8.0 with persistence disabled, one local server, binary
top-level string keys, one-hour per-key TTL, `allkeys-lfu`, and a 128 MiB
dataset-memory allowance. PackedGen uses 1% async capacity slack and jemalloc
configured with one arena, immediate dirty/muzzy decay, and no thread cache.

The catalog contains 2.187M mixed 8/16/24/32/48-byte binary keys. The mixed
trace is 95% reads, 2% updates, 2% unique admissions, and 1% deletes, with an
80/20 hot-key distribution and deliberate misses. PackedGen is an in-process
call with pipeline 1; Redis uses TCP pipeline 32. Consequently, the throughput
result measures the benefit of an embedded cache as well as the data structure.

| 10M-operation mixed trace | PackedGen | Redis 8.8 | Better |
| --- | ---: | ---: | --- |
| Throughput | **8.812 Mops/s median** (7.498–9.569) | 1.773 Mops/s | Higher |
| Read hit rate | **77.003% median** | 75.365% | Higher |
| Final entries | **1,249,541 median** | 803,643 | Higher |
| Live requested/used memory | 134.38 MB | **134.20 MB** | Lower |
| Process RSS growth | 156.27–157.11 MB | **152.85 MB** | Lower |

At nearly equal live memory, PackedGen retained about **55.5% more records**,
raised hit rate by about **1.64 percentage points**, and delivered about
**5.0x** the median command rate. Its tuned RSS was about 2.2–2.8% higher than
Redis in these runs. Without allocator tuning, macOS system malloc retained
far more construction and churn pages; allocator selection is therefore part
of the production configuration, not a cosmetic benchmark option.

## Operation matrix

Each focused row uses the same dataset, eight clients/workers, TTL, and memory
setup. PackedGen timing includes async-capacity settlement and, for unique
admission, the final compacting rebuild. Higher Mops/s is better.

| Operation | PackedGen Mops/s | Redis Mops/s | PackedGen / Redis |
| --- | ---: | ---: | ---: |
| Read hit | **50.984** | 2.011 | **25.4x** |
| Read miss | **133.699** | 2.420 | **55.2x** |
| Update hit | **1.922** | 1.210 | **1.59x** |
| Insert new at capacity | **0.587** | 0.536 | **1.10x** |
| Delete hit | **7.485** | 1.521 | **4.92x** |
| Delete miss | **78.841** | 1.958 | **40.3x** |

The narrowest lead is sustained unique admission. Removing a forced
every-256-batches full-generation scan improved the pre-settlement admission
path from 0.062 to 0.912 Mops/s at 1.25M entries. A tested fully dynamic Papaya
overlay was slower and retained more memory, so the adaptive atomic overlay
remains the default.

## Memory behavior and limits

- Bulk loading constructs the frozen key/value generation directly and starts
  with a small writable successor overlay.
- The atomic frozen map now carries values through the same in-place key
  permutation. It no longer collects a second full-key array before packing.
- Capacity eviction uses bounded physical samples. A whole logical scan is
  reserved for expiration discovery or the rare case where all physical
  samplers return empty; cache size no longer causes a scheduled O(n) cliff.
- Bulk-loaded and newly admitted values occupy reusable 256-slot arena blocks;
  known replacement candidates remain individually boxed to protect update
  latency. A native three-epoch quiescent-state collector reclaims both forms.
  Allocator bins, arenas, decay, and thread caches still affect physical RSS
  even when requested live bytes are unchanged.
- TTL resolution is 100 ms. Maintenance and lazy reads remove expired entries.
- Reads are lock-free. Writes are multiwriter, but same-key mutation stripes,
  eviction ownership, rebuilding, and the allocator mean the complete cache is
  not a lock-free system.

The 2026-08-09 blocker pass replaced the raced direct-cache `seize` collector,
restored complete-turnover density to a ten-cycle median of 110.925 B/entry
versus inline Papaya at 138.758, and removed the nonlinear admission cliff:
one-million and five-million strict new-key traces completed at 0.944 and
0.918 Mops/s. All five focused Miri tests, all 27 direct-cache ASan tests, and
the exact concurrent replacement/read TSan test now pass. Detailed evidence
and the current Papaya boundary are in `CACHE_PROOF.md`.

This is strong enough for an opt-in embedded hot tier and publication as an
experimental cache library. The remaining release gates are Linux-pinned
p50/p99/p999 latency and RSS, the now-scripted 24-hour churn run, allocator
matrices, observing the new Rust 1.88 job pass in remote CI, and higher
new-key/read-heavy mixed throughput without giving back density.

## Reproduce

```text
cargo build --release --example redis_memory_probe --features jemalloc-probe

_RJEM_MALLOC_CONF=narenas:1,dirty_decay_ms:0,muzzy_decay_ms:0,tcache:false \
  ./target/release/examples/redis_memory_probe \
  packedgen-workload 1249581 64 3600 10000000 8 2187000 32 128 mixed

./target/release/examples/redis_memory_probe \
  redis-workload 770000 64 3600 10000000 8 2187000 32 128 mixed
```

Replace `mixed` with `read-hit`, `read-miss`, `update-hit`, `insert-new`,
`delete-hit`, or `delete-miss` for focused rows. `delete-hit` requires no more
operations than initially live keys.
