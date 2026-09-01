# Concurrent ETS-like map experiment

Date: 2026-07-15

## Outcome

`ConcurrentSwissMap` is the first usable multi-reader/multi-writer backend in
this repository. It is not a complete ETS replacement, but it removes the
single-writer restriction for point operations without giving up the packed-key
RAM advantage.

The design is deliberately simple:

- a power-of-two number of cache-line-aligned shards;
- one read/write lock, SegmentedSwiss table, and packed-key arena per shard;
- one hash reused for shard selection and the inner Swiss operation;
- shard-local length counters, avoiding one globally contended write counter;
- shard bits selected below SwissTable's seven-bit SIMD tag, preserving both
  tag entropy and low bucket-index entropy;
- no global lock for lookup, insert, update, upsert, or remove.

Two keys in different shards can be written simultaneously. All operations on
one key are linearizable because that key always routes to the same shard.

## ETS-like operation surface

| Need | Rust operation | Atomicity |
|---|---|---|
| lookup with owned result | `get_cloned` | shard read lock |
| lookup without value clone | `with_value` | closure runs under shard read lock |
| insert or replace | `try_insert` | one shard write lock |
| insert only if absent | `try_insert_new` | check and insert under one write lock |
| partial/conditional update | `update` | closure runs under one write lock |
| insert or update | `try_upsert_with` | one write-lock transaction |
| delete/take | `remove` | one shard write lock |
| race-free lazy-expiry delete | `remove_if` | predicate and remove under one lock |
| size | `len` | sum of shard-local atomics |
| whole-table reset | `clear` | locks every shard in index order |
| delete-churn reclamation | `compact_key_arenas` | one shard rewritten at a time |

User closures must be short and must not re-enter a write operation on the same
map. This is the same practical rule as DashMap guard-based operations: holding
a shard lock across arbitrary nested map work can deadlock.

## RAM result

One million 32-byte binary keys with `u64` values, 64 shards, balanced load:

| Backend | Requested B/entry | Allocations | Difference |
|---|---:|---:|---:|
| Concurrent SegmentedSwiss | **54.309** | 4,557 | **35.7% less RAM than DashMap** |
| DashMap with boxed binary keys | 84.438 | 1,000,066 | baseline |
| ordinary HashBrown with boxed keys | 84.429 | 1,000,002 | non-concurrent reference |

The concurrent layer costs about 0.72 B/entry over the earlier 53.59 B/entry
single-writer SegmentedSwiss result. Its 8-KiB per-shard arena segments bound
aggregate tail waste while still eliminating per-key allocation.

Reproduce:

```text
cargo run --release --example memory_probe -- concurrent-swiss-binary 1000000
cargo run --release --example memory_probe -- dashmap-binary 1000000
```

## Throughput result

The concurrent probe uses 200,000 preloaded binary keys, two million operations,
64 shards, and the median of three samples. Values are millions of operations
per second on the Apple M4 Max development machine. Delete-hit has 200,000
unique operations because every key can be successfully removed only once.

| Workload | ConcurrentSwiss 1T | DashMap 1T | global-lock HashBrown 1T | ConcurrentSwiss 8T | DashMap 8T | global-lock HashBrown 8T |
|---|---:|---:|---:|---:|---:|---:|
| read hit | 21.84 | 21.79 | 32.54 | **83.57** | 79.72 | 12.00 |
| read miss | 24.17 | 45.44 | 54.13 | 87.28 | **93.38** | 15.88 |
| update hit | 15.84 | 20.58 | 23.17 | 42.26 | **55.49** | 9.35 |
| insert miss | **14.09** | 9.32 | 10.12 | 30.57 | **41.60** | 4.97 |
| delete hit | 16.46 | 25.35 | **32.21** | **40.64** | 34.38 | 9.46 |
| delete miss | 25.74 | 48.03 | 47.29 | **91.94** | 89.76 | 24.51 |
| 90% hit / 5% miss / 3% update / 1% insert / 1% delete | 12.84 | 19.42 | **21.63** | 39.31 | **77.30** | 8.04 |

The important result is nuanced. Pure concurrent reads reach DashMap parity,
some delete cases win, and every sharded design decisively beats one global
lock once writes are concurrent. DashMap is still substantially better for the
eight-thread cache mix and distributed updates/inserts. ConcurrentSwiss is
therefore the RAM leader in this comparison, not yet the overall throughput
leader.

The benchmark is a development smoke test, not a cross-machine claim. It does
not report p99 latency or pin threads. Linux CPU-pinned and tail-latency runs
remain release gates.

Reproduce one workload or the full matrix:

```text
cargo run --release --example concurrent_matrix -- \
  200000 2000000 8 64 cache_mix_90r5m3u1i1d
cargo run --release --example concurrent_matrix -- 200000 500000 8 64
```

## Delete churn

Packed arenas are append-only on the request path, so a remove immediately
drops the table entry but initially leaves its key bytes retained. The new
`compact_key_arenas` maintenance operation fixes unbounded churn: it copies live
keys into a fresh arena, updates packed references without rebuilding Swiss
tables, and processes one shard at a time. Other shards remain available.

Compaction is fallible and staging is atomic within the current shard. If a
later shard fails to allocate, earlier shards remain compacted and the failing
shard remains unchanged. Peak memory temporarily includes its old and new
arena plus eight bytes per live entry in that shard.

## What is still missing versus ETS

This phase intentionally covers concurrent point operations, not the complete
Erlang table model. The follow-up packed generation experiment now implements
lock-free reads and safe publication; see
`EXPERIMENT_LOCKFREE_GENERATIONS.md`. Remaining capabilities include:

- table traversal, match specifications, `select`, and continuation cursors;
- `set`/`ordered_set`/`bag`/`duplicate_bag` variants;
- owner/heir lifetime behavior and process-level access permissions;
- consistent snapshots while writes continue;
- automatic per-shard compaction thresholds and background scheduling;
- proven p99 behavior under a slow writer or allocator stall;
- Loom/model checking of the safe generation publication protocol.

The next-generation experiment preserved packed density and replaced
read-side locking with ArcSwap publication plus a Papaya epoch-reclaimed
overlay. Striped atomic generation routing now also removes the point-write gate
and changed-key replay. It reached a strong concurrent-read result, but base
updates/deletes remain slower than the lock-based controls.
