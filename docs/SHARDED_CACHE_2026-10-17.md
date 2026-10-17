# ShardedCache: lock-free reads without rebuilds

`ShardedCache<K, V, W, S>` is a bounded concurrent cache built to lead on
memory per entry and on concurrent reads. It also had to stay close to
quick_cache on writes and hit ratio. It sits beside `DirectPackedCache`
rather than replacing it. `DirectPackedCache` keeps byte-string keys,
TTLs, admission policies, and the packed index. `ShardedCache` takes any
`K: Hash + Eq`.

## Why a new index

Under churn, `DirectPackedCache` loses most of its write time to its index
design:

- Overlay slots are write-once, so every new key burns one.
- A full overlay triggers a PtrHash rebuild of the whole base.
- With probation evicting mostly new keys, fills rebuilt the base about every
  0.75 of the overlay's capacity in inserts. That was 30 to 39% of
  single-writer fill time.

Reusing overlay slots safely would mean changing the index's lock-free
protocol. The overlay compares inline key bytes, and a rewritten slot can be
read torn.

`ShardedCache` stores keys in the epoch-reclaimed entries instead of the table.
An entry never changes after it is published, so a reader compares keys only in
memory that its epoch pin keeps alive. A writer can therefore reuse a slot as
soon as it frees it. A reader racing that reuse sees either the old or the new
handle. Both are live for its epoch, and the reader checks the key in that
entry. Tables are sized once and never rebuilt.

## Design

- **Shards.** By default there are four per available thread, rounded to a power
  of two, and fewer for small caches so that each shard holds at least 64
  entries. A shard has a fixed table of 64-byte buckets and holds its share of
  the entry and weight limits. Writers take the shard's mutex.
- **Buckets.** A control word carries six tag bytes and an overflow count, and
  the bucket holds six atomic handles. Tags are matched with one word-wide
  zero-byte test.
- **Placement.** A key lives in its home bucket or one alternate bucket, so
  lookups touch at most two buckets. The home is one multiply of the key hash.
  The alternate comes from MurmurHash3's finalizer.
  - The home's overflow count is the number of its keys that live in their
    alternates. A lookup reads the alternate only when that count is nonzero.
  - When both buckets are full, the cache first moves one resident key to its
    own other bucket. The copy is published before the old slot is cleared, so
    readers always find it.
  - If no resident key can move, the cache evicts an unread entry from the two
    buckets. This happened on fewer than 0.1% of inserts into a full shard.
- **Eviction.** Each shard uses an S3-FIFO-style policy:
  - New keys wait in a probationary FIFO, 2% of the shard by default. Its
    records hold the entry handle and the key hash.
  - An unread probationary key leaves first and goes into a ghost filter. The
    ghost has four generations and remembers three eighths to half of the
    shard.
  - A key the ghost remembers is admitted straight to the main area.
  - The main area is a CLOCK queue in the order keys entered it: on leaving
    probation after being read, on overflowing probation's record cap, or on
    a ghost hit. A read key loses one of its two access marks and goes to the
    back.
  - Probation and main-queue records are slot records: a 24-bit slot plus that
    slot's placement generation, kept in each bucket's spare word. A record
    of a removed, moved, or reused slot is skipped when popped. Main records
    take 4 bytes each.
  - Past 1.25 times the shard's capacity, the main queue drops stale records
    from its front in chunks of 64. A table sweep remains the fallback when
    the queue is exhausted.
- **Reclamation.** Entries are boxes in the direct arena's epoch scheme.
  - Retirements are drained on the threads that retired them.
  - Emptied boxes go into a lock-free thread-local stash of up to 256 boxes,
    each at most 512 bytes. That thread's later allocations reuse them.
  - An advancing thread drains ready queues that have waited for about six
    rotations, so an idle thread cannot strand its retirements.
- **Reads.** `get` pins per call and clones the value. `pin()` returns a guard
  for borrowed reads that pins once.
- **Weights.** Weights come from a `Weighter`. A value heavier than its shard's
  share of the weight limit is refused, and any previous value for its key is
  removed, so readers never see a value the caller replaced.

## Correctness evidence

- **Unit tests.** They cover:
  - insert, replace, and remove;
  - borrowed keys;
  - the entry and weight bounds;
  - colliding keys that fill both buckets and unlink cleanly;
  - scan resistance;
  - drop releasing every value;
  - guard reads surviving replacement;
  - two-bucket placement under a million-key churn;
  - unread main-area keys leaving in insertion order;
  - forced pair evictions staying under 0.1%;
  - an 8-thread race of replacements, removals, and evictions that counts live
    values to prove each one is dropped exactly once.
- **Arena tests.** They prove that retired values are destroyed outside the
  reclaim gate and on the thread that retired them.
- **CI tools.** `scripts/cache_proof.sh` runs the module under Miri, ASan, and
  TSan.

## Memory

Requested live bytes per resident entry after inserting twice the capacity,
from `examples/cache_density_compare.rs` (`stats_alloc`, not RSS). Allocator
rounding of the boxed entries is not included.

| Entries | Key | Value | ShardedCache | DirectPackedCache | quick_cache |
| ---: | --- | --- | ---: | ---: | ---: |
| 100,000 | 8 B | `u64` | 46.7 | 46.8 | 118.0 |
| 100,000 | 32 B | `[u8; 64]` | 127.4 | 128.2 | 306.7 |
| 1,000,000 | 8 B | `u64` | 45.0 | 44.2 | 94.4 |
| 1,000,000 | 32 B | `u64` | 69.0 | 69.0 | 144.7 |
| 1,000,000 | 32 B | `[u8; 64]` | 125.1 | 125.1 | 245.4 |

Overhead beyond the key and value is 29 to 31.4 bytes per entry:

| Component | Bytes per entry |
| --- | ---: |
| Table at 80% load | 13.3 |
| Entry metadata | 8 |
| Main queue, with its slack | about 4 to 5 |
| Ghost filter | about 2 |
| Probation records | about 2 |

For comparison, the overhead is 28 to 32 for `DirectPackedCache` and 78 to 210
for quick_cache. The main queue added about 3.5 bytes per entry and bought
about 3.4 points of trace hit rate.

## Throughput against quick_cache 0.7

Measured with `bounded_cache_probe`: 32-byte keys, 64-byte `Arc<[u8]>` values,
and 50,000 entries. Reads ran 16 million requests and writes 8 million (4
million on Linux). Each figure is the paired median of three runs; positive
means `ShardedCache` is faster.

Hosts:
- macOS: a shared 16-core Apple Silicon machine.
- Linux: an aarch64 Docker VM on the same machine, using glibc's allocator.

| Workload | macOS 1T | macOS 2T | macOS 8T | Linux 1T | Linux 2T | Linux 8T |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Resident hit | −32% | +2% | +241% | +4% | +46% | +161% |
| Read miss | +66% | +137% | +626% | +53% | +152% | +500% |
| Turnover (95% reads) | −22% | −4% | +9% | −18% | −19% | +6% |
| Cold fill | −18% | −6% | −35% | −28% | −37% | −52% |
| Replace | −5% | +27% | −30% | −18% | −28% | −37% |
| Delete and reinsert | −10% | −3% | −3% | −25% | −32% | −18% |

Eight-thread resident hits reached 362 Mops/s against quick_cache's 109.

Writes trail at low thread counts. Lock-free reads require deferred
reclamation, so an insert cannot reuse the evicted entry's hot memory the way
quick_cache's slab does, and every operation pays an epoch pin, which is a
full barrier. A guard read through `pin()` took 22 ns at 50,000 entries,
against 34 ns for `get`.

## Hit ratio

The ARC paper traces were replayed through `hit_ratio_sim`; the traces are
cited, not committed. Values are read hit percentages at the default
configuration.

| Cell | ShardedCache | quick_cache | quick_cache sharded | DirectPackedCache |
| --- | ---: | ---: | ---: | ---: |
| OLTP 1,000 | 36.21 | 35.15 | 36.12 | 35.30 |
| OLTP 5,000 | 53.11 | 54.29 | 53.25 | 52.83 |
| OLTP 15,000 | 64.34 | 64.83 | 64.57 | 63.80 |
| P3 8,192 | 4.30 | 3.64 | 3.74 | 3.87 |
| P3 32,768 | 13.20 | 12.94 | 13.11 | 12.66 |
| P3 131,072 | 48.50 | 48.61 | 48.73 | 45.33 |
| S1 65,536 | 6.07 | 6.17 | 6.23 | 5.86 |
| S1 262,144 | 23.69 | 23.77 | 23.95 | 23.11 |
| S1 524,288 | 42.54 | 41.99 | 42.76 | 41.60 |
| P8 8,192 | 10.09 | 10.40 | 10.39 | 9.40 |
| P8 32,768 | 31.75 | 32.41 | 32.30 | 30.92 |
| P8 131,072 | 70.20 | 70.01 | 70.11 | 69.40 |
| S3 100,000 | 12.23 | 12.67 | 12.83 | 11.57 |
| S3 400,000 | 41.77 | 41.75 | 42.33 | 40.00 |
| S3 800,000 | 68.70 | 66.99 | 68.35 | 66.79 |
| DS1 1,000,000 | 14.94 | 14.86 | 14.86 | 14.36 |
| DS1 4,000,000 | 44.74 | 44.80 | 44.76 | 43.88 |
| DS1 8,000,000 | 68.07 | 69.22 | 69.31 | 68.20 |

Summed over the 18 cells, ShardedCache is 0.06 points behind single-shard
quick_cache, and DirectPackedCache is 15.43 behind. ShardedCache is within
about one point in every cell and ahead in 9. The largest gaps are OLTP at
5,000 entries (−1.19) and DS1 at 8,000,000 (−1.14).

The insertion-order main queue closed most of the gap. A CLOCK over the
table's own slots sweeps in hash order and loses recency: before the queue,
the sum was −3.44 and P3 at 131,072 entries read 46.80%.

## Rejected and reverted

- **Linear probing over buckets.** Its overflow counts were correct, but fill
  churn clustered the table. Within a million inserts, live keys sat hundreds
  of buckets from home and every overflow count saturated. Single-writer
  throughput fell from 8.7 to 4.3 Mops/s as runs grew from one million to
  sixteen million operations.
- **Two-bucket placement without relocation.** Evicting from full pairs
  overrode the policy and cost 15 points on the trace sum.
- **Raw hash bits for the buckets.** For one seed in forty, the fast hasher's
  halves correlated. That left 2.4% of inserts evicting from full pairs, and CI
  hit such a seed. The full finalizer on the home bucket fixed it but cost
  12–15% of single-thread reads, so only the alternate takes it.
- **A 10% probation share.** 2% read better on DS1, P3, and S3; for example,
  DS1 at 8,000,000 entries read 68.06% against 63.91%.
- **Lock-free replacement by compare-exchange.** It added a lookup to every
  new-key insert and did not move replace throughput, which was dominated by
  frees.
- **Looking keys up before taking the shard lock.** −6% on single-writer
  fills, no gain on replace.
- **Prefetching the next probation victim's bucket.** +4–7% on single-writer
  fills, −5% elsewhere.
- **A CLOCK over the table's slots as the main policy.** Hash order cost 3.4
  points of trace hit rate against the insertion-order queue.
- **Compacting the whole main queue in one pass.** Without evictions, as
  under delete and reinsert, a pass scanned thousands of stale records under
  the shard lock while other writers parked.
- **The direct arena's slab partitions for entries.** Under churn its partition
  locks and block lookups cost more than boxed entries: eight-thread turnover
  went from 20 to 46 Mops/s after switching to boxes.

## Reproduce

```sh
cargo run --release --example cache_density_compare
cargo build --release --example bounded_cache_probe --example hit_ratio_sim
PROBE=target/release/examples/bounded_cache_probe
$PROBE sharded turnover 50000 8000000 8 64 1
$PROBE quickcache turnover 50000 8000000 8 64 1
target/release/examples/hit_ratio_sim file:PATH/DS1.keys 8000000 0 1 sharded quick
cargo test --release --lib sharded_cache
```
