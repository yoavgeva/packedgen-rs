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
- **Storage and reclamation.** Entries live in slab chunks of 8 to 256
  slots, in `src/sharded_arena.rs`, under the direct arena's reader protocol
  and epoch state machine.
  - A writer pins, takes a vacant slot from its thread's stack for the
    cache, and fills it before taking the shard lock.
  - After the lock, still pinned, it queues the entries it unpublished in its
    thread's retired queue. Each carries the generation token read after
    the unpublication, aged three advances. The advance that reuses the
    tag's epoch slot found that slot and the next one quiescent, the direct
    arena's two-rotation aging.
  - In the same queue lock, the writer takes up to 16 aged entries, drops
    their values, and pushes the slots onto its own stack, so it refills
    slots whose lines it just touched.
  - Every 64 retirements a writer also drains one other queue, so a thread
    that stops writing does not strand its retirements. `maintain()` drains
    every queue.
  - A thread's stack overflows into a shared depot, draws from it in batches
    of 32, and returns to it when the thread exits. Chunks are freed when the
    cache drops; a stack left for a dropped cache is forgotten without
    touching its slots.
  - The generation advances once 512 retirements are pending across the
    cache. A failed try waits for 256 more.
- **Reads.** `get` pins per call and clones the value. `pin()` returns a guard
  for borrowed reads that pins once.
- **Weights.** Weights come from a `Weighter`. A value heavier than its shard's
  share of the weight limit is refused, and any previous value for its key is
  removed, so readers never see a value the caller replaced.

## API beyond get and insert

- **Loading once.** `get_or_insert_with` and `get_or_try_insert_with` take a
  borrowed key and a loader.
  - A caller that misses registers a flight, keyed by hash and key, in its
    shard's lane and runs the loader outside every lock.
  - Callers that miss the same key while it loads wait on the flight and
    share its value.
  - The leader inserts the value before unlisting the flight. A caller that
    finds no flight looks the key up again under the lane lock, so it either
    joins the flight or sees the inserted value.
  - If the loader fails or panics, the flight is abandoned and one waiter
    loads again with its own loader.
- **Expiry.** `insert_with_ttl` and the builder's `time_to_live` store a
  deadline in the entry's 31-bit expiry field, in 100 ms ticks since the
  cache was built.
  - An entry stays readable for its time to live and at most one tick more.
  - Reads of entries without a deadline never read the clock.
  - Eviction removes an expired entry wherever it meets one, ignoring its
    access marks. When a full bucket pair forces an eviction, an expired
    entry in the pair goes first.
  - Expired entries the policy has not reached still count in `len`.
- **Statistics.** `stats()` reports evictions and expirations, which shard
  writers count under their locks. With `record_stats()`, it also reports hits
  and misses. Those are counted on 32 cache-line-padded stripes chosen by
  thread, so the read path adds one uncontended atomic increment.
  `peek` and `contains_key` neither count nor mark entries as read.
- **Removal listener.** A `RemovalListener` hears each entry that leaves, with
  a cause: `Size`, `Expired`, `Replaced`, or `Explicit`. The cache calls it
  after releasing the shard lock and before retiring the entry. It may use the
  cache, but must not load the removed key through `get_or_insert_with`: the
  removal can come from that key's own load on the same thread. A listener
  that panics still leaves every removed entry retired.
- **Clearing.** `clear` sweeps each shard under its lock. It retires every
  entry, resets the probation, main, and ghost state, and leaves the eviction
  and expiration counts in place.

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
  - an 8-thread race of replacements, removals, loads, clears, and evictions
    that counts live values to prove each one is dropped exactly once;
  - eight racing callers loading a missing key once;
  - failing and panicking loads handing the key to a waiter;
  - expired entries reading as missing, and leaving before read live keys;
  - statistics, every removal cause, `clear`, and `peek`.
- **Arena tests.** They cover:
  - aging of exactly three advances;
  - a pin blocking the second advance past its epoch;
  - `maintain()` dropping every removed value;
  - an exiting thread returning its vacant slots;
  - a dropped cache leaving other threads' stacks harmless;
  - other writers dropping an idle thread's retired values (verified to fail
    without the foreign drains).
- **Allocation contract.** `tests/sharded_cache_allocations.rs` counts
  allocations: 100,000 inserts into a warm, full cache must allocate fewer
  than 1,000 times. Boxed entries allocated about 50,000 times.
- **Loom models.** `tests/cache_concurrency_model.rs` model-checks the token
  rule. It covers a reader racing a pinned writer and a reclaimer, and a
  reader admitted at the epoch after the writer's pin. A negative control
  shows the model catches aging of one advance.
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

## Slab storage against boxed entries

Entries first lived in individually allocated boxes in the direct arena.
They now live in slab chunks (see "Storage and reclamation").

Process RSS per resident entry with 1,000,000 entries after 3,000,000
inserts. Unlike the requested-bytes table above, this includes allocator
headers and rounding:

| Host | Key, value | Boxed entries | Slab | quick_cache |
| --- | --- | ---: | ---: | ---: |
| Linux, glibc | `u64`, `u64` | 53.4 | 45.4 | 84.0 |
| Linux, glibc | `[u8; 32]`, `[u8; 64]` | 133.5 | 125.6 | 191.0 |
| macOS | `u64`, `u64` | 54.8 | 47.1 | 120.5 |
| macOS | `[u8; 32]`, `[u8; 64]` | 135.5 | 135.3 | 332.2 |

Throughput of slab storage against boxed entries, paired medians of three
runs with 50,000 entries and 64-byte `Arc<[u8]>` values; positive means the
slab is faster:

| Workload | Linux 1T | Linux 8T | macOS 1T | macOS 8T |
| --- | ---: | ---: | ---: | ---: |
| Cold fill | +3.8% | +3.0% | +9.4% | +1.4% |
| Replace | +10.2% | −5.8% | +13.1% | +5.6% |
| Delete and reinsert | +8.9% | −4.5% | +11.2% | −2.6% |
| Turnover (95% reads) | −2.1% | +2.5% | −7.7% | +0.2% |
| Resident hit | +23.8% | +11.2% | −17.9% | −23.6% |

With jemalloc on Linux, single-thread cells moved by 0 to +16% and
eight-thread cells by −5 to −10%.

The macOS resident-hit loss is a layout effect of the benchmark's
allocation order. It builds each value just before inserting it. A boxed
entry, allocated next, often reused the 64-byte block the value's `Vec` had
just freed. It then sat beside its value, and one prefetch covered both.
Slab slots sit apart from values.

A read microbenchmark separates the effects:

- Guard reads cost 11 ns and a pin 3.7 ns in both designs.
- A `get` cost 34 ns with boxes and 41 ns with slabs.
- With values allocated in a pass of their own, `get` costs matched within
  the noise.
- A `get` pays memory latency in full: its pin's `SeqCst` RMW keeps one
  call's misses from overlapping the next call's.

## Throughput against quick_cache 0.7

Measured with `bounded_cache_probe` on the slab storage: 32-byte keys,
64-byte `Arc<[u8]>` values, and 50,000 entries. Reads ran 16 million
requests and writes 8 million on macOS, and half that on Linux. Each figure is
the paired median of three runs; positive means `ShardedCache` is faster.

Hosts:
- macOS: a shared 16-core Apple Silicon machine.
- Linux: an aarch64 Docker VM on the same machine, using glibc's allocator.

| Workload | macOS 1T | macOS 2T | macOS 8T | Linux 1T | Linux 2T | Linux 8T |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Resident hit | −39% | −3% | +153% | +16% | +36% | +155% |
| Read miss | +84% | +174% | +638% | +72% | +155% | +530% |
| Turnover (95% reads) | −27% | −8% | +7% | −18% | −10% | +9% |
| Cold fill | −10% | −14% | −34% | −25% | −41% | −53% |
| Replace | −3% | −4% | −25% | −9% | −27% | −42% |
| Delete and reinsert | −14% | −11% | −10% | −27% | −31% | −23% |

Eight-thread resident hits reached 275 Mops/s on macOS against quick_cache's
109, and 233 against 94 on Linux.

Writes still trail. Lock-free reads require deferred reclamation: an insert
cannot reuse the evicted entry's memory until three epoch advances later,
where quick_cache frees under its shard lock. Every operation also pays an
epoch pin, a full barrier, and a write takes its shard lock and its thread's
retired-queue lock. A guard read through `pin()` took 11 ns at 50,000 entries,
against 34 to 41 ns for `get`, whose pin keeps one call's cache misses from
overlapping the next call's.

The earlier boxed-entry storage measured macOS resident hits at −32%, +2%,
and +241%. On macOS those boxes often sat beside their values (see "Slab
storage against boxed entries").

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
- **Boxed entries in the direct arena.** About one insert in two allocated:
  a thread's 256-box stash could not hold its 512-entry reclaim batches, and
  the stash interleaved entry frees with values in glibc's heap. See
  "Slab storage against boxed entries" for the comparison.
- **Retired queues per shard, under the shard lock.** Retiring needed no
  extra lock. But every thread writing a shard moved its queue's cache lines
  between cores, and eight-thread replace fell 45%.
- **Per-thread lanes within each shard's queue.** Each thread kept 64 lanes,
  one per shard, too many to stay cached: a lane's first loads missed on
  most visits.
- **A lock-free free stack per shard.** Popping a slot another core
  reclaimed missed under the shard lock, and the stack's head moved between
  cores on every write.
- **Reclaiming in batches of 32.** Freeing values together was meant to
  overlap their cache misses; eight-thread writes fell 12 to 16% instead.
- **64-byte slots.** Aligning slots to cache lines moved reads by −8 to +5%
  and cost 8 bytes per 56-byte entry.
- **Trying an advance every 32 retirements per shard.** Each try scans 128
  reader counters; eight-thread replace fell 63%. Tries now wait for 512
  retirements across the cache.

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
