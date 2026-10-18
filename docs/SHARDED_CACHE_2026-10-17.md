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
  entries. Caches large enough to give every shard at least 256 entries split
  further, up to sixteen shards per thread, so writers rarely wait on one
  shard's lock. A shard holds its share of the entry and weight limits, and
  writers take its mutex.
- **One bucket table.** All shards' 64-byte buckets form one table, in
  stretches of equal length. A read computes its bucket from the hash and two
  fields of the cache, without loading the shard.
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
    records hold the key's slot record, its hash, and the handle it was
    published with. A writer that takes a full shard's lock prefetches the
    next victim's bucket and entry together.
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
  - In the same queue lock, the writer takes as many aged entries as it
    retired, plus one while entries due since an earlier advance remain. It
    drops their values and pushes the slots onto its own stack, so it refills
    slots whose lines it just touched. Paced this way, frees keep step with
    the writes' allocations (see "Write path").
  - Every 16 retirements a writer visits one other queue. It drains it only
    if the queue's newest entry is aged, meaning its thread has retired
    nothing for three advances, and then returns to it until it is empty.
    `maintain()` drains every queue.
  - A thread's stack overflows into a shared depot, draws from it in batches
    of 32, and returns to it when the thread exits. Chunks are freed when the
    cache drops; a stack left for a dropped cache is forgotten without
    touching its slots.
  - The generation advances once 512 retirements are pending across the
    cache. A failed try waits for 256 more.
- **Reads.** `get` hashes the key, then pins and clones the value. Hashing
  before pinning lets the hash run while the previous call's unpin completes
  (see "Read path"). `pin()` returns a guard for borrowed reads that pins once.
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
    without the foreign drains), and leaving a retiring thread's alone;
  - writes reclaiming as many entries as they retire, and one more while a
    backlog is due;
  - a write's reclamation batch fitting one cache line.
- **Layout and sizing tests.** They check that a write's shard state spans
  two cache lines, that shards share one bucket table, that the default
  shard count gives every extra shard at least 256 entries, that a cache
  with more shards than entries stays bounded, and that probation records
  name the entries evictions prefetch.
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
| 100,000 | 8 B | `u64` | 48.1 | 46.8 | 118.0 |
| 100,000 | 32 B | `[u8; 64]` | 129.4 | 128.2 | 306.7 |
| 1,000,000 | 8 B | `u64` | 45.5 | 44.2 | 94.4 |
| 1,000,000 | 32 B | `u64` | 69.5 | 69.0 | 144.7 |
| 1,000,000 | 32 B | `[u8; 64]` | 125.6 | 125.1 | 245.4 |

Overhead beyond the key and value is 29.5 to 33.4 bytes per entry. On a
16-thread machine, a 100,000-entry cache now has 256 shards, and their fixed
state adds about 1.4 bytes per entry; at 1,000,000 entries it adds about 0.5:

| Component | Bytes per entry |
| --- | ---: |
| Table at 80% load | 13.3 |
| Entry metadata | 8 |
| Main queue, with its slack | about 4 to 5 |
| Ghost filter | about 2 |
| Probation records, 24 bytes each | about 2 to 3 |

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

After the write-path work, the slab figures are 45.8 and 126.0 on Linux
(quick_cache 82.9 and 191.3 in the same run) and 47.8 and 136.0 on macOS.

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
- A `get` paid memory latency in full: its pin's `SeqCst` RMW held back the
  next call's loads until this call's had finished. "Read path" covers the
  part of that it no longer pays.

## Read path

On one thread on macOS, `get` trailed quick_cache by 42% at 50,000 entries,
yet a borrowed read under one guard took 11 ns. At 2,000 entries, a
cycle-sampled profile placed 30% of `get`'s cycles at its unpin, waiting for
the value clone's atomic increment. quick_cache spent 42% of its cycles in
its key hash, which ran while its previous call's unlock completed.

A pin's `SeqCst` read-modify-write holds back the loads after it, the key's
included. `get` hashed under its pin, so a call could not start hashing until
the previous call's clone and unpin had finished. `get`, `peek`, and
`contains_key` now hash first. Resident hits on one thread of an M4 Max, in
millions per second:

| Entries | Hashing under the pin | Hashing first | quick_cache |
| ---: | ---: | ---: | ---: |
| 2,000 | 44 | 59 | 63 |
| 50,000 | 24 | 35 | 39 |
| 1,000,000 | 4.5 | 7.7 | 8.5 |

On the Linux VM the change was neutral: one build pair read 10 to 14% slower
on one thread, and the same sources rebuilt with one codegen unit read 3%
faster. Builds with inline LSE atomics also measured within noise.

Other variants, none kept:

- A `Release` unpin instead of `SeqCst`, hashing before or after the pin:
  within 2%, so the epoch protocol is unchanged.
- Prefetching the home bucket before pinning: 2 to 6% slower than hashing
  first alone.
- Computing the bucket's address before pinning: within noise.
- `#[inline]` on `get`: within noise in the probe.
- Taking and filling an insert's slot before pinning: within noise on
  writes.
- Prefetching the home bucket, or the eviction victim's bucket, for writing:
  the same cell moved from +12% to −6% between runs, within the machine's
  noise.

## Write path

On the Linux VM at eight threads, a shard's lock was held for 138 ns per
replace and 290 ns per cold-fill insert, against 48 and 64 ns on one thread,
and writers waited about 100 ns to take it. The difference is cache misses:
each line of shard state a write touches was last written on another core.
These changes followed, each measured against the one before:

- **Paced reclamation.** Writes reclaimed up to 16 aged entries, and an
  advance makes a whole batch of retirements due at once, so values were
  freed in bursts. glibc's per-thread cache holds seven blocks; frees past it
  went to the arenas, and 15% of eight-thread cold-fill time was spent in
  free's compare-and-swap loops, with 8% in calloc. A write now frees as many
  entries as it retires: at eight threads, cold fill +18% and
  delete-reinsert +8%.
- **Shard state in two cache lines.** The counts were kept twice, and the
  writer's state spanned four lines of the lock block. `len`, `weight`, and
  the eviction count now sit beside the lock word, followed by the probation
  queue; the ghost and the main queue fill the next line, and the limits moved
  to the shard's read-only header. At eight threads: replace +13%,
  delete-reinsert +11.5%, cold fill +7%. Replace's hold time fell to 95 ns.
- **Four-slot reclamation batches.** Two 16-slot batches per write, zeroed
  and handed on by value, took most of a single-thread replace's time after
  the lock. Single-thread replace on macOS: +12%.
- **Prefetching the victim's entry.** Evicting followed the probation
  record, then the victim's bucket, then the entry its handle names: three
  misses in series. Records now carry the handle, and a writer that takes a
  full shard's lock prefetches the bucket and the entry together. Eight-thread
  cold fill: +6% on Linux, +8.5% on macOS.
- **More shards over one bucket table.** Going from 64 to 1,024 shards made
  eight-thread replace 45% faster and cold fill 28%. Large caches now default
  to up to sixteen shards per thread, as long as each holds at least 256
  entries; the trace hit-ratio sum moved from 654.28 to 654.15. More shards
  alone cost reads up to 13%, as each read loaded its shard to find the
  bucket table. With one shared table, a read loads no shard state. On macOS
  at eight threads: delete-reinsert +17%, replace +14%, turnover +6%, cold
  fill +4%, and reads +4%.

Pacing is a trade-off between allocators and workloads. Freeing up to four
entries per write instead changed Linux throughput by:

| Workload | 1T | 2T | 8T |
| --- | ---: | ---: | ---: |
| Cold fill | +1% | −15% | −17% |
| Turnover (95% reads) | −1% | −1% | −8% |
| Replace | +20% | +27% | +6% |
| Delete and reinsert | +14% | +11% | +4% |

Cold fill and turnover free values other threads allocated, and they are a
full cache's steady state, so pacing stays. On macOS, two-thread cold fill
was 14% faster freeing four at a time and eight threads were equal.

After these changes, an eight-thread cold-fill insert still holds the lock
for about 267 ns: 60 to find the key, 141 to evict, 77 to place, and 60 for
the ghost and the probation queue, each including about 10 ns of timer
overhead.

## Throughput against quick_cache 0.7

Measured with `bounded_cache_probe` after reads began hashing before they
pin: 32-byte keys, 64-byte `Arc<[u8]>` values, and 50,000 entries. Reads ran
16 million requests and writes 8 million on macOS, and half that on Linux.
Each figure is the paired median of three runs; positive means
`ShardedCache` is faster.

Hosts:
- macOS: a shared 16-core Apple Silicon machine (an M4 Max), with other
  containers and services using about four cores during the runs.
- Linux: an aarch64 Docker VM on the same machine, using glibc's allocator.

Two-thread cells varied most between runs, by up to 30% for one build, as
threads landed on performance or efficiency cores. On Linux, rebuilding the
same sources with a different code layout moved single-thread cells by up to
14%.

| Workload | macOS 1T | macOS 2T | macOS 8T | Linux 1T | Linux 2T | Linux 8T |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Resident hit | −10% | +30% | +292% | +2% | +20% | +133% |
| Read miss | +91% | +184% | +670% | +75% | +169% | +556% |
| Turnover (95% reads) | −13% | −4% | +33% | −21% | −8% | +44% |
| Cold fill | −10% | −17% | −20% | −21% | −8% | −19% |
| Replace | +7% | −4% | +2% | −16% | −14% | −11% |
| Delete and reinsert | +6% | 0% | +35% | −20% | −10% | +30% |

Before reads hashed first, macOS resident hits read −42%, −9%, and +151%,
and macOS turnover −22%, −11%, and +27%. The write code did not change; its
cells moved within the noise above. Before the write-path changes, the write
rows read −10 to −53% for cold fill, −3 to −42% for replace, −10 to −31% for
delete and reinsert, and −27 to +9% for turnover.

Eight-thread resident hits reached 418 Mops/s on macOS against quick_cache's
107, and 212 against 94 on Linux.

Cold fill still trails: every insert into a full shard evicts under the lock
(see "Write path"), and single-thread writes pay what lock-free reads
require. An insert cannot reuse the evicted entry's memory until three epoch
advances later, where quick_cache frees under its shard lock, and every
write pays an epoch pin and its thread's retired-queue lock. A single-thread
`get` still waits at its unpin for the value clone's increment, the remaining
difference from quick_cache on macOS (see "Read path").

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

The table predates the larger default shard count for big caches, which
moved ShardedCache's sum by −0.13 points; no cell moved by more than 0.08.

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
- **Prefetching only the next probation victim's bucket.** +4–7% on
  single-writer fills, −5% elsewhere. Prefetching its entry too, from the
  handle its record now carries, and only for a full shard, is kept (see
  "Write path").
- **Looking keys up before the lock, again at eight threads.** No change.
- **Spinning longer before parking.** Within noise: writers waited on the
  holder's misses, not on parking.
- **Checking the ghost before the lock.** With atomic ghost words, the check
  left the lock, but eight-thread cold fill did not move and delete-reinsert
  fell 11%.
- **Prefetching all of an insert's lines for writing.** The ghost word and
  probation tail go unused when replacing, and taking them exclusive cost
  eight-thread replace 6%.
- **Freeing up to four entries per write.** See the trade-off under "Write
  path".
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
target/release/examples/hit_ratio_sim arc:PATH/P3.lis 131072 0 1 sharded/shards=64 sharded
cargo test --release --lib sharded_cache
```
