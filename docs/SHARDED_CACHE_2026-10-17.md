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
  The alternate lies an offset past the home that the key's tag picks.
  - The home's overflow count is the number of its keys that live in their
    alternates. A lookup reads the alternate only when that count is nonzero.
  - A bit per slot, in the writer-only generations word, marks a key living
    in its alternate. With the slot's tag it names the key's other bucket, so
    relocation and removal never read a resident's key to find it.
  - When both buckets are full, the cache first moves one resident key to its
    own other bucket. The copy is published before the old slot is cleared, so
    readers always find it.
  - If no resident key can move, the cache evicts an unread entry from the two
    buckets. This happened on fewer than 0.1% of inserts into a full shard.
- **Eviction.** Each shard uses an S3-FIFO-style policy:
  - New keys wait in a probationary FIFO. Its records hold the key's slot
    record, its hash, and the handle it was published with. A writer that
    takes a full shard's lock prefetches the next victim's bucket and entry
    together.
  - Each shard sizes its probation to the keys that return (see "Adaptive
    probation"). It starts at 0.25% of the shard and stays between one
    entry and a quarter of the shard.
  - An unread probationary key leaves first and goes into a ghost filter. The
    ghost has eight generations of a sixteenth of the shard and remembers
    seven sixteenths to half of it. A key's words in every generation fill
    one 64-byte line.
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
| 100,000 | 8 B | `u64` | 48.0 | 46.8 | 118.0 |
| 100,000 | 32 B | `[u8; 64]` | 129.3 | 128.2 | 306.7 |
| 1,000,000 | 8 B | `u64` | 45.1 | 44.2 | 94.4 |
| 1,000,000 | 32 B | `u64` | 69.1 | 69.0 | 144.7 |
| 1,000,000 | 32 B | `[u8; 64]` | 125.2 | 125.1 | 245.4 |

Overhead beyond the key and value is 29.1 to 33.3 bytes per entry. On a
16-thread machine, a 100,000-entry cache now has 256 shards, and their fixed
state adds about 1.4 bytes per entry; at 1,000,000 entries it adds about 0.5:

| Component | Bytes per entry |
| --- | ---: |
| Table at 80% load | 13.3 |
| Entry metadata | 8 |
| Main queue, with its slack | about 4 to 5 |
| Ghost filter | about 2 |
| Main filter | about 0.25 |
| Probation records, 24 bytes each | about 0.1 at the starting share |

Probation records grow with the probation a shard adapts to. A workload
that drives probation to its cap of a quarter of the shard holds up to 7.5
bytes of records per entry, plus the queue's growth slack.

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
  to the shard's read-only header. (Adaptive probation later put its shadow
  filters and target in the ghost's struct, and the main queue in a 24-byte
  ring, to keep this layout.) At eight threads: replace +13%,
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
- **Prefetching every line an eviction writes.** Besides the victim's bucket
  and entry, an evicting insert writes the victim's ghost and shadow words
  and reads, then writes, the new key's home bucket. A writer that takes a
  full shard's lock now prefetches all of them, those it writes for writing.
  Eight-thread turnover: +4.6 to +5.4% on macOS and +3.6 to +4.5% on Linux;
  cold fill +1 to +7%, with every median positive and ranges crossing zero.
- **Stores instead of swaps.** Unpublishing a handle swapped it with an
  AcqRel read-modify-write. Under the shard lock a relaxed load and a
  release store do the same: writers change handles only under the lock,
  and readers still holding the old handle are covered by the writer's pin.
  That is one fewer read-modify-write per replace, eviction, and removal,
  an out-of-line call on Linux aarch64. Eight-thread cold fill on macOS:
  +5.0%; other cells within 4%.
- **Relocating without reading residents.** Both candidate buckets were
  full on 6.4% of inserts into a full shard. Relocation then read each
  resident it tried, 1.5 on average, to hash its key and find its other
  bucket: a cache miss apiece in a large cache, about 260 ns per relocation
  on macOS at 1,000,000 entries. The alternate is now an offset that the
  key's tag picks, and a bit per slot marks keys in their alternates, so a
  slot alone names its key's other bucket. At 1,000,000 entries on macOS,
  single-thread cold fill gained 11% and turnover 7%, and cold fill's p99
  insert fell from 625 to 375 ns; eight threads gained 1 to 4%. At 50,000
  entries every cell stayed within 3%. On Linux, single-thread cells at
  1,000,000 entries moved by −4 to +2%: the VM backs memory with 2 MB
  transparent huge pages, which likely made the old reads cheaper there.
- **Compacting the main queue a chunk at a time.** Each relocated main key
  leaves a stale record behind and a new one at the back. A full shard
  taking only new keys never evicts its oldest main keys, so stale records
  gathered behind a live front. Cleanup dropped stale records from the front
  64 at a time and, finding none, compacted the whole queue under the lock:
  77 µs per pass for 15,600-entry shards and 1.3 ms for 250,000-entry ones,
  timed directly. A cursor now walks the queue 64 records per append once
  the queue is within a sixteenth of its limit. Live records slide over
  dropped ones, and the gap behind them frees its slots when the walk
  reaches the back. The walk prefetches the chunk's buckets first and
  updates without branching on each record's fate, which cut a chunk from
  700 to about 500 ns. Single-thread cold fill at 4,000,000 entries: p99.999
  fell from 79 to 15 µs with 256 shards; p99 rose by one or two 42 ns timer
  ticks, as 0.4% of inserts now walk a chunk; throughput stayed within
  noise. The cursor fits the writer's second cache line because the filters
  now derive where the main filter starts from their lines' length, and
  keep the ghost's generation index in the low bits of its key count.
- **Clearing the ghost's next generation ahead.** Starting a ghost
  generation cleared its word in every ghost line at once, about 1.6 ns a
  line under the lock: 0.5 µs for 15,600-entry shards, 6.5 µs for 250,000
  entries, and 25 µs (37 µs at most) for 1,000,000, timed directly. The last
  sixty-fourth of each generation now clears the next one's word, at most
  32 lines per remembered key, so the oldest generation fades over that
  sixty-fourth instead of vanishing at its end. Clearing over the whole
  generation instead cost 0.79 points of the trace hit-ratio sum, and over
  its last quarter 0.10; the bounded version moved the sum from 667.05 to
  667.08, and throughput stayed within noise on macOS and Linux.
- **Prefetching the next slot to reclaim.** Reclaiming drops a value, which
  reads its slot, last touched a few advances earlier, and then the value's
  own memory. On x86 the slot had left the 1 MB L2, so the two missed in
  series: dropping reclaimed values took 22% of single-thread replace
  time, against 11% for quick_cache's drop of a replaced value. Each write
  now prefetches the slot its thread reclaims next, a write ahead.
  Single-thread writes gained 5 to 9% on x86, 9 to 11% for cold fill and
  replace on the Linux VM, and 6% for cold fill on macOS; four- and
  eight-thread writes moved by −3 to +7%, and reads stayed within noise.

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

Measured with `bounded_cache_probe` after adaptive probation and the
write-path changes through the next-slot prefetch: 32-byte keys, 64-byte `Arc<[u8]>` values, and 50,000
entries. Reads ran
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
14%, and whole runs drifted by about 10% between sessions: quick_cache's own
figures moved as much. Direct before-and-after comparisons, as quoted under
"Write path" and "Adaptive probation", are the better guide to each change.

| Workload | macOS 1T | macOS 2T | macOS 8T | Linux 1T | Linux 2T | Linux 8T |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Resident hit | −8% | +45% | +292% | −11% | +22% | +156% |
| Read miss | +90% | +196% | +685% | +74% | +182% | +582% |
| Turnover (95% reads) | −3% | −5% | +42% | −22% | 0% | +65% |
| Cold fill | −5% | −23% | −14% | −12% | −16% | −13% |
| Replace | −1% | −10% | +13% | −18% | −15% | −1% |
| Delete and reinsert | −5% | −1% | +54% | −10% | −14% | +44% |

The macOS single-thread resident-hit cell ranged from −23 to −1% across
its three pairs. Before the next-slot prefetch, a session earlier, the
table read 0%, +40%, and +283% for macOS resident hits, −13%, +7%, and
+42% for macOS turnover, and for single-thread cold fill, replace, and
delete and reinsert −9%, −3%, and +5% on macOS and −21%, −15%, and −14%
on Linux.

Before reads hashed first, macOS resident hits read −42%, −9%, and +151%,
and macOS turnover −22%, −11%, and +27%. Before the write-path changes, the
write rows read −10 to −53% for cold fill, −3 to −42% for replace, −10 to
−31% for delete and reinsert, and −27 to +9% for turnover.

Eight-thread resident hits reached 413 Mops/s on macOS against quick_cache's
105, and 219 against 86 on Linux.

At 1,000,000 entries on macOS, past the caches, one thread read misses at
29 Mops/s against quick_cache's 66 to 73, and resident hits at 7.8 against
8.9 to 9.1, in two runs each. A miss loads one or two 64-byte bucket lines
from a 13 MB table, where quick_cache's control bytes for as many entries
stay in L2; a hit loads the bucket, the entry, and the value's count, one
line more than quick_cache (see "Rejected and reverted" for layouts tried).
Eight threads read misses at 263 to 267 Mops/s against 105 to 111, and
hits at 69 against 62.

Cold fill still trails: every insert into a full shard evicts under the lock
(see "Write path"), and single-thread writes pay what lock-free reads
require. An insert cannot reuse the evicted entry's memory until three epoch
advances later, where quick_cache frees under its shard lock, and every
write pays an epoch pin and its thread's retired-queue lock. A single-thread
`get` still waits at its unpin for the value clone's increment, the remaining
difference from quick_cache on macOS (see "Read path").

Two bounds, measured on one Linux thread and unsound with concurrent
readers, locate the single-thread write cost:

- Freeing removed entries at once instead of queuing them made cold fill
  29 to 32% faster, delete and reinsert 25 to 27%, and replace 34 to 35%:
  ahead of quick_cache, which led by 13 to 20%.
- Dropping only the retired queue's lock moved the same cells by −4 to +1%.

On Apple Silicon the delay is not the cost. Scanning only the reader shards
in use cut a lone writer's backlog from 1,536 removed entries to under 256,
so slots and values were reused about 100 writes after their last use
instead of 1,500 (see "Rejected and reverted"). Single-thread cold fill
moved by +0.3% on macOS and +2 to +4% on Linux, and on x86 by +6%: there
the delayed slot read missed, which a prefetch now hides (see "Write
path"). A macOS profile of single-thread turnover
puts a write at 1.4 to 1.6 times quick_cache's cycles. Evicting costs less
than quick_cache's eviction; queuing, tagging, and later freeing each
removed entry take about 80 cycles, spread over the retired queue, the
reclamation batches, and the slot stacks, with no single step
dominating. In single-thread cold fill, freeing the evicted values also
took about 60 cycles per insert against quick_cache's 36, most of the
difference in the macOS allocator's chunk-list pushes: probation evicts
each shard's newest key, so the freed values belong to chunks the
allocator filled shortly before.

The earlier boxed-entry storage measured macOS resident hits at −32%, +2%,
and +241%. On macOS those boxes often sat beside their values (see "Slab
storage against boxed entries").

### On x86_64

`.github/workflows/bench.yml` runs the same comparison on GitHub's x86_64
runners when a branch under `bench/` is pushed. The last run, on an AMD EPYC
9V74 with 4 vCPUs and the next-slot prefetch, read as follows; each figure
is the median of three pairs, and positive means `ShardedCache` is faster:

| Workload | 50,000, 1T | 50,000, 4T | 1,000,000, 1T | 1,000,000, 4T |
| --- | ---: | ---: | ---: | ---: |
| Resident hit | −1% | +33% | −13% | +5% |
| Read miss | +28% | +58% | −61% | −32% |
| Turnover (95% reads) | 0% | +30% | −8% | +17% |
| Cold fill | −20% | +3% | +15% | +61% |
| Replace | −23% | −17% | +6% | +9% |
| Delete and reinsert | −23% | +6% | +5% | +23% |

Runners differ: an earlier run on another runner, before the prefetch, read
single-thread writes at −27 to −31% at 50,000 entries. On x86 every
read-modify-write is a full barrier, and a ShardedCache replace makes seven,
where quick_cache's makes three: the writer's pin and unpin, its retired
queue's lock and unlock, and the shard lock and the old value's count,
which both share. As upper bounds, skipping the writer's pin moved
single-thread writes by −2 to +4%, and a retired-queue lock that unlocks
with a store by 0 to +1%, delete and reinsert by +14% within −2 to +34%
(see "Rejected and reverted").

## Tail latency against quick_cache 0.7

Every operation was timed (`LATENCY_STRIDE` 1) at 1,000,000 entries,
8,000,000 operations, and 64-byte values. Each cell is the median of three
runs, in nanoseconds, as ShardedCache / quick_cache; both hosts' timers tick
every 42 ns. Cold fill includes filling the empty cache.

macOS:

| Cell | p99 | p99.9 | p99.99 | p99.999 |
| --- | ---: | ---: | ---: | ---: |
| Cold fill, 1 thread | 375 / 292 | 1,959 / 1,167 | 5,000 / 5,375 | 8,583 / 191,625 |
| Cold fill, 8 threads | 875 / 792 | 4,750 / 10,750 | 12,792 / 99,291 | 27,500 / 385,791 |
| Turnover, 1 thread | 458 / 375 | 2,334 / 1,833 | 5,083 / 5,000 | 9,000 / 10,917 |
| Turnover, 8 threads | 709 / 750 | 1,166 / 10,583 | 5,333 / 28,666 | 14,042 / 78,125 |

Linux:

| Cell | p99 | p99.9 | p99.99 | p99.999 |
| --- | ---: | ---: | ---: | ---: |
| Cold fill, 1 thread | 583 / 500 | 5,083 / 4,708 | 7,583 / 9,042 | 14,625 / 255,250 |
| Cold fill, 8 threads | 1,292 / 1,209 | 6,583 / 29,166 | 43,459 / 140,750 | 122,292 / 496,916 |
| Turnover, 1 thread | 709 / 708 | 4,834 / 5,250 | 7,333 / 7,709 | 14,166 / 15,292 |
| Turnover, 8 threads | 833 / 958 | 2,292 / 13,625 | 11,750 / 54,292 | 25,750 / 90,000 |

From p99.99 on, ShardedCache's tail is the shorter one in every cell but
macOS single-thread turnover, which was level: up to 22 times shorter at
p99.999 for single-thread cold fill, where quick_cache stalled for 190 to
255 µs. At eight threads it is 2.3 to 14 times shorter from p99.9, as
readers never wait on a shard lock. On one thread its p99 trails by up to two
timer ticks (83 ns), and on macOS its p99.9 by 0.5 to 0.8 µs: the bounded
steps below run on a fraction of a percent of inserts there.

Two steps of a write used to take time proportional to the shard under its
lock: compacting the whole main queue and clearing the ghost's next
generation. Each now does a bounded share, and relocation no longer reads the
residents it tries (see "Write path"). What remains is the CLOCK sweep past
read keys, as in quick_cache's hot ring, and a scan of 128 reader counters
every 512 retirements. On turnover with 1,000,000 entries per shard, the
sweep averaged 3.3 records per main-area eviction; 13 of 908,149 sweeps
passed 1,000 records, and the longest took 1,504.

## Hit ratio

The ARC paper traces were replayed through `hit_ratio_sim`; the traces are
cited, not committed. Values are read hit percentages at the default
configuration. "Fixed 2%" is ShardedCache before probation adapted.

| Cell | ShardedCache | Fixed 2% | quick_cache | quick_cache sharded |
| --- | ---: | ---: | ---: | ---: |
| OLTP 1,000 | 39.76 | 36.20 | 35.15 | 36.13 |
| OLTP 5,000 | 54.73 | 53.13 | 54.29 | 53.24 |
| OLTP 15,000 | 65.36 | 64.35 | 64.83 | 64.55 |
| P3 8,192 | 4.53 | 4.26 | 3.64 | 3.75 |
| P3 32,768 | 13.42 | 13.09 | 12.94 | 13.11 |
| P3 131,072 | 48.69 | 48.39 | 48.61 | 48.73 |
| S1 65,536 | 6.16 | 6.06 | 6.17 | 6.23 |
| S1 262,144 | 24.00 | 23.66 | 23.77 | 23.94 |
| S1 524,288 | 43.06 | 42.53 | 41.99 | 42.73 |
| P8 8,192 | 10.11 | 10.09 | 10.40 | 10.37 |
| P8 32,768 | 31.98 | 31.73 | 32.41 | 32.31 |
| P8 131,072 | 70.28 | 70.17 | 70.01 | 70.11 |
| S3 100,000 | 12.55 | 12.28 | 12.67 | 12.83 |
| S3 400,000 | 42.35 | 41.76 | 41.75 | 42.27 |
| S3 800,000 | 69.45 | 68.70 | 66.99 | 68.34 |
| DS1 1,000,000 | 15.25 | 14.94 | 14.86 | 14.86 |
| DS1 4,000,000 | 45.08 | 44.74 | 44.80 | 44.75 |
| DS1 8,000,000 | 70.26 | 68.06 | 69.22 | 69.31 |

Summed over the 18 cells, ShardedCache reads 667.03, against 654.51 for
single-shard quick_cache and 657.58 for its sharded cache. It is ahead of the
sharded cache in 13 cells, by up to 3.63 points (OLTP at 1,000 entries), and
at most 0.33 behind (P8 at 32,768). The default hasher is seeded per process,
so sums vary by about 0.1 between runs.

The insertion-order main queue closed most of the earlier gap. A CLOCK over
the table's own slots sweeps in hash order and loses recency: before the
queue, the sum was −3.44 against quick_cache and P3 at 131,072 entries read
46.80%.

### Adaptive probation

No fixed probation share suits every trace. Against the old 2%, summed
changes per share were:

| Probation | 0.5% | 1% | 3% | 5% | 10% | 20% |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| OLTP, three cells | −3.51 | −3.18 | +1.02 | +3.31 | +5.52 | +6.51 |
| DS1 at 8,000,000 | +1.84 | +1.31 | −1.45 | −3.87 | −8.06 | −12.48 |
| All 18 cells | +1.04 | +0.15 | −2.33 | −7.07 | −22.99 | −53.31 |

Picking each cell's best fixed share would have reached 667.77. So each
shard adapts its own share, ARC-style, from the keys that return soon after
leaving a queue:

- The ghost's current generation holds up to a sixteenth of the shard's
  most recent probation evictions. A missed key it remembers would most
  likely have hit with a little more probation, and grows probation by one
  entry. The line that answers is the one that admits the key, so this
  costs no extra miss.
- A main filter holds as many of the most recent main-area evictions, in one
  generation cleared when it fills. A missed key it remembers shrinks
  probation by two entries. It is read only while it remembers a key, so
  inserts that evict only from probation, as in cold fill, never touch it.

Probation stays between one entry and a quarter of the shard. Only writers
adapt, under the shard lock; reads are unchanged.

Variants measured on the way, as summed changes against the old 2%:

- Dedicated shadow filters of a 32nd of the shard for both queues, beside a
  four-generation ghost: +11.47. Each evicting insert then read and wrote
  two more lines of filter words, and cold fill lost 3.7% on one thread and
  9.2% on two. With the eight-generation ghost they read +12.77; the
  current-generation pair kept here reads +12.90.
- The ghost's current generation for probation against a dedicated main
  shadow of a 32nd: +10.26. The ghost's window, up to a sixteenth, outweighed
  the shadow's.
- A main-area ghost as large as the probation ghost, compared hit for hit:
  −16.66. Probation's ghost spans far more evictions, so its hits swamped
  the main area's, and DS1 at 4,000,000 entries lost 5.95.
- With dedicated shadows, starting at 2% (+9.70), at 0.5% (+11.13), and at
  0.25% (+11.32, kept). Large shards converge slowly, and DS1 favors a small
  probation.
- Shrinking by one entry per main-shadow hit instead of two: the same sum,
  but P8 at 131,072 entries fell 0.48.
- Shadows of a 16th or a 64th of the shard: within 0.1 of a 32nd.
- Shadows whose generations held at least eight keys, as the ghost's do: a
  1.25×-capacity loop at 10,000 entries lost 3 points, as shards of 156
  entries got shadows of a fifth of the shard.
- A cap of 10% instead of 25%: −1 point, from OLTP.

On synthetic traces at 10,000 and 100,000 entries, adaptation changed Zipf,
scan-mixed Zipf, shifting-Zipf, and turnover hit rates by 0 to +0.44
points. A loop of 1.25 times the capacity read 59.8% and 58.1%, +0.5 and +0.1
over fixed probation, against 61.1% and 63.4% for quick_cache's sharded
cache; the dedicated shadows had lifted it to 60.7% and 60.3%.

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
  12–15% of single-thread reads, so only the alternate took it, until the
  alternate became an offset of the key's tag. Over 300 random seeds of that
  test's churn, with `u64` and 32-byte keys, the tag offsets evicted from full
  pairs on at most 5 of 200,000 inserts, the same as the finalizer.
- **A fixed 10% probation share.** 2% read better on DS1, P3, and S3; for
  example, DS1 at 8,000,000 entries read 68.06% against 63.91%. Probation
  now adapts per shard instead (see "Adaptive probation").
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
- **A fuller table.** At 89% load instead of 80%, 48% of keys lived in
  their alternates instead of 34%, relocations rose from 0.05 to 0.19 per
  insert, and 16 inserts in 100,000 evicted from a full pair, overriding
  the policy, to save 1.3 bytes per entry. At 91% load, 44 to 55 inserts in
  100,000 did; at 95%, 1,700.
- **Prefetching every lookup's alternate bucket.** A third of a churned
  table's keys live in their alternates, and a miss checks the alternate
  whenever its home has overflowed. At 1,000,000 entries the prefetch made
  eight-thread misses 8% faster and single-thread turnover 6%, but at
  50,000 entries, with both lines cached, it cost resident hits 8% on one
  thread and 24% on eight.
- **Prefetching the home bucket before pinning.** A pin's acquiring
  read-modify-write holds back the loads after it, but not a prefetch. At
  1,000,000 entries hits moved by −0.5 to +1%; at 50,000, reads lost 5 to
  9% to the extra instruction.
- **Tags in an array of their own.** Apart from the handles, a
  1,000,000-entry cache's control words fit in 1.4 MB, which stays in L2,
  and misses ran 2.7 times as fast. But a hit then waited for its tag word
  before loading the handle line, and resident hits lost 13 to 20% at both
  50,000 and 1,000,000 entries. Prefetching the handle line with the tags
  restored hits and kept misses at 1,000,000 entries 1.6 to 2 times as
  fast, but eight-thread turnover lost 9 to 14%: a line of control words
  covers eight buckets, so each write invalidates it for readers of all
  eight. The split would also have fit seven handles in a line, about 0.5
  bytes per entry less.
- **Writers without a pin.** A writer holds its shard's lock while it
  reads entries, so its pin serves only the retirement tag. As an upper
  bound, writes that skipped the pin altogether, unsoundly, moved
  single-thread writes by −1 to +4% on macOS, −5 to +2% on Linux, and −2
  to +4% on x86, and multi-thread writes by +1 to +8%. A sound replacement still needs a
  release that the next advance acquires, so it would save one of the two
  read-modify-writes at most.
- **A spin lock for the retired queues.** Its uncontended unlock is a
  store, where the mutex's is a read-modify-write, a full barrier on x86.
  On x86, single-thread writes moved by 0 to +1%, delete and reinsert by
  +14% within −2 to +34%; on macOS, by −2 to +7% within wider ranges. Not
  enough to replace the mutex with unsafe code of our own.
- **Scanning only the reader shards in use.** Each pin marked its reader
  shard in a per-cache mask; tries scanned only marked shards' counters and
  came due after 32 retirements per marked shard. A lone writer's backlog
  fell from 1,536 removed entries to under 256, and requested memory at
  100,000 entries by 0.7 to 1.6 bytes per entry. Single-thread writes on
  Linux gained 2 to 12%, and eight-thread replace 2 to 6%. But the mark
  check on every pin cost reads: misses lost 1.2 to 1.6% on macOS and 1.8 to
  3.4% on Linux, and eight-thread resident hits 3.2 to 3.6% on Linux. On
  x86, single-thread cold fill gained 6% and reads lost 2 to 3%. A
  process-wide mask set once per thread would cost reads nothing, but fills
  once 64 threads have used any cache, as under thread churn.

## Reproduce

```sh
cargo run --release --example cache_density_compare
cargo build --release --example bounded_cache_probe --example hit_ratio_sim
PROBE=target/release/examples/bounded_cache_probe
$PROBE sharded turnover 50000 8000000 8 64 1
$PROBE quickcache turnover 50000 8000000 8 64 1
$PROBE sharded cold-fill 1000000 8000000 1 64 1 1
target/release/examples/hit_ratio_sim file:PATH/DS1.keys 8000000 0 1 sharded quick
target/release/examples/hit_ratio_sim arc:PATH/P3.lis 131072 0 1 sharded/shards=64 sharded
cargo test --release --lib sharded_cache
sh scripts/compare_sharded_quick.sh 3 "1 4" 50000 target/compare-50k.csv
python3 scripts/summarize_compare.py target/compare-50k.csv
git push origin HEAD:refs/heads/bench/NAME   # the same on x86_64, and against HEAD^
```
