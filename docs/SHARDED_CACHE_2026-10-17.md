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
- **Buckets.** A bucket fills one 64-byte line with ten slots: a tag word
  with the tag bytes of slots 0 to 7, a meta word with slots 8 and 9's tags
  and the overflow count, and ten atomic four-byte handles. Tags are
  matched with word-wide zero-byte tests, and a lookup reads the meta word
  only after slots 0 to 7 miss. A handle is its slot's offset in the slab
  region (see "Four-byte handles").
- **Placement.** A key lives in its home bucket or one alternate bucket, so
  lookups touch at most two buckets. The home is one multiply of the key hash.
  The alternate lies an offset past the home that the key's tag picks.
  - The home's overflow count is the number of its keys that live in their
    alternates. A lookup reads the alternate only when that count is nonzero.
  - A bit per slot, in the meta word, marks a key living in its alternate.
    With the slot's tag it names the key's other bucket, so relocation and
    removal never read a resident's key to find it.
  - When both buckets are full, the cache first moves one resident key from
    its home to its own alternate. The key is counted in its home's overflow
    and published in the alternate before its home slot is cleared, so
    readers always find it. Keys never move back home (see "Relocating only
    away from home").
  - If no resident key can move, the cache evicts an unread entry from the two
    buckets. This happened on fewer than 0.1% of inserts into a full shard.
- **Eviction.** Each shard uses an S3-FIFO-style policy:
  - New keys wait in a probationary FIFO. Its records hold the key's slot
    record, its hash, and the handle it was published with. An insert
    whose new key misses in a full shard prefetches the next victim's
    bucket and entry together.
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
    slot's placement generation, kept in each bucket's generations word, or
    for slots 8 and 9 its meta word. A record of a removed, moved, or reused
    slot is skipped when popped. Main records take 4 bytes each.
  - Past 1.25 times the shard's capacity, the main queue drops stale records
    from its front in chunks of 64. A table sweep remains the fallback when
    the queue is exhausted.
- **Storage and reclamation.** Entries live in one slab region per cache,
  with small overflow chunks past it (see "The slab region"), in
  `src/sharded_arena.rs`, under the direct arena's reader protocol and
  epoch state machine.
  - A writer pins, takes a vacant slot from its thread's stack for the
    cache, and fills it before taking the shard lock.
  - After the lock, still pinned, it queues the entries it unpublished in its
    thread's retired queue. Each carries the generation token read after
    the unpublication, aged three advances. The advance that reuses the
    tag's epoch slot found that slot and the next one quiescent, the direct
    arena's two-rotation aging.
  - In the same queue lock, the writer takes as many aged entries as it
    retired, and as many more while entries due since an earlier advance
    remain, within one reclamation batch of four. It
    drops their values and pushes the slots onto its own stack, so it refills
    slots whose lines it just touched. Paced this way, frees keep step with
    the writes' allocations (see "Write path").
  - Every 16 retirements a writer visits one other queue. It drains it only
    if the queue's newest entry is aged, meaning its thread has retired
    nothing for three advances, and then returns to it until it is empty.
    `maintain()` drains every queue.
  - A thread's stack overflows into a shared depot, draws from it in batches
    of 32, and returns to it when the thread exits. The region is freed when
    the cache drops; a stack left for a dropped cache is forgotten without
    touching its slots.
  - The generation advances once retirements pending across the cache reach
    32 per reader shard any thread has claimed, at most 512, and a try
    scans only the claimed shards' counters (see "Reader shards in use"). A
    failed try waits for half as many more.
- **Reads.** `get` hashes the key, then pins and clones the value. Hashing
  before pinning lets the hash run while the previous call's unpin completes
  (see "Read path"). `pin()` returns a guard for borrowed reads that pins once.
  On Linux a thread's pins reuse an epoch announcement it keeps parked
  between them, without a read-modify-write (see "Parked pin
  announcements").
- **Weights.** Weights come from a `Weighter`. A value heavier than its shard's
  share of the weight limit is refused, and any previous value for its key is
  removed, so readers never see a value the caller replaced.

## API beyond get and insert

- **Loading once.** `get_or_insert_with` and `get_or_try_insert_with` take a
  borrowed key and a loader.
  - A caller that misses registers a flight, keyed by hash and key, in its
    shard's lane under the shard lock, and runs the loader outside every
    lock.
  - Callers that miss the same key while it loads wait on the flight and
    share its value.
  - The leader's insert unlists the flight under the shard lock. A caller
    that finds no flight looks the key up again under that lock, so it
    either joins the flight or sees the inserted value.
  - Writes supersede loads. An insert, removal, clear, or drain unlists the
    flights of its keys under the shard lock and marks them superseded; the
    leader's insert, under the same lock, then caches nothing, while its
    callers still get the value. A loader may read its source before an
    invalidation (update the source, then remove the key) and insert after
    it; without this the old value would stay cached. Callers that miss
    after the write list a new flight and load afresh. A count of listed
    flights, on the shard's limits line that inserts read anyway, spares
    writes the lane when nothing loads.
  - If the loader fails or panics, the flight is abandoned and one waiter
    loads again with its own loader.
  - `get_or_insert_async` and `get_or_try_insert_async` share the flights.
    An asynchronous waiter leaves its waker on the flight, under the lock
    that guards the flight's state, so it cannot miss the landing; the
    landing wakes those wakers after releasing the lock, as it notifies
    blocking waiters. A leader's future dropped mid-load abandons the
    flight, and a waiter's future dropped mid-wait clears its waker. The
    futures need no particular executor and are `Send` when the cache, the
    key, and the loader are.
- **Placeholder guards.** `get_value_or_guard` and
  `get_value_or_guard_async` return the value, or, when no load is in
  progress, a `PlaceholderGuard` that lists a flight as the loaders do. The
  caller inserts through `PlaceholderGuard::insert`, which inserts and
  hands the value to the waiters; a guard dropped without inserting
  abandons the flight, and a waiter takes it over. Loaders and guards
  share flights, so each waits for the other. `get_value_or_guard` waits
  up to its timeout and returns `Timeout` past it; a deadline already
  passed returns before parking, so a zero timeout never waits.
- **Expiry.** `insert_with_ttl` and the builder's `time_to_live` store a
  deadline in the entry's 31-bit expiry field, in 100 ms ticks since the
  cache was built.
  - An entry stays readable for its time to live and at most one tick more.
  - Reads of entries without a deadline never read the clock.
  - Eviction removes an expired entry wherever it meets one, ignoring its
    access marks. When a full bucket pair forces an eviction, an expired
    entry in the pair goes first.
  - Expired entries the policy has not reached still count in `len`.
  - With the builder's `time_to_idle`, an insert sets the deadline that far
    ahead, and a marking read of a live entry moves it to that far after
    the read, never earlier. The move happens in the out-of-line clock call
    that reads of entries with deadlines already make, so `get` keeps its
    instructions (364 before, 365 after on aarch64) and caches without
    deadlines never reach it. It is a compare-and-swap on the metadata
    word, once per 100 ms tick per entry at most, that keeps access marks
    set meanwhile. `peek`, `contains_key`, and iteration do not move it.
  - An entry holds one deadline, so a cache takes a time to live or a time
    to idle, not both; `build` panics if given both. An entry inserted with
    its own time to live in a cache with a time to idle expires at the
    later of that deadline and the time to idle after its last read.
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
- **Iteration.** `iter` clones the live entries, and the guard's `iter`
  borrows them, a shard at a time.
  - Each shard's handles are copied under its lock and read after it is
    released. A shard is seen as it was at one moment, each entry at most
    once, and the loop body may write to the cache.
  - Iterating is not reading: entries are neither marked nor counted.
  - The cloning iterator refreshes its pin between shards, so a long
    iteration holds back reclamation for one shard's copy at most.
- **Retaining.** `retain` offers each live entry to a predicate outside
  every lock, so the predicate may write to the cache.
  - It removes a rejected entry only if it is still published, found again
    under the shard lock by its handle. The caller's pin keeps that handle
    from naming another entry meanwhile, so an entry replaced after it was
    offered stays.
  - Expired entries are removed without being offered, as `Expired`; the
    rest as `Explicit`.
- **Conditional writes.** `remove_if` offers a live value to its
  predicate and removes it under the same shard lock, so no write to the
  key lands between the decision and the removal; the predicate must not
  write to the cache. `replace` writes a key only if it holds a live value
  and otherwise hands the pair back. It takes its slot before locking, as
  inserts do, fills it only once it finds the live value, and otherwise
  gives it back unwritten; a value too heavy to cache removes the old one,
  as an insert's does. Expired entries count as absent for both.
- **Draining.** `drain` empties each shard under its lock when it
  reaches it, sharing `clear`'s sweep, then yields clones of that shard's
  live entries. The loop body may write to the cache. Dropping the
  iterator stops it once the shard it reached is emptied, and retires that
  shard's entries, so the listener hears each one.
- **Memory.** `memory_used` reports the table, the entry slots handed out
  so far, each shard's filters, main-queue ring, and probation queue, and
  the bookkeeping: reader counters, retired-slot queues, shard locks, and
  load lanes, about 33 KB whatever the capacity plus half a kilobyte per
  shard. Without it, a 1,000-entry cache reported 48 KB of its 103 KB of
  resident memory; with it, 91 KB.
  Region slots never handed out are not counted, since a large region's
  untouched pages take no memory; memory keys and values own elsewhere is
  not counted either, as in quick_cache's `memory_used`.
- **Resizing.** `set_capacity` and `set_max_weight` change the limits at
  run time.
  - Tables are never rebuilt, so the builder's `max_capacity` reserves the
    table, the slab region, the main queues' rings, and the shard count for
    the largest capacity, about 8 bytes of table per reserved entry beyond
    the limit. Without it, the reserve is the built capacity.
  - `set_capacity` returns the limit it set: the request, raised to one
    entry per shard or lowered to the reserve.
  - Under a cache-wide resize lock, each shard takes its new shares under
    its own lock and evicts down to them before the call returns, so the
    listener hears `Size` for each. A shard whose entry share changes
    starts its ghost and main filters afresh at the new size, keeping its
    probation target within the new bounds.
  - Shard limits became atomics read with relaxed loads under the shard
    lock. `get` and `evict` compile to the same instructions; the insert
    body to 1,229 against 1,234.
- **Batched inserts.** `insert_many` inserts pairs in order, as single inserts
  would, and returns how many it cached. It takes one epoch pin per call
  instead of one per insert. Each chunk of 32 keys is hashed, and their home
  buckets prefetched, before the chunk is inserted. The pin is refreshed
  between chunks: held across a long batch, it would block every advance past
  its epoch, and in a test 49,500 removed entries waited, against 96 for
  single inserts. Removed entries are retired two at a time, so each
  retirement can also drain two of a backlog. A call with one pair is a
  plain insert. See "Batched inserts" under "Throughput against quick_cache
  0.7" for its effect.

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
  - a reader finding a key that relocation moves between its search of the
    key's home and of its alternate;
  - unread main-area keys leaving in insertion order;
  - forced pair evictions staying under 0.1%;
  - an 8-thread race of replacements, removals, loads, clears, and evictions
    that counts live values to prove each one is dropped exactly once;
  - eight racing callers loading a missing key once;
  - asynchronous and blocking callers, under either kind of leader, loading
    a missing key once; a dropped or failed asynchronous load waking its
    waiter and handing it the key;
  - batched inserts leaving the cache as single inserts do, pinning once,
    prefetching each home bucket, letting their removed entries be
    reclaimed, and draining a reclamation backlog as single inserts do
    (verified to fail with retirements four at a time);
  - failing and panicking loads handing the key to a waiter;
  - expired entries reading as missing, and leaving before read live keys;
  - reads extending entries by the time to idle while peeks, membership
    checks, and iteration do not, and an entry's own later deadline
    surviving reads (fails without the extension);
  - statistics, every removal cause, `clear`, and `peek`;
  - iteration visiting each live entry once, without marking it, while the
    loop body and another thread write;
  - `retain` removing rejected and expired entries with their causes, and
    sparing entries its predicate replaced;
  - `remove_if` removing only what its predicate accepts and never
    offering absent or expired values; `replace` writing only live keys,
    restarting the time to live, reweighing, removing the old value when
    the new one is too heavy, and giving its slot back when it fails
    (verified to fail without the give-back);
  - a placeholder guard inserting while another caller times out, a
    dropped guard waking its asynchronous waiter and handing it a guard,
    and a loader and a guard caller both waiting for a guard's insert;
  - writes superseding loads: an insert, removal, `insert_many`, clear,
    or drain while a key loads keeps the load from caching over it, and
    callers after the write load afresh; a writer that updates a source
    and invalidates the key, racing loaders on every loading path, never
    leaves an older version cached (the old code failed it in 4 runs of
    5); superseded loads giving back their slots and dropping their
    values (verified to fail without either); a load taking the shard
    lock twice; and asynchronous waiters that give up during a long load
    reusing their waker slots (1,000 slots before, three after);
  - `drain` yielding live entries once and reporting expired ones, and a
    dropped drain retiring the shard it emptied (verified to fail without
    the retirement on drop); `memory_used` counting the table, slots
    handed out, and their reuse;
  - `set_capacity` evicting down at once and growing back within the
    reserve, `set_max_weight` evicting down and refusing values heavier
    than the new shares, and resizes racing four writing threads.
- **Arena tests.** They cover:
  - aging of exactly three advances;
  - a pin blocking the second advance past its epoch;
  - `maintain()` dropping every removed value;
  - an exiting thread returning its vacant slots;
  - a dropped cache leaving other threads' stacks harmless;
  - other writers dropping an idle thread's retired values (verified to fail
    without the foreign drains), and leaving a retiring thread's alone;
  - writes reclaiming as many entries as they retire, and as many more while
    a backlog is due;
  - a write's reclamation batch fitting one cache line;
  - a thread's consecutive pins reusing one announcement until the epoch
    moves (1,000 of 1,000 pins took a read-modify-write before);
  - exiting threads returning their announcer slots, and a thread exiting
    under a pin a thread-local still holds keeping its slot (each verified
    to fail without its condition);
  - on Linux, `membarrier` registering and an advance evicting a parked
    announcement.
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
  shows the model catches aging of one advance. A second model gives each
  reader its own shard and scans only the claimed ones, with a reader
  claiming its shard while the writer retires and the main thread
  advances; its negative control reads the mask with a load instead of a
  read-modify-write, and the model finds a reclamation beneath the reader.
  A third model runs parked announcements: a reader reusing a parked
  epoch while a pinned writer retires and the main thread advances,
  evicting it, and a reader announcing afresh and then reusing. Negative
  controls drop the light fence and the heavy fence in turn; the model
  finds a reclamation beneath the reader in each (see "Parked pin
  announcements"). These models take minutes unoptimized, so they run with
  `--release`, as CI's model step does.
- **CI tools.** `scripts/cache_proof.sh` runs the module under Miri, ASan, and
  TSan. CI runs the tests on x86_64 Linux, Apple Silicon macOS, and x86_64
  Windows; on macOS and Windows, which lack `membarrier` and `madvise`, they
  run the portable fallbacks.

## Memory

Requested live bytes per resident entry after inserting twice the capacity,
from `examples/cache_density_compare.rs` (`stats_alloc`, not RSS). Allocator
rounding of the boxed entries is not included.

| Entries | Key | Value | ShardedCache | DirectPackedCache | quick_cache |
| ---: | --- | --- | ---: | ---: | ---: |
| 100,000 | 8 B | `u64` | 42.9 | 46.7 | 118.0 |
| 100,000 | 32 B | `[u8; 64]` | 127.0 | 128.4 | 306.7 |
| 1,000,000 | 8 B | `u64` | 39.7 | 44.2 | 94.4 |
| 1,000,000 | 32 B | `u64` | 63.8 | 69.0 | 144.7 |
| 1,000,000 | 32 B | `[u8; 64]` | 120.1 | 125.1 | 245.4 |

Overhead beyond the key and value is 23.7 to 24.1 bytes per entry at
1,000,000 entries, and 26.9 to 31.0 at 100,000, where the slab region's
spare slots, which a single-threaded fill never touches, add 1 to 4.6 on a
16-thread machine (see "The slab region"). Before four-byte handles it was
29.1 to 33.3 at both sizes. On a 16-thread machine, a 100,000-entry cache has 256 shards, and
their fixed state adds about 1.4 bytes per entry; at 1,000,000 entries it
adds about 0.5:

| Component | Bytes per entry |
| --- | ---: |
| Table at 80% load | 8.0 |
| Entry metadata | 8 |
| Main queue, with its slack | about 4 to 5 |
| Ghost filter | about 2 |
| Main filter | about 0.25 |
| Probation records, 16 bytes each | about 0.05 at the starting share |

Probation records grow with the probation a shard adapts to. A workload
that drives probation to its cap of a quarter of the shard holds up to 5
bytes of records per entry, plus the queue's growth slack.

For comparison, the overhead is 28 to 32 for `DirectPackedCache` and 78 to 210
for quick_cache. The main queue added about 3.5 bytes per entry and bought
about 3.4 points of trace hit rate.

## The slab region

Entries lived in slab chunks of 8 to 256 slots, allocated as a cache grew
and freed only when it dropped. Under write churn on more threads than free
cores, a reader preempted while pinned holds back every advance for its
time slice while the other threads go on retiring entries, so the slab's
high-water mark climbed past the cache's capacity and stayed there. Slots
allocated at 50,000 entries, replacing keys, on the 16-core M4 Max with
other work keeping about seven cores busy:

| Threads | Slots allocated | Over capacity |
| ---: | ---: | ---: |
| 2 to 4 | 52,000 to 54,000 | +5 to 8% |
| 8 | 57,000 to 59,000 | +14 to 18% |
| 12 to 16 | 77,000 to 134,000 | +54 to 168% |

A 1,000-entry cache reached 17,900 slots at eight threads. At 56 bytes a
slot, the worst run above kept 94 bytes per entry of slab beyond the
cache's own.

Each cache now reserves one region at build: its entry limit, plus 1,024
slots, plus 256 per available thread, for the reclamation backlog and each
thread's stack of vacant slots. The spare was 4,096 slots plus 256 per
thread until advances followed the reader shards in use (see "Reader shards
in use"). Its pages are touched only as
slots are first handed out. Past the region, writers take slots one at a
time from overflow chunks of 64, the lowest-addressed chunk with room
first. Overflow slots return to their chunk, never to a thread's stack,
and vacant region slots are taken first, so a chunk is freed once every
entry in it has been removed and reclaimed. An entry that stays cached
keeps its chunk; small chunks keep that cost small. Entries are never
moved: a value with interior mutability, such as an atomic, would lose
updates readers made through a guard to the old copy.

Requested bytes now include the region's spare slots, which a
single-threaded fill never touches: at 100,000 entries on 16 threads the
first spare added 2 to 8.5 bytes per entry, and the current one 1 to 4.6;
at 1,000,000 entries, under 1.

Throughput against the previous commit was level, but only once both
sides were built alike. In the first comparison, built with the default
16 codegen units, resident hits ran 11 to 16% slower and turnover 10 to
14%, with `get`'s machine code identical but for the compiler declining to
inline `BuildHasher::hash_one` (see "Hashing without `hash_one`"). Built
with one codegen unit, and again with the hashing change in both, resident
hits moved by −2 to +14% at 50,000 entries and by 0 to +1% at 1,000,000,
and writes by −3 to +4%.

### Huge pages on Linux

Many distributions enable transparent huge pages only where `madvise`
asks for them; GitHub's x86_64 runners do. There a million-entry cache's
table and region took 4 KB pages, a TLB entry each. On x86_64 and aarch64
Linux both allocations are now advised with `MADV_HUGEPAGE` over the whole
huge pages they span, before anything touches them, and the table is
allocated zeroed, an empty table, so a large one comes untouched from the
kernel instead of being written bucket by bucket. A test reads the
mappings' flags from `/proc/self/smaps`.

On a runner, with 4,000,000-entry resident hits running, the advised
process held 54 MB of huge pages and the parent none. Against the parent
commit, over seven pairs, cells at 1,000,000 entries moved by −1 to +12%
(cold fill +2% on one thread, +1 to +7% between pairs; turnover +3 to +4%;
four-thread resident hits +6%), and cells at 50,000 entries by −5 to +6%. The Linux VM could not show it: its memory was fragmented by other
containers, and even advised, it gave the process 2 MB of huge pages out of
2 GB.

## Four-byte handles

A bucket held six eight-byte entry addresses beside a control word and a
generations word: 13.3 bytes of table per entry at 80% load, the largest
share of the overhead. It now holds ten four-byte handles, so the table
takes 8.0 bytes per entry. A region slot's handle is its offset in words
from one word below the region's first slot, so readers decode it with one
add from a base they load before pinning. An overflow slot's handle has the
top bit set and names its chunk's id and its index there; the chunk's
address comes from a directory of lazily allocated blocks, found through
the region's header word, the word just below the first slot.

Bigger buckets also displace fewer keys into their alternates, so misses
probe fewer lines. Over the ARC traces' 18 cells the summed hit rate moved
from 667.01 to 667.70, within the hasher's seed noise.

Readers never decode an overflow handle inside their lookup. When a lookup
meets one with its key's tag, it gives up, and a cold, out-of-line function
searches again with the decoding path. Decoding inline, though no overflow
handle existed in the benchmark, made resident hits at 1,000,000 entries
20 to 25% slower on Apple Silicon in both codegen settings: whether as an
out-of-line call, inlined, or reading the directory through the region's
header to keep no extra state live. An overflow branch that only panicked,
or one that gave up the lookup, cost nothing, so the cost is the shape of
the code a returning path gives the lookup loop, not the instructions the
hit runs. Writers, under the shard lock, decode inline.

Before codegen was controlled, several decodings looked costly and were
not. Built with default codegen units against a lucky baseline, a chunk
directory, a multiply by the slot size, and three ALU steps each lost 17 to
25% of resident hits. Built with one codegen unit on both sides, the
directory and the ALU steps were level, as were one or two added ALU steps
on eight-byte handles, byte offsets instead of words, and four-byte handles
held in eight-byte atomics.

Against the slab region alone, the median of three pairs on macOS, each
build compared with the same build of the previous commit:

| Workload | Entries | Default, 1T | Default, 8T | One unit, 1T | One unit, 8T |
| --- | ---: | ---: | ---: | ---: | ---: |
| Resident hit | 50,000 | +3% | +2% | +22% | 0% |
| Resident hit | 1,000,000 | +2% | +3% | +1% | +3% |
| Read miss | 50,000 | +1% | −10% | −1% | −8% |
| Read miss | 1,000,000 | +10% | +9% | +12% | +27% |
| Turnover | 1,000,000 | +10% | +6% | +4% | +1% |
| Cold fill | 1,000,000 | +17% | +4% | +14% | +3% |

The other write cells moved by −8 to +10%. The 50,000-entry miss and
one-thread hit cells varied by 10 to 25% between runs.

On the Linux VM, the region and the handles together against the hashing
change, at 1,000,000 entries: resident hits +12 to +21%, misses +15 to
+27%, turnover +8 to +30%, and cold fill +5 to +22%, in both codegen
settings. At 50,000 entries cells moved by −9 to +17%, 17 of 20 within
±10%.

On x86_64 runners, against the hashing change at 50,000 entries on an AMD
EPYC 9V45 over five pairs, every cell moved by −7 to +6% in both codegen
settings. At 1,000,000 entries, two runs on EPYC 7763 runners, each
against quick_cache in the same run, bracket the change: before it, one
thread trailed quick_cache by 17% on resident hits and 52% on misses, and
four threads by 16% on misses; with the region and handles, but before
readers deferred overflow decoding, one thread led by 4% on hits and
trailed by 31% on misses, and four threads led by 23% on misses.
Single-thread turnover went from +5% to +20%.

Against quick_cache at 1,000,000 entries on macOS, the default build's
single thread was level on resident hits (+2%, from −14%) and 9% ahead on
turnover, and trailed by 51% on misses (from −59%). quick_cache's control
bytes, a byte per slot, stay in the core's L2; ShardedCache's tag words share
their lines with the handles, so a miss loads a bucket line from memory.
Eight threads led by 29% on hits, 65% on turnover, and 2.4 times on misses.

## Reader shards in use

Each thread's index, assigned on its first pin of any cache, sets a bit in
a process-wide mask before the index is first used. An advance reads the
mask after the generation, with a zero-delta read-modify-write, and scans
only the claimed shards' reader counters; advances come due after 32
retirements per claimed shard, at most the 512 they waited for before. A
pin costs nothing more: the index assignment it already makes sets the
bit once per thread. Indices are never reused, so once 64 threads have
claimed shards, every shard is scanned and advances wait for 512
retirements, as before.

A lone writer's removed values used to wait for three advances of 512
retirements: 1,536 of them were still held after 100,000 inserts into a
1,000-entry cache. They now wait for three of 32, and
`tests/sharded_cache_reclamation.rs` checks that at most 256 are held. At
50,000 entries, a single writer's slab high-water mark fell from 51,600 to
50,200 slots; with more threads, preemption decided it either way. The
region's spare then fell from 4,096 slots plus 256 per thread to 1,024 plus
256: at 1,000 and 50,000 entries, one to four threads' cold fill and
turnover and eight threads' turnover stayed in the region, while bursts
behind preempted readers at eight and sixteen threads spilled into
overflow chunks with either spare.

Reading the mask with a load was enough under the C++ sequentially
consistent order, by the argument above, but not under the Loom model's,
which found a reader that pinned a retired epoch after an advance's load
had missed its claim. The read-modify-write observes every earlier claim
in the mask's modification order, as the zero-delta counter reads do, and
the model then holds.

Throughput moved little: on macOS every cell moved by −5 to +11% in both
codegen settings, within run-to-run ranges; on the Linux VM, at 50,000
entries, writes gained 1 to 4% in the default build and moved by −7 to
+3% with one codegen unit. On an x86_64 runner (an AMD EPYC 7763), at
50,000 entries, single-thread cold fill gained 12% in the default build
and 7% with one codegen unit, both within tight ranges; the other cells
moved by −10 to +23% on a runner whose own baseline varied by up to 60%
between pairs. An earlier form, which marked the shard on
every pin, gained 2 to 12% on single-thread writes before later
write-path changes took most of that, and cost misses 1.2 to 3.4% (see
"Rejected and reverted").

## Parked pin announcements

A pin counted itself in its reader shard's counter with a sequentially
consistent read-modify-write, and its unpin uncounted it with another. On
x86 either is a full barrier, so a read's loads waited for the previous
read's misses to finish.

On Linux, threads now announce their pins:

- A thread claims one of 64 announcer slots on its first pin, and each cache
  has a 128-byte line per slot. A pin writes the epoch it pins there, and
  the last pin to leave keeps that epoch announced, parked.
- The thread's next pin marks the parked announcement active with a store
  and a compiler fence, then loads the line's eviction flag and the
  generation. If no advance evicted the announcement and the generation
  still names its epoch, the pin holds that epoch. No read-modify-write or
  fence instruction runs, and the unpin is a store.
- Otherwise the pin announces afresh: a store, a sequentially consistent
  fence, and a check that the generation has not moved, as with the
  counters. A thread that keeps reading does this once per advance.
- An advance that a parked announcement blocks evicts it. It sets the
  line's eviction flag, issues `membarrier(MEMBARRIER_CMD_PRIVATE_EXPEDITED)`,
  and reads the line again. The system call makes every running thread of
  the process execute a full barrier, so either the advance sees a pin that
  marked the line active, and waits, or that pin sees the flag and
  announces afresh.
- Only a thread that stopped pinning since the last advance needs evicting.
  On 4-vCPU runners, one barrier cost its caller 3 to 4.4 µs, and every
  other running thread 1 to 2 µs. That is too much to pay on every advance,
  which comes due after 32 to 512 retirements.
- A thread returns its slot when it exits, unless it still holds a pin
  that another thread-local's destructor drops later. The slot then stays
  held for good.
- These pin through the counters as before: threads past the 64th, threads
  pinning during their exit, kernels without `membarrier`, and other
  platforms.

The announcement state machine lives in `src/direct_epoch_protocol.rs`,
which the Loom models include. A compiler fence orders only with the
system call, not with an ordinary fence, so the model does not issue one.
Instead the reader holds a mutex across its reuse, and the eviction takes
the same mutex in place of the system call. The reuse then falls wholly
before or wholly after the barrier, as when the barrier lands before its
store or after its loads; landing between them only adds ordering. Removing
either side lets the model reclaim a slot under the reader.

Each cache spends 8 KB on the lines: 0.16 bytes per entry at 50,000
entries. Throughput over the counters, from builds of their own on GitHub
runners, five pairs per read cell and three per write cell. Guard reads
pin once per 64 reads, a bound:

| CPU | Entries | Threads | Hits | Misses | Guard hits | Guard misses |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| AMD EPYC 7763 | 50,000 | 1 | −2% | +2% | +33% | +18% |
| AMD EPYC 7763 | 50,000 | 4 | +1% | +3% | +7% | +18% |
| AMD EPYC 7763 | 1,000,000 | 1 | 0% | +4% | +47% | +39% |
| AMD EPYC 7763 | 1,000,000 | 4 | −2% | +3% | +1% | +8% |
| AMD EPYC 9V45 | 50,000 | 1 | +9% | +22% | +15% | +39% |
| AMD EPYC 9V45 | 50,000 | 4 | +7% | +10% | +22% | +30% |
| Intel Xeon 8573C | 1,000,000 | 1 | +4% | +9% | +10% | +18% |
| Intel Xeon 8573C | 1,000,000 | 4 | +6% | +14% | +16% | +38% |
| Neoverse N2 | 50,000 | 1 | +6 to +26% | +8 to +10% | +21 to +28% | +18 to +19% |
| Neoverse N2 | 50,000 | 4 | +17% | +6 to +9% | +17 to +25% | +17 to +22% |
| Neoverse N2 | 1,000,000 | 1 | 0 to +3% | −1 to +3% | +2 to +6% | +10 to +13% |
| Neoverse N2 | 1,000,000 | 4 | +1 to +3% | −6 to +3% | +4 to +5% | +3 to +12% |

Writes moved by −7 to +11%, mostly within 4%; single-thread turnover
gained 3 to 11%, except on the EPYC 7763 at 1,000,000 entries (−1%). The 4-vCPU runners are two cores
with two threads each, so four threads share execution units, and the
announcements' few added instructions weigh more there than at one
thread. On the EPYC 7763, where guard reads ran up to 47% faster, the
announcements left reads level: what a guard saves there is not the
barrier.

macOS keeps the counters. It has no `membarrier`, and announcements with a
sequentially consistent fence on each reuse read hits 12 to 24% slower on
an M4 Max (see "Rejected and reverted"). Builds that cannot announce
compile the counters alone: a first version that checked for an announcer
slot on every pin read resident hits 12 to 15% slower there at 50,000
entries, while `get` now compiles to the same instructions as before.

## Slab storage against boxed entries

Entries first lived in individually allocated boxes in the direct arena.
They then lived in slab chunks, and now in a slab region (see "Storage and
reclamation").

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

### Hashing without `hash_one`

`BuildHasher::hash_one` is a generic default method, and whether the
compiler inlines it depends on code elsewhere in the crate. Built with one
codegen unit, as release profiles tuned for speed often are, `get` called it
out of line, and on an M4 Max resident hits ran 23 to 25% slower than in the
default build, which inlined it. The two `get` functions differed in nothing
else. Writing `hash_one`'s three steps (`build_hasher`, `Hash::hash`,
`finish`) out in `get` restored the one-codegen-unit build, and making the
hash an opaque value cost it 10% more. The cause is the shape of the
generated code, not the hash: on Apple Silicon a cycle profile placed the
lost time on the entry's key load and the unpin.

Every operation now hashes through one always-inlined helper with the steps
written out. Against the previous commit, each build compared with the
previous default build, in millions of operations per second (median of
three pairs):

| Workload | Entries | Threads | Before, default | After, default | Before, one unit | After, one unit |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Resident hit | 50,000 | 1 | 31.3 | 30.1 | 27.0 | 30.6 |
| Resident hit | 50,000 | 8 | 384 | 354 | 297 | 372 |
| Resident hit | 1,000,000 | 1 | 7.44 | 7.48 | 5.70 | 7.37 |
| Resident hit | 1,000,000 | 8 | 65.2 | 65.6 | 49.1 | 65.8 |
| Turnover | 50,000 | 1 | 18.6 | 18.3 | 16.2 | 17.9 |
| Turnover | 1,000,000 | 1 | 7.51 | 7.37 | 6.42 | 7.30 |

Misses and writes moved within noise in both builds. The default build's
50,000-entry, eight-thread cells varied by up to 30% between runs. On the
Linux VM, where the default build was the slower one, each build compared
with its own previous build: turnover gained 5 to 13% in the default build
and 2 to 20% with one codegen unit, and resident hits −0.4 to +6% and −3 to
+11%; misses and replaces moved by −9 to +8%, within their run-to-run
ranges.

On an x86_64 runner (an AMD EPYC 7763, four vCPUs), against the previous
commit over five pairs at 50,000 entries, every cell of the default build
moved by −5 to +1%; with one codegen unit, resident hits gained 7% on one
thread and misses 3 to 4%, and the rest moved by −2 to +1%.

A `BuildHasher` that overrides `hash_one`, as ahash's nightly-only
specialization does, is bypassed; on stable Rust it computes the same
steps.

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
- **Prefetching only for new keys.** The prefetch ran before the lookup, so
  replacing a resident key in a full shard fetched a victim it would not
  evict. It now runs once the lookup misses. Replacing resident `u64` keys
  in a full cache on one thread, best of five on macOS: 40.2 to 42.6 ns
  against 42.4 to 45.0 ns at 50,000 entries. The probe's write workloads,
  whose replaces run below capacity, stayed within 3% at 50,000 and
  1,000,000 entries.
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
- **Relocating only away from home.** Relocation also moved keys from their
  alternates back home, and a lookup could then miss a resident key. It
  searched the home before the key was published there, and the alternate
  after the key was cleared from it. `get` returned `None`, and
  `get_or_insert_with` ran its loader again and replaced the value. A
  test-only hook that runs an insert between a lookup's two buckets
  reproduces it. Keys now move only from home to alternate, an order no
  lookup can miss. Over 40 churns of a full shard:
  - relocations fell from 0.018 to 0.009 per insert, and no insert evicted
    from a full pair;
  - 23.6% of keys lived in their alternates instead of 17.4%;
  - 57% of buckets counted overflow instead of 53%, so more lookups read a
    second bucket.

  None of this showed in throughput. On macOS, single-thread hits, misses,
  cold fill, and turnover at 50,000 and 1,000,000 entries moved by −4 to
  +7%, with almost every range crossing zero; hits at 1,000,000 entries by
  0% and misses by −1%. Mokabench moved by +0.1 to +1.4% (geometric means
  at 1 to 16 clients), and x86_64 cells against the parent commit stayed
  within the runner's noise. The trace hit-ratio sum read 667.60 and 667.75
  against 667.66 and 667.62.

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

Measured with `bounded_cache_probe` at the end of the changes described
above, through advances over the reader shards in use: 32-byte keys, 64-byte
`Arc<[u8]>` values, and 50,000 entries. Reads ran 16 million requests and
writes 8 million on macOS, and half that on Linux. Each figure is the paired
median of three runs, from default release builds; positive means
`ShardedCache` is faster.

Hosts:
- macOS: a shared 16-core Apple Silicon machine (an M4 Max). Other
  containers and services kept about ten cores busy during these runs, a
  load average of 9 to 10, against about four for the previous table.
- Linux: an aarch64 Docker VM on the same machine, using glibc's allocator.

On aarch64 Linux, Rust's default target calls an out-of-line helper for
every atomic read-modify-write, which picks LSE instructions at run time.
Built with `-C target-feature=+lse` on the Linux VM, single-thread resident
hits ran 25% faster for ShardedCache and 32% faster for quick_cache, and
turnover 18% and 9%. The gaps between the two moved by under 8 points
either way, and replace varied too much between pairs to read. On aarch64
servers, either cache gains from a build that inlines LSE atomics.

Two-thread cells have varied most between runs, by up to 30% for one
build, as threads land on performance or efficiency cores, and eight-thread
cells suffer most from the other load. Code layout alone moved some cells by
20% or more (see "Hashing without `hash_one`"). Direct before-and-after
comparisons, as quoted for each change, are the better guide to it.

| Workload | macOS 1T | macOS 2T | macOS 8T | Linux 1T | Linux 2T | Linux 8T |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Resident hit | −1% | +68% | +223% | −4% | +37% | +130% |
| Read miss | +87% | +178% | +370% | +79% | +277% | +493% |
| Turnover (95% reads) | +2% | +16% | +51% | −20% | −6% | +68% |
| Cold fill | −9% | −16% | −15% | −17% | −21% | −7% |
| Replace | +11% | −3% | +5% | −16% | −17% | +22% |
| Delete and reinsert | −5% | +8% | +37% | −23% | −14% | +44% |

The previous table, before hashing without `hash_one`, the slab region,
four-byte handles, and advances over the reader shards in use, read −8%,
+45%, and +292% for macOS resident hits; +90%, +196%, and +685% for macOS
misses; −3%, −5%, and +42% for macOS turnover; and −11%, +22%, and +156%
for Linux resident hits. Its single-thread writes read −5%, −1%, and −5% on
macOS and −12%, −18%, and −10% on Linux, for cold fill, replace, and
delete and reinsert. The eight-thread reads fell on the busier host:
ShardedCache's eight-thread resident hits ran at 318 Mops/s on macOS,
against 413 before, while quick_cache's ran at 110, against 105, and on
Linux at 209 against 92.

At 1,000,000 entries on macOS, past the caches, two pairs each:

| Workload | 1T | 8T |
| --- | ---: | ---: |
| Resident hit | +2% | +9% |
| Read miss | −50% | +109% |
| Turnover (95% reads) | +5% | +61% |
| Cold fill | +12% | +4% |
| Replace | +47% | +63% |
| Delete and reinsert | +32% | +70% |

One thread read misses at 28 Mops/s against quick_cache's 55, and
resident hits at 7.6 against 7.4; before four-byte handles they read 29
against 66 to 73, and 7.8 against 8.9 to 9.1. A miss loads one or two
64-byte bucket lines from the table, where quick_cache's control bytes for
as many entries stay in L2; a hit loads the bucket, the entry, and the
value's count (see "Four-byte handles" and "Rejected and reverted" for
layouts tried). Eight threads read misses at 235 Mops/s against 112, and
hits at 60 against 55.

Cold fill still trails: every insert into a full shard evicts under the lock
(see "Write path"), and single-thread writes pay what lock-free reads
require. An insert cannot reuse the evicted entry's memory until three epoch
advances later, where quick_cache frees under its shard lock, and every
write pays an epoch pin and its thread's retired-queue lock. A single-thread
`get` still waits at its unpin for the value clone's increment, which keeps
single-thread hits about level with quick_cache's rather than ahead (see
"Read path").

Two bounds, measured on one Linux thread and unsound with concurrent
readers, locate the single-thread write cost:

- Freeing removed entries at once instead of queuing them made cold fill
  29 to 32% faster, delete and reinsert 25 to 27%, and replace 34 to 35%:
  ahead of quick_cache, which led by 13 to 20%.
- Dropping only the retired queue's lock moved the same cells by −4 to +1%.

On Apple Silicon the delay is not the cost. Scanning only the reader shards
in use cut a lone writer's backlog from 1,536 removed entries to under 256,
so slots and values are reused about 100 writes after their last use
instead of 1,500 (see "Reader shards in use"). Single-thread cold fill
moved by +0.3% on macOS and +2 to +4% on Linux in the first form of that
change, and by 7 to 12% on x86 in the current one: there the delayed slot
read missed, which a prefetch now hides (see "Write path"). A macOS profile of single-thread turnover
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

The Linux VM has no hardware counters, and neither has a GitHub x86_64
runner, where `perf stat` reports cycles and cache misses as unsupported.
Cachegrind, which counts instructions exactly and simulates caches, ran
each write workload at 400,000 and 800,000 operations; the difference is
per operation, with setup removed. At 50,000 entries on one Linux thread:

| Per operation | Cold fill | Replace | Delete and reinsert |
| --- | ---: | ---: | ---: |
| ShardedCache instructions | 1,805 | 1,410 | 2,045 |
| quick_cache instructions | 1,217 | 963 | 1,349 |
| ShardedCache L1 data misses | 15.2 | 11.8 | 17.6 |
| quick_cache L1 data misses | 12.8 | 10.4 | 13.9 |

Allocator work was identical for the two caches, and ShardedCache hashed
once per insert where quick_cache hashed twice. Retiring and reclaiming
removed entries took 370 to 410 instructions per write, 60 to 80% of the
difference, spread over about a hundred source lines with none above 16;
the rest was spread over the policy, the table, and the insert itself.
Those instructions are cheap, as the bound above shows: freeing at once
gained 3 to 10%.

### Updates of a full cache

The probe's `update` workload upserts the seeded keys into a cache of
exactly their count: mostly replacements, and reinsertions of the few that
seeding evicted. The other resident workloads run at four times their key
count, so they never wrote to a full cache. On macOS, five paired runs of
4,000,000 operations, median and range:

| Entries | 1T | 8T |
| ---: | ---: | ---: |
| 50,000 | +11% (+5 to +19%) | +10% (−9 to +15%) |
| 1,000,000 | +56% (+49 to +58%) | +58% (+50 to +89%) |

### With `u64` keys and values

The probe's 32-byte keys and `Arc` payloads cost both caches hashing and
reference counts. With `u64` keys and values those costs fall away, and
ShardedCache's fixed work per operation shows: the epoch pin, and on writes
the new entry and the retirement of the old one, where quick_cache
overwrites in place under its shard lock. A scratch benchmark, at 50,000
entries, ran `get` hits on resident keys with room to spare, and upserts of
the seeded keys into an exactly full cache; each figure is the range over
three runs of the best of seven:

| Workload | macOS 1T | macOS 8T | Linux 1T | Linux 8T |
| --- | ---: | ---: | ---: | ---: |
| Resident hit | −7 to −13% | +806 to +830% | −2 to +3% | +694 to +715% |
| Upsert into a full cache | −56 to −60% | −1 to +6% | −53 to −54% | −15 to +31% |

On one thread, upserts ran at 23 to 26 Mops/s against quick_cache's 50 to
61. On macOS a pin is two read-modify-writes on shared counters, where
Linux reuses a parked announcement (see "Parked pin announcements"), which
leaves single-thread hits behind there and level on Linux.

### Batched inserts

`bounded_cache_probe`'s `sharded-batch` backend writes through
`insert_many`, 64 writes per call. On macOS, against `insert` in the same
build, the paired median of three runs:

| Workload | 50,000, 1T | 50,000, 8T | 1,000,000, 1T | 1,000,000, 8T |
| --- | ---: | ---: | ---: | ---: |
| Replace | +24% | +20% | +60% | +44% |
| Cold fill | +5% | +19% | +36% | +11% |

Replace finds each key before writing it, and with the batch's buckets
prefetched its finds overlap: at 1,000,000 entries batched replace ran at
5.7 Mops/s on one thread, against quick_cache's 2.5. Cold fill at 50,000
entries evicts on every insert from buckets already cached, and gains
little.

Batches first retired their removed entries four at a time. A retirement
reclaims as many aged entries as it queues, and then drained one more of a
due backlog, but only within its reclamation batch of four, so retiring
four never drained any. On a loaded host, a pinned thread preempted for its
time slice leaves a backlog behind it. Batches then kept it until the slab
region ran out, and writers took overflow slots one at a time under the
depot's lock: eight-thread batched cold fill ran at 9.2 and 12.3 Mops/s at
50,000 and 1,000,000 entries, against 17.1 and 21.6 for single inserts,
and some runs fell to 1.8. Batches now retire two at a time, and a
retirement reclaims twice what it queues while a backlog is due, which
brought those cells to 20.2 and 24.0 Mops/s. Batched replace gave up 2 to
12% of its gain against four at a time, and single inserts moved within
noise.

Sharing `insert`'s body with `insert_many` first cost single replaces 12%
at 50,000 entries. With two copies of the body, the compiler stopped
inlining five helpers it had inlined for their single call site:
Cachegrind counted 1,439 instructions per replace against 1,410. Forcing
them inline, and giving the slot stacks one shared thread-local accessor,
brought it to 1,405, and single-operation cells against the parent commit
moved by −4 to +12%, every range but one crossing zero.

### On x86_64

`.github/workflows/bench.yml` runs the same comparison on GitHub's x86_64
runners when a branch under `bench/` is pushed. The last run, on an AMD EPYC
9V45 with 4 vCPUs and the changes above, read as follows; each figure is
the median of three pairs, and positive means `ShardedCache` is faster:

| Workload | 50,000, 1T | 50,000, 4T | 1,000,000, 1T | 1,000,000, 4T |
| --- | ---: | ---: | ---: | ---: |
| Resident hit | +13% | +11% | +4% | −8% |
| Read miss | +18% | +63% | −54% | −21% |
| Turnover (95% reads) | −2% | +31% | +15% | +42% |
| Cold fill | −21% | +4% | +48% | +86% |
| Replace | −36% | −18% | −5% | −8% |
| Delete and reinsert | −32% | +9% | −9% | +26% |

The previous run, on an EPYC 9V74 before those changes, read −1%, +33%,
−13%, and +5% for resident hits, +28%, +58%, −61%, and −32% for misses, and
−20 to −23% for single-thread writes at 50,000 entries. Runners differ by
more than the changes: single-thread replace at 50,000 entries trailed by
15% on an EPYC 7763 and by 36 to 46% on EPYC 9V45 runners, with nearly the
same code.

On x86 every
read-modify-write is a full barrier, and a ShardedCache replace makes seven,
where quick_cache's makes three: the writer's pin and unpin, its retired
queue's lock and unlock, and the shard lock and the old value's count,
which both share. As upper bounds, skipping the writer's pin moved
single-thread writes by −2 to +4%, and a retired-queue lock that unlocks
with a store by 0 to +1%, delete and reinsert by +14% within −2 to +34%
(see "Rejected and reverted").

### On mokabench

[mokabench](https://github.com/moka-rs/mokabench), the moka project's load
generator, replays the ARC paper traces from a pool of client threads. Each
client takes 200 trace lines at a time, reads every block, and on a miss
builds a 128-byte `Arc<[u8]>` value and inserts it. `scripts/mokabench.sh`
adds a ShardedCache driver that mirrors mokabench's quick_cache driver, and
moves quick_cache to 0.7. Most of these cells miss more often than they hit,
so the replay weighs writes more than the tables above do.

Each figure is ShardedCache's throughput against quick_cache 0.7, the median
of three runs, with the code and hosts of the throughput tables above;
positive means ShardedCache is faster. Hit ratios, as ShardedCache /
quick_cache, are from one client, whose replay order is fixed. On macOS:

| Cell | Hit ratio | 1 client | 4 | 8 | 16 |
| --- | ---: | ---: | ---: | ---: | ---: |
| OLTP 256 | 22.2 / 19.3 | −17% | +2% | −13% | +15% |
| OLTP 512 | 31.8 / 28.0 | −12% | +9% | +10% | +40% |
| OLTP 1,000 | 39.9 / 36.4 | −11% | +9% | +4% | +32% |
| OLTP 2,000 | 46.4 / 43.9 | −10% | +2% | −5% | +23% |
| P3 20,000 | 8.8 / 8.3 | −10% | +3% | −5% | +19% |
| P3 160,000 | 53.4 / 53.8 | −13% | +15% | +37% | +61% |
| P8 20,000 | 22.5 / 22.6 | −2% | −5% | +5% | +26% |
| P8 160,000 | 76.6 / 76.5 | +2% | +15% | +61% | +91% |
| S1 100,000 | 9.5 / 9.6 | −9% | +6% | −14% | +11% |
| S1 800,000 | 57.3 / 56.9 | +23% | +48% | +92% | +152% |
| S3 100,000 | 12.6 / 12.8 | +5% | +4% | +12% | +8% |
| S3 400,000 | 42.4 / 42.3 | +20% | +16% | +46% | +59% |
| S3 800,000 | 69.5 / 68.3 | +25% | +26% | +57% | +113% |
| DS1 1,000,000 | 15.3 / 14.9 | +3% | +28% | +2% | +62% |
| DS1 4,000,000 | 45.1 / 44.8 | +2% | +27% | +47% | +70% |
| DS1 8,000,000 | 70.2 / 69.3 | +2% | +48% | +90% | +129% |
| Geometric mean | | −1% | +15% | +22% | +51% |

Geometric means over the same 16 cells on each host, with the previous
run's, before hashing without `hash_one`, the slab region, four-byte
handles, and advances over the reader shards in use, in brackets:

| Host | 1 client | 2 | 4 | 8 | 16 |
| --- | ---: | ---: | ---: | ---: | ---: |
| macOS | −1% (−3%) | | +15% (+6%) | +22% (+24%) | +51% (+63%) |
| Linux VM | −5% (−12%) | | +6% (+3%) | +27% (+25%) | +35% (+40%) |
| x86_64, 4 vCPUs | −5% (−4%) | +12% (+14%) | +21% (+27%) | | |

The x86_64 runs were on an AMD EPYC 7763, the previous one on an EPYC
9V74, and the macOS host carried more other load this time, which weighs
on sixteen clients most.

- ShardedCache's mean one-client hit ratio was 38.97%, against 37.99% for
  quick_cache; earlier, moka 0.12's sync cache read 37.87%. On the six S3
  and DS1 cells both replay, mokabench's ratios agreed with
  `hit_ratio_sim`'s to 0.02 points for both caches (see "Hit ratio").
- moka's sync cache ran at 2.4 to 22 times less throughput than
  ShardedCache in one macOS run, before these changes.
- The lead grows with the cache and the client count: on DS1 at 8,000,000
  entries, from +2% at one client to +129% at sixteen on macOS, and from
  +7% at one client to +34% at four on x86_64.
- The losses are single clients and small caches: OLTP at up to 2,000
  entries, P3 and P8 at 20,000, and S1 and S3 at 100,000, where most reads
  miss and most operations insert and evict. At one client these cells read
  −30% to +5% across the hosts, the worst on x86_64's OLTP. On the Linux
  VM, P3 and P8 at 20,000 also trailed at eight and sixteen clients, by 4
  to 29%.
- Sixteen clients fill the macOS host's 16 cores, four of them efficiency
  cores, beside the other load described above. quick_cache slowed from
  eight clients to sixteen in all 16 cells, and ShardedCache in 4.
- OLTP's runs last 40 to 90 ms, so its cells vary most.

A Linux profile of P3 at one client, before these changes, puts the
difference in writes. Lookups took about 420 ms for both caches; the write path took about 570 ms for
ShardedCache and 300 for quick_cache, in queuing, placing, and later freeing
removed entries, as on the probe (see "Throughput against quick_cache
0.7"). Within the difference, mokabench's own zeroing of new values took 77
ms against 16, which points to colder memory from the allocator: evicted
values are freed three advances after their eviction rather than at once.

### Against the first version and moka

The first `ShardedCache` (`f871aa6`, 2026-10-17) kept boxed entries in the
direct arena, eight-byte handles, and a table per shard. Its probe, the
version after `#24` (`2635115`), and `6d0d3be` ("now") were built alike and
run in rotation with quick_cache 0.7; each figure is the paired median of
three runs. The later backlog drain moved single-insert cells within noise
(see "Batched inserts"). The x86_64 runs were on an AMD EPYC 7763 with 4
vCPUs.

Now over the first version:

| Workload | macOS 50K, 1T | 50K, 8T | 1M, 1T | 1M, 8T | x86 50K, 1T | 50K, 4T | 1M, 1T | 1M, 4T |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Resident hit | +25% | +15% | +32% | +37% | +26% | +2% | −17% | −16% |
| Read miss | +8% | −1% | +16% | +23% | +16% | +16% | +42% | +46% |
| Cold fill | +9% | +34% | +32% | +53% | +49% | +131% | +103% | +112% |
| Turnover (95% reads) | +9% | +36% | +77% | +32% | +16% | +38% | +12% | +59% |
| Replace | +10% | +26% | +29% | +34% | +67% | +49% | +43% | +135% |
| Delete and reinsert | +25% | +34% | +18% | +46% | +46% | +42% | +30% | +58% |

Now over quick_cache:

| Workload | macOS 50K, 1T | 50K, 8T | 1M, 1T | 1M, 8T | x86 50K, 1T | 50K, 4T | 1M, 1T | 1M, 4T |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Resident hit | −9% | +282% | −7% | +16% | −20% | +37% | −18% | +5% |
| Read miss | +98% | +568% | −53% | +171% | +12% | +50% | −46% | −4% |
| Cold fill | −4% | −15% | +13% | +17% | −21% | +9% | +39% | +64% |
| Turnover (95% reads) | −11% | +52% | +10% | +34% | −3% | +33% | −9% | +42% |
| Replace | +29% | +3% | +47% | +66% | −17% | −19% | −9% | −5% |
| Delete and reinsert | +1% | +43% | +32% | +56% | −16% | +5% | +4% | +20% |

Resident hits at 1,000,000 entries on x86_64 ran 17% slower than the first
version, the one regression here. Cachegrind on that runner counted 388
instructions and 4.8 L1 data misses per hit now, against 391 and 4.7 for
the first version, with the working set inside the simulated last level
and no mispredicted branches in either, so the difference is in the
layout's latency rather than in work or misses. The first version's boxed
entries were allocated after their values, as under "Slab storage against
boxed entries". A dedicated A/B on macOS, five pairs with both codegen
settings, read single-thread resident hits at 0 to +6% and turnover at −2
to +1% against `#24`.

On mokabench, ShardedCache's throughput over quick_cache's and moka's sync
cache (0.12.8), as geometric means over the 16 cells. Each host ran each
version against quick_cache in the same job, so the ratios compare across
versions; macOS ran once and x86_64 three times:

| Host | Clients | First over quick_cache | `#24` | Now | Now over moka |
| --- | ---: | ---: | ---: | ---: | ---: |
| macOS | 1 | 0.85x | | 1.02x | 3.36x |
| macOS | 4 | 0.81x | | 1.13x | 5.29x |
| macOS | 16 | 1.11x | | 1.42x | 12.51x |
| x86_64 | 1 | 0.73x | 0.97x | 0.99x | 4.56x |
| x86_64 | 2 | 0.63x | 1.14x | 1.18x | 7.02x |
| x86_64 | 4 | 0.70x | 1.27x | 1.37x | 9.54x |

The mean one-client hit ratio rose from 37.75% to 38.97% (38.95% on
x86_64), against 37.99% for quick_cache and 37.87% for moka. moka leads in
5 of the 16 cells, by 2.9 points on P3 at 20,000 entries and by under a
point in the others: OLTP at 256 entries, DS1 at 4,000,000, and S3 at
400,000 and 800,000.

Requested bytes per entry beyond the key and value, measured as in
"Memory", with moka 0.12.16's sync cache read after `run_pending_tasks`:

| Entries | Key, value | First version | Now | quick_cache | moka |
| ---: | --- | ---: | ---: | ---: | ---: |
| 100,000 | 8 B, `u64` | 30.7 | 26.9 | 102.0 | 232.0 |
| 100,000 | 32 B, `u64` | 30.9 | 28.1 | 140.9 | 237.3 |
| 100,000 | 32 B, `[u8; 64]` | 31.4 | 31.0 | 210.7 | 236.8 |
| 1,000,000 | 8 B, `u64` | 29.0 | 23.7 | 78.4 | 223.7 |
| 1,000,000 | 32 B, `u64` | 29.0 | 23.8 | 104.7 | 227.5 |
| 1,000,000 | 32 B, `[u8; 64]` | 29.1 | 24.1 | 149.4 | 227.5 |

The first version's figures leave out the allocator's rounding of its
boxed entries.

## Tail latency against quick_cache 0.7

Every operation was timed (`LATENCY_STRIDE` 1) at 1,000,000 entries,
8,000,000 operations, and 64-byte values, with the code and hosts of the
throughput tables above. Each cell is the median of three runs, in
nanoseconds, as ShardedCache / quick_cache; both hosts' timers tick every
42 ns. Cold fill includes filling the empty cache.

macOS:

| Cell | p99 | p99.9 | p99.99 | p99.999 |
| --- | ---: | ---: | ---: | ---: |
| Cold fill, 1 thread | 458 / 416 | 1,375 / 1,917 | 5,458 / 8,958 | 14,917 / 232,208 |
| Cold fill, 8 threads | 917 / 917 | 2,625 / 13,958 | 23,042 / 122,959 | 83,833 / 416,041 |
| Turnover, 1 thread | 584 / 542 | 2,250 / 3,708 | 6,125 / 6,583 | 13,500 / 21,750 |
| Turnover, 8 threads | 709 / 875 | 1,708 / 14,208 | 10,125 / 62,125 | 36,958 / 115,083 |

Linux:

| Cell | p99 | p99.9 | p99.99 | p99.999 |
| --- | ---: | ---: | ---: | ---: |
| Cold fill, 1 thread | 625 / 625 | 1,708 / 3,083 | 8,958 / 14,666 | 20,625 / 264,041 |
| Cold fill, 8 threads | 1,334 / 1,333 | 7,833 / 33,292 | 44,958 / 178,917 | 127,167 / 575,333 |
| Turnover, 1 thread | 791 / 916 | 3,208 / 4,958 | 10,083 / 11,375 | 19,208 / 25,667 |
| Turnover, 8 threads | 750 / 1,166 | 1,834 / 31,250 | 16,041 / 107,000 | 64,500 / 243,500 |

From p99.9 on, ShardedCache's tail is the shorter one in every cell: up to
16 times shorter at p99.999 for single-thread cold fill, where quick_cache
stalled for 232 to 264 µs, and 3 to 17 times shorter from p99.9 at eight
threads, as readers never wait on a shard lock. At p99 ShardedCache trails
by one timer tick (42 ns) on one macOS thread, is level for cold fill
elsewhere, and is shorter for turnover on Linux and at eight threads. In the previous run,
before the changes since "Hashing without `hash_one`", single-thread cold
fill's p99.9 trailed, 1,959 against 1,167 ns on macOS, and the p99.999 for
macOS single-thread turnover was level.

Two steps of a write used to take time proportional to the shard under its
lock: compacting the whole main queue and clearing the ghost's next
generation. Each now does a bounded share, and relocation no longer reads the
residents it tries (see "Write path"). What remains is the CLOCK sweep past
read keys, as in quick_cache's hot ring, and a scan of two reader counters
per shard in use every 32 retirements per shard. On turnover with 1,000,000
entries per shard, the sweep averaged 3.3 records per main-area eviction;
13 of 908,149 sweeps passed 1,000 records, and the longest took 1,504.

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
- **A per-bucket summary of tags for misses.** A 32-bit word per bucket,
  in an array beside the table, about 0.5 bytes per entry, held a bit per
  class of the tags in its slots and a bit set while its overflow count
  was nonzero.
  - A first version, checked before loading the bucket, cost resident hits
    9 to 34%, likely from the shape it gave `get`'s code.
  - A second version checked the home's summary, and the alternate's when
    the home had overflowed, before pinning, so a miss it ruled out took no
    pin and loaded no bucket. A tag's class was its low five bits, with 31
    sharing 30's bit. In a full cache about a quarter of misses passed.
    Writers kept summaries exact under the shard lock: a placement set its
    tag's bit, and a removal cleared it unless a word-wide byte test found
    another tag of its class in the bucket.
  - On one thread of an M4 Max, misses at 1,000,000 entries ran at 62 to
    68 Mops/s instead of 26 to 30, and 7 to 17% faster at 50,000. Resident
    hits lost 1 to 2% at 1,000,000 entries and moved by −11 to +4% at
    50,000, in noisy runs, for about 30 more instructions per read.
    Single-thread cold fills lost 5 to 6%, and eight-thread writes 6 to 14%.
  - On mokabench, against the same commit without summaries, over three
    paired repetitions of 16 cells, the geometric mean was −6% at one
    client, −8% at four, −11% at eight, and −12.5% at sixteen. It lost even
    where 9 to 13% of reads hit. A miss in a cache is mostly followed by an insert of
    the same key, which loads the same bucket line, so ruling the miss out
    early saved little, while every read paid the check and every write the
    upkeep.
  - Recomputing a bucket's summary from all ten slots after each removal
    retired 17% more instructions in a cold fill, and cost it 12 to 13%:
    the loop over the tag bytes did not compile to straight-line code.
    Updating it in place, storing it without testing for a change, and
    inlining the placement brought the cost to 5 to 6%. Release stores,
    relaxed ones, prefetching the summary lines, and sending every
    summary store to one word each left that cost unchanged.
- **Decoding overflow handles inside readers' lookups.** Resident hits at
  1,000,000 entries lost 20 to 25% on Apple Silicon, though no overflow
  handle existed; see "Four-byte handles".
- **64-byte slots.** Aligning slots to cache lines moved reads by −8 to +5%
  and cost 8 bytes per 56-byte entry.
- **Trying an advance every 32 retirements per shard.** Each try scans 128
  reader counters; eight-thread replace fell 63%. Tries now wait for 512
  retirements across the cache.
- **Shorter reclamation delays on mokabench.** On the Linux VM, P3 at
  20,000 entries and one client, where probation evicts keys soon after
  they enter, advancing every 128, 32, or 8 retirements across the cache
  instead of 512 took −1%, 0%, and +6% time over five runs. At 32,
  mokabench's zeroing of new values fell from 5.6% of the time to 2.5%, as
  freed values came back warmer, but evicting, retiring, and the counter
  scans grew by as much.
- **Fewer retirement reports, or no foreign drains, on mokabench.** On the
  same cell at eight clients, reporting retirements every 128 instead of 16
  took 8% more time, and skipping drains of other threads' queues 1% more.
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
- **A floor under the probation target.** With no key ever read again,
  probation adapts down to one entry, so each cold-fill insert evicts the
  shard's previous insert, whose lines another core may still hold. A floor
  of 64 entries made eight-thread cold fill 4.5% faster and single-thread
  cold fill 16% slower; floors of 4 and 16 moved eight threads by 1 to 2%.
- **A spin lock for the retired queues.** Its uncontended unlock is a
  store, where the mutex's is a read-modify-write, a full barrier on x86.
  On x86, single-thread writes moved by 0 to +1%, delete and reinsert by
  +14% within −2 to +34%; on macOS, by −2 to +7% within wider ranges. Not
  enough to replace the mutex with unsafe code of our own.
- **Scanning only the reader shards in use, marked on every pin.** Each
  pin marked its reader shard in a per-cache mask; tries scanned only
  marked shards' counters and came due after 32 retirements per marked
  shard. A lone writer's backlog fell from 1,536 removed entries to under
  256, and requested memory at 100,000 entries by 0.7 to 1.6 bytes per
  entry. Single-thread writes on Linux gained 2 to 12%, and eight-thread
  replace 2 to 6%. But the mark check on every pin cost reads: misses lost
  1.2 to 1.6% on macOS and 1.8 to 3.4% on Linux, and eight-thread resident
  hits 3.2 to 3.6% on Linux. On x86, single-thread cold fill gained 6% and
  reads lost 2 to 3%. The process-wide mask set once per thread replaced it
  (see "Reader shards in use").
- **Reads without a pin, as a bound.** With `get`'s pin removed, which is
  unsound, one-thread resident hits on an M4 Max ran 11 to 20% faster at
  50,000 entries and 8 to 9% at 1,000,000, and misses 15% and 26 to 28%.
  An acquiring pin and a releasing unpin, in place of `SeqCst` both, moved
  reads by −2 to +7%, with no consistent sign.
- **Misses that skip the alternate bucket, as a bound.** One-thread misses
  moved by −1 to +2% at both 50,000 and 1,000,000 entries: a miss pays for
  its home bucket's line, not its alternate's.
- **Freeing removed entries at once, as a bound.** On the Linux VM at
  50,000 entries, freeing each removed entry without waiting for readers,
  which is unsound, made one-thread cold fills 4 to 6% faster, and delete
  and reinsert and turnover 3 to 10%; replace moved within its noise. The
  same bound had been 29 to 35% before advances over the reader shards in
  use and the eviction prefetch. Deferred reclamation is no longer the main
  single-thread write cost.
- **Trimming the retire path.** Fusing its two scans for aged slots into one
  loop added 8 instructions per write in Cachegrind. Earlier redesigns of
  that path (per-thread lanes, a lock-free free stack, a spin lock, larger
  batches) are above.
- **Batched reads.** A guard method looking up eight `u64` keys at a time,
  prefetching their home buckets first, ran single-thread resident hits on
  an M4 Max at 175 to 207 Mops/s against 201 to 208 for the guard's own
  reads at 50,000 entries, and at 43 to 49 against 38 to 41 at 1,000,000.
  Prefetching each key's first candidate entry as well made both sizes
  slower. The core already overlaps independent guard reads, so a batched
  read API did not pay for itself.
- **Access marks in the bucket.** Each entry's two access marks moved into
  the top bits of its slot's generation byte, leaving six bits of placement
  count. That would let caches without weights or deadlines drop the 8-byte
  entry metadata, and let eviction sweeps read marks from bucket lines they
  already load. Trace hit ratios stayed level (667.65 and 667.63), and
  writes moved by −5 to +8% on macOS; but single-thread resident hits fell
  7 to 14%, eight-thread hits 5 to 12%, and eight-thread turnover 6%. A hit
  had to find its bucket again and load the marks after comparing its key.
- **Read-path variants on x86_64, as bounds.** On GitHub runners, five
  pairs per cell against the same commit, in builds of their own:
  - Reads through one guard refreshed every 64 operations, so without a
    per-read pin: single-thread resident hits +7 to +21% at 50,000 entries
    (EPYC 9V74 and 9V45) and +45 to +47% at 1,000,000 (EPYC 7763), misses
    +15 to +37% and +61 to +71%, and turnover +4 to +5% and +32 to +33%.
    Four threads gained 0 to +18%.
  - An unpin that stores the decremented count instead of a
    read-modify-write, sound only while each reader shard has one thread,
    as in the probe: −11 to +4% on every workload. The pin's own
    read-modify-write remains, and on x86 any read-modify-write holds back
    the next read's loads until the previous read's have finished, so
    consecutive reads' misses still run in series.
  - Huge pages turned off: −5 to +5%. Slots aligned to 64 bytes, which
    puts each 56-byte entry in one cache line: −4 to +9%.

  Removing the pin's barrier needs an asymmetric fence: readers announcing
  with plain stores, and advances issuing a process-wide barrier
  (`membarrier` on Linux). Parked announcements now do that on Linux, and
  gained up to 26%; on the EPYC 7763 they left reads level, so there the
  barrier was not the gap (see "Parked pin announcements").
- **A barrier on every advance.** Announcing with plain stores but
  unparking on every unpin would make each advance issue `membarrier`. It
  cost its caller 3 to 4.4 µs and each other running thread 1 to 2 µs on
  4-vCPU runners (Intel Xeon 8573C, Neoverse N2), while advances come due
  after 32 to 512 retirements, a few microseconds of writes. Parking the
  announcement confines the barrier to threads that stopped pinning.
- **Announcements on macOS, with fences.** Without `membarrier`, a reuse
  needs a sequentially consistent fence in place of the compiler fence. On
  an M4 Max that read hits 12 to 24% slower than the counters' read-modify-
  writes, and turnover 10 to 15% slower in three of four cells; misses
  moved by −6 to +13%.
- **Closing scc's lead on eight-thread writes.** scc 3.8's `HashCache`
  locks one 32-entry bucket per write, evicts that bucket's least recently
  used entry in place, and drops it at once, as its readers lock buckets
  too. On macOS it scaled cold fill 3.5 times from one thread to eight at
  50,000 entries, against 1.9 times for ShardedCache, and led
  delete-reinsert by 63%. At eight threads, 19% of ShardedCache's worker
  samples waited on locks, mostly shard locks held across misses on lines
  another core last wrote, and the rest on the slot depot. Measured:
  - More shards: 512 and 2,048 instead of 128 lifted eight-thread cold fill
    7% and 34% and delete-reinsert 14% and 13%. Trace hit ratios held down
    to about 128 entries per shard, within −0.6 to +0.4 points on six
    cells, but at 64 entries P8 at 32,768 lost 2.0 points and at 32 lost
    7.8, and each doubling of the shard count adds about 1.4 bytes per
    entry of shard state at 100,000 entries.
  - Thread batches of 64 slots instead of 32: −2 to +4%.

  The rest is the policy: an evicting insert touches the shard's header,
  probation and main queues, ghost and shadow words, and the victim's
  bucket and entry, where scc touches one bucket. Its hit ratio is the
  other side of that: 32.8% against 39.0% on the mokabench traces.
- **Smaller shards.** Shards of at least 128 entries instead of 256, and
  up to 64 per thread instead of 16, on macOS, five pairs: eight-thread
  writes +3 to +13%, single-thread cold fill −4 to −6%, and eight-thread
  misses at 1,000,000 entries −13%. The 18 hit-ratio cells summed 1.80
  points lower, P3 at 131,072 entries 0.71 lower, and bytes per entry rose
  by 2.1 at 50,000 entries, 4.4 at 131,072, and 0.5 at 1,000,000.
- **Buffered policy updates, bounded first.** Caffeine places an entry at
  once and queues its policy work for a batched pass. A bound for that: a
  build with no policy, where an insert into a full shard evicts the next
  occupied slot of a table sweep, without probation, the main queue, or
  filters. On macOS, five pairs, eight-thread cold fill gained 13% at
  50,000 entries and 2.5% at 1,000,000, delete-reinsert 25% and 14%;
  replace was level, turnover fell 9 to 11% with the lost hit ratio, and
  single-thread cold fill fell 24 to 38% to the sweep. Buffering moves the
  policy's work rather than removing it, so it gains no more than that,
  against scc's 88% lead on eight-thread cold fill at 50,000 entries. The
  rest of that lead is scc's per-bucket locks and its entries stored in
  the bucket.

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
sh scripts/mokabench.sh 3 1,4,8,16 "s3 ds1 p8 s1 p3 oltp" target/mokabench.csv
python3 scripts/summarize_mokabench.py target/mokabench.csv
$PROBE sharded-batch replace 1000000 4000000 1 64 1
# Instructions per write, on Linux: run twice and subtract.
valgrind --tool=cachegrind --cache-sim=yes $PROBE sharded replace 50000 400000 1 64 1
valgrind --tool=cachegrind --cache-sim=yes $PROBE sharded replace 50000 800000 1 64 1
```
