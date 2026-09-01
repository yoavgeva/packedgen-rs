# PackedGen

PackedGen is an experimental Rust library for memory-dense concurrent maps
built from immutable packed generations, lock-free mutable overlays, atomic
value slots, and exact online rebuild publication.

The primary backend is not an implementation of the Elastic Hashing paper. It
uses PtrHash-indexed frozen generations plus lock-free overlays. The repository
retains an auditable, attributed `opthash` workspace crate implementing the
paper-derived Elastic and Funnel maps as explicit research and comparison
backends. The exact boundary is documented in
[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

PackedGen currently supports 64-bit targets only. The primary cache stores
native pointers in compact 64-bit handles, and unsupported pointer widths fail
with an explicit compile-time error rather than relying on incidental layout
or shift failures.

## Status

This repository is **not production-ready**. Its primary
`AtomicPackedGenMap` has lock-free readers and point
writers, but still needs Loom/Miri/sanitizer verification, Linux-pinned p99
measurements, traversal/eviction APIs, and a stable public API.

`ConcurrentSwissMap` now provides a separate practical multi-reader,
multi-writer backend. Power-of-two shards independently own a read/write lock,
SegmentedSwiss table, packed-key arena, and live counter. Point reads and writes
never acquire a global lock; atomic insert-new, update, upsert, conditional
remove, and shard-by-shard key compaction cover the first ETS-like service
surface. It remains lock-based and does not yet implement ETS traversal, match
specifications, ownership, or bags.

The lock-free implementation has four variants. `LockFreeBinaryMap` is a fully
dynamic Papaya control. `LockFreeHybridMap` combines a dense immutable packed
base with a lock-free mutable overlay. `LockFreeGenerationMap` publishes those
generations through ArcSwap using stable generic value cells.
`AtomicPackedGenMap` (an alias for `LockFreeAtomicU64GenerationMap`) additionally
stores `NonMaxU64` values directly
in atomic frozen slots, so existing-key update/delete/reinsert operations need
no overlay allocation. New 32-byte keys are stored inline in lock-free overlay
nodes, avoiding the separate boxed-key allocation. Workloads with exact 8, 16,
24, 31, 40, 48, 56, or 64-byte keys can explicitly preallocate the matching
inline class; unexpected widths use the fully dynamic fallback. Miss-heavy
workloads can embed a nine-bit digest fingerprint into otherwise unused packed
reference bits for frozen keys up to 255 bytes. This adds no retained bytes and
rejects most misses before fetching member key bytes; longer-key generations
fall back to exact references. The ordinary hit-heavy policy remains
exact-only. Readers and point writers acquire no library-owned lock,
including during rebuilds. A striped atomic handoff keeps equal-key writes on
one generation until their predecessor stripe closes, then safely activates
the new overlay or direct atomic base. Per-stripe shadow state also lets clean
atomic-base hits bypass an unnecessary overlay lookup. Rebuild packs the stable
predecessor without changed-key replay. Whole-table maintenance can still
starve under nonstop writes, so this remains experimental rather than an ETS
replacement.

The opt-in `prepared-keys` feature adds 16-byte exact handles for explicitly
identified hot keys. Preparation captures either a frozen dense slot or a
native adaptive-overlay slot. Reads use either stable slot directly while it
remains valid; prepared mutations retain the frozen direct-slot fast path and
otherwise take the ordinary exact writer route. Callers still provide the
original key bytes, and stale, wrong-key, cross-map, or rebuilt handles fall
back exactly. Allocation-free `prepare_key_batch` and `get_prepared_batch`
APIs amortize generation pinning for caller-owned request batches and cheaply
refresh a hot set after rebuild.

Write-heavy callers can additionally enable `prepared-batch-gate`. It adds 16
cache-line-separated writer gates—1,040 measured requested bytes per live
generation—to amortize update/replacement writer pinning once per batch. The
ordinary `prepared-keys` path keeps zero additional persistent batch state.

For cache-style new keys, `AtomicPackedGenMap::entry` performs one exact probe
and returns `AtomicEntry::Occupied(value)` or a one-shot
`AtomicVacantEntry`. The vacant path reuses the route hash and frozen absence
proof for `insert`/`insert_new`, while concurrent writers still resolve through
the same atomic slot or overlay cell. Keep vacant handles short-lived: they pin
one writer stripe so that a rebuild cannot invalidate the proof.

When the caller only needs the existing-or-inserted value,
`AtomicPackedGenMap::get_or_insert` is faster than composing the entry API. It
uses one writer pin, one frozen lookup, and one overlay traversal; a racing
writer's value is returned without replacement. Successful insert, upsert,
delete, and prepared direct-slot mutations also combine their logical-length
publication with writer release on the same stripe word.

For worker loops or request batches, the optional `operation-batch` feature
adds `operation_guard()` and `get_or_insert_batch()`. One short-lived guard
amortizes generation handoff protection across reads and every atomic mutation.
It costs about 1 KiB per live generation, adds no per-entry bytes, and can delay
a rebuild while retained, so guards should not be stored indefinitely.

Exact 32-byte new keys use an elastic atomic overlay. The primary table reserves
1.10x slots; only actual four-bucket overflow lazily publishes an ordered packed
segment, with a smaller emergency segment and Papaya residual spill behind it.
Each segment uses lock-free ArcSwap publication and borrowed guards, so readers
and steady writers do not contend on an Arc reference count. At one million
live overlay keys this measured 46.290 requested B/entry and 14 allocations,
versus DashMap at 84.438 B/entry and boxed Papaya at 82.937 B/entry.

`AtomicGenerationOverlay::AtomicUpTo32` extends the same physical layout to
mixed zero-through-32-byte keys. Short lengths occupy the otherwise unused
final byte, while disjoint control-tag domains keep short encodings exact even
when their bytes equal a full-width key. It adds no per-slot RAM. At one million
16-byte keys it measured 46.290 B/entry versus DashMap at 68.438 and boxed
Papaya at 66.937; at 31 bytes the controls used 83.438 and 81.937 respectively.
At 100K entries its RAM crossover with boxed DashMap is 14-byte keys; smaller
fixed keys can favor a purpose-sized table.

`AtomicGenerationOverlay::AtomicUpTo16` is that purpose-sized path. It uses two
atomic key words instead of four while retaining the same elastic publication
algorithm and exact short/full-width tag separation. At one million entries it
measured **28.250 B/entry and 14 allocations** for every 0–16-byte key length,
versus DashMap at 60.438–68.438 and boxed Papaya at 58.937–66.937 for 8–16
bytes.

`AtomicGenerationOverlay::AtomicUpTo8` specializes the same implementation for
integer-sized and other zero-through-8-byte keys. One million 8-byte keys used
**19.230 B/entry and 14 allocations**, versus 28.250 for `AtomicUpTo16`,
35.084 for exact-width inline Papaya, 58.937 for boxed Papaya, and 60.438 for
DashMap. In the 100K-key alternating operation probe it led all four overlay
controls on new-key insertion at both one and eight threads. It is opt-in:
keys longer than eight bytes remain exact through the generic Papaya fallback.

For caches that cannot bound key length,
`AtomicGenerationOverlay::AtomicAdaptive` learns a short exact sample and
shares one configured capacity proportionally across 8, 16, 24, and 32-byte
atomic classes plus one exact six-word atomic 48-byte class. After learning,
residual widths use an append-only concurrent arbitrary-length table with
native bucket sampling. The bounded startup sample uses the same native stable
cells and remains directly preparable; only emergency saturation spills to
Papaya. On a one-million-entry mixed 8/16/24/32/48-byte fixture it
used **33.074 B/entry and 8,229 allocations** with the retained 1.15x
read-optimized headroom, versus 61.693 for direct Papaya and 71.238 for
DashMap.
Capacity must include distinct insertion churn until the next generation
rebuild. Adaptive metadata snapshots detect slot pressure, key-distribution
drift, and inline-class spill without adding foreground counter updates; a
background worker can call `rebuild_adaptive_if_needed` to publish at most one
required generation.
Design, measurements, and FerricStore integration limits are documented in
[`docs/ADAPTIVE_CACHE.md`](docs/ADAPTIVE_CACHE.md).
The direct Papaya/SCC/DashMap/Flurry/HashBrown density, churn, operation, and
stability comparison is in
[`docs/ADAPTIVE_EXTERNAL_COMPARISON.md`](docs/ADAPTIVE_EXTERNAL_COMPARISON.md).

`MutableSegmentCache` is the variable-value packed-segment experiment:
arbitrary binary keys and values, compact relative TTL, lock-free point reads,
multi-writer CAS updates, immediate tombstones, and about 103.4 B/entry for the
one-million-entry 64-byte-value fixture. The opt-in
`OnlineMutableSegmentCache` adds striped writer redirection and two reader
epochs so a stable predecessor can be packed and published while point traffic
continues, without replaying a changed-key log or adding per-entry generation
metadata. The online wrapper adds under one kilobyte of fixed state in that
fixture. Unique overlay churn now drives `compaction_recommended()` and
`compact_if_recommended()` without requiring a caller-supplied cache-size
ceiling; the caller still owns background-thread scheduling. It remains
experimental: automatic scheduling integration, pinned-Linux latency, and a
lower roughly 1.9x rebuild peak are still release gates. The operation, RAM,
rejected-experiment, and live-compaction results are in
[`docs/MUTABLE_SEGMENT_CONCURRENCY.md`](docs/MUTABLE_SEGMENT_CONCURRENCY.md).

`PackedCache<V>` and the newer `DirectPackedCache<V>` build experimental
arbitrary-value caches on that adaptive index. They add epoch-protected values, caller-accounted weight,
optional entry limits, 100 ms-resolution per-item TTL, compact sampled CLOCK
eviction, statistics, and background maintenance. A reusable pinned guard is
the fast read API; an owned thread-local guard keeps values stable after
replacement or removal. The arena activates allocation partitions as the
population grows, so callers do not need to predict a maximum item count.
The direct variant stores the protected entry pointer in the index and packs
weight, expiry, and CLOCK state into one atomic word, removing the separate
generational slot directory. Reads, replace-only updates, and removals are
lock-free. General upsert and multi-step TTL coordination retain compact
striped serialization.

```rust
use packedgen::{CacheConfig, CacheWriteOutcome, DirectPackedCache};

let cache = DirectPackedCache::try_new(
    CacheConfig::new(1_024).with_max_entries(16),
)
.unwrap();
assert_eq!(
    cache
        .insert_discard_with_options(b"key", b"value".to_vec(), 5, None)
        .unwrap(),
    CacheWriteOutcome::Inserted,
);
assert_eq!(cache.get(b"key").unwrap().as_slice(), b"value");
assert!(cache.remove_discard(b"key"));
```

For repeated get-or-insert or dedup-style admission, a pinned direct guard can
create a caller-owned `adaptive_admission()` session. It learns an
existing-key phase after two consecutive occupied results and then uses an
exact lock-free precheck; the first genuinely absent key switches it back to
the ordinary atomic insertion path. Its lazy `insert_if_absent_with_options_by`
form skips value construction on certified existing keys. The session owns the
tiny saturating counter, so the cache, entries, and ordinary read guard gain no
memory or steady-path branch.

For pipelines that already use `admission_batch()`, the batch reserves eight
arena slots under one partition lock and consumes them on demand. Partial and
duplicate-heavy batches return every unused or unpublished slot exactly; the
reservation is caller-stack state and adds no cache or per-entry bytes. This
is intentionally isolated from ordinary insertion, reads, updates, and
removals. On the portable hash backend, a batch whose generation still
has an empty frozen base also computes only the mutable routing hash lane;
there is no frozen index to justify the independent second digest. Frozen-base
batches and the hardware-AES shared-hash backend retain their existing route.
The current one-million-entry mixed-key/64-byte-value batch row is 105.476
requested B/entry versus 133.693 for the inline Papaya control.

Negative-lookup-heavy deployments can additionally enable
`miss-optimized-frozen`. It statically replaces each frozen generation's
16-byte interleaved key/value slot with three dense streams: one byte of
fingerprint, a five-byte exact arena locator, and an eight-byte atomic value
handle. Requested slot storage is therefore exactly 14 B/entry. The eight-bit
fingerprint rejects roughly 255/256 absent candidates before loading either
the locator or key bytes; the remaining candidates are still verified exactly.
Generations containing keys longer than 255 bytes automatically use an exact
six-byte route/offset fallback. The representation adds two allocations per
generation relative to the default, never per entry.

Against the preceding four-byte-route/two-byte-offset miss layout, reversed
1M-entry/4T tests improved negative deletes 6.6-6.7% raw and the fair 90%-miss
mix 6.4-6.8%; pure-miss raw medians gained 0.9-3.4%. Successful reads remained
order-neutral and the complete 93.268%-hit prepared pipeline changed only
-0.4%/+1.4% after Papaya normalization. Against the normal interleaved cache,
80% misses remains order-sensitive, while 85% misses gained 3.2%/7.7%
normalized median and 16-19% normalized p05. Keep the default for ordinary
mixed caches; use this mode around/above 85% misses for performance, or
explicitly for its 2-byte frozen-slot saving, and confirm it on the deployment
workload. The rejected layouts and full reversed-order evidence are recorded
in `docs/PACKED_CACHE.md`.

Frozen generation construction uses checked 32-bit permutation slots. This
halves the temporary destination buffer from 8 to 4 B/input entry without
changing retained map storage or lookup code. In reversed one-million-entry
runs, construction improved 5.2-9.7% across PtrHash and PHast; rebuild improved
1.1-4.9% for interleaved storage. A longer 17-sample miss-layout confirmation
put PHast rebuild at -0.3%/+1.2% by order, effectively neutral. The dedicated
`frozen_build_peak_probe` and `frozen_rebuild_peak_probe` separate one-shot
process peak RSS from retained base bytes when evaluating future builder and
rebuild changes.

For an unbounded restore or bulk-fill phase, `bulk_admission_batch()` also
coalesces logical-length publication until the scope is dropped. Inserted keys
remain immediately readable and duplicate prevention stays exact; only
`len()` is temporarily behind. Dropping the scope publishes the exact total.
This removes a contended per-key length update without changing ordinary
`admission_batch()` semantics or adding cache/per-entry memory. Use the
ordinary batch when live exact length must be observed inside the fill scope.
When both capacity limits are disabled, the bulk scope also bypasses
capacity-only hashing, eviction-victim retention, and eviction-window counters;
those operations cannot affect an unbounded cache. Bounded bulk scopes keep
their ordinary capacity behavior.

With `--features prepared-keys`, `DirectPackedCache` can prepare a caller-owned
handle for a small known hot set and use exact prepared reads, replace-only or
upsert mutation, discard removal, and touch. Wrong, stale, cross-cache,
overlay, and post-rebuild
handles remain exact by falling back to the ordinary adaptive route. In the
90%-of-reads-on-1%-of-keys fixture, scalar/eight-thread hot reads improved
26.9%/24.4%, replacement 20.9%/5.8%, and touch 23.7%/27.0%. Prepared mutations
added another 3.5% at one thread and 2.6% in the longer eight-thread pairing
to the already-prepared tracked mixed trace. The 1%
handle set adds 0.160 B/total entry. Preparing every key would spend 16
B/entry; that still remains denser than the measured inline Papaya control,
but a small adaptive hot set preserves much more of the RAM advantage.
Persistent touch through a reusable guard now shares the guard's validated
generation reader instead of reacquiring ArcSwap publication state for every
operation. Reversed comparisons improved ordinary touch by 17.5-27.8% at one
thread and 13.9-22.2% at four threads. Extending the same route to prepared
hot keys improved that workload by 15.4-28.4% and 12.4-55.8%, respectively,
bringing the scalar prepared path within about 2% of inline Papaya. TTL-changing
touches retain the serialized authoritative path.

Replacement-heavy request pipelines can use `guard.replacement_batch()` (or
the statistics-free `replacement_batch_untracked()`) to publish replaced old
values to the epoch collector 64 at a time. Index values, entry count, weight,
capacity enforcement, and new-value visibility remain immediate; only the
aggregate replacement statistic waits for the 512-operation refresh or scope
drop. The batch accepts both ordinary and prepared hot keys, so a mixed hot-set
pipeline does not need separate scopes. At 1M entries, the named 90%-on-1%-hot
prepared trace measured Direct/Papaya at 5.895/6.071, 16.391/11.195, and
29.391/13.481 Mops/s at 1/4/8 threads: scalar was near parity at -2.9%, while
Direct led by 46.4% and 118.0% at 4/8 threads.
Two reversed preserved-binary comparisons improved Direct by 30.9-48.2% at
four threads and 11.4-17.1% at eight threads; scalar changed -1.5%/+10.1% with
host order. The scope owns a 512-byte pointer buffer and adds no cache or
per-entry RAM. A 256-handle follow-up raised multicore medians but was rejected
because its reverse p05 fell 14.5% at four threads and 22.4% at eight threads.
Without prepared handles, reversed Direct batch/ordinary pairs were 2.5-5.1%
slower at one thread, 19.2-20.5% faster at four threads, and 2.2-5.3% faster at
eight threads on the loaded host. Scalar ordinary callers should keep the
normal method; the scope targets pipelines with multiple writers or prepared
hot keys.

When the caller already groups prepared hot-key replacements, the separate
`guard.prepared_replacement_batch()` pipeline can replace up to 64 keys through
one generation snapshot. It is update-only: absent keys remain absent, every
successful item returns and retires its exact displaced handle, and stale,
wrong, cross-cache, or overlay handles fall back to the ordinary exact route.
All charges validate before any index publication. This is a separate scope so
ordinary replacement batches retain their original layout and allocation.
On the final finite-weight 100K-entry fixture, true batch-64 versus the scalar
prepared replacement scope measured 15.195/8.796 Mops/s at one thread and
30.184/18.771 at four threads, gains of 72.8% and 60.8%. At eight threads it
won both launch orders by 4.0-16.5%; p05 changed -5.7%/+7.2%, so saturated use
still needs workload-specific tail testing. In the colocated batch fixture,
Direct led inline Papaya by 90.2%, 162.6%, and 125.2% at 1/4/8 threads. Higher
Mops/s is better. Capacity accounting completes for the group before one final
limit enforcement, preventing an early eviction from invalidating a later
item's accounting. The prepared pipeline allocates at most 1.5 KiB of reusable
value/result scratch after first use in addition to the existing 512-byte
retirement buffer; neither adds per-entry bytes. The later recycler described
below adds one empty `OnceLock` word to the arena owner and allocates its bounded
spare pool only after this opt-in pipeline is used.
With the existing `prepared-batch-gate` feature, the same cache pipeline uses
one of 16 cache-line-separated generation gates for the whole group. After
normalizing each binary to its colocated Papaya control, this added 8.1% at two
writers and 29.0% at four; p05 ratios improved 9.3% and 6.8%. It was not a
clear win at one or eight writers. The feature costs 1,040 requested bytes per
live generation, not per entry, so it remains an explicit 2–4-writer tuning
choice rather than the density-first default. In the complete 95/2/2/0.5/0.5
pipeline mix at four writers, two opposite launch orders improved the
Papaya-normalized ratio by 1.6% and 2.9%. That smaller whole-cache gain is
consistent with replacements comprising only 2% of requests.

The prepared replacement scope is also a complete mixed-operation pipeline:
it forwards tracked ordinary/prepared reads, conditional insertion, scalar
ordinary/prepared replacement, and ordinary/prepared deletion while retaining
its batch scratch and reclamation window. A deterministic Redis-like fixture
uses 95% hit reads, 2% read misses, 2% replacements, 0.5% insertions, and 0.5%
deletions in the same ordered trace. At one million starting entries, scalar
and true-batch Direct/Papaya ratios moved from 1.079x to 1.116x at one writer
and from 1.121x to 1.153x at four writers: 3.5% and 2.9% improvements after
normalizing to the colocated control. The final batch path led Papaya by 11.6%
and 15.3%. Eight-writer host variance was too high to rank batching, although
Direct batch led Papaya by 14.1-18.6% in both runs. Both implementations had
the same final population and read-hit rate. This is the stronger production
signal: the large replacement-only gain becomes a useful but necessarily
smaller whole-cache gain when writes are only 3% of requests.

Batch-size sweeps are reproducible with
`PACKED_CACHE_REPLACEMENT_BATCH_SIZE=1..64`. In the replacement-only fixture,
one-thread medians for 8/16/32/64 were 13.885/14.259/14.771/14.850 Mops/s;
32 had the better p05 than 64. Two opposite-order four-thread sweeps put 32 at
28.024/29.924 and 64 at 25.277/30.892 Mops/s; 32 won both p05 comparisons.
In the complete read-heavy mix, normalized 16-vs-64 comparisons changed
direction after launch order reversal and were within about 1-2%. Therefore
the library does not guess an automatic size: use 32 for a tail-conscious
replacement-heavy queue, or flush the application's natural burst up to 64
when total pipeline latency determines the boundary.

The true replacement pipeline now reserves recyclable boxed value storage once
for both prepared and ordinary cold-key batches. The pool is created only when
this opt-in API is used, is sharded by writer, and keeps at most 1,024 spare
allocations per active shard. Entries gain no fields; a `prepared-keys` build's
inactive cache carries only one empty `OnceLock` word in its arena owner and
allocates no pool. Builds without that feature carry no recycler state.
In two opposite-order four-writer runs of 300 million replacements, maximum RSS
fell from 341/349 MB to 200/212 MB (39-41%), retired instructions fell 38%,
median throughput improved 23-38%, and p05 improved 46-50%. Adding the ordinary
cold-key batch on top of prepared-only recycling improved replacement medians
36-38% at one writer and 14-17% at four writers. In the complete read-heavy
mix, scalar Papaya-normalized comparisons changed from -0.9% to +3.1% with
launch order, while the final four-writer Direct/Papaya median was 53.766/44.045
Mops/s. The result is therefore a strong churn/RAM win and a neutral-to-positive
whole-cache win, not a claim that the small mixed-workload delta is stable on
this host. Miri, AddressSanitizer, ThreadSanitizer, exact-drop tests, and a
four-writer/four-reader recycling stress test pass.

Feature-gated diagnostics now separate live reclamation debt from allocator
page retention. Completed 100-million-replacement runs retained only about
1,000-1,500 retired values and 300-3,600 recyclable boxes; explicit maintenance
drains the retired queues to zero in the regression suite. Use a single sample
for process-RSS work so the result describes one cache lifecycle:

```text
PACKED_CACHE_PREPARE_ALL=1 \
PACKED_CACHE_RECLAMATION_STATS=1 \
PACKED_CACHE_ALLOCATION_STATS=1 \
cargo run --release \
  --features prepared-keys,cache-diagnostics,allocation-diagnostics \
  --example cache_probe -- \
  100000 100000000 4 1 \
  replace_hit_prepared_hot_bulk_64_90pct_on_1pct direct-packed-cache
```

One sample is useful for RSS and exact counters, not for a stable throughput or
p05 claim. Allocation instrumentation also changes allocator timing, so compare
uninstrumented preserved binaries for performance.

For caches configured with a finite byte budget, equal-charge replacements no
longer execute a shared zero-delta capacity update. In the prepared-hot batch
fixture with a 256-byte budget per maximum entry, the retained ordering
improved Direct by 9.2%, 12.0%, and 9.0% at 1/4/8 threads, while p05 improved
6.8%, 17.8%, and 3.5%. The entry-count-only and scalar 95%-read controls stayed
neutral. The final weighted Direct/Papaya comparison was 5.001/4.972,
16.481/9.816, and 25.792/12.655 Mops/s at 1/4/8 threads, so Direct was at
parity at one thread and led by 67.9%/103.8% at four/eight. This adds no state
or RAM; it only recognizes that replacing an item with the same charge cannot
move any capacity counter. The benchmark fixture is selected with
`PACKED_CACHE_WEIGHT_LIMIT_PER_ENTRY=256`.

Successful `remove_discard` no longer enters a second arena reader epoch merely
to inspect the handle returned by the exact atomic index removal. That handle
has one retirement owner; older readers remain protected by the existing epoch
collector. Repeated 1M-entry frozen-delete comparisons improved the ordinary
path at every measured thread count. Delete-heavy pipelines can use
`guard.removal_batch()` to retain one generation writer, perform one-swap exact
deletion, publish retired handles 64 at a time, and coalesce index-length
decrements. Key absence, capacity, weight, and bounded-cache `len()` remain
immediate. An unbounded cache's index-derived `len()` publishes at each
512-delete refresh and scope drop, as do removal statistics.
`removal_batch_untracked()` omits those statistics when the surrounding
service already counts requests. With `prepared-keys`, an already-prepared hot
set can also use `batch.remove_discard_prepared(key, handle)`. The exact frozen
slot is removed without rehashing or reacquiring a writer; stale and wrong-key
handles fall back to the ordinary exact batch route. At 1M entries this path
measured 17.876/43.120/53.639 Mops/s at 1/4/8 threads versus inline Papaya at
15.060/20.808/23.018, so higher-is-better throughput was 18.7%, 107.2%, and
133.0% ahead. Preparing one-use handles inside the measured delete instead
produced 6.969/24.133/36.973 Mops/s: preparation must be amortized across a
reused hot set, not added to a one-shot scan. Handles remain 16 bytes: retaining
them for 1% of keys costs 0.160 B per total entry and the API adds no resident
cache state. In the initial 17-sample preserved-binary
comparison, frozen Direct improved from 9.246/21.134/32.850 to
10.057/27.287/44.688 Mops/s at 1/4/8 threads; colocated Papaya measured
14.323/20.767/22.813. Mutable-overlay Direct improved from
13.236/24.888/36.252 to 14.319/27.726/47.585, versus Papaya at
14.182/21.180/23.178. Reclamation now caches the last adjacent arena-block
lookup; profiling reduced epoch-drain samples from 31/162 to 2/134. The batch
uses one 512-byte handle buffer per active scope and adds no cache or per-entry
RAM. A follow-up preserved-binary A/B that coalesced length decrements was
scalar-neutral (+0.1%) and improved frozen 4-thread delete by 14.2-20.0% and
8-thread delete by 8.5-19.0%. Its same-process frozen Direct/Papaya results were
10.121/15.012, 31.584/20.516, and 50.182/22.864 Mops/s at 1/4/8 threads.
Mutable-generation Direct/Papaya measured 14.200/14.902, 35.385/21.026, and
52.473/22.802. A later mutable-only one-lane hash experiment improved batch
delete but was rejected after a 0.5-0.9% Papaya-normalized scalar read-mix
regression. Two longer reversed 95%-read scalar comparisons for the retained
scope improved 1.6% and 2.6% after Papaya normalization, so ordinary
read-dominant traffic does not pay for the explicit delete path.

At one million mixed binary keys with 64-byte values, DirectPackedCache's
insert/replace path used **105.110 requested B/entry and 2,008,244
allocations** (PackedCache used 118.053). Conditional admission and bulk load
use the current native dense arena at 105.540 B/entry and 1,013,488
allocations. The
original `Arc`-wrapped Papaya control used 157.693 B/entry, but a
new stronger Papaya control stores the same value and compact CLOCK/TTL/weight
metadata directly in each map node and uses **133.693 B/entry with 2,000,006
allocations**. Against that inline control, DirectPackedCache uses 21.4% less
total requested RAM and 41.0% less non-payload overhead after subtracting the
unavoidable 64-byte value. This remains a useful density lead but does not pass
the candidate 25% total-memory release gate. The direct entry now fixes its
metadata beside the value prefix without changing the 72-byte entry size;
three simultaneous one-thread read pairs improved by 19.8%, 14.8%, and 5.5%.
Sampled CLOCK hits also avoid an exclusive write when already marked, improving
uniform eight-thread pressure by 13.1% strict and 18.1% async while preserving
the measured hit rate. A later portable-default
optimization carries two randomized hash lanes into the frozen index, removes
integer division from compact bucket routing, and skips a provably empty
writable overlay. Its local frozen-miss row reached 394.701 versus 346.984
Mops/s for inline Papaya, and three 95%-read processes led by 31.9–45.1%.
New-key insertion still trailed 30.160 to 48.268 Mops/s. Later automatic
worker fast paths raised non-expiring touch from 41.443 to 57.981–59.411
Mops/s and put guarded delete hit within 2.2% of inline Papaya while delete
miss led by 4.7% in the first usable run. Papaya still leads touch, and the
saturated host makes isolated confirmation mandatory. Across three earlier
local concurrent read/write processes,
DirectPackedCache
also had lower p95/p99/p99.9 read latency while performing one adaptive
rebuild. Host variance is high, so pinned Linux confirmation remains required.

The adaptive startup fallback filter is bounded by the learned sample working
set. After the native reclamation and dense-arena redesign, the complete
conditional-admission path is **105.540 B/entry** with 1,013,488 allocations.
The latest bounded-guard
admission path batches exact index-length deltas in
one allocation-free scalar. Two independent eight-thread triple-pair sweeps
improved new-key admission by 19.5–26.1% in absolute medians and 17.1–26.8%
after normalization to each adjacent Papaya control. The final local median
was 37.499 Mops/s versus 31.390 before batching; inline Papaya still led near
50.5 Mops/s. Exact current fresh memory is 105.540 B/entry and 1,013,488
allocations.
Atomic bucket probes also carry the scanned control word into publication and
derive the sole in-progress writer from the append frontier instead of running
a second whole-word byte match. In the latest same-process eight-thread matrix,
Direct/Papaya measured 37.381/49.707 Mops/s for new keys, 67.797/56.304 for
read hits, 15.039/9.982 for replacement, 408.754/363.009 for read misses, and
54.186/52.199 for the 95%-read mix. These are loaded-host optimization results,
not release claims.
Ordinary guarded admission now also recognizes a stable empty frozen base in
the out-of-line generation writer and computes only the mutable routing hash.
Preserved-binary comparisons improved one-million-new-key insertion by
15.4-21.1% at one thread and 8.8-9.1% at eight threads, with no repeatable
Papaya-normalized 95%-read regression. Outlining the rare filter-positive
learning-sample probe then added 3.3-5.9% at one thread and 0.7-1.9% at eight,
while its read-heavy and batch controls remained positive or neutral by launch
order. The final focused Direct/Papaya medians were 11.972/14.319 and
53.986/67.108 Mops/s at one/eight threads, leaving 16.4%/19.6% gaps. Neither
change adds cache or per-entry memory.
In a separate tuned-jemalloc 30-turnover run at 100K entries, live allocation
plateaued at 113.085 B/entry versus inline Papaya's 138.869 and median RSS at
25.30 versus 27.03 MB. Including explicit rebuild maintenance made the complete
turnover cycle 3.3% slower than Papaya, so density does not come for free.
The 2026-08-09 blocker pass replaced the direct cache's `seize` reclamation
path with native three-epoch QSBR and reusable 256-slot blocks. A newer
200K-entry, 16-writer, ten-turnover run held a 110.925 B/entry median versus
inline Papaya at 138.758. All five focused Miri tests, all 27 direct-cache ASan
tests, and the exact concurrent replacement/read TSan test pass. Sustained
full-cache admission now completes one-million and five-million new-key traces
at 0.944 and 0.918 Mops/s instead of hitting the former nonlinear stall.
At-capacity direct-cache insertion now uses Redis-style bounded physical
sampling from both adaptive atomic tables and the dense frozen base instead of
normally scanning the logical map. Batched startup-fallback coverage prevents
an immortal key pocket, and exact locked handle revalidation remains mandatory
before removal. The 10K-entry/20K-admission probe reaches 3.650 median Mops/s
with 1,480 original cold survivors and no per-entry memory growth.
For pipelined cache fills, `DirectCacheGuard::admission_batch()` keeps
accounting visible but coalesces capacity enforcement. Its adaptive internal
window is derived from proactive entry headroom and weight capacity, so long
pipelines self-flush instead of accumulating unbounded debt. The final paired
full-cache probe improved by 11.5% and 4.5% at 10K and 100K entries with one
writer, and by 18.1% and 13.8% with eight writers. Dropping the scope restores
any remaining debt. The current one-million-entry memory row is **105.540
B/entry and 1,013,488 allocations**. A Papaya control given the exact victim
remains substantially faster, so victim selection and eviction
coordination—not basic admission—are the next full-cache target.

The eviction probe now accepts an explicit key length, which exposed a severe
generic-fallback cliff above the adaptive 48-byte class. The first retained
population-weighted Papaya refill recovered that path to 2.870/1.791 Mops/s at
one/eight writers. Its successor is an append-only arbitrary-length table with
a safe public API: four bucket choices, eight slots per bucket, write-once
stable cells, sharded length publication, and direct rotated bucket sampling. Papaya remains
only for the bounded learning sample and emergency spill, and stochastic
population weighting prevents that smaller population from becoming immortal.

In the final 15-sample preserved-binary gate, 64-byte batch admission measured
**4.415 versus 3.093 Mops/s** at 10K/one writer and **2.782 versus 2.179** at
10K/eight writers. At 100K it measured **3.196 versus 2.100** and **2.676
versus 1.953**. Normalized to each adjacent exact-victim Papaya control, the
improvements were 43.9%, 27.8%, 45.3%, and 52.1%, respectively. Original-key
survivor counts were equal or slightly lower. The Papaya control is still an
upper bound: it is handed the exact victim and omits discovery, CLOCK, TTL,
accounting, and capacity policy.

Long admission batches now advance reclamation every 512 successful new keys.
In the 10K-entry pressure probe, the earlier reclamation pass reduced requested
memory to 441.571 B/entry for 64-byte keys and 180.091 for 16-byte keys. The
native fallback lowers the final 64-byte pressure state again from 441.276 to
**417.380 B/entry** (-5.4%); maintenance reaches 188.463. The 16-byte path is
effectively unchanged because it allocates no native fallback. At 100K,
64-byte pressure moves from 410.407 to **397.927 B/entry** (-3.0%) and the
maintained states are equal. Fresh long-key RAM crosses Papaya allocation
cliffs in this historical two-allocation checkpoint. The reported allocation
column counts allocation operations during the trace; it is not a count of
allocations still live.
Packed victim chunks leave requested live RAM neutral within 0.2 B/entry in
repeated pressure probes, while cumulative allocation calls fall about 22% for
64-byte keys and 59% for 16-byte keys.

The latest fallback stores its stable cell, key length, and trailing key bytes
in one allocation behind the same eight-byte append-only slot. The small
audited allocation module is the only unsafe part of this fallback;
publication, lookup, and ownership remain safe to callers. Focused Miri tests
cover variable lengths, over-aligned values, drop ownership, and concurrent
same-key publication. Versus the preceding native
layout, 100K/key64 seeded memory fell from 193.383 to **185.710 B/entry** and
pressure memory from 370.667 to **347.124**; pressure-trace allocations fell
from 710,557 to **416,519**. At 10K the corresponding rows are 202.976 to
**195.351** and 390.354 to **366.459**. Maintained memory and the key16 control
remain effectively equal. Alternating operation gates improved 1T long-key
insert/read/update/95%-read mix by about 15%/27%/17%/18%; 8T insert/read/mix
improved about 3%/10%/29%, while the noisy update median improved about 2%.
A one-full-turnover cache gate also improved the prior native/Papaya ratio at
all four shapes: roughly +9%/+8% at 10K 1T/8T and +1%/+5% at 100K.

Wide inline 33–64-byte adaptive tiers were tested and reverted. A lazy tier
saved RAM and accelerated admission but reduced the 95%-read overlay mix by
about 30% versus the existing generic fallback. A full tier recovered and
slightly improved the read mix, but increased fresh RAM by about 41% in the
over-reserved fixture. Neither passed the combined RAM/read/write gate.

A new hard-capacity 95%-read comparison continuously admits unique keys. At
200K entries and eight threads, the 1M-operation median is 50.968 Mops/s for
DirectPackedCache versus 62.353 for a strong Papaya uniform-replacement
control; hit rates are 97.689% and 97.562%. Across 10M operations Direct reaches
34.193 versus 52.969 Mops/s, but retains a higher 80.142% hit rate versus
78.685%. On the 80/20 hot-set version, Direct's hit-rate advantage grows to
86.296% versus 78.697%, although multicore throughput still trails 38.385 to
51.065 Mops/s. These are noisy local development results, not release claims.
DirectPackedCache now also has opt-in asynchronous eviction with a caller-set
hard-pressure threshold. With 1% slack, writers coalesce wakeups to one
maintenance worker and help synchronously only after crossing the hard
threshold; the default remains strict synchronous capacity. In repeated local
10M-operation traces, async improved the uniform-pressure median from 33.873
to 39.163 Mops/s and the 80/20 hot-set median from 40.511 to 44.867. Papaya's
strong uniform-replacement control still led at 42.168 and 50.376 Mops/s,
while Direct retained higher read hit rates. Final Direct residency was below
the 200K soft target in both tests. A shorter final verified-source run put
async at 47.304 Mops/s versus 40.510 strict and 44.165 Papaya, demonstrating
that async can lead in short pressure bursts but not yet in sustained churn.

DirectPackedCache also has an opt-in, scan-resistant admission doorkeeper. It
uses compact rotating membership generations and rejects a new key once after
the cache reaches its expected population; a repeated miss is admitted.
Rejection happens before allocating the candidate value, while the default
configuration and resident read path are unchanged. A typical bounded-cache
configuration is:

```rust
let config = CacheConfig::new(max_weight)
    .with_max_entries(expected_entries)
    .with_admission_doorkeeper(expected_entries)
    .with_adaptive_frequency_admission(2, 5_000)
    .with_eviction_batch(256)
    .with_async_eviction(11_000);

let cache = Arc::new(DirectPackedCache::new(config));
let maintenance = cache.spawn_maintenance(Duration::from_millis(1));
```

This is the opt-in R-0005 tuning candidate, not the library default. Plain
`CacheConfig::new` continues to use strict synchronous capacity enforcement,
CLOCK admission, and no doorkeeper or frequency sketch. Do not promote the
tuned profile to the default until R-0005 validates the immutable candidate.

The async setting permits temporary growth to 110% of the soft limits and
therefore requires keeping the returned maintenance worker alive. It is an
explicit throughput/strict-capacity tradeoff, not a universal default. The
doorkeeper adds four to eight bytes per expected entry and can be used with
strict synchronous eviction as well. Adaptive frequency admission adds another
four to eight bytes per expected entry. It stays on second-sighting admission
below the configured reuse threshold; after pressure begins, one out of every
16 resident hits updates the frequency sketch. Adaptive admission uses a
bounded rolling reuse estimate, so an early cold fill cannot permanently delay
policy activation. Exact QuickCache comparisons, including the workloads where
PackedGen still loses, are in
[`docs/QUICKCACHE_GAP_EXPERIMENT_2026-08-30.md`](docs/QUICKCACHE_GAP_EXPERIMENT_2026-08-30.md).
The same-workload rolling-reuse follow-up is in
[`docs/ROLLING_REUSE_EXPERIMENT_2026-09-01.md`](docs/ROLLING_REUSE_EXPERIMENT_2026-09-01.md).

Production telemetry is also opt-in. `cache-production-diagnostics` exposes
capacity peaks and debt, retirement, eviction/admission policy, worker, and
arena statistics through `DirectPackedCache::production_diagnostics`.
`cache-pressure-timing` adds foreground/background nanosecond timing and a
bounded p99 histogram. The default build carries neither the counters nor
their hot-path atomic cost. Candidate scope, exact checks, and remaining R-0005
gates are recorded in
[`docs/R0005_CANDIDATE_2026-09-01.md`](docs/R0005_CANDIDATE_2026-09-01.md).

The cache now has a direct bulk-load constructor for snapshot restore and a
Redis-style equal-live-memory harness. On the local 64-byte-value, one-hour-TTL
trace, PackedGen retained about 1.250M entries versus Redis 8.8's 804K at
roughly 134.3 MB live memory. The honest 10M-operation result, including async
capacity settlement and final compaction, was 8.812 Mops/s median with a
77.003% read hit rate versus Redis at 1.773 Mops/s and 75.365%. PackedGen is
embedded while Redis used pipeline-32 TCP, so this is a deployment-shape
comparison rather than a hash-only claim. Sustained new-key admission is the
narrowest lead: 0.587 versus 0.536 Mops/s after all settlement work.
Configuration, operation rows, RSS behavior, caveats, and reproduction are in
[`docs/REDIS_STYLE_CACHE.md`](docs/REDIS_STYLE_CACHE.md).

Architecture, operation results, and every retained/rejected cache experiment are in
[`docs/PACKED_CACHE.md`](docs/PACKED_CACHE.md).
The independent correctness, benchmark, endurance, and release-gate protocol is
in [`docs/CACHE_PROOF.md`](docs/CACHE_PROOF.md); `scripts/cache_proof.sh` emits
machine-readable comparison results and retains competitor wins. It now also
provides exhaustive Loom models for the direct cache's publication and
three-epoch reclamation protocol, a time-bounded 24-hour churn mode with
automatic retained-memory and trend failure limits, and a dedicated Rust 1.88
compatibility gate mirrored in GitHub Actions.

The current direct DashMap comparison is summarized in
[`docs/ATOMIC_VS_DASHMAP.md`](docs/ATOMIC_VS_DASHMAP.md). In the local warm
probe, the atomic generation uses 42.2% less frozen RAM and wins eight-thread
read-heavy cache mixes, while DashMap wins broad single-thread latency and
one-hot-key contention. The newer conditional new-key probe puts embedded-
fingerprint PackedGen near DashMap insert at one thread and in the same range
at eight threads; DashMap still leads the single-thread entry API.

The broader current Rust concurrent-map comparison is in
[`docs/CONCURRENT_MAP_COMPARISON.md`](docs/CONCURRENT_MAP_COMPARISON.md). It
adds Papaya, `scc`, Flurry, and `RwLock<HashBrown>`, measures 1/2/4/8/12/16
worker scaling, and reports one-million-entry RAM. PackedGen is the density
winner and the prepared batch path leads several measured multicore read and
existing-key write workloads. Competing maps still win important scalar,
new-key, and contended-key cases.

`PackedBinaryMap` is the first database-oriented layout. It stores eight-byte
references in the table and immutable key bytes in a segmented arena. Its raw
lookup path hashes caller bytes once. Deletes cannot make the core re-hash a
reference as if it were the original key: tombstone cleanup is deferred and a
bounded, byte-aware routing rebuild preserves survivors.

Routing memory is explicit through `RouteCacheBudget`: the default adaptive
policy keeps one packed `u32` route slot per configured entry using four-way,
two-choice buckets; `Compact` disables direct routes, and `ReadOptimized`
reserves two physical route slots per entry. Capacity-aware location encoding
and a four-node relocation bound currently retain about 96.74% of adaptive
routes without creating a near-capacity insertion cliff.
Delete maintenance is synchronous by default. Single-writer services can select
`MaintenanceMode::Deferred`, observe `maintenance_due()`, and call `maintain()`
at a controlled boundary so table rebuild and arena compaction do not land on
the request that crosses the delete threshold. `PackedMapStats` exposes
maintenance runs, compaction-staging failures, and reclaimed arena capacity.
`try_begin_maintenance` plus `prepare_maintenance_step` can copy compacted key
bytes in bounded owner-selected slices before `finish_maintenance` performs the
remaining table cutover.
Structural mutation marks a staged plan stale; the writer can detect this and
restart allocation-safely. Replacing only a value keeps the plan valid because
packed key references do not move.
Every successful maintenance cutover advances an observable `PackedGeneration`;
exhaustive short writer-trace tests verify that a stale staged plan cannot
resurrect a removed key or discard an inserted key.
Deferred mode emits its soft maintenance signal at 25% deleted entries and
forces maintenance at 50% by default, bounding ignored tombstones and dead
arena bytes. `with_maintenance_threshold_percents` can select a stricter policy;
construction validates it and precomputes the exact entry counts exposed in
`PackedMapStats`.

The packed generation prototype now implements the core of the intended
production architecture:

1. one table per application shard;
2. a single ordered writer per shard;
3. lock-free readers protected by generation/epoch reclamation;
4. immutable entry records, atomically replaced on update;
5. background rebuild into a new table generation;
6. atomic generation cutover while the active writer overlay remains published;
7. explicit metrics for probes, rebuilds, tombstones, and bytes.

## Example

```rust
use packedgen::{ElasticConfig, FixedElasticMap, InsertOutcome};

let config = ElasticConfig::new(1_000_000)
    .with_reserve_exponent(6) // delta = 1/64, target occupancy ~98.4%
    .unwrap();
let mut map = FixedElasticMap::new(config);

assert_eq!(map.try_insert(b"key".to_vec(), 42), Ok(InsertOutcome::Inserted));
assert_eq!(map.get(b"key".as_slice()), Some(&42));
```

Services should prefer `PackedBinaryMap::try_new(config)`, which reports core
geometry, capacity, and every eager auxiliary allocation as `PackedBuildError`
instead of panicking.
`PackedBinaryMap::try_from_entries` atomically loads an iterator: duplicate keys
replace earlier values, while failure reports the input index and never exposes
the partial map.

The concurrent binary-key API uses owned lookup results by default:

```rust
use packedgen::ConcurrentSwissMap;

let map = ConcurrentSwissMap::with_capacity(1_000_000);
map.try_insert(b"key", 41_u64).unwrap();
map.update(b"key", |value| *value += 1).unwrap();
assert_eq!(map.get_cloned(b"key"), Some(42));
assert!(!map.try_insert_new(b"key", 99).unwrap());
```

The allocation-free atomic generation uses a checked value domain:

```rust
use packedgen::{AtomicPackedGenMap, NonMaxU64};

let map = AtomicPackedGenMap::try_from_entries(
    [(b"counter".as_slice(), NonMaxU64::new(0).unwrap())],
    128,
)
.unwrap();
assert_eq!(
    map.update(b"counter", |value| NonMaxU64::new(value.get() + 1).unwrap()),
    Some(NonMaxU64::new(1).unwrap())
);
assert_eq!(map.get(b"counter").map(NonMaxU64::get), Some(1));
```

With `--features prepared-keys`, repeated hot-key access can prepare a handle:

```rust
use packedgen::{AtomicPackedGenMap, NonMaxU64};

let map = AtomicPackedGenMap::try_from_entries(
    [(b"hot".as_slice(), NonMaxU64::new(7).unwrap())],
    0,
)
.unwrap();
let hot = map.prepare_key(b"hot");
assert_eq!(map.get_prepared(b"hot", &hot).map(NonMaxU64::get), Some(7));

let keys = [b"hot".as_slice()];
let handles = [hot];
let mut values = [None];
map.get_prepared_batch(&keys, &handles, &mut values);
assert_eq!(values[0].map(NonMaxU64::get), Some(7));
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
The concise pass/fail matrix is in [`docs/STATUS.md`](docs/STATUS.md).
Every successful and failed layout experiment is summarized in
[`docs/EXPERIMENT_MATRIX.md`](docs/EXPERIMENT_MATRIX.md).
The concurrent ETS-like experiment and its current limits are recorded in
[`docs/EXPERIMENT_CONCURRENT_ETS.md`](docs/EXPERIMENT_CONCURRENT_ETS.md).
The lock-free reader design, RAM/operation matrix, exact rebuild protocol, and
high-churn failure are recorded in
[`docs/EXPERIMENT_LOCKFREE_GENERATIONS.md`](docs/EXPERIMENT_LOCKFREE_GENERATIONS.md).

At one million 32-byte binary keys, the default adaptive packed `1/64` layout
uses about 54.991 requested bytes per entry versus HashBrown's 84.429 (34.9%
less); the read-optimized two-slot policy is about 59.022 B/entry (30.1% less).
It also removes the million per-key allocations. At one million keys, fixed
batches of 32 reduce Elastic lookup from roughly 66 ns to 54 ns/key, versus
~28 ns/key for batched HashBrown.
PackedGen is therefore not yet "better than SwissTable" overall; the release
gates require the remaining latency work and a RAM-limited system win.

`FrozenPackedMap` explores a second, immutable backend built on PtrHash 2.0.1.
It retains exact semantics by checking the original packed key after perfect
indexing. At one million 32-byte keys it used 49.004 requested bytes per entry,
42.0% less than HashBrown. With the opt-in `gxhash` hardware-AES feature, its
measured successful lookup was ~14.5 ns/key versus HashBrown's ~27.1 ns/key on
the same million-key corpus. Frozen construction is slower (~1.78 ms versus
~0.48 ms for 16K entries), so this backend deliberately trades build and
mutation support for density and read speed. The portable XXH3-128 path remains
the default; enable `gxhash` only on supported AES-capable targets.

The same-requested-RAM probe exposes allocation cliffs that a single
bytes-per-entry point hides. With a 64 MiB requested-allocation budget, packed
Elastic held 1,032,192 records versus HashBrown's 917,504 (**12.5% more**), but
its five-sample median successful lookup was 149–159 ns/key across two processes
versus 35–37 ns/key. This fails the project's 25%-more-records density gate and
the lookup gate; it is evidence of a useful capacity direction, not an overall
win. The RAM probe also includes segmented SwissTable and immutable PtrHash
backends as comparison baselines; they explore different tradeoffs and do not
claim to implement the paper's elastic placement schedule.

At one million 32-byte binary keys, the 64-shard concurrent SegmentedSwiss map
used 54.309 requested B/entry versus DashMap with boxed keys at 84.438, a 35.7%
reduction with about 4,557 allocations instead of one million. In the current
eight-thread smoke run, pure read hits were near DashMap (83.57 versus 79.72
million operations/s), while DashMap still led the cache-like mixed workload
(77.30 versus 39.31 million operations/s). This is a real concurrent RAM win,
not yet a general concurrent speed win.

The atomic packed generation used 48.761 requested B/entry both before and
after updating 1% of one million existing keys, versus DashMap at 84.438. Warm
existing-key updates allocate zero bytes. One million newly inserted 32-byte
keys used 58.951 requested B/entry with inline keys versus 82.931 for boxed-key
Papaya. Configured exact 16/31/40/64-byte inline classes reduced requested RAM
by 20.6–35.4% versus their boxed controls and halved allocation counts. Its
latest broad eight-thread smoke reached
110.802 M read hits/s and 90.610 Mops/s on the 90/5/3/1/1 cache mix, versus
DashMap at 68.060 and 71.301 respectively. Distributed update is close but
noisy (41.877–64.544 Mops/s in recent runs). Bounded exponential CAS backoff
raised the latest eight-thread one-hot-key result to 6.012 Mops/s for the exact
policy and 8.532 for the one-byte policy, but DashMap still leads clearly at
58.755. In million-entry generic rebuild probes,
median striped handoff was about 4–28 microseconds from no writers through a
four-writer whole-table sweep, and base replacement was about 2–16 microseconds.
The full ~178–196 ms packed build ran while point operations continued. There
is no changed-key replay cliff; mutation throughput and pinned p99 latency remain
open release gates.

## Development

```text
cargo fmt --all -- --check
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo run --release --example memory_probe -- all 1000000
cargo run --release --example ram_budget_probe -- 64 1000000
cargo run --release --example fixed32_experiment -- 1000000
cargo run --release --features gxhash --example hybrid_probe -- 1000000 1 2000000
cargo run --release --features gxhash --example operation_matrix -- 1000000 1000000 10000
cargo run --release --example simd_key_probe -- 100000 2000000 31 all
cargo run --release --example concurrent_matrix -- 200000 500000 8 64
cargo run --release --example concurrent_map_scaling_probe -- 100000 3000000 16 7 64
cargo run --release --example memory_probe -- concurrent-swiss-binary 1000000
cargo run --release --features gxhash --example memory_probe -- lockfree-generation-churn-binary 1000000
cargo run --release --features gxhash --example generation_rebuild_probe -- 1000000 1 4 10000
cargo run --release --features gxhash --example atomic_overlay_probe -- 100000 300000 8 21 32 insert_miss
cargo bench --features gxhash --bench mixed_workloads -- successful_lookup_binary_32
cargo bench
# or run the complete local release audit
scripts/audit.sh
```

## Acknowledgements

- Martín Farach-Colton, Andrew Krapivin, and William Kuszmaul, authors of the
  elastic/funnel hashing paper.
- Aaron Ang and the `opthash-rs` contributors, whose Apache-2.0 implementation
  is retained as the owned paper-derived comparison backend.
