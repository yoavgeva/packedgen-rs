# Optimization experiment matrix

Date: 2026-07-16

This file records every implemented direction, including designs that lost.
Numbers are Apple M4 Max release-mode smoke evidence. They are useful for
choosing the next implementation, not as cross-machine claims. Results from
different harnesses are not mixed into a single speed ranking.

## Current one-million-entry RAM frontier

The common shape is a 32-byte binary key and `u64` value. Requested live bytes
exclude allocator bookkeeping and process RSS.

| Backend | Mutability | B/entry | Difference vs ordinary HashBrown | Decision |
|---|---|---:|---:|---|
| HashBrown with owned boxed keys | mutable | 84.429 | baseline | required external baseline |
| PackedSwiss | mutable | 67.652 | 19.9% less | fair packed-key Swiss baseline |
| Packed Elastic, read optimized | mutable, fixed epoch | 59.022 | 30.1% less | keep when all routes should be cached |
| Packed Elastic, adaptive | mutable, fixed epoch | 54.991 | 34.9% less | keep as paper-derived default |
| Cacheline two-choice bucket | mutable | 53.878 | 36.2% less | reject current lookup algorithm |
| SegmentedSwiss, balanced | mutable | 53.59 | 36.5% less | keep as mutable performance/RAM frontier |
| Concurrent SegmentedSwiss, 64 shards | concurrent mutable | 54.309 | 35.7% less | keep as concurrent RAM frontier; mixed speed open |
| Generic lock-free generation, 1% overlay | concurrent generational | 49.561 | 41.3% less | arbitrary cloneable values |
| Atomic lock-free generation, 1% existing-key churn | concurrent generational | **48.761** | **42.3% less** | direct frozen slots; zero overlay records |
| Hybrid frozen + 1% delta, no filter | generational | 49.033 | 41.9% less | keep for read-mostly generations |
| Hybrid frozen + 1% delta, 8-bit filter | generational | 50.043 | 40.7% less | default hybrid latency tradeoff |
| Frozen PtrHash | immutable | 48.557–49.004 | about 42% less | keep for immutable generations |
| Fixed32SoA, compact | mutable, fixed 32-byte key | **45.89** | **45.6% less** | keep as densest mutable specialization |

Ordinary HashBrown owns one allocation per boxed key in this comparison.
PackedSwiss, SegmentedSwiss, Elastic, Hybrid, Frozen, and SoA remove that cost,
so PackedSwiss is the fairer control for separating key-storage gains from
table-placement gains.

## Unified operation comparison

The operation probe uses one million deterministic `[u8; 32]` keys and `u64`
values. Maps are sized for 1.05 million records and initially loaded with one
million. Each read column is the median of five one-million-query random
samples. Each mutation column is the median of five disjoint 10,000-operation
batches. The table reports the median result from three fresh processes;
lower is better.

```text
cargo run --quiet --release --features gxhash --example operation_matrix -- \
  1000000 1000000 10000
```

| Backend | Read hit | Read miss | Update hit | Delete hit | Delete miss | Insert miss |
|---|---:|---:|---:|---:|---:|---:|
| HashBrown inline `[u8; 32]` | **50.34 ns** | **18.88 ns** | **25.70 ns** | **26.44 ns** | **6.51 ns** | **15.92 ns** |
| PackedSwiss | 66.15 ns | 20.87 ns | 32.72 ns | 33.31 ns | 9.68 ns | 31.38 ns |
| SegmentedSwiss balanced | 72.73 ns | 40.33 ns | 34.49 ns | 35.68 ns | 30.68 ns | 51.00 ns |
| Packed Elastic adaptive | 139.39 ns | 78.07 ns | 49.56 ns | 52.41 ns | 17.60 ns | 272.53 ns |
| Packed Elastic read optimized | 117.07 ns | 67.53 ns | 50.40 ns | 56.25 ns | 20.50 ns | 221.50 ns |
| Fixed32SoA balanced | 62.06 ns | 36.65 ns | 26.05 ns | 49.76 ns | 22.41 ns | 36.13 ns |
| Cacheline two-choice bucket | 259.31 ns | 249.19 ns | 53.70 ns | 115.85 ns | 197.38 ns | 423.13 ns |
| Hybrid, eight-bit filter | 116.57 ns | 40.83 ns | 70.24 ns | 39.52 ns | 70.87 ns | 34.82 ns |
| Frozen PtrHash | 61.37 ns | 57.54 ns | — | — | — | — |

Operation definitions:

- **Read hit:** `get` for a randomly selected existing key.
- **Read miss:** `get` for a key from a disjoint corpus.
- **Update hit:** normal map insertion for an existing key, replacing its value.
- **Delete hit:** removal of a known live key.
- **Delete miss:** attempted removal of a key that was never present.
- **Insert miss:** insertion of a genuinely new key into pre-sized capacity.

HashBrown stores the fixed key inline in this operation fixture, so its timed
insert does not allocate a box. Packed backends copy the new key into their
already-sized arena. Frozen has no mutation API. A Hybrid update of a base key
is logically an update hit but physically creates a delta overlay; its cost is
therefore intentionally included rather than hidden.

The results sharpen the RAM table's tradeoffs. Fixed32SoA is within about 23%
of HashBrown on random read hits and is effectively tied on update hits, but its
dense route repair makes successful deletion about 1.9 times slower. PackedSwiss
is the strongest general packed-key operation baseline. SegmentedSwiss buys
substantial RAM density with slower misses and new inserts. Hybrid inserts are
competitive because the filter usually proves the new key absent from the
frozen base, while base updates and missing deletes still check both
generations.

Packed Elastic remains update/delete-competitive relative to its read cost,
but new insertion is its largest deficit: roughly 14 times HashBrown for the
read-optimized policy and 17 times for adaptive. The adaptive route cache
originally searched up to 128 cuckoo buckets near saturation and measured about
11 microseconds for the final five percent of inserts. Capping relocation at
four nodes reduced that cliff to about 273 ns while retaining 96.74% of routes.

## Implemented ideas and outcomes

### 1. Exact paper-derived Elastic core

The owned `opthash` core implements the paper schedule and explicit reserve
fraction. With `u64 -> u64`, reserve `1/64` measured 19.900 B/entry versus
HashBrown's 35.652, but successful lookup remained roughly 54–71 ns versus
about 3–4 ns for HashBrown on the small hot fixture.

Decision: keep as the algorithmic core and correctness oracle. High occupancy
is a genuine RAM result, but exact multi-level probing alone is not a fast
general replacement for SwissTable.

### 2. Packed binary-key arena

Replacing owned boxed keys with eight-byte arena references removed one million
per-key allocations and made table memory observable by component. Reducing the
default segment from 1 MiB to 64 KiB removed the 100K-entry arena cliff while
adding negligible directory overhead at one million keys.

Decision: keep. This optimization benefits every backend and is independent of
Elastic placement.

### 3. Duplicate negative-filter removal

The service wrapper and Elastic core both retained an eight-bit-per-entry
stable membership filter. Exposing the core proof and deleting the wrapper copy
reduced adaptive Packed Elastic from 57.040 to 55.991 B/entry before the route
cache rewrite. A 32K missing lookup measured about 9.78 ns, successful lookup
was unchanged at roughly 61–64 ns on the million-key quick run, and the tested
load path improved from roughly 13.4 to 17.6 million inserts/s.

Decision: keep. One source of membership truth is both smaller and faster.

### 4. Packed two-choice Elastic route cache

Capacity-aware location codes combine the route and fingerprint in one `u32`.
Two-choice placement plus four-node relocation increased adaptive coverage from
80.49% to 96.74% while reducing route bytes from 5.031 to 4.031 MB. The complete
adaptive map is now 54.991 B/entry. The combined candidate/negative proof fixed
an initial miss regression; final 32K smoke results were about 24.25 ns per hit,
10.84 ns per miss, and 16.31 million inserts/s. A million-key hit measured
about 54.83 ns.

Decision: keep. See `EXPERIMENT_ROUTE_CACHE_V2.md` for the failed intermediate
versions and exact tradeoffs.

### 5. Fixed-batch route lookup

`PackedBinaryMap::get_many` prepares hashes and direct candidates first, then
resolves independent reads in route order. The million-key batch-of-32 fixture
improved from about 66.2 to 54.0 ns/key, but remained 1.94 times HashBrown's
27.9 ns/key.

Decision: keep as a throughput API. It does not solve scalar latency.

### 6. Segmented SwissTable

Splitting the hash range across independently sized Swiss tables avoids one
global power-of-two capacity cliff. Balanced mode retained about 53.59 B/entry.
In a recent quick sequential million-key Criterion run it measured about
29.1 ns/hit versus PackedSwiss at 26.7 ns and ordinary HashBrown at 23.2 ns.

A binary-search segment router was also tried. It regressed the same fixture to
roughly 41–43 ns versus about 40 ns for the short linear/predictable route and
was reverted.

Decision: keep linear-routed SegmentedSwiss as the best current general mutable
RAM/performance compromise, but label it non-elastic.

### 6b. Concurrent sharded SegmentedSwiss

Sixty-four cache-line-aligned shards add multi-reader/multi-writer point
operations, atomic insert-new/update/upsert/conditional-remove, shard-local
counters, and one-shard-at-a-time dead-key compaction. The concurrent layout
retained 54.309 B/entry at one million 32-byte keys versus DashMap's 84.438.
An eight-thread local smoke run reached 83.57 M read hits/s versus DashMap at
79.72 M/s, but the 90/5/3/1/1 cache mix reached only 39.31 M operations/s versus
DashMap at 77.30 M/s.

Decision: keep as the concurrent RAM frontier. It proves that sharding need not
erase packed-key density, but reader/writer interference still fails the mixed
throughput gate. See `EXPERIMENT_CONCURRENT_ETS.md`.

### 7. Cacheline two-choice bucket table

Twelve-slot SIMD-tag buckets plus dense entries retained 53.878 B/entry and
inserted about 22.6 million entries/s. The million-key hit fixture measured
about 147 ns, far behind PackedSwiss, SegmentedSwiss, and Elastic's direct
route. The extra dependent bucket-to-entry-to-arena reads dominate.

Decision: retain as negative evidence, not as a recommended backend.

### 8. Frozen exact perfect hashing

`FrozenPackedMap` uses PtrHash for a dense slot and always verifies the original
key bytes. With the opt-in hardware-AES hash, earlier million-key evidence was
about 49.0 B/entry and 14.5 ns/hit versus ordinary HashBrown at 84.429 B/entry
and 27.1 ns/hit. The newer hybrid harness reports 48.557 B/entry; allocation
placement and enabled hashing account for small run differences.

Decision: keep. This is the clearest existing win, but it is immutable and more
expensive to build.

### 9. Frozen base plus mutable delta

At 1% churn, the unfiltered hybrid retained 49.033 B/base entry. Hot overlay
hits were about 10.85 ns, but base hits, misses, and new inserts paid for both
generations. Optional stable filters expose rather than hide the RAM/latency
trade:

| Hybrid policy | B/entry | Base hit | Miss | New insert |
|---|---:|---:|---:|---:|
| no filter | 49.033 | 85.28 ns | 84.18 ns | 101.48 ns |
| 4 bits/entry | 49.538 | 108.50 ns | 54.65 ns | 66.05 ns |
| 8 bits/entry | 50.043 | 109.59 ns | 40.73 ns | 56.97 ns |
| PackedSwiss control | 67.652 | 74.79 ns | 24.12 ns | 24.68 ns |

Decision: keep for bounded-churn, read-mostly generations. It needs background
merge/publication before production use. See `EXPERIMENT_HYBRID_GENERATIONS.md`.

### 10. Fixed-32-byte structure of arrays

For digest-like `[u8; 32]` keys, dense key and value vectors plus a segmented
Swiss directory of `u32` indexes retained 45.89–46.35 B/entry. It now supports
insert, replace, lookup, dense swap-remove with route repair, and clear. In the
expanded random-query probe, compact SoA measured 59.54 ns/hit, 46.62 ns/miss,
and 43.62 ns/remove versus PackedSwiss at 74.47, 19.57, and 31.62 ns.

Decision: keep. It is the best RAM result for a mutable backend and wins the
tested random hit path, but loses misses/removal and is specialized to exactly
32-byte keys. See `EXPERIMENT_FIXED32_SOA.md`.

### 11. Deferred and staged maintenance

Moving Packed Elastic rebuild work out of the delete request reduced a
4,096-delete batch from about 1.68 ms to 98.2 us. The subsequent whole-map
maintenance remained about 1.46 ms, and staged key copying did not remove the
whole-map cutover.

Decision: keep deferred mode for the paper-derived backend. The separate
lock-free generation backend now supplies background construction, striped
atomic publication, and reclamation.

### 12. Lock-free overlay and atomic packed generations

Papaya provides the fully dynamic lock-free control. Combining the same
epoch-reclaimed overlay with Frozen PtrHash, ArcSwap, 4,096 atomic writer
stripes, and 64 padded length deltas retains 49.561 B/entry with a 1% generic
overlay. Stable cells reduce warm update allocation from Papaya's 89 bytes to
24 bytes, but the remaining `Arc<V>` allocation scales poorly.

The `NonMaxU64` specialization splits frozen key identity from one direct
`AtomicU64` per perfect-hash slot. Existing-key updates/deletes allocate zero
bytes and 1% churn retains 48.761 B/entry with no overlay records. Its latest
eight-thread smoke reached 110.802 M read hits/s, 42.415 M delete hits/s, and
90.610 Mops/s on the cache mix versus DashMap at 68.060, 28.583, and 71.301.
Distributed update is noisy but competitive; one-hot-key CAS contention and
new-key insertion remain clear losses.

The first naive layered cutover lost one contended increment because old/new
writers could update the same key simultaneously. The corrected protocol keeps
equal keys on an open predecessor stripe and closes that stripe only at zero
writers. Million-entry median handoff measured about 4–28 microseconds across
no-writer, 10K-hot-set, and whole-table-write cases. Equivalent-base publication
was about 2–16 microseconds and the expensive build ran with operations enabled.
Atomic generations perform a second zero-writer transition before a stripe
mutates the new base directly. A mixed rebuild test also caught and fixed stale
`len` baseline publication. There is no changed-key replay.

Decision: keep the generic map as the arbitrary-value path and the atomic map as
the packed concurrent RAM/performance frontier. This is not a universal
faster-than-SwissTable claim because insertion and hot-key results still lose. See
`EXPERIMENT_LOCKFREE_GENERATIONS.md`.

### 13. Exact prepared handles and batched hot operations

Implicit shared and thread-local exact slot caches were both implemented and
removed after losing hot-read throughput. Explicit 24-byte handles instead let
the caller identify the hot 1% for 0.24 byte per total key. They retain exact
key verification and safely fall back across wrong keys, maps, overlays,
deletes, and rebuilds.

Allocation-free batch reads share one generation pin. Batch 16 improves scalar
prepared reads by 18.9% at 100,000 entries and 22.3% at one million. On the
90%-on-1% mixed hot trace it reaches 48.713–416.942 Mops/s across one to eight
threads.

Prepared update and replacement batches first shared a generation snapshot but
kept a writer-stripe pin per item. A single batch-wide gate improved low-core
results but collapsed at eight threads due to one contended cache line, so it
was rejected. The retained opt-in layout uses 16 cache-line-separated gates,
adding 1,040 requested bytes per live generation.

The sharded-gate batch reaches 42.203/62.851/85.426/107.078 M updates/s and
41.651/57.369/78.834/108.648 M replacements/s at one, two, four, and eight
threads. These are 39.8–66.4% and 24.2–43.6% above scalar prepared operations.
Under eight continuous batch writers, median rebuild redirection increases from
10.666 to 22.250 microseconds while background construction remains essentially
flat.

Decision: keep `prepared-keys` as the zero-extra-state explicit hot-key API.
Keep `prepared-batch-gate` as an opt-in write-heavy mode; do not make one global
gate or implicit caches the default.

### 14. Portable SIMD exact-key verification

PackedGen already uses proven SIMD for 16-byte tag masks and experimental
four-lane packed-reference tags, while the atomic fixed-32 overlay scans its
eight control bytes with one SWAR word operation. A focused exact-key probe
compared native `[u8; 32]` equality, two scalar `u128` comparisons, a branchless
four-`u64` XOR, and `wide::u64x4`.

On the M4 Max, explicit `u64x4` lost 10.8% on hits, 14.1% when the first byte
differed, 14.8% when the final byte differed, and 6.5% on a 95%-hit mix. The
branchless scalar words won the isolated microbenchmark by 17.3–29.5%, but
putting that comparison into the real frozen/prepared path changed prepared
reads from 12.150 to 12.190 ns. Arena resolution and surrounding generation
work consumed the isolated gain.

Decision: reject a new general SIMD key-equality feature and remove the scalar
replacement. Keep native equality, the existing successful SIMD tag scans,
SWAR atomic controls, and hardware-accelerated hashing. Re-run the standalone
probe on AVX2/AVX-512 machines before making an architecture-specific decision.

## Honest current conclusion

There is no single general mutable backend here that is both smaller and faster
than SwissTable on every operation. The strongest results are workload-shaped:

- immutable: Frozen PtrHash wins RAM and tested successful reads;
- read-mostly with bounded churn: Hybrid keeps near-frozen density;
- concurrent read-mostly with atomic-word values: direct-slot generations win
  the tested read, delete, and cache-mix paths while losing new inserts and
  single-hot-key updates;
- fixed 32-byte keys: SoA is the densest mutable layout and wins tested random
  hits, while losing misses and removal;
- general mutable packed bytes: SegmentedSwiss is the current speed/RAM
  frontier;
- paper-derived mutable Elastic: adaptive Packed Elastic is now close to
  SegmentedSwiss RAM, but its exact fallback and insertion schedule remain the
  main latency work.

The path to a broadly better library is therefore a family of explicit
backends selected by workload, backed by the same exact-key semantics and
comparison harness—not one data structure marketed as a universal win.
