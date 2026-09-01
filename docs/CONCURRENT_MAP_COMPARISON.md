# Rust concurrent-map comparison

Date: 2026-07-16

This comparison asks two separate questions:

1. How many live 32-byte-key records fit in RAM?
2. What happens to point-operation throughput as more worker threads run?

It compares the packed atomic PackedGen generation with DashMap 6.2.1,
Papaya 0.2.4, `scc` 3.8.5, Flurry 0.5.2, and the common baseline of HashBrown
0.17.1 behind one `parking_lot::RwLock`. The historical tables use the same
FoldHash builder, 32-byte binary keys, `u64` values, pre-sized tables, and
identical operation traces. PackedGen's current portable default now uses two
Rapidhash lanes, so a fresh full external matrix is required before those rows
describe current-source performance. DashMap uses 64 shards.

## How to read the results

- Throughput is millions of operations per second (Mops/s): **higher is
  better**.
- Memory is requested live heap bytes per entry (B/entry): **lower is better**.
- Thread counts are active unpinned OS threads, not a promise that the OS used
  that many identical physical cores.
- These are local development results, not portable guarantees or a claim that
  one map wins every workload.

The machine was an Apple M4 Max with 12 performance cores and four efficiency
cores, macOS Darwin 25.5.0, and Rust 1.94.0. The `scc` documentation says its
optimal SIMD path requires 256-bit SIMD and specifically notes that Apple
M-series CPUs do not provide it. Its result may therefore improve relative to
the others on AVX2-capable x86-64.

Papaya and Flurry use one reclamation guard per 4,096 point operations. This
amortizes pinning cost while periodically allowing garbage reclamation. Holding
one guard forever would make write-heavy throughput look better while allowing
retired memory to grow; pinning every individual operation would measure a
different, lower-throughput API usage.

Eviction caches such as Moka and Quick Cache are intentionally outside this raw
map ranking. They add admission/eviction, capacity enforcement, expiration,
and other bookkeeping that this benchmark does not ask the other maps to
provide. Double-buffered or eventually consistent maps are also excluded
because the tested maps publish completed per-key writes immediately. They
deserve a separate end-to-end cache-policy comparison, not an entry-size point
operation table that would punish them for additional semantics.

## RAM at one million entries

The memory probe counts requested heap bytes allocated minus deallocated while
the live map remains in scope. It does not measure allocator size classes,
process RSS, thread stacks, or allocator fragmentation.

| Implementation | Live heap | B/entry | Allocations | Entries in PackedGen's RAM |
|---|---:|---:|---:|---:|
| **PackedGen atomic packed generation** | **49,135,325** | **49.135** | **11,404** | 1.000x |
| Papaya | 74,893,016 | 74.893 | 2,000,006 | 0.656x |
| `RwLock<HashBrown>` | 84,428,808 | 84.429 | 1,000,002 | 0.582x |
| DashMap | 84,437,504 | 84.438 | 1,000,066 | 0.582x |
| `scc::HashMap` | 86,534,848 | 86.535 | 1,000,009 | 0.568x |
| Flurry | 168,785,873 | 168.786 | 3,000,021 | 0.291x |

The final column holds RAM constant at PackedGen's 49.135 MB. In the other
direction, PackedGen fits approximately **52.4% more entries than Papaya**,
**71.8% more than DashMap**, **76.1% more than `scc`**, and **243.5% more than
Flurry** in the same requested index heap.

This PackedGen state is not read-only: packed base values are atomic and can
be updated, deleted, and reinserted in place. It also includes 1% compact
overlay headroom for new keys. The tradeoff is structural: new keys beyond the
overlay budget require a generation rebuild, and deleted packed keys do not
release their arena bytes until rebuild. The other maps are fully dynamic.

## Successful-read scaling

These are medians over 11 alternating samples, 100,000 preloaded keys, and
3,000,000 operations per sample.

| Implementation | 1 thread | 2 | 4 | 8 | 12 | 16 | Peak |
|---|---:|---:|---:|---:|---:|---:|---:|
| PackedGen | 16.994 | 29.269 | 71.652 | **149.537** | 113.821 | 91.923 | 149.537 (8) |
| DashMap | 49.817 | 53.945 | 79.011 | 101.488 | **101.861** | 95.270 | 101.861 (12) |
| **Papaya** | 45.948 | 50.661 | 186.618 | **358.530** | 276.620 | 274.575 | **358.530 (8)** |
| `scc` | 38.189 | 34.535 | 89.601 | **137.315** | 136.208 | 125.148 | 137.315 (8) |
| Flurry | 17.171 | 28.151 | 83.643 | **161.687** | 145.630 | 64.750 | 161.687 (8) |
| `RwLock<HashBrown>` | **56.832** | 29.851 | 24.927 | 15.137 | 10.104 | 12.972 | 56.832 (1) |

Papaya is the clear read-hit winner. PackedGen peaks at 8 threads, where it
is 47% faster than DashMap and 8.9% faster than `scc`, but it is still 58.3%
behind Papaya. The single-lock HashBrown control wins one-thread reads and then
degrades under shared-lock cache-line contention.

The 16-thread column is not automatically better than 8 or 12. It activates
enough workers to cover all 16 physical cores, including the four efficiency
cores, and adds scheduling, coherence, and memory-bandwidth pressure. Papaya,
PackedGen, `scc`, DashMap, and Flurry all reached their read-hit peak before
16 threads in this run.

## Missing-read scaling

The same long probe for absent keys:

| Implementation | 1 thread | 4 threads | 8 threads | 12 threads | 16 threads |
|---|---:|---:|---:|---:|---:|
| PackedGen embedded fingerprint | 30.246 | 118.751 | 134.294 | 199.119 | 112.111 |
| DashMap | 55.658 | 91.460 | 82.123 | 102.824 | 95.800 |
| **Papaya** | **76.700** | **292.892** | **580.397** | **527.968** | 469.627 |
| `scc` | 74.431 | 133.710 | 141.642 | 155.863 | 136.281 |
| Flurry | 59.482 | 228.424 | 479.383 | 460.936 | **480.862** |
| `RwLock<HashBrown>` | 71.608 | 29.690 | 16.166 | 11.722 | 12.380 |

Papaya has the best miss path through 12 threads; Flurry narrowly leads at 16.
PackedGen's zero-byte embedded fingerprint makes its miss path much faster
than its exact packed-key schedule, but Papaya remains the stronger control.

## Read-heavy cache mix

This trace is 95% successful reads, 2% read misses, 2% distributed updates,
0.5% new-key inserts, and 0.5% successful deletes. Results are seven-sample
medians over 3,000,000 operations.

| Implementation | 1 thread | 2 | 4 | 8 | 12 | 16 |
|---|---:|---:|---:|---:|---:|---:|
| PackedGen | 14.545 | 32.872 | 67.964 | 123.667 | 101.025 | **138.122** |
| DashMap | 35.048 | 49.469 | 71.058 | 96.715 | 92.589 | 98.677 |
| **Papaya** | 31.077 | **67.642** | **98.345** | **233.002** | **167.881** | **199.237** |
| `scc` | 30.565 | 53.904 | 80.742 | 134.355 | 116.735 | 155.751 |
| Flurry | 16.276 | 34.197 | 72.150 | 124.168 | 122.940 | 132.502 |
| `RwLock<HashBrown>` | **42.387** | 26.576 | 17.985 | 10.251 | 7.358 | 6.671 |

Papaya is the best cache-like throughput choice on this machine. At 16 threads
it is 44.2% faster than PackedGen, while PackedGen is 40.0% faster than
DashMap and uses 41.8% less RAM than DashMap. PackedGen is therefore a real
memory/performance tradeoff, not the overall speed winner.

## Eight-thread operation matrix

The mutation rows use seven-sample medians with 300,000 requested operations;
successful delete is capped at the 100,000 preloaded keys. Read and cache rows
use the longer probes above. **Higher is better.** Bold marks the row winner.

| Workload | PackedGen | DashMap | Papaya | `scc` | Flurry | RwLock HB |
|---|---:|---:|---:|---:|---:|---:|
| Read hit | 149.537 | 101.488 | **358.530** | 137.315 | 161.687 | 15.137 |
| Read miss | 134.294 | 82.123 | **580.397** | 141.642 | 479.383 | 16.166 |
| Insert/replace hit | **49.881** | 46.155 | 25.024 | 48.469 | 22.955 | 4.237 |
| Insert new key | 27.897 | 48.417 | **66.124** | 55.964 | 15.150 | 7.466 |
| Update hit, distributed keys | 49.157 | 52.854 | 25.233 | **82.503** | 26.726 | 10.975 |
| Update miss | 64.575 | 77.519 | 188.171 | 139.157 | **252.118** | 30.142 |
| Update one hot key | 5.496 | 64.397 | 4.626 | **93.379** | 14.999 | 76.716 |
| Delete hit | 44.872 | 51.923 | 44.786 | **99.614** | 16.099 | 9.313 |
| Delete miss | 66.213 | 85.398 | **515.021** | 127.551 | 264.570 | 29.412 |
| 95% read-hit cache mix | 123.667 | 96.715 | **233.002** | 134.355 | 124.168 | 10.251 |

The one-hot-key row is an intentionally pathological counter-like contention
test, not the normal cache pattern. It measures every thread repeatedly
incrementing the value under one key. PackedGen and Papaya use compare-and-swap
replacement there and lose heavily to `scc`'s bucket serialization and
DashMap's shard lock. A real counter payload should normally use an atomic
integer rather than replacing the map value for every increment.

## What the comparison says

- **Memory-first:** PackedGen is the clear winner. Its packed generation uses
  34.4% less RAM than Papaya and roughly 41.8% less than DashMap/HashBrown.
- **Read-heavy speed-first:** Papaya is the control to beat. It wins read hits,
  misses, and the realistic 95%-read cache trace.
- **Mutation speed-first:** `scc` wins distributed successful updates, one-hot
  updates, and successful deletes on this M4 Max, despite lacking its preferred
  256-bit SIMD path.
- **Single-thread simplicity:** `RwLock<HashBrown>` is excellent without
  contention, but one global lock is not an ETS-like multicore design.
- **Flurry:** its miss path can be fast, but the measured RAM is about 3.46x
  PackedGen and Flurry's own documentation recommends Papaya or DashMap when
  performance and memory under load matter.
- **PackedGen today:** it is already the density winner and can scale strongly
  on distributed keys. It is not yet the fastest general concurrent map.

The next optimization targets are consequently clear: reduce the packed base
read-hit schedule, replace the slow new-key insertion path, and improve
distributed update before revisiting hot-key CAS behavior. Optimizing only the
hot-key case would not improve the normal cache trace enough.

## Insert-path optimization pass

The first retained new-key optimization keeps Papaya's preliminary occupancy
probe, but performs the probe and conditional insertion through one pinned map
view. Previously, the direct frozen-base miss path pinned the overlay once to
probe it and a second time to attempt insertion. The new path is used by
`insert`, `insert_new`, and `upsert` and remains lock-free.

Two alternating old/new runs used 100,000 preloaded 32-byte keys, 300,000 new
keys, and 21 or 41 median samples. **Higher throughput is better.** The more
conservative repeat was:

| Threads | Old Mops/s | Shared-pin Mops/s | Change |
|---:|---:|---:|---:|
| 1 | 10.391 | **11.377** | **+9.5%** |
| 2 | **11.511** | 11.475 | -0.3% |
| 4 | 21.820 | **21.929** | **+0.5%** |
| 8 | 26.682 | **27.434** | **+2.8%** |

The first alternating pair improved all four points by 7.7% to 30.9%, so the
exact multicore gain remains scheduler-sensitive, but neither pair showed a
meaningful regression. The 95%-read cache trace still reached 140.953 Mops/s
at 16 threads. The optimization does not change table layout or measured RAM.

Four tempting micro-optimizations were measured and rejected:

- Skipping the writer hash on clean base reads destroyed high-thread read
  scaling; the existing hash step appears to warm and distribute the lookup.
- Reducing 4,096 writer stripes to 256 padded counters increased real stripe
  collisions and slowed 4- and 8-thread updates.
- Going directly from a frozen-base miss to `try_insert`, without the
  preliminary overlay probe, helped one thread but hurt multicore insertion.
- Relaxing atomic value CAS failure ordering from acquire to relaxed slowed
  8-thread distributed updates by about 17% in the focused run.

These failures narrow the next useful work: the remaining gains are more
likely to come from a purpose-built compact new-key overlay or a redesigned
generation writer handoff than from weaker atomics or fewer safety checks.

## Atomic bucket overlay

The next pass implemented that purpose-built overlay and made it the default
for the atomic 32-byte-key map. It uses eight-slot buckets with one packed
atomic control word, colocated 32-byte keys and atomic values, and three
deterministic bucket choices. A saturated key route falls back to Papaya, so
overflow remains bounded and exact. It uses no unsafe code and reuses the
writer's existing hash instead of hashing each key twice.

At 300,000 resident overlay entries, **lower RAM and allocation counts are
better**:

| Overlay | B/entry | Live allocations |
|---|---:|---:|
| **Atomic bucket** | **51.919** | **3,006** |
| Inline-key Papaya | 56.011 | 300,015 |
| ArcSwap dense control | 66.225 | 300,075 |
| Boxed-key Papaya | 79.950 | 600,012 |

At eight threads and 31 alternating samples, **higher throughput is better**:

| Overlay operation | Atomic bucket | Inline Papaya | Difference |
|---|---:|---:|---:|
| Insert new key | **39.778** | 34.312 | **+15.9%** |
| Read overlay hit | 126.658 | **151.742** | -16.5% |
| Update overlay hit | 60.662 | **62.292** | -2.6% |

The sequential 1/2/4/8/12/16-thread insertion sweep also favored the atomic
bucket at every point, although the unpinned high-core medians remain noisy.
On the complete frozen-base path, eight-thread new-key insertion improved from
the preceding inline-overlay result of 27.434 to 36.156 Mops/s (+31.8%). A
deliberately undersized overlay still sustained 17.260 Mops/s by overflowing
to Papaya instead of scanning the entire front table.

The cost is explicit: reserving the atomic bucket headroom raises the full
one-million-entry generation from 48.789 to 49.135 B/entry (+0.7%), and a hot
overlay-resident read remains slower than inline Papaya. Once rebuild packs
those keys into the frozen generation, reads return to the faster packed-base
path. This makes the new default a multicore insertion/density optimization,
not a claim that it wins every isolated operation.

### Packed writer-stripe length accounting

The next retained insertion pass removed the separate 64-way, cache-line-padded
length counter bank. Signed logical-length deltas now occupy otherwise unused
bits in the existing 4,096 writer-stripe words. Writer counts, handoff flags,
and length deltas remain independently masked, and stripe closure preserves the
packed delta until the successor captures the predecessor's stable length.

This keeps exact `len()` semantics and the same point-operation atomic count,
but successful inserts and deletes now update the writer-stripe cache line that
the operation already pinned instead of contending on a second 64-way counter
bank. It also removes 4,096 requested bytes per live generation.

Seven alternating old/new process pairs used 300,000 distinct insertions into
an empty exact-32-byte atomic overlay. **Higher throughput is better.**

| Threads | Separate length counters | Packed stripe delta | Change |
|---:|---:|---:|---:|
| 1 | **19.856 Mops/s** | 19.581 Mops/s | -1.4% |
| 8 | 41.014 Mops/s | **54.871 Mops/s** | **+33.8%** |

In the complete frozen-base comparison, a later 15-sample run measured
49.614 Mops/s for the exact default and 51.230 Mops/s for the one-byte miss
filter versus DashMap at 49.363 Mops/s. Two separate 31-sample runs put the
exact default at 46.688-48.716, the one-byte policy at 48.506-52.282, and
DashMap at 50.129-50.809 Mops/s. Both PackedGen policies therefore straddled
DashMap across unpinned runs. The honest conclusion is that multicore new-key
insertion is now tied within local noise rather than a stable overall win.
Single-thread insertion remains a clear loss.

The same change improved eight-thread successful delete in the complete probe
to 61.813 Mops/s versus DashMap at 52.694 Mops/s. Five alternating read-hit
process pairs showed less than a 1% median change after normalizing against the
DashMap control, so the mutation gain did not require a retained read-path
tradeoff.

Three related ideas were measured and rejected:

- lazily calculating secondary and tertiary bucket routes regressed
  single-thread insertion and did not improve eight threads;
- multiply-high bucket reduction was about 1.7% slower at one thread and flat
  at eight threads;
- 128/256 separate length counters traded a 2-8% multicore gain for a 6-8%
  single-thread loss and 4-12 KiB more metadata.

### Fused conditional insertion and combined length release

Packing the count and signed length delta into one word enabled a further
reduction: successful `insert`/`insert_new`/`upsert`, delete, and single
prepared-slot mutations now update the delta and release their writer count in
one compare-and-swap. The previous paths performed those as two
read-modify-write operations on the same stripe word. No persistent state or
per-entry bytes were added.

The atomic API also exposes `AtomicEntry` and a one-shot
`AtomicVacantEntry`. The vacant handle keeps the key's writer stripe pinned and
carries both its precomputed hash and one exact absence proof. Deleted frozen
members carry their verified dense slot; true frozen nonmembers go directly to
the overlay. A concurrent insert still resolves through the slot/overlay CAS,
and a rebuild cannot close that stripe until the handle is consumed or
dropped. Tests cover frozen deletion/reinsertion, overlay tombstones, a racing
winner, and concurrent rebuild redirection.

The first `get_or_insert` implementation reused the public vacancy proof but
still traversed the overlay once to prove absence and again to publish. It was
replaced rather than retained: its one-byte path measured 14.252/46.647 Mops/s
at 1/8 threads, effectively the same as the adjacent entry control at
14.188/46.034. The production primitive now follows the direct
`insert_new` route: one writer pin, one frozen lookup, and one overlay
traversal. It returns a racing writer's value without replacing it.

The focused 100K-base/200K-new-key, 21-sample `gxhash` run includes
DashMap and raw Papaya 0.2.4. Papaya reuses one guard per worker; this
pure-insertion trace retires no records, so the long-lived guard does not defer
reclamation work. **Higher Mops/s is better**:

| Implementation/policy | 1 thread | 8 threads | Extra frozen RAM |
|---|---:|---:|---:|
| PackedGen `insert_new`, exact | 12.220 | 46.760 | 0 B/key |
| PackedGen `insert_new`, embedded fingerprint | 18.247 | 50.364 | **0 B/key** |
| PackedGen `insert_new`, one-byte filter | **20.655** | 51.547 | 1 B/key |
| DashMap insert | **21.145** | 48.328 | n/a |
| Papaya insert | 20.180 | **62.283** | n/a |

For the cache-style conditional path, embedded `get` then `insert_new`
measured 10.899/41.099 Mops/s at 1/8 threads and embedded `entry` then
`insert_new` reached 12.669/44.586. The true fused embedded operation reached
17.075/50.813; the one-byte version reached **19.412/51.201**, improving over
its entry composition by 49.6%/12.1%. DashMap entry measured 20.643/47.354.

Papaya's native `get_or_insert` reached **20.583/64.601**, so it remains 5.7%
faster at one thread and 20.7% faster at eight. That is a much narrower
single-thread gap than the composed PackedGen result, but the multicore gap is
still material. The embedded policy remains the density-friendly choice
because its fingerprint occupies bits already retained by the frozen key
reference.

A separate writer-release probe temporarily restored the old two-publication
path. Against that 15-sample control, the retained 31-sample embedded results
put successful upsert-miss at 14.338 vs 14.429 Mops/s at one thread and 48.549
vs 46.497 at eight; successful delete reached 32.242 vs 30.160 and 69.676 vs
62.784. Upsert-hit, which has no length change, remained within local noise.
Because these were sequential unpinned runs rather than a same-binary paired
control, the structural one-RMW reduction is firm but the percentages are only
directional.

### Reusable operation guard and biased stripe arithmetic

Papaya's comparison reuses one reclamation guard for each worker loop, while
the ordinary PackedGen path protects generation handoff separately for every
write. The optional `operation-batch` feature now provides a reusable
`AtomicOperationGuard` for read, insert, insert-new, get-or-insert, update,
upsert, remove, and conditional remove. `get_or_insert_batch` scopes the same
optimization automatically to three caller-owned slices. A retained guard can
delay rebuild handoff, so the intended lifetime is one request or bounded
worker batch.

The feature reuses the existing 16 prepared-batch gates: measured persistent
cost is about 1,040 bytes per live generation and zero bytes per entry. Tests
cover every guarded CRUD operation, exact concurrent distinct-key publication,
length accounting, and a rebuild racing a live guard.

The packed length field is now biased around zero on 64-bit targets. That lets
successful point operations change the length and release their writer with
one `fetch_add`/`fetch_sub`, rather than a potentially retrying whole-word CAS.
Guarded operations likewise adjust a stripe with one fetch operation. The
32-bit path retains the checked CAS because its narrower delta range can
realistically approach a boundary.

The focused 100K-base/200K-new-key, 21-sample run measured the following;
**higher Mops/s is better**:

| Conditional new-key path | 1 thread | 8 threads |
|---|---:|---:|
| PackedGen point, one-byte | 20.393 | 51.574 |
| **PackedGen guarded, one-byte** | **24.817** | **57.593** |
| DashMap entry | 22.937 | 49.234 |
| Papaya `get_or_insert` | 21.443 | **69.488** |

The guard improves PackedGen by 21.7%/11.7% at 1/8 threads. It leads Papaya by
15.7% at one thread and DashMap by 17.0% at eight, but Papaya remains 20.7%
faster at eight threads. The remaining gap is therefore inside concurrent
overlay publication and exact length maintenance, not repeated hashing or
generation pinning.

Three zero-RAM micro-optimizations were rejected around the guard work. Taking
the tag directly from the high hash byte regressed the one-byte fused path by
about 2.8%/1.3% at 1/8 threads. Reusing the control word observed by the bucket
scan for the later claim CAS also failed to improve the target path and made
the scalar result less stable; the fresh pre-CAS load remains. Precomputing the
four 64-bit query words before bucket traversal regressed the guarded one-byte
path by about 9% at both thread counts, so the compiler-friendly on-demand
loads remain.

#### Release-store slot publication

The atomic overlay's `writing` byte already serializes writers within one
eight-slot bucket: after a successful claim, every other writer that observes
the marker waits, readers never change controls, and occupied controls are not
cleared. The claimant therefore exclusively owns the control word until tag
publication. The retained path replaces its marker with one release store
instead of a second whole-word read-modify-write. It changes no layout, RAM,
load target, or read path.

A focused 100K-capacity/500K-new-key probe used 31 samples per run. The old RMW
publication reached 12.586 Mops/s at one thread and 52.963 at eight. Two
adjacent release-store runs reached 13.609-13.814 Mops/s (**+8.1% to +9.8%**)
and 53.501-54.971 (**+1.0% to +3.8%**) respectively. In the complete
100K-base/200K-new-key comparison, guarded one-byte `get_or_insert` moved from
23.572 to 25.510 Mops/s at one thread (+8.2%) and from 57.051 to 57.897 at
eight (+1.5%); the point path stayed within 0.6% in either direction.

The adjacent-slot concurrency test now repeats 64 synchronized eight-writer
rounds against a single bucket, in addition to the existing duplicate-key,
CRUD, overflow, and rebuild tests. This optimization is retained because it
removes one RMW structurally, improves the publication-heavy focused trace,
and introduces no persistent or read-side cost.

#### Lazy fourth-choice overflow suppression

The atomic overlay now derives a fourth distinct bucket only after its first
three candidate buckets are completely full. The common primary/secondary
path, slot layout, target load, and retained metadata are unchanged. Exact
lookups follow the same placement invariant: the fourth bucket is consulted
only when the preceding three contain no empty slot.

Five fresh-process allocation samples at 100,000 live 32-byte overlay keys
showed the purpose of the change. Median fallback allocations fell from 1,059
to 363 (**-65.7%**), while requested live memory fell from 52.406 to 52.042
B/entry (**-0.7%**) without reserving another byte. The saving comes from
keeping keys in already allocated atomic slots instead of boxed Papaya
overflow records.

Adjacent 31-sample probes expose the performance tradeoff; **higher Mops/s is
better**:

| Workload | Three choices | Lazy fourth | Change |
|---|---:|---:|---:|
| New-key insert, 1 thread | **14.333** | 13.899 | -3.0% |
| New-key insert, 8 threads | 52.340 | **55.999** | **+7.0%** |
| Overlay read hit, 1 thread | **19.486** | 19.057 | -2.2% |
| Overlay read hit, 8 threads | 166.553 | **166.969** | +0.2% |

The full guarded one-byte conditional-insert path remained nearly flat at
eight threads (57.897 before and 57.751 Mops/s after) and changed from 25.510
to 24.985 at one thread (-2.1%) across separate complete runs. The lazy fourth
choice is retained because the project prioritizes density and multicore use:
it removes roughly two thirds of overflow allocations, saves RAM, and improves
the saturated eight-writer path, for a measured 2-3% scalar cost.

A fifth lazy bucket was measured and rejected. It reduced median fallback
allocations from 363 to 131 and memory from 52.042 to 51.930 B/entry, only
another 0.2% density gain. In return, new-key insertion fell from 13.899 to
13.110 Mops/s at one thread (-5.7%) and from 55.999 to 46.614 at eight
(-16.8%); overlay read hit also fell from 19.057/166.969 to
18.928/160.039 Mops/s at 1/8 threads. Four choices are therefore the measured
stopping point for this layout.

#### Zero-RAM definite-overflow proof

First-fit placement also gives an exact negative proof for the fallback map.
If any of a key's candidate atomic buckets still has a stopping slot, that key
could never have overflowed past all four buckets into Papaya. After an atomic
lookup misses, the retained path rechecks only the packed control words and
skips the fallback lookup when it sees such a slot. Variable-width keys, zero
capacity, and four-full-bucket cases still take the fallback path. The proof
adds no bits, allocations, or persistent bytes.

Adjacent 31/41-sample 100K-base probes measured the following; **higher Mops/s
is better**:

| Miss operation | Before proof, 1/8 threads | Retained proof, 1/8 threads | Change |
|---|---:|---:|---:|
| Read | 39.531 / 293.302 | **42.335 / 319.578** | **+7.1% / +9.0%** |
| Update | 25.022 / 77.285 | **25.324 / 81.445** | **+1.2% / +5.4%** |
| Delete | 24.843 / 73.297 | **25.736 / 83.592** | **+3.6% / +14.0%** |

The first implementation returned a new three-state lookup enum, which made
the compiler carry the miss proof through the successful-read path. It raised
scalar miss throughput to 45.945 Mops/s but regressed overlay hit throughput
from 19.057/166.969 to 18.149/140.280 Mops/s at 1/8 threads. An output-flag
variant showed the same hit regression and was rejected. The retained shape
leaves the original `Option` lookup unchanged and performs a separate packed
control recheck only after a miss. Two 41-sample hit runs measured
18.720-19.096 Mops/s at one thread and 166.085-166.961 at eight, within the
preceding baseline range.

#### Lazy compact atomic overflow tier

Exact 32-byte keys that exhaust all four primary choices now enter ordered
packed atomic overflow segments before the generic Papaya fallback. A segment
is absent until the preceding table is full. Racing initializers publish one
candidate through `ArcSwap` compare-and-swap; losing candidates are reclaimed,
so no writer waits for another initializer. Reads and established writers
borrow an ArcSwap guard rather than incrementing a shared Arc count. A stopping
slot proves that the key could not have reached a later segment, preserving the
fast miss path and exact cross-tier uniqueness. Any residual spill continues to
Papaya.

The retained geometry is elastic rather than reserving 1.25 slots per requested
primary entry. The primary reserves 1.10x and the overflow reserve is 1/32 of
requested capacity with the same 1.10x target. Its first segment owns 80% of
that reserve; the final 20% is allocated only if the first segment fills. At
100,000 live overlay keys, five allocation samples were identical at **46.849
B/entry and 14 live allocations**, versus a 52.042 B/entry median and 363
allocations for the preceding 1.25x/direct-overflow layout. That is **10.0%
less RAM and 96% fewer live allocations**.

The focused new-key probe moved from 13.899/55.999 to
**14.979/54.485 Mops/s** at 1/8 threads: +7.8% scalar throughput and -2.7% at
eight in adjacent 41-sample runs. The complete guarded one-byte
`get_or_insert` probe measured **25.555/58.216 Mops/s**, versus DashMap entry at
23.237/50.672 and Papaya at 23.053/71.006. Overlay hit reads reached
21.184/174.082 Mops/s and the retained miss proof reached 42.652/309.219.

At one million live keys, the elastic overlay retained **46.290 B/entry and 14
allocations**, versus DashMap's 84.438 B/entry and 1,000,066 allocations and
boxed Papaya's 82.937 B/entry and 2,000,011 allocations. These are requested
allocator bytes, not RSS.

The geometry sweep found a clear knee. A 1.20x primary used 50.014 B/entry and
inserted at 14.656/56.898 Mops/s; 1.15x used 48.142 B/entry and
15.024/54.469; the retained 1.10x used 47.131 and 14.979/54.485. Pushing to
1.075x reached 46.534 B/entry but dropped eight-thread insertion to 51.638, so
it was rejected. Secondary-tier divisors from 64 through 768 were also swept;
undersizing eventually increased boxed spill and hurt throughput.

A synchronized 64-writer test forces lock-free lazy initialization, the second
tier, and final fallback concurrently and verifies every key and exact length.
The initial `load_full` design was rejected after its shared Arc refcount
reduced eight-thread overlay reads to 139.771 Mops/s. Borrowed guards restored
173.792-182.213 Mops/s in two 41-sample runs without changing RAM.

The final overflow reserve was then segmented to reduce both steady and
simultaneous first-publication memory. Fixed 512-entry pieces reached 46.422
B/entry at 100K and 45.653 at 1M, but made insertion traverse too many full
segments and fell to 13.640/51.159 Mops/s at 1/8 threads. A 75%/25% split used
46.781/46.219 B/entry at 100K/1M but remained about 1.8% behind an adjacent
unsplit insertion control. The retained 80%/20% split uses 46.849/46.290 and
keeps only the first segment live in both allocation probes. Adjacent unpinned
insertion runs varied materially with machine state; normalized against the
embedded-filter control in the same alternating probe, the split stayed within
roughly 1-2% of the unsplit layout. The 87.5% split saved less RAM without a
clearer throughput result and was rejected. The fixed two-segment array also
removes all segment-directory heap allocations.

A fresh final 21-sample competitor run measured the best ordinary PackedGen
new-key path at **17.757/54.062 Mops/s** for 1/8 threads, DashMap insert at
21.395/50.391, and Papaya insert at 18.318/71.632. Thus PackedGen is 7.3%
faster than DashMap at eight threads while using 45.2% less requested RAM in
the one-million-entry comparison, but DashMap remains 17.0% faster for scalar
insertion and Papaya remains 32.5% faster for eight-thread insertion. Higher
Mops/s is better; lower B/entry and allocation count are better.

#### Bounded atomic-update backoff

The generic atomic update loop now pauses for 1, 2, 4, then at most 8 processor
spin hints after consecutive compare-and-swap failures. There is no pause on an
uncontended success and no added state or retained memory. Caps of 1 and 4 gave
only small or inconsistent hot-key gains; a cap of 16 reached 12.052 Mops/s in
one filter probe but reduced distributed-update throughput, so it was rejected.

With the retained cap of 8, the final alternating DashMap comparison measured
the exact/one-byte hot-key paths at **6.012/8.532 Mops/s** versus DashMap at
58.755. DashMap remains the decisive winner for one shared counter, but the
exact path is 45% above the preceding 4.134 result and the one-byte path is more
than twice that old baseline. Distributed eight-thread update remained
**56.411 Mops/s** for exact PackedGen versus 55.718 for DashMap. The 95%-read
cache mix reached 87.795 exact and 91.838 one-byte versus DashMap at 86.394.

Two writer-pin experiments were also rejected. Replacing the ordinary
load/CAS pin with one `fetch_add` was 3.4% slower for scalar insertion and tied
at eight threads in the adjacent run. Relaxing the predecessor-active flag's
acquire load was 0.6% slower scalar and produced no stable multicore gain.

#### Variable zero-through-32-byte atomic keys

The opt-in `AtomicUpTo32` mode reuses the exact same 40-byte physical slot for
mixed short keys. A zero-through-31-byte key is zero-padded and stores its
length in byte 32; a 32-byte key remains unchanged. Short and full-width keys
use disjoint occupied control-tag ranges, so an adversarial full key equal to a
short key's physical encoding remains distinct even under a hash collision.
Keys over 32 bytes retain the exact Papaya fallback. No unsafe code, side
metadata, per-key allocation, or extra slot bytes are introduced.

At 100K live entries the mode is flat at **46.850 B/entry and 14 allocations**
for 8, 16, 24, and 31-byte keys. DashMap measured 40.855, 48.855, 56.855, and
63.855 B/entry, making 14 bytes the measured crossover; the packed mode is not
the RAM choice for very small fixed keys. Boxed Papaya ranged from 64.216 to
87.216 B/entry. At one million entries, the packed mode used **46.290
B/entry**; for 16/31-byte keys DashMap used 68.438/83.438 and boxed Papaya used
66.937/81.937. Thus the variable atomic mode saves 32.4–44.5% versus DashMap
and 30.8–43.5% versus Papaya in those million-entry probes.

The alternating operation probe shows the speed tradeoff; higher Mops/s is
better:

| Key/workload | Threads | Atomic 0–32 | Exact-width inline Papaya | Boxed Papaya |
|---|---:|---:|---:|---:|
| 16-byte insert miss | 1 / 8 | **14.737 / 49.421** | 14.275 / 40.026 | 7.722 / 33.849 |
| 16-byte read hit | 1 / 8 | 19.408 / 124.749 | **20.815 / 158.986** | 12.417 / 97.642 |
| 16-byte update hit | 1 / 8 | 14.897 / 54.831 | **16.682 / 61.945** | 11.917 / 54.111 |
| 31-byte insert miss | 1 / 8 | **12.559 / 44.935** | 11.917 / 36.692 | 8.995 / 34.442 |
| 31-byte read hit | 1 / 8 | **15.417 / 111.403** | 11.736 / 107.140 | 10.157 / 85.002 |
| 31-byte update hit | 1 / 8 | **11.109 / 43.857** | 9.709 / 40.930 | 10.409 / **43.994** |

On exact 32-byte keys, splitting the tag domain changed insert/update results
by roughly -2% to +2% versus `AtomicFixed32`; eight-thread reads were 130.161
versus 121.751 Mops/s in the same 21-sample run. The mixed mode is therefore a
practical cache-key option, while the specialized exact mode remains available
when its full tag fingerprint is preferred.

#### Purpose-sized zero-through-16-byte atomic keys

The packed bucket implementation is now monomorphized by physical key width,
allowing `AtomicUpTo16` to use two atomic key words rather than four without
copying or forking the publication algorithm. It retains the elastic 1.10x
primary, ordered lazy overflow segments, in-band short length, disjoint
short/full-width tags, residual fallback, and lock-free initialization tests.

At 100K live entries, the new mode used **28.810 B/entry and 14 allocations**
for 8, 12, and 16-byte keys, versus the 0–32 mode at 46.851 and DashMap at
40.855–48.855. At one million entries it reached **28.250 B/entry**, while
DashMap used 60.438/64.438/68.438 for 8/12/16 bytes and boxed Papaya used
58.937/66.937 for 8/16. That is 53.3–58.7% less requested RAM than DashMap and
52.1–57.8% less than Papaya in the measured endpoints.

The focused operation results were also competitive; higher Mops/s is better:

| Key/workload | Threads | Atomic 0–16 | Atomic 0–32 | Exact-width inline Papaya | Boxed Papaya |
|---|---:|---:|---:|---:|---:|
| 8-byte insert miss | 1 / 8 | **17.127 / 50.838** | 15.075 / 50.389 | 15.825 / 42.208 | 7.965 / 34.980 |
| 8-byte read hit | 1 / 8 | 20.346 / 153.981 | 19.287 / 152.840 | **24.642 / 179.721** | 13.333 / 122.666 |
| 8-byte update hit | 1 / 8 | 17.576 / 56.930 | 15.385 / 55.820 | **20.432 / 63.465** | 10.752 / 54.775 |
| 16-byte insert miss | 1 / 8 | **17.000 / 48.918** | 12.795 / 48.781 | 13.629 / 40.323 | 8.048 / 33.715 |
| 16-byte read hit | 1 / 8 | **20.743 / 138.008** | 18.614 / 135.155 | 19.231 / **142.402** | 11.594 / 93.766 |
| 16-byte update hit | 1 / 8 | 16.898 / 57.687 | 14.655 / 54.989 | **17.686 / 59.202** | 10.657 / 53.285 |

After the shared 16/32-byte implementation was factored into one const-generic
publication core, a larger post-refactor validation used 300K resident keys,
3M operations, `gxhash`, and 21 alternating samples. At eight threads the
0–16 mode measured **46.269 M insert misses/s**, **108.900 M overlay read
hits/s**, and **57.691 M overlay updates/s**. The corresponding 0–32 results
were 38.910/88.328/52.150, exact-width inline Papaya was
39.458/80.698/57.226, and boxed Papaya was 29.048/68.275/47.445. Higher is
better. The same large new-key fixture measured 8.550 M inserts/s at one
thread, versus 7.627/7.401/5.138 for those controls. This is a validation
trace, not a replacement for the smaller focused table above: changing table
capacity and working-set size changes absolute throughput.

Successful delete remained in the same range as all controls. A forced
64-writer capacity-one test verifies lazy overflow and fallback publication for
the 16-byte physical instantiation. Re-running exact-32 after the generic
refactor measured 45.046 M inserts/s and 127.789 M read hits/s at eight threads,
within the surrounding unpinned ranges.

#### Integer-sized zero-through-8-byte atomic keys

`AtomicUpTo8` is a third monomorphization of the same reviewed publication
core, using one atomic key word per slot. Lengths zero through seven use the
in-band final byte, an 8-byte key remains raw, and disjoint tag domains prevent
an exact full-width key from aliasing a short physical encoding. Longer keys
fall back to the generic exact table. Dedicated tests cover every supported
length, the adversarial short/full encoding pair, rebuild/update/delete, the
9-byte fallback, and 64 racing writers that force lazy overflow publication.

At one million 8-byte keys it retained **19.230 B/entry in 14 allocations**.
That is 31.9% less than atomic 0–16 at 28.250, 58.5% less than atomic 0–32 at
46.290, 45.2% less than exact-width inline Papaya at 35.084, 67.4% less than
boxed Papaya at 58.937, and 68.2% less than DashMap at 60.438.

The complete 100K-key/300K-operation comparison below uses `gxhash` and 15
alternating samples. Values are Mops/s at one/eight threads; **higher is
better**. “Overlay” rows isolate keys inserted into the mutable layer, while
ordinary update/delete rows start with keys in the packed frozen generation.

| Workload | Atomic 0–8 | Atomic 0–16 | Atomic 0–32 | Inline Papaya | Boxed Papaya |
|---|---:|---:|---:|---:|---:|
| New-key insert | **25.688 / 50.832** | 20.835 / 48.317 | 15.717 / 48.188 | 18.314 / 46.158 | 8.993 / 34.801 |
| Existing-key insert | **22.279** / 64.745 | 21.635 / 65.431 | 21.466 / **66.070** | 21.965 / 61.826 | 21.790 / 60.728 |
| Overlay read hit | 26.784 / 162.889 | 21.759 / 151.257 | 21.090 / 137.491 | **27.460 / 174.381** | 17.493 / 115.488 |
| Overlay update hit | 19.239 / **65.759** | 18.451 / 61.124 | 15.455 / 61.092 | **23.084** / 65.034 | 15.281 / 64.550 |
| Read miss | 43.497 / 300.463 | 39.381 / 282.963 | 39.477 / 256.620 | **46.513 / 305.694** | 43.957 / 292.600 |
| Update miss | 25.753 / 79.695 | 25.446 / 77.505 | 24.862 / 74.321 | **31.944 / 85.824** | 29.926 / 84.812 |
| Delete hit | 29.305 / 59.468 | **31.521** / 61.706 | 29.135 / **62.837** | 29.228 / 60.330 | 30.955 / 60.772 |
| Delete miss | 25.436 / 81.731 | 24.900 / 80.029 | 24.690 / 81.468 | **25.784** / 77.375 | 25.551 / **82.125** |
| One hot update key | 57.977 / 7.838 | 57.780 / 6.640 | **58.013 / 9.573** | 57.219 / 6.903 | 57.730 / 7.760 |
| Forced 100x overflow | 4.321 / 16.507 | 4.115 / 15.675 | 4.066 / 15.891 | **12.301 / 24.174** | 6.349 / 20.725 |

The result is a strong default candidate when the caller can bound keys to
eight bytes: it is simultaneously the density leader and the new-key insert
leader, with competitive steady operations. The deliberate 100x
under-provisioning case is the clear exception because every miss must exhaust
the fixed atomic tiers before reaching the growing fallback. Callers expecting
that degree of unbounded growth should choose a dynamic Papaya mode or rebuild
with realistic overlay capacity.

#### Learned adaptive mixed-key cache mode

`AtomicAdaptive` removes the caller-supplied maximum-key requirement. It first
stores a 64-to-4,096-key exact sample in a native append-only stable-cell table,
learns the observed 8/16/24/32/exact-48/residual distribution, then publishes
proportionally sized atomic classes that share one total capacity. Exact
48-byte keys use a six-word atomic class; other long keys use a second native
arbitrary-length table. Separate bounded two-probe filters identify possible
sample/emergency and residual keys. The sampling gate drains before table
publication, preventing cross-destination duplicate keys; steady state is
lock-free, while the one-time handoff can briefly spin arriving writers. The
sample remains live after publication and exposes exact stable prepared-read
slots; only emergency saturation spills to Papaya.

At one million mixed keys (40% 8, 25% 16, 15% 24, 10% 32, 10% 48 bytes), it
used **35.639 B/entry**, versus atomic-32 at 56.653, Papaya at 69.737, and
DashMap at 71.238. It therefore saved 37.1%, 48.9%, and 50.0% respectively.
An all-8-byte fixture was 19.515 versus the explicit specialization's 19.230;
an all-48-byte fixture was 99.137 versus Papaya's 98.937.

The later retained 1.15x/elastic-overflow pass replaced the inline Papaya
48-byte class with the exact atomic class. On the same one-million-entry mixed
distribution, requested memory fell from 34.397 to **33.074 B/entry** and
allocations from 107,822 to **8,229**. Full-turnover memory fell from 68.556 to
65.934 B/live entry. Mixed one-thread read/update/cache/insert throughput stayed
within -2.7% to +0.3% of the inline-48 build; exact-48 reads traded 16.1% for
13.6% faster updates and 2.9% faster inserts. This is retained as a mixed-cache
density optimization, not an exact-48 read optimization.

With capacity including distinct insert churn until rebuild, the 90%-read
mixed cache trace measured 12.965/25.183 Mops/s at one/eight threads, and the
95%-read trace measured 15.995/80.964. DashMap measured 22.225/18.272 and
20.549/66.205 respectively. Higher is better: adaptive wins both eight-thread
traces but not single-thread latency. Learned insert reached 12.884/37.490
versus DashMap 14.848/42.593; cold fill, which includes learning and
publication, was materially slower at 12.095/19.935 versus 22.278/37.151.

The detailed design, exact capacity rule, and FerricStore recommendation are
in [`ADAPTIVE_CACHE.md`](ADAPTIVE_CACHE.md).

A dedicated inline-key Papaya overflow tier was also measured and rejected.
It reduced the 100K-entry allocation median from 363 to 186, but its larger
inline nodes raised retained memory from 52.042 to 52.192 B/entry. Focused
new-key insertion fell from 13.899/55.999 to 12.924/54.337 Mops/s at 1/8
threads. The packed atomic second tier replaces this rejected layout; only its
residual spill continues to the boxed generic fallback.

#### Rejected: independently atomic control bytes

A same-RAM alternative replaced each bucket's one atomic 64-bit control word
with eight independently atomic bytes. Publication then needed a byte CAS plus
a release store instead of two whole-word read-modify-write operations, and
different slots no longer interfered at the CAS. The slot count, key/value
layout, load target, and total control bytes were unchanged.

The 100K-base/200K-new-key, 21-sample conditional-insert probe showed why this
was not retained. **Higher Mops/s is better**:

| Path | Packed control word | Atomic bytes | Change |
|---|---:|---:|---:|
| Point `get_or_insert`, 1 thread | **21.182** | 19.156 | -9.6% |
| Point `get_or_insert`, 8 threads | 52.479 | **53.618** | +2.2% |
| Guarded `get_or_insert`, 1 thread | 23.572 | **23.748** | +0.7% |
| Guarded `get_or_insert`, 8 threads | 57.051 | **57.170** | +0.2% |

The broader eight-thread operation matrix confirmed the tradeoff. Atomic
bytes improved read miss from 287.253 to 299.906 Mops/s (+4.4%), update miss
from 81.756 to 85.412 (+4.5%), and the contended hot-key update from 4.638 to
4.748 (+2.4%). They regressed overlay read hit from 105.750 to 89.017 Mops/s
(-15.8%), ordinary read hit by 5.0%, insertion paths by 2.3-3.7%, update hit by
5.2%, and successful delete by 3.8%. A 100K-entry allocation probe measured
52.395 versus 52.409 B/entry; the 0.03% difference was overflow/hash-run noise,
as both layouts reserve exactly one control byte per slot.

The rejected design exchanged one acquired bucket load and SIMD-style byte
matching for up to eight acquired byte loads. Reduced publication interference
was too small to repay that read and scan cost, even at eight writers. The
packed whole-word control remains the production layout.

### Whole-word control scanning

The next pass replaced the scalar eight-slot control loop with a branch-light
whole-word byte-match mask. Readers now treat an unpublished `writing` slot as
the logical end of the bucket: the read can linearize before that pending
insertion instead of spinning on an unrelated key. Writers retain the stronger
rule and wait before concluding that a key is absent, preventing duplicate
publication.

The old scalar scanner was temporarily retained as an alternating-order
runtime control and then removed from the production path. At 300,000 resident
overlay entries, eight threads, and 31 samples, **higher throughput is better**:

| Operation | Whole-word scan | Scalar scan | Difference |
|---|---:|---:|---:|
| Overlay read hit | **87.567** | 69.163 | **+26.6%** |
| Overlay update hit | **47.991** | 45.121 | **+6.4%** |
| Frozen-base read hit, 30M operations | 62.083 | **62.251** | -0.3% |

At one thread, the whole-word scan improved overlay reads from 7.253 to 9.374
Mops/s (+29.2%) and overlay updates from 6.148 to 6.945 Mops/s (+13.0%). A
15-sample eight-thread insertion control was effectively flat at 41.088 versus
41.623 Mops/s (-1.3%), while deliberate overflow was also flat at 20.763
versus 20.818 Mops/s (-0.3%). An alternating comparison against the other
overlay implementations placed whole-word atomic reads within 1.1% of inline
Papaya (80.571 versus 81.449 Mops/s).

This optimization adds no per-entry metadata or allocation. Unit tests cover
all byte states in the packed control word and prove that a reader can still
find an older published slot while an adjacent slot is being initialized.

## PHast+ frozen-index experiment

The optional `phast` feature now permits paired experiments between the normal
PtrHash frozen index and PHast+ from the 2025 PHast paper. PtrHash remains the
default. Both backends use the same packed key arena, dense atomic values,
embedded key fingerprint, writer routing, overlay, and exact byte verification.

An isolated PHast+ index initially looked 20–30% faster for member lookups, but
the complete generation did not preserve that advantage. At one million keys,
eight threads, five million operations, and 25 alternating paired samples, the
95%-read cache trace measured:

| Frozen index | Mops/s | Paired change vs PtrHash |
|---|---:|---:|
| **PtrHash** | **67.452** | baseline |
| PHast+ | 64.282 | -2.0% |

The complete retained-base and construction comparison also showed that the
index-density difference is too small to matter at map level. **Lower build,
rebuild, bytes, and bits are better.**

| Entries | Backend | Build ns/entry | Rebuild ns/entry | Base B/entry | Index bits/entry |
|---:|---|---:|---:|---:|---:|
| 300,000 | PtrHash | **105.115** | 132.200 | 48.507 | 2.990 |
| 300,000 | PHast+ | 117.180 | **123.607** | **48.484** | **2.805** |
| 1,000,000 | PtrHash | **134.004** | 164.090 | 48.433 | 2.990 |
| 1,000,000 | PHast+ | 137.082 | **146.657** | **48.406** | **2.777** |

PHast+ therefore saves only 0.027 byte/key at one million entries—about 27 KB
total—while rebuilding 10.6% faster. Construction is 2.3% slower, and the
cache workload is 2.0% slower. This is not enough to replace PtrHash.

PHast+'s main weakness was negative lookup. Pairing it with the existing
one-byte blocked filter raised one eight-thread miss run from 229.794 to
337.360 Mops/s, but added one byte/key and did not create a stable cache-mix
win; a paired run was only +1.8%. Temporary two-bit and four-bit variants were
also measured and removed:

- two bits/key lost 10.9% on the paired cache trace;
- four bits/key lost 20.7% on read miss, 30.7% on update miss, 45.1% on delete
  miss, and 4.1% on the complete cache trace, while adding 0.5 byte/key.

The result is a useful rejection: PHast+ remains an explicit experimental
backend for further research, but PtrHash remains the production default and
the failed sub-byte filter policies are not part of the API.

## Non-minimal k-PHF cache-line experiment

A July 2026 paper on non-minimal k-perfect hashing proposes mapping every key
to a bounded cache-line bin. Its reference `u64` set can compare all inline
keys in that line directly, and reports large-table gains once a 1-PHF no
longer fits in cache. PackedGen implemented an independent safe-Rust
prototype for exact 32-byte keys:

- deterministic PtrHash-style pilot construction with recursive bump fallback;
- eight packed key references in one aligned 64-byte line;
- eight atomic values in a parallel aligned line;
- SIMD comparison of the embedded nine-bit fingerprints;
- exact packed-arena byte verification for every candidate.

The prototype is retained only behind the `kphf` experiment feature. At one
million keys, eight threads, and 15 alternating samples, **higher throughput
and lower bytes are better**:

| Backend | B/entry | Index bits/entry | Build ns/entry | Hit Mops/s | Miss Mops/s |
|---|---:|---:|---:|---:|---:|
| **PtrHash atomic** | 48.433 | 2.990 | **135.945** | **132.648** | **408.531** |
| k-PHF, `k=8` | **48.324** | **0.897** | 1,809.611 | 124.249 | 265.137 |

Paired changes were -4.3% for hits and -28.9% for misses. At five million
keys, the k-PHF map reached 48.265 B/entry versus PtrHash's 48.401, but paired
hits were -15.7% and misses -20.9%. The safe constructor remained roughly
12× slower.

Two additional geometries were measured and rejected at one million keys:

| Geometry | B/entry | Hit change | Miss change |
|---|---:|---:|---:|
| `k=4`, half-line key/value arrays | 48.417 | -5.4% | -29.0% |
| `k=4`, one-line interleaved entries | 48.417 | -10.1% | -36.5% |

This explains why the paper's result does not directly transfer. Inline
`u64` sets finish membership testing in the routed line; PackedGen must
route, scan fingerprints, and sometimes fetch separate packed key bytes.
The metadata reduction is real, but only saves 0.1–0.14 byte/key at scale and
does not repay the extra candidate scan. The k-PHF is therefore not being
promoted into the generation backend.

## Duplicate-hash schedule

The original PtrHash generation hashed a 32-byte key once for writer
stripe/overlay routing and again for the frozen 128-bit digest. The optional
`shared-gx` feature carries one 16-byte Gx digest through writer routing,
prehashed overlay access, frozen construction, exact PtrHash lookup, and
rebuild de-duplication.

The current portable default uses a different retained solution: two
independently randomized Rapidhash lanes are combined into the carried 128-bit
context. This preserves the frozen index's full digest domain while avoiding a
second hashing algorithm and a third key scan. It replaced the earlier
two-FoldHash-lane checkpoint after preserved-binary cache A/B. Current
cache-level results are in `CACHE_PROOF.md`; the shared-Gx comparison below is
retained as historical evidence rather than the present default decision.

The first implementation used `Option<u128>` and inflated the carried context
to roughly 48 bytes. Collapsing the feature-enabled representation to exactly
one `u128` was necessary before it became competitive. A size test now locks
that invariant.

The full one-thread, 100,000-entry operation matrix improved most base-touching
operations, but not the pure new-key path. **Higher throughput is better.**

| Operation | Shared Gx change vs split schedule |
|---|---:|
| Read hit | +5.1% |
| Read miss | +11.1% |
| Insert existing | +1.1% |
| Insert new | **-14.8%** |
| Update hit | +0.4% |
| Update miss | +17.4% |
| One hot-key update | +9.3% |
| Delete hit | +8.3% |
| Delete miss | +6.2% |
| 95%-read cache mix | +10.9% |

The eight-thread result was mixed even after fixing the Gx seed so both
versions built the same frozen digest domain: read hit/miss and distributed
update improved, while insert hit, delete hit/miss, and the complete cache mix
regressed. The cache mix was 24.5% slower in that paired run. Unpinned
high-core results remained variable enough that this cannot replace the
default.

The updated isolated hash probe also invalidated the earlier additive-cost
assumption. **Lower nanoseconds are better.**

| Hash schedule | 1T ns/op | 1T Mops/s | 8T ns/op | 8T Mops/s |
|---|---:|---:|---:|---:|
| FoldHash route only | **6.056** | **165.117** | **0.884** | **1,130.927** |
| Gx128 only | 8.977 | 111.397 | 1.195 | 836.740 |
| Split route + Gx128 | 7.735 | 129.290 | 1.036 | 965.357 |
| Shared Gx128, low-64 route | 9.799 | 102.050 | 1.329 | 752.526 |
| Shared XXH3-128, low-64 route | 12.485 | 80.097 | 1.602 | 624.085 |

Compiler overlap makes the split row cheaper than naively adding the first two
rows, so the complete map matrix is the authority. XXH3-128 was slower and was
not integrated. Shared Gx is retained as a complete opt-in implementation for
future pinned work, but it is not enabled by `gxhash` alone.

### Tiny exact linear fallback

Randomized shared-Gx construction exposed a PtrHash 2.0.1 small-set edge case:
some two-key digest layouts triggered an internal bounds-precondition abort.
Frozen maps with at most eight entries now use an exact linear representation,
verify duplicate input bytes during construction, and skip perfect-hash
metadata entirely. A 256-seed atomic-generation regression test covers short,
long, hit, and miss semantics. Large maps are unchanged.

### Shared atomic slot-cache rejection

An adaptive direct-mapped cache of exact frozen slots was also tested. It used
512 entries, or 8 KiB / 0.082 byte per key, for the 100,000-key fixture and
verified original key bytes on every cache hit. Uniform operations showed no
complete cache-mix win. On a new locality fixture with 90% of reads targeting
1% of keys, the cache was 12.2% slower at one thread and 69.7% slower at eight
threads. Concurrent cache fills created more coherence traffic than the saved
PtrHash work. The implementation was removed; the hot-set workload remains in
the benchmark suite.

An exact thread-local variant removed the shared coherence point and used a
fixed 8 KiB per participating thread. It still lost 13.9% at one thread and
37.7% at eight threads on the same 90%-on-1% hot-set trace. TLS access, cache
probing, and exact slot-byte verification cost more than the saved PtrHash
metadata lookup. That implementation was also removed.

### Prepared exact hot-key handles

The retained `prepared-keys` feature attacks locality without an implicit
cache. `prepare_key` returns a 16-byte opaque handle containing writer-stripe
routing and a generation-specific dense slot. `get_prepared`, `update_prepared`,
`insert_prepared`, and `remove_prepared` use the slot only when:

- the current writer stripe still permits direct-base access;
- the frozen-base identity matches;
- the original caller-supplied key bytes exactly match the stored key.

Wrong-key, cross-map, overlay, deleted, and post-rebuild handles fall back to
the ordinary exact operation. `AtomicPreparedKey::fallback` lets a mixed batch
mark ordinary keys without retaining handles for them. Concurrent prepared
updates remain ordered through repeated rebuilds in the cutover stress test.

Preparing the hottest 1% of a 100,000-key map costs 24 KiB, or 0.24 byte per
total key. Preparation measured 21.125 ns/handle. On a 100%-prepared hot read,
normal lookup took 23.038 ns and prepared lookup 12.209 ns (+88.7%
throughput), so preparation amortized after 1.95 reads. At one million keys
the corresponding figures were 20.504 ns preparation, 30.185 ns normal read,
16.912 ns prepared read, and 1.55 reads to break even.

On the complete trace where 90% of reads target the prepared 1% hot set,
**higher Mops/s is better**:

| Threads | Normal atomic | Prepared atomic | Paired change | DashMap |
|---:|---:|---:|---:|---:|
| 1 | 33.044 | 49.226 | **+50.9%** | **50.964** |
| 2 | 65.379 | **98.778** | **+49.2%** | 55.653 |
| 4 | 136.885 | **194.936** | **+40.6%** | 80.326 |
| 8 | 272.524 | **380.798** | **+42.0%** | 100.459 |

Prepared reads nearly match DashMap at one thread, then lead by 1.78×, 2.43×,
and 3.79× at two, four, and eight threads. Uniform read misses changed by only
−0.7% to −2.3% in the paired matrix because unprepared keys use the normal
path.

Prepared hot-set updates also improved the normal atomic path:

| Threads | Normal atomic | Prepared atomic | Paired change | DashMap |
|---:|---:|---:|---:|---:|
| 1 | 27.915 | 36.138 | **+28.3%** | **50.418** |
| 2 | 35.235 | **42.781** | **+14.9%** | 42.677 |
| 4 | 45.204 | **52.244** | **+17.7%** | 49.582 |
| 8 | 70.853 | **73.989** | **+7.6%** | 56.592 |

Prepared insert/replace hits were a smaller win: +9.4%, −0.3%, +1.5%, and
+4.6% at one, two, four, and eight threads. The feature is therefore useful
for explicitly identified hot keys, not a replacement for normal lookups or a
reason to retain handles for every key.

#### Allocation-free prepared batches

`get_prepared_batch` accepts caller-owned key, handle, and output slices. It
pins one current generation for the batch, verifies every original key, and
allocates nothing. If a rebuild publishes during the call, it detects the
generation change and refreshes every result through the new generation before
returning. `prepare_key_batch` similarly refreshes a caller-owned handle set.

For latency, **lower ns/read is better**. For throughput and percentage gain,
**higher is better**. In the 31-sample 100%-prepared probe:

| Entries | Scalar prepared | Batch 16 | DashMap | Batch vs scalar |
|---:|---:|---:|---:|---:|
| 100,000 | 11.384 ns | 9.574 ns | **9.176 ns** | **+18.9% throughput** |
| 1,000,000 | 13.769 ns | **11.258 ns** | 13.053 ns | **+22.3% throughput** |

Larger batches reached 9.037 ns at 100,000 entries, but did not generalize to
the million-entry fixture. Batch 16 is retained as the conservative concurrent
probe size.

A later base-snapshot pass removed one immutable-generation base load per
item. The batch now pins that base once, verifies every original key and stripe
state, and takes the existing exact fallback for stale or shadowed handles. In
the cleaner 100K-entry repetition, batch-64 improved from 9.420 to 5.436
ns/read, or 106.2 to 184.0 Mops/s. Direct Papaya measured 142.6 Mops/s in the
same alternating run. Repeated-rebuild and stale-handle tests remain exact.

On the complete 90%-on-1% hot-set trace, including mixed-batch assembly,
**higher Mops/s is better**:

| Threads | Normal atomic | Scalar prepared | Batch 16 | DashMap |
|---:|---:|---:|---:|---:|
| 1 | 29.068 | 40.779 | **48.713** | 44.032 |
| 2 | 62.138 | 89.649 | **102.174** | 57.264 |
| 4 | 128.872 | 175.386 | **212.291** | 74.947 |
| 8 | 271.308 | 372.387 | **416.942** | 100.497 |

Batching adds 19.5%, 14.0%, 21.0%, and 12.0% over scalar prepared reads at
one, two, four, and eight threads. It leads DashMap by 1.11×, 1.78×, 2.83×,
and 4.15× respectively in this trace.

Bulk handle refresh lowers preparation from 21.125 to 16.208 ns/handle at
100,000 entries and from 20.504 to 16.217 ns at one million entries. Because
lower preparation latency is better, these correspond to 30.3% and 26.4%
higher preparation throughput.

#### Prepared update and replacement batches

The ordinary `prepared-keys` implementation shares one generation snapshot
but retains a striped writer pin per item. That zero-extra-state version
improves update throughput over scalar prepared operations by 28.9%, 38.0%,
1.5%, and 3.4% at one, two, four, and eight threads. Replacement batching
improves by 16.2%, 9.3%, 4.4%, and 3.8%.

A second experiment replaced those per-item pins with one shared generation
gate per batch. It helped through four threads, but at eight threads the single
cache line reduced update throughput from 72.613 to 54.724 Mops/s, a 24.6%
loss. That single-gate layout was rejected.

The retained `prepared-batch-gate` feature instead uses 16 cache-line-separated
gates. A batch selects one gate from its first direct handle, while rebuild
closes all gates before closing writer stripes. Exact wrong-key, stale,
cross-map, overlay, and rebuild fallback semantics remain unchanged.

For these tables, **higher Mops/s is better**:

| Update threads | Scalar prepared | Sharded-gate batch 16 | DashMap | Batch vs scalar |
|---:|---:|---:|---:|---:|
| 1 | 30.186 | 42.203 | **44.958** | **+39.8%** |
| 2 | 41.356 | **62.851** | 42.952 | **+52.0%** |
| 4 | 51.327 | **85.426** | 49.022 | **+66.4%** |
| 8 | 70.369 | **107.078** | 53.675 | **+52.2%** |

| Replacement threads | Scalar prepared | Sharded-gate batch 16 | DashMap | Batch vs scalar |
|---:|---:|---:|---:|---:|
| 1 | 33.526 | **41.651** | 19.383 | **+24.2%** |
| 2 | 40.016 | **57.369** | 12.572 | **+43.4%** |
| 4 | 60.827 | **78.834** | 33.230 | **+29.6%** |
| 8 | 75.658 | **108.648** | 44.254 | **+43.6%** |

The same pass replaced the remaining prepared load-plus-CAS pins with the
single fetch-and-backout acquisition and reused one pinned base per sharded
batch. Against the immediately preceding binary, update batch-2/256 increased
from 67.2/99.7 to 75.6/111.3 Mops/s. Replacement batch-2/256 increased from
67.3/100.2 to 74.8/118.8. Direct Papaya measured 25.0 and 23.4 Mops/s for the
respective scalar update and replacement controls; these operations require
Papaya to accept an owned candidate key. Higher throughput is better.

The gate storage measured 1,040 requested bytes and one allocation per live
generation: 0.0104 byte/key at 100,000 entries and about 0.001 byte/key at one
million. With no active batch writers, median writer redirection changed from
3.958 to 4.041 us. Under eight continuous batch writers it increased from
10.666 to 22.250 us, while background construction remained 15.314 versus
15.415 ms. The feature is therefore opt-in for write-heavy batched workloads;
read-focused users can retain the ordinary prepared path.

The cache needs the exact displaced handle, not merely the new value reported
by the older update batch. `replace_prepared_batch` now provides that update-only
primitive: deleted keys stay deleted and its output contains the exact old
value from each successful atomic swap. Wrong-key, cross-map, overlay, deleted,
stale, and rebuild-transition cases use the ordinary exact fallback. A
four-writer stress test checks the returned old value across eight concurrent
rebuilds in both prepared write modes.

For the portable mode at one thread, higher Mops/s is better:

| Replacement fixture | Scalar prepared | Exact batch 64 | DashMap | Papaya | Batch vs scalar |
|---|---:|---:|---:|---:|---:|
| 100K entries, 1K hot | 61.814 | **92.294** | 67.066 | 27.327 | **+49.3%** |
| 1M entries, 10K hot | 53.730 | **62.574** | 43.861 | 22.404 | **+16.5%** |

This primitive adds no map or per-key state. The cache-level wrapper owns and
reuses its bounded input/output scratch only while that explicit pipeline is
alive.

## Reproduce

```text
cargo run --release --example concurrent_map_scaling_probe -- \
  100000 3000000 16 11 64 read_hit
cargo run --release --example concurrent_map_scaling_probe -- \
  100000 3000000 16 7 64 read_miss
cargo run --release --example concurrent_map_scaling_probe -- \
  100000 3000000 16 7 64 cache_mix_95rh2rm2u0.5i0.5d
cargo run --release --example concurrent_map_scaling_probe -- \
  100000 300000 16 7 64

cargo run --release --example memory_probe -- \
  lockfree-atomic-generation-fingerprint-binary 1000000
cargo run --release --example memory_probe -- dashmap-binary 1000000
cargo run --release --example memory_probe -- lockfree-binary 1000000
cargo run --release --example memory_probe -- scc-binary 1000000
cargo run --release --example memory_probe -- flurry-binary 1000000
cargo run --release --example memory_probe -- rwlock-hashbrown-binary 1000000

cargo run --release --features gxhash --example atomic_overlay_probe -- \
  300000 3000000 8 31 32 overlay_read_hit
cargo run --release --features gxhash --example atomic_overlay_probe -- \
  300000 3000000 8 31 32 overlay_update_hit

cargo run --release --features gxhash,phast \
  --example concurrent_map_scaling_probe -- \
  1000000 5000000 8 25 64 cache_mix_95rh2rm2u0.5i0.5d ptr-phast 8
cargo run --release --features gxhash,phast \
  --example atomic_index_backend_probe -- 1000000 9

cargo run --release --features gxhash,kphf \
  --example kphf_frozen_probe -- 1000000 5000000 8 15 1
cargo run --release --features gxhash \
  --example hash_schedule_probe -- 1000000 30000000 8 15
cargo run --release --features gxhash,shared-gx \
  --example concurrent_map_scaling_probe -- \
  100000 1000000 8 15 64 all packedgen-atomic-ptrhash 8
cargo run --release --features gxhash,prepared-keys \
  --example concurrent_map_scaling_probe -- \
  100000 2000000 8 31 64 read_hotset_90pct_on_1pct prepared-compare
cargo run --release --features gxhash,prepared-keys \
  --example prepared_key_probe -- 100000 1000 5000000 15
cargo run --release --features gxhash,prepared-keys \
  --example prepared_batch_probe -- 1000000 10000 2000000 31
cargo run --release --features gxhash,prepared-keys \
  --example prepared_batch_probe -- 100000 1000 2000000 31 update
cargo run --release --features gxhash,prepared-batch-gate \
  --example concurrent_map_scaling_probe -- \
  100000 1000000 8 31 64 update_hotset_90pct_on_1pct prepared-compare
cargo run --release --features gxhash,prepared-batch-gate \
  --example concurrent_map_scaling_probe -- \
  100000 1000000 8 31 64 insert_hotset_90pct_on_1pct prepared-compare
cargo run --release --features gxhash,prepared-batch-gate \
  --example prepared_batch_rebuild_probe -- 100000 1000 8 15
```

Official implementation notes used to select and interpret the controls:

- [DashMap documentation](https://docs.rs/dashmap/6.2.1/dashmap/)
- [Papaya documentation](https://docs.rs/papaya/0.2.4/papaya/)
- [`scc::HashMap` documentation](https://docs.rs/scc/3.8.5/scc/hash_map/struct.HashMap.html)
- [Flurry documentation](https://docs.rs/flurry/0.5.2/flurry/)
- [HashBrown documentation](https://docs.rs/hashbrown/0.17.1/hashbrown/struct.HashMap.html)
- [Non-minimal k-perfect hashing paper](https://arxiv.org/abs/2607.07257)
- [Authors' k-PHF reference implementation](https://github.com/RagnarGrootKoerkamp/static-hash-sets)
