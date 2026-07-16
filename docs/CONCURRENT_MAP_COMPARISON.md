# Rust concurrent-map comparison

Date: 2026-07-16

This comparison asks two separate questions:

1. How many live 32-byte-key records fit in RAM?
2. What happens to point-operation throughput as more worker threads run?

It compares the packed atomic PackedGen generation with DashMap 6.2.1,
Papaya 0.2.4, `scc` 3.8.5, Flurry 0.5.2, and the common baseline of HashBrown
0.17.1 behind one `parking_lot::RwLock`. All implementations use the same
FoldHash builder, 32-byte binary keys, `u64` values, pre-sized tables, and
identical operation traces. DashMap uses 64 shards.

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

The one-hot-key row is an intentionally pathological write-contention test,
not the normal cache pattern. It measures every thread repeatedly changing the
same value. PackedGen and Papaya use compare-and-swap replacement there and
lose heavily to `scc`'s bucket serialization and DashMap's shard lock.

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

The retained PtrHash generation currently hashes a 32-byte key once for writer
stripe/overlay routing and again for the frozen 128-bit digest. The optional
`shared-gx` feature now carries one 16-byte Gx digest through writer routing,
prehashed overlay access, frozen construction, exact PtrHash lookup, and
rebuild de-duplication. The default remains the split FoldHash + Gx schedule.

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
cache. `prepare_key` returns a 24-byte opaque handle containing writer routing
and a generation-specific dense slot. `get_prepared`, `update_prepared`,
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

The gate storage measured 1,040 requested bytes and one allocation per live
generation: 0.0104 byte/key at 100,000 entries and about 0.001 byte/key at one
million. With no active batch writers, median writer redirection changed from
3.958 to 4.041 us. Under eight continuous batch writers it increased from
10.666 to 22.250 us, while background construction remained 15.314 versus
15.415 ms. The feature is therefore opt-in for write-heavy batched workloads;
read-focused users can retain the ordinary prepared path.

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
