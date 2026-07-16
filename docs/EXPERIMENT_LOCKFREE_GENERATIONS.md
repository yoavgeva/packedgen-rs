# Lock-free packed-generation experiment

Date: 2026-07-15

## Outcome

The repository now has four safe concurrent prototypes:

- `LockFreeBinaryMap`: a fully dynamic Papaya control;
- `LockFreeHybridMap`: an immutable packed `FrozenPackedMap` plus a Papaya
  overlay, with lock-free reads and point writes but owner-managed rebuilds;
- `LockFreeGenerationMap`: recursively layered packed generations with ArcSwap
  publication and generic stable value cells;
- `LockFreeAtomicU64GenerationMap`: the same online-generation owner with
  allocation-free direct atomic values in frozen perfect-hash slots.

No point operation acquires a library-owned lock. Rebuild calls use a mutex only
to prevent two aggregate maintenance jobs from publishing simultaneously.
Point-operation progress otherwise inherits Papaya's documented lock-free
operations plus native atomic-word progress for direct slots. This crate still
contains no unsafe code.

The implementation relies on the documented safety/progress contracts of
[Papaya 0.2.4](https://docs.rs/papaya/0.2.4/papaya/) and
[ArcSwap](https://docs.rs/arc-swap/latest/arc_swap/). Papaya owns epoch
reclamation and retrying point mutations; ArcSwap protects published layer and
base lifetimes.

This reaches the point-operation lock-free goal. It does not make whole-table
rebuild wait-free: a nonstop stream of writers can starve stripe closure while
the point writers themselves continue.

## Layered read path

The generic steady-state packed lookup performs:

1. one lock-free ArcSwap load of the active layer;
2. one pinned Papaya overlay lookup;
3. on overlay miss only, one immutable `FrozenPackedMap` lookup;
4. exact original-key verification before returning a base value.

During the short redirection/build interval, an overlay miss may recurse into
the previous stable layer. Once the equivalent frozen base is installed, the
old layer is reclaimed after existing ArcSwap readers release it and lookup
depth returns to one.

`Present(value)` overrides the base and `Deleted` hides a base record. A reader
that began before publication may finish against the old layer; a later reader
may use the new layer. Both paths preserve exact key semantics.

The atomic specialization keeps frozen keys in perfect-hash order and stores
one `AtomicU64` value at each matching dense slot. The key-only frozen slot plus
atomic value occupies the same 16 bytes as the generic packed
key-reference/value slot. `u64::MAX` is reserved as the deletion marker,
exposed through the checked `NonMaxU64` value type. Existing frozen keys
therefore update, reinsert, and delete without allocating an overlay key or
value record. Only keys absent from the frozen generation require Papaya. For
the common 32-byte binary-key case, the atomic generation stores the key inline
in the Papaya node instead of retaining a separate `Box<[u8]>`. An explicit
`CompactSized` mode provides the same layout for exact 8, 16, 24, 31, 40, 48,
56, or 64-byte workloads. Unexpected lengths use the boxed-key fallback, so the
map remains general.

Direct atomic generations also retain a permanent `overlay may shadow base`
bit in every writer stripe. Once a stripe has a direct frozen base and has
never promoted a base key into its overlay, base hits can safely run before the
overlay lookup. A bit is set before any base override is published, survives
direct-base activation, and is never cleared within that generation. Dirty
stripes retain overlay-first ordering. This applies to reads and mutations;
new overlay keys still fall through from a definite base miss.

## Why naive layering was wrong

The first layered prototype immediately sent every new writer to the new
overlay while already-pinned writers finished on the predecessor. A contention
test found a lost increment: an old-layer and new-layer update of the same key
could both read the same value and publish the same successor.

That version was rejected. The test observed 19,999 increments instead of
20,000 and now remains as a repeated cutover regression test.

## Exact striped handoff

Every generation owns 4,096 atomic writer stripes. Equal keys use the same
stripe. Each stripe contains a writer count, closed bit, direct-base bit, and
permanent overlay-shadow bit.

Online rebuild now works as follows:

1. publish a new empty overlay whose base points to the previous generation;
2. while a predecessor stripe is open, point writers for that stripe continue
   writing to the predecessor;
3. rebuild closes each predecessor stripe only with an atomic `0 -> CLOSED`
   transition; a busy stripe remains open and its writers keep progressing;
4. after a stripe closes, future writers for that stripe use the new overlay;
5. after all stripes close, the predecessor is immutable and its complete
   logical state is packed while readers and writers continue;
6. atomically replace the `Previous` base with the exactly equivalent frozen
   base—without copying, stopping, or replaying the active overlay;
7. for the atomic specialization, switch each new-layer stripe from overlay
   mode to direct-base mode only at zero active writers;
8. reclaim the old generation after pinned readers release it.

There is never an old-layer and new-layer writer for the same stripe. This
orders conflicting updates without a global writer gate. Stripe collisions are
safe; they only make handoff coarser. Maintenance can starve if one stripe never
reaches zero writers, but this does not block those writers or unrelated point
operations.

If frozen construction fails, the new overlay remains correct and continues to
reference the previous layer. A later successful rebuild compacts the extra
depth. `GenerationMapStats::layer_depth` exposes that state.

The direct-base activation barrier matters. Publishing a mutable atomic base
and immediately using it would let an already-pinned writer create an overlay
record while a later writer changed the same frozen slot. The second per-stripe
transition prevents that cross-location race. A stronger mixed
insert/delete/rebuild test also found that the new layer originally copied its
starting `len` before predecessor stripes completed. The fixed protocol records
the predecessor's stable length after all stripes close, while new-layer
mutations remain in 64 cache-line-separated length deltas.

## Online rebuild measurement

One million 32-byte keys, 1% preexisting overlay, four optional writers,
`gxhash`, Apple M4 Max. Values are medians from three fresh runs:

| Writers | Writer working set | Point writes during build | Total rebuild | Stripe handoff | Background build | Base publish |
|---:|---:|---:|---:|---:|---:|---:|
| 0 | 0 | 0 | 178.043 ms | 3.875 us | 177.624 ms | 15.584 us |
| 4 | 10,000 keys | 1,481,994 | 186.684 ms | 13.334 us | 186.175 ms | 2.125 us |
| 4 | 1,000,000 keys | 634,730 | 183.663 ms | 27.667 us | 182.957 ms | 2.542 us |

The earlier exact snapshot/delta-replay version paused writers for about 2.3 ms
with a 10,000-key changed set and **522 ms** when 518,439 distinct keys changed.
Layering eliminates that replay and its linear pause. The whole-table sweep is
now similar to the bounded-hot-set case.

One whole-table run took 3.597 ms to close a busy stripe. That is maintenance
delay, not a global writer pause: operations kept using the open predecessor
stripe. The probe still needs per-operation latency histograms and CPU pinning
before this can become a p99 claim.

Reproduce:

```text
cargo run --quiet --release --features gxhash --example \
  generation_rebuild_probe -- 1000000 1 0
cargo run --quiet --release --features gxhash --example \
  generation_rebuild_probe -- 1000000 1 4 10000
cargo run --quiet --release --features gxhash --example \
  generation_rebuild_probe -- 1000000 1 4 1000000
```

## RAM

One million 32-byte binary keys with `u64` values:

| Backend | Requested B/entry | Allocations | Interpretation |
|---|---:|---:|---|
| Atomic packed generation, empty | **48.761** | 11,401 | direct mutable frozen slots |
| Atomic packed generation, 1% existing-key churn | **48.761** | 11,402 | zero overlay records |
| Generic packed generation, 1% overlay | 49.561 | 41,409 | stable `Arc` value cells |
| Direct packed lock-free hybrid, 1% overlay | 49.523 | 41,394 | no online generation owner |
| Concurrent SegmentedSwiss, 64 shards | 54.309 | 4,557 | lock-based mutable RAM control |
| Papaya with boxed binary keys | 74.893 | 2,000,006 | fully dynamic lock-free control |
| DashMap with boxed binary keys | 84.438 | 1,000,066 | established concurrent baseline |

The atomic generation uses 42.3% less requested memory than DashMap in this
fixture, and mutating 1% of existing keys adds no retained overlay records. The
4,096 writer states and 64 padded length deltas cost about 0.038 B/entry at one
million entries.
Requested allocations are not RSS; allocator overhead, temporary generations,
and retired epoch records still require a pinned RSS experiment.

### New-key overlay layout

One million newly inserted 32-byte keys, starting from an empty frozen base:

| Atomic overlay | Requested B/entry | Allocations | Result |
|---|---:|---:|---|
| Inline-32 Papaya nodes | **58.951** | **1,000,015** | selected default |
| Dense ArcSwap pointer slots | 66.058 | 1,000,076 | rejected on throughput |
| Boxed-key Papaya nodes | 82.931 | 2,000,012 | general-key control |

Inlining removes one allocation per new 32-byte key and uses 28.9% less
requested memory than the boxed-key control. The dense pointer-slot prototype
also removed one allocation, but each insertion needed an ArcSwap pointer CAS;
it is retained only as an explicit experimental control.

Configured exact-width classes preserve the same one-allocation shape. With
500,000 new keys, the requested-byte results were:

| Exact key width | Inline B/entry | Boxed B/entry | Inline reduction | Inline allocations | Boxed allocations |
|---:|---:|---:|---:|---:|---:|
| 16 | **43.302** | 67.007 | **35.4%** | **500,036** | 1,000,012 |
| 31 | **59.302** | 82.007 | **27.7%** | **500,036** | 1,000,012 |
| 40 | **67.302** | 91.007 | **26.0%** | **500,036** | 1,000,012 |
| 64 | **91.302** | 115.007 | **20.6%** | **500,036** | 1,000,012 |

Lower bytes and allocation counts are better. The explicit width matters:
preallocating several unused inline tables was tested first and increased RAM
by 15–33%, so that automatic multi-class layout was rejected. A workload picks
one expected exact class and sends every unexpected length to the boxed
fallback.

## Warm mutation allocation

One million warm updates spread across 100,000 existing keys:

| Backend | Allocations/update | Bytes allocated/update |
|---|---:|---:|
| Atomic packed generation | **0** | **0** |
| Generic packed generation stable cell | 1 | 24 |
| Papaya owned-key update | 2.0625 | 89 |
| Concurrent SegmentedSwiss | **0** | **0** |
| DashMap borrowed update | **0** | **0** |

Stable generic cells successfully remove repeated key allocation, reducing the
Papaya-style path from 89 to 24 allocated bytes/update. They still allocate a
new `Arc<V>` and flatten under allocator contention. Direct atomic slots remove
that final allocation. The reproducible probe is
`examples/mutation_allocation_probe.rs`.

## Concurrent operation matrix

The matrix preloads 200,000 keys, uses two million operations (200,000 for
successful deletion), 64 shards where applicable, and reports the median of
three samples. Values are millions of operations per second from one unpinned
local process, so they are directional rather than cross-machine claims.

| Eight-thread workload | ConcurrentSwiss | Papaya dynamic | Generic generation | Atomic generation | DashMap |
|---|---:|---:|---:|---:|---:|
| read hit | 30.575 | **183.003** | 61.606 | 110.802 | 68.060 |
| read miss | 96.653 | **397.931** | 203.958 | 203.135 | 97.187 |
| update hit, distributed | 51.683 | 11.274 | 2.393 | 49.662 | **58.284** |
| update one hot key | 41.597 | 6.612 | 1.667 | 5.204 | **43.483** |
| insert miss | 31.762 | **64.574** | 26.483 | 32.366 | 51.611 |
| delete hit | 34.476 | 32.677 | 18.970 | **42.415** | 28.583 |
| delete miss | 76.884 | **343.478** | 68.285 | 79.627 | 81.577 |
| 90% hit / 5% miss / 3% update / 1% insert / 1% delete | 53.318 | **100.072** | 53.069 | 90.610 | 71.301 |

The atomic specialization changes the conclusion. It beats DashMap in this
local smoke on read hits, read misses, successful deletion, and the read-heavy
cache mix while using 42.3% less requested RAM. Distributed update is within
about 15% of DashMap in the final run; another fresh run reached 64.544 Mops/s,
so CPU pinning is required before claiming an update win. This broad matrix
predates the inline-key overlay optimization measured below.

The adversarial one-hot-key update is a clear loss. Arbitrary closure updates
retry a CAS on the same atomic word, while lock-based controls serialize without
recomputing. Lock-free progress is not a throughput win under maximum
single-location contention.

The local results are noisy: atomic distributed update ranged from 41.877 to
64.544 Mops/s across recent runs. Linux CPU pinning, repeated fresh processes,
and p50/p99 latency remain required before release decisions.

### Focused overlay comparison

The broad matrix runs implementations sequentially and is sensitive to local
scheduling. A focused probe rotates the three atomic overlay strategies on
every sample. With 200,000 frozen update keys, 500,000 operations, eight
threads, 11 samples, and `gxhash`, the medians were:

| Strategy | Insert miss | Distributed update hit | One hot-key update |
|---|---:|---:|---:|
| Inline-32 Papaya | **32.488** | **58.289** | **7.520** |
| Boxed-key Papaya | 26.882 | 49.991 | 7.009 |
| Dense ArcSwap-32 | 1.941 | 49.883 | 7.071 |

Values are Mops/s, so higher is better. Inline-32 improves insertion by 20.9%,
distributed update by 16.6%, and the hot-key case by 7.3% over the boxed-key
control in this focused run. It also wins RAM. The dense ArcSwap table is a
clear rejection: despite its RAM reduction, insertion is about 14x slower than
the boxed control.

The focused probe is `examples/atomic_overlay_probe.rs`. These remain unpinned
Apple M4 Max results; alternating order reduces, but does not eliminate,
platform noise.

The probe now covers read hit/miss, insert hit/miss, update hit/miss, one-hot
update, and delete hit/miss. Successful delete automatically limits the sample
to one operation per preloaded key. A sixth argument can select one workload.
Higher Mops/s is better; for time-per-operation, RAM, and allocation counts,
lower is better.

### Clean-stripe base-first A/B

The immediate before/after probe used 100,000 frozen keys, 300,000 requested
operations, eight threads, nine alternating samples, and the default inline-32
overlay. Only read routing changed:

| Workload | Overlay-first Mops/s | Clean-stripe base-first Mops/s |
|---|---:|---:|
| read hit | 153.094 | **169.165** |
| read miss | **212.440** | 206.239 |
| insert hit | **64.423** | 63.236 |
| insert miss | 28.463 | **33.300** |
| update hit | **57.963** | 57.287 |
| update miss | **78.997** | 74.513 |
| one hot-key update | **4.854** | 4.678 |
| delete hit | 43.938 | **47.615** |
| delete miss | 74.975 | **78.062** |

The intended signal is the 10.5% read-hit gain. Read miss lost 2.9%; every
mutation uses identical code in this A/B, so its movement is a direct estimate
of unpinned scheduler noise rather than an effect attributed to read routing.
Ten repeated monotonic-reader/rebuild tests plus the broader cutover suite
passed with both clean and shadowed stripes.

For configured exact widths, a 21-sample insert-only run measured 35.370 vs
32.320 Mops/s at 16 bytes, 31.798 vs 30.529 at 31 bytes, 30.802 vs 31.755 at
40 bytes, and 31.018 vs 31.786 at 64 bytes (inline vs boxed). Exact-width
inlining is therefore a consistent RAM win, but only a performance tie outside
the strong 16/32-byte results on this machine.

### Frozen-base definite-negative policies

Three exact policies now share the same atomic generation API:

- `Disabled` performs the perfect-hash route and original-key comparison;
- `EmbeddedFingerprint` stores nine digest bits in the unused high length bits
  of packed references when every frozen key is at most 255 bytes;
- `OneBytePerEntry` uses a deletion-safe two-bit blocked filter before the
  perfect-hash lookup.

Neither shortcut is authoritative: every positive result still reaches the
exact member-key check. Direct deletes never clear filter state, and rebuild
constructs new negative metadata before publication. A generation containing
any key longer than 255 bytes automatically uses exact-only references for the
embedded policy.

At one million frozen 32-byte keys, requested live memory was:

| Policy | Bytes/entry | Filter bytes | Direction |
|---|---:|---:|---|
| Exact-only | 48.789 | 0 | lower is better |
| Embedded fingerprint | **48.789** | **0** | lower is better |
| One-byte blocked filter | 49.789 | 1,000,000 | lower is better |

The controlled operation probe clones one writer-routing hasher across every
policy, preloads 100,000 frozen keys, requests 300,000 operations, uses eight
threads and 21 alternating samples, warms identical hit and miss routes before
timing, and keeps the inline-32 overlay fixed:

| Workload | Exact-only | Embedded fingerprint | One-byte filter |
|---|---:|---:|---:|
| read hit | **209.802** | 185.257 | 194.758 |
| read miss | 202.111 | 309.225 | **337.917** |
| insert hit | 59.529 | 60.039 | **60.979** |
| insert miss | 30.905 | 31.923 | **34.366** |
| update hit | 61.360 | 58.909 | **64.765** |
| update miss | 72.660 | **78.921** | 69.948 |
| one hot-key update | 4.537 | 4.673 | **4.751** |
| delete hit | 41.612 | 43.997 | **44.595** |
| delete miss | 72.420 | **83.123** | 76.598 |

Values are Mops/s, so higher is better. The embedded policy uses no additional
RAM and improves every miss operation in this run, while exact-only retains the
strong read-hit path. The one-byte policy leads pure read and insert misses but
does not dominate update/delete misses. Hot-key numbers and small hit-path
differences remain scheduler/contention sensitive.

For the cache decision, the probe precomputes the mixed query trace outside the
timed loop and runs one thread over 100,000 keys, one million reads, and 31
alternating warm samples:

| Workload | Exact-only Mops/s | Embedded Mops/s | One-byte Mops/s |
|---|---:|---:|---:|
| all hits | **28.192** | 26.734 | 26.538 |
| all misses | 28.487 | 45.181 | **47.889** |
| 95% hits | **29.996** | 29.670 | 28.817 |
| 97% hits | **30.682** | 29.630 | 29.486 |
| 98% hits | **29.720** | 29.376 | 28.400 |
| 99% hits | **31.706** | 30.152 | 29.826 |

The pure-hit and pure-miss latencies estimate a crossover near 14% misses on
this machine. All tested 95–99% hit mixes favor exact-only, so `Disabled`
remains the ordinary atomic-map default. `EmbeddedFingerprint` is the zero-RAM
miss-heavy option. The one-byte filter remains an explicit extreme-miss
experiment: its extra byte is not justified as a general default.

Reproduce the policy probe with:

```text
cargo run --release --features gxhash --example atomic_filter_probe -- \
  100000 300000 8 21
cargo run --release --features gxhash --example atomic_filter_probe -- \
  100000 1000000 1 31 read_98_hit
```

### Rejected write-path attacks

- A multi-class automatic inline overlay reserved unused tables and increased
  requested RAM by 15–33%; explicit one-width selection replaced it.
- Reusing the frozen 128-bit digest for writer routing removed one key hash but
  did not improve the controlled medians. Eight-thread distributed-update
  results were 55.1–57.6 Mops/s with reuse and 57.3–58.5 with the independent
  route hash, depending on overlay control, so the extra digest plumbing was
  removed.
- Papaya 0.2.4 exposes no public prehashed/raw point API. Caching a full hash in
  each inline key would add eight bytes per record and work against the primary
  RAM goal; maintaining a local Papaya fork was not justified.
- Replacing the load/compare-exchange writer pin with one `fetch_add` passed
  stress tests but produced conflicting A/B results. It helped the default
  single-thread distributed update (19.934 vs 17.654 Mops/s) yet lost the
  adjacent eight-thread run (46.065 vs 65.449). The proven CAS state machine
  remains.

Reproduce:

```text
cargo run --release --features gxhash --example concurrent_matrix -- \
  200000 2000000 8 64 read_hit
cargo run --release --features gxhash --example concurrent_matrix -- \
  200000 2000000 8 64 cache_mix_90r5m3u1i1d
cargo run --release --features gxhash --example atomic_overlay_probe -- \
  100000 300000 8 21 32 insert_miss
```

## Current decision

The direct all-operation comparison with DashMap, including one/eight-thread
throughput, RAM, allocation shape, and delete/rebuild lifecycle, is maintained
in [`ATOMIC_VS_DASHMAP.md`](ATOMIC_VS_DASHMAP.md).

Keep all four lock-free prototypes:

- use `LockFreeBinaryMap` as the fully dynamic control;
- use `LockFreeHybridMap` when an external owner provides generation boundaries;
- retain `LockFreeGenerationMap` for arbitrary cloneable values and as the
  generic correctness path;
- continue `LockFreeAtomicU64GenerationMap` as the current RAM/performance
  frontier when `NonMaxU64` is an acceptable value domain; its default new-key
  path now uses inline 32-byte Papaya keys.

The next insertion attack is genuinely variable-length key packing. Writer
pin/unpin plus exact-length accounting
still need amortization around point writes. The next update attack is an
explicit atomic-operation API for workloads where one native operation can
replace a retrying closure without weakening deletion semantics. Generic
values still need a pluggable atomic value codec. The next proof work is a model
of the stripe
`OPEN/count -> CLOSED -> DIRECT_BASE` state machine plus pinned p99 latency
during rebuild.

## Claim boundary

| Claim | Status |
|---|---|
| lock-free point reads | achieved |
| lock-free point writes with no library-owned point-path lock | achieved |
| exact same-key ordering across generation handoff | achieved by striped routing and contention tests |
| exact concurrent insert/update/delete semantics | achieved in stress/differential tests |
| multiple concurrent writers during packed build | achieved |
| packed RAM below DashMap | achieved in requested-byte probe |
| packed eight-thread read throughput above DashMap | achieved in local smoke |
| atomic packed mixed-operation throughput above DashMap | achieved in local smoke; pinned confirmation pending |
| allocation-free existing-key update/delete | achieved for `NonMaxU64` direct slots |
| one-allocation new-key insert | achieved by default for 32-byte keys and explicitly for exact 8/16/24/31/40/48/56/64-byte workloads |
| arbitrary `u64` domain | not achieved; `u64::MAX` is reserved |
| wait-free or starvation-free rebuild | not achieved |
| complete ETS traversal/ownership/bag API | not implemented |
| production readiness | not claimed |
