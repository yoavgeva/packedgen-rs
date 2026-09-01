# Adaptive external-map comparison

This comparison asks whether `AtomicAdaptive` is worth continuing when tested
against direct concurrent maps, rather than only against other PackedGen
overlay strategies. It does not involve FerricStore.

## Method

- Apple M4 Max, macOS, unpinned worker threads.
- Rust 1.94 release builds with PackedGen's `gxhash` feature.
- 100,000 resident mixed-width keys and exact `u64` values.
- Key distribution: 40% 8-byte, 25% 16-byte, 15% 24-byte, 10% 32-byte,
  and 10% 48-byte.
- Identical pre-generated keys and operation order.
- Capacity includes every distinct insertion in the timed trace.
- Implementations execute in alternating order for each sample.
- DashMap uses 64 shards. Papaya and Flurry retain one guard per worker.
- The longer leading-contender trace uses one million operations and 31
  samples at eight threads.

The competitors are direct Papaya 0.2.4, `scc::HashMap` 3.8.5, DashMap 6.2.1,
Flurry 0.5.2, and `RwLock<HashBrown>` 0.17.1. PackedGen's internal Papaya
overlay and fixed atomic layout remain controls, but are not substitutes for
the direct libraries.

## Requested memory

Fresh one-million-key maps, lower is better:

| Implementation | Bytes/live entry | Allocations |
|---|---:|---:|
| Adaptive | **33.074** | **8,229** |
| Direct Papaya | 61.693 | 2,000,006 |
| RwLock HashBrown | 71.229 | 1,000,002 |
| DashMap | 71.238 | 1,000,066 |
| SCC | 73.335 | 1,000,010 |
| Flurry | 155.586 | 3,000,021 |

Adaptive uses 46.4% less requested live memory than direct Papaya, 53.6% less
than DashMap, 54.9% less than SCC, and 78.7% less than Flurry.

## Large-cache optimization pass

The next pass extended automatic learning with an exact 48-byte inline class.
Keys through 32 bytes retain the packed atomic tables, exact 48-byte keys use
an inline concurrent table, and uncommon widths remain exact. These historical
rows predate the native arbitrary-length fallback; current post-learning
uncommon widths use that table, with Papaya only for startup/emergency spill.
The caller still supplies no key-width maximum.

At one million mixed keys, lower memory is better:

| Layout | Fresh B/entry | Allocations | B/live after 100% turnover |
|---|---:|---:|---:|
| Previous Adaptive | 35.639 | 207,416 | 71.066 |
| Adaptive with learned inline-48, 1.10x | **33.267** | **107,822** | **66.294** |
| Learned inline-48, read-optimized 1.15x | 34.397 | 107,822 | 68.556 |
| **Retained exact atomic-48, 1.15x** | **33.074** | **8,229** | **65.934** |
| Direct Papaya | 61.693 | 2,000,006 | 80.567 |

The retained layout reduces fresh memory by 7.2% versus the original Adaptive
layout and uses 46.4% less than direct Papaya. After complete key turnover it
uses 18.2% less memory than Papaya.

The first experiment used a six-word atomic table for 48-byte keys. It reached
31.656 B/entry and only 8,229 allocations, but isolated 48-byte reads were
11.767 Mops/s versus Papaya's 25.075 at one thread and 39.665 versus 61.376 at
eight threads. That design was rejected: maximum packing did not repay scalar
six-word verification. The design was revisited after the four-choice 1.15x
layout and lazy atomic overflow work. The new version uses 33.074 B/entry and
8,229 allocations. Against the immediately preceding inline-48 build, its
one-thread mixed read/update/cache/insert changes were -0.7%/-1.3%/+0.3%/-2.7%,
while exact-48 updates and inserts improved 13.6% and 2.9%. Exact-48 reads
remain 16.1% slower, so the retention is a mixed-cache density decision rather
than a universal 48-byte speed claim.

In a direct eight-thread run, higher is better: mixed read/update/cache/insert
were 102.345/48.472/75.270/44.919 Mops/s for Adaptive and
105.906/31.094/83.475/49.958 for Papaya. Whole-sample p95 spreads reached
31-276%, so these numbers show direction only. Adaptive is close on reads,
leads updates, and trails the cache mix and insert paths by about 10%.

The retained hybrid was tested at one million resident keys. Higher throughput
is better:

| Workload | Adaptive | Direct Papaya | Difference |
|---|---:|---:|---:|
| Eight-thread read hit | 43.675 | 47.770 | -8.6% |
| Eight-thread 95%-read cache mix | 44.006 | 45.559 | -3.4% |
| Eight-thread learned insert | 43.857 | 54.167 | -19.0% |
| Eight-thread full turnover | 45.656 | 53.414 | -14.5% |

An adjacent cache-mix run measured 40.057 versus 40.531 Mops/s, or -1.2%.
The host remains unpinned, so the honest local conclusion is a 1-4% cache-mix
gap rather than a claimed tie. Insert accounting and turnover remain the next
performance targets.

## Writer-acquisition optimization pass

The retained writer pin now acquires an open stripe with one `fetch_add`
instead of a load-plus-CAS retry loop. Stripe closure still succeeds only from
zero writers: an acquisition that observes an already closed stripe backs its
increment out and retries on the published generation. A direct unit test
covers the open count, release, and closed backout states.

In the focused 100K-entry/300K-operation probe, one-byte eight-thread
`insert_new` increased from 37.564 to 39.948 Mops/s (+6.35%). In the immediate
larger learned adaptive trace, insertion reached 44.750 Mops/s versus direct
Papaya's 55.893. The final retained-source repetition measured 43.857 versus
54.167, a 19.0% gap. Higher throughput is better. The unpinned host is noisy,
so these are development-machine direction, not release claims.

The same pass tested and rejected the following changes:

- relaxed CAS acquisition: direct Papaya-relative insert and churn regressed;
- blanket cross-crate mutation inlining: adaptive insert fell to 41.549 Mops/s
  versus Papaya's 51.842 and focused code size/performance worsened;
- multiply-high bucket reduction: alternating modulo/multiply-high runs did not
  show a repeatable end-to-end win;
- relaxed predecessor and empty-base counter ordering: eight-thread results
  were flat to slower;
- 1,024 rather than 4,096 writer stripes: focused eight-thread insertion fell
  from about 42.2 to 38.9 Mops/s;
- deferred destructor-only stripe release: eight-thread results were flat and
  single-thread insertion regressed from about 25.2 to 20.3 Mops/s.

The empty-frozen-base CRUD shortcuts were retained because they remove an
unnecessary base probe with no additional memory. Their isolated performance
effect was small on this host.

## Atomic headroom frontier

The learned atomic classes originally reserved 1.10 slots per planned entry.
An Adaptive-only alternating A/B removed external-map scheduling noise and
compared 1.10x with 1.15x on the one-million-key mixed fixture. Higher
throughput is better:

| Workload | 1.15x change in first ordering | Reverse ordering |
|---|---:|---:|
| Mixed read hit | +33% | +24% |
| 95%-read cache mix | +6.1% | +2.6% |
| Learned insert | +13% | +8.5% |

For the then-current inline-48 layout, the extra headroom raised fresh memory
from 33.267 to 34.397 B/entry (+3.4%)
and full-turnover memory from 66.294 to 68.556 B/live entry. The 1.15x layout
is retained because it materially shortens successful probe chains while
remaining 44.2% smaller than direct Papaya when fresh. The later atomic-48
change lowered the retained figures to 33.074 and 65.934 respectively.

The next 1.20x point reached 35.525 B/entry, but paired reads were flat (+2.8%
then -1.4%) and cache/insert changes did not repeat. It was rejected as past
the density/performance knee.

The direct-map smoke after retention remained noisy: Adaptive measured
40.108/32.422/47.771/34.661 Mops/s for read/cache-95/insert/update, while
Papaya measured 32.026/42.256/54.428/19.586. Whole-sample p95 spread reached
83-277%, so those crossings are recorded but not treated as release claims.

After deleting and replacing the entire one-million-key population, every map
again contains one million live keys. Capacity was reserved for the full
turnover. Lower is better:

| Implementation | Bytes/live entry after 100% turnover |
|---|---:|
| Adaptive | **65.934** |
| Direct Papaya | 80.567 |
| DashMap | 123.666 |
| SCC | 127.860 |

Adaptive still wins, but its advantage over Papaya narrows to 18.2% because
deleted atomic key slots remain physical until generation rebuild. Its
advantage remains 46.7% versus DashMap and 48.4% versus SCC.

## Longer eight-thread operation trace

Throughput is Mops/s; higher is better. `p95 spread` is the 95th-percentile
whole-sample duration above the median; lower is better. It is a stability
signal, not per-operation p95 latency.

| Workload | Adaptive | Direct Papaya | SCC | DashMap |
|---|---:|---:|---:|---:|
| Learned unique insert | 18.541 | **24.039** | 19.290 | 10.283 |
| Mixed-key read hit | 25.670 | 26.385 | 25.961 | **26.852** |
| 95%-read cache mix | 22.288 | **27.771** | 18.809 | 21.399 |
| 50% delete / 50% replacement | 32.798 | **38.436** | 32.335 | 7.909 |

| Workload | Adaptive p95 spread | Papaya | SCC | DashMap |
|---|---:|---:|---:|---:|
| Learned unique insert | 77.0% | **48.0%** | 98.5% | 55.1% |
| Mixed-key read hit | 119.2% | **48.3%** | 49.9% | 58.3% |
| 95%-read cache mix | 63.0% | **49.4%** | 71.6% | 125.1% |
| Turnover | 357.1% | 290.2% | 409.6% | **188.3%** |

The host was visibly noisy. Shorter scaling runs contained implausible 8-to-16
thread discontinuities, and the longer trace still has large p95 spreads.
These results rank local tradeoffs but cannot support scaling or tail-latency
claims. A pinned Linux run is mandatory before release conclusions.

## Exploratory operation coverage

The shorter 11-sample eight-thread trace adds operation shape coverage. Higher
is better:

| Workload | Adaptive | Direct Papaya | SCC | DashMap |
|---|---:|---:|---:|---:|
| Read miss | 108.136 | **196.570** | 161.967 | 104.173 |
| Distributed update hit | 20.406 | 14.155 | 11.582 | **24.433** |
| One hot-key update | 8.295 | 5.202 | 8.655 | **89.697** |
| Delete hit | 50.419 | 51.010 | **71.063** | 30.411 |
| Delete miss | 53.434 | **245.539** | 67.887 | 24.051 |
| 90%-on-1%-hot-set read | 74.640 | **206.196** | 93.556 | 86.015 |
| 90%-on-1%-hot-set update | 33.609 | 36.153 | 42.081 | **46.191** |

These figures show where further work is and is not useful. The exact negative
path trails Papaya materially, hot-set reads need attention, and the single
hot-write key remains unsuitable for CAS-based atomic values. Distributed
updates and successful deletes are competitive.

## Exploratory native C++ comparison

A later Apple M4 Max run added native C++ controls using Apple Clang 17. The
direct mixed-binary fixture remained 40% 8-byte, 25% 16-byte, 15% 24-byte,
10% 32-byte, and 10% 48-byte keys with `u64` values. C++ controls used the same
fast hash and key corpus. Cross-language allocator, ABI, and hash differences
remain, so this is a local tradeoff check rather than a release headline.

Requested live bytes at one million entries are lower-is-better:

| Implementation | Concurrent semantics | Bytes/live entry |
|---|---|---:|
| PackedGen adaptive | lock-free steady-state point operations | **33.074** |
| libcuckoo | multi-reader/multi-writer cuckoo map | 54.246 |
| `std::unordered_map` | single-writer node map | 70.400 |
| oneTBB 2023.1 `concurrent_hash_map` | accessor-locked concurrent map | 79.177 |
| Boost 1.91 flat map | single-writer flat map | 79.412 |
| Boost 1.91 `concurrent_flat_map` | visitation-locked concurrent flat map | 80.460 |
| phmap parallel flat, 64 mutex shards | sharded concurrent flat map | 83.605 |
| phmap flat | single-writer flat map | 83.606 |

PackedGen used 39.0% less requested memory than the closest C++ control,
libcuckoo, and 58.2% less than oneTBB. These measurements include owned C++
string allocations. They do not include allocator fragmentation or process
RSS, which still require a pinned Linux run.

The matched 31-sample mixed-key operation trace below uses 200K resident keys,
one million operations, and eight threads. Throughput is Mops/s; higher is
better:

| Workload | PackedGen adaptive | oneTBB concurrent hash | Winner |
|---|---:|---:|---|
| Read hit | **87.605** | 62.352 | PackedGen +40.5% |
| Distributed update hit | 44.332 | **61.383** | oneTBB +38.5% |
| Learned unique insert | **46.218** | 17.251 | PackedGen +167.9% |
| 95%-read cache mix | **75.915** | 49.015 | PackedGen +54.9% |

At one thread, PackedGen measured 15.581/12.521/14.815/12.550 Mops/s for
read/update/insert/cache mix versus oneTBB at
12.657/11.919/9.170/11.961. PackedGen led this particular trace, but the host
showed large sample spreads, including 119% for PackedGen update and 246% for
oneTBB update at eight threads. Linux core pinning remains mandatory.

### Restricted fixed-word ceiling

Junction and Rust LeapMap cannot directly store the mixed variable-length key
fixture with equivalent collision-safe semantics. They were therefore measured
separately as a `u64 -> u64` algorithm ceiling. PackedGen used its learned
8-byte binary layout. Junction numbers use DLMalloc in-use bytes, while the
other rows use requested live bytes, so the density comparison is approximate:

| Implementation | Bytes/live entry | Read hit, 8T | Update/exchange, 8T | Insert, 8T | 95%-read mix, 8T |
|---|---:|---:|---:|---:|---:|
| PackedGen adaptive 8-byte | **19.230** | 110.648 | 54.210 | 49.838 | **91.164** |
| Junction Grampa `u64` | 36.328 | 311.717 | 112.646 exchange | 44.293 | 273.988 |
| Junction Leapfrog `u64` | 37.749 | incomplete | incomplete | incomplete | incomplete |
| Rust LeapMap 0.3.3 `u64` | 81.789 | **405.837** | **150.612** | **151.707** | 66.761 |

Junction Grampa is a very fast word map, but its exchange is not equivalent to
PackedGen's arbitrary atomic read-modify-write callback. Rust LeapMap used
pre-mixed `u64` keys with `SimpleHasher`; PackedGen hashed byte slices with
`gxhash`. The restricted controls show the available speed ceiling, not a
general mixed-key replacement. PackedGen is about 47% smaller than Grampa and
76% smaller than Rust LeapMap on this fixed-key fixture. PackedGen also beat
both controls on new inserts versus Grampa and on the complete LeapMap cache
mix, while their pure word reads and atomic exchanges were much faster.

Sources were oneTBB 2023.1, Junction commit
`fa76568b3bf6665965a8281d1765db8608633ee8`, Turf commit
`29ba08510207cb1ecf4c533a4ca60a60712600ce`, and Leapfrog 0.3.3. Folly was not
included because its current Homebrew build requires a large dependency stack;
`ConcurrentHashMap` and `AtomicHashMap` remain Linux comparison targets. The
C++/fixed-word harness is exploratory and must be made permanent before these
results are used in release material.

## Decision

Adaptive is worth continuing as a density-first concurrent map. It is not the
overall speed leader and should not be described as better than Papaya.

The current measured choice is:

- choose direct Papaya when read-heavy throughput and simpler dynamic growth
  matter more than memory;
- choose Adaptive when fitting substantially more exact mixed keys into RAM is
  worth a modest-to-material throughput tradeoff;
- choose SCC for several mutation-heavy shapes;
- choose DashMap for one-hot-key writes and familiar locking semantics.

Before any integration decision, repeat the permanent matrix on pinned Linux,
add per-operation p50/p95/p99/p99.9 latency, measure rebuild overlap, and add
native C++ controls using the same serialized operation trace.

## Reproduction

```text
cargo run --release --features gxhash --example memory_probe -- \
  lockfree-atomic-overlay-adaptive-mixed-binary 1000000

cargo run --release --features gxhash --example atomic_overlay_probe -- \
  100000 1000000 8 31 0 overlay_read_hit adaptive-leading
```
