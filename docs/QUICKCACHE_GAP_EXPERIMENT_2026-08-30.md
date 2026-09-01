# QuickCache gap experiment — 2026-08-30

## Goal

Improve bounded-cache insertion, eviction, policy quality, and memory without
regressing DirectPackedCache's resident borrowed reads or stampede behavior.
The external `oltp-ferric-lab` runner used four workers, 50,000 logical
entries, 250,000 operations per worker, equal logical byte limits, and fresh
processes. Throughput is M logical operations/second (higher is better), hit
rate is percent (higher is usually better), p99 is microseconds (lower is
better), and RSS is ending process growth in MiB (lower is better but it is not
allocator-live or peak memory).

## Retained library changes

1. Weighted eviction now derives bounded proactive headroom from current
   population, average charge, overage, and 1/1024 of the byte budget instead
   of always selecting exactly one victim. A focused uniform-weight TDD case
   proves that one pressure event creates bounded headroom without crossing the
   hard limit.
2. `CacheConfig::with_admission_doorkeeper(expected_entries)` enables an
   opt-in, lock-free-on-the-common-path, two-generation membership doorkeeper.
   It uses two hashes and nominally 16 bits per expected entry per generation.
   Power-of-two word rounding makes the actual allocation between four and
   eight bytes per expected entry. Initial fill is unchanged. After the entry
   hint has first been reached, a one-off absent key returns
   `CacheAdmissionOutcome::Rejected`; a repeated sighting is admitted.
3. The doorkeeper rejects before value-arena allocation and adaptive-index
   mutation. It is implemented with safe Rust atomics plus a mutex used only at
   generation rotation. It is supported by scalar, guarded, batched, bulk, and
   bulk-load paths and remains disabled by default.
4. `with_adaptive_frequency_admission(2, 6_000)` adds an opt-in two-hash,
   saturating frequency sketch. The cache uses ordinary second-sighting
   admission until it has at least 1,024 published lookups and 60% hits.
   Borrowed guards publish those counters at their existing refresh/drop
   boundary. Once eviction pressure exists, one of every 16 resident hits feeds
   the sketch; default and pre-pressure reads do not hash into it.
5. Victim sampling compares `(expired, accessed, frequency)`. A saved victim
   reservoir now revalidates both CLOCK access and current frequency instead of
   evicting from stale order. All new policy code is safe Rust. The frequency
   sketch adds four to eight bytes per expected entry, making total opt-in
   doorkeeper plus frequency metadata approximately eight to sixteen bytes per
   expected entry after power-of-two rounding.
6. Adaptive native victim sampling now skips key-width classes whose configured
   native capacity is zero. This removes empty bucket and overflow probes without
   changing the populated-class order or adding resident metadata. Five
   alternating full-cache pairs improved scalar admission by 8.7–10.8% and
   batch admission by 5.7–10.4%.
7. DirectPackedCache now has one monotonic cache-level expiration marker.
   Maintenance skips the full expiration search until a TTL-capable insertion,
   bulk load, or touch has occurred. The marker adds no per-entry bytes and is
   conservative: after TTL is ever used, ordinary expiration maintenance remains
   enabled. Release/acquire ordering publishes the marker before a TTL entry can
   become visible.
8. `with_tiered_frequency_admission(2, 6_000, 8, 7_500)` retains the ordinary
   adaptive gate after 60% reuse and raises only its maximum victim-frequency
   evidence after 75% reuse. It reuses the existing byte-counter sketch and
   adds no per-entry metadata. Invalid tiers that activate earlier or weaken
   the base are rejected during cache construction.
9. Frequency aging no longer halves the entire sketch on the foreground
   observation that crosses an aging boundary. The existing counter byte now
   stores a four-bit frequency and a four-bit epoch; estimates apply elapsed
   halvings lazily, and observations update only their two hashed counters.
   The boundary itself advances one cache-level epoch and halves the victim
   gate. This removes an O(sketch size) request-path pause without adding
   per-entry or per-counter bytes. The sketch remains approximate: a counter
   untouched for sixteen complete windows can alias an old epoch, affecting
   policy only, never key/value correctness.
10. Async foreground enforcement now restores the configured hard ceiling plus
    a 1/1024-capacity cushion instead of waiting for, or itself restoring, the
    soft target. The background worker remains responsible for the soft target.
    This keeps exact entry and weighted hard bounds while limiting the unlucky
    caller to bounded work. Ten graph pairs reduced maximum operation latency
    by about 94%; median throughput moved about -0.5% and p99.9 about -1.3%,
    both within the observed process noise. No per-entry state was added.
11. `DirectCacheGuard::refresh` releases its guarded-writer generation and
    lazily reacquires one on the next guarded mutation. A deterministic
    red/green test reproduced a maintenance rebuild waiting forever on an idle
    refreshed guard. The formerly hanging version-churn seed now completes with
    four rebuilds, no active readers, and no retired debt. Long admission-batch
    scopes explicitly reacquire after their internal refresh so their existing
    reservation invariant remains intact.
12. Capacity-pressure timing is separated into the explicit
    `cache-pressure-timing` feature. Ordinary `cache-diagnostics` retains exact
   reclamation snapshots but adds no timing object to the cache and performs
   no request-path clock reads. A red/green feature-boundary test prevents
   observer overhead from silently returning to comparison builds.
13. The neutral adapter retains cache-level conditional admission as its
    default after the liveness-safe lazy guard refresh changed the tradeoff for
    miss-by-miss guarded admission. Five pressure pairs measured 14.20 Mops/s
    cache-level versus 9.41 Mops/s guarded at the median; p99 was about 1.00
    versus 1.50 microseconds and p99.9 about 1.92 versus 2.42. Hit rate,
    rebuilds, active readers, and retirement debt were unchanged. Guarded
    admission remains available for true caller-owned batches.

## Rejected experiments

| Variant | Result | Decision |
| --- | --- | --- |
| Guarded scalar insertion with synchronous eviction | Became CPU-bound after reaching capacity because the protected operation generation overlaps eviction mutation | Keep only as an experimental adapter arm; do not select |
| Guarded insertion with async eviction | 1.80–2.12 Mops/s cold fill versus 2.24–2.31 for cache-level async insertion | Reject |
| Entry-only bound | About 1.76–1.85 Mops/s versus 1.36–1.52 for the old weighted path | Useful isolation only; not a replacement for arbitrary weighted values |
| Proactive weighted headroom | Roughly 9% median cold-fill gain in the initial A/B | Retain |
| Async eviction, 10% hard overshoot | Roughly 36% cold-fill gain over improved synchronous eviction | Use in the finalist configuration; remains opt-in |
| Overlay 1× / 2× / 4× / 8× | 2× was fastest; larger overlays reduced rebuilds but lost speed and RAM | Retain 2× in this workload adapter |
| Eviction batch 32 / 64 / 128 / 256 / 512 | 256 was a small directional win; 512 regressed | Use 256 in finalist adapter |
| Access marks every 1 / 2 / 4 / 8 / 16 hits | Less sampling did not materially close hit rate and every-hit marking reduced resident throughput | Revert |
| Two-bit reference credits in existing metadata | Less than one percentage-point hit-rate gain and extra atomic cost; also reduced maximum TTL encoding | Revert completely |
| Doorkeeper generation history in victim ranking | Graph throughput moved about +1.4%, pressure about -1.4%, large objects were neutral, and hit rates were unchanged in three paired runs | Revert completely; the one/two-generation signal is too coarse |
| Approximate eviction ghosts bypass admission | Graph hit rate rose about 2.8 points and large objects about 1.9, but graph throughput fell about 16%, pressure quality regressed, and insertion/RSS rose | Revert completely; it admits too much work |
| Approximate eviction ghosts only mark a readmitted entry accessed | Admission volume stayed stable, but graph/large hit rates were unchanged and graph/pressure throughput fell about 4.6%/3.6% | Revert completely; sampled CLOCK consumes the binary hint too cheaply |
| Static frequency gate 1/2 | Gate 2 materially improved graph and large-object quality, but static gating reduced version-churn hit rate | Retain only behind adaptive reuse gating |
| Adaptive frequency threshold 25% | Activated during pressure-skew and lost about 4.5 hit-rate points | Replace with the retained 60% threshold |
| Frequency gate 3/4 | At 300K operations gate 4 improved graph/large throughput over gate 2, but reduced pressure and version throughput | Keep gate 2 as the universal compromise |
| Packed 4-bit frequency counters | Halved sketch storage to two-to-four bytes/entry, but resident-hot lost about 3% in all three paired runs | Revert completely; retain byte counters |
| Two frequency counters localized within one 64-counter block | Pressure throughput improved 0.3–7.9%, but hit rate fell in all five pairs; large-object hit rate fell 0.05–0.13 point and throughput lost in four of five pairs | Revert completely; correlated sketch collisions damage the remaining policy gap |
| Counting the doorkeeper bit as candidate frequency | Scan resistance stayed exact, but graph hit rate lost 0.8–1.5 points and large-object hit rate lost 2.5–2.7 points; large throughput fell 9.5–11.3% in all five pairs | Revert completely; the extra admission barrier is intentional protection against churn |
| Omitting frequency-protected survivors from the victim reservoir | Large throughput improved slightly in four of five pairs, but quality did not improve consistently; graph throughput lost in four of five and scan leaned negative | Revert completely; keep current-frequency revalidation at reservoir consumption rather than over-protecting a sampled estimate |
| One-pass reservoir builder with a fixed 64-index stack buffer | Removed repeated eligibility scans but regressed pressure, graph, and scan consistently; large objects were neutral-to-negative | Revert completely; the simple iterator/chunk form compiles better than manual reverse-index bookkeeping |
| Sorted eligible-prefix reservoir builder | Reused the victim rank to replace filtering with `partition_point`, but pressure lost in four of five pairs and large-object quality leaned negative | Revert completely; retain the general filtered chunk builder |
| Packed guard hit count plus CLOCK phase in one word | Preserved mixed tracked/untracked semantics, but eight long resident-hot pairs ranged from -1.9% to +5.7% with a roughly -0.3% median | Revert completely; counter bookkeeping is not the resident bottleneck |
| Immediate first-generation compaction at the entry hint | Raised resident-hot by roughly 27% and improved pressure/large quality, but scan resistance lost about 2.6% median and macOS retained the temporary rebuild pages | Revert as a universal trigger; steady-state reuse must be observed first |
| Reuse-gated first-generation compaction | Raised resident-hot by roughly 25% and reduced allocator-live bytes from 15.09 MB to 13.48 MB, but repeated longer cold-fill controls still leaned about 1-2% slower and post-rebuild physical RSS retained about 6.2 MiB | Do not retain automatically; preserve as evidence for a future explicit read-tier compaction API |
| Borrow exact-width adaptive read keys instead of copying into scratch | Pressure-skew improved roughly 4-5% at the median, but resident-hot was neutral and cold fill lost about 2-3% | Revert completely; the changed probe code shape costs more on absent keys than the avoided exact-key copy saves |
| Defer append-frontier discovery until an overlay tag miss | The append-only invariant is valid, but miss-heavy cold fill and pressure regressed broadly after the code-motion change | Revert completely; retain frontier-first read scans |
| Combine adaptive sample/fallback filter address calculation | Removed one identical hash reduction after fixed-table misses, but paired medians were about -0.7% cold fill and -2.8% pressure-skew; resident-hot was noisy with one severe loss | Revert completely; the larger combined helper produces worse compiled code than two small filter probes |
| Interleave sample/fallback routing-filter words | Kept the same bit budget and placed both miss hints together, but longer pairs lost resident-hot in all runs (about -1% to -16%), cold fill in two of three, and pressure was mixed | Revert completely; changing adaptive-overlay object and code layout harms the successful-hit path despite better theoretical miss locality |
| Defer the dynamic-fallback filter until after a sampled-table miss | Preserved exact routing and saved one filter load on sampled hits, but longer pairs lost about 5-17% resident-hot, 8-10% cold fill, and two of three pressure runs | Revert the production refactor; retain only the zero-runtime characterization test proving the hints stay independent |
| Cache the monotonic TTL marker in each read guard | Cold fill improved about 2-4%, but median resident-hot and pressure throughput regressed; the extra guard state/layout was not neutral | Revert production code; retain the race test proving a pre-existing guard observes the first TTL admission |
| Check the global TTL marker directly before entry metadata | Pressure improved in all three longer pairs, but resident-hot lost about 2-12% in all three and cold fill was mixed | Revert completely; keep unconditional exact entry expiry checks as the universal path and reserve TTL-free specialization for a future explicit API |
| Local checked/sentinel wrappers around `ptr_hash` 2.0.1 | Prevented the arbitrary-miss remap overrun, but frozen hits regressed about 1-5% and a Cargo patch would not protect published downstream users | Reject; pin official safe 2.1.1 |
| Official `ptr_hash` 2.1.1 with ordinary lookup inlining | Fixed the UB, but hot hits lost in four of five paired runs by roughly 0.2-2.9% | Keep the safe release and force only the two internal lookup boundaries inline |
| Force frozen construction helpers inline | Recovered roughly 0.2-0.4% of build time, but frozen hits regressed 1.5-3% in all three pairs | Revert; retain the 0.64% safe-construction cost |
| General unstable sort for 32-key frozen batches | Preserved exact duplicate/hit/miss ordering, but five paired runs were roughly 10-17% slower than the small stack insertion sort | Revert production code; retain the 32-key regression and dedicated frozen-batch benchmark |
| Stream each frozen batch digest directly into lookup | Removed about 768 bytes of temporary digest/slot arrays at batch 32, but five paired runs were about 16% slower | Revert; independent precomputation gives the compiler useful instruction-level parallelism |
| Zip frozen batch arrays instead of indexing by one counter | Preserved exactness and exposed bounds-safe iteration, but the all-hit path lost about 20% in three paired runs | Revert; the explicit fixed-range loop unrolls and schedules better |
| Force the complete public frozen multi-get inline | Kept semantics and allocation behavior, but all-hit latency regressed about 14-20% in three paired runs | Revert; preserve the helpful call boundary and inline only the two tiny index boundaries |
| PtrHash native `index_batch` prefetching | The all-feature prototype initially improved 32-key batches, but disturbed scalar-miss code layout by about 3-4%. Isolating it from PHast preserved scalar operations, while exact default and `gxhash`-only controls showed roughly 1-2.6% slower miss batches and unstable hit batches | Revert completely; direct scalar slot calculation remains the only broad batch win |
| Build frozen batch results with `array::from_fn` | Removed explicit result-array writes, but three paired runs lost roughly 9-11% on hits and 15-18% on misses/mixed batches | Revert; the explicit fixed-range loop gives LLVM a substantially better schedule |
| Hard-limit foreground eviction plus one-batch background slices | Pressure/graph/version/large p99 moved about +41%/+18%/+17%/+6%; pressure and graph throughput lost about 6%/4% | Revert; releasing and reacquiring maintenance ownership added more work and jitter |
| Foreground drains exactly to the async hard limit, without headroom | The newer tail run cut maximum latency but raised foreground enforcement from about 4 events to roughly 1,100–1,300 and worsened graph p99.9 in all five pairs | Reject exact-ceiling restoration; retain hard-limit overage plus a 1/1024 cushion |
| Proactive headroom 1/256 | Pressure throughput lost about 2.6%; graph lost about 1.9% and 1.75 hit-rate points | Revert; retain 1/1024 |
| Proactive headroom 1/512 | Mild graph/version speedups, but graph quality lost about 0.7 point and pressure p999 was inconsistent | Revert; retain 1/1024 |
| One maintenance mutex owning the victim reservoir | Safe and simpler, but graph throughput lost about 3% in two of three pairs, graph p99 worsened in two of three, and pressure p999 worsened | Revert; retain the separate uncontended reservoir mutex |
| Eviction without per-key mutation stripes | Median throughput was -0.2% pressure, +0.2% graph, +0.3% version, and +0.3% large; hit rates were unchanged | Revert; the more complex concurrency proof bought no measurable speed |
| Frequency before CLOCK in victim rank | In five alternating 400K-operation pairs graph quality gained only about 0.07 point; throughput was neutral overall and version churn leaned negative | Revert; a recent CLOCK mark remains the primary rank |
| Partial victim-prefix selection | Median throughput gained about 3.8% pressure and 1.0% graph, but lost about 1.3% version and changed reservoir policy quality | Revert; do not exchange workload behavior for sorting work |
| One-byte victim rank instead of the equivalent tuple | Median throughput moved about -2.9% pressure, -2.2% graph, +0.9% version, and 0% large | Revert; the tuple compiles better for this distribution |
| Two-capacity doorkeeper rotation interval | Inserted roughly 1.5–2.2× as many candidates; graph/large hit rate lost about 2.7–3.5/2.3 points and throughput lost about 17–34%/8–11% | Revert completely; remembering recurring misses longer weakens scan protection |
| Frequency gate 3/4 after maintenance optimization | Gate 4 improved large-object throughput about 7% and hit rate about 1.5 points, but pressure throughput regressed while hit rate was flat; gate 3 was not a broad win | Retain universal gate 2; a future cost-aware policy must be adaptive rather than a global specialization |
| Gate 4 activated only at 75%, without a base tier | Graph/large hit rate lost about 4.6/2.7 points and throughput lost about 21%/10% because quality fell before the gate could activate | Reject; high-reuse strictness must layer over the base rather than replace it |
| Tiered high gate 10/12/15 | Did not improve on gate 8; graph and large-object hit rates flattened or declined slightly | Retain high gate 8 as the measured ceiling |
| Tiered high threshold 76%/77% | 76% delayed useful protection; 77% lost about 0.6 graph and 1.3 large-object hit-rate points versus 75% | Retain the 75% high-reuse threshold |
| Tiered eviction batch 128/512 | Batch 128 lost about 0.6 graph hit-rate point and slightly worsened graph/pressure p99.9; batch 512 lost quality and materially worsened graph tails | Retain batch 256 after the policy change |
| Async hard ceilings 102.5% / 105% / 115% / 120% | 115% improved graph p99.9, but reduced pressure throughput in all five pairs and worsened large-object tails; other settings were not broad wins | Retain the 110% experimental finalist and expose the ceiling in the lab adapter |
| Moving the complete victim batch into the reservoir | Avoided key copies and helped graph throughput, but median pressure throughput fell about 2.9% and retained records made the hot reservoir less compact | Revert; compact reservoir keys are worth their copy |
| One compact reservoir batch instead of 64-key chunks | Reduced allocations, but pressure p99.9 worsened in all three focused latency pairs and graph tails also leaned worse | Revert; retain incremental 64-key release |
| 1,024-bit duplicate prefilter for native victim samples | Exact behavior was preserved, but median pressure/graph throughput moved about -1.3%/-1.2% | Revert; bookkeeping cost exceeded the saved comparisons |
| Accepting duplicate physical victim samples | Exact conditional removal remained memory-safe, but graph hit rate fell about 1.3 points in every pair and large-object quality also declined | Revert; candidate diversity is policy-critical |
| One pinned native-sampling batch | Reproduced the scalar sample sequence exactly and reduced publication loads, but graph throughput lost about 3% at the median | Revert; the larger sampling frame/code shape was not a universal win |
| Rolling 64-counter frequency-aging slices | Removed the full reset burst, but starting aging immediately reduced pressure/graph/large hit rate by about 1.0/0.55/0.4 points | Revert in favor of epoch-lazy aging, which preserves boundary semantics |

## Frozen 32-key multi-get input order — 2026-08-31

`FrozenPackedMap::get_many` already precomputed independent digests and perfect
slots, but then insertion-sorted the random slots before exact verification.
At one million entries that ordering did not create useful spatial locality;
it only added quadratic comparisons and a 256-byte order array. Verifying the
precomputed slots directly in caller order removes both costs without changing
the result array, exact key checks, retained memory, or scalar APIs. Lower
latency is better.

| 32-key batch | Sorted control | Input-order candidate | Change |
| --- | ---: | ---: | ---: |
| All hits | 597.61 ns | 251.14 ns | -58.0% |
| All misses | 464.19 ns | 257.44 ns | -44.5% |
| 50% hit / 50% miss | 501.71 ns | 267.99 ns | -46.6% |

Three final comparison runs put the all-hit frozen batch at 255.33 ns versus
807.35 ns for 32 individual HashBrown lookups, about 3.2x faster. A new 32-key
regression covers duplicates, hits, misses, and caller-order preservation.
Scalar frozen hit/miss controls stayed neutral or improved, the selected cache
matrix reported zero failures and correctness violations, and retained RAM is
unchanged. General unstable sorting and fully streamed digest calculation were
both slower and remain documented in the rejected table. PtrHash's native
batch-prefetch API was also rejected after narrower default and `gxhash`
feature tests exposed batch regressions that the first all-feature result had
hidden.

## Frequency-aware A/B

Three exact-seed, order-alternated pairs used four workers, 50,000 entries,
200,000 operations per worker, strict synchronous eviction, and frequency gate
2. The ratios below compare pressure-only sampled resident frequency plus stale
reservoir revalidation with the prior admission-only frequency sketch.

| Workload | Median paired throughput change | Hit-rate change | Decision |
| --- | ---: | ---: | --- |
| Resident hot | -0.2% | 0 points | Neutral; a follow-up avoided hashing before pressure |
| Version churn | +15.3% | +0.19 points | Retain |
| Pressure skew | +5.7% | +1.32 points | Retain |
| Graph traversal | +4.8% | +1.59 points | Retain |
| Large objects | +10.1% | +2.88 points | Retain |

The resident follow-up moved the pressure check ahead of hashing. In a later
three-seed confirmation its median was 21.63 Mops/s versus 21.18 for the prior
binary, although one run was a scheduler outlier. This is directional local
evidence, not a publication-grade confidence interval.

## Earlier strict comparison

These are medians of three fresh-process runs with four workers, 50,000
entries, and 200,000 operations per worker. PackedGen uses adaptive frequency
gate 2, a 60% activation threshold, synchronous eviction, batch 64, and overlay
2x. Throughput is Mops/s; RSS is ending process growth in MiB.

| Workload | PackedGen Mops/s | Quick Mops/s | PackedGen hit | Quick hit | PackedGen RSS | Quick RSS |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Resident hot | 21.70 | **52.00** | **100.00%** | 99.68% | **15.3** | 19.3 |
| Cold fill | **5.63** | 4.12 | 0% | 0% | **25.6** | 39.2 |
| Pressure skew | 5.92 | **10.23** | 67.43% | **72.61%** | **32.5** | 34.4 |
| Graph traversal | 1.06 | **1.30** | 74.93% | **81.81%** | 64.3 | **43.5** |
| Version churn | 4.45 | **5.63** | **8.53%** | 5.83% | **32.7** | 37.4 |
| Scan resistance | **5.73** | 3.74 | 52.36% | **52.39%** | **25.9** | 38.4 |
| Large objects | 1.25 | **1.58** | 77.42% | **80.11%** | 240.1 | **229.5** |
| Stampede | **24.12** | 9.74 | **99.570%** | 99.567% | 9.9 | **5.6** |

PackedGen wins throughput in cold fill, scan resistance, and stampede. It uses
less ending RSS in five cells. QuickCache still leads resident throughput and
the general pressure/turnover cells. Ending RSS is noisy allocator retention,
not peak or allocator-live memory; graph and large-object memory remain open
issues.

## Earlier async finalist

The read-heavy finalist uses wake-driven async eviction, a 110% hard ceiling,
and batch 256. Medians below use the same 200K-operation shape for throughput.
All PackedGen runs drained to 49,956–49,983 entries, but peak overshoot was not
captured.

| Workload | PackedGen Mops/s | Quick Mops/s | PackedGen hit | Quick hit | Direction |
| --- | ---: | ---: | ---: | ---: | --- |
| Cold fill | **5.84** | 3.94 | 0% | 0% | PackedGen |
| Pressure skew | 6.78 | **10.23** | 67.75% | **72.60%** | QuickCache |
| Graph traversal | 1.279 | **1.299** | 76.62% | **81.83%** | Near throughput tie; Quick quality |
| Version churn | **5.22** | 4.59 | **8.59%** | 5.79% | PackedGen |
| Large objects | 1.22 | **1.58** | 76.16% | **80.14%** | QuickCache |

Separate 50K-operation latency processes produced these three-run medians:

| Workload | PackedGen p99 µs | Quick p99 µs | PackedGen p999 µs | Quick p999 µs |
| --- | ---: | ---: | ---: | ---: |
| Cold fill | **1.791** | 2.085 | **3.293** | 8.671 |
| Pressure skew | **1.750** | 1.958 | 21.055 | **6.375** |
| Graph traversal | **7.835** | 11.295 | 35.551 | **22.383** |
| Version churn | **1.208** | 2.000 | **3.875** | 7.251 |
| Large objects | 10.959 | **10.711** | 31.167 | **15.631** |

Async eviction fixes the synchronous p99 gap but not maintenance p999. Batch
256 had the best pressure and graph p999 in the final 64/128/256 focused sweep.
The next architectural task is incremental/cooperative victim maintenance with
a fixed foreground work budget, not a larger admission sketch.

## Profiler-guided sampler and maintenance follow-up

An admitted-insert microbenchmark showed that raw index publication was not the
turnover bottleneck. With an already pinned guard, new insertion reached 52.58
Mops/s and a 32-item caller batch reached 62.18 Mops/s. Full-cache admission fell
to 2.31 Mops/s scalar and 2.27 Mops/s batched, while the Papaya manual-victim
control reached 13.12 Mops/s.

A macOS CPU trace attributed 54.1% of samples inclusively to eviction
enforcement, 24.4% to `evict_to_limits_locked`, 18.9% to victim collection, and
16.7% to native atomic-entry sampling. Actual `insert_if_absent_inner` accounted
for only 2.9%. This rejected allocation or index publication as the primary
optimization target.

Skipping native key classes with zero configured capacity produced the
following exact-binary A/B result:

| Boundary | Result | Interpretation |
| --- | ---: | --- |
| Full-cache scalar admission | +8.7% to +10.8% across five pairs | Retain |
| Full-cache batch admission | +5.7% to +10.4% across five pairs | Retain |
| One-million-operation pressure run | +2.46% median; 6/7 pairs positive | Retain |
| Hit rate | No consistent movement | Sampling quality preserved |

The next trace-level finding was larger: the harness requests maintenance every
4,096 operations, and PackedGen searched all resident entries for expiration
even when no TTL had ever been supplied. All workers wait at that maintenance
barrier. The monotonic cache-level marker eliminates this impossible search.
Three alternating 300,000-operation pairs gave these median paired throughput
changes; higher is better:

| Workload | Change |
| --- | ---: |
| Resident hot | **+133.6%** |
| Cold fill | **+30.7%** |
| Pressure skew | **+85.7%** |
| Graph traversal | **+13.7%** |
| Version churn | **+40.5%** |
| Scan resistance | **+28.1%** |
| Large objects | **+10.8%** |
| Stampede | **+10.7%** |

These gains do not apply while TTL expiration is active. They are nevertheless
valid for permanent-entry caches and mixed systems during the period before TTL
is first used. Exact expiration tests cover runtime admission, bulk load, touch,
and lazy read expiration.

## Retained finalist versus QuickCache after the fix

The table is the median of three fresh processes per candidate, four workers,
50,000 logical entries, and 500,000 operations per worker. PackedGen uses async
eviction, a 110% hard ceiling, batch 256, overlay 2×, and adaptive frequency gate
2. Throughput is Mops/s and higher is better. Hit rate is also higher-is-better,
but more hits can represent less source work rather than faster cache code.

| Workload | PackedGen | QuickCache | Packed hit | Quick hit | Throughput direction |
| --- | ---: | ---: | ---: | ---: | --- |
| Resident hot | 47.41 | **50.55** | **100.00%** | 99.70% | Quick by about 6% |
| Cold fill | **7.73** | 4.02 | 0% | 0% | PackedGen by about 93% |
| Pressure skew | **12.49** | 12.05 | 74.20% | **78.54%** | PackedGen by about 4%; Quick quality |
| Graph traversal | **1.425** | 1.292 | 77.41% | **82.22%** | PackedGen by about 10%; Quick quality |
| Version churn | 6.803 | **8.671** | **10.95%** | 9.76% | Noisy Quick lead; one Quick run was 4.22 |
| Scan resistance | **6.213** | 3.587 | 50.95% | **50.96%** | PackedGen by about 73% |
| Large objects | 1.363 | **1.631** | 77.25% | **81.55%** | Quick by about 20% plus quality |
| Stampede | **34.57** | 9.53 | **99.45%** | 99.41% | PackedGen by about 263% |

This is exploratory single-host evidence, not a universal winner claim. It does
show that the retained implementation now leads five of eight throughput cells,
is close on resident reads, and has one clear remaining general gap: retention
quality for pressure, graph, and especially expensive large objects.

## Tiered high-reuse finalist — 2026-08-31

Five alternating 600,000-operation pairs compared the preceding gate-2 build
with base gate 2 at 60% reuse plus high gate 8 at 75%. Higher throughput and hit
rate are better:

| Workload | Median throughput change | Hit-rate change | Outcome |
| --- | ---: | ---: | --- |
| Pressure skew | **+4.8%** | approximately 0 points | Base behavior preserved |
| Graph traversal | **+13.1%** | **+3.50 points** | Retain |
| Version churn | -2.1% | approximately 0 points | Timing noise; extended latency median was neutral |
| Scan resistance | +1.0% | 0 points | Neutral |
| Large objects | **+10.7%** | **+2.41 points** | Retain |

All five graph and all five large-object pairs improved both throughput and hit
rate. Graph p99.9 improved by 28–34% in three paired latency runs. Seven longer
version-churn pairs had median p99 and p99.9 changes of approximately zero.

The next table compares this tiered finalist with QuickCache. Values are medians
of three fresh processes, four workers, 50,000 entries, and 600,000 operations
per worker:

| Workload | PackedGen Mops/s | Quick Mops/s | Packed hit | Quick hit | Direction |
| --- | ---: | ---: | ---: | ---: | --- |
| Resident hot | 48.32 | **50.89** | **100.00%** | 99.70% | Quick by about 5% |
| Cold fill | **7.56** | 3.97 | 0% | 0% | PackedGen by about 90% |
| Pressure skew | **13.63** | 12.44 | 75.23% | **79.17%** | PackedGen by about 10%; Quick quality |
| Graph traversal | **1.631** | 1.332 | 80.72% | **82.26%** | PackedGen by about 22%; quality gap 1.54 points |
| Version churn | 7.015 | **8.667** | **11.21%** | 10.29% | Noisy Quick lead |
| Scan resistance | **6.985** | 3.763 | 50.79% | **50.80%** | PackedGen by about 86% |
| Large objects | 1.535 | **1.720** | 79.54% | **81.70%** | Quick by about 11%; quality gap 2.16 points |
| Stampede | **25.26** | 10.03 | 99.55% | **99.57%** | PackedGen by about 152% |

Three 200,000-operation latency runs produced these medians. Lower is better:

| Workload | PackedGen p99 µs | Quick p99 µs | PackedGen p99.9 µs | Quick p99.9 µs |
| --- | ---: | ---: | ---: | ---: |
| Resident hot | **0.209** | 0.292 | **0.334** | 0.750 |
| Cold fill | **1.209** | 2.000 | **3.459** | 6.335 |
| Pressure skew | **1.125** | 1.625 | 2.833 | **2.625** |
| Graph traversal | **6.211** | 8.799 | 21.839 | **16.215** |
| Version churn | **1.208** | 1.750 | **4.711** | 4.751 |
| Scan resistance | **1.250** | 2.000 | **3.583** | 6.543 |
| Large objects | 9.423 | **9.375** | 14.215 | **12.879** |
| Stampede | **0.167** | 1.209 | **1.209** | 5.419 |

PackedGen now wins p99 in seven cells and ties closely on large objects.
QuickCache still has the stronger p99.9 under pressure, graph traversal, and
large objects. These runs remain local exploratory evidence rather than a
production winner claim.

## Epoch-lazy frequency aging — 2026-08-31

The preceding frequency sketch performed one full array-wide atomic halving on
the request that crossed each aging boundary. At 50,000 expected entries the
power-of-two sketch has 262,144 counters. The retained encoding shares the
existing counter byte between a four-bit frequency and four-bit epoch. An aging
boundary is now O(1); counter aging is calculated when either of its two hash
locations is next observed or estimated.

Five alternating 600,000-operation pairs compared lazy aging with the preceding
tiered binary. Hit rates stayed effectively unchanged. Higher throughput is
better:

| Workload | Median paired throughput change | Direction |
| --- | ---: | --- |
| Pressure skew | **+5.4%** | 3/5 pairs positive; noisy but favorable median |
| Graph traversal | **+2.0%** | 3/5 positive; high process variance |
| Version churn | -0.8% | Near noise; frequency enforcement is inactive below 60% reuse |
| Large objects | +0.2% | Neutral |

Ten longer pressure and version latency pairs made the tail direction clearer:
pressure p99.9 improved in 7/10 pairs with an approximately 1.9% median
reduction, and version p99.9 improved in 7/10 with an approximately 7.7%
median reduction. Five graph pairs improved p99.9 by about 3.7% at the median;
large-object p99.9 was neutral at about +0.3%. Lower latency is better.

The current three-process comparison against QuickCache used four workers,
50,000 entries, 600,000 operations per worker for throughput and 300,000 for
latency:

| Workload | Packed Mops/s | Quick Mops/s | Packed hit | Quick hit | Packed p99 µs | Quick p99 µs | Packed p99.9 µs | Quick p99.9 µs |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Pressure skew | **13.124** | 12.447 | 75.15% | **79.17%** | **1.042** | 1.583 | **2.167** | 2.625 |
| Graph traversal | **1.635** | 1.326 | 80.76% | **82.27%** | **6.003** | 8.083 | **15.047** | 15.335 |
| Version churn | 7.053 | **7.931** | **11.19%** | 10.28% | **1.333** | 1.542 | 4.627 | **3.793** |
| Large objects | 1.538 | **1.713** | 79.58% | **81.71%** | 8.879 | **8.711** | 13.127 | **12.375** |

This closes the local p99.9 gap in pressure and graph at these seeds, but not
the production tail problem. PackedGen maximum samples were still much more
volatile for graph and large objects: medians were about 3.89 ms versus 0.157
ms and 0.513 ms versus 0.057 ms, respectively. Maximums are scheduler-sensitive,
yet the repeated gap requires open-loop and maintenance-pause attribution.

## Hard-ceiling tail attribution and liveness — 2026-08-31

Diagnostics first measured only pressure paths. In five graph runs the longest
foreground capacity enforcement was 1.4–4.3 ms and the maximum logical
operation was 1.5–4.3 ms. Large-object runs had no foreground enforcement and
0.17–0.20 ms operation maxima. This attributed the repeated graph maximum to
hard-limit coordination rather than source allocation or large-value
reclamation.

Draining exactly to the hard ceiling removed the millisecond maximum but caused
roughly 1,100–1,300 tiny foreground enforcements and consistently worsened
p99.9. The retained compromise removes the actual overage plus 1/1024 of the
soft capacity. Across ten graph A/B pairs:

- maximum operation latency fell by about 94% in all ten pairs, from roughly
  3.9–4.5 ms to 0.21–0.25 ms in the balanced runs;
- median paired throughput moved about -0.5%, with high host variance;
- p99.9 improved in six of ten pairs and about 1.3% at the median; and
- hit rate and final capacity stayed stable.

The same investigation exposed a pre-existing liveness failure in version
churn. Scheduled maintenance could rebuild while worker-local guards were
refreshed but idle at a barrier. Refresh eagerly pinned the successor writer
generation, so rebuild waited for the guard while the guard's thread waited for
rebuild. A deterministic test failed before the fix and completes afterward.
The exact formerly hanging workload now finishes at 5.10 Mops/s, p99.9 5.13
microseconds, maximum 86 microseconds, four rebuilds, zero maintenance errors,
zero active readers, and zero retired values.

Pressure timing initially shared the broad `cache-diagnostics` feature used by
the neutral adapter. That observer changed the candidate being measured. The
timers and their cache object are now compiled only by
`cache-pressure-timing`; the ordinary comparison build reports zero timing
counters and has no pressure-timing state.

### Latest observer-free QuickCache baseline

These numbers describe the current dirty worktree after the liveness and tail
fixes. They supersede older tables for current-worktree selection, but they do
not erase the older immutable experiment history. An initial rerun explicitly
forced guarded admission and measured a real 9.00 Mops/s pressure result. A
fresh A/B then identified that override as the regression; the table below uses
the adapter's cache-level default. Medians use three runs, four workers, 50,000
entries, and 600,000 operations per worker. Higher throughput and hit rate are
better; lower latency is better.

| Workload | Packed Mops/s | Quick Mops/s | Packed hit | Quick hit | Packed p99 us | Quick p99 us | Packed p99.9 us | Quick p99.9 us | Packed max us | Quick max us |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Resident hot | 47.920 | **49.341** | **100.00%** | 99.69% | **0.208** | 0.250 | **0.375** | 0.792 | **28.2** | 45.7 |
| Cold fill | **6.929** | 4.002 | 0% | 0% | **1.166** | 1.917 | **3.583** | 4.667 | 181.0 | **148.2** |
| Pressure skew | **13.941** | 12.295 | 75.19% | **79.22%** | **1.125** | 1.459 | **2.125** | 2.459 | 214.4 | **53.3** |
| Graph traversal | **1.637** | 1.335 | 80.72% | **82.28%** | **5.667** | 7.043 | **10.423** | 14.463 | **229.9** | 602.1 |
| Version churn | 7.002 | **7.041** | **11.19%** | 10.29% | **1.333** | 1.541 | 4.083 | **3.041** | **80.1** | 98.8 |
| Scan resistance | **6.643** | 3.574 | 50.79% | **50.80%** | **1.291** | 2.000 | **3.625** | 6.587 | **35.6** | 170.8 |
| Large objects | 1.529 | **1.696** | 79.56% | **81.72%** | 8.215 | **8.083** | 11.879 | **11.335** | 942.6 | **80.5** |
| Stampede | **24.532** | 9.893 | **99.57%** | 99.57% | **0.167** | 1.250 | **1.375** | 5.543 | 186.5 | **45.9** |

The graph maximum is now hundreds of microseconds rather than milliseconds,
which validates the bounded foreground change. PackedGen leads pressure by
about 13%, graph by about 23%, cold fill by about 73%, scan resistance by about
86%, and stampede by about 148%. Resident throughput is within 3% and version
churn is effectively tied. Large objects remain the clear turnover gap:
QuickCache is about 11% faster, retains about 2.2 more hit-rate points, and has
a much smaller maximum sample. Pressure also performs roughly 96,000 extra
source loads per 2.4 million operations despite winning cache-path throughput.
The next policy work must reduce those misses without giving back scan
resistance or density.

## Blocked doorkeeper locality — 2026-08-31

The admission doorkeeper previously mapped its two membership bits across the
whole filter. Every miss therefore loaded and updated two unrelated atomic
words in the current generation, while also loading the corresponding two words
from the previous generation. The retained blocked layout selects one word from
the first hash and places two distinct bits in that word. It preserves the
filter allocation and two-bit membership test while halving the atomic updates
and improving cache-line locality. A failure-first test proves one observation
changes exactly one word and sets both bits.

The following current-versus-frozen-prior-binary throughput comparison uses
four workers, 50,000 entries, 600,000 operations per worker, exact paired seeds,
and alternating execution order. The ordinary cells use five pairs. Resident
hot uses ten longer pairs at 1.2 million operations per worker because its
initial five-pair result suggested a possible code-layout regression. Higher is
better.

| Workload | Prior Mops/s | Blocked Mops/s | Median paired change | Blocked wins |
| --- | ---: | ---: | ---: | ---: |
| Resident hot | 49.809 | 49.677 | -0.80% | 4/10 |
| Cold fill | 7.084 | 7.822 | +9.73% | 5/5 |
| Pressure skew | 13.362 | 14.220 | +5.46% | 4/5 |
| Graph traversal | 1.588 | 1.703 | +7.23% | 5/5 |
| Version churn | 6.716 | 6.993 | +4.60% | 5/5 |
| Scan resistance | 6.700 | 7.192 | +4.23% | 4/5 |
| Large objects | 1.535 | 1.545 | +0.64% | 4/5 |
| Stampede | 24.524 | 24.968 | +1.81% | 4/5 |

Resident pairs ranged from -7.99% to +3.82%, despite the timed resident hit path
never observing the doorkeeper. Its -0.80% paired median is inside the local
noise envelope rather than evidence of a changed read path. Logical bounds,
correctness, maintenance errors, and per-entry memory were unchanged. Median
hit-rate movement stayed within 0.10 percentage point in every cell; graph and
large-object quality moved slightly upward.

Latency was checked independently. Three paired runs covered all eight cells;
the four initially ambiguous cells received seven fresh pairs. Lower is
better.

| Workload | Median paired p99 change | Median paired p99.9 change |
| --- | ---: | ---: |
| Resident hot | 0.00% | 0.00% |
| Cold fill | -6.87% | 0.00% |
| Pressure skew | -7.97% | -6.25% |
| Graph traversal | -5.65% | -3.77% |
| Version churn | -6.42% | 0.00% |
| Scan resistance | -9.30% | -2.24% |
| Large objects | -1.03% | +0.36% |
| Stampede | 0.00% | -20.03% |

The +0.36% large-object p99.9 movement is below one histogram step in practical
terms: the median absolute p99.9 improved from 11.255 to 11.087 microseconds,
and its median paired maximum improved 27.6%. This candidate is retained because
it improves every miss-heavy throughput cell, keeps resident and stampede hit
latency unchanged, adds no RAM or unsafe code, and shows no reproducible tail or
policy-quality regression. The immutable-revision and independent-reproduction
gates remain open.

## Reservoir preserves CLOCK second chances — 2026-08-31

Victim collection clears each sampled entry's access bit and ranks accessed
entries behind unaccessed entries. Previously, every unremoved key was then
saved in the victim reservoir, including entries whose access bit had just
earned a second chance. A following capacity drain could therefore evict an
accessed survivor from the reservoir before a new native sampling round.

The retained change omits non-expired accessed survivors from the reservoir.
They remain resident and must be sampled again before eviction. Expired entries
remain eligible regardless of their access bit. The implementation preserves
victim order across 64-key reservoir chunks, adds no per-entry metadata or
unsafe code, and reduces temporary reservoir key storage. Failure-first tests
cover both filtering semantics and order across multiple chunks.

The throughput A/B uses the frozen blocked-doorkeeper binary as the baseline,
four workers, 50,000 entries, exact paired seeds, and alternating order. Most
cells use five pairs at 600,000 operations per worker. Resident and scan use ten
longer pairs at 1.2 million operations per worker. Higher is better.

| Workload | Median paired throughput change | Policy-quality movement |
| --- | ---: | ---: |
| Resident hot | +0.63% | unchanged |
| Cold fill | +5.36% | unchanged |
| Pressure skew | +5.74% | +0.20 hit-rate point; 4,659 fewer loads |
| Graph traversal | +2.44% | +0.21 hit-rate point; 64,586 fewer loads |
| Version churn | +4.44% | unchanged |
| Scan resistance | +5.19% | unchanged |
| Large objects | +1.08% | +0.26 hit-rate point; 23,306 fewer loads |
| Stampede | +1.70% | effectively unchanged |

Large-object throughput improved in all five pairs, as did cold fill, version,
and stampede. Graph won three of five pairs while improving hit rate in every
pair. Pressure won four of five and also improved hit rate in every pair. The
optimized reservoir builder removed an initial scan-throughput concern; the
ten longer scan pairs then measured a +5.19% median with no material hit-rate
movement.

Latency used three pairs for the initial eight-cell screen and seven fresh pairs
for resident, version, scan, and large objects. Lower is better.

| Workload | Median paired p99 change | Median paired p99.9 change |
| --- | ---: | ---: |
| Resident hot | -0.48% | -0.30% |
| Cold fill | 0.00% | -3.42% |
| Pressure skew | -7.66% | -4.15% |
| Graph traversal | -0.78% | -4.19% |
| Version churn | -0.08% | +1.06% |
| Scan resistance | -3.08% | -1.16% |
| Large objects | -0.54% | -1.77% |
| Stampede | 0.00% | +0.08% |

The small version and stampede p99.9 movements are below a stable histogram
step and did not repeat as broader percentile regressions. Maximum single
samples remained volatile in both directions, including resident hot where the
changed eviction path is never entered. This candidate is retained because all
eight throughput medians improved, policy quality improved in every pressure
cell, measured percentiles stayed neutral or improved in the remaining gap,
and entry RAM is unchanged.

A final three-pair throughput check compared the rebuilt retained source with
the current QuickCache control. This is a directional local check rather than a
replacement for the preregistered seven-pair qualification. Positive paired
change means PackedGen was faster.

| Workload | PackedGen Mops/s | QuickCache Mops/s | Median paired difference |
| --- | ---: | ---: | ---: |
| Resident hot | 48.269 | **54.787** | -10.02% |
| Cold fill | **7.946** | 3.942 | +103.62% |
| Pressure skew | **13.332** | 12.457 | +6.92% |
| Graph traversal | **1.656** | 1.337 | +23.78% |
| Version churn | 7.066 | **8.787** | -22.68% |
| Scan resistance | **6.857** | 3.631 | +87.33% |
| Large objects | 1.561 | **1.692** | -7.83% |
| Stampede | **24.792** | 9.874 | +146.11% |

PackedGen won five of eight throughput cells in all three pairs. The large
object gap narrowed from about 9.5% in the preceding batch to 7.8%, while its
hit-rate deficit narrowed from roughly 2.14 to 1.86 points. QuickCache remained
ahead there, so the policy gap is improved rather than solved. QuickCache was
also unusually strong in resident and version churn in this batch. The frozen
PackedGen-binary A/B is the attribution control: the retained change improved
version churn by about 4.4% and resident by about 0.6%, so those comparator gaps
are not regressions caused by the reservoir change.

## Native doorkeeper versus QuickCache

One exact-seed directional throughput run used the native doorkeeper, async
eviction, batch 256, and overlay 2×. These are single local samples, not a
publishable ranking.

| Workload | PackedGen Mops/s | QuickCache Mops/s | PackedGen hit | Quick hit | PackedGen RSS | Quick RSS | Direction |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| Resident hot | 20.44 | **52.12** | **100.00%** | 99.67% | **15.4** | 19.3 | Quick throughput; PackedGen density |
| Cold fill | **6.44** | 4.09 | 0% | 0% | **25.5** | 40.3 | PackedGen |
| Pressure skew | 7.00 | **10.54** | 67.86% | **74.68%** | 47.2 | **34.8** | QuickCache |
| Graph traversal | 1.00 | **1.30** | 66.28% | **81.96%** | 213.1 | **42.1** | QuickCache |
| Version churn | 5.19 | **8.29** | **9.15%** | 6.84% | 51.4 | **38.8** | Mixed; Quick throughput |
| Scan resistance | **4.88** | 3.59 | 51.60% | **51.91%** | **25.6** | 38.2 | PackedGen |
| Large objects | 0.86 | **1.55** | 65.89% | **80.63%** | 388.6 | **230.3** | QuickCache |
| Stampede | **23.78** | 9.76 | 99.55% | **99.57%** | 10.5 | **6.8** | PackedGen throughput |

Cold fill rejects one-off candidates only after the first 50,000 admissions;
932,590 of one million later admission attempts were rejected before cache
allocation. That is legitimate cache admission behavior, not equivalent work
to QuickCache admitting every miss, so source-load and endpoint tests must
remain part of later comparisons.

## Exact-seed latency

| Workload | PackedGen p99 µs | QuickCache p99 µs | Better |
| --- | ---: | ---: | --- |
| Resident hot | **0.208** | 0.333 | PackedGen |
| Cold fill | **1.083** | 1.958 | PackedGen |
| Pressure skew | **0.958** | 1.584 | PackedGen |
| Graph traversal | 17.807 | **8.083** | QuickCache |
| Version churn | **1.167** | 1.500 | PackedGen |
| Scan resistance | **1.250** | 2.000 | PackedGen |
| Large objects | 10.463 | **9.167** | QuickCache |
| Stampede | **0.167** | 1.209 | PackedGen |

The maximum-latency samples remain noisy and PackedGen still has worse graph
and large-object tails. Peak RSS, allocator-live bytes, open-loop queuing, CPU,
and maintenance-pause histograms remain required before a production claim.

## Verification

- Red/green scalar doorkeeper test: first one-off pressure key is rejected and
  its second sighting is admitted while the cache remains bounded.
- Red/green blocked-doorkeeper test: one observation updates one atomic word,
  sets two distinct membership bits, and the repeated observation is seen.
- Red/green CLOCK-reservoir test: a non-expired accessed survivor is omitted,
  while an unaccessed and an expired candidate remain eligible.
- Filtered reservoir order is preserved across multiple 64-key chunks and an
  already-consumed victim prefix.
- Guarded admission-batch doorkeeper test.
- Frequency gate rejects a low-evidence candidate behind frequent victims.
- Adaptive frequency stays on second-sighting admission for low-reuse traffic
  and activates after published high-reuse reads.
- Tiered frequency keeps its base gate below 75% reuse, raises the maximum gate
  above that threshold, and rejects weaker, earlier, or out-of-range tiers.
- Failure-first frequency-aging tests prove that a boundary changes only the
  two observed counter bytes and that three lazy epochs match three eager
  halvings before epoch wrap.
- Sampled resident frequency plus reservoir revalidation preserves a hot entry
  through repeated admitted churn.
- Eight-writer variable-weight replacement and eviction stress reconstructs
  live entries and weight independently, and proves exact capacity accounting.
- Zero population-hint configuration test.
- Concurrent rotation/admission test with four writers.
- Native sampling test proves that only allocated adaptive key classes are
  visited.
- Failure-first maintenance test proves that a never-expiring cache does not
  advance the expiration/victim scan cursor; runtime and bulk-load TTL tests
  prove that expiration remains active after TTL use.
- Failure-first async tests prove entry and byte enforcement remove hard-limit
  overage plus bounded cushion, and that a contended writer can return as soon
  as another owner crosses below the hard ceiling.
- A refreshed guarded writer no longer blocks adaptive rebuild; long guarded
  admission batches reacquire their required internal reservation.
- Pressure timing is red/green tested both enabled and disabled.
- `cargo test --lib --all-features`: 119 passed.
- `cargo test --test direct_packed_cache --all-features`: 66 passed.
- `cargo test --test direct_cache_differential --all-features`: 2 passed; one
  long soak remains ignored.
- `cargo test --test atomic_generation_map --all-features`: 47 passed.
- `cargo clippy --all-features --all-targets -- -D warnings`: passed.
- Formatting and whitespace checks: passed.

## Current conclusion

The profiler-guided and tiered-policy changes remain useful and add no per-entry
RAM. Epoch-lazy aging removed the array-wide foreground reset. The newest work
also closes the repeated millisecond graph maximum and fixes a reproducible
guard/rebuild deadlock. Those are production-relevant wins.

The blocked doorkeeper and CLOCK-reservoir correction are the newest retained
optimizations. They add no per-entry memory or unsafe code. The latest frozen
binary A/B improved all eight throughput medians; pressure, graph, and large
objects also improved hit rate. In the final directional QuickCache check,
PackedGen led cold fill, pressure, graph, scan, and stampede. QuickCache led
resident throughput by about 10%, version churn by about 23% in an unusually
strong comparator batch, and large objects by about 7.8%; the large-object hit
gap narrowed to roughly 1.86 points.

PackedGen retains the denser representation, but the full matrix must still be
repeated on a final immutable revision. The next work is a candidate-neutral
policy improvement for the remaining large-object gap and an explanation of
the current version-churn comparator variance. Do not weaken scan resistance,
add per-entry metadata, or hide observer cost to manufacture a win.
