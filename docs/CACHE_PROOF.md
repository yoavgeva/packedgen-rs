# PackedGen cache proof protocol

Date: 2026-08-26

PackedGen is considered a production candidate only when it passes correctness,
memory, throughput, latency, and endurance gates on reproducible workloads. A
single favorable operation or machine is not enough.

## Current status

| Gate | Evidence today | Status |
|---|---|---|
| Sequential cache semantics | Four deterministic 50K-operation traces against a `HashMap` model, including binary keys, reads, misses, insert, replace, insert-if-absent, delete, touch, immediate expiry, accounting, and maintenance | Pass in CI-sized coverage |
| Concurrent publication and maintenance | Eight writers over disjoint mixed-width keys while maintenance repeatedly rebuilds; exact final values, entry count, and weight are checked | Pass in CI-sized coverage |
| Long reclamation/rebuild soak | Configurable release differential/reclamation/rebuild workload | Two-million-operation local soak passes; long pinned-host run pending |
| Operation matrix | Alternating-order DirectPackedCache plus inline and Arc Papaya comparisons cover hit, miss, replace, insert, delete, touch, read-heavy traffic, hard-cap pressure, and hot-set pressure at 1/2/4/8/16 threads | Current 8-thread 95%-read result is effectively tied with inline Papaya (121.397/121.470 Mops/s); insert and TTL touch still trail; pinned Linux pending |
| Memory matrix | Process-isolated requested allocation for DirectPackedCache plus inline and Arc equal-metadata Papaya controls at 100K and 1M entries, plus repeated-turnover live allocation/RSS | Current Direct/inline-Papaya rows are 106.566/138.579 B at 100K and 105.481/133.693 B at 1M; Direct cuts non-payload overhead by about 40% |
| Tail latency | Per-operation p50/p95/p99/p99.9 while writes and rebuilds run | After empty-epoch reclamation, current local Direct/Papaya p50/p95/p99/p99.9 are 42/42, 167/166, 250/209, and 375/292 ns; sub-microsecond budget passes locally, pinned Linux pending |
| Memory stability | Repeated full key turnover with live allocation, allocations, RSS, mutation throughput, and maintenance time per cycle; wall-clock endurance runner enforces per-cycle growth and sampled-trend bounds | Ten-turnover 200K plateau and 325-turnover harness smoke pass locally; 24-hour pinned-Linux run pending |
| Memory safety | Four Loom models cover publication, replacement, removal, reader entry, forced empty-epoch advancement, and three-epoch reclamation; nine focused Miri tests, all 38 ordinary direct-cache ASan tests, and two ordinary/prepared TSan tests pass | Passes locally; destination epochs now require quiescence even when their retirement queues are empty |
| Minimum Rust version | Rust 1.88 all-target/all-feature check | Pass locally; dedicated CI job added |
| Independent reproduction | Clean Linux host instructions and machine-readable result bundle | Partial |
| Distribution | The owned companion is now `packedgen-opthash 0.1.0`; both companion and root package archives verify in publication order | Code blocker fixed; publish the companion before `packedgen` |
| Dependency advisories | Fresh RustSec scan plus a machine-checked dependency-path exception | No vulnerability advisory; the `fxhash 0.2.1` unmaintained notice is confined to unused PtrHash aliases and fails review if that path or source usage changes |

The two new tests live in `tests/direct_cache_differential.rs`. The ordinary
test is deliberately deterministic: failures report both the seed and exact
operation number, so a mismatch can be replayed.

## Running the proof suite

Fast correctness and static checks:

```text
scripts/cache_proof.sh --quick
```

Verify the declared minimum Rust version independently:

```text
scripts/cache_proof.sh --msrv
```

The release comparison matrix:

```text
scripts/cache_proof.sh --bench
```

Results are written beneath `target/cache-proof/<UTC timestamp>/` as
`throughput.csv`, `latency.csv`, and `memory.csv`. Throughput is reported in
Mops/s, where higher is better. Latency percentiles, requested bytes per entry,
and instability percentages are lower-is-better. The throughput file also
reports final residency and read hit rate so an implementation cannot appear
faster merely by retaining fewer records.

The default benchmark uses 200K resident records, two million operations, nine
alternating samples, and 1/2/4/8/16 threads. Environment variables can make a
run larger without changing the script:

```text
PACKEDGEN_PROOF_ENTRIES=1000000 \
PACKEDGEN_PROOF_OPERATIONS=20000000 \
PACKEDGEN_PROOF_WRITE_OPERATIONS=4000000 \
PACKEDGEN_PROOF_SAMPLES=15 \
scripts/cache_proof.sh --bench
```

The proof runner measures the portable default hash schedule. An accelerated
variant can be recorded separately with
`PACKEDGEN_PROOF_FEATURES=shared-gx`; it is never silently mixed into the
default result bundle.

Run the longer concurrent reclamation and rebuild test separately:

```text
PACKEDGEN_SOAK_OPERATIONS=10000000 scripts/cache_proof.sh --soak
```

Focused memory-safety checks use the installed nightly toolchain:

```text
scripts/cache_proof.sh --miri
scripts/cache_proof.sh --sanitizers
```

Run repeated 100% key-turnover memory checks independently for PackedGen and
inline Papaya:

```text
_RJEM_MALLOC_CONF='narenas:1,dirty_decay_ms:0,muzzy_decay_ms:0,tcache:false' \
PACKEDGEN_SOAK_FEATURES='gxhash,jemalloc-probe' \
PACKEDGEN_SOAK_ENTRIES=100000 \
PACKEDGEN_SOAK_CYCLES=30 \
scripts/cache_proof.sh --memory-soak
```

Run the full wall-clock endurance gate on a pinned Linux host:

```text
PACKEDGEN_PROOF_RUN_ID=linux-24h scripts/cache_proof.sh --endurance
```

The endurance defaults are 200K resident entries, 16 writers, 86,400 seconds,
tuned jemalloc, a 25% hard per-cycle live-allocation growth ceiling, and a 5%
maximum late-versus-early sampled trend after warm-up. Every turnover is
checked, while one row per 1,000 cycles plus the final row is written to keep
the result bundle bounded. The CSV and compiler/host configuration are stored
beneath `target/cache-proof/<run id>/`. The limits, report interval, duration,
entry count, and thread count are configurable through the
`PACKEDGEN_ENDURANCE_*` variables.

The Miri mode uses permissive provenance because DirectPackedCache deliberately
stores exposed pointer addresses in its compact `u64` index. Rust defines that
conversion through the exposed-provenance APIs, but strict-provenance Miri
cannot model recovering a pointer from an integer-only index and therefore
cannot fully prove this layout. Nine focused tests pass: stale arena handles,
partial direct reservations, guarded generation insertion, values held across
replacement and cache drop, mixed block/boxed retirement, concurrent
replacement with readers and reclamation, guarded-admission accounting and
rebuild, exact prepared-replacement recycler drops, and unbounded-admission
accounting. A separate
bulk-frozen build currently reaches a Miri failure inside PtrHash's
Rayon/Crossbeam dependency rather than PackedGen.

AddressSanitizer passes all 38 ordinary DirectPackedCache tests. Fully
instrumented-standard-library ThreadSanitizer runs pass the 4-writer/4-reader,
20K-operation replacement and pinned-read stress test and the prepared-value
recycler concurrency test. The exhaustive small-state Loom models described in
the blocker-closure pass below now cover the publication and reclamation
protocol that these dynamic checks cannot prove by themselves.

`--all` runs quick checks, the minimum-version check, the comparison matrix,
and the release soak. The 24-hour endurance and nightly safety modes remain
explicit because of their runtime and toolchain requirements. The benchmark
deliberately retains losing results; it is evidence collection, not a
benchmark that fails when a competitor wins.

## 2026-08-26 final optimization review

This review changed no production cache behavior. The release example still
matches the preserved best-known binary byte for byte. It fixed one stale Miri
test selector in the proof script and added a minimal DirectPackedCache example
to the README.

Correctness and compatibility checks passed: the default workspace and root
all-feature test matrices, Rust 1.88 minimum-version check, warnings-denied
Clippy and Rustdoc, formatting, deterministic differential traces, a
two-million-operation release soak, nine focused Miri cases, all 38 ordinary
DirectPackedCache tests under AddressSanitizer, and two ThreadSanitizer
concurrency cases. The deliberately nightly-only `opthash` feature is not part
of the stable all-workspace/all-feature matrix.

The representative local Redis-like fixture used 100K variable binary keys,
one million operations, five alternating samples, and the `operation-batch`
feature. Higher throughput is better:

| Operation | Direct 1T | Papaya 1T | Direct 8T | Papaya 8T |
|---|---:|---:|---:|---:|
| Read hit | **15.855** | 12.417 | 139.926 | **141.025** |
| Read miss | **67.526** | 65.724 | **520.258** | 491.360 |
| Replace hit | 3.880 | **4.172** | **19.465** | 11.617 |
| Insert miss | 9.762 | **13.501** | 47.237 | **65.677** |
| Remove hit | 8.532 | **14.512** | **31.228** | 23.760 |
| Remove miss | **69.671** | 65.770 | **528.611** | 487.686 |
| TTL touch | 15.767 | **21.424** | 160.038 | **208.629** |
| 95%-read mix | 13.124 | **13.588** | 97.138 | **115.588** |

At the same 100K-record point, Direct used 106.582 requested B/entry and
102,153 allocations versus inline Papaya at 138.579 B/entry and 200,006
allocations: 23.1% less requested RAM and about 49% fewer live allocations.
The 20-cycle turnover check remained bounded at roughly 112-114 live B/entry,
while inline Papaya was usually about 139 B/entry with periodic 162 B/entry
spikes. Process RSS was approximately level on this small allocator-sensitive
run, so requested allocation—not RSS—is the supported density claim until a
pinned Linux endurance run is complete.

In the concurrent 95%-read latency probe, both caches had 42 ns p50. Direct's
p95/p99/p99.9 were 208/292/375 ns versus Papaya's 167/250/333 ns, and Direct's
rebuild-sensitive maximum was much higher. This keeps tail latency, sustained
new-key admission, and TTL touch on the blocker list.

The modified Elastic implementation is now the explicitly owned
`packedgen-opthash 0.1.0` companion instead of claiming compatibility with
upstream `opthash 0.10.3`. Both package archives verify locally in the required
publication order through `scripts/package_release.sh`; the companion must be
published before the root crate. A fresh RustSec scan found no vulnerability advisory, but reports
the unmaintained transitive `fxhash 0.2.1` dependency through `ptr_hash`.

At that review checkpoint, the result was a technically strong, unusually
memory-dense experimental cache rather than a general production or drop-in
Papaya/Redis replacement. The blocker-closure pass below addresses package
independence, formal modeling, and dependency policy. Pinned-Linux endurance,
latency, and independent reproduction remain necessary for broad production
claims.

## 2026-08-26 blocker-closure pass

The package and formal-model blockers are now fixed in source. The modified
Elastic implementation is the honestly named `packedgen-opthash 0.1.0`
companion, and `scripts/package_release.sh` verifies both archives in the order
they must be published.

The original Loom model initially failed and exposed a real direct-arena reclamation
defect. The collector reused physical epoch numbers `0 -> 1 -> 2 -> 0`, so a
reader paused before registration could accept the same number after a full
cycle. Replacing that value with a monotonic token while retaining the same
three physical counter slots removed the ABA without adding RAM. Loom then
showed that a maintenance-side counter load could observe stale zero during a
registration race. The reclaimer now uses a zero-delta atomic RMW only when it
checks a physical epoch's quiescence.

A later forced-maintenance model found the remaining empty-epoch case. A reader
could enter epoch 0, maintenance could advance through the empty slot, and the
reader could then load a value retired under epoch 1. Reusing epoch 0 without a
quiescence check detached that reader from epoch 1's retirement queue. Every
destination epoch now proves quiescence before reuse, including empty ones.
The regression fails on the old collector and passes on the retained fix.

All four Loom models now pass exhaustively. Nine Miri cases, 38 ordinary ASan cache
tests, both TSan concurrency cases, the deterministic differential suite, and
the two-million-operation release soak also pass after the fix.

The repeated 100K-entry comparison after this change produced:

| Operation | Direct 1T | Papaya 1T | Direct 8T | Papaya 8T |
|---|---:|---:|---:|---:|
| Read hit | **22.153** | 20.894 | **178.060** | 160.307 |
| Read miss | **72.210** | 66.818 | **550.585** | 498.608 |
| Replace hit | **5.888** | 4.746 | **23.445** | 12.282 |
| Insert miss | 12.936 | **14.932** | 52.039 | **64.136** |
| Remove hit | 12.692 | **15.504** | **34.968** | 24.193 |
| Remove miss | **74.307** | 65.967 | **568.626** | 495.366 |
| TTL touch | 23.292 | **34.133** | 187.046 | **254.450** |
| 95%-read mix | 18.248 | **19.452** | 121.397 | **121.470** |

Higher Mops/s is better. Direct leads most read/miss/replace and multicore
delete rows; Papaya still leads new insertion, TTL touch, scalar delete, and
the scalar mix. The eight-thread read-heavy mix is effectively tied.

Direct uses 106.566 B/entry at 100K and 105.481 B/entry at 1M versus inline
Papaya's 138.579 and 133.693. For the 1M 64-byte-value fixture, non-payload
overhead is 41.481 versus 69.693 B/entry, a 40.5% reduction; the same memory
budget therefore retains about 26.7% more Direct entries. This overhead metric
is the stable cache-density gate because total percentage savings necessarily
shrink as the caller's unavoidable value payload grows.

The current mixed read/write latency row is Direct/Papaya 42/42 ns p50,
167/166 p95, 250/209 p99, and 375/292 p99.9. Both meet the provisional local
500 ns p99 and 1 microsecond p99.9 service budgets. The maximum remains
scheduler-sensitive and is recorded rather than used alone as a release gate.
The same mutation-heavy latency fixture reports 82.057/92.894 read Mops/s, so
the percentile result does not hide Papaya's 11.7% throughput lead in that
particular workload. The separate eight-thread 95%-read operation matrix is
effectively tied.

The optimized `operation-batch` release probe built from the safety-fixed
source is archived locally as `target/preserved/cache_probe_qsbr_safe_candidate`
with SHA-256
`81b57994bea3b22dbe2c93f3a74409897b9681b07b8227d94336fc5cdc4679c8`
and a 3,859,552-byte file size. This is the new comparison anchor; it is not
expected to match the pre-fix binary because the reclamation protocol changed.

The RustSec exception for transitive `fxhash 0.2.1` is narrow and executable:
`scripts/security_audit.sh` denies every warning except its unmaintained notice,
proves the sole path is through PtrHash, and fails if PackedGen begins using the
excepted hasher aliases. There is no reported vulnerability or patched fxhash
release. This exception should disappear when upstream makes the aliases
optional or a measured replacement clears the performance and RAM gates.

The only gate that cannot be completed on this macOS host is the 24-hour pinned
Linux result. `.github/workflows/endurance.yml` now supplies a manual,
dedicated-runner workflow with CPU affinity, bounded live-memory growth, trend
analysis, environment capture, and evidence upload. Its result remains pending
until a matching Linux runner executes it.

## Required release gates

1. No model mismatch, corruption, crash, deadlock, or accounting drift.
2. Loom, Miri, and sanitizer-clean cache publication, replacement, removal, and
   reclamation ordering.
3. Stable live allocation and RSS during a 24-hour full-capacity churn run;
   no monotonic growth after warm-up.
4. At least 35% lower non-payload overhead and 20% lower total requested memory
   than the strongest general-purpose concurrent control on the published
   64-byte-value fixture, including keys, TTL, eviction, and allocator overhead.
5. Read-heavy throughput at or above Papaya on the target multicore workload,
   or within 10% when PackedGen retains at least 25% more live records in the
   same memory budget.
6. No operation class may hide a severe regression: insert, replace, delete,
   miss, expiry, and full-cache admission are all published.
7. p99 stays below 500 ns and p99.9 below 1 microsecond during mutation and
   rebuild on the pinned Linux reference host.
8. A second machine can reproduce the direction of the memory and performance
   results from a clean checkout.

These are candidate gates, not claims that PackedGen passes them today.

## First harness smoke result

The initial Apple M4 Max smoke run used only 10K entries, 100K operations,
three samples, and one or two threads. It validates the harness; it is too
small and noisy to rank the libraries for release.

- All four 50K-operation sequential differential traces passed.
- The eight-writer concurrent-maintenance trace passed with exact final values
  and accounting.
- DirectPackedCache used 120.119 requested B/entry versus 131.410 for the
  strongest inline Papaya control: 8.6% less at this small size, below the 25%
  release gate. The older Arc Papaya control used 155.410.
- The operation smoke used the older Arc control. DirectPackedCache was level
  with or ahead of it for some read-hit, replacement, and new-insert rows; the
  stronger inline-control rerun is reported below.
- Even the Arc Papaya control led read miss, successful delete, delete miss,
  touch, and the overall read-heavy mix in several rows. DirectPackedCache
  retained a higher hot-set pressure hit rate, but Papaya completed more
  operations per second.
- Several three-sample rows had extreme variability. They are retained in the
  generated CSV but are not optimization evidence.

This result is intentionally honest: the proof framework already records both
the density advantage and the remaining operation-level weaknesses.

## Strong inline-control throughput rerun

The first stronger-control run used 200K entries, one million operations,
eight threads, and seven alternating samples. Whole-sample variability was
still high, so these are local directional measurements rather than release
numbers. Higher Mops/s is better.

| Operation | DirectPackedCache | Inline Papaya | Current direction |
|---|---:|---:|---|
| Read hit | **64.238** | 56.025 | PackedGen +14.7% |
| Read miss | 199.867 | **347.740** | Papaya +74.0% |
| Replace hit | **10.176** | 8.935 | PackedGen +13.9% |
| Insert new | 20.601 | **45.868** | Papaya 2.23x |
| Delete hit | 16.858 | **21.512** | Papaya +27.6% |
| Touch hit | 31.223 | **100.245** | Papaya 3.21x |
| 95%-read mixed | **51.009** | 43.490 | PackedGen +17.3% |
| Full-cache pressure, strict | **30.224** | 28.358 | PackedGen +6.6% |
| Full-cache pressure, async | **31.817** | 28.358 | PackedGen +12.2% |
| Hot full-cache pressure, async | **38.633** | 27.190 | PackedGen +42.1% |

This changes the optimization priorities. PackedGen is competitive on the
target mixed cache path, but its new-key, miss, delete, and touch primitives
remain important weaknesses. The inline control is now the primary baseline;
the Arc control remains in the CSV only to show the cost of shareable owned
values.

## Portable read-path optimization checkpoint

A later default-build iteration retained four changes that add no per-entry
storage: multiply-high bucket reduction replaces repeated integer division, a
pinned read guard no longer registers a redundant per-operation writer during
conditional admission, a one-way generation flag skips the writable overlay
while it is known to be empty, and a fused overlay probe returns both the cell
result and overflow-stopping proof without rereading the same controls. At that
checkpoint the portable hash schedule carried two independently randomized
FoldHash lanes into PtrHash, avoiding the former FoldHash-plus-XXH3 key rescan
while retaining a full 128-bit digest domain. A later experiment, documented
below, replaces those lanes with Rapidhash.

The tempting one-lane expansion was rejected even though it benchmarked well:
expanding 64 random bits into 128 does not restore collision entropy and would
make very large frozen generations unnecessarily fragile. The retained
two-lane schedule passed the complete atomic-generation suite, including
concurrent rebuild and exact-state tests.

On the heavily loaded local Apple host, the strongest reproducible directional
rows were as follows. Higher Mops/s is better. These are optimization evidence,
not pinned-host release claims.

| Operation | DirectPackedCache | Inline Papaya | Direction |
|---|---:|---:|---|
| Frozen read miss, 11 alternating samples | **394.701** | 346.984 | PackedGen +13.8% |
| 95%-read mix, process 1 | **64.663** | 49.042 | PackedGen +31.9% |
| 95%-read mix, process 2 | **72.870** | 50.204 | PackedGen +45.1% |
| 95%-read mix, process 3 | **62.841** | 45.555 | PackedGen +37.9% |
| New-key insertion | 30.160 | **48.268** | Papaya 1.60x |

The one-byte-per-frozen-entry negative filter was tested and rejected: it used
more RAM and did not improve the miss median. At that checkpoint requested
memory remained effectively unchanged at 105.105 B/entry; the new per-
generation state added only 176 bytes to the one-million-entry process
measurement. New-key insert, delete, and touch remained the next operation-
level targets.

Two insertion-side follow-ups were also rejected. Delaying the second hash
lane did not improve the isolated adaptive insertion result because the
compiler already removed unused work. Delaying insertion bucket derivation
also failed to improve the normalized result, so only the read probe computes
later bucket choices lazily. Exact-width key borrowing produced contradictory
alternating-run ratios on the loaded host and was reverted to the simpler
fixed-array encoding.

A one-pass randomized XXH3-128 replacement was also slower on short mixed
keys: Direct admission fell from 29.605 to 26.721 Mops/s while the inline
Papaya control stayed near 55–56 Mops/s. The portable two-lane FoldHash schedule
therefore remains the faster measured choice despite reading the key twice.

The later portable default replaces each FoldHash lane with an independently
randomized Rapidhash lane. Two lanes still produce a genuine 128-bit carried
digest; this is not the rejected 64-to-128 expansion. Simultaneous preserved-
binary comparisons put Rapidhash admission 2.6% and 2.9% above FoldHash in two
8-thread pairs. At one thread it improved read hit 2.7% and the 95%-read mix
4.3%; the 8-thread mix improved 20.0%. Read-miss median was 1.9% lower while
its low-percentile throughput improved, so isolated Linux must confirm that
small trade. The complete semantic and rebuild suites remain mandatory before
this checkpoint is considered closed.

Capacity accounting now avoids updating a dimension whose limit is disabled.
A cache without an entry limit reuses the index's exact logical length. With a
disabled weight limit, live weight remains exact at quiescence by scanning the
weight already stored in each protected entry when requested; finite weight
limits keep the original O(1) sharded counters. The scalar entry-only admission
row moved from 7.284 to 7.461 Mops/s (+2.4%); multicore absolute direction was
positive but too host-sensitive to quantify.

Direct value allocation then tested a sequence of block arenas. A global slot
counter improved scalar admission but collapsed under eight writers. Key-
sharded 1,024-entry blocks still shared counter lines and inflated memory to
109.137 B/entry through partial block tails. Thread-owned 128-entry blocks
removed those failures, but applying them to replacements lost to the system
allocator's mature thread cache. The retained split uses block slots only for
conditional absent-key admission and bulk loading, and tags known boxed
replacements in the pointer's otherwise-free low bit so retirement selects the
matching reclaimer.

At one million mixed keys and 64-byte values, the boxed insert/replace path is
105.110 B/entry with 2,008,244 allocations. The conditional-admission arena is
105.248 B/entry with 1,016,091 allocations: +0.138 requested B/entry for
992,153 fewer allocator calls. A final matched eight-thread 95%-read mix moved
from 39.336 to 42.179 Mops/s (+7.2%), while p05 moved from 16.415 to 23.443
(+42.8%). The adjacent replacement pair was effectively flat at 2.357/2.368
Mops/s after restoring boxed allocation outside the mutation stripe. An arena-
lifecycle test alternates two live caches, drops the thread-local batch owner,
and verifies that no stale block slot can be reused. Miri rejected the first
container-of reclaimer because casting `&Collector` back to its parent state
violated stacked-borrow provenance. The retained owner separates collector and
block-state allocations, registers the provenance-preserving `Arc::as_ptr`
state pointer in an atomically published immutable map, and drops the collector
before unregistering the still-live state. The focused mixed block/boxed Miri
test passes. With that lock-free registry, the final uniform-pressure pair
improved strict Direct 7.738 to 8.029 Mops/s and async Direct 16.954 to 19.502;
hot-pressure normalized medians also remained positive.

The next read-locality pass retained three changes without growing an entry.
First, the independent expiry-word load became relaxed: index publication
already orders the value, and the deadline does not publish another payload.
On the current layout, alternating preserved binaries moved the median
Direct/inline-Papaya one-thread read ratio from 1.247 to 1.495. Second,
`DirectArenaEntry` now fixes its eight-byte metadata before the value. For the
64-byte control payload this keeps metadata beside the bytes a caller reads,
instead of placing it at offset 64; three simultaneous pairs improved Direct
by 19.8%, 14.8%, and 5.5%, and requested memory remained exactly 105.110
B/entry boxed and 105.248 for conditional admission. Third, sampled CLOCK hits
avoid an exclusive `fetch_or` when the bit is already set. Uniform eight-thread
pressure improved 13.1% strict and 18.1% async with effectively identical hit
rates; hot strict was flat within noise and hot async improved 6.3%.

Three adjacent variants were rejected. A monotonic no-TTL flag regressed the
clean one-thread read row, sampling CLOCK every 32 rather than every 16 had no
repeatable gain, and prechecking victim bits before `fetch_and` produced
contradictory pressure results including an async hot regression. The retained
path therefore removes redundant writes on hits only; it does not weaken the
sampling interval or add adaptive state.

Touch gained a separate adaptive fast path. `touch(None)` on an entry that is
already non-expiring changes only the atomic CLOCK bit, so it can safely
linearize at one protected index read without taking the mutation stripe.
Expiring entries retain the locked deadline-changing path. Two runs reached
59.411 and 57.981 Mops/s versus the adjacent 41.443 locked baseline, a 40–43%
gain. Inline Papaya still led at 102.749 and 93.182 Mops/s. A same-cell CAS
variant was rejected at 32.129 Mops/s, and the concurrent replacement/touch
stress now exercises the retained path.

Delete now has a worker-guard path as well. It reuses the cache's epoch pin and
the generation read-batch reservation, while a read-only absence certificate
returns misses before the cache mutation stripe. In the first usable 8-thread
run, successful delete reached 21.196 Mops/s versus inline Papaya's 21.678
(within 2.2%), up 19.1% from Direct's adjacent 17.796 baseline. Delete miss
reached 365.720 versus Papaya's 349.378 (+4.7%) and more than doubled the older
Direct result. Omitting the absence certificate was rejected because misses
fell to 51.095 Mops/s.

A later repeat cannot rank the implementations: the host was concurrently
consuming about ten CPU cores in a virtual machine plus four near-core Python
processes, and all cache rows collapsed together. That run remains documented
as host-noise evidence. The guarded remove path passes rebuild cutover, exact
accounting, and concurrent remove/readmission tests; isolated Linux still
decides the release numbers.

Guarded new-key admission now batches its index-length publication as well.
The read-batch reservation already prevents generation cutover, so a bounded
cache can publish every exact key/value first, accumulate successful inserts
in one guard-local scalar, and apply one atomic length delta on refresh or
drop. The first version used 4,096 per-stripe counters; it improved admission
but needlessly allocated about 16 KiB per writing guard. The retained version
publishes the whole delta to the first open stripe in the batch. Generation
length now sums all signed stripe deltas before applying the nonnegative clamp,
so an insert batched on one stripe and a removal on another still cancel
exactly. There is no heap allocation, cache allocation, or per-entry state.

Two separate three-pair eight-thread sweeps preserved the direction despite
the saturated host. The scalar predecessor moved the Direct median from
31.814 to 40.114 Mops/s (+26.1%) and the median Direct/Papaya ratio from 0.631
to 0.739 (+17.1%). The final adaptive source moved 31.390 to 37.499 Mops/s
(+19.5%) and its normalized ratio from 0.599 to 0.760 (+26.8%). Inline Papaya
still led the final adjacent rows at roughly 50.5 Mops/s. At one thread the
absolute median improved 9.749 to 9.955 Mops/s (+2.1%). The eight-thread
95%-read mix moved 55.337 to 58.392 (+5.5%); normalized to Papaya it was +1.7%,
which is more credible than the raw number on this host because admission is
only a small fraction of that trace.

Refreshing after every operation makes the refresh itself dominate: retained
Direct measured about 5.9 Mops/s versus 37.5 at the normal 16,384-operation
interval. A singleton-batch detector therefore switches the next 64 insert
attempts to immediate accounting before retrying batching. The final
refresh-every-operation comparison was inconclusive under host noise (about
-3.4% absolute but +7.6% after normalization to the adjacent Papaya control),
so the documented worker guidance remains to refresh periodically rather than
per operation. Tests cover duplicate admission, insert/remove before flush,
cross-stripe cancellation, public size/weight, refresh, and rebuild. The
one-million-entry admission measurement remains exactly 105.248 B/entry and
1,016,091 allocations.

## Control-frontier and fallback-filter checkpoint

The next insertion pass retained two control-word changes. A write scan now
returns its already acquired control word through an output scalar, so the
empty-slot compare/exchange does not reload the same bucket. Keeping that word
inside `BucketScan` enlarged the hot result from 16 to 24 bytes and regressed
normalized eight-thread insertion about 13.1%; that representation was
rejected. The output-scalar version improved the three-pair eight-thread
absolute median 3.9% and its Direct/inline-Papaya normalized median 8.2%. At one
thread the normalized improvement was 6.1%.

Atomic bucket controls are append-only: occupied bytes are never cleared, a
claim always takes the first empty byte, and a second writer cannot claim while
that byte is WRITING. Therefore the only possible WRITING byte is immediately
before the first EMPTY, or the final byte when a claim fills the bucket. Reads,
writes, physical victim sampling, and scans now derive that frontier after the
EMPTY match instead of performing a second whole-word match for WRITING. Unit
tests cover every occupied-prefix length and the full-bucket case; concurrent
admission, maintenance, and replacement suites cover publication behavior.

Across six additional order-balanced eight-thread pairs, higher throughput is
better: new-key insertion improved by a median 6.2%, replacement by 5.3%, and
the 95%-read cache mix by 12.1%. All six mixed-cache pairs were positive.
Normalization to the adjacent inline Papaya control put read hit about 1.8%
above the pre-frontier ratio, while read miss was neutral to slightly positive.
The current eleven-sample same-process snapshot was:

| Operation | DirectPackedCache | Inline Papaya | Direction |
|---|---:|---:|---|
| New-key insertion | 37.381 | **49.707** | Papaya +33.0% throughput, Direct 24.8% lower |
| Read hit | **67.797** | 56.304 | Direct +20.4% |
| Replacement hit | **15.039** | 9.982 | Direct +50.7% |
| Read miss | **408.754** | 363.009 | Direct +12.6% |
| 95%-read mix | **54.186** | 52.199 | Direct +3.8% |
| Strict full-cache mix | 7.964 | **11.948** | Papaya +50.0% throughput |
| Async 1%-slack full-cache mix | **13.331** | 11.948 | Direct +11.6% |

The pressure rows are policy comparisons: strict Direct enforces capacity
synchronously, while the async row permits the documented 1% temporary slack
and worker thread. Its measured hit rate was 97.937% versus Papaya's 97.541%.
The host was simultaneously running a virtual machine and four CPU-bound
Python processes, so these rows are directional optimization evidence, not
release claims.

The adaptive fallback filter is now bounded by the learned sample working set.
At one million configured entries, the two-bit filter shrinks from 15,625 to
4,096 atomic words (about 125 KiB to 32 KiB). Marked hashes still have no false
negatives; the resulting roughly 64 bits per sampled key keeps the estimated
two-bit false-positive rate near 0.1%. Requested cache memory moved from
105.248 to **105.155 B/entry** with allocations unchanged at 1,016,091.
Three direct-only insertion pairs were effectively neutral at +0.1% median.

Four adjacent experiments were rejected: a guard-owned admission-arena cache
was +2.5% normalized at one thread but -1.3% at eight; a relaxed control load
with conditional Acquire hurt replacement about 8% at one thread and 26% in
the first eight-thread pair; low-byte tag mapping lost about 12–13% on the
one-thread read-hit row and 4% on the cache mix; and an empty-base branch that
skipped one stripe load was neutral at one thread but 9–18% slower in the two
eight-thread orderings. None remains in the source.

### Post-frontier code-shape pass

A follow-up profile again placed atomic bucket scans first and the direct arena
allocator second on the new-key path. Six narrower changes were tested against
the preserved frontier binary and then reverted:

- Inlining every scan/publication helper made the scalar read-hit row 6–8%
  slower. Restricting the hints to the write path improved one 95%-read pair
  about 4.5%, but replacement fell about 4.3%; publication-only hints then
  moved the mix about 6.8% lower.
- Checking tag candidates before decoding the append frontier improved the
  order-balanced scalar read result about 4%, but the eight-thread geometric
  direction was roughly -6.6% read, -9.6% replacement, -7.8% insertion, and
  -12.2% on the 95%-read mix.
- Deriving the occupied prefix from `leading_zeros` used fewer instructions
  than byte-wise EMPTY matching, but the scalar insertion result fell about
  4.4%. On the loaded eight-thread run its Direct/inline-Papaya ratio moved
  from 1.362 to 1.254 for reads and 1.184 to 0.838 for insertion, despite a
  positive mixed row. The broader regression overrides that isolated gain.
- Inlining expiry encoding globally improved scalar new-key insertion about
  2.9% but reduced replacement about 2.8%. Specializing only arena-backed
  admission still showed about +3.5% insertion but a repeatable roughly 3%
  mixed-cache loss.

An immutable byte-array key-slot experiment was also abandoned before
benchmarking because it required expanding unsafe code outside the deliberately
isolated cache arena. The crate continues to deny unsafe code in the index
core. The retained implementation after this pass is therefore unchanged:
the scanned-control handoff, append-frontier derivation, and bounded fallback
filter remain; none of the code-shape experiments above remains in source.

The host was under exceptional contention during this pass (a virtual machine
near ten logical CPUs plus several CPU-bound processes). Results were collected
in both order-balanced preserved-binary pairs and within-process Papaya-normalized
runs. Their purpose is rejection and direction finding; pinned Linux remains
the release gate.

### Allocator and fallback-routing knee pass

The next admission profile still placed the fixed-bucket probe first and the
thread-local direct-value allocator second. The allocator was already at its
measured knee. Forcing the local-pop helper inline reduced scalar insertion
about 3.3%. Changing its arena block from 128 slots to 256 reduced insertion
about 8.2%; 64 slots was about 1.6% lower at both one and eight threads. A
fixed 1,024-address TLS stack was effectively flat on insertion (about -0.6%
at one thread and +1.0% at two) but reduced the two-thread 95%-read mix about
5.8%. All four layouts were reverted, retaining the 128-slot `Vec` stack.

The fallback filter is a monotonic routing hint, not the publication edge for
fallback entries. Its map supplies the actual synchronization. Making the
filter mark and probe relaxed therefore preserves linearizability: a false
result racing an unfinished insertion can linearize before that insertion,
while an observed bit only causes a synchronized fallback-map probe. The
longer one-thread repeat was neutral on miss and about +1.6% on the 95%-read
mix. At two threads, within-process Direct/inline-Papaya ratios moved in the
positive direction for insertion, miss, replacement, and mix; the four-thread
mix was effectively neutral under host contention. This ordering relaxation
is retained with no RAM or allocation change.

Three adjacent filter/tag shortcuts failed the broader gate:

- Halving the filter to 32 bits per sampled key would save only 16 KiB at one
  million configured entries. Two orderings reduced two-thread insertion by
  about 6% and 20%, and four-thread direction remained negative, overriding a
  small scalar mixed-cache gain.
- Replacing multiply-high filter reduction with a power-of-two mask appeared
  positive at the original 200K size, where that fast path was not active. At
  300K entries, where the capped 4,096-word path was active, read miss fell
  about 6.2% and the 95%-read mix about 4.4% despite insertion rising 4.0%.
- Mapping a raw hash byte around the two reserved control values improved miss
  about 3.3%, but scalar insertion fell about 4.6% and the mix about 1.7%.

All three are reverted. Range/distribution tests for the filter reducer and
slot tags remain as correctness coverage. These local percentages are
directional because the same exceptionally loaded host produced wide tails;
no row supersedes the pinned isolated-Linux release gate.

### Adaptive admission probe and ABI pass

A sampled scalar admission profile moved the next investigation away from the
value allocator. About 225 of 292 worker samples were in fixed-table probing,
insertion, or index routing, while only 17 were in the arena. Four changes
targeting that index path were evaluated and reverted:

- Forcing exact-width modes for the 8/16/24/32-byte adaptive classes reduced
  one-thread insertion about 3.2% and read hit about 2.8%. Short keys correctly
  reached fallback, but avoiding variable-width encoding did not repay the
  fallback traffic and extra specialization.
- Outlining the non-adaptive body to shrink the adaptive caller initially
  appeared positive, but the longer two-thread Papaya-normalized gate reduced
  insertion about 3.8%, miss about 5%, and replacement about 8.5%. A roughly
  2.1% mixed-cache gain was not broad enough to retain it.
- Replacing the 16-byte `AtomicFixedInsert<C>` return with an 8-byte status and
  caller-owned candidate cell was effectively neutral on insertion (-0.9%)
  and reduced the longer one-thread read, replacement, and 95%-read mix about
  4.3%, 5.0%, and 5.2% respectively.
- An all-zero bucket shortcut was narrowed from reads and writes to writes
  after full-cache misses regressed. The writer-only form averaged about
  +10.5% insertion and +7.4% mix in the first order-balanced scalar pair, but
  replacement was about 3.2% lower. In the later full CRUD gate its normalized
  insertion direction remained about +9.9%, while the mixed Direct/Papaya
  ratio fell from 1.412 to 0.880. That failed the balanced-cache gate.

Changing the bucket claim from strong to weak compare/exchange produced a
byte-for-byte identical optimized binary on this Apple ARM target, so it is a
measured no-op rather than a retained source change. The current implementation
therefore remains the baseline: the admission allocator is no longer the first
optimization target, but none of these index ABI or empty-bucket shortcuts
survived the full operation matrix. The host was again heavily shared; these
results are useful for rejecting broad regressions, while a pinned Linux run is
required to distinguish small wins.

### Fixed-table probe census and first-bucket pass

A temporary release diagnostic reproduced the one-million learned inserts in
the mixed 8/16/24/32/48-byte cache workload. Of those inserts, 889,661
(88.97%) landed in their first selected bucket, 75,122 (7.51%) in the second,
21,432 (2.14%) in the third, 7,794 (0.78%) in the fourth, and 5,991 (0.60%) in
the first overflow segment. The second overflow segment and generic fallback
received no records. That is about 1.17 fixed-bucket scans per insertion.

The same workload inserted from eight threads produced one failed empty-slot
claim per million records. This rules out overflow routing and distributed-key
CAS contention as material admission bottlenecks. The common first-bucket
scan, key publication, and surrounding generated code remain the target.

Six zero-RAM first-bucket changes were then rejected:

- Publishing from the already-zero saved control byte removed three
  instructions from each specialized claim function and improved scalar
  insertion about 4.8%, but replacement and the 95%-read mix fell about 5.4%
  and 7.3%. Adding rare-failure spin backoff restored the mix to neutral while
  insertion and replacement remained about 1% lower.
- Returning the empty offset and passing its bucket separately kept the
  16-byte scan result. Its first scalar gate improved the mix 6.8%, but
  insertion fell 1.4%. A longer 3M-operation/21-sample admission gate was
  effectively neutral on the median (-0.25%) and reduced mean p05 about 4.7%.
- Returning only a read-side empty offset reduced read hit about 7.6%, miss
  2.5%, and replacement 4.6%, with no mixed-cache gain.
- Testing exact class width before the adaptive mode check initially measured
  +0.9% insertion, +2.5% replacement, and +8.2% mix, but read hit fell 2.1%.
  Isolating it to insertion removed the apparent wins and left every measured
  row slightly lower.
- Letting reads ignore the append-frontier WRITING decode is linearizable—the
  marker cannot equal a published tag and a racing miss can precede
  publication—but generated read-miss throughput fell from 90.541 to 61.101
  Mops/s while the mix stayed flat.

All diagnostic counters, helpers, and candidates were removed. The optimized
cache executable again matches the preserved baseline byte for byte. The
probe census changes the roadmap: do not spend RAM on overflow headroom or
redesign the bucket CAS protocol for distributed inserts; pursue an
algorithm-level reduction in first-bucket work and validate it without moving
the read/update code layout.

The final all-target validation exposed and fixed a separate cache-lifecycle
race. Direct arenas keyed their reclamation registry by collector address. A
dropping owner freed its collector before removing that registry entry, so a
concurrently created cache could briefly reuse the address and trip the live
key assertion. A lifecycle mutex now serializes registration with the narrow
drop interval spanning collector destruction and registry removal. It is used
only at cache construction/destruction, adds no per-entry state, and does not
touch foreground cache operations. An eight-thread/8,000-owner stress test and
five repeated DirectPackedCache suites pass after the fix.

## Pipelined admission and overage-aware eviction

The next retained optimization targets Redis-style write pipelines rather than
changing scalar cache semantics. `DirectCacheGuard::admission_batch()` updates
entry and weight accounting for every successful conditional insert, but
coalesces limit enforcement. The last published entry is protected during that
eviction. Duplicate inserts retain the original value and add no capacity
debt. Long scopes self-flush: the entry window is roughly twice proactive
headroom (32 at 10K entries and 128 at 100K with this configuration), and the
weight window is 0.1% of finite configured capacity. Drop enforces residual
debt.

When a small entry-only cache receives several admissions together, the first
eviction pass now includes the measured overage in addition to normal proactive
headroom. Large caches whose normal headroom already reaches the configured
eviction batch skip the extra 64-shard count; an earlier unconditional version
was rejected after it erased the 100K-entry gain.

The alternating full-cache probe reports medians where higher is better. Its
paired candidate/scalar ratio reduces host drift:

| Capacity / writers | Scalar Mops/s | Batch-32 Mops/s | Paired change |
|---|---:|---:|---:|
| 10K / 1 | 4.322 | **4.873** | **+11.5%** |
| 100K / 1 | 2.793 | **2.946** | **+4.5%** |
| 10K / 8 | 1.750 | **2.032** | **+18.1%** |
| 100K / 8 | 1.940 | **2.261** | **+13.8%** |

Papaya's control is deliberately stronger than a real cache admission path: it
is handed the exact resident victim and performs one remove plus one insert,
without CLOCK sampling, TTL, cache statistics, or capacity policy. It remains
roughly 2x faster at 10K/1T and more than 5x faster in the eight-writer rows.
That is an upper bound showing that victim discovery and synchronized eviction
remain the dominant full-cache gap, not a claim that Papaya supplies equivalent
cache behavior.

Non-full insertion did not show a stable multicore win, so the API is not
presented as a generic insertion accelerator. The one-million-entry memory
probe remains exactly 105.155 requested B/entry and 1,016,091 allocations.
Duplicate, batch-drop, automatic long-scope flush, newest-entry protection, and
eight-writer capacity tests pass; the full all-target/all-feature suite also
passes.

### Long-key fallback and reclamation proof

Adding a key-length control to `cache_eviction_probe` exposed a separate cliff:
unsupported widths such as 64 bytes live in the generic Papaya fallback, while
the previous victim sampler visited that table only eight keys at a time and
only on every fourth refill. At 10K capacity and 20K admissions, the old
64-byte path measured 0.154/0.158 Mops/s scalar/batch at one writer and
0.137/0.133 at eight writers.

The retained sampler uses the old sparse schedule while the fallback is below
half the physical population. Once it dominates, one rotated traversal fills a
bounded reservoir up to 16 eviction windows (1,024 keys at the default batch,
with the existing 4,096 cap). A traversal cannot return the same live Papaya
entry twice, so its former growing-vector duplicate check was removed. Sampled
keys now share a contiguous byte buffer; retained keys use 64-key chunks and
one end offset per key instead of one allocation per key. The final 17-sample
results were:

| Writers | Scalar Mops/s | Batch-32 Mops/s | Papaya exact-victim upper bound |
|---:|---:|---:|---:|
| 1 | 2.558 | **2.870** | 8.508 |
| 8 | 1.742 | **1.791** | 11.543 |

Higher throughput is better. The Papaya row is not an equivalent cache: it is
given the exact victim and performs no CLOCK sampling, TTL, statistics, or
capacity-policy work. Original-key survivors remained about 3.1K because the
large fallback refill is moderately admission-resistant; a prefix-only refill
was faster in places but more biased and was rejected.

The previous retained batch result was 1.857/1.503 Mops/s at one/eight writers,
so the final path is 54.6%/19.2% faster on this probe and 18.2x/13.5x above the
original 0.158/0.133 cliff. The packed reservoir independently improved
long-key one-writer admission about 3-4%. Across five repeated 100K/eight-writer
pairs its median was 1.031 versus 0.992 Mops/s (+3.9%); the single-run direction
was noisy, so the repeated paired result is the acceptance gate. Turnover was
unchanged.

One long admission batch also used to keep its epoch pinned for the full
pipeline, preventing retired arena slots from recycling. Reclamation progress
now belongs to the guard and refreshes every 512 successful admissions, while
capacity enforcement retains its smaller adaptive entry/weight windows. This
avoids refreshing at every short batch boundary. Requested live-byte results
at 10K entries were:

| Key bytes | Seeded B/entry | Pressure before | Pressure retained | After maintenance |
|---:|---:|---:|---:|---:|
| 16 | 170.407 | 272.939 | **180.091** | **139.043** |
| 64 | 238.346 | 546.472 | **441.571** | **188.777** |

Lower bytes per entry is better. `memory_probe`'s allocation column is the
cumulative number of allocation calls in the measured region, not the number
of allocations that remain live. Adaptive maintenance compacts churned key
metadata, which is why its live-byte result can be below the original seeded
mutable generation.

Seven-run follow-ups compared the old boxed-key reservoir with packed chunks.
Requested-memory medians remained within 0.2 B/entry in every pressure state:
64-byte pressure 441.494/441.629 and maintained 188.706/188.639 B/entry;
16-byte pressure 179.504/179.790 and maintained 139.025/139.081. These tiny
bidirectional differences are randomized-layout noise, not a density claim.
Lower cumulative allocation counts are the stable result: about 91.3K to 71.5K
(-22%) for 64-byte pressure and 32.2K to 13.2K (-59%) for 16-byte pressure.
A 32-key chunk reduced one snapshot by less than 0.5 B/entry, regressed another,
and added about 630 allocation calls, so the 64-key chunk remains.

Two wide-inline follow-ups were deliberately rejected. A segmented 49–64-byte
tier saved RAM and accelerated full-cache admission, but the 95%-read overlay
mix fell about 30% versus the generic fallback. A single full table recovered
and slightly improved the read mix, but increased fresh memory about 41% in
the deliberately 3x-over-reserved fixture. That result kept the exact-48
classes and motivated the arbitrary-length successor below.

### Native arbitrary-length fallback

The retained successor moves post-learning residual keys into an append-only
concurrent table with a safe public API. Each hash receives up to four eight-slot bucket
choices. A control-word claim serializes one writer at a bucket frontier; the
winner initializes a write-once stable cell and exact trailing key in one
allocation, then publishes its
tag with release ordering. Readers use acquire control loads. Since deletion
changes the stable cell and generation rebuild owns reclamation, published
entries never move and the implementation needs no second epoch domain. Raw
allocation/deallocation is isolated in one audited module; all table and caller
interfaces remain safe.

The table adds native rotated bucket sampling, cache-line-separated length
shards, and no fixed state when adaptive learning assigns it zero capacity.
The separate Papaya membership hint is also conditional. While the learning
gate is closed and drained, the builder seeds that hint from the bounded
startup sample before publishing the learned tables. Later saturation spill
marks it directly. Victim-window shares use seed-derived stochastic rounding,
so a small Papaya sample receives its population-weighted share over time
instead of losing every rounding tie to the dominant native table.

The 15-sample preserved-binary full-cache gate was:

| Shape | Previous batch Mops/s | Native batch Mops/s | Previous/Papaya | Native/Papaya | Normalized change |
|---|---:|---:|---:|---:|---:|
| 10K/20K, 1 writer | 3.093 | **4.415** | 0.348 | **0.500** | **+43.9%** |
| 10K/20K, 8 writers | 2.179 | **2.782** | 0.162 | **0.207** | **+27.8%** |
| 100K/50K, 1 writer | 2.100 | **3.196** | 0.258 | **0.375** | **+45.3%** |
| 100K/50K, 8 writers | 1.953 | **2.676** | 0.109 | **0.165** | **+52.1%** |

Higher Mops/s and ratios are better. The adjacent Papaya control receives the
exact victim, so it removes host-speed drift but remains an upper bound rather
than equivalent cache work. Original survivors were equal or slightly lower
with native sampling: about 3.1K at 10K and 66.4K at 100K.

Requested-memory comparisons show the intended pressure benefit without a
universal fresh-table claim:

| Shape | Previous B/entry | Native B/entry | Direction |
|---|---:|---:|---:|
| 10K key64 seeded | 238.346 | **230.150** | native -3.4% |
| 10K key64 pressure | 441.276 | **417.380** | native -5.4% |
| 10K key64 maintained | 188.850 | **188.463** | effectively equal |
| 100K key64 seeded | **217.258** | 220.606 | native +1.5% |
| 100K key64 pressure | 410.407 | **397.927** | native -3.0% |
| 100K key64 maintained | **157.056** | 157.077 | equal |

Papaya's internal capacity steps also make native seeded memory 5.3% larger at
30K, while native's own curve is smoother. The 16-byte control allocates no
native table and is unchanged. The operation matrix does not justify calling
this a general-purpose speed leader: read/delete/read-heavy mix mostly trade
after Papaya normalization, scalar long-key update regressed about 12% in the
clean one-thread comparison, and eight-thread learned insertion still trails
Papaya. The accepted claim is narrower and stronger: native sampling closes a
large real-cache admission gap and lowers long-key pressure memory.

The final one-allocation follow-up removes the separate boxed-key allocation
without growing the eight-byte slot:

| Shape | Previous native | One-allocation native | Change |
|---|---:|---:|---:|
| 10K key64 seeded B/entry | 202.976 | **195.351** | **-3.8%** |
| 10K key64 pressure B/entry | 390.354 | **366.459** | **-6.1%** |
| 100K key64 seeded B/entry | 193.383 | **185.710** | **-4.0%** |
| 100K key64 pressure B/entry | 370.667 | **347.124** | **-6.4%** |
| 100K pressure allocation calls | 710,557 | **416,519** | **-41.4%** |

Maintained memory and key16 are effectively unchanged. The latest layout also
beats the original Papaya-backed fallback on seeded and pressure RAM at every
tested 1K/10K/30K/100K point. Alternating old/new probes improved 1T long-key
insert/read/update/95%-read mix about 15%/27%/17%/18%, and 8T insert/read/mix
about 3%/10%/29%; the noisy 8T update median improved about 2%. A separate
one-turnover full-cache gate improved the preceding-native/Papaya ratio at all
four shapes: +9.0%/+8.2% at 10K 1T/8T and +1.3%/+4.9% at 100K.

Rejected layouts are retained as evidence. `ArcSwap` slots saved memory but
collapsed multicore admission; one global length counter did the same. Inline
entries and lazy bucket pages cut allocation calls but added 27–31 B/entry to
fresh caches. Lowering load to 83% improved reads but reduced insertion about
5% and consumed more slots. Allocating a prepared entry before bucket claim
helped isolated insertion but lost about 6–7% in repeated 100K/one-writer
full-cache runs. A route-only hash split improved empty-base 1T insertion but
was neutral at 8T and made the non-empty full-cache gate less stable, so it was
also reverted. The final layout keeps eight-byte atomic pointer slots, 87%
planned load, sharded length, one allocation after a successful claim, and the
original complete hash schedule.

The machine-readable follow-up bundle is retained under
`target/cache-proof/portable-read-path-20260722/`. The host was simultaneously
running several unrelated CPU-heavy processes, and the variability is visible:
Direct's frozen miss was 292.546 versus Papaya's 310.449 Mops/s in that bundle
(-5.8%), while the 95%-read mix was 36.200 versus 25.498 (+42.0%). Async
full-cache pressure was 47.506 versus 50.876 (-6.6%) while retaining a 95.707%
versus 95.182% read hit rate. The concurrent latency row remained positive:
Direct delivered 18.202 versus 15.521 read Mops/s, p99 334 versus 417 ns, and
p99.9 417 versus 542 ns while performing one rebuild. This disagreement is
why none of the local throughput rows supersedes the pinned-Linux gate.

## First concurrent tail-latency result

The new `cache_latency_probe` measures every read with eight reader threads
while two writers execute 200K replacements, remove/reinsert operations, and
unique insertions. DirectPackedCache performed one adaptive rebuild in every
sample; Papaya has no equivalent maintenance step. Both finished with exactly
219,945 entries and a 95.000% read hit rate.

Across three independent five-sample processes, lower latency is better:

| Process | Implementation | Read Mops/s | p50 ns | p95 ns | p99 ns | p99.9 ns | Median maximum |
|---:|---|---:|---:|---:|---:|---:|---:|
| 1 | DirectPackedCache | **36.109** | 84 | **250** | **334** | **458** | **1.057 ms** |
| 1 | Inline Papaya | 26.722 | 84 | 333 | 417 | 542 | 2.478 ms |
| 2 | DirectPackedCache | **15.628** | **84** | **292** | **375** | **500** | **8.881 ms** |
| 2 | Inline Papaya | 11.943 | 125 | 375 | 459 | 625 | 14.860 ms |
| 3 | DirectPackedCache | **33.403** | 84 | **250** | **334** | **458** | **7.502 ms** |
| 3 | Inline Papaya | 20.712 | 84 | 333 | 417 | 542 | 9.119 ms |

PackedGen won p95, p99, p99.9, and measured read throughput in all three local
processes even while rebuilding. Maximum latency is strongly affected by host
scheduling and is not yet a service-level claim. The required follow-up is the
same run on isolated Linux cores with CPU frequency control.

## First memory-stability result

The turnover harness removes and replaces every key once per cycle with eight
writers, then records maintenance time, live allocator bytes, allocation
count, and process RSS. Both implementations finish every cycle with exactly
100K live entries.

With the macOS system allocator, PackedGen live allocation remained bounded at
roughly 112–114 B/entry, but RSS retained rebuild pages and grew to 69.4 MB by
cycle 10; inline Papaya reached 46.9 MB. This is allocator retention rather
than a live-object leak, but it is still an unacceptable production default if
the process cannot release pages.

The tuned jemalloc configuration above reached a plateau. Across cycles 10–30:

| Metric | DirectPackedCache | Inline Papaya | Better |
|---|---:|---:|---|
| Median live B/entry | **113.085** | 138.869 | PackedGen, 18.6% lower |
| Live B/entry range | **111.979–114.444** | 138.772–162.666 | PackedGen, narrower/lower |
| Median RSS, cycles 15–30 | **25.30 MB** | 27.03 MB | PackedGen, 6.4% lower |
| RSS range, cycles 15–30 | **24.64–25.49 MB** | 25.61–28.43 MB | PackedGen, narrower/lower |
| Median turnover loop | **1.324 Mops/s** | 1.115 Mops/s | PackedGen +18.7% |
| Median maintenance | 17.185 ms | **0.002 ms** | Papaya |
| Turnover plus maintenance | 1.079 Mops/s | **1.115 Mops/s** | Papaya +3.3% |

This passes the short-run bounded-memory gate with tuned jemalloc, but not the
24-hour endurance gate. PackedGen pays an explicit generation-rebuild cost in
exchange for the lower steady memory footprint; that cost must remain visible
in production metrics and tail-latency tests.

Stable-Rust verification note: `cargo test --all-targets --all-features`,
all-target/all-feature Clippy with warnings denied, and Rustdoc with warnings
denied pass for the `packedgen` package. Forcing `--workspace --all-features`
also enables a nightly-only `foldhash` feature in the retained `elastic-core`
research member and therefore does not compile on stable Rust.

## 2026-08-08 embedded Redis-like gate

This gate deliberately did not start Redis or include network/protocol costs.
It tested PackedGen as an embedded, bounded, TTL-aware in-memory cache and as a
mutable in-memory storage index. The host was an Apple M4 Max with 16 logical
CPUs, macOS 26.5.1, and stable Rust 1.94.0. The portable default hash schedule
was used for the comparison bundle. Raw cache results are retained locally in
`target/cache-proof/redis-like-20260808/`; the tuned-jemalloc turnover results
are in `target/cache-proof/redis-like-20260808-soak/`.

Correctness and safety coverage completed as follows:

- 254 library and integration tests passed, with the documented long soak
  ignored in the ordinary suite and then run separately for five million
  operations in release mode. Formatting, all-target/all-feature Clippy,
  doctests, strict Rustdoc, and diff whitespace checks passed.
- Five focused Miri reclamation, lifetime, mixed-reclaimer, concurrent-read,
  and guarded-admission tests passed with permissive provenance.
- AddressSanitizer passed all 27 `DirectPackedCache` integration tests and the
  focused mutable-segment differential/compaction test.
- ThreadSanitizer passed the focused multiwriter mutable-segment test, but the
  direct cache replacement/read test reported a race in `seize` 0.5.1's raw
  collector between `try_retire` and `traverse`. Whether this is a dependency
  false positive or a real publication defect remains unresolved, so the
  bounded cache does not pass the memory-safety release gate.

The repeated throughput matrix used 200K resident mixed binary keys, one
million operations, seven alternating samples, and 1/8/16 threads. At 16
threads, higher Mops/s is better:

| Operation | DirectPackedCache | Inline Papaya | Direction |
|---|---:|---:|---|
| Read hit | 141.136 | **182.289** | Direct -22.6% |
| Read miss | 675.980 | **1,072.338** | Direct -37.0% |
| Replace hit | **18.283** | 12.277 | Direct +48.9% |
| Insert new | 50.825 | **74.126** | Direct -31.4% |
| Insert new, batch 32 | 46.333 | **66.094** | Direct -29.9% |
| Delete hit | 24.000 | **25.302** | Direct -5.1% |
| Delete miss | 584.767 | **627.878** | Direct -6.9% |
| TTL touch | 105.963 | **182.913** | Direct -42.1% |
| 95%-read mix | 97.443 | **153.419** | Direct -36.5% |
| 95%-read tracked mix | 98.813 | **127.666** | Direct -22.6% |
| Full-cache pressure, async | 79.907 | **98.646** | Direct -19.0% |
| Hot full-cache pressure, async | 88.676 | **91.169** | Direct -2.7%; hit rate 98.128% versus 97.574% |

The same bundle preserved the static density lead: at one million records,
Direct used 105.018 requested B/entry versus inline Papaya's 133.693, or 21.4%
less. During an 8-reader/2-writer workload with equal 95% hit rate and equal
final residency, Direct delivered 39.777 versus 32.462 read Mops/s (+22.5%).
Its p50 tied at 83 ns; p95/p99/p99.9 were 250/292/375 ns versus
291/334/458 ns, respectively 14.1%, 12.6%, and 18.1% lower.

That snapshot density does **not** survive complete key turnover in the
current implementation. With 100K entries, eight writers, 30 full turnover
cycles, and tuned jemalloc, Direct moved from 110.408 B/entry at build to a
stable 194--197 B/entry after the first rebuild. Cycle 30 was 195.546 B/entry
and 34.29 MB RSS versus Papaya's 138.874 B/entry and 26.31 MB RSS: Direct was
40.8% larger in live allocation and 30.3% larger in RSS. An independent
five-cycle rerun reproduced the result. This supersedes the earlier
112--114-B/entry turnover result: bulk-built block entries are dense, but the
current churn/rebuild path leaves the replacement representation materially
larger.

The integrated no-server workload used 200K capacity, 64-byte values, mixed
binary keys, one-hour TTLs, asynchronous eviction, and a 400K-key catalog.
The 95%-read/4%-write/1%-delete workload held the hard capacity and scaled from
6.908 Mops/s at one thread to 22.077 at eight and 29.033 at sixteen. At sixteen
threads isolated hit/miss/update/delete-hit/delete-miss rates were
114.669/228.546/6.679/18.040/169.905 Mops/s. Full-cache admission had a severe
nonlinear cliff: 100K and 200K new keys completed at 0.962 and 1.099 Mops/s,
400K fell to 0.014 Mops/s, and the one-million-key case was stopped after more
than six minutes while still consuming roughly nine CPU cores. The completed
400K case converged exactly to 200K resident entries with 400K evictions.

The separate online mutable-segment storage path remains stronger for
write-heavy database-style use. With one million base records and 16 threads,
it used 103.386 B/entry versus Papaya's 140.494 (-26.4%). It was 9.2% slower on
pristine hits, 17.0% slower on misses, and 32.2% slower on inserts, but 6.22x
as fast on updates and 53.9% faster on deletes. The 95%-read/5%-update mix was
8.1% slower at eight threads and 53.3% faster at sixteen; the 80/20 mix was
66.2% faster at eight and 203.5% faster at sixteen. Post-workload memory stayed
18.7--24.5% below Papaya for the tested update, insertion, deletion, and mixed
rows.

Online compaction also remained live under 16 concurrent workers: insertion
compaction completed in 407.795 ms while foreground traffic ran at
26.294 Mops/s. Localized update/delete compactions completed in 137.086 and
140.096 ms. Concurrent insertion publication retained a predecessor/retry
generation and ended at 160.436 MB, so compaction peak and prompt predecessor
reclamation remain production gates.

Verdict: the mutable-segment engine is a promising embedded in-memory database
index for multicore update-heavy workloads, but still needs pinned Linux and
longer publication/reclamation validation. `DirectPackedCache` is not yet
ready as a general Redis-like cache: its static density and mixed tail latency
are strong, but the ThreadSanitizer report, post-turnover memory regression,
and sustained full-capacity admission cliff are release blockers.

## 2026-08-09 direct-cache blocker closure

This section supersedes the three `DirectPackedCache` blockers in the
2026-08-08 verdict above. The cache no longer uses `seize` for direct-value
reclamation. It now owns a three-epoch quiescent-state collector with padded
reader and retirement shards, batched retirement publication, and an explicit
forced-reclamation maintenance boundary. Dense values use 256-slot blocks,
writer-local home partitions, cross-partition reclaimed-slot reuse, and a
64-way address-range directory. Ordinary reads acquire the arena epoch lazily,
so read-only guards do not delay adaptive generation replacement.

Miri found and guided the removal of two invalid early arena prototypes. The
first published a slot pointer before moving its owning block to its final
tree location. The second reconstructed a mutable block pointer from an
exposed address after later tree borrows. The retained implementation installs
the block first, caches only a provenance-preserving shared `NonNull`, and
confines post-publication mutation to partition-locked `UnsafeCell` metadata.

The final safety/correctness gate is green:

- all 254 ordinary library and integration tests passed across all targets and
  features; strict Clippy, formatting, and Rustdoc also passed;
- `cargo +1.88.0 check --all-targets --all-features` passed with the declared
  minimum Rust version, including every comparison and benchmark target;
- the two-million-operation release differential/reclamation/rebuild soak
  passed;
- all five focused Miri lifetime, mixed-reclaimer, concurrency, stale-slot,
  and guarded-admission tests passed;
- AddressSanitizer passed all 27 direct-cache integration tests;
- ThreadSanitizer passed the exact concurrent replacement and pinned-read
  test with the native collector. The prior `seize` race is no longer on this
  path.

With tuned jemalloc, 200K mixed binary keys, 64-byte inline values, 16 writers,
and ten complete key turnovers, lower memory is better:

| Metric | DirectPackedCache | Inline Papaya | Direction |
|---|---:|---:|---|
| Initial live B/entry | **109.700** | 138.491 | Direct -20.8% |
| Median live B/entry, cycles 1-10 | **110.925** | 138.758 | Direct -20.1% |
| Observed live B/entry range | **110.322-114.476** | 138.727-162.485 | Direct lower |
| Typical live allocations | **1.4K-1.6K** | about 400.5K | Direct about 99.6% fewer |
| Median foreground turnover | **2.213 Mops/s** | 1.665 Mops/s | Direct +32.9% before explicit maintenance |

This closes the post-turnover live-memory regression. RSS remains
allocator/host-sensitive and still requires the pinned Linux endurance gate.
Direct maintenance took roughly 25-42 ms per cycle in this run; Papaya does
not perform the equivalent packed-generation rebuild, so that cost remains
visible rather than being folded into the foreground number.

The full-capacity admission cliff is also removed. At 200K capacity, 16
writers, 64-byte values, asynchronous eviction, and a 512-operation guard
refresh interval, one million new keys completed at 0.944 Mops/s and five
million at 0.918 Mops/s. Both converged near 200K live entries. The five-million
run ended near 110.0 live B/entry. This supersedes the old 400K result of 0.014
Mops/s and the one-million run that did not finish after six minutes. The path
is now bounded and stable, but sub-1-Mop/s strict churn is still an optimization
target rather than a throughput win.

The local Apple M4 Max operation snapshot below uses 200K resident keys and 16
threads. Most rows use five one-million-operation samples; the ordinary
95%-read mix uses seven five-million-operation samples to reduce short-window
scheduler noise. Higher Mops/s is better. These are directional development
numbers, not the pinned-Linux release gate.

| Operation | DirectPackedCache | Inline Papaya | Arc Papaya |
|---|---:|---:|---:|
| Read hit | 173.024 | **349.345** | 165.736 |
| Read miss | **879.540** | 589.174 | 773.520 |
| Update/replace hit | **13.623** | 12.404 | 7.831 |
| Insert new | 32.069 | 75.938 | **98.287** |
| Delete hit | 18.067 | **23.899** | 18.464 |
| Delete miss | **816.187** | 91.827 | 67.997 |
| TTL touch | **90.655** | 85.908 | 17.283 |
| 95%-read ordinary mix, 5M ops | 60.655 | **101.941** | 74.566 |
| Full-cache pressure, synchronous | 57.569 | **93.183** | 63.429 |
| Full-cache pressure, async 1% | 64.480 | **93.183** | 63.429 |
| Hot full-cache pressure, async 1% | 75.826 | **83.790** | 58.371 |

The honest result is a differentiated cache, not a universal Papaya
replacement. Direct leads the tested miss, update, touch, and missing-delete
rows; async Direct beats the Arc control under both pressure traces. Inline
Papaya remains substantially faster for read hits, new keys, successful
delete, and the longer ordinary mixed trace. PackedGen's production argument
is its roughly 20% live-memory saving, very low allocation count, bounded
arbitrary-value storage, and competitive mutation/pressure behavior—not a
claim that every operation is faster.

The former safety, turnover-density, and nonlinear-admission blockers are
closed locally. Remaining publication gates are pinned isolated Linux
throughput/RSS/tail latency, executing the scripted 24-hour churn gate,
observing the newly added CI enforcement of the locally verified Rust 1.88
baseline, and improvement of new-key and read-heavy mixed throughput without
giving back density.

The endurance command itself was smoke-tested with 20K entries and four
writers. It completed 150 full turnovers in 2.010 seconds, moving from 121.878
to 130.267 live B/entry; the sampled late value was below the earlier sample.
An intentionally impossible zero-growth limit failed on the first turnover,
proving the regression guard is active. This validates the harness only, not
the still-pending 24-hour Linux endurance claim.
