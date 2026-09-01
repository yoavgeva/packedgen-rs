# P2 churn, large-object, memory, and scaling work — 2026-09-01

Status: implementation-qualified, performance results exploratory until rerun
from an immutable candidate revision.

## Scope and invariant

This work targets the P2 weaknesses from R-0004: version-churn tail latency,
large-object turnover, physical memory, and multicore contention. The existing
synchronous configuration remains the safe default. Async eviction,
doorkeeper/frequency admission, the larger background sample, and all tuned
values below remain opt-in until R-0005 validates an immutable revision.

Benchmark values use the OLTP cache experiment's Redis-like object workloads.
Throughput and hit rate are higher-is-better. Latency and RSS are
lower-is-better. Every comparison below uses identical seeds within its paired
set, but the candidate was a content-hashed dirty worktree rather than a tag.

## Accepted implementation changes

1. Doorkeeper rotation now advances a generation and lazily clears 16-word
   groups. A request clears at most 128 bytes instead of synchronously clearing
   the complete filter.
2. Async maintenance defers the potentially large final drop of retired index
   generations until no read cache owns them.
3. Conditional admission carries one randomized key-hash context through the
   doorkeeper and exact index publication instead of hashing a new key again.
4. Values charged at least 4 KiB receive denser access marking only while the
   frequency policy is under pressure. Small/resident reads keep the 1/16
   sampling schedule.
5. Resident frequency refresh no longer increments the globally shared aging
   counter. Admissions still advance bounded lazy aging.
6. Async background eviction may examine up to 1,024 candidates while removal
   and foreground work remain bounded by the configured 256-entry batch.
7. `AtomicReadCache::refresh` is a true no-op when neither the generation nor a
   transitional base changed. This removes repeated Arc/base cloning from the
   normal 512-operation refresh cadence.
8. The rolling reuse estimator now receives cache-line-separated pending
   updates and publishes them every 4,096 observations. Only the rare publisher
   updates the bounded global summary.
9. New explicit lifetime choices were added without changing existing reads:
   `get_cloned`/`peek_cloned`, `pin_cloned`, `pin_shareable(interval)`, and
   `DirectCacheGuard::quiesce`. Shareable/cloned values can bound or eliminate
   epoch-retirement debt without making borrowed references unsound.
10. The `shared-gx` feature now computes the route from its full carried digest;
    the portable build retains its cheaper route-only lane.

## QuickCache comparison

Tuned PackedGen configuration:

```text
PACKEDGEN_EVICTION=async
PACKEDGEN_HARD_LIMIT_BPS=11000
PACKEDGEN_ADMISSION_POLICY=doorkeeper-frequency-adaptive
PACKEDGEN_FREQUENCY_GATE=2
PACKEDGEN_FREQUENCY_MIN_HIT_BPS=5000
PACKEDGEN_FREQUENCY_HIGH_GATE=8
PACKEDGEN_FREQUENCY_HIGH_HIT_BPS=7500
PACKEDGEN_EVICTION_BATCH=256
PACKEDGEN_OVERLAY_MULTIPLIER=1
```

Each table cell is the median of seven paired local runs unless noted.

### Version churn, 4 workers, 250,000 operations per worker

| Metric | PackedGen | QuickCache | Result |
|---|---:|---:|---:|
| Throughput | 8.134 Mops/s | 6.589 Mops/s | PackedGen +23.5% |
| Hit rate | 9.46% | 6.84% | PackedGen +2.62 points |
| p99 | 1.417 us | 1.708 us | PackedGen 17.0% lower |
| p99.9 | 3.459 us | 3.251 us | PackedGen 6.4% higher |
| Ending RSS | 54.3 MiB | 40.3 MiB | PackedGen 34.5% higher |

The no-op refresh alone reduced same-seed median churn p99 from 1.708 us to
1.459 us and p99.9 from 3.751 us to 3.251 us in its isolated five-run gate.
Version p99 is now strong; p99.9 and physical memory remain open.

Calling the new explicit `DirectCacheGuard::quiesce` at the benchmark's
existing 512-operation boundary changes that conclusion for a
latency-sensitive integration:

| Metric | PackedGen quiescent | QuickCache | Result |
|---|---:|---:|---:|
| Throughput | 7.783 Mops/s | 6.589 Mops/s | PackedGen +18.1% |
| p99 | 1.375 us | 1.708 us | PackedGen 19.5% lower |
| p99.9 | 2.125 us | 3.251 us | PackedGen 34.6% lower |
| Ending RSS | 48.6 MiB | 41.9 MiB | PackedGen about 15.9% higher |

This is the recommended churn-tail mode. It remains explicit because it gives
up about 4.3% PackedGen throughput versus ordinary refresh, and large-object
throughput loses about 2% when quiesced at the same cadence.

A later five-seed same-machine check of the final ordinary-refresh source
measured 2.875 us median p99.9 versus QuickCache's 3.293 us. That is encouraging
but does not replace the original seven-pair result above: the tail moved
enough between sessions that only the immutable two-session R-0005 run can
close this blocker honestly.

### Large objects, throughput scaling

| Workers | PackedGen | QuickCache | Throughput result | PackedGen hit | QuickCache hit | PackedGen RSS | QuickCache RSS |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 4 | 1.600 Mops/s | 1.510 Mops/s | +6.0% | 80.82% | 80.61% | 249.3 MiB | 236.3 MiB |
| 8 | 3.165 Mops/s | 2.770 Mops/s | +14.3% | 81.66% | 81.56% | 270.3 MiB | 230.0 MiB |
| 16 | 3.644 Mops/s | 3.906 Mops/s | -6.7% | 81.93% | 82.03% | 326.8 MiB | 259.0 MiB |

In a separate five-run 4-worker latency gate, PackedGen had 8.083 us median
p99 and 11.167 us median p99.9 versus QuickCache's 8.919 us and 12.127 us.

### Resident hot regression gate, 4 workers, five runs

PackedGen retained about 64.8 Mops/s with a 0.208 us median p99. QuickCache
measured about 39.2 Mops/s with a 0.292 us median p99. No resident-hot
regression was observed.

## Contention evidence

Production diagnostics on the same 16-worker, four-million-operation
large-object shape measured 1,853,023 rolling-estimator CAS retries before
sharding and exactly 0 afterward. The diagnostic-build throughput moved from
3.551 to 3.638 Mops/s. The clean non-diagnostic paired gate subsequently led
QuickCache at 4 and 8 workers and narrowed the 16-worker gap to 6.7%.

A clean post-sharding 32-million-operation sample confirmed that estimator
publication is no longer a material stack. The benchmark's 4 KiB synthetic
source materialization dominated the sample (41,662 top-of-stack samples),
followed by PackedGen frozen-slot lookup (5,003), session refresh (2,834), and
victim collection (1,416). `record_frequency_reuse_shard` appeared only 82
times. On that single long seed PackedGen measured 2.951 Mops/s and 552.3 MiB
ending RSS versus QuickCache's 3.990 Mops/s and 437.6 MiB. This is useful
profile evidence, not a paired selection result.

## Memory findings and read-lifetime choices

The diagnostic run reported about 4.3 MiB of direct arena storage, so the large
RSS gap is not a bloated PackedGen entry header. It is dominated by large
payload retirement peaks and allocator retention. A borrowed guard is allowed
to retain values until its caller refreshes or drops it; reclaiming them earlier
would invalidate live Rust references and be unsound.

Measured 16-worker choices:

| Read lifetime | Throughput | Ending RSS | Interpretation |
|---|---:|---:|---|
| Borrowed, normal refresh | 3.644 Mops/s | 326.8 MiB | Fastest PackedGen path; highest retirement peak |
| Borrowed, full quiescence every 512 reads | 3.573 Mops/s | 305.5 MiB | About 2% slower and 6.5% less RSS |
| Owned epoch guard | 2.406 Mops/s | 284.8 MiB | Within about 10% of QuickCache RSS, but much slower |
| `Arc<T>` cloned/read-session experiments | roughly 3.1–3.4 Mops/s | roughly 285–325 MiB | Useful bounded option, not the primary winner |

Endpoint RSS is allocator-dependent and is not a substitute for peak RSS or
allocator-live bytes. R-0005 must capture both.

## Rejected or non-default experiments

- Allocating an open-addressed dedup table for each victim sample regressed
  16-worker throughput by about 1–2% and did not reduce RSS; reverted.
- Checking only the 32 most recent victim handles was statistically neutral on
  the same five seeds and weakened global deduplication; reverted.
- Returning the route hash beside every lookup looked cheaper but enlarged the
  lookup path and regressed 16-worker throughput by roughly 2–3%; reverted.
- Overlay multipliers 2, 4, and 8 reduced rebuild frequency, but multiplier 1
  remained best for large-object high-core throughput. Multiplier 4 improved
  churn p99 while reducing hit rate, so it was not selected globally.
- Candidate windows above 1,024 saved more memory but reduced throughput. The
  background-only 1,024 cap is the measured knee.
- Heavy-value access marking every other hit was flat on throughput (3.630
  versus 3.632 Mops/s), reduced hit rate by about 0.11 point, and did not save
  RSS; reverted.
- The SIMD `shared-gx` build produced only a small/noisy throughput movement
  (3.689 Mops/s median) while median RSS rose to 327.5 MiB; it remains opt-in.
- A four-entry per-reader full-hash cache regressed throughput by about 2.4%; a
  key compare and larger guard cost more than the saved hash lane; reverted.
- Eviction batches 128, 512, and 1,024 did not improve the combined result.
  Batch 256 retained the best throughput/hit-rate knee.
- Tightening async hard-limit headroom from 10% to 5% or 2.5% did not reliably
  reduce RSS; 2.5% was slower. No foreground enforcement occurred in the
  sweep, so permitted capacity debt does not explain the payload-retirement
  peak.
- The split miss-optimized frozen layout regressed median throughput to 3.597
  Mops/s and median RSS to 332.6 MiB; it remains off.
- Deriving the second perfect-hash lane from the already-computed route saved
  one key scan and gained about 1.3% throughput, but raised median RSS roughly
  14%. Independent randomized lanes were restored.
- Bounding PtrHash's Rayon pool to 1, 2, or 4 threads was statistically flat;
  rebuild oversubscription is not the primary remaining 16-worker gap.
- Passing a precomputed route into every borrowed lookup and reusing it for
  frequency marking gained only about 0.8% in a direct large-object A/B and
  about 1.8% in version churn, but reduced resident-hot throughput from 64.542
  to 59.580 Mops/s (7.7%). The complete experiment was reverted.
- Full quiescence and cloned/shareable sessions remain explicit API choices;
  none silently replaces the fastest borrowed path.

## Qualification completed in this worktree

- `cargo test --lib`: 138 passed.
- `cargo test --all-features`: 143 unit tests and all integration tests passed;
  the explicitly marked long-running soak remained ignored.
- `cargo clippy --all-targets --all-features -- -D warnings`: passed.
- Formatting check: passed.

## Remaining P2 blockers

1. Ordinary-refresh version-churn p99.9 is still 6.4% above the paired
   QuickCache median. Explicit 512-operation quiescence clears the tail bar by
   34.6%, but the integration must consciously select that tradeoff.
2. Borrowed-path 16-worker throughput is 6.7% behind QuickCache.
3. Borrowed-path physical memory is 26.2% above QuickCache at 16 workers.
4. The faster borrowed path cannot safely reclaim the current epoch behind a
   caller that still holds references. Production integration must choose an
   explicit refresh/quiescence bound or store cheaply shareable values.
5. Peak RSS, allocator-live bytes, long-run debt, and the exact immutable
   candidate still require R-0005. These exploratory numbers must not be used
   as a release claim.
