# Rolling adaptive-reuse experiment — 2026-09-01

## Goal

Repeat the eight workloads used by the external EXP-0003 cache screening and
improve PackedGen's pressure behavior without regressing resident reads,
stampede handling, scan resistance, or low-reuse version churn.

The R-0004 rerun exercised the default synchronous CLOCK configuration. It
showed strong resident and stampede behavior but lost all six turnover cells to
QuickCache. Enabling the already retained async/doorkeeper/frequency mechanisms
closed most of that mechanism gap, but the lifetime reuse counter activated
frequency admission too slowly after an early cold phase.

## Retained candidate

The candidate replaces separate lifetime hit and lookup atomics with one packed
rolling state:

- at most 16,384 recent lookup observations are retained;
- history is halved before a new batch would cross the window;
- guards still publish reuse only at their existing refresh/drop boundary;
- one compare/exchange replaces up to two atomic additions per publication;
- the state shrinks from two cache-level `AtomicU64`s to one; and
- no per-entry metadata, unsafe code, or read-hit hashing was added.

The failure-first test records 100,000 early misses followed by sustained hits.
The lifetime estimator remained inactive; the bounded estimator activates. A
reverse-direction test proves that admission turns off after reuse disappears,
and an eight-thread test proves `hits <= lookups <= 16,384` under concurrent
publication.

The measured configuration was:

```rust
CacheConfig::new(profile_max_weight)
    .with_overlay_capacity(100_000)
    .with_eviction_batch(256)
    .with_admission_doorkeeper(50_000)
    .with_tiered_frequency_admission(2, 5_000, 8, 7_500)
    .with_async_eviction(11_000)
```

The async hard ceiling is 110% of the soft logical byte limit. Every measured
process drained to the ordinary per-profile logical bound, corresponding to
approximately 50,000 equal-shape objects.

## Experiment boundary

PackedGen and QuickCache 0.7.0 ran in fresh processes on the same deterministic
trace and seed. Each record used four workers, 250,000 operations per worker,
and a 50,000-object logical capacity. Five pairs alternated candidate order.
Throughput and every-operation HDR latency were separate phases.

Eight profiles x two candidates x two phases x five pairs produced 160 records
and 160 million logical operations. All records completed their scheduled
operations with matching same-pair semantic checksums, zero operation,
correctness, maintenance, or histogram failures, bounded final capacity, exact
latency sample counts, and zero ending readers or reclamation debt.

This remains noisy single-host, closed-loop, synthetic-source evidence. The
host was not isolated from unrelated load. Medians below are useful for local
mechanism selection, not a production winner claim.

## Throughput and cache quality

Throughput is M logical operations/second; higher is better. Hit rate is also
higher-is-better when source work is equivalent.

| Profile | PackedGen | QuickCache | Packed hit | Quick hit | Direction |
| --- | ---: | ---: | ---: | ---: | --- |
| Resident hot | **45.222** | 43.946 | **100.00%** | 99.69% | PackedGen +2.9% |
| Cold fill | **6.613** | 4.152 | 0% | 0% | PackedGen +59.3% |
| Pressure skew | **11.225** | 10.808 | 71.19% | **74.57%** | PackedGen +3.9%; Quick quality |
| Graph traversal | **1.381** | 1.234 | 80.59% | **81.94%** | PackedGen +11.9%; Quick quality |
| Version churn | **6.216** | 5.652 | **9.44%** | 6.85% | PackedGen +10.0% |
| Scan resistance | **6.385** | 3.640 | 51.89% | **51.91%** | PackedGen +75.4%; quality tie |
| Large objects | 1.419 | **1.542** | 78.50% | **80.61%** | QuickCache +8.7% plus quality |
| Stampede | **31.853** | 9.097 | 99.41% | 99.40% | PackedGen +250.1% |

PackedGen led seven throughput cells. Large objects remain the only throughput
loss. The remaining hit-rate gaps are 3.38 points for pressure, 1.35 for graph,
and 2.11 for large objects.

## Latency

Latency is microseconds; lower is better.

| Profile | Packed p99 | Quick p99 | Packed p99.9 | Quick p99.9 |
| --- | ---: | ---: | ---: | ---: |
| Resident hot | **0.250** | 0.334 | **0.375** | 0.875 |
| Cold fill | **1.250** | 1.875 | **2.501** | 4.043 |
| Pressure skew | **1.459** | 1.500 | 2.625 | **2.501** |
| Graph traversal | **6.543** | 7.795 | **14.167** | 15.007 |
| Version churn | 1.666 | **1.416** | 4.085 | **2.501** |
| Scan resistance | **1.292** | 1.958 | **2.125** | 4.085 |
| Large objects | **9.047** | 9.295 | **12.751** | 12.799 |
| Stampede | **0.125** | 1.334 | **1.834** | 9.751 |

PackedGen led seven p99 cells and six p99.9 cells. Version churn remains the
clear tail loss. Pressure p99.9 was about 5% slower despite its slightly better
p99. Maximum samples remain volatile; PackedGen had larger maxima in cold fill,
graph, version churn, large objects, and stampede.

## Ending RSS warning

PackedGen used less ending process-RSS growth for resident hot (15.3 vs 19.3
MiB), cold fill (25.9 vs 39.9 MiB), and scan resistance (25.9 vs 39.6 MiB).
It used more for pressure (46.8 vs 35.8 MiB), graph (54.8 vs 42.5 MiB), version
churn (57.3 vs 38.7 MiB), large objects (270.4 vs 232.6 MiB), and stampede
(10.7 vs 6.8 MiB). Ending RSS includes allocator retention and is neither peak
RSS nor allocator-live memory. Turnover memory remains a blocker.

## Decision

Retain the rolling estimator and the 50% base activation threshold as an
opt-in external-workload candidate. It materially improves the exact pressure
screen while preserving the other seven throughput profiles and the strong
resident/stampede latency paths. The library's strict synchronous CLOCK
configuration remains the safe default until R-0005 validates the tagged
candidate.

Do not claim a general cache win. The next optimization target is large-object
retention and turnover memory, followed by version-churn p99.9. Any follow-up
must preserve scan hit rate, resident throughput, and the current seven-cell
throughput lead.

## Rejected follow-ups

Five-pair focused grids retained the base gate of 2, a 50% rolling activation
threshold, and the high-reuse gate of 8 at 75%:

| Variant | Pressure Mops/s / hit | Graph Mops/s / hit | Large Mops/s / hit | Decision |
| --- | ---: | ---: | ---: | --- |
| Base gate 2 only | 10.771 / 71.16% | 1.538 / 77.42% | 1.380 / 77.44% | Quality too weak for graph/large |
| High gate 4 | 11.433 / 71.13% | 1.475 / 79.69% | 1.443 / 78.46% | Graph quality below gate 8 |
| High gate 6 | 11.953 / 71.12% | 1.465 / 80.51% | 1.433 / 78.51% | No broad throughput win |
| High gate 8 | 11.270 / 71.00% | **1.598 / 80.58%** | **1.446 / 78.48%** | Retain universal compromise |
| High gate 10 | **12.208 / 71.07%** | 1.580 / 80.57% | 1.426 / 78.47% | Pressure-only speedup |

In a separate pressure/scan grid, lowering base activation from 60% to 50%
raised pressure hit rate from 70.31% to 71.10% and throughput from 10.76 to
11.37 Mops/s. Scan hit rate stayed at 51.88--51.89%; its median throughput was
6.60 versus 5.86 Mops/s in that grid. A 55% threshold was slightly faster for
pressure in that run but retained about 0.46 fewer hit-rate points, so 50% is
the better cache-quality choice for the full comparison.

## Traceability

- PackedGen base revision: `8d1c324139159a9d9f42939296984a9af30fac05`
- Rolling-reuse implementation revision:
  `5bc0c2f54d6511d6566d24d2f2ccf9a92906390c`
- Measured working-tree content SHA-256: `c0f738058203ae7b220400dae4f35804dc2cb96c38e620c9af661c884c5e8dce`
- PackedGen runner SHA-256: `21e95170f4a1f39ecbcc1d826fcf950a55cf67f8f508f81fd87824149500210f`
- QuickCache runner SHA-256: `3beb4497959ad3b8b71772b1a8cee5d2b5d419ed68075b3f2d02d5555793c3f5`
- Lab revision: `01bb83ea07eda2639b735eb3027cf180b1cc9a5a`
- Rust compiler: `rustc 1.94.0 (4a4ef493e 2026-03-02)`
- Ignored local evidence: `20260901-rolling-full-throughput-working/` and
  `20260901-rolling-full-latency-working/` under EXP-0003's `runs/` directory

The measured source preceded the feature-gated production-diagnostics commit.
Those diagnostics are absent from default builds, but the exact immutable
candidate still requires a fresh R-0005 run; these exploratory measurements
must not be relabeled as tagged-candidate evidence.
