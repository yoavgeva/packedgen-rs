# PackedGen bounded cache comparison and maintenance safety

The synchronous default remains competitive on several two-worker read cases,
but QuickCache is substantially faster on bounded insertion and turnover in this
screen. A maintenance deadlock was also reproduced with a failing test and fixed
without adding locks to reads or ordinary writes. These results identify work to
do; they do not qualify PackedGen for production or prove a general cache win.

All work was confined to `packedgen-rs`. FerricStore and the external screening
lab were not changed. This is a new controlled probe, not a rerun of the external
graph, hostile-scan, or version-churn traces and not an R-0005 qualification.

## Environment and reproducibility

Native macOS ARM64, Rust 1.98.0, 16 logical and physical cores, and 128 GiB RAM.
Other applications remained running. Each cache ran alone in its own process,
but the host itself was not isolated. The system allocator, randomized default
hashers, empty default feature set, and synchronous cache configuration were
used. No opt-in admission or asynchronous eviction tuning was enabled.

The repository was dirty on `codex/p2-churn-tail`, based on `26d1d93`. There is no
new immutable candidate revision. The runner records the tracked diff, source
hashes, lockfile, toolchain, hardware, and executable identity. The preserved
pre-deadlock-fix executable has SHA-256
`4f77eb2acdea29c6d986556caadfba3185e7fd12c4dfb07c320ab3cea7c9da64`.

QuickCache 0.7.0 is pinned as a development-only dependency. Both adapters use
32-byte keys and `Arc<[u8]>` values. The owned PackedGen adapter clones the Arc,
like QuickCache; a separate borrowed adapter retains scoped zero-copy access.
Payload allocation and initialization are included in timed writes. Timed reads
check key/value association; untimed final checks verify replacement versions.

Reproduce the comparison and its complete summary from the repository root:

```sh
sh scripts/bounded_cache_probe.sh full target/bounded-cache-new-run
awk -v expected_repetitions=6 -v expected_configurations=24 \
    -f scripts/summarize_bounded_cache.awk target/bounded-cache-new-run/*.csv
```

The completed dataset is `target/bounded-cache-2026-10-03-full-r2`: 432 processes,
24 configurations, six paired seeds, and three adapters. Backend order rotates
between repetitions. All processes passed their value and capacity assertions.
Raw results and stderr are kept individually. Generated files under `target`
are local evidence, not checked-in artifacts; the tables below preserve the
summary, not every raw sample.

## Workloads and measurement limits

Throughput is millions of logical requests per second, abbreviated Mops/s here;
higher is better. A delete and reinsert request contains two cache API calls.
Do not compare that number directly with a single-call delete benchmark.

Cold fill begins empty, admits unique keys across all workers, and keeps the
fixed entry budget throughout the run. Turnover uses 95% read requests, a hot
tenth of the keys, a working set twice the entry budget, and 5% scan writes.
Read misses load and admit a payload. Different hit rates therefore change the
amount of source and insertion work; turnover is not a pure instruction-count
comparison.

Resident hit, miss, replacement, and delete/reinsert controls have a logical
capacity of four times the seeded entry count so randomized QuickCache shard
skew does not invalidate resident operations. Pressure workloads retain the
original entry budget. Both caches use a logical charge of key bytes plus
payload bytes; this excludes Arc, allocator, and index overhead. These are not
equal physical-memory budgets and do not demonstrate a RAM advantage.

PackedGen quiesces its reusable guard every 512 requests and performs full
maintenance every 4096 writes per worker, included in timing. Seed and final
maintenance are outside timing, with final maintenance time separately reported.
Capacity assertions also run before final maintenance. At two workers the
maintenance cadence is per worker, not a fixed global write interval.

The throughput runner disables latency sampling. No p99, peak RSS,
allocator-live, full-core scaling, or expensive-source conclusion follows from
these measurements. Six process repetitions are a useful screen, not a
statistical proof of a less-than-5% regression bound.

## Throughput comparison

Each rate is the median of six processes. The final column is the median of
paired owned-PackedGen versus QuickCache percentage changes, not the ratio of
the two displayed medians. Positive means PackedGen is faster; negative means
slower. Borrowed access is shown separately because its ownership semantics
differ from QuickCache's returned Arc.

| Payload | Workers | Request | PackedGen owned Mops/s | PackedGen borrowed Mops/s | QuickCache Mops/s | Owned paired change |
| --- | ---: | --- | ---: | ---: | ---: | ---: |
| 64 B | 1 | Resident hit | 24.812 | 28.528 | 36.213 | -31.0% |
| 64 B | 1 | Read miss | 52.273 | 55.007 | 51.607 | +1.7% |
| 64 B | 1 | Cold fill | 1.836 | 1.831 | 9.612 | -80.8% |
| 64 B | 1 | Turnover | 7.562 | 7.739 | 20.561 | -63.2% |
| 64 B | 1 | Replace | 4.711 | 4.980 | 7.402 | -39.0% |
| 64 B | 1 | Delete and reinsert | 3.424 | 3.463 | 6.084 | -43.7% |
| 64 B | 2 | Resident hit | 58.240 | 66.344 | 59.165 | -0.3% |
| 64 B | 2 | Read miss | 103.985 | 104.286 | 72.985 | +42.9% |
| 64 B | 2 | Cold fill | 1.532 | 1.543 | 8.362 | -81.8% |
| 64 B | 2 | Turnover | 6.730 | 7.001 | 19.849 | -65.3% |
| 64 B | 2 | Replace | 5.477 | 5.420 | 7.392 | -25.9% |
| 64 B | 2 | Delete and reinsert | 2.673 | 2.748 | 4.430 | -39.3% |
| 64 KiB | 1 | Resident hit | 39.834 | 45.530 | 48.316 | -18.1% |
| 64 KiB | 1 | Read miss | 59.023 | 59.983 | 96.234 | -38.5% |
| 64 KiB | 1 | Cold fill | 0.336 | 0.336 | 0.593 | -43.0% |
| 64 KiB | 1 | Turnover | 1.636 | 1.584 | 3.172 | -48.4% |
| 64 KiB | 1 | Replace | 0.503 | 0.498 | 0.523 | -3.0% |
| 64 KiB | 1 | Delete and reinsert | 0.422 | 0.425 | 0.509 | -17.7% |
| 64 KiB | 2 | Resident hit | 96.815 | 105.968 | 72.077 | +34.1% |
| 64 KiB | 2 | Read miss | 123.837 | 125.123 | 90.997 | +38.4% |
| 64 KiB | 2 | Cold fill | 0.388 | 0.385 | 0.722 | -46.4% |
| 64 KiB | 2 | Turnover | 1.892 | 1.974 | 3.806 | -51.5% |
| 64 KiB | 2 | Replace | 0.676 | 0.663 | 0.694 | -1.5% |
| 64 KiB | 2 | Delete and reinsert | 0.480 | 0.504 | 0.673 | -27.1% |

In small-object cold fill, the owned adapter is about 5.2 to 5.5 times slower
than QuickCache by the displayed median rates. This is an important repeated
weakness, not a close result hidden by noise. Two-worker small-object hit parity
is less certain: its paired changes ranged from -13.7% to +4.6%.

| Turnover payload | Workers | PackedGen read hit rate | QuickCache read hit rate |
| --- | ---: | ---: | ---: |
| 64 B | 1 | 87.359% | 89.969% |
| 64 B | 2 | 87.223% | 89.966% |
| 64 KiB | 1 | 86.775% | 89.878% |
| 64 KiB | 2 | 87.584% | 89.832% |

The policy gap is present even in this relatively read-friendly trace. The
proposed next performance investigation is to separate raw admission cost from
eviction and rebuild work, then test guarded and batched admission against the
same controls. Eviction overhead is a hypothesis, not an established explanation
for the entire observed gap.

## Rejected initial setup

The initial full run used twice the seeded capacity for resident controls.
QuickCache retained only 1022 of 1024 seeds in a large-object two-worker miss
control because of randomized shard skew. The pre-timing assertion correctly
rejected the run. Results remain in `target/bounded-cache-2026-10-03-full`; they
were not pooled with the corrected run or counted as a successful comparison.
The corrected four-times-capacity controls passed all six repetitions. Six
probe tests cover fixed cold-fill budgets, disjoint writer ownership, adapter
semantics, resident seeding, stale replacements, and latency phase aliasing.

## Maintenance deadlock and test driven fix

The old full-maintenance path held the capacity gate while rebuilding the
adaptive index. Rebuild waits for guarded writers to release their old
generation reservation. A writer under capacity pressure can need that same
gate before it returns and quiesces. This creates a lock cycle and affects
periodic full maintenance as well as explicit calls.

The regression holds a real guard after an existing-key admission at a
64-entry limit, starts rebuild on another thread, and observes publication of
the successor generation. It checks that the capacity gate is available and,
when available, admits a new key under pressure using the retained guard. It
quiesces and joins the maintenance thread before asserting, so the old code
fails without leaving an intentionally deadlocked test thread.

The test failed on the old implementation with the expected gate-availability
assertion and passed after the fix. A separate full-maintenance mutex preserves
cycle serialization and completion coalescing. The capacity gate now protects
only expiration and eviction, and is released before rebuild and reclamation.
Both constructors initialize the mutex. No new unsafe block, per-entry field,
or lock on ordinary reads and writes was introduced. Independent source review
found no concrete issue in the fix.

Calling full maintenance on the same thread as a retained guarded-mutation
reservation still requires refreshing, quiescing, or dropping that guard first;
this is now documented. The fix removes the cross-thread lock inversion, not
the rebuild's intentional wait for an idle writer reservation.

After the fix, the native default gate passed 218 tests with one explicitly
ignored long soak: 146 library tests, five concurrency models, one allocation
test, two differential tests, 58 direct-cache integration tests, and six probe
tests. This is bounded correctness evidence, not a complete reclamation or
production qualification.

Focused native-nightly Miri checks passed the pointer-buffer reuse test and
four panicking-destructor/reclamation tests. Commands and logs:

```sh
cargo +nightly miri test --locked --lib \
    cache_arena::tests::direct_arena_pointer_collection_reuses_handle_buffer -- --exact
cargo +nightly miri test --locked --lib cache_arena::tests::panicking
```

The logs are `target/bounded-cache-miri-2026-10-03.log` and
`target/bounded-cache-miri-panic-2026-10-03.log`. Miri warned that the existing
`with_exposed_provenance_mut` pointer reconstruction can hide pointer bugs.
These successful runs are not strict-provenance qualification. The nightly
compiler also warned about its newly deprecated `fetch_update` name; no
nightly-only rename was applied to an API supported from Rust 1.88.

Formatting, whitespace checks, shell syntax checks, and strict root-package
linting passed. The lint gate was:

```sh
cargo clippy --locked -p packedgen --all-targets \
    --features cache-production-diagnostics,prepared-keys -- -D warnings
```

This does not claim every optional feature or every workspace member was linted.
The old/new summary tool also passed an identical-input zero-change control and
rejected an intentionally incomplete pair set. Both summary tools check matched
entry limits, weights, request counts, and latency sampling settings before
reporting paired percentages.

The full root-package library and integration suite with
`cache-production-diagnostics,prepared-keys` passed on both native Rust 1.98 and
Linux Rust 1.88: 377 tests passed and one long soak was explicitly ignored on
each platform. The native log is
`target/bounded-cache-feature-tests-2026-10-03.log`; the Linux log is
`target/bounded-cache-linux188-2026-10-03.log`. The native executables had lengthy
startup delays, not observed test-body failures. Listing tests in parallel was
used to prepare runners; the remaining listing-only jobs were terminated before
timing, and that termination was not counted as a test result.

The Linux run used the official ARM64 runtime pinned to
`rust@sha256:38bc5a86d998772d4aec2348656ed21438d20fcdce2795b56ca434cf21430d89`.
The image was fetched because it was no longer in the local image cache. The
ephemeral container had two CPUs and 4 GiB RAM, the source mounted read-only,
the existing verification target/cache mounted writable, and no host credentials
or Docker socket mounted. The command inside it was:

```sh
cargo test --locked -p packedgen --features cache-production-diagnostics,prepared-keys \
    --lib --tests
```

## Performance screen for the maintenance fix

The completed old/new dataset is
`target/bounded-cache-regression-2026-10-03`: 288 successful native processes,
24 configurations, and six pairs per configuration. It compares the preserved
pre-fix executable with candidate SHA-256
`af7ced4f814a6b1819f6e3edce92e312f25c2ea901500a08f5405e2eef2c4952`.
The probe source was identical; only the library maintenance fix changed.
Old/new execution order alternates. No owned test, lint, or container validation
job was running during this cohort. Other host applications remained active.

Reads use two million requests; small-object writes use 500,000 and large-object
writes use 100,000. This cohort is not pooled with the longer QuickCache
comparison. All entries below use owned Arc reads. Positive paired change is
faster; negative is slower. The range shows all six paired changes, not a
confidence interval.

| Payload | Workers | Request | Old Mops/s | Fixed Mops/s | Paired change | Paired range |
| --- | ---: | --- | ---: | ---: | ---: | --- |
| 64 B | 1 | Resident hit | 19.550 | 20.976 | -0.7% | -28.0% to +21.6% |
| 64 B | 1 | Read miss | 52.185 | 51.760 | -0.7% | -2.0% to +7.6% |
| 64 B | 1 | Cold fill | 1.717 | 1.822 | +7.2% | -3.5% to +76.2% |
| 64 B | 1 | Turnover | 6.924 | 6.945 | -2.5% | -10.6% to +4.3% |
| 64 B | 1 | Replace | 4.539 | 4.304 | -4.1% | -6.8% to +4.1% |
| 64 B | 1 | Delete and reinsert | 3.174 | 3.100 | -0.4% | -6.7% to +7.9% |
| 64 B | 2 | Resident hit | 52.831 | 54.363 | +2.4% | -14.1% to +11.8% |
| 64 B | 2 | Read miss | 106.687 | 104.826 | -2.3% | -6.3% to +7.6% |
| 64 B | 2 | Cold fill | 1.491 | 1.531 | +3.5% | -12.5% to +18.6% |
| 64 B | 2 | Turnover | 6.614 | 6.799 | +1.6% | -4.1% to +14.3% |
| 64 B | 2 | Replace | 5.030 | 4.933 | -1.9% | -9.2% to +8.0% |
| 64 B | 2 | Delete and reinsert | 2.414 | 2.407 | +3.9% | -7.0% to +11.9% |
| 64 KiB | 1 | Resident hit | 38.794 | 37.134 | -6.4% | -12.8% to +1.8% |
| 64 KiB | 1 | Read miss | 62.867 | 59.636 | -3.7% | -7.7% to +9.2% |
| 64 KiB | 1 | Cold fill | 0.313 | 0.316 | +2.8% | -2.6% to +12.9% |
| 64 KiB | 1 | Turnover | 1.329 | 1.376 | +3.9% | -12.5% to +16.8% |
| 64 KiB | 1 | Replace | 0.464 | 0.473 | +1.6% | -0.7% to +9.3% |
| 64 KiB | 1 | Delete and reinsert | 0.368 | 0.349 | -3.4% | -16.9% to +9.2% |
| 64 KiB | 2 | Resident hit | 91.240 | 89.641 | -2.0% | -4.4% to +8.9% |
| 64 KiB | 2 | Read miss | 119.217 | 116.392 | -1.7% | -5.1% to +10.2% |
| 64 KiB | 2 | Cold fill | 0.351 | 0.378 | +9.1% | -0.1% to +33.7% |
| 64 KiB | 2 | Turnover | 1.775 | 1.875 | +4.3% | -14.7% to +9.0% |
| 64 KiB | 2 | Replace | 0.594 | 0.588 | +0.4% | -18.4% to +7.1% |
| 64 KiB | 2 | Delete and reinsert | 0.426 | 0.428 | +0.8% | -13.9% to +8.7% |

The one-worker large-object read hit is an unresolved performance flag:
its -6.4% paired median exceeds a 5% regression screen. The other medians being
closer or positive does not erase that flag. Large positive outliers also make
the apparent insertion improvements unsuitable for a performance claim.

Reproduce the screen:

```sh
sh scripts/bounded_cache_regression.sh PATH_TO_PRESERVED_BASELINE target/new-regression
awk -v expected_repetitions=6 -f scripts/summarize_bounded_regression.awk \
    target/new-regression/*.csv
```

## Overloaded followup and remaining qualification

A longer owned-read followup requested 18 pairs and ten million requests per
process, followed by six borrowed-read pairs. It was stopped by the operator
after absolute rates collapsed to roughly 1.1 to 1.2 Mops/s in early resident
controls and observed host load averages rose to 61.17, 45.77, and 28.59 on the
16-core machine. This overload observation does not prove the original -6.4%
flag is noise or explain its cause.

The partial owned dataset remains at
`target/bounded-cache-owned-read-regression-2026-10-03`: 88 CSV paths, including
one unfinished empty file. The strict summary correctly rejects this incomplete
cohort. The queued borrowed followup did not run. Only the owned benchmark
shells and their current probe were terminated; no other applications, services,
or containers were stopped. No incomplete followup rates were pooled into either
completed table.

The runner now supports `direct-owned` or `direct-borrowed` and `full`, `reads`,
or `large-reads`
scope, with independently recorded repetition and read-request overrides. On a
quiet host, the longer read checks can be run with:

```sh
PACKEDGEN_REGRESSION_PAIRS=18 PACKEDGEN_REGRESSION_READ_OPERATIONS=10000000 \
    sh scripts/bounded_cache_regression.sh PATH_TO_PRESERVED_BASELINE \
    target/new-owned-reads direct-owned reads
awk -v expected_repetitions=18 -v expected_configurations=8 \
    -f scripts/summarize_bounded_regression.awk target/new-owned-reads/*.csv
PACKEDGEN_REGRESSION_PAIRS=6 PACKEDGEN_REGRESSION_READ_OPERATIONS=10000000 \
    sh scripts/bounded_cache_regression.sh PATH_TO_PRESERVED_BASELINE \
    target/new-borrowed-reads direct-borrowed reads
```

The deadlock repair has passed its test-driven and bounded correctness gates,
but a no-performance-regression result is not established. Keep the fix under
review; do not revert to the deadlocking implementation to satisfy a throughput
number. The completed followup below did not reproduce the read-hit flag, but
neither cohort establishes a dedicated-host no-regression result.

Peak physical memory, tail latency, all-core scaling, long soaks, sanitizers,
complete unsafe qualification, and the external R-0005 traces still require
their separate gates. The repository remains dirty, with no new commit, push,
merge, or candidate tag from this continuation.

## Focused read followup on a quieter host

With host load averages initially 4.68, 5.42, and 7.70, the exact preserved
pre-repair and post-repair executables were compared again. Each owned and
borrowed cohort contains 12 paired seeds, two million requests per process,
64 KiB values, one worker, and both read controls: 96 processes in total.
No validation or other benchmark jobs from this task overlapped these runs.
Other applications remained running, and each process has before/after host
load records. This remains an exploratory shared-host screen.

| Read path | Request | Old Mops/s | Fixed Mops/s | Paired median change | Paired range |
| --- | --- | ---: | ---: | ---: | --- |
| Owned | Resident hit | 39.316 | 39.574 | +1.6% | -7.3% to +10.0% |
| Owned | Read miss | 63.034 | 61.113 | -3.1% | -6.7% to -0.5% |
| Borrowed | Resident hit | 46.308 | 44.644 | -1.7% | -16.8% to +7.0% |
| Borrowed | Read miss | 65.389 | 64.408 | -1.1% | -4.8% to +1.2% |

Higher Mops/s and positive change are better. The previous -6.4% resident-hit
median was not reproduced. The small miss losses remain visible, especially
the owned case where all 12 pairs were negative. Neither the newer hit result
nor the broad paired ranges prove the earlier result was only noise.

The complete cohorts are
`target/bounded-cache-large-owned-2026-10-03-r2` and
`target/bounded-cache-large-borrowed-2026-10-03-r2`. They retain the same
baseline `4f77eb2a…` and fixed executable `af7ced4f…` as the earlier deadlock
repair comparison. They were run before changing the probe's insertion adapter.

```sh
PACKEDGEN_REGRESSION_PAIRS=12 sh scripts/bounded_cache_regression.sh \
    PATH_TO_PRESERVED_BASELINE target/new-large-owned direct-owned large-reads
awk -v expected_repetitions=12 -v expected_configurations=2 \
    -f scripts/summarize_bounded_regression.awk target/new-large-owned/*.csv
```

## Guarded scalar insertion experiment

The probe now has opt-in `direct-guarded-owned` and `direct-guarded-borrowed`
backends for unique-key cold fill only. They reuse the worker guard for
conditional insertion. Other workloads are rejected rather than silently
replacing upsert semantics with insert-if-absent semantics. The default cache
configuration, synchronous eviction, capacity limits, payload creation,
refresh cadence, and maintenance cadence are unchanged.

Tests were added before the adapter: compilation failed because the new path
did not exist, then passed after implementation. Tests cover concurrent cold
fill across refresh and full maintenance, both payload sizes and guard modes,
rejection of turnover, and buffered insertion-statistic publication. All nine
probe tests passed in the default native run.

The completed cohort at
`target/bounded-cache-guarded-admission-2026-10-03` contains 72 fresh processes:
four configurations, six paired seeds, and three adapters. Both PackedGen paths
and QuickCache run from the same executable. Small values use 500,000 writes
and 50,000 entries; large values use 100,000 writes and 1,024 entries.

| Payload | Workers | Cache-level Mops/s | Guarded Mops/s | QuickCache Mops/s | Guarded versus cache-level paired change | Paired range |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| 64 B | 1 | 1.836 | 1.974 | 9.394 | +6.7% | +2.4% to +14.3% |
| 64 B | 2 | 1.809 | 1.704 | 7.602 | -2.7% | -10.0% to +7.8% |
| 64 KiB | 1 | 0.340 | 0.350 | 0.615 | +3.3% | +0.4% to +9.7% |
| 64 KiB | 2 | 0.423 | 0.433 | 0.583 | +2.9% | -4.2% to +6.7% |

Guard reuse helps the one-writer screens but is not a universal improvement;
the small-object two-writer case loses. QuickCache still leads every cold-fill
cell. Do not promote this adapter as a general insertion fix or pool it with
the earlier full comparison. It remains an opt-in experiment, not a library
default change.

```sh
sh scripts/bounded_cache_admission.sh target/new-guarded-admission
awk -v alternate_backend=direct-guarded-owned -v expected_repetitions=6 \
    -v expected_configurations=4 -f scripts/summarize_bounded_cache.awk \
    target/new-guarded-admission/*.csv
```

## Safe eviction buffer reuse

Reservoir eviction previously took the outer `Vec` out of its mutex, processed
its hints, then extended a fresh empty replacement `Vec` with the survivors.
That reallocated and copied the batch headers on each return. The new path
returns the existing `Vec` directly when the stored list is empty. If another
producer supplied hints, it preserves the prior append order instead of
overwriting them. No unsafe block, read-path operation, per-entry field, victim
policy, capacity threshold, or reclamation protocol was added or changed.

The allocation-reuse test failed on both native macOS and Linux before the
implementation: expected capacity 32, observed capacity 4. It now verifies the
same allocation and capacity after both a partial and complete reservoir drain.
A second test verifies that independently supplied hints survive restoration
in the original stack order. Both are ordinary safe-Rust ownership tests.

Keeping the outer buffer retains its high-water capacity between drains; this
is an explicit memory tradeoff, not evidence of lower total RSS. The confirmed
benefit is avoiding the outer-buffer allocation and header copy on the ordinary
return path. Throughput and physical-memory conclusions require measurement.

Linux Rust 1.88.0 completed 388 tests with no failures across 24 suites, with one
long soak ignored, using `cache-production-diagnostics,prepared-keys` and the
probe tests. Strict native linting passed for all package targets with those
features. The three focused reservoir tests also passed under Miri. Existing
exposed-provenance warnings remain; these are not strict-provenance or complete
unsafe qualification. Native aggregate verification and the performance screen
are recorded below when complete.
