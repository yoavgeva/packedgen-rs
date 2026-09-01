# Roadmap

## Alternative backend experiments

- [x] Build an exact frozen PtrHash backend over packed binary keys.
- [x] Compare frozen retained bytes, hits, misses, and construction with HashBrown.
- [x] Prototype PHast+ as an optional exact frozen backend and reject it as the
  default after paired full-generation hit, miss, cache, mutation, RAM, build,
  and rebuild measurements.
- [x] Test and remove two-bit/four-bit pre-routing filters after they failed the
  complete read-heavy cache and negative-operation matrix.
- [x] Implement a safe cache-line non-minimal k-PHF prototype from the July
  2026 paper; retain it only as an experiment after exact 32-byte keys lost
  hit/miss throughput despite a 0.1–0.14 byte/key density gain.
- [x] Implement a complete shared-Gx atomic hash path and retain it only as an
  opt-in experiment after insert-miss and multicore cache regressions prevented
  promotion over the then-current split FoldHash + Gx schedule.
- [x] Replace that portable split schedule with two independently randomized
  FoldHash lanes carried as one 128-bit frozen digest; retain after full
  atomic/rebuild correctness and repeated cache-mix checks.
- [x] Replace the two portable FoldHash lanes with independently randomized
  Rapidhash lanes after preserved-binary admission/read/mix A/B; retain the
  full 128-bit digest domain and leave `shared-gx` opt-in.
- [x] Split direct-value allocation by operation: retain 128-slot thread-local
  blocks for conditional admission and bulk load after the 8-thread 95%-read
  mix improved, while preserving boxed allocation for replacement churn.
- [x] Measure and reject shared XXH3-128 routing after it was slower than both
  Gx schedules.
- [x] Add an exact linear frozen representation for at most eight entries after
  randomized two-key PtrHash layouts exposed an upstream bounds abort.
- [x] Test and remove an adaptive exact atomic slot cache after it lost 12.2%
  on one-thread and 69.7% on eight-thread hot-set reads.
- [x] Test and remove an exact 8-KiB/thread slot cache after it still lost
  13.9% on one-thread and 37.7% on eight-thread hot-set reads.
- [x] Add opt-in 16-byte prepared-key handles with exact read/update/insert/
  remove fallback across wrong keys, maps, overlays, deletes, and rebuilds.
- [x] Measure prepared 1%-hot-set reads at +40.6% to +50.9%, with preparation
  amortizing after roughly two reads.
- [x] Add a 90%-on-1%-hot-set read workload to the concurrent matrix.
- [x] Add allocation-free prepared read batches with exact mixed prepared/
  fallback items and rebuild-publication detection.
- [x] Add bulk prepared-handle refresh; measured 26.4% to 30.3% higher
  preparation throughput than scalar refresh.
- [x] Add exact prepared update and insert/replacement batches with ordinary
  per-item striped pinning and fallback.
- [x] Test and reject one shared prepared writer gate after eight-thread update
  throughput fell 24.6% versus per-item striped pinning.
- [x] Retain opt-in 16-way cache-line-separated prepared batch gates: write
  batches gain 24.2% to 66.4% over scalar prepared operations for 1–8 threads,
  at 1,040 measured bytes per live generation.
- [x] Compare native 32-byte equality, scalar word comparison, and portable
  `wide::u64x4`; reject the explicit SIMD path after it lost 6.5% to 14.8%.
- [x] Remove the scalar four-word equality experiment after its microbenchmark
  win failed to improve real prepared reads (12.150 to 12.190 ns).
- [x] Correct allocator accounting to avoid double-counting reallocation deltas.
- Add batched/prefetched frozen lookup and serialization.
- Build a fully dynamic cache-line bucket directory with dense entry indexes.
- Compare both alternatives under identical fixed-RAM and operation fixtures.

## Gate 0: algorithm and measurement integrity

- Pin and audit the finite elastic geometry and batch schedule.
- Preserve upstream Apache-2.0 notices if code is vendored.
- Add deterministic placement vectors derived independently from the paper.
- Differential-test insert, replace, remove, miss, and rebuild behavior.
- Instrument successful, unsuccessful, and insertion probe counts.
- [x] Measure requested live bytes through a counting allocator.
- [x] Compare retained working sets under the same requested-allocation budget.
- Measure process RSS and page residency under a pinned system workload.

## Gate 1: database key layout

- [x] Replace one allocation per key with an immutable generation-owned key arena.
- [x] Store packed `{segment, key_offset, key_length}` references in eight-byte slots.
- [x] Add prehashed, allocation-free lookup using original byte equivalence.
- [x] Add bounded byte-aware routing rebuild after delete churn.
- [x] Add a verified direct-location accelerator with exact-search fallback.
- [x] Add bucket-local definite-negative proofs guarded by overflow bits.
- [x] Adapt routing-cache size for small epochs to avoid measured memory cliffs.
- [x] Add a permanent one-million-key lookup comparison for out-of-cache behavior.
- [x] Add fixed-size batched lookup with route probes ordered across the batch.
- [x] Compact dead arena bytes during a full generation rebuild.
- [x] Allow the single writer to defer maintenance to a controlled boundary.
- Define a packed metadata value suitable for disk-location indexes.
- Keep hot payload ownership separate from index slots.
- [x] Provide fallible whole-map construction across core and auxiliary indexes.
- [x] Provide an atomic fallible batch-load API with indexed errors.

## Gate 2: concurrent-writer, lock-free-reader generations

- [x] Stage packed-key copying in bounded owner-driven maintenance steps.
- [x] Detect stale staged work and restart its snapshot allocation-safely.
- [x] Publish immutable packed generations with release/acquire ordering.
- [x] Replace generic overlay values by stable pointer-swap cells and reclaim old values.
- [x] Add allocation-free direct atomic frozen slots for a checked word-sized value domain.
- [x] Reclaim replaced overlay records and generations after pinned readers exit.
- [x] Build a new generation without blocking reads or bounded-hot-set writers.
- [x] Replay an exact multi-writer changed-key delta and atomically cut over.
- [x] Replace changed-key replay with striped layered publication so unbounded
  churn does not create an unbounded final replay pause.
- [x] Switch mutable frozen slots on per stripe only after pre-publication
  overlay writers reach zero.
- [x] Remove the separate boxed-key allocation for the common 32-byte atomic
  overlay path by storing the key inline in a lock-free Papaya node.
- [x] Replace the atomic fixed-32 overlay's scalar control loop with whole-word
  tag matching and non-blocking read treatment of unpublished slots.
- [x] Add explicit one-allocation inline overlays for exact 8, 16, 24, 31, 40,
  48, 56, and 64-byte key workloads without reserving unused size classes.
- [x] Let clean direct-base stripes bypass overlay lookup while permanently
  preserving overlay-first ordering after any base-key promotion.
- [x] Embed a zero-byte nine-bit negative fingerprint into short frozen-key
  references and retain exact semantics plus a long-key fallback.
- [x] Add an optional deletion-safe one-byte blocked base filter and measure it
  against exact-only and embedded-fingerprint policies for every point operation.
- [x] Generalize compact new-key storage to arbitrary binary-key lengths with
  native bucket sampling and one allocation for the stable cell plus trailing
  exact key, while preserving the eight-byte slot.
- [x] Move exact-length deltas into the existing writer-stripe words, removing
  the separate 64-way padded counter bank without weakening online rebuilds.
- [x] Combine successful insert length publication with writer release and add
  an exact vacant-entry proof that reuses a cache miss across insertion.
- [x] Add a true single-traversal atomic `get_or_insert` and combine successful
  upsert/delete/prepared-slot length publication with writer release.
- [x] Add an optional reusable operation guard and conditional-insert batch so
  worker loops amortize generation protection across ordinary atomic CRUD.
- [x] Reuse single-RMW acquisition and one pinned immutable base across
  prepared read/update/insert batches; retain exact stale-handle fallbacks.
- [x] Bias packed stripe deltas so 64-bit successful length publication uses
  one non-retrying fetch arithmetic operation instead of a CAS loop.
- [x] Replace load-plus-CAS writer acquisition with one fetch-and-backout RMW;
  retain 4,096 stripes after the smaller routing array regressed insertion.
- [x] Replace fixed 1.25x atomic overlay headroom with a 1.10x primary and a
  lock-free lazily published packed overflow tier; retain Papaya only for
  residual spill and variable-width keys.
- [x] Split the packed overflow reserve into ordered 80%/20% lazy segments,
  reducing retained and simultaneous first-publication memory without adding
  per-entry metadata or a writer wait state.
- [x] Measure the adaptive atomic-slot headroom frontier; retain 1.15x after
  repeatable read/cache/insert gains and reject the flat 1.20x point.
- [x] Revisit exact 48-byte adaptive keys with the retained 1.15x buckets and
  elastic overflow; replace inline Papaya after mixed throughput stayed flat
  while fresh RAM fell to 33.074 B/entry and allocations fell 92.4%.
- [x] Add bounded exponential retry backoff to atomic value updates; improve
  the measured one-hot-key path while retaining distributed update throughput.
- [x] Add an exact allocation-free zero-through-32-byte atomic key mode using
  in-band short lengths and disjoint short/full-width control-tag domains.
- [x] Generalize the packed atomic bucket by physical key width and add a
  zero-through-16-byte mode without duplicating the publication algorithm.
- [x] Instantiate the shared atomic bucket for zero-through-8-byte keys;
  retain the measured 19.230 B/entry integer-sized overlay.
- [x] Add an opt-in learned adaptive cache overlay that shares one capacity
  across observed 8/16/24/32-byte atomic classes and an exact atomic-48 class,
  with exact dynamic fallback for residual widths.
- [x] Validate adaptive distribution drift and expose physical slot,
  six-class distribution, and inline-key spill observability without adding
  request-path counter contention.
- [x] Add serialized churn-aware adaptive rebuild recommendation and
  publication with stale-policy revalidation.
- [x] Compare adaptive mixed keys directly with Papaya, SCC, Flurry, DashMap,
  and locked HashBrown under fresh RAM, full turnover, uniform, hot-set, miss,
  update, delete, and cache-mix traces.
- [x] Move the bounded adaptive learning sample from Papaya into native stable
  cells, keep it physically sampleable after publication, and expose exact
  16-byte prepared read handles for native sample/fixed-class slots.
- [x] Reuse a carried atomic vacancy in Direct cache admission and preserve the
  fast successful-delete path while making guard delete misses read-only.
- [x] Bound miss-only benchmark setup state and alternate focused Direct/Papaya
  controls inside one process with identical final populations.
- Add pinned Linux scaling and per-operation p50/p95/p99/p99.9 latency.
- Add native libcuckoo, oneTBB, and Folly controls through a serialized common
  trace without including language-FFI overhead.
- Add a FerricStore shadow adapter and runtime maintenance telemetry.
- Further improve the remaining single-thread conditional-insert gap.
- Generalize allocation-free slots through explicit safe atomic value codecs.
- Prove/model the striped open-count-to-closed handoff and define a maintenance
  starvation policy.
- [x] Model stale-cutover rejection across short insert/delete/update traces.
- Prove the concurrent publication and reclamation implementation with Loom.

## Gate 3: service semantics

- [x] Bound deferred tombstones with a measured forced-maintenance ceiling.
- [x] Definite-negative membership policies with deletion-safe maintenance.
- TTL metadata and conditional delete/update primitives.
- Batch lookup and batch publication to amortize FFI or network boundaries.
- [x] Expose maintenance runs, staging failures, and reclaimed arena capacity.
- [x] Configurable pressure/rebuild thresholds.

## Release gates

A stable release requires:

- no correctness mismatch in long differential/property runs;
- Miri, sanitizers, and Loom clean;
- p99 read latency measured during writes and rebuilds;
- missing-key benchmarks, not only successful lookups;
- measured total bytes per live entry including arenas and filters;
- a documented crash/recovery contract for every consumer.
