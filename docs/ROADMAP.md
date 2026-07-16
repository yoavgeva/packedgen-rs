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
  promotion over the split FoldHash + Gx schedule.
- [x] Measure and reject shared XXH3-128 routing after it was slower than both
  Gx schedules.
- [x] Add an exact linear frozen representation for at most eight entries after
  randomized two-key PtrHash layouts exposed an upstream bounds abort.
- [x] Test and remove an adaptive exact atomic slot cache after it lost 12.2%
  on one-thread and 69.7% on eight-thread hot-set reads.
- [x] Test and remove an exact 8-KiB/thread slot cache after it still lost
  13.9% on one-thread and 37.7% on eight-thread hot-set reads.
- [x] Add opt-in 24-byte prepared-key handles with exact read/update/insert/
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
- Generalize compact new-key storage to arbitrary binary-key lengths without
  losing the inline-32 RAM and insertion gains.
- Amortize or combine writer handoff and exact-length accounting without
  weakening online-rebuild correctness.
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
