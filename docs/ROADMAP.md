# Roadmap

## Gate 0: algorithm and measurement integrity

- Pin and audit the finite elastic geometry and batch schedule.
- Preserve upstream Apache-2.0 notices if code is vendored.
- Add deterministic placement vectors derived independently from the paper.
- Differential-test insert, replace, remove, miss, and rebuild behavior.
- Instrument successful, unsuccessful, and insertion probe counts.
- Measure resident bytes through a counting allocator and process RSS.

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
- Define a packed metadata value suitable for disk-location indexes.
- Keep hot payload ownership separate from index slots.
- [ ] Provide fallible whole-map construction and batch-load APIs.

## Gate 2: single-writer, lock-free-reader generations

- Publish immutable entry records with release/acquire ordering.
- Replace values by pointer swap; reclaim old records after readers exit.
- Build a new generation without blocking reads.
- Replay the single-writer delta and atomically cut over.
- Prove no stale resurrection across insert/delete/rebuild races with Loom.

## Gate 3: service semantics

- Tombstone and deletion policy with bounded degradation.
- Definite-negative membership filter with deletion-safe maintenance.
- TTL metadata and conditional delete/update primitives.
- Batch lookup and batch publication to amortize FFI or network boundaries.
- Operational metrics and configurable pressure/rebuild thresholds.

## Release gates

A stable release requires:

- no correctness mismatch in long differential/property runs;
- Miri, sanitizers, and Loom clean;
- p99 read latency measured during writes and rebuilds;
- missing-key benchmarks, not only successful lookups;
- measured total bytes per live entry including arenas and filters;
- a documented crash/recovery contract for every consumer.
