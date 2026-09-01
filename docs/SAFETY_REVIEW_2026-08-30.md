# Safety follow-up — 2026-08-30

This follow-up covers the root PackedGen cache and the `packedgen-opthash`
companion. It records the fixes made after the 2026-08-27 review. It does not
turn the experimental crate into a production-readiness claim.

## Fixed

- `RegionCursor` now carries its slot type. A region entered as
  `ArenaSlots<E>` can only yield `*mut E`; the former independent `enter<T>` and
  `step<E>` type parameters can no longer disagree in safe internal code. The
  marker adds no storage or runtime work.
- Zero-sized arena layouts use checked safe `Layout` constructors. Two
  unnecessary `from_size_align_unchecked` calls were removed.
- Python key destruction uses `Python::try_attach`. Interpreter shutdown or GC
  traversal can no longer make `HashedAny::drop` panic while attempting to
  attach. If CPython is unavailable, the final reference is deliberately left
  to process teardown.
- Direct-cache retirement batches retain ownership of every unprocessed token
  until reclamation completes. If arbitrary `V::drop` code panics, remaining
  boxed handles and staged arena handles are returned to an owner queue.
- Arena reclamation releases the entry whose destructor unwound and requeues
  only entries whose destructor never started. Retrying reclamation therefore
  neither double-drops the failed entry nor loses the remaining cache capacity.
- Recycled boxed allocations have an unwind deallocation guard, so a panicking
  destructor cannot lose the allocation before it reaches the recycle pool.

## TDD regressions

The boxed/mixed-batch regression was run against the old implementation first
and failed with an empty owner queue where two unprocessed retirements were
expected. It passes after the ownership guard. A second regression covers the
arena path and verifies that the two destructors not yet started are requeued,
then reclaimed exactly once on retry. Both pass under Miri.

The Python suite now starts subprocesses with live Elastic and Funnel maps and
requires clean interpreter shutdown. The typed cursor is exercised by the
existing Elastic/Funnel mutable-iterator parity tests and strict-provenance
Miri.

## Verification

- Root all-feature suite: 311 passed, 0 failed, 1 long soak ignored.
- Companion Rust suite: 374 passed, 0 failed.
- Python extension suite: 177 passed, 0 failed.
- Loom: all 5 cache concurrency models passed.
- AddressSanitizer: all 38 direct-cache integration tests passed.
- ThreadSanitizer: concurrent replacement/pinned reads and prepared recycled
  replacement passed.
- Miri: both new panic regressions passed with permissive provenance; both
  Elastic and Funnel `iter_mut` parity tests passed with strict provenance.
- Root and Python-feature companion Clippy gates passed with warnings denied.
- Formatting, diff checks, and the RustSec dependency audit passed.

## Performance gate

The release cache probe used 100,000 resident entries, 3,000,000 operations,
8 threads, and 11 samples. Higher Mops/s is better.

| DirectPackedCache workload | Median Mops/s | p05 Mops/s |
|---|---:|---:|
| Replace hit | 26.485 | 25.121 |
| Insert miss | 57.910 | 55.086 |
| 95% read mix | 142.501 | 133.942 |

The changes do not alter the read/index hot path, atomic orderings, entry
layout, or eight-byte published handles. Normal reclamation remains batched;
queue restoration and one-at-a-time release are unwind-only behavior.

## Remaining architectural limitation

Direct-cache handles intentionally encode native addresses in eight-byte index
values and reconstruct them with exposed provenance. Safe public callers cannot
forge these private handles, and the focused permissive-provenance Miri,
AddressSanitizer, ThreadSanitizer, and Loom checks pass. Strict-provenance Miri
still cannot fully verify this representation.

A block-ID/slot directory could preserve strict provenance, but it would add a
lookup to the cache hot path. It should be developed as a separately benchmarked
prototype and retained only if its latency and RAM costs pass the existing
gates; this review does not mislabel that verification limitation as a proven
memory-safety defect.

## Frozen-index arbitrary-miss fix — 2026-08-31

The full differential gate exposed an intermittent out-of-bounds unchecked
read in the former `ptr_hash` 2.0.1 dependency. Its packed remap vector was
initialized only through the highest remap slot used by construction members,
but an arbitrary non-member query could address the remaining logical tail.
The failure reproduced in `concurrent_mixed_writes_and_maintenance_preserve_exact_state`
after roughly 173 repetitions and terminated inside `Packed::index`.

The failure-first regression
`arbitrary_misses_never_reach_an_unmapped_ptrhash_tail` builds many small frozen
maps and exhaustively probes non-members chosen to exercise that tail. It
aborted against 2.0.1 and passes with 2.1.1. The concurrent differential test
also completed 500 consecutive repetitions after the fix.

PackedGen now pins official `ptr_hash` 2.1.1. This matters for publication: a
repository-local Cargo patch would not protect downstream users of the crate.
No new PackedGen `unsafe` block was added. Two internal frozen-lookup boundaries
are forced inline because upstream's small `index` wrapper is not annotated for
inlining; this recovers more than the safe dependency's original lookup cost.

| Gate | Unsafe 2.0.1 control | Safe 2.1.1 + minimal inlining | Result |
| --- | ---: | ---: | --- |
| Frozen hit, median | 8.029 ns | 6.761 ns | 15.8% lower latency |
| Frozen miss, median | 9.501 ns | 7.347 ns | 22.7% lower latency |
| Frozen build, 16,384 entries | 1.337 ms | 1.345 ms | 0.64% slower |
| Frozen live RAM, 50,000 entries | 49.628 B/entry | 49.628 B/entry | unchanged |
| Frozen live RAM, 1,000,000 entries | 46.557 B/entry | 46.557 B/entry | unchanged |

Construction inlining experiments recovered a fraction of the 0.64% build
cost, but made frozen hits 1.5-3% slower through code-layout growth, so they
were rejected. Selected-cache screening found no supported throughput or hit
quality regression: seven-pair resident-hot was neutral, while longer scan and
pressure controls had positive paired medians. Correctness violations remained
zero. The small build cost is retained as the only measured tradeoff for
eliminating the dependency UB; it affects frozen construction/rebuild, not the
read path.
