# Rust performance and memory review — 2026-09-12

Base revision: `26d1d93`, branch `codex/p2-churn-tail`. This is a focused
implementation/practices review of the direct cache, arena, hashing, and
qualification tooling, not a new full unsafe audit or R-0005 qualification.

## Verdict

PackedGen has a credible specialized advantage: compact indexing and borrowed
resident reads. It already uses many of the techniques normally recommended
for high-performance Rust. It is not yet demonstrated to be the best general
bounded cache. The P2 report still has higher physical memory and a large-object
throughput deficit at 16 workers; its results are explicitly exploratory.
Neither fewer allocations nor a smaller entry header proves lower CPU cost.

## Guidance checked against implementation

| Practice | Evidence in PackedGen | Assessment |
| --- | --- | --- |
| Borrow instead of deep-copying resident data | `DirectCacheGuard::get` returns `&V`; `get_cloned` makes ownership costs explicit | Keep this separation. New allocation tests protect warm overlay hits/misses for 32/73-byte keys and 64-byte/64-KiB values. |
| Share large values only when needed | `pin_cloned` and `pin_shareable` support `Arc` values | Arc reads avoid payload copies but still cost atomic reference counting. Short owned sessions can reduce retirement retention. |
| Reduce per-record allocations | `DirectValueArena`, block admission, packed keys, bounded prepared replacement recycler | Already implemented. Inspect retained blocks and size-class occupancy before adding more pooling. |
| Measure layout and preserve alignment | `DirectArenaEntry` has `repr(C)`, one atomic metadata word, layout tests; protected handles have size/alignment tests | Already protected. Do not add `repr(packed)` to atomic or reference-containing objects. Large/aligned `V` can introduce padding. |
| Isolate shared counters | Padded capacity/counter/reader shards and batched rolling observations | Already implemented. Padding is a memory tradeoff; 64-byte padding is not proof of no false sharing on every CPU. Profile the actual target. |
| Keep synchronization justified | Epoch publication/reclamation protocol, local ownership wrappers, documented atomic accesses, Loom tests | Do not weaken orderings from throughput measurements alone. Models cover selected protocols, not every production execution. |
| Keep optional overhead optional | Diagnostics and accelerated/prepared-key features are gated; tuned async admission remains opt-in | Preserve default behavior. Report instrumented and uninstrumented throughput separately. |
| Profile optimized code | Existing release probes, allocation instrumentation, memory soak | Added an opt-in `profiling` Cargo profile with release optimization and line tables. No default LTO, allocator, or panic-strategy change. |
| Preserve dependency identity during comparisons | Cargo.lock and pinned safety-sensitive dependencies | Quick proof and comparison commands now use `--locked`. Compiler, features, allocator, source revision, and workload identity still need recording for each result bundle. |

The [Rust Performance Book's allocation guidance](https://nnethercote.github.io/perf-book/heap-allocations.html)
supports allocation-site profiling, avoiding unnecessary clones, and choosing
container capacity based on observed sizes. It also warns that SmallVec-style
allocation reductions can lose speed. These are measurement-led techniques,
not blanket replacements to apply throughout a cache.

The [Rust layout reference](https://doc.rust-lang.org/reference/type-layout.html)
defines representation/alignment constraints; the
[atomic documentation](https://doc.rust-lang.org/std/sync/atomic/index.html)
defines the synchronization obligations. A compact representation must satisfy
both. Existing exposed-provenance handles remain a verification limitation:
[Miri](https://github.com/rust-lang/miri) execution checks, especially permissive
provenance runs, are not a proof that all safe callers and interleavings are sound.

## Important remaining work

1. **Physical-memory accounting under churn.** Correlate peak RSS, live allocator
   bytes, active/reusable arena bytes, retired bytes, and rebuild overlap.
   Keep the same allocator and equivalent payload ownership for QuickCache/Moka
   comparisons. A weight limit is a caller-accounted logical limit, not an RSS
   ceiling; values can own heap storage that `size_of::<V>()` does not include.
2. **Contention and tails.** Profile the maintenance gate, victim reservoir,
   hard-cap reservation, and allocator at 1/4/8/16 workers. Existing batching
   does not prove all foreground work is bounded by time. Retained readers can
   prevent safe reclamation; do not force-free their values to meet a budget.
3. **Threat model.** `GenerationHashBuilder` uses randomized Rapidhash lanes.
   The pinned dependency describes itself as non-cryptographic and only
   minimally HashDoS resistant. Randomized does not mean cryptographically
   secure against adaptive hostile clients. A selectable hardened hasher would
   need design and comparative measurement; it was not silently substituted.
   See [Rapidhash 4.4.2](https://docs.rs/crate/rapidhash/4.4.2).
4. **Generic cache API and evidence.** Dynamic capacity reduction still needs
   a public design and tests. Retain the independent QuickCache/Moka comparisons;
   the local `cache_probe` Papaya control is not their substitute. Do not
   combine mutable/frozen maps, logical/requested bytes/RSS, and different
   cache adapters into one ranking.

## Changes and reproduction

No library runtime code, layout, unsafe block, atomic ordering, or default
feature changed in this review. These are regression-prevention and profiling
improvements, not a claimed throughput increase.

```sh
cargo test --locked --test direct_cache_allocations
cargo test --locked --features cache-production-diagnostics,prepared-keys --test direct_cache_allocations
cargo clippy --locked --test direct_cache_allocations -- -D warnings
cargo build --locked --profile profiling --example cache_probe
```

The allocation regression deliberately has one test in its own binary because
the allocator counts are process-wide. Setup and first-touch state are outside
the measurement. It checks tracked/untracked borrowed reads and cloned Arc
reads; a deep `Box<[u8]>` clone is the positive control. This is not a claim
that cold guards, mutation, expiry cleanup, or concurrent rebuilds allocate
nothing. Instrumented runs must not be used as uninstrumented throughput data.

The [Cargo profiles documentation](https://doc.rust-lang.org/cargo/reference/profiles.html)
explains profile inheritance and debug information. `profiling` inherits
release settings and retains line tables without forcing LTO or CPU-specific
instructions. Downstream applications control profiles at their own workspace
root. Record any frame-pointer, native CPU, LTO, or allocator flags and use
identical flags for every compared implementation.

## Verification in this review

- Rust 1.98.0 on aarch64-apple-darwin.
- Default allocation regression: passed. Each measured warm borrowed/Arc
  read region reported zero allocations, reallocations, and allocated bytes;
  deep-copy controls registered the expected payload allocations.
- Strict Clippy for the new test (including the root library): passed.
- Formatting, shell syntax, locked Cargo metadata, and diff whitespace checks:
  passed.
- The combined library/direct-cache/allocation test run with
  `cache-production-diagnostics,prepared-keys` compiled, but its library test
  executable emitted no test-start output and accumulated no CPU time for
  4 minutes 15 seconds. It was explicitly terminated; this feature-enabled
  run is **unverified**, not passed. Its integration binaries were not reached.
- No new QuickCache/Moka timing comparison, full unsafe audit, Miri, sanitizer,
  or endurance qualification was performed by this review. No runtime cache
  implementation changed, so no new throughput improvement is claimed.
