# Safety review — 2026-08-27

This review covers the root `packedgen` map/cache implementation and the unsafe
surface of the `packedgen-opthash` companion crate. It does not claim that the
experimental library is production-ready.

## Fixed findings

- Writer acquisition now uses a checked compare-exchange. A full 16-bit writer
  count can no longer wrap through zero or carry into the packed length while a
  close/rebuild observes the stripe.
- Direct-cache epoch tokens use checked advancement. Exhaustion stops
  reclamation and retains retired values instead of allowing token ABA.
- Direct reader counts retain their original single `SeqCst` increment. An
  Arc-style half-address-space ceiling aborts before wrap; panicking was
  deliberately rejected because a caught panic could leak repeated increments.
- Direct arena reclamation sorts and validates offsets before running any
  destructor. A duplicate retirement now fails before the first drop instead
  of double-dropping and discovering the duplicate during slot release.
- Direct arena reclamation now has unwind-only release guards. If user `Drop`
  code panics, every value whose destructor started has its occupied bit
  cleared after destructor cleanup, so block teardown cannot drop it twice.
  The normal path forgets the guard and retains the original 16-byte temporary
  records and batched slot release.
- Elastic `clear` now clears each occupied control byte and decrements both the
  level and table lengths before entering user `Drop` code. A caught destructor
  panic therefore leaves a valid map containing exactly the unvisited entries,
  rather than controls that point at already-destroyed values. The traversal
  remains the shared SIMD arena scan.
- The root crate no longer has whole-file `unsafe_code` exceptions. Unsafe is
  allowed only on the pointer-owning implementations/functions that need it,
  with an explicit reason and an immediately adjacent safety argument.
- `clippy::undocumented_unsafe_blocks` is now part of the root lint policy.
- The Python companion binding no longer uses `unreachable_unchecked` to decode
  pointer tags; broken tag invariants take a safe failure path.
- The root crate is explicitly 64-bit-only. Unsupported pointer widths now
  receive a clear compile-time error instead of incidental layout/shift errors.
- The companion crate's raw arena accessors are now `unsafe fn` with explicit
  bounds and initialization contracts. Previously, private safe helpers could
  invoke undefined behavior when their debug-only index assertions were
  violated; every production and test caller now carries the local proof.
- All production and test unsafe sites in the companion crate have adjacent
  safety arguments. `unsafe_op_in_unsafe_fn` and
  `clippy::undocumented_unsafe_blocks` are both denied for that crate.
- CI now executes the root runtime suite with all features enabled, rather than
  relying on an all-feature Clippy build to compile those configurations.
- Raw integer reconstruction of direct-cache and mutable-segment handles is now
  an `unsafe` operation with an explicit publication contract. Each cache has
  one private, documented decoding boundary instead of allowing arbitrary safe
  internal callers to manufacture pointer-bearing handles.
- Retirement now consumes non-`Copy` removed-handle tokens. Exact index
  replacement/removal is the only production transition that can create one,
  making the single-retirement obligation visible to the Rust type system.
- Mutable-segment values now distinguish encoded, live, and removed states.
  Immediate tombstones cannot reach pointer dereference or retirement, and a
  regression test covers that rejection.
- All new handle wrappers are transparent and have compile-time size and
  alignment assertions. Published and removed handles remain exactly eight
  bytes; the stronger ownership model adds no per-entry memory.
- Companion arena allocation now rejects a control-byte extent larger than its
  allocator layout before touching memory. The former safe helper trusted this
  relationship entirely to callers.
- Region cloning now enforces equal source/destination capacities in release
  builds before any raw pointer access, rather than relying on a debug-only
  assertion.
- Allocator-sensitive arena teardown functions are now `unsafe fn`. Every
  caller carries a local proof that the allocator created the arena and that
  initialized values are destroyed exactly once; ordinary safe internal code
  can no longer accidentally deallocate through an unrelated allocator.
- The crate-private `TableBackend` is now an `unsafe trait` with an explicit
  location, initialization, lifecycle, and scan-uniqueness contract. Elastic
  and Funnel implementations document how they uphold that contract.
- The root crate now denies unsafe operations inside `unsafe fn` bodies unless
  each operation has its own explicit unsafe block and local justification.
- Generational-arena replacement, discard replacement, and expiry mutation are
  now `unsafe fn`. Safe cache entry points cross that boundary only beside the
  same-key mutation-stripe guard that makes the raw pointer stable; the locked
  insertion helpers also declare the stripe requirement instead of accepting
  it as an invisible safe-Rust convention.
- Elastic exact-match inspection and Funnel raw-slot inspection/publication are
  now unsafe internal boundaries. Every caller documents the geometry, bounds,
  and initialization proof immediately beside the call. This adds no release
  bounds check, runtime permit, per-entry state, or synchronization.

## Verification

- Root all-feature all-target suite: 299 passed, 0 failed, 1 explicitly ignored
  soak.
- Companion default suite: 368 passed, 0 failed.
- Loom: 3 exhaustive small-state models passed, including the new full writer
  counter versus close race.
- Root and companion strict Clippy suites passed. The root suite enforces
  documented unsafe blocks.
- Python companion feature compiled successfully.
- Targeted Miri checks passed for writer exhaustion, reader-count margin, epoch
  exhaustion, and duplicate retirement.
- Targeted Miri checks passed for live/immediate value separation, partial
  direct-arena reservation, exact retirement after replacement, and protected
  values surviving cache teardown.
- Targeted companion Miri checks passed for arena allocation/layout, direct
  Elastic slot-reference resolution, and Funnel SIMD-group boundary scanning.
- Targeted Miri checks passed for panicking Elastic clear, panicking
  unpublished direct-arena cleanup, and panicking batched direct reclamation.
  The catch-unwind regressions verify exact live counts and exactly one
  destructor call per value after final teardown.
- Strict-provenance Miri passed the new invalid-control-extent rejection,
  mismatched-clone rejection, and the generational cache TTL mutation path.
- Targeted AddressSanitizer duplicate-retirement check passed.
- A fresh ThreadSanitizer run could not be built with the installed 2026
  nightly because sanitizer-instrumented dependencies and the prebuilt standard
  library now reject their ABI mismatch. This is a tooling gap, not a test
  failure; Loom exercises the changed concurrency protocol.

Miri reports the expected warning for direct cache handles: they intentionally
encode a native pointer address in `u64` and reconstruct it with exposed
provenance. Miri can execute that mode, but strict-provenance Miri cannot fully
verify it. The allocation remains alive until epoch quiescence; this is an
architectural verification limitation to keep visible.

## Performance gate

The destructor-unwind follow-up was measured against release baselines captured
immediately before the patch. Elastic clear used 50 Criterion samples over
20,000 entries. Lower time and higher throughput are better.

| Elastic clear | Before | After | Criterion change |
|---|---:|---:|---:|
| Median time | 76.685 us | 73.449 us | -4.1% |
| Throughput | 260.81 Melem/s | 272.30 Melem/s | +4.3% |

The direct cache used 100,000 resident entries, three million operations,
eight threads, and 21 samples. Higher Mops/s is better.

| Direct cache operation | Before | Final unwind-only guard | Change |
|---|---:|---:|---:|
| Remove hit | 34.278 | 33.783 | -1.4% |
| Replace hit | 25.247 | 25.634 | +1.5% |
| Insert miss | 57.820 | 58.478 | +1.1% |
| 95% read cache mix | 138.768 | 138.830 | +0.0% |

An independent remove repeat measured 33.525 Mops/s (-2.2% absolute), while
the unchanged PackedCache control slowed 3.7% and the Papaya Arc control slowed
1.9%. Relative Direct/Packed performance improved about 1.6%, and
Direct/Papaya-Arc changed about -0.3%. The absolute remove movement is therefore
machine drift rather than a measured implementation regression. The first
guard prototype, which enlarged temporary retirement records, measured only
30.807 Mops/s and was rejected; the retained unwind-only design restores the
original normal batching and moves exceptional cleanup off the normal path.

The follow-up safe-boundary batch retains only cold-path and compile-time
changes. A proposed zero-sized mutation permit was removed after its first noisy
run showed red headline numbers; even though an immediate repeat recovered the
baseline, performance certainty takes priority over that additional type proof.
The retained solution instead makes the generational arena operations and
locked helpers explicitly unsafe. It adds no argument, guard representation,
branch, allocation, atomic operation, or published-layout change.

The final release gate used 100,000 resident entries, three million operations,
eight threads, and 31 samples. The saved pre-batch column used 15 samples from
the same local process and workload. Higher Mops/s is better.

| Workload | Pre-batch | Final explicit-boundary build | Change |
|---|---:|---:|---:|
| Replace hit | 15.594 | 15.543 | -0.3% |
| Insert miss | 19.494 | 19.489 | -0.0% |
| Touch hit | 38.426 | 38.236 | -0.5% |
| 95% read cache mix | 89.419 | 88.761 | -0.7% |

The unchanged DirectPackedCache control moved -0.7%/-0.5%/-1.2%/+0.8% across
the same four workloads in the preceding retained run. Papaya controls ranged
from -5.8% to +3.2%. The final PackedCache movements are inside that control
noise. The retained allocation check and clone assertion execute only during
construction/clone; the backend, allocator, lint, and mutation-boundary changes
alter safety contracts rather than steady-state instructions or published
layout.

The Elastic/Funnel slot boundary was additionally isolated with preserved
release binaries built immediately before and after only that source patch.
Both used the same 100-sample Criterion hit benchmark; higher Mops/s is better.

| Lookup | Safe helper before | Explicit unsafe boundary | Change |
|---|---:|---:|---:|
| Elastic read hit | 25.269 | 25.245 | -0.1% |
| Funnel read hit | 60.079 | 59.834 | -0.4% |

Criterion classified both differences as no performance change. An initial
comparison against the older `safety-after` baseline showed Elastic at 25.24
versus 29.30 Mops/s. Rebuilding and measuring the current source with this
entire slot-boundary patch removed produced 25.269 Mops/s, proving that the
non-adjacent gap was not caused by this fix.

The newest type-state hardening was gated against a release build captured
immediately before the change. These are medians from the same 8-thread,
21-sample probe with 100,000 resident entries and three million operations per
sample; higher Mops/s is better.

| Direct cache operation | Before type-state hardening | After | Change |
|---|---:|---:|---:|
| Read hit | 203.255 | 215.144 | +5.8% |
| Read miss | 587.185 | 594.255 | +1.2% |
| Replace hit | 24.409 | 24.802 | +1.6% |
| Insert miss | 55.996 | 58.271 | +4.1% |
| Remove hit | 33.452 | 34.751 | +3.9% |
| 95% read cache mix | 130.905 | 137.398 | +5.0% |

Every p05 result also improved. The positive deltas should be treated as local
run variance rather than a speedup claim; the important gate result is that no
operation shows a measurable regression. The transparent ownership wrappers
compile away, and no atomic ordering, synchronization step, allocation, or
published layout changed.

Earlier safety fixes were gated independently below.

All values below are median Mops/s on the same 8-thread local probe; higher is
better. The direct-cache after-run used 21 samples.

| Direct cache operation | Saved pre-fix | After fixes | Change |
|---|---:|---:|---:|
| Read hit | 122.858 | 134.935 | +9.8% |
| Replace hit | 20.934 | 21.400 | +2.2% |
| Insert miss | 57.736 | 58.289 | +1.0% |
| Remove hit | 35.403 | 35.158 | -0.7% |

The remove difference is normal local noise; its p05 improved from 31.349 to
31.733 Mops/s. No direct-cache category has a measurable regression.

An immediately alternating source-level A/B isolated the writer-counter change:

| Update path | Old unchecked RMW | Checked CAS | Change |
|---|---:|---:|---:|
| Normal | 43.008 | 43.405 | +0.9% |
| Prepared scalar | 64.370 | 63.491 | -1.4% |
| Prepared batch 64 | 151.401 | 150.498 | -0.6% |

Those differences are within run-to-run noise. A checked CAS on every direct
cache read was separately tested and rejected after a roughly 7% loss; the
retained half-space exhaustion guard preserves the original atomic hot path.

The companion arena boundary was measured independently with Criterion before
and after hardening. These are mean Mops/s; higher is better.

| Workload | Elastic before | Elastic after | Change | Funnel before | Funnel after | Change |
|---|---:|---:|---:|---:|---:|---:|
| Insert | 29.883 | 29.810 | -0.24% | 75.471 | 74.809 | -0.88% |
| Read hit | 29.130 | 29.305 | +0.60% | 55.958 | 57.632 | +2.99% |
| Read miss | 3.922 | 3.910 | -0.30% | 26.414 | 26.334 | -0.30% |
| Mixed | 22.660 | 22.654 | -0.03% | 44.280 | 44.188 | -0.21% |
| Delete-heavy | 11.166 | 11.030 | -1.21% | 9.756 | 9.667 | -0.91% |

The retained implementation adds no release-mode bounds branch. An alternative
that used unconditional assertions in each arena access was measured and
rejected after approximately 7% slower Elastic inserts and 12% slower Elastic
read hits. The final deltas above are within local benchmark variance.

## Remaining safety work

- Add a supported, reproducible ThreadSanitizer job using a standard library
  built with the same sanitizer ABI.
- Run pinned Linux endurance/p99 and the ignored reclamation/rebuild soak before
  any production-readiness claim.
- Decide whether to retain exposed-provenance raw-pointer handles or introduce a
  compact ID/directory mode that strict-provenance Miri can verify without
  sacrificing the measured cache hit/update performance.
- Replace private hot-path debug-only bounds proofs with zero-cost validated
  location types. The current raw accessors are correctly unsafe and locally
  documented, but reviewers still validate each caller's bounds and
  initialization proof.
- Move direct and mutable-segment index decoding behind dedicated adapters so
  exact replace/remove operations produce reclamation tokens directly. Current
  decoding is centralized and non-`Copy` retirement prevents accidental token
  reuse, but the operation-to-token relationship remains module convention.
- Consider making the companion arena own its allocator. The current unsafe
  teardown boundary is honest and audited; ownership would remove the allocator
  identity proof completely, at the cost of a wider generic refactor that must
  be layout- and performance-gated.

## Follow-up — 2026-08-28

Two additional unsafe-boundary issues were closed without changing the cache's
published layout or adding work to normal read, update, insert, or delete paths.

First, packed-segment construction now checks that the safe index allocation
exactly matches the raw compaction extent before any pointer arithmetic begins.
The multiplication is checked for overflow and an inconsistent extent fails the
build with `CapacityOverflow`. A release-mode regression test covers short,
long, and overflowing extents. This is a one-time construction check; repeated
500,000-entry build probes remained 0.03 seconds after warm-up both before and
after the change.

Second, `ExtractIf` no longer performs user hashing while a predicate panic is
unwinding. Natural exhaustion and ordinary early drops retain deferred
tombstone cleanup. During an unwind, cleanup is deferred until a later mutation,
so `Drop` invokes neither the user hasher nor allocation. A counting-hasher
regression test covers both Elastic and Funnel backends and verifies recovery on
the next insert.

The retained `ExtractIf` design was selected through source-level A/B testing.
The safety build was compared with an otherwise identical control that omitted
only the panic-state check:

| One-item early drop | Safety build | Control | Criterion result |
|---|---:|---:|---|
| Elastic | 40.543 ns | 39.038 ns | No change detected (`p = 0.42`) |
| Funnel | 18.813 ns | 18.504 ns | No change detected (`p = 0.89`) |

The confidence intervals included zero. A 256-drop burst was also slightly
faster than the previous cleanup implementation (Elastic 5.374 vs 5.525 us;
Funnel 10.905 vs 11.101 us). Full extraction showed no regression. Variants
that put extra state writes around every predicate call, added an inline panic
guard, or moved the threshold check into a cold helper were measured and
rejected because they harmed code layout or the tiny early-drop path.

Final validation on the retained code passed:

- root release suite: 302 passed, 1 deliberately ignored;
- companion release suite: 374 passed;
- strict Clippy for both packages with all targets (and root all features);
- targeted Miri regressions: 3 passed across the packed-index and both
  `ExtractIf` backends.

These changes close the two concrete findings above. They do not replace the
remaining production-readiness work listed in the previous section, especially
pinned sanitizer/endurance coverage and the broader raw-pointer provenance
review.

## Follow-up — forced empty-epoch reuse

A cross-thread Loom schedule exposed one remaining direct-cache
reclamation defect. A reader could remain registered in physical epoch 0 while
maintenance advanced through an empty epoch, load a value afterward, and have
that value retired in physical epoch 1. A later cycle checked epoch 1, observed
no readers there, and reclaimed the value while the epoch-0 reader still held
its pointer.

`DirectArenaOwner::try_advance` now requires every destination physical epoch
to be quiescent before reuse, whether or not that epoch currently owns retired
values. The new model is ordered to reproduce the exact failure and fails on
the old implementation. Reader entry, lookup, pointer encoding, entry layout,
and per-entry memory are unchanged.

The retained fix passed the complete root release matrix, four Loom models,
nine focused Miri cases, all 38 ordinary direct-cache AddressSanitizer tests,
and both ordinary and prepared ThreadSanitizer tests. CI now runs the Loom,
Miri, and sanitizer gates and separately checks the Python feature and the
companion's `no_std` core as an `rlib`.

Preserved before/after release binaries were measured in reversed order with
100,000 variable binary-key entries, three million operations, eight threads,
and seven samples per run. Averaging both orders, the fixed build moved +0.7%
for replacement, +0.8% for deletion, and +0.6% for the 95%-read mix. Twenty
turnover cycles measured 13.171 ms median maintenance before and 13.189 ms
after (+0.14% time), with turnover changing -0.46%. These movements are within
local run noise; the safety fix adds no measurable foreground regression.

Two redundant compile-time pointer-tag alignment assertions were also tested
and rejected. Despite having no executed branch, adding them to the hot arena
modules changed release code layout. Reversed-order runs measured about 2.2%
lower replacement throughput and 1.1% lower 95%-read throughput. The alignment
already follows from the leading `AtomicU64`, so the assertions were removed
rather than retaining a nonessential source change with a repeatable negative
measurement.

Making `DirectArenaPin::get` explicitly unsafe and routing every call through a
private cache adapter was tested separately and also rejected. The change added
no intended runtime operation, but its release layout measured about 2.6%
lower replacement throughput and 5.2% lower 95%-read throughput; deletion was
about 2.6% faster. The mixed result does not justify disturbing the hot cache
layout. Arena-branded handle types remain the preferred future direction, but
they need an architecture-level design and their own performance gate rather
than a syntactic unsafe-boundary refactor.

## Follow-up — active safety gates and scoped unsafe permissions

The repository's active CI now runs the companion crate's complete
strict-provenance Miri suite, its complete AddressSanitizer suite, and the
Python binding's runtime suite. These checks previously existed only in the
nested companion workflow or as local scripts, so ordinary root CI could pass
without exercising them. The AddressSanitizer job explicitly permits allocator
null returns because the parity suite deliberately requests an impossible
allocation to verify fallible `try_reserve`; without that mode ASan aborts
before Rust can observe and handle the allocation failure.

Six companion source modules no longer waive `unsafe_code` for the whole file.
Arena, iterator, SIMD, map-shell, Elastic, and Funnel unsafe permissions are now
attached only to the raw operation, contract, or implementation that needs
them, with a specific reason. The Python module retains one file-level waiver:
PyO3-generated wrappers contain unsafe code outside the lexical reach of a
per-method attribute. The crate-level `deny(unsafe_code)` therefore protects
all other code added to those six modules from silently expanding the unsafe
surface.

Python object tags now use `NonNull::map_addr` rather than converting pointers
to integers and reconstructing pointers from those integers. Tagging and
untagging preserve the original CPython allocation provenance while keeping
the existing 16-byte `HashedAny` layout. The benchmark launcher was also made
safe for empty argument arrays on macOS Bash 3.2, which restored the local
Python/runtime validation path without changing library code.

Final validation on the retained code passed:

- root release suite: 303 passed, 1 deliberately ignored soak;
- root Loom suite: 4 exhaustive models passed;
- companion release suite: 374 passed;
- companion strict-provenance Miri: 343 passed, 31 deliberately oversized
  cases ignored under interpretation;
- companion AddressSanitizer: 374 passed;
- Python runtime suite: 175 passed;
- strict Clippy for both packages, no-std `rlib`, dependency audit, and both
  publish-package verifications.

The main cache release probe built from a clean target before and after this
batch has the same SHA-256 digest,
`bdd51050db787f18e4ba5c1e5f42437d3165c469d6e79942a17a7000bde1cdcf`.
Thus the scoped unsafe permissions, CI integration, shell repair, and Python
feature-gated provenance change produce byte-for-byte identical main-cache
code. A release-mode Python insert A/B was additionally normalized to the
built-in `dict` control to remove machine drift. The provenance-preserving
build changed normalized Funnel and Elastic insert time by approximately
-0.6% and -0.7%, respectively; both are within noise and neither is a
regression.

At that checkpoint, the batch did not claim that all architectural unsafe work
was complete. The
root direct-cache `u64` handles still reconstruct native pointer addresses with
exposed provenance, and safe crate-private arena dereference helpers still rely
on owner/location conventions that the type system does not brand. The tested
syntactic adapter for the direct hot path was rejected for a repeatable 95%-read
regression, so eliminating those remaining conventions requires a different
arena-branded handle design with its own binary and workload gate.

## Follow-up — protected arena handles without hot-path cost

The remaining safe raw-handle dereference convention has now been removed.
`DirectArenaPin` and `CacheValuePin` expose only an unsafe `protect` constructor
that binds a live handle to the pin lifetime. Safe dereference happens through
transparent `ProtectedDirectHandle` and `ProtectedCacheValueHandle` wrappers.
Both wrappers have tests requiring the same size and alignment as their
eight-byte raw handles; they add no entry metadata, allocation, branch, atomic
operation, or synchronization.

The change followed a red-green sequence. Tests first described the protected
API and failed because neither protected type nor constructor existed. After
the wrappers passed their layout and behavior tests, the former safe `get`
methods were removed. The resulting compile failure enumerated every direct and
segment cache dependency, and each was migrated through a private same-cache
adapter that is the sole caller of the unsafe constructor.

The companion arena contract is now also explicit: `ArenaSlots` is an unsafe
trait because its safe default methods dereference implementor-provided control
and slot pointers. Elastic `Level`, Funnel `FlatStorage`, and the bounded test
descriptor carry documented unsafe implementations. The test descriptor now
owns real matching arrays instead of using dangling sentinel pointers, even in
the capacity-mismatch test.

Safety automation was tightened at the same time:

- root `clippy::undocumented_unsafe_blocks` is denied directly;
- the Python feature is checked by Clippy rather than compile-only CI;
- companion strict Miri and AddressSanitizer are separate jobs, with 45- and
  30-minute limits respectively, so the approximately 29.5-minute Miri suite
  cannot consume ASan's execution window.

The performance gate isolated only the old-versus-protected dereference inside
otherwise identical source. The optimized direct-cache symbols retained the
same addresses and byte-for-byte machine code for read, touch, expiration,
conditional insertion, insertion, replacement, removal, and victim removal.
The mutable-segment lookup retained the same instruction sequence; only linked
page addresses moved. Thus the protected proof adds exactly zero hot-path
instructions.

A secondary reversed-order throughput check used 100,000 resident entries,
three million operations, eight threads, and 21 samples per order. Averaging
both orders, higher Mops/s is better.

| Direct cache workload | Preserved prior release | Protected handles | Change |
|---|---:|---:|---:|
| Read hit | 192.969 | 209.173 | +8.4% |
| Read miss | 551.544 | 588.319 | +6.7% |
| Replace hit | 24.552 | 24.674 | +0.5% |
| Insert miss | 57.403 | 57.912 | +0.9% |
| Remove hit | 33.680 | 33.665 | -0.04% |
| 95% read cache mix | 132.945 | 138.692 | +4.3% |

The positive movements are not claimed as a speedup because the primary proof
is the isolated machine-code comparison. The only negative movement is 0.04%,
well below local noise.

Retained validation passed the complete root release suite (303 passed and one
deliberately ignored soak), all four Loom models, all nine focused root Miri
checks, 38 direct-cache AddressSanitizer tests, both ordinary and prepared
ThreadSanitizer concurrency tests, and all three changed companion arena tests
under strict-provenance Miri. The companion release suite remains 374 passing,
and default plus Python-enabled Clippy are clean.

## Follow-up — explicit owner and retirement boundaries

The remaining direct-cache and mutable-segment owner convention is now an
explicit unsafe boundary. Public cache operations remain safe, but the
crate-private operations that dereference, immediately reclaim, defer-retire,
or transfer protection for a raw handle are `unsafe fn` with documented
same-arena, liveness, reachability, and unique-retirement requirements. The
private direct and segment adapters that bind an index handle to an arena pin
are unsafe as well. The exact-removal helpers that mint a non-duplicable
retirement token no longer hide their ownership-transfer precondition behind a
safe function.

This was implemented as two compiler-driven red/green cycles. The first
contract change produced 71 production compile failures and exposed eight
additional unit-test callers. Each production caller was repaired beside one
of three authoritative proofs: an unchanged index value paired with the cache's
own pin, a failed publication leaving an exclusively owned candidate, or a
successful exact removal/replacement transferring sole retirement ownership.
The second red step found six deferred-batch sites that minted removal tokens;
each token construction is now adjacent to its successful exact mutation.
Unsafe permissions remain function-scoped, and every new unsafe expression has
an adjacent safety argument.

The change adds no owner identifier, per-entry field, allocation, atomic
operation, branch, or synchronization. A `prepared-keys` release
`cache_probe` was preserved before the change and rebuilt after the final
source. Both binaries are 3,840,496 bytes. All 2,724,720 executable bytes are
identical, as are the exception tables, read-only constants, strings, unwind
tables, writable data, and thread-local layouts. The whole-file hashes differ
because the Mach-O UUID/link records and 119 bytes of source-location metadata
changed; the latter encode shifted line numbers. Therefore this boundary
tightening adds zero executed instructions and zero cache memory overhead. A
secondary one- and eight-thread smoke comparison covered read hits, misses,
replacement, new insertion, hit/miss deletion, and the 95%-read mix alongside
both Papaya controls; timing movement followed ordinary host noise rather than
the safety candidate, consistent with identical machine code.

Final validation for this follow-up passed:

- formatting plus strict all-target/all-feature Clippy;
- Rust 1.88 all-target/all-feature compatibility for the root package and the
  companion package check;
- complete root release suite: 305 passed and one deliberately ignored long
  soak;
- all four exhaustive Loom models;
- all nine focused root Miri checks under permissive provenance;
- 38 direct-cache AddressSanitizer tests;
- both ordinary and prepared ThreadSanitizer concurrency tests.

This follow-up does not remove the direct cache's exposed-provenance `u64`
pointer representation. That representation remains an explicitly audited
architecture constraint and is why the direct-cache Miri gate uses permissive
provenance. The manual long-running reclamation soak also remains a separate
release/endurance gate.

## Follow-up — raw protected-value constructors and scoped permissions

The final raw-pointer-to-protected-value constructors are now explicit unsafe
boundaries. `ArenaValue::protected`, `ArenaValue::retired`, and
`DirectArenaValue::protected` require the caller to prove the exact collector or
owner, active guard or reader epoch, pointer liveness, and—when retiring—unique
unreachability and reclamation ownership. The four production callers carry
those proofs immediately beside the transfer. Ordinary safe code inside the
arena module can no longer manufacture a dereferenceable protected value from
an arbitrary non-null pointer.

This used a compiler-driven red/green cycle. Changing only the contracts first
produced five expected failures: four unproven production calls and the
deny-by-default unsafe declaration. The green step repaired each caller rather
than weakening the crate lint. At the same time, unsafe permissions covering the
complete `ArenaPin`, `DirectValueArena`, `DirectArenaReservation`,
`DirectArenaPin`, `CacheValueArena`, `CacheValuePin`, and `CacheValueRef`
implementations were moved to only the individual functions that contain or
declare unsafe operations. Future safe methods added to those implementations
therefore remain protected by `unsafe_code = "deny"`.

The change adds no value field, handle field, allocation, branch, atomic
operation, or synchronization. A `prepared-keys` release `cache_probe` was
preserved immediately before the red step and rebuilt from the retained source.
Both Mach-O binaries are 3,840,496 bytes. Their complete 2,724,720-byte
executable `__text` sections are byte-identical with SHA-256
`ed8d932b1e32b6c7adc4b9b3207b5e3a072529d761d0e77c1f067c7112bbd1b9`.
The whole-file hashes differ because the source change also changes linked
metadata; the executable section does not. Together with the unchanged types
and file size, this establishes zero added executed instructions and zero
per-entry memory overhead for the retained batch.

Final validation passed:

- formatting, all-target/all-feature checking, and strict all-target/all-feature
  Clippy;
- Rust 1.88 all-feature library compatibility;
- complete root release suite: 305 passed and one deliberately ignored long
  soak;
- all four exhaustive Loom models;
- all nine focused root Miri checks under permissive provenance;
- all 38 direct-cache AddressSanitizer tests;
- ordinary and prepared-key ThreadSanitizer concurrency tests.

This batch intentionally does not change the exposed-provenance `u64` handle
architecture or claim completion of the separate pinned-Linux endurance gate.

## Follow-up — lower reclamation contracts and unique pool batches

The remaining direct-owner/state helpers that can destroy, defer, or release a
raw allocation are now explicit unsafe boundaries. Their contracts require the
exact owner/state, initialization state, unreachability or quiescence, and unique
release responsibility. Every caller carries its proof beside the call. The
whole-implementation unsafe permissions on `DirectArenaOwner`,
`DirectArenaState`, `CacheValuePool`, and `SharedCacheValuePool` were removed;
only the individual functions that declare or perform an unsafe operation are
permitted to do so.

The cache-value transfer batch is no longer `Copy` or `Clone`. The compiler now
represents a detached intrusive free-list batch as one ownership token. Local
pool absorption returns `Result<(), CacheValueBatch>`, so capacity rejection
returns that exact token instead of depending on a duplicated raw-pointer
descriptor. A focused test forces rejection, verifies the unchanged returned
descriptor, and deallocates it exactly once; the test also passes Miri.

This work used two compiler-driven red/green cycles. Strengthening the raw
contracts first produced 20 expected compile errors: 19 unacknowledged unsafe
transfers and one use-after-move that exposed the former `Copy` dependency.
Removing the four broad unsafe permissions then produced 30 expected lint
failures, enumerating every function that needed a scoped permission. Both red
outputs are retained under `target/safety-review/` for the local run.

The optimized performance proof used the `prepared-keys` release `cache_probe`
immediately before and after the changes. Both binaries are 3,840,496 bytes.
Their complete 2,724,720-byte `__text` sections are byte-identical with SHA-256
`3f513e6d42e4a42e20ca1a75b228f31de1004fd6ba23079cad506ce61e37ec1b`.
The ownership and lint hardening therefore adds zero executable instructions and
zero per-entry memory.

Final validation passed:

- formatting and strict all-target/all-feature Clippy;
- Rust 1.88 all-target/all-feature compatibility;
- complete root release suite: 306 passed and one deliberately ignored long
  soak;
- all four exhaustive Loom models;
- all nine existing focused root Miri checks plus the new ownership-return test,
  under permissive provenance;
- all 38 direct-cache AddressSanitizer tests;
- ordinary and prepared-key ThreadSanitizer concurrency tests.

This batch does not change the exposed-provenance handle representation, replace
the copied Loom model with production atomics, or complete the separate pinned
Linux endurance gate.

## Follow-up — direct-block partition-lock contracts

The five `DirectArenaBlock` operations that read or mutate `UnsafeCell` slot
state are now explicit unsafe boundaries. Their contracts require the caller to
hold the mutex for the partition that owns the block; release additionally
requires an occupied, exactly owned slot. All 13 production calls now carry the
specific partition-guard or exclusive locked-state proof beside the call. Safe
module code can no longer claim, inspect, or release a direct block while
silently omitting that synchronization requirement.

The broad unsafe permissions on the complete `DirectArenaBlock` and
`DirectArenaPartition` implementations were removed. A second compiler-driven
red pass enumerated 27 unique unsafe declarations or operations. Permission is
now limited to the five block-state primitives and six partition functions that
cross the boundary; every other current and future method remains protected by
the crate's deny-by-default unsafe lint. The slot-address helper also no longer
dereferences `MaybeUninit` merely to obtain its pointer: it uses a direct raw
pointer cast from `UnsafeCell::get`.

This work used two red/green cycles. The contract-only red build produced 13
expected unsafe-call errors. Removing the two broad permissions produced 27
unique expected unsafe-code errors. Both compiler outputs are retained under
`target/safety-review/`.

The optimized performance proof used the `gxhash,prepared-keys` release
`cache_probe` immediately before and after the changes. Both Mach-O binaries are
3,838,832 bytes. Their complete executable `__text` dumps are byte-identical
with SHA-256
`37e62ddfd0aa13dd0a907783503c921fced453548c67db31578f787ca4abdf23`.
The retained hardening therefore adds zero executable instructions and zero
per-entry memory.

Final validation passed:

- the three focused partial-reservation and panic-reclamation lifecycle tests;
- formatting and strict all-target/all-feature Clippy;
- Rust 1.88 all-target/all-feature compatibility;
- complete root release suite: 306 passed and one deliberately ignored long
  soak;
- all four exhaustive Loom models;
- all nine scripted focused root Miri checks plus the unique batch-ownership
  test, under permissive provenance;
- all 38 direct-cache AddressSanitizer tests;
- ordinary and prepared-key ThreadSanitizer concurrency tests.

This batch does not replace the copied Loom model with the production epoch
protocol, change exposed-provenance handles, or complete the pinned-Linux
endurance gate.

## Follow-up — scoped value-arena and dynamic-entry permissions

The whole-implementation unsafe permission on `ValueArena` was removed. A
compiler-driven red build identified 15 unsafe declarations or operations,
confined to six functions: collector-protected `get` plus the five same-key
mutation operations `remove`, `replace`, `replace_discard`, `remove_discard`,
and `update_expiry`. Only those functions now receive scoped permission. Safe
allocation, free-slot accounting, access-bit handling, length, weight, pinning,
and construction methods remain protected by the crate-wide
`unsafe_code = "deny"` lint.

The broad permissions on `DynamicEntrySlot` and `DynamicEntryRef` were also
removed. A second red build identified exactly three unsafe sites: destruction
of a failed unpublished candidate, projection of the initialized trailing key,
and projection of the initialized cell. Permission is now limited to `set`,
`key`, and `cell`; empty construction and acquire publication remain entirely
safe and deny future unsafe additions.

The two compiler outputs are retained as
`target/safety-review/value-arena-scopes-red.txt` and
`target/safety-review/dynamic-entry-scopes-red.txt`. No data structure, branch,
allocation, atomic operation, synchronization operation, or runtime expression
changed.

The optimized performance proof used the `gxhash,prepared-keys` release
`cache_probe` immediately before and after the changes. Both Mach-O binaries are
3,838,832 bytes. Their complete executable `__text` dumps are byte-identical
with SHA-256
`37e62ddfd0aa13dd0a907783503c921fced453548c67db31578f787ca4abdf23`.
The retained hardening therefore adds zero executable instructions and zero
per-entry memory.

Focused validation first passed the stale-value-arena-handle case, all four
dynamic-entry exact-layout, over-alignment, failed-publication, and exact-drop
tests, and all 20 generic `PackedCache` lifecycle and concurrency tests. Final
validation passed:

- formatting and strict all-target/all-feature Clippy;
- Rust 1.88 all-target/all-feature compatibility;
- complete root release suite: 306 passed and one deliberately ignored long
  soak;
- all four exhaustive Loom models;
- 14 focused root Miri cases under permissive provenance, including all four
  dynamic-entry tests;
- all 38 direct-cache AddressSanitizer tests;
- ordinary and prepared-key ThreadSanitizer concurrency tests.

This batch does not replace the copied Loom model with the production epoch
protocol, change exposed-provenance handles, harden Python shutdown ownership,
or complete the pinned-Linux endurance gate.

## Follow-up — production-faithful epoch reclamation and pin-bound retirement

The direct-cache reclamation protocol is now shared with its Loom model instead
of being copied into the test. `direct_epoch_protocol.rs` owns reader entry and
exit, tagged epoch-token progression, and advance planning; production uses it
with standard atomics and the model includes the same source with Loom atomics.
The model therefore exercises the actual three-slot state machine, including
the non-contiguous `0, 1, 2, 4` token transition that prevents a stalled reader
from accepting a slot after a complete cycle.

This TDD pass first replaced the shared helpers with deliberately incorrect
stubs. Three reclamation tests failed while the unrelated saturated-counter
test continued to pass. Restoring the real tagged-token behavior then exposed a
genuine production gap: an exact index removal could be published before its
retirement epoch was selected, allowing two collector rotations to reclaim the
allocation while an earlier reader still held it. The failing schedule is
retained in `target/safety-review/production-loom-shared-failure-trace.txt`.

The accepted invariant is:

- every exact removal or replacement is covered from index mutation through
  retirement publication by either a reader pin or a short mutation pin;
- the collector observes all mutation slots as quiescent before rotating;
- every non-current reader epoch is quiescent before rotating; and
- a retired bag is reclaimed only after aging for two rotations.

Mutation pins are 64 cache-line-separated Boolean slots selected from the
calling thread's shard, with an exact counted overflow path. The first mutation
synchronizes once with the reclaim gate before permanently enabling mutator
scans; steady-state mutations use one successful Boolean compare-exchange on
entry and one store on exit. This adds a fixed 4 KiB per cache, not per entry:
about 0.041 B/entry at 100,000 entries and 0.004 B/entry at one million.

Standalone delete misses remain pin-free because the mutation pin is acquired
inside the retry-safe exact-removal predicate only after a value is found.
Removal batches retain one pin for a group of 64 successful exact removals and
publish that complete group before releasing it. Replacement batches now
acquire their existing reader pin before bulk index publication. The two arena
APIs that allowed callers to queue an unpinned removal were deleted, and every
remaining unsafe retirement contract explicitly requires a same-owner pin to
span the exact index mutation through queue publication.

Several safe but slower candidates were rejected rather than retained. A
global zero-delta atomic RMW on every retirement passed Loom but reduced the
eight-thread replacement result by about 34%. A counted sharded mutation pin
reduced the eight-thread delete-only result by about 16.5%. Holding a reader pin
for an entire removal batch reduced batch performance by roughly 6–7%. A
collector retry throttle did not recover useful throughput and delayed memory
reclamation. The Boolean-slot and 64-removal grouping design retained the
required ordering with substantially lower cost.

Optimized `gxhash,prepared-keys` release probes produced the following paired
results. Higher Mops/s is better; small changes around one to three percent are
ordinary run-to-run noise on this host. The final one-time reclaim-gate
handshake was added after these probes; it runs only on the cache's first
mutation and does not alter any measured steady-state hot path.

| Operation | Threads | Before | After | Change |
| --- | ---: | ---: | ---: | ---: |
| read hit | 1 | 29.321 | 29.199 | -0.42% |
| read hit | 8 | 122.733 | 122.458 | -0.22% |
| read miss | 8 | 525.607 | 538.700 | +2.49% |
| replace hit | 8 | 22.864 | 23.058 | +0.85% |
| standalone remove hit | 1 | 16.340 | 15.949 | -2.39% |
| standalone remove hit | 8 | 33.839 | 30.956 | -8.52% |
| batched remove hit | 8 | 59.018 | 73.467 | +24.48% |
| remove miss | 8 | 539.494 | 535.404 | -0.76% |
| 95% read cache mix | 8 | 92.548 | 94.929 | +2.57% |
| unbounded insert miss | 8 | 58.755 | 57.404 | -2.30% |

The standalone eight-thread, 100%-successful-delete microbenchmark is the one
material regression. It pays the ordering operation that closes the unsafe
window on every deletion. The Redis-like read-dominated mix improved, batched
deletion improved by 24.5%, and reads and replacements remained statistically
flat, so the accepted fix does not trade away the intended production profile.

Final validation passed:

- all five Loom models, including the removal model at four preemptions;
- complete all-feature suite: 307 passed, one deliberately ignored long soak;
- strict all-target/all-feature Clippy and formatting;
- all nine focused Miri checks under permissive provenance;
- all 38 direct-cache AddressSanitizer tests;
- ordinary and prepared-key ThreadSanitizer concurrency tests; and
- Rust 1.88 root and companion-crate compatibility checks.

This batch does not prove reclamation under a pinned-Linux week-long endurance
run or remove the remaining audited unsafe storage primitives. It closes the
identified epoch-publication race and narrows the public retirement surface
without changing the read hot path.

## Follow-up — reentrant destructor reclamation deadlock

The full review found one correctness blocker in direct-cache reclamation.
`DirectArenaOwner::try_advance` held the non-reentrant collector gate while
destroying retired values. If a user value's `Drop` implementation performed
the same arena's first standalone mutation, `activate_mutators` attempted to
lock that gate again on the same thread and deadlocked.

This was fixed with TDD. The retained regression retires a value whose
destructor activates and enters the same arena's first mutation. Before the
fix it timed out after two seconds; after the fix it completes normally. The
collector now installs the exact arena owner's address in a safe thread-local
scope only while invoking retired-value destructors. First mutation activation
checks that marker before taking the gate. A same-owner reentrant destructor
can publish the permanent mutation-observation flag directly because its
thread already owns the gate. Re-entry into a different arena still takes that
other arena's gate normally. The scope restores the previous owner even during
unwinding and contains no new unsafe code.

The Loom state model was also corrected to start with mutation observation
disabled, matching production. It now models the real first-use gate handshake
instead of assuming the permanent flag was already set. All five models pass,
and the removal model also passes at four preemptions.

Optimized `gxhash,prepared-keys` release probes alternated the preserved
pre-fix binary with the fixed binary. Values are Mops/s, so higher is better.
Each value below averages two independent median runs; the deeper replacement
probe used 15 samples per run and four million operations per sample.

| Operation | Threads | Before | After | Change |
| --- | ---: | ---: | ---: | ---: |
| read hit | 8 | 111.342 | 113.486 | +1.93% |
| replace hit, deeper probe | 1 | 5.054 | 5.087 | +0.65% |
| replace hit, final-source probe | 8 | 21.675 | 22.155 | +2.21% |
| standalone remove hit | 8 | 26.414 | 30.040 | +13.73% |
| batched remove hit | 8 | 67.198 | 67.572 | +0.56% |
| 95% read cache mix, final-source probe | 8 | 89.193 | 89.489 | +0.33% |

The standalone-delete samples had high tail variance, so their apparent gain
is not treated as a speedup. The realistic read-dominated mix changed by less
than one percent, while the deeper collector-heavy replacement probes did not
regress. The change adds one thread-local machine word per participating
thread, not per cache or entry, and changes neither cache nor entry layout.
The preserved pre-fix binary has SHA-256
`b6ab97f7fa1f8f33be0363c44df1f75d38b749415a1aac81ee8ea5cc611794fd`;
the final-source binary has
`5d3f548eddda5484fd9e777327a02f076e26d913b1e3855cc30f51a6e697dcf1`.

Fresh validation for this follow-up passed:

- the deterministic red/green destructor-reentry regression;
- all five Loom models and the removal model at four preemptions;
- complete all-feature root suite: 307 passed, one deliberate soak ignored;
- strict all-target/all-feature Clippy and formatting;
- the regression under permissive-provenance Miri with symbolic alignment;
- the regression under AddressSanitizer and ThreadSanitizer.

Strict-provenance Miri still cannot execute the direct cache's pre-existing
integer-address handle reconstruction. The new owner marker itself uses the
strict-provenance `ptr::addr` operation and does not expand that boundary.
