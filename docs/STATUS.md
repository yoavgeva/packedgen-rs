# Implementation status

Date: 2026-07-16

PackedGen's primary backend is a PtrHash-indexed packed generation with a
lock-free mutable overlay. The repository also contains an experimental
paper-derived single-writer map, a lock-based `ConcurrentSwissMap`, and Papaya
controls. The packed generation has lock-free readers, concurrent writers,
exact striped layered publication without changed-key replay, and an atomic
`NonMaxU64` specialization with direct mutable frozen slots. It has
reproducible comparisons against HashBrown 0.17.1 and DashMap 6.2.1, but is not
yet a production replacement for SwissTable or ETS.

An additional immutable `FrozenPackedMap` prototype now demonstrates a stronger
read-only point: 49.004 requested bytes/entry and ~14.5 ns successful lookup at
one million 32-byte keys with hardware-accelerated hashing, versus HashBrown at
84.429 bytes/entry and ~27.1 ns. For mutable packed keys, packed two-choice
route metadata and a 64-KiB arena bring the adaptive layout to 54.991 requested
bytes/entry at one million keys; the read-optimized policy is 59.022 B/entry.
These are storage/layout improvements, not a claim that
the paper-derived Elastic probe is faster than SwissTable.

## Current evidence

| Area | Best current evidence | Gate |
| --- | --- | --- |
| Correctness | Full workspace suite, differential tests, 256 short staged-cutover writer traces | Pass for the current single-writer API |
| One-million-entry allocation | 54.991 requested B/entry (adaptive) vs HashBrown 84.429, 34.9% less | Pass |
| Fixed 64 MiB working set | 1,032,192 entries vs 917,504, 12.5% more | Fail: target is 25% more |
| Large successful lookup | Batch-32 54.0 ns/key vs 27.9 ns/key, 1.94x slower | Fail: limit is 1.5x |
| Fixed-budget scalar lookup | Five-sample median 121.1 ns/key vs 39.7 ns/key, 3.05x slower | Fail |
| Missing lookup | Roughly 10.1–10.8 ns under deferred churn vs HashBrown 4.35–4.73 ns | Fail |
| Atomic batch loading | 15.6 M entries/s vs 35.7 M entries/s, 2.28x slower | Fail: limit is 2x |
| Deferred delete request path | 98.2 us per 4,096 deletes vs HashBrown 69.6 us, 1.41x slower | Pass request-path ratio only |
| Maintenance pause | About 1.46–1.64 ms for tested rebuild fixtures | Fail: final cutover remains whole-map |
| Concurrent RAM | 54.309 B/entry with 64 shards vs DashMap 84.438 | Pass: 35.7% less requested RAM |
| Concurrent correctness | Multi-writer inserts, atomic updates/insert-new, read/write overlap, delete compaction | Pass for lock-based point operations |
| Concurrent read hit, 8 threads | 83.57 Mops/s vs DashMap 79.72 Mops/s | Pass in local smoke; pinned run pending |
| Concurrent cache mix, 8 threads | 39.31 Mops/s vs DashMap 77.30 Mops/s | Fail: DashMap is 1.97x faster |
| Atomic lock-free packed RAM | 48.761 B/entry empty and after 1% existing-key churn vs DashMap 84.438 | Pass: 42.3% less requested RAM |
| Atomic warm update allocation | 0 bytes/update vs generic generation 24 and Papaya owned-key update 89 | Pass for `NonMaxU64` |
| Atomic packed read hit, 8 threads | 110.802 Mops/s vs DashMap 68.060 | Pass in latest local smoke; 1-thread result loses |
| Atomic packed distributed update, 8 threads | 41.877–64.544 Mops/s vs DashMap 54.555–59.819 | Inconclusive until pinned; same order of magnitude |
| Atomic packed delete hit, 8 threads | 42.415 Mops/s vs DashMap 28.583 | Pass in latest local smoke |
| Atomic packed cache mix, 8 threads | 90.610 Mops/s vs DashMap 71.301 | Pass in latest local smoke; pinned run pending |
| Atomic prepared 1%-hot-set reads | 49.226–380.798 Mops/s across 1–8 threads; +40.6% to +50.9% vs normal atomic | Pass opt-in path; 24-byte handles add 0.24 B/total key when retained for 1% |
| Atomic prepared batch-16 hot-set reads | 48.713–416.942 Mops/s across 1–8 threads; +12.0% to +21.0% vs scalar prepared | Pass; 1.11x/1.78x/2.83x/4.15x DashMap at 1/2/4/8 threads in the same 31-sample trace |
| Atomic prepared 1%-hot-set updates | +28.3%, +14.9%, +17.7%, and +7.6% at 1/2/4/8 threads | Pass opt-in path; stale/wrong handles fall back exactly |
| Atomic prepared sharded-gate batch updates | 42.203/62.851/85.426/107.078 Mops/s at 1/2/4/8 threads | Pass opt-in write mode; +39.8%/+52.0%/+66.4%/+52.2% vs scalar prepared |
| Atomic prepared sharded-gate replacement batches | 41.651/57.369/78.834/108.648 Mops/s at 1/2/4/8 threads | Pass opt-in write mode; +24.2%/+43.4%/+29.6%/+43.6% vs scalar prepared |
| Prepared batch-gate RAM | +1,040 requested bytes and one allocation per live generation; +0.0104 B/key at 100K | Pass as opt-in; no per-key gate growth |
| Prepared batch-gate rebuild handoff | 10.666 to 22.250 us median under eight continuous batch writers; build 15.314 vs 15.415 ms | Accept opt-in tradeoff; single shared gate rejected |
| Explicit SIMD fixed-32 equality | `wide::u64x4` was 6.5–14.8% slower than native equality; manual words gave no end-to-end read win | Reject new lookup SIMD; retain proven SIMD tag scans and hardware hashing |
| Atomic prepared-key amortization | 21.125 ns preparation; 23.038 to 12.209 ns/read; break-even 1.95 reads | Pass for repeatedly accessed frozen keys |
| Atomic prepared bulk refresh | 21.125 to 16.208 ns/handle at 100K entries; 20.504 to 16.217 at 1M | Pass; +30.3%/+26.4% preparation throughput |
| Atomic one-hot-key update, 8 threads | 5.204 Mops/s vs DashMap 43.483 | Fail: CAS retry storm |
| Atomic vs DashMap warm cache mix, 8 threads | 134.627 vs 87.813 Mops/s at 90% read hits; 169.376 vs 87.475 at 95% | Pass local alternating probe; +53.3%/+93.6%, pinned confirmation pending |
| Atomic vs DashMap warm single-thread cache mix | 21.645 vs 34.939 Mops/s at 90% read hits; 23.460 vs 36.528 at 95% | Fail: DashMap is 1.61x/1.56x faster |
| Atomic vs DashMap new-key insert, 8 threads | 30.868 vs 51.320 Mops/s | Fail: DashMap is 66.3% faster |
| Atomic vs DashMap one-hot update, 8 threads | 4.134 vs 58.872 Mops/s | Fail: DashMap is 14.2x faster |
| Atomic delete/reclamation RAM | 97.578 B/live entry after 50% delete; 48.915 after rebuild vs DashMap 136.875 | Pass retained RAM; rebuild peak and policy remain open |
| Atomic inline-32 new-key RAM | 58.951 B/entry vs boxed-key Papaya 82.931 | Pass: 28.9% less and half the allocations |
| Atomic inline-32 focused insert, 8 threads | 32.488 Mops/s vs boxed-key Papaya 26.882 | Pass: 20.9% faster in 11-sample alternating probe |
| Atomic inline-32 focused distributed update | 58.289 Mops/s vs boxed-key Papaya 49.991 | Pass in focused local probe; pinned confirmation pending |
| Atomic configured-width new-key RAM | Inline 16/31/40/64-byte keys use 20.6–35.4% less requested RAM than boxed controls and half the allocations | Pass for exact configured widths; variable-length packing open |
| Atomic configured-width insertion | 21-sample inline/boxed medians range from +9.4% at 16 bytes to -3.0% at 40 bytes | RAM win; performance is width-dependent |
| Atomic clean-stripe read routing | Read hit 169.165 Mops/s vs 153.094 overlay-first; read miss 206.239 vs 212.440 | Keep; +10.5% hit, -2.9% miss in immediate A/B |
| Atomic focused operation coverage | Read/insert/update/delete hit and miss plus hot-key update in one alternating-order probe | Pass harness coverage; pinned run pending |
| Atomic embedded base fingerprint RAM | 48.789 B/entry at one million 32-byte keys, identical to exact-only; one-byte filter is 49.789 | Pass: optional miss accelerator adds zero retained bytes |
| Atomic embedded fingerprint misses | Warm read miss 45.181 vs 28.487 Mops/s; update/delete miss +8.6%/+14.8% in the controlled eight-thread probe | Keep as explicit miss-heavy policy; pinned confirmation pending |
| Atomic embedded fingerprint hit tradeoff | Warm read hit 26.734 vs 28.192 Mops/s; exact-only also wins tested 95–99% single-thread hit mixes | Exact-only remains the default for read-heavy maps |
| Dense ArcSwap-32 insertion | 1.941 Mops/s vs boxed-key Papaya 26.882 | Rejected: RAM win does not justify ~14x insertion loss |
| Lock-free readers | ArcSwap generation + Papaya overlay; readers continue through rebuild | Implemented and stress-tested |
| Lock-free generation writes | Papaya/direct-slot CAS plus 4,096 striped routing states and 64 length deltas; no point-path lock | Implemented; naive cross-layer and length-baseline races rejected by tests |
| Concurrent online rebuild | 4–28 us median stripe handoff and 2–16 us base publication | Pass local smoke; operations continue during handoff/build |
| Unbounded-churn rebuild | Whole-table sweep no longer has 522 ms replay; one stripe-handoff outlier was 3.6 ms | Replay fixed; pinned p99 and maintenance-starvation policy open |
| RSS/system win | Requested-byte harness exists; pinned RSS and cold-read workload do not | Not demonstrated |
| Frozen exact generation | 42.0% less requested RAM and ~1.87x faster million-key hits with `gxhash` | Pass for immutable reads; build and portability remain |
| Fixed-32 SoA specialization | 45.89 B/entry; faster random hits but slower misses/removes than PackedSwiss | Pass for fixed-width RAM; mixed-operation gate open |
| Hybrid 1% generation | 49.033–50.043 B/base entry depending on filter | Pass for read-mostly RAM; misses/inserts remain slower |

Numbers are Apple M4 Max development-machine smoke measurements unless noted.
They are useful for direction and regression detection, not cross-machine
claims. The repository's Linux-pinned benchmark protocol remains the authority
for release decisions.

## Ten-milestone implementation pass

1. Maintenance outcomes and isolated rebuild timing.
2. Bounded staged arena-copy preparation.
3. Safe restart of structurally stale plans.
4. Fallible whole-map construction.
5. Atomic fallible batch loading with indexed errors.
6. Measured and bounded deferred tombstone degradation.
7. Validated configurable soft/hard maintenance pressure.
8. Observable published generations and staged-cutover state modeling.
9. Same-requested-RAM working-set comparison harness.
10. Repeatable repository audit and consolidated release evidence.

## Reproduce

Run the complete local audit:

```text
scripts/audit.sh
```

For a faster smoke run, lower the fixture sizes without changing the harness:

```text
PACKEDGEN_AUDIT_ENTRIES=65536 \
PACKEDGEN_AUDIT_BUDGET_MIB=4 \
PACKEDGEN_AUDIT_LOOKUPS=100000 \
PACKEDGEN_AUDIT_SAMPLES=3 scripts/audit.sh
```

The next production-critical work is arbitrary-length packed new-key storage,
writer-accounting amortization, a
generic atomic value codec, hard overlay limits, a proof/model of striped handoff/direct-base
activation, traversal/eviction APIs, and a
Linux-pinned RSS/cold-read/p99 benchmark. The current lock-free evidence and
claim boundary are in `EXPERIMENT_LOCKFREE_GENERATIONS.md`.
