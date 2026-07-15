# Implementation status

Date: 2026-07-15

ElasticHash is an experimental, usable single-writer Rust map with a
paper-derived owned core, packed binary keys, explicit fixed capacity, and
reproducible comparisons against HashBrown 0.17.1. It is not yet a production
replacement for SwissTable, ETS, or a concurrent storage-engine index.

An additional immutable `FrozenPackedMap` prototype now demonstrates a stronger
read-only point: 49.004 requested bytes/entry and ~14.5 ns successful lookup at
one million 32-byte keys with hardware-accelerated hashing, versus HashBrown at
84.429 bytes/entry and ~27.1 ns. This is the current best backend for frozen
generations, not a replacement for the dynamic map.

## Current evidence

| Area | Best current evidence | Gate |
| --- | --- | --- |
| Correctness | Full workspace suite, differential tests, 256 short staged-cutover writer traces | Pass for the current single-writer API |
| One-million-entry allocation | 62.533 requested B/entry vs HashBrown 84.429, 25.9% less | Pass |
| Fixed 64 MiB working set | 1,032,192 entries vs 917,504, 12.5% more | Fail: target is 25% more |
| Large successful lookup | Batch-32 54.0 ns/key vs 27.9 ns/key, 1.94x slower | Fail: limit is 1.5x |
| Fixed-budget scalar lookup | Five-sample median 149–159 ns/key vs 35–37 ns/key, about 4.0–4.5x slower | Fail |
| Missing lookup | Roughly 10.1–10.8 ns under deferred churn vs HashBrown 4.35–4.73 ns | Fail |
| Atomic batch loading | 15.6 M entries/s vs 35.7 M entries/s, 2.28x slower | Fail: limit is 2x |
| Deferred delete request path | 98.2 us per 4,096 deletes vs HashBrown 69.6 us, 1.41x slower | Pass request-path ratio only |
| Maintenance pause | About 1.46–1.64 ms for tested rebuild fixtures | Fail: final cutover remains whole-map |
| Concurrent readers | Generation identity and stale-plan model exist; publication/reclamation do not | Not implemented |
| RSS/system win | Requested-byte harness exists; pinned RSS and cold-read workload do not | Not demonstrated |
| Frozen exact generation | 42.0% less requested RAM and ~1.87x faster million-key hits with `gxhash` | Pass for immutable reads; build and portability remain |

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
ELASTICHASH_AUDIT_ENTRIES=65536 \
ELASTICHASH_AUDIT_BUDGET_MIB=4 \
ELASTICHASH_AUDIT_LOOKUPS=100000 \
ELASTICHASH_AUDIT_SAMPLES=3 scripts/audit.sh
```

The next production-critical work is immutable reader publication, delta replay,
epoch reclamation, Loom proofs, and a Linux-pinned RSS/cold-read benchmark.
