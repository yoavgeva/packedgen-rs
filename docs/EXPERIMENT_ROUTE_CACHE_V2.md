# Two-choice packed route-cache experiment

## Question

Can direct locations cover nearly every paper-derived Elastic route without
spending five metadata bytes per cached entry or slowing the common paths?

## Design

The second route cache makes two changes:

- `(level, slot)` is converted to one capacity-aware geometric location code;
- the remaining `u32` bits hold a fingerprint, so location and tag occupy one
  four-byte word instead of separate `u32` and `u8` arrays.

Four-entry buckets use two candidate locations and bounded cuckoo-path
relocation. Any overflow, unrepresentable location, stale route, or fingerprint
collision falls back to the exact Elastic schedule. The cache can therefore
change latency but not map semantics.

The scalar lookup API exhausts and verifies matching route candidates, then
checks only the two overflow bits. This avoids rescanning both buckets to make
the negative proof. A sparse fast path for the two-slots-per-entry policy places
into the primary bucket immediately when it has space.

## Results

Apple M4 Max release-mode smoke measurements, one million 32-byte keys and
`u64` values unless noted:

| Metric | Previous cache | Packed two-choice | Result |
|---|---:|---:|---:|
| Adaptive total RAM | 55.991 B/entry | **54.991 B/entry** | 1.000 B/entry saved |
| Adaptive route metadata | 5.031 MB | **4.031 MB** | 19.9% less |
| Adaptive cached routes | 80.49% | **96.74%** | 16.25 points more |
| Read-optimized total RAM | — | **59.022 B/entry** | all 1M routes cached |
| 32K successful hit | ~26.73 ns | **~24.25 ns** | about 9% faster |
| 32K miss | ~9.82 ns | ~10.84 ns | about 10% slower |
| 16K bulk insert, reserve 1/64 | ~17.58 M/s | ~16.31 M/s | about 7% slower |
| 1M successful hit | ~61.42 ns | **~54.83 ns** | about 11% faster |

The first two-choice version rescanned both buckets after candidate iteration.
It measured 15.83 ns on the 32K miss fixture, 61% behind the old cache. Reusing
the exhausted candidate scan's proof reduced that to 10.84 ns. The first
insertion version also scanned both buckets even under the sparse
read-optimized policy and measured about 14.4 M entries/s. Lazy secondary
inspection recovered that to 16.3 M/s.

The initial 128-bucket relocation search retained 98.25% of routes, but the
unified operation probe exposed an approximately 11-us insertion tail while
filling the last five percent of adaptive capacity. A four-node bound retains
96.74% of routes and reduced that final-percent insertion measurement to about
273 ns. This coverage/insert trade is the current default.

## Decision

Keep this route cache. It improves RAM, route coverage, and both measured hit
fixtures, with modest remaining miss and insertion costs. The exact Elastic
fallback is still far slower than a direct hit, so the coverage increase is
more important at the adaptive one-slot budget than its percentage alone
suggests.

Correctness tests cover disabled and tiny caches, one-million-entry geometry,
the largest packed geometry, unpackable fallback, overflow safety, relocation,
clear, and the combined candidate-plus-negative proof.
