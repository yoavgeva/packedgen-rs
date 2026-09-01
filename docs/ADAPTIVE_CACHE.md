# Adaptive cache overlay

`AtomicGenerationOverlay::AtomicAdaptive` is the variable-key cache mode for
callers that cannot bound maximum key length. It is experimental and remains
opt-in.

## What it does

The overlay begins with a bounded exact sample in a native append-only
stable-cell table. The sample target is between 64 and 4,096 inserted keys,
scaled to roughly 1/64 of configured capacity. It counts the observed
zero-through-8, 9-through-16,
17-through-24, 25-through-32, exact-48, and residual-width populations.

After sampling, one writer builds five atomic tables, including an exact
six-word 48-byte class, whose logical capacities share one total budget in
proportion to the observed distribution. Atomic classes retain the same 1.15x
physical headroom and lazy 1/32 overflow reserve as the fixed-width modes.
Post-learning residual widths use a concurrent arbitrary-length table with
four bucket choices and eight append-only slots per bucket. Each successful
claim publishes one allocation containing the stable value cell and trailing
exact key bytes; deletes change
the cell and generation rebuild reclaims physical slots. The table exposes
rotated bucket sampling directly, so cache eviction does not need to walk a
logical hash-map iterator. Sampled keys remain in their native stable cells
after publication; only emergency saturation spills to Papaya. Separate
bounded two-bit hints avoid unnecessary sample/emergency and residual probes
while preserving no-false-negative routing.

Publication closes and drains sampling writers before the learned tables
become writable. This prevents a key from being published in both the sample
and an atomic class. Steady-state reads and writes use no library-owned lock;
writers arriving during the one-time publication handoff can spin briefly.
Consequently, the mode is not yet a strict lock-free guarantee across its
learning transition.

## Capacity means churn capacity

Atomic key slots are not reclaimed until a generation rebuild. A cache should
therefore configure:

```text
overlay_capacity = maximum live overlay keys
                 + expected new-key insertions before the next rebuild
```

Replacing or deleting an existing key changes its atomic value but does not
free its key slot. If a cache holds 10 million live keys and expects 500,000
distinct new keys before maintenance, use at least 10.5 million—not 10
million—as the overlay capacity. Rebuild or rotate the generation before the
planned churn reserve is exhausted.

The library can now measure this itself; the initial capacity is a generation
churn budget, not a maximum cache population. Maintenance reuses the same
budget after packing live records into the frozen base.

## Rust usage

```rust
use packedgen::{
    AdaptiveRebuildPolicy, AtomicGenerationOverlay,
    LockFreeAtomicU64GenerationMap, NonMaxU64,
};

let map = LockFreeAtomicU64GenerationMap::try_from_entries_with_overlay(
    std::iter::empty::<(Vec<u8>, NonMaxU64)>(),
    1_100_000, // live keys plus insertion churn before rebuild
    AtomicGenerationOverlay::AtomicAdaptive,
)?;

// Run periodically on a background maintenance worker, never a request task.
if let Some(rebuild) =
    map.rebuild_adaptive_if_needed(AdaptiveRebuildPolicy::default())?
{
    eprintln!("published generation {}", rebuild.to_generation);
}
```

No key is rejected because of length. Keys beyond the learned atomic classes
use the exact native dynamic fallback; the bounded startup sample is also
native, with Papaya retained only as an emergency fallback.

## Native arbitrary-length checkpoint

The retained native fallback exposes a safe API and isolates its allocation
unsafe code in one small module. Bucket controls publish write-once entries
with acquire/release ordering, length updates are spread
across cache-line-separated shards, and the shard count scales down for small
tables. Separate bounded routing filters cover the native sample/emergency
population and the post-learning residual table. Concurrent same-key
publication, mixed 1–128-byte CRUD/rebuild, and short/long learning-handoff
races are covered by tests. Focused Miri runs also pass for
variable lengths, over-aligned cells, exact destruction, lookup/sampling, and
the concurrent equal-key race.

Victim requests are split between native startup, native residual, and
emergency Papaya populations with seed-derived stochastic rounding. This
matters when the 4,096-key startup sample is a tiny fraction of a large cache:
always rounding toward the residual population would otherwise make startup
keys effectively unsampleable in small victim windows.

The final preserved-binary 64-byte full-cache gate produced:

| Shape | Previous batch Mops/s | Native batch Mops/s | Adjacent Papaya exact-victim | Native normalized gain |
|---|---:|---:|---:|---:|
| 10K entries, 20K admissions, 1 writer | 3.093 | **4.415** | 8.824 | **43.9%** |
| 10K entries, 20K admissions, 8 writers | 2.179 | **2.782** | 13.419 | **27.8%** |
| 100K entries, 50K admissions, 1 writer | 2.100 | **3.196** | 8.531 | **45.3%** |
| 100K entries, 50K admissions, 8 writers | 1.953 | **2.676** | 16.193 | **52.1%** |

Higher throughput is better. The normalized gain compares each PackedGen row
to the Papaya control in the same binary, then compares those ratios. Papaya is
given the exact victim and therefore remains an upper bound rather than an
equivalent cache.

At 10K entries, 64-byte pressure memory improves from 441.276 to **417.380
B/entry** (-5.4%); at 100K it improves from 410.407 to **397.927** (-3.0%).
Maintained memory is effectively equal, and the 16-byte control is unchanged.
Those rows describe the first two-allocation native checkpoint. The retained
one-allocation entry improves it again:

| Shape | Two-allocation native | One-allocation native | Change |
|---|---:|---:|---:|
| 10K key64 seeded B/entry | 202.976 | **195.351** | **-3.8%** |
| 10K key64 pressure B/entry | 390.354 | **366.459** | **-6.1%** |
| 100K key64 seeded B/entry | 193.383 | **185.710** | **-4.0%** |
| 100K key64 pressure B/entry | 370.667 | **347.124** | **-6.4%** |
| 100K pressure allocation calls | 710,557 | **416,519** | **-41.4%** |

Maintained memory and the 16-byte control remain effectively equal. Against
the original Papaya-backed fallback, the latest native layout now wins seeded
and pressure memory at every tested 1K/10K/30K/100K point.

Alternating 100K/key64 operation probes versus the preceding native layout
improved 1T insert/read/update/95%-read mix by about 15%/27%/17%/18%. At 8T,
insert/read/mix improved about 3%/10%/29%; update was highly variable but its
six-run median improved about 2%. Higher Mops/s is better. Papaya remains ahead
on raw insert, read, miss, and the 95%-read mix, while the native table leads
Papaya on atomic update; the density and complete cache-policy work are the
reason to retain it.

A separate one-full-turnover full-cache gate normalized every run to its
adjacent exact-victim Papaya control. The new/preceding-native ratio improved
about 9.0%/8.2% at 10K 1T/8T and 1.3%/4.9% at 100K. This verifies that the
allocation win did not sacrifice cache admission or victim quality.

## Automatic maintenance recommendation

`adaptive_overlay_stats()` scans immutable overlay key metadata without taking
the rebuild mutex or adding any counter updates to foreground operations. It
reports physical slots, the sampled and subsequently observed six-class key
distributions, learned table capacities, short-key fallback spill, and ratios
in basis points.

`rebuild_adaptive_if_needed()` evaluates the snapshot, serializes competing
maintenance calls, rechecks the policy after acquiring the maintenance gate,
and publishes at most one generation. The default policy recommends rebuild
for any of:

- 75% physical slot utilization;
- at least 256 learned insertions and 15% total-variation distribution drift;
- at least 256 learned insertions and 5% short-key fallback spill.

Deletes intentionally do not lower physical utilization. A rebuild recovers
those slots, resets learning, and keeps the same configured churn budget. The
metadata scan is O(occupied overlay records), so FerricStore should call it on
a timer or dedicated maintenance process rather than after every operation.

The permanent `adaptive_maintenance_probe` measured the following unpinned
local medians. Lower is better:

| Occupied records | Metadata scan | Scan/record | Forced maintenance total | Writer redirect |
|---:|---:|---:|---:|---:|
| 100,000 | 0.216 ms | 2.160 ns | 17.474 ms | 8.5 us |
| 1,000,000 | 2.836 ms | 2.836 ns | 213.225 ms | 8.3 us |

At one million records, 188.140 ms of the total was frozen-base construction
and 9.773 ms was publication. Point operations continue during construction;
the redirect timing is the request-path-sensitive handoff measurement. Run the
probe with `cargo run --release --example adaptive_maintenance_probe`.

## Current measurements

The mixed fixture contains 40% 8-byte, 25% 16-byte, 15% 24-byte, 10% 32-byte,
and 10% 48-byte keys. Requested allocation at one million entries was:

| Implementation | Bytes/entry | Allocations |
|---|---:|---:|
| Adaptive atomic with exact atomic-48 | **33.074** | **8,229** |
| Atomic zero-through-32 | 56.653 | 200,042 |
| Boxed Papaya | 69.737 | 2,000,011 |
| DashMap | 71.238 | 1,000,066 |

Adaptive used 41.6% less RAM than the zero-through-32 table, 52.6% less than
Papaya, and 53.6% less than DashMap. On homogeneous 8-byte keys, the earlier
four-atomic-class layout used 19.515 B/entry versus the specialized mode's
19.230. Exact 48-byte keys now use the learned atomic class; other
residual widths continue to degrade to the dynamic fallback without reserving
unused atomic classes.

The Papaya row above is PackedGen's Papaya-overlay control and includes
generation metadata. A later direct-library probe measured Papaya at 61.693
B/entry, making Adaptive's direct RAM advantage 46.4%. The full direct
Papaya/SCC/DashMap/Flurry/HashBrown comparison, including complete turnover,
is in [ADAPTIVE_EXTERNAL_COMPARISON.md](ADAPTIVE_EXTERNAL_COMPARISON.md).

The mixed operation probe used 100,000 resident keys, hardware-accelerated
hashing, alternating implementation order, and 21 or 31 median samples.
Higher Mops/s is better:

| Operation | Threads | Adaptive | Atomic 0–32 | Papaya | DashMap |
|---|---:|---:|---:|---:|---:|
| Learned new-key insert | 1 / 8 | 12.884 / 37.490 | 10.999 / 35.649 | 10.128 / 27.106 | **14.848 / 42.593** |
| Overlay read hit | 1 / 8 | 15.557 / **88.572** | 15.092 / 81.916 | 12.861 / 69.418 | **24.800** / 70.825 |
| Overlay update hit | 1 / 8 | 13.862 / **44.491** | 13.085 / 42.512 | 12.142 / 42.687 | **33.979** / 43.249 |
| Read miss | 8 | **182.746** | 168.185 | 175.528 | 79.601 |
| Delete hit | 8 | 45.051 | 46.492 | **47.402** | 40.855 |
| 90% read cache mix | 1 / 8 | 12.965 / **25.183** | 13.843 / 24.211 | 13.420 / 24.787 | **22.225** / 18.272 |
| 95% read cache mix | 1 / 8 | 15.995 / **80.964** | 12.625 / 75.405 | 12.551 / 66.177 | **20.549** / 66.205 |

Cold fill includes sampling, proportional table allocation, fallback reserve,
and the publication handoff. In the same fixture it measured 12.095/19.935
Mops/s at one/eight threads versus DashMap at 22.278/37.151. This startup cost
must be included in cache warm-up planning even though learned steady-state
insertion is much closer.

The zero-foreground-counter maintenance design measured 27.404 Mops/s for
eight-thread learned insertion in a noisy 31-sample follow-up, versus 23.158
for atomic zero-through-32, 23.097 for Papaya, and 28.315 for DashMap. Absolute
throughput varied with host load, so the important result is that the rejected
global-counter prototype's severe contention disappeared; pinned confirmation
is still required.

These are unpinned Apple M4 Max development measurements. They are useful for
local direction, not release claims. Linux-pinned tail latency, Loom, Miri,
sanitizers, eviction integration, and automatic maintenance scheduling remain
required before production use.

## FerricStore recommendation

The mode is a promising hot-tier backend when the workload is read-heavy,
multi-core, and has mixed uncontrolled key lengths. Do not replace ETS or the
existing hot tier directly yet. First expose it through a NIF/resource adapter,
run shadow traffic with the real key-length and churn distribution, and invoke
`rebuild_adaptive_if_needed()` from a dedicated maintenance process. Export its
recommendation reasons, scan duration, rebuild duration, and generation count.
Retain a runtime switch back to the current implementation until pinned
latency and reclamation testing pass.
