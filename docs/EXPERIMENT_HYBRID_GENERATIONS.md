# Hybrid frozen-base experiment

`HybridPackedMap` tests a read-mostly generational layout:

- a dense immutable `FrozenPackedMap` base;
- a small mutable `PackedSwissMap` delta for new and replaced keys; and
- one tombstone bit per frozen slot, so deleting a base key retains no second
  copy of that key.

Lookup order is delta, then exact frozen lookup, then the tombstone bit. This is
important for correctness: an overlay wins over its older base value, and
removing that overlay sets the base tombstone so the old value cannot reappear.

## Correctness

`tests/hybrid_map.rs` covers binary and empty keys, base/delta precedence,
delete-reinsert-delete behavior, and 50,000 randomized operations checked after
every step against `std::collections::HashMap`.

Returning an owned value from a base replacement or removal requires `V: Clone`
because values in the frozen generation cannot be moved individually. Read-only
operations do not require `Clone`.

## Measurement workload

Measured on an Apple M4 Max with:

```text
cargo build --release --features gxhash --example hybrid_probe
target/release/examples/hybrid_probe 1000000 1 2000000
```

The corpus has 1,000,000 unique 32-byte keys and `u64` values. One-percent
churn means 5,000 base keys receive overlays and 5,000 different base keys are
deleted. The delta reserves the full 10,000-write merge budget. Retained bytes
are requested allocator bytes sampled after those changes.

Read latency is the median of five warmed samples of 2,000,000 randomized
queries. Base-hit queries exclude the changed range, overlay-hit queries target
the 5,000-entry delta, and misses use a disjoint 1,000,000-key corpus. Update and
insert numbers are bulk averages rather than medians. The timed 5,000 absent
inserts run after the RAM sample and fit the pre-reserved delta budget.

| Implementation | B/entry | Base hit ns | Overlay hit ns | Miss ns | Update ns | Insert ns |
|---|---:|---:|---:|---:|---:|---:|
| Frozen PtrHash | 48.557 | 111.11 | — | 70.88 | — | — |
| Hybrid, no filter | 49.033 | 85.28 | 10.85 | 84.18 | 71.18 | 101.48 |
| Hybrid, 4 filter bits/entry | 49.538 | 108.50 | 10.91 | 54.65 | 91.34 | 66.05 |
| Hybrid, 8 filter bits/entry | 50.043 | 109.59 | 11.04 | 40.73 | 82.42 | 56.97 |
| Packed SwissTable | 67.652 | 74.79 | 13.72 | 24.12 | 31.70 | 24.68 |

Absolute latency is sensitive to CPU state and allocation placement, so the
probe is intended for architectural comparison rather than a release gate.
Repeated runs showed the same qualitative result: the delta's hot hits are
cheap, while base hits and especially misses pay for both generations.

## Retained-memory breakdown

At the RAM sample, the hybrid held:

| Component | Retained state |
|---|---:|
| Frozen packed-key arena | 32,059,392 bytes |
| Frozen dense slots | 16,000,000 bytes |
| PtrHash metadata | 2.990 bits/base entry |
| Delta | 5,000 entries / 14,336 table capacity |
| Delta packed-key arena | 196,704 bytes |
| Tombstone allocation | 125,000 bytes |
| Set tombstones | 5,000 |
| Optional negative filter | 0, 505,000, or 1,010,000 bytes |

The tombstone allocation is exactly one bit per base entry. At this churn level,
the unfiltered hybrid used 27.5% fewer requested bytes than `PackedSwissMap`;
even the eight-bit filter policy used 26.0% less.

## Conclusion

The layout is worthwhile for read-mostly generations with a bounded delta. It
keeps nearly frozen-map density and gives excellent locality for recently
changed hot keys. It is not a general SwissTable replacement yet: unchanged
base hits pay an extra delta probe, and unfiltered misses probe both generations.

The stable filters expose the trade explicitly. Four bits per planned entry
cut miss and absent-insert latency substantially for another 0.505 B/entry;
eight bits improve them further for 1.010 B/entry. Both slow unchanged base hits
because those keys must hash and touch the filter before entering PtrHash. The
eight-bit policy is the default, while `HybridFilterMode` lets read-mostly
owners choose the unfiltered density point or the four-bit compromise.

A production version still needs a generation-merge operation, a threshold
policy, and concurrent publication. The optional membership filters improve
misses and new inserts but do not yet match PackedSwiss on those operations.
