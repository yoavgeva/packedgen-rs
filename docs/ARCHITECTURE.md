# PackedGen architecture and identity

Date: 2026-07-16

PackedGen is a packed generational concurrent-map library. Its primary
placement algorithm is not the Elastic Hashing algorithm from *Optimal Bounds
for Open Addressing Without Reordering*.

## Primary implementation

`PackedGenMap<V>` and `AtomicPackedGenMap` are the primary public aliases.
Their data path combines:

1. an immutable packed-key generation indexed by PtrHash;
2. exact original-key verification after perfect-hash routing;
3. a lock-free mutable overlay for new or shadowed keys;
4. ArcSwap generation publication;
5. 4,096 writer-routing stripes for exact rebuild handoff;
6. direct atomic frozen value slots for `AtomicPackedGenMap`;
7. optional prepared hot-key handles and allocation-free batches.

The `prepared-batch-gate` feature adds 16 cache-line-separated batch writer
gates. It is intended for write-heavy request batches and costs 1,040 measured
requested bytes per live generation.

## Retained comparison and research backends

- `FrozenPackedMap`: immutable exact PtrHash map and the frozen-generation
  foundation.
- `ConcurrentSwissMap`, `SegmentedSwissMap`, and `PackedSwissMap`: practical
  SwissTable-derived controls.
- `Fixed32SoaMap`: fixed-width mutable RAM specialization.
- `LockFreeBinaryMap` and `LockFreeHybridMap`: dynamic and hybrid controls.
- `FixedElasticMap` and `PackedBinaryMap`: paper-derived Elastic Hashing
  experiments backed by the owned `opthash` workspace crate.
- PHast+ and k-PHF modules: optional frozen-index experiments.

The paper-derived types retain their Elastic names because those specific
backends implement the paper. They are not the basis for PackedGen's strongest
concurrent results.

## SIMD policy

PackedGen uses SIMD or word-parallel operations only where measurements justify
them:

- retained: `wide::u8x16` tag matching in cache-line buckets;
- retained experimentally: `wide::u64x4` packed-tag checks in the k-PHF module;
- retained: one-word SWAR scans for the atomic overlay's eight control bytes;
- retained: hardware-accelerated Gx hashing when `gxhash` is enabled;
- rejected on Apple M4 Max: explicit `wide::u64x4` 32-byte key equality, which
  was 6.5% to 14.8% slower than native equality;
- rejected: manual four-word equality after an isolated microbenchmark win
  changed real prepared reads from 12.150 to 12.190 ns.

Architecture-specific SIMD should be reconsidered using the standalone
`simd_key_probe` on AVX2 and AVX-512 machines. It should not replace native
equality without an end-to-end PackedGen workload win.

## Claim boundary

PackedGen can claim measured RAM density and workload-specific throughput
results. It cannot claim to be a universal SwissTable replacement or a
concurrent implementation of the Elastic Hashing paper.
