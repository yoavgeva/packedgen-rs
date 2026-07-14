# Initial local evidence

Date: 2026-07-14

These are development-machine measurements, not universal performance claims.
They exist to make the project's tradeoff explicit and reproducible. HashBrown
0.17.1 is the comparison because it is the current Rust SwissTable
implementation.

## Requested live allocation

One million entries, optimized build. Measurements use an instrumented system
allocator and report requested bytes still live after the map is filled. They
exclude allocator bookkeeping, fragmentation, executable pages, and process
overhead, so they are not RSS measurements.

| Key/value shape | Implementation | Bytes/entry | Difference vs HashBrown |
| --- | --- | ---: | ---: |
| `u64 -> u64` | Elastic, reserve 1/8 | 38.776 | 8.8% more |
| `u64 -> u64` | Elastic, reserve 1/64 | 19.900 | **44.2% less** |
| `u64 -> u64` | HashBrown | 35.652 | baseline |
| 32-byte boxed key -> `u64` | Elastic, reserve 1/8 | 87.553 | 3.7% more |
| 32-byte boxed key -> `u64` | Elastic, reserve 1/64 | 60.289 | **28.6% less** |
| 32-byte boxed key -> `u64` | HashBrown | 84.429 | baseline |

The one-byte-per-entry service-layer negative filter is included in the elastic
numbers. The binary-key shape still performs one allocation per key; a packed
key arena is a future optimization.

Reproduce:

```text
cargo run --release --example memory_probe -- all 1000000
```

## Point-operation latency

Criterion smoke runs used deterministic mixed keys, optimized code, 10 samples,
a one-second measurement window, and the same process/machine. Short runs are
noisy; the scale of the differences is nevertheless clear.

| Workload | Elastic 1/8 | Elastic 1/64 | HashBrown |
| --- | ---: | ---: | ---: |
| Successful `u64` lookup | ~55 ns | ~75 ns | ~3.8 ns |
| Missing `u64` lookup, before negative filter | ~538 ns | ~584 ns | ~3.0 ns |
| Missing `u64` lookup, with negative filter | ~21 ns | ~35 ns | ~2.9 ns |
| Successful 32-byte binary lookup | ~56 ns | ~68 ns | ~8.2 ns |
| Bulk insertion throughput | ~23.6 M/s | ~21.0 M/s | ~329 M/s |

The stable negative filter reduced missing-lookup latency by roughly 94–96%
without allowing false negatives. It sets bits on insertion, retains them on
ordinary deletion, and rebuilds when the core starts a new allocation epoch.

Reproduce:

```text
cargo bench --bench point_ops
cargo bench --bench mixed_workloads
```

## Interpretation

- The high-occupancy configuration has a real, measured memory-density
  advantage over SwissTable for both tested layouts.
- The conservative 1/8 configuration has no memory advantage here.
- HashBrown remains dramatically faster for in-cache point operations and
  insertion.
- ElasticHash's plausible storage-engine win is fitting a larger working set in
  RAM and avoiding cold I/O. It is not currently a general-purpose HashBrown
  throughput replacement.
- The next implementation targets are packed binary-key storage and generation
  rebuilding. Both must preserve the measured density advantage.
