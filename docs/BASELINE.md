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
| 32-byte packed key -> `u64`, before routing cache | Packed Elastic, reserve 1/64 | 52.408 | **37.9% less** |
| 32-byte packed key -> `u64`, accelerated | Packed Elastic, reserve 1/64 | 62.533 | **25.9% less** |

The one-byte-per-entry service-layer negative filter is included in the elastic
numbers. The packed shape stores an eight-byte reference in each table slot and
uses a segmented arena. The accelerated shape also includes a bounded two-way
direct-location cache and one overflow bit per bucket.

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

The first packed-key run used 20 Criterion samples with one-second warmup and a
two-second measurement window:

| Packed binary workload | Elastic 1/8 | Elastic 1/64 | HashBrown |
| --- | ---: | ---: | ---: |
| Successful 32-byte lookup | ~55.2 ns | ~69.8 ns | ~7.0 ns |
| 32-byte insertion throughput | ~20.3 M/s | ~18.7 M/s | ~33.1 M/s |

After adding the verified-location accelerator:

| Accelerated binary workload | Elastic 1/8 | Elastic 1/64 | HashBrown |
| --- | ---: | ---: | ---: |
| Successful 32-byte lookup | ~24.9 ns | ~27.8 ns | ~6.8 ns |
| Missing 32-byte lookup | ~10.5 ns | ~11.5 ns | ~4.8 ns |
| 32-byte insertion throughput | ~18.9 M/s | ~16.4 M/s | ~37.0 M/s |

At one million keys, where both indexes exceed the small in-cache fixture, the
read-optimized `1/64` packed map measured ~63.7 ns per successful lookup versus
HashBrown's ~25.8 ns. The gap narrows from roughly 4x at 32K keys to 2.47x, but
still misses the 1.5x release gate.

The cache is advisory: every direct location is checked against table bounds,
control fingerprint, and original key bytes. Stale entries and tag collisions
fall back to the exact elastic schedule. A bucket can reject an absent tag only
when its overflow bit proves every assigned live route was cached.

The first packed sweep exposed a 100,000-entry cliff. Adaptive cache budgeting
reduced that point from 76.898 to 70.570 B/entry versus HashBrown's 64.768
(+9.0%), bringing every tested capacity inside the 10% no-cliff ceiling. At
250,000 and 1,000,000 entries the layout saves about 24.6% and 25.9%; around
HashBrown's efficient thresholds it remains roughly 5–6% larger.

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
- Packed binary-key storage plus verified direct routing preserves a density
  advantage at favorable capacities and cuts successful lookup by about 60%,
  but the hit, miss, insertion, and median-density gates still need work.
- The next optimization target is the exact-query routing path: probe schedule,
  candidate dispatch, and batched lookup. RAM density alone is insufficient.
