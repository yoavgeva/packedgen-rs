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

## Same requested-RAM working set

The `ram_budget_probe` binary-searches the largest 32-byte-key map whose
requested live allocations fit a fixed budget, then times deterministic
successful lookups over each implementation's resulting working set. At a 64
MiB budget with one million queries on the development machine:

| Implementation | Entries | Live requested bytes | Budget used | Hit latency |
| --- | ---: | ---: | ---: | ---: |
| Packed Elastic, reserve 1/64, read optimized | 1,032,192 | 63,939,352 | 95.3% | 149–159 ns |
| HashBrown | 917,504 | 55,574,536 | 82.8% | 35–37 ns |

Packed Elastic holds 12.5% more records under this exact budget, below the 25%
density release gate, and the five-sample median successful lookup was
approximately 4.0–4.5x slower across two fresh processes. HashBrown's
next allocation-capacity step exceeds the budget, which explains its unused
space and demonstrates why a fixed-budget result can differ sharply from the
one-million-entry bytes-per-entry result. These are requested allocator bytes,
not RSS; page residency and cache-miss behavior still require a Linux-pinned
system run. The process-to-process timing spread is retained here rather than
selecting the more favorable sample.

Reproduce the raw smoke fixture:

```text
cargo run --release --example ram_budget_probe -- 64 1000000
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

A paired scalar-and-batch smoke run measured fixed batches of 32 at ~54.0
ns/key for packed ElasticHash versus ~27.9 ns/key for HashBrown, a 1.94x gap.
In that same run scalar packed lookup was ~66.2 ns/key, so ordering independent
route probes across the batch improved ElasticHash by 18.4% without increasing
resident map memory. This is an API-level throughput option, not a claim that
individual request latency improved, and it still misses the 1.5x gate.

The first explicit churn smoke run filled a 16K packed map, deleted 4,096
32-byte keys, and included the threshold-triggered table rebuild and arena
compaction. Packed ElasticHash took ~1.68 ms for the batch (~2.43 M deletes/s)
versus ~69 us for HashBrown (~59.2 M deletes/s). The operations are not
equivalent—HashBrown does not compact an external key arena—but the 24x pause
ratio demonstrates that compaction must become incremental or move off the
request path before the churn gate can pass.

With `MaintenanceMode::Deferred`, the same 4,096-delete request-path batch took
~98.2 us (~41.7 M deletes/s), 1.41x HashBrown and 16.7x faster than synchronous
ElasticHash maintenance. The owner must subsequently call `maintain()` to pay
the rebuild and reclaim dead bytes; this makes the work schedulable but does not
yet make maintenance incremental or concurrent.

An isolated maintenance benchmark removes request-path deletion from the timed
region and rebuilds the 12,288 survivors. Packed ElasticHash measured ~1.46 ms
versus ~118 us for rebuilding HashBrown from the same owned entries, a 12.4x
gap. `PackedMapStats` now reports completed maintenance runs, failed compaction
staging, and cumulative allocated arena bytes reclaimed so this cost and its
memory effect are observable in a service.

The first staged-maintenance API bounds key copying by entry count. Preparing
256 live 32-byte keys measured ~2.57 us (~99.7 M keys/s). This removes key-byte
copying from the final cutover in caller-selected slices, but the initial live
reference snapshot and final table rebuild are still whole-map operations; the
1.46 ms cutover evidence therefore remains the governing p99 failure.

Capturing the initial 12,288-reference maintenance snapshot measured ~35.0 us.
Structural mutations invalidate that snapshot; allocation-safe restart has the
same whole-snapshot cost, while value-only replacement leaves it valid. This is
small relative to cutover but remains an unbounded-by-budget phase.

Fallible construction at 100K read-optimized capacity measured ~29.8 us for
packed ElasticHash versus ~1.33 us for pre-sized HashBrown, a 22x gap. The
Elastic constructor eagerly initializes high-occupancy table geometry, filter,
and route storage; the typed failure path is service-usable, but initial memory
touch is materially more expensive.

The atomic public batch loader measured ~15.6 M 32-byte entries/s at 16K keys,
versus ~35.7 M/s for collecting owned boxed keys into HashBrown. ElasticHash is
2.28x slower and narrowly misses the 2x insertion gate. Failure drops the
partial map and reports the zero-based input index; duplicate keys use normal
replacement semantics.

The deferred-delete lookup sweep measured successful 32-byte hits at ~30.1 ns
with no deletes, ~33.6 ns at 12.5%, ~35.4 ns at the 25% soft maintenance signal,
and ~39.5 ns immediately below the forced 50% ceiling. The bounded worst case
is therefore 31% slower than the fresh map and ~4.9x HashBrown at the same
point. Missing lookup stayed roughly flat at ~10.1–10.8 ns because the stable
negative filter avoids tombstone probing. Deferred mode forcibly rebuilds at
50% deleted entries even if the owner ignores the soft signal. These defaults
are configurable with `with_maintenance_threshold_percents`; exact rounded-up
entry thresholds are computed once during construction, so policy selection
does not add percentage arithmetic to the request path. A stricter hard limit
trades more frequent rebuilds for a lower tombstone-latency ceiling.

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
- Batched lookup overlaps independent route probes and improves the large-index
  hit path, but further work must remove dependent reads from scalar and batch
  candidate dispatch. RAM density alone is insufficient.
