# Performance gates

ElasticHash is not considered usable merely because it reaches a high load
factor. A release must demonstrate a better storage-engine tradeoff than
HashBrown/SwissTable on the same machine, allocator, key corpus, values, and
logical capacity.

## Required v0.x gates

For large binary-keyed, fixed-epoch indexes:

1. **Density:** at least 25% more live entries within the same requested-byte
   budget on the median of a capacity sweep, including keys, values, control
   metadata, filters, and arenas.
2. **No memory cliff regression:** no tested capacity may use more than 10%
   additional requested bytes versus HashBrown. Capacity geometry must adapt
   when SwissTable happens to sit at an efficient power-of-two boundary.
3. **Successful lookup:** no more than 1.5x HashBrown latency for the target
   binary-key workload. The stretch goal is parity.
4. **Missing lookup:** no more than 2x HashBrown latency with the stable
   definite-negative filter enabled.
5. **Insertion:** no more than 2x HashBrown time in a pre-sized fixed epoch.
6. **Churn:** bounded p99 latency and memory after delete/reinsert workloads;
   rebuild work must be observable and incrementally schedulable.
7. **System win:** a RAM-limited storage-engine benchmark must complete more
   operations per second by retaining a larger working set and avoiding cold
   reads.

The accelerated packed layout passes the one-million-entry density target and
the tested no-cliff sweep after adaptive cache budgeting. It is close to the
missing-lookup and insertion limits, but still fails the successful-hit and
median-sweep density gates. Fixed 32-key batches narrow the measured large-index
hit gap from 2.56x to 1.94x HashBrown, still outside the 1.5x limit.
Synchronous delete-threshold rebuild and arena compaction also fails the churn
gate: the first 16K-entry smoke fixture pauses for ~1.68 ms at the threshold.
Deferred mode reduces that request-path delete batch to ~98.2 us, 1.41x
HashBrown, but the separately scheduled maintenance pause still fails the p99
gate until rebuilding becomes incremental or concurrent.
Measurements that fail a gate remain in the repository; they are optimization
inputs, not marketing exclusions.

## Correctness gates

- Differential agreement with `HashMap` across mixed operations.
- No false negatives from membership filters.
- Exact behavior at configured capacity, including replacement at capacity.
- Fallible allocation for service-controlled growth.
- Miri and sanitizer-clean unsafe code once the owned core is introduced.
- Loom models for concurrent publication and generation reclamation before
  lock-free readers are exposed.
