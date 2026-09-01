# Performance gates

PackedGen is not considered usable merely because it reaches a high load
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
Under the same 64 MiB requested-allocation budget, the packed map holds 12.5%
more 32-byte-key records, below the 25% density gate, while its scalar hit loop
has a five-sample median approximately 4.0–4.5x slower across two processes.
This fixed-budget harness captures capacity-allocation cliffs but does not yet
measure RSS or page residency.
Synchronous delete-threshold rebuild and arena compaction also fails the churn
gate: the first 16K-entry smoke fixture pauses for ~1.68 ms at the threshold.
Deferred mode reduces that request-path delete batch to ~98.2 us, 1.41x
HashBrown, but the separately scheduled maintenance pause still fails the p99
gate until rebuilding becomes incremental or concurrent.
Deferred tombstones are bounded by a forced 50% rebuild by default. Immediately
below that ceiling, measured successful-hit latency is 31% above a fresh packed
map; missing lookup remains flat. Operators can configure stricter validated
soft/hard percentages when that tradeoff fits their workload. Exact entry
thresholds are precomputed at construction. The bound prevents unlimited
degradation but does not itself pass the successful-hit gate.
Measurements that fail a gate remain in the repository; they are optimization
inputs, not marketing exclusions.

The immutable `FrozenPackedMap` is evaluated separately from the fully dynamic
gate. Its first million-key run used 42.0% fewer requested bytes than HashBrown
and measured successful lookup about 1.87x faster with the opt-in hardware-AES
hasher. Under 64 MiB it retained 50% more records, with about 1.19x slower hits
over that larger working set. Construction was about 3.7x slower than building
HashBrown at 16K entries. These results justify continued frozen-backend work
but do not satisfy dynamic insert/delete or portable-performance requirements.

## Correctness gates

- Differential agreement with `HashMap` across mixed operations.
- Full-cache differential agreement across insert, replace, insert-if-absent,
  read, miss, delete, touch, immediate expiry, maintenance, entry count, and
  caller-accounted weight.
- Concurrent mixed-width writes during repeated adaptive maintenance must
  preserve exact final values and accounting.
- No false negatives from membership filters.
- Exact behavior at configured capacity, including replacement at capacity.
- Fallible allocation for service-controlled growth.
- Miri and sanitizer-clean unsafe code once the owned core is introduced.
- Loom models for concurrent publication and generation reclamation before
  lock-free readers are exposed.

The executable cache proof protocol and current status are documented in
[`CACHE_PROOF.md`](CACHE_PROOF.md).
