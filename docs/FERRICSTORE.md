# FerricStore integration target

FerricStore is a strong eventual consumer because its primary keydir is already
binary-keyed, sharded, point-read-heavy, and recoverable from durable records.
The first integration should replace only the per-shard key-to-location index.

## Proposed packed entry

The exact layout must be benchmarked, but the initial target is:

```text
key arena: raw immutable key bytes owned by the table generation

slot:
  key_offset       u32/u64
  key_length       u32
  expire_at_ms     u64
  lfu              u32
  location_kind    u8
  location_id      u64
  offset           u64
  value_size       u32
  hot_value_handle optional
```

This avoids arbitrary BEAM terms and the general ETS tuple representation. The
RAM gain from packing may exceed the additional gain from reducing empty slots;
benchmarks must report the two separately.

## Integration order

1. Shadow mode: publish every keydir mutation to ETS and ElasticHash, but serve
   from ETS. Continuously compare sampled reads.
2. Cold-metadata reads: serve key-to-disk locations from ElasticHash while hot
   values remain in ETS.
3. Native hot values: store immutable value buffers in Rust and return them as
   resource binaries, using the zero-copy pattern FerricStore already employs
   for native read buffers.
4. Remove the ETS keydir only after parity, restart recovery, scans, LFU, TTL,
   compaction, and memory-pressure behavior all pass production-shaped tests.

## Expected outcome

- **Capacity:** likely meaningful for metadata-heavy or cold-key-heavy data.
  The exact result depends on key length and hot value size.
- **Throughput:** not guaranteed. Elastic hashing is attractive near full
  occupancy, but its ranked probes can create more cache misses than SwissTable
  or ETS. Normal-load point reads may be slower.
- **System performance:** fitting a working set in RAM that otherwise spills to
  disk can dominate a modest per-lookup CPU regression. This is the most
  plausible FerricStore win.

