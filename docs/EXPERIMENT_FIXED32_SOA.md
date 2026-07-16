# Fixed-32-byte structure-of-arrays experiment

## Question

Can a mutable map specialized for 32-byte keys save RAM without losing to the
existing packed-key Swiss maps on successful lookup or construction?

## Prototype

`Fixed32SoaMap<V>` keeps keys and values in separate dense vectors. Its Swiss
directory stores only a `u32` dense-vector index. The directory is split into
independently sized hash ranges so a large map does not round the entire
allocation to the next power of two.

The important cost model for 32-byte keys and `u64` values is:

- 32 bytes/entry for dense keys;
- 8 bytes/entry for dense values;
- roughly 5--6 bytes/entry for the Swiss index and control bytes.

The experiment supports insert, replace, get, get-mut, contains, remove, and
clear. Removal swap-removes a dense record and rewrites the moved record's
four-byte directory slot; randomized differential tests cover that repair.

## Method

Command:

```text
cargo run --quiet --release --example fixed32_experiment -- 1000000
```

The probe builds one million deterministic 32-byte keys with `u64` values.
Retained requested bytes are recorded through `stats_alloc`. Successful and
missing lookups use precomputed pseudo-random query sequences for eight passes.
Removal deletes every second key and includes dense-index repair. The numbers
below are medians of three warmed release runs on an Apple M4 Max with Rust
1.94.0. All maps use their default `hashbrown::DefaultHashBuilder`.

| implementation | bytes/entry | build ns/entry | hit ns | miss ns | remove ns |
|---|---:|---:|---:|---:|---:|
| Fixed32SoA compact | 45.89 | 23.23 | 59.54 | 46.62 | 43.62 |
| Fixed32SoA balanced | 46.35 | 19.47 | 68.97 | 36.65 | 41.65 |
| SegmentedSwiss compact | 52.03 | 25.38 | 71.73 | 54.60 | 37.79 |
| SegmentedSwiss balanced | 53.59 | 25.09 | 72.37 | 48.11 | 37.39 |
| PackedSwiss | 67.65 | 20.08 | 74.47 | 19.57 | 31.62 |

## Result

The balanced structure-of-arrays prototype retained 13.5% fewer bytes than
balanced SegmentedSwiss and 31.5% fewer than PackedSwiss. In this expanded run
its successful hits were about 5% faster than balanced SegmentedSwiss and about
7% faster than PackedSwiss, while its median build time matched PackedSwiss.
Compact mode was the fastest SoA hit policy in this sample.

The result is not a clean sweep. PackedSwiss misses were roughly twice as fast
as balanced SoA, and PackedSwiss removal was about 24% faster. The dense repair
is correct and bounded, but it adds a second directory lookup whenever the last
record moves.

Compact mode saved only another 0.46 bytes/entry and made median builds about
19% slower than balanced mode. It traded faster hits for slower misses, so
balanced remains the safer general default.

These are promising specialization results, not a general HashMap victory. The
comparison fixes key width at 32 bytes and uses a single machine and key
distribution. Process-to-process hit timing varied materially, so the ratios
are directional smoke evidence. The next useful checks are Criterion
mixed-operation runs, different value widths, and adversarial segment skew.
