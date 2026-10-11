# Requested Live Allocation Density

16 sizes: 15 log-spaced points from 65,536 to 2,097,152 plus 250,000. Requested live bytes after construction, before drop, measured with stats_alloc over the system allocator; requested bytes, not RSS; excludes allocator metadata, fragmentation, RSS and process overhead. Hashbrown/DashMap/generation maps use identical 32-byte keys and u64 values (atomic uses NonMaxU64). FixedElasticMap uses mixed u64 keys, matching the existing density test, and reserve exponent 6. Generation-map overlay capacity is 0 (constructed base only). DashMap uses its default hasher/shard count. Lazy constructors warmed before measurement; versions pinned to the repository lockfile. One construction per backend/size; no timing conclusions.

## Table A: u64 keys (current density-test domain)

| Entries | HashMap<u64,u64> B/entry | FixedElasticMap B/entry | Fixed/hashbrown ratio |
| ---: | ---: | ---: | ---: |
| 65536 | 34.000 | 36.360 | 1.0694x |
| 83944 | 26.544 | 28.386 | 1.0694x |
| 107523 | 20.723 | 22.162 | 1.0694x |
| 137725 | 32.358 | 34.438 | 1.0643x |
| 176410 | 25.262 | 26.886 | 1.0643x |
| 225961 | 19.722 | 20.990 | 1.0643x |
| 250000 | 35.652 | 18.972 | 0.5321x |
| 289430 | 30.795 | 32.693 | 1.0616x |
| 370728 | 24.042 | 25.524 | 1.0616x |
| 474860 | 37.539 | 19.927 | 0.5308x |
| 608242 | 29.307 | 31.074 | 1.0603x |
| 779089 | 22.880 | 24.260 | 1.0603x |
| 997925 | 35.726 | 18.940 | 0.5301x |
| 1278228 | 27.891 | 29.553 | 1.0596x |
| 1637266 | 21.775 | 23.072 | 1.0596x |
| 2097152 | 34.000 | 36.013 | 1.0592x |

FixedElasticMap median ratio: 1.0610x; worst (maximum): 1.0694x.

## Table B: 32-byte keys (primary products)

| Entries | hashbrown B/entry (ratio) | AtomicPackedGenMap B/entry (ratio) | PackedGenMap<u64> B/entry (ratio) | DashMap B/entry (ratio) |
| ---: | ---: | ---: | ---: | ---: |
| 65536 | 82.000 (1.0000x) | 49.218 (0.6002x) | 49.218 (0.6002x) | 82.133 (1.0016x) |
| 83944 | 64.018 (1.0000x) | 49.051 (0.7662x) | 49.051 (0.7662x) | 64.122 (1.0016x) |
| 107523 | 49.980 (1.0000x) | 49.199 (0.9844x) | 49.199 (0.9844x) | 50.061 (1.0016x) |
| 137725 | 78.039 (1.0000x) | 49.150 (0.6298x) | 49.150 (0.6298x) | 78.102 (1.0008x) |
| 176410 | 60.926 (1.0000x) | 49.021 (0.8046x) | 49.021 (0.8046x) | 60.975 (1.0008x) |
| 225961 | 47.565 (1.0000x) | 48.822 (1.0264x) | 48.822 (1.0264x) | 55.779 (1.1727x) |
| 250000 | 85.983 (1.0000x) | 48.848 (0.5681x) | 48.848 (0.5681x) | 86.018 (1.0004x) |
| 289430 | 74.269 (1.0000x) | 48.737 (0.6562x) | 48.737 (0.6562x) | 74.300 (1.0004x) |
| 370728 | 57.983 (1.0000x) | 48.711 (0.8401x) | 48.711 (0.8401x) | 58.006 (1.0004x) |
| 474860 | 90.535 (1.0000x) | 48.520 (0.5359x) | 48.520 (0.5359x) | 90.554 (1.0002x) |
| 608242 | 70.682 (1.0000x) | 48.484 (0.6860x) | 48.484 (0.6860x) | 70.696 (1.0002x) |
| 779089 | 55.182 (1.0000x) | 48.509 (0.8791x) | 48.509 (0.8791x) | 55.193 (1.0002x) |
| 997925 | 86.162 (1.0000x) | 48.489 (0.5628x) | 48.489 (0.5628x) | 86.171 (1.0001x) |
| 1278228 | 67.268 (1.0000x) | 48.480 (0.7207x) | 48.480 (0.7207x) | 67.274 (1.0001x) |
| 1637266 | 52.516 (1.0000x) | 48.444 (0.9225x) | 48.444 (0.9225x) | 52.522 (1.0001x) |
| 2097152 | 82.000 (1.0000x) | 48.411 (0.5904x) | 48.411 (0.5904x) | 82.004 (1.0001x) |

| Backend | Median ratio to hashbrown | Worst (maximum) ratio |
| --- | ---: | ---: |
| hashbrown | 1.0000x | 1.0000x |
| AtomicPackedGenMap | 0.7033x | 1.0264x |
| PackedGenMap<u64> | 0.7033x | 1.0264x |
| DashMap | 1.0004x | 1.1727x |

The memory-density test now asserts a nine-size sweep (eight log-spaced sizes from 65,536 to 1,048,576 plus 250,000), bounding AtomicPackedGenMap bytes per entry and median/worst hashbrown ratios for both primary and research maps instead of asserting a general saving at one post-resize size.
