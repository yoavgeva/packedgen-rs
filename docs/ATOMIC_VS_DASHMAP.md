# Atomic generation versus DashMap

Date: 2026-07-15

This is the direct external comparison for `LockFreeAtomicU64GenerationMap`.
It uses 100,000 preloaded 32-byte keys, 300,000 requested operations, 64
DashMap shards, 15 alternating samples, identical operation traces, a cloned
FoldHash state, and identical untimed hit/miss warmup. Results are unpinned
Apple M4 Max development measurements, so small differences need pinned
confirmation.

## How to read the tables

- Throughput is Mops/s: **higher is better**.
- Memory is bytes per live entry: **lower is better**.
- Delta is `(PackedGen / DashMap) - 1`: positive means PackedGen is faster;
  negative means DashMap is faster.
- The main tables use the ordinary exact-only PackedGen policy, not a
  per-row cherry-picked policy.

## Retained memory

The one-million-key requested-allocation probe reports:

| State | PackedGen B/entry | DashMap B/entry | PackedGen difference |
|---|---:|---:|---:|
| Frozen/default generation | **48.789** | 84.438 | **42.2% less** |
| Frozen + embedded fingerprint | **48.789** | 84.438 | **42.2% less** |
| Frozen + one-byte filter | **49.789** | 84.438 | **41.0% less** |
| All keys in inline-32 mutable overlay | **58.959** | 84.438 | **30.2% less** |

The default frozen generation therefore fits about **73% more entries** than
DashMap in the same index-memory budget. Even the fully mutable inline overlay
fits about **43% more entries**.

Allocation shape also differs:

| Case | PackedGen | DashMap |
|---|---:|---:|
| One-million-key frozen/base allocations | **11,401** | 1,000,066 |
| One-million new mutable keys | 1,000,015 | 1,000,066 |
| Warm existing-key update allocations | **0** | **0** |

The packed frozen arena avoids one allocation per member key. New mutable keys
still need approximately one allocation in both maps, but PackedGen retains
fewer bytes because its 32-byte key is inline with the overlay record.

## One-thread throughput

| Workload | PackedGen | DashMap | Delta | Winner |
|---|---:|---:|---:|---|
| Read hit | 20.290 | **29.354** | -30.9% | DashMap |
| Read miss | 32.487 | **59.659** | -45.6% | DashMap |
| Read, 95% hits | 28.005 | **45.492** | -38.4% | DashMap |
| Read, 99% hits | 25.229 | **40.655** | -37.9% | DashMap |
| Insert/replace hit | **18.702** | 16.999 | +10.0% | PackedGen |
| Insert new key | 10.626 | **25.230** | -57.9% | DashMap |
| Update hit | 20.635 | **44.850** | -54.0% | DashMap |
| Update miss | 17.259 | **56.612** | -69.5% | DashMap |
| Update one hot key | 51.435 | **157.463** | -67.3% | DashMap |
| Delete hit | **29.821** | 28.707 | +3.9% | PackedGen |
| Delete miss | 15.782 | **57.713** | -72.7% | DashMap |
| Cache mix: 90% read hit | 21.645 | **34.939** | -38.0% | DashMap |
| Cache mix: 95% read hit | 23.460 | **36.528** | -35.8% | DashMap |

**Conclusion:** DashMap is the better low-latency single-thread map. The atomic
generation is not currently a single-core replacement for DashMap.

## Eight-thread throughput

| Workload | PackedGen | DashMap | Delta | Winner |
|---|---:|---:|---:|---|
| Read hit | **202.071** | 96.329 | +109.8% | PackedGen |
| Read miss | **220.723** | 97.313 | +126.8% | PackedGen |
| Read, 95% hits | **174.732** | 97.814 | +78.6% | PackedGen |
| Read, 99% hits | **220.913** | 102.077 | +116.4% | PackedGen |
| Insert/replace hit | **64.781** | 47.559 | +36.2% | PackedGen |
| Insert new key | 30.868 | **51.320** | -39.9% | DashMap |
| Update hit | **64.203** | 55.529 | +15.6% | PackedGen |
| Update miss | 78.167 | **83.097** | -5.9% | DashMap |
| Update one hot key | 4.134 | **58.872** | -93.0% | DashMap |
| Delete hit | 43.628 | **50.263** | -13.2% | DashMap |
| Delete miss | 79.504 | **89.723** | -11.4% | DashMap |
| Cache mix: 90% read hit | **134.627** | 87.813 | +53.3% | PackedGen |
| Cache mix: 95% read hit | **169.376** | 87.475 | +93.6% | PackedGen |

**Conclusion:** with distributed keys, PackedGen scales much better and wins
the read-heavy cache workloads while using substantially less RAM. DashMap
still wins new-key insertion, successful/missing delete, and especially a
single contended hot key.

## Miss-heavy policies

These are explicit workload choices rather than the ordinary default:

| Eight-thread workload | Best PackedGen policy | PackedGen | DashMap | Delta |
|---|---|---:|---:|---:|
| Read miss | One-byte filter | **403.226** | 97.313 | +314.4% |
| Insert new key | One-byte filter | 33.857 | **51.320** | -34.0% |
| Update miss | Embedded fingerprint | **96.621** | 83.097 | +16.3% |
| Delete miss | Embedded fingerprint | **89.998** | 89.723 | +0.3% |
| 90% read-hit cache mix | Embedded fingerprint | **156.074** | 87.813 | +77.7% |

The embedded fingerprint costs zero extra retained bytes. The one-byte filter
adds exactly one byte per frozen entry; it is justified only when read misses
are unusually frequent. Even the best PackedGen policy does not beat DashMap
for new-key insertion in this test.

## Delete and reclamation lifecycle

After deleting half of one million keys:

| State | Live entries | Live bytes | B/live entry |
|---|---:|---:|---:|
| DashMap after delete | 500,000 | 68,437,504 | 136.875 |
| PackedGen before rebuild | 500,000 | **48,788,781** | **97.578** |
| PackedGen after rebuild | 500,000 | **24,457,438** | **48.915** |

DashMap releases each removed boxed key immediately but retains table capacity.
PackedGen marks frozen values deleted and does not reclaim their packed keys
until generation rebuild. Rebuild restores dense memory, but temporarily needs
old and new generation storage and is whole-map maintenance. Point operations
continue during the build, although rebuild is not starvation-free.

## Practical selection

- Choose default PackedGen for a large, multi-core, read-heavy hot tier where
  RAM density and distributed-key throughput dominate.
- Choose embedded-fingerprint PackedGen when misses are material and the
  extra hit-path check is acceptable.
- Choose DashMap for single-thread latency, frequent new-key insertion, or a
  highly contended hot key.
- Do not claim a universal PackedGen win: its advantage depends on sufficient
  concurrency and a mostly stable key population.

Reproduce:

```text
cargo run --release --features gxhash --example atomic_vs_dashmap_probe -- \
  100000 300000 8 15 64
cargo run --release --features gxhash --example memory_probe -- \
  dashmap-binary 1000000
cargo run --release --features gxhash --example memory_probe -- \
  lockfree-atomic-generation-binary 1000000
```
