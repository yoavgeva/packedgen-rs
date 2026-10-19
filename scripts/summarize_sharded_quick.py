"""Summarizes scripts/compare_sharded_quick.sh output as a Markdown table.

Each cell shows the median of ShardedCache's throughput over quick_cache's
across pairs, with the range; positive means ShardedCache is faster.
"""

import collections
import statistics
import sys

runs = collections.defaultdict(dict)
for line in open(sys.argv[1]):
    entries, *row = line.strip().split(",")
    if len(row) < 10 or row[0] not in ("sharded", "quickcache"):
        continue
    backend, workload, workers, seed = row[0], row[1], row[5], row[7]
    runs[(workload, entries, workers, seed)][backend] = float(row[9])

cells = collections.defaultdict(list)
quick = collections.defaultdict(list)
for (workload, entries, workers, _), pair in runs.items():
    if len(pair) == 2:
        cells[(workload, entries, workers)].append(pair["sharded"] / pair["quickcache"] - 1)
        quick[(workload, entries, workers)].append(pair["quickcache"])

print("| Workload | Entries | Threads | ShardedCache vs quick_cache | Range | quick_cache Mops/s |")
print("| --- | ---: | ---: | ---: | --- | ---: |")
for key in sorted(cells, key=lambda k: (int(k[1]), int(k[2]), k[0])):
    ratios = cells[key]
    print(
        f"| {key[0]} | {key[1]} | {key[2]} | {100 * statistics.median(ratios):+.0f}% "
        f"| {100 * min(ratios):+.0f} to {100 * max(ratios):+.0f}% "
        f"| {statistics.median(quick[key]):.1f} |"
    )
