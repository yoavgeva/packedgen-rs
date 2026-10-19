"""Summarizes compare_sharded_quick.sh or compare_sharded_builds.sh output
as a Markdown table.

usage: summarize_compare.py CSV [NEW OLD]

Each cell shows the median across pairs of NEW's throughput over OLD's, with
the range; positive means NEW is faster. NEW and OLD default to `sharded`
and `quickcache`; for compare_sharded_builds.sh pass `head base`.
"""

import collections
import statistics
import sys

new, old = sys.argv[2:4] if len(sys.argv) > 3 else ("sharded", "quickcache")
runs = collections.defaultdict(dict)
for line in open(sys.argv[1]):
    entries, *row = line.strip().split(",")
    if len(row) < 10 or row[0] not in (new, old):
        continue
    backend, workload, workers, seed = row[0], row[1], row[5], row[7]
    runs[(workload, entries, workers, seed)][backend] = float(row[9])

cells = collections.defaultdict(list)
quick = collections.defaultdict(list)
for (workload, entries, workers, _), pair in runs.items():
    if len(pair) == 2:
        cells[(workload, entries, workers)].append(pair[new] / pair[old] - 1)
        quick[(workload, entries, workers)].append(pair[old])

print(f"| Workload | Entries | Threads | {new} vs {old} | Range | {old} Mops/s |")
print("| --- | ---: | ---: | ---: | --- | ---: |")
for key in sorted(cells, key=lambda k: (int(k[1]), int(k[2]), k[0])):
    ratios = cells[key]
    print(
        f"| {key[0]} | {key[1]} | {key[2]} | {100 * statistics.median(ratios):+.0f}% "
        f"| {100 * min(ratios):+.0f} to {100 * max(ratios):+.0f}% "
        f"| {statistics.median(quick[key]):.1f} |"
    )
