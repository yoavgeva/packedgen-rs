"""Summarizes scripts/mokabench.sh output as Markdown tables.

usage: summarize_mokabench.py CSV

The first table shows, per trace, capacity, and client count, each cache's
median throughput in million reads per second, with its hit ratio, and
ShardedCache's throughput over quick_cache's and moka's. The second shows
the geometric mean of those ratios per client count, and the last each
cache's mean hit ratio over the one-client cells, whose replay order is
fixed.
"""

import collections
import csv
import statistics
import sys

NAMES = {
    "PackedGen ShardedCache": "ShardedCache",
    "QuickCache Sync Cache": "quick_cache",
    "Moka Sync Cache": "moka",
    "Moka Async Cache": "moka async",
    "Moka SegmentedCache(8)": "moka segmented",
}
CACHES = list(NAMES.values())

speed = collections.defaultdict(list)
hits = collections.defaultdict(list)
for row in csv.reader(open(sys.argv[1])):
    if len(row) != 9:
        continue
    trace, _rep, cache, capacity, clients, _inserts, reads, ratio, secs = (
        field.strip() for field in row
    )
    cell = (trace, int(capacity), int(clients))
    name = NAMES.get(cache, cache)
    speed[cell, name].append(int(reads) / float(secs) / 1e6)
    hits[cell, name].append(float(ratio))

cells = sorted({cell for cell, _ in speed})
present = [c for c in CACHES if any((cell, c) in speed for cell in cells)]
others = [c for c in ("quick_cache", "moka") if c in present]
median = statistics.median


def ratio(cell, other):
    if (cell, "ShardedCache") in speed and (cell, other) in speed:
        return median(speed[cell, "ShardedCache"]) / median(speed[cell, other])
    return None


print("| trace | capacity | clients | "
      + " | ".join(present)
      + "".join(f" | over {o}" for o in others)
      + " |")
print("|---|---:|---:|" + "---:|" * (len(present) + len(others)))
for cell in cells:
    shown = []
    for name in present:
        if (cell, name) in speed:
            shown.append(f"{median(speed[cell, name]):.1f} ({median(hits[cell, name]):.1f}%)")
        else:
            shown.append("")
    for other in others:
        r = ratio(cell, other)
        shown.append(f"{r:.2f}x" if r else "")
    print(f"| {cell[0]} | {cell[1]:,} | {cell[2]} | " + " | ".join(shown) + " |")

print()
print("| clients | " + " | ".join(f"over {o}: geomean (range)" for o in others) + " |")
print("|---:|" + "---|" * len(others))
for clients in sorted({cell[2] for cell in cells}):
    shown = []
    for other in others:
        rs = [r for cell in cells if cell[2] == clients and (r := ratio(cell, other))]
        shown.append(
            f"{statistics.geometric_mean(rs):.2f}x ({min(rs):.2f} to {max(rs):.2f})" if rs else ""
        )
    print(f"| {clients} | " + " | ".join(shown) + " |")

print()
print("| cache | mean one-client hit ratio | cells |")
print("|---|---:|---:|")
common = [
    cell for cell in cells
    if cell[2] == 1 and all((cell, name) in hits for name in present)
]
for name in present:
    if common:
        mean = statistics.mean(median(hits[cell, name]) for cell in common)
        print(f"| {name} | {mean:.2f}% | {len(common)} |")
