import sys, collections, statistics
# Usage: gains.py CSV  -- rows from ab.sh (label,entries,probe row...)
rows = collections.defaultdict(list)
for line in open(sys.argv[1]):
    f = line.strip().split(',')
    if len(f) < 12:
        continue
    rows[(f[3], int(f[1]), int(f[7]), f[0])].append(float(f[11]))
labels = [l for l in ('v0', 'v1', 'v2', 'batch', 'quick') if any(k[3] == l for k in rows)]
med = lambda k: statistics.median(rows[k]) if k in rows else None
def pct(a, b):
    return f'{100 * (a / b - 1):+.0f}%' if a and b else ''
print('| Workload | Entries | Threads | ' + ' | '.join(labels) + ' | v2 vs v0 | v2 vs v1 | v2 vs quick | batch vs quick |')
print('| --- | ---: | ---: |' + ' ---: |' * (len(labels) + 4))
order = ['resident-hit', 'read-miss', 'cold-fill', 'turnover', 'replace', 'delete-reinsert']
for k in sorted({k[:3] for k in rows}, key=lambda k: (k[1], k[2], order.index(k[0]))):
    m = {l: med(k + (l,)) for l in labels}
    cells = [f'{m[l]:.1f}' if m[l] else '' for l in labels]
    print(f'| {k[0]} | {k[1]:,} | {k[2]} | ' + ' | '.join(cells)
          + f" | {pct(m.get('v2'), m.get('v0'))} | {pct(m.get('v2'), m.get('v1'))}"
          + f" | {pct(m.get('v2'), m.get('quick'))} | {pct(m.get('batch'), m.get('quick'))} |")
