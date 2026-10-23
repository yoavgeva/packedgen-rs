import sys, collections, statistics
# usage: allsum.py CSV...  -- ab.sh rows; medians per backend and each over ShardedCache
rows = collections.defaultdict(list)
for path in sys.argv[1:]:
    for line in open(path):
        f = line.strip().split(',')
        if len(f) < 12:
            continue
        rows[(f[3], int(f[1]), int(f[7]), f[0])].append(float(f[11]))
labels = [l for l in ('base', 'guard', 'unpinstore', 'quick') if any(k[3] == l for k in rows)]
order = ['resident-hit', 'read-miss', 'cold-fill', 'turnover', 'replace', 'delete-reinsert']
print('| Workload | Entries | Threads | ' + ' | '.join(labels) + ' |')
print('| --- | ---: | ---: |' + ' ---: |' * len(labels))
for k in sorted({k[:3] for k in rows}, key=lambda k: (k[1], k[2], order.index(k[0]))):
    base = statistics.median(rows[k + ('base',)]) if k + ('base',) in rows else None
    cells = []
    for l in labels:
        if k + (l,) not in rows:
            cells.append('')
            continue
        m = statistics.median(rows[k + (l,)])
        cells.append(f'{m:.1f}' if l == 'base' or not base else f'{m:.1f} ({100 * (m / base - 1):+.0f}%)')
    print(f'| {k[0]} | {k[1]:,} | {k[2]} | ' + ' | '.join(cells) + ' |')
