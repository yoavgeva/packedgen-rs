import sys, collections, re
# usage: sum.py OPS DIFF.cg...  -- per-operation event totals and top functions
ops = int(sys.argv[1])
for path in sys.argv[2:]:
    fn = None; tot = [0] * 9; by_fn = collections.defaultdict(lambda: [0] * 9)
    for line in open(path):
        if line.startswith('fn='): fn = line[3:].strip()
        elif line[:1].isdigit() or line[:1] == '-':
            p = list(map(int, line.split()))[1:]
            for i, v in enumerate(p):
                tot[i] += v; by_fn[fn][i] += v
    names = 'Ir I1mr ILmr Dr D1mr DLmr Dw D1mw DLmw'.split()
    print(f'### {path.split("/")[-1]}')
    print(' '.join(f'{n}={t/ops:.2f}' for n, t in zip(names, tot)))
    for name, ev in sorted(by_fn.items(), key=lambda kv: -kv[1][0])[:15]:
        short = re.sub(r'<[^<>]*>', '', re.sub(r'<[^<>]*>', '', name))[-80:]
        print(f'  {ev[0]/ops:7.1f} Ir {ev[3]/ops:5.1f} Dr {ev[4]/ops:5.2f} D1mr {ev[5]/ops:5.2f} DLmr  {short}')
