import sys, collections
def load(path):
    fn = fl = None; c = collections.defaultdict(lambda: [0]*9)
    for line in open(path):
        if line.startswith('fl='): fl = line[3:].strip()
        elif line.startswith('fn='): fn = line[3:].strip()
        elif line[:1].isdigit():
            p = list(map(int, line.split()))
            row = c[(fl, fn, p[0])]
            for i, v in enumerate(p[1:]): row[i] += v
    return c
a, b = load(sys.argv[1]), load(sys.argv[2])
with open(sys.argv[3], 'w') as out:
    out.write('events: Ir I1mr ILmr Dr D1mr DLmr Dw D1mw DLmw\n')
    for key in set(a) | set(b):
        fl, fn, line = key
        d = [y - x for x, y in zip(a.get(key, [0]*9), b.get(key, [0]*9))]
        if any(d):
            out.write(f'fl={fl}\nfn={fn}\n{line} ' + ' '.join(map(str, d)) + '\n')
