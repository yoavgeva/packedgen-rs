import sys, collections, re
def load(path):
    fn = fl = None
    events = None
    by_fn = collections.Counter(); by_fl = collections.Counter(); by_line = collections.Counter()
    for line in open(path):
        if line.startswith('events:'):
            events = line.split()[1:]
        elif line.startswith('fl='):
            fl = line[3:].strip()
        elif line.startswith('fn='):
            fn = line[3:].strip()
        elif line[:1].isdigit() or line[:1] == '-':
            parts = line.split()
            ir = int(parts[1]) if len(parts) > 1 else 0
            by_fn[fn] += ir; by_fl[fl] += ir; by_line[(fl, int(parts[0]))] += ir
    return by_fn, by_fl, by_line
ops = 400000
def short(name):
    name = re.sub(r'<[^<>]*>', '', name)
    name = re.sub(r'<[^<>]*>', '', name)
    return name[-90:]
for path in sys.argv[1:]:
    by_fn, by_fl, by_line = load(path)
    total = sum(by_fn.values())
    print(f'=== {path.split("/")[-1]}  total {total/ops:.0f} Ir/op')
    for name, ir in by_fn.most_common(18):
        print(f'{ir/ops:7.1f}  {short(name)}')
