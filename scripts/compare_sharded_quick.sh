#!/bin/sh
# Throwaway profiling run (bench/x86-profile): single-thread cold fill and
# delete-reinsert, ShardedCache against quick_cache, on the x86 runner.
set -u
entries=${3:-50000}
out=${4:-target/compare-sharded-quick.csv}
: > "$out"
[ "$entries" = 50000 ] || exit 0
mkdir -p target
sudo apt-get update -qq > /dev/null 2>&1
sudo apt-get install -y -qq valgrind binutils > /dev/null 2>&1
sudo apt-get install -y -qq linux-tools-common "linux-tools-$(uname -r)" > /dev/null 2>&1 \
    || sudo apt-get install -y -qq linux-tools-azure > /dev/null 2>&1 || true
sudo sysctl -w kernel.perf_event_paranoid=-1 kernel.kptr_restrict=0 > /dev/null 2>&1 || true
CARGO_PROFILE_RELEASE_DEBUG=line-tables-only cargo build --locked --release --example bounded_cache_probe
P=target/release/examples/bounded_cache_probe
cp "$P" target/compare-probe-bin.csv
W="cold-fill delete-reinsert"
B="sharded quickcache"

echo "== timing (Mops/s, 50,000 entries, 4M ops, 1 thread)"
for i in 1 2 3 4 5 6; do for w in $W; do
    if [ $((i % 2)) -eq 0 ]; then order='quickcache sharded'; else order='sharded quickcache'; fi
    for b in $order; do "$P" "$b" "$w" 50000 4000000 1 64 "$i" 0 | tail -n 1 | cut -d, -f1,2,10; done
done; done | tee target/compare-timing.csv
python3 - target/compare-timing.csv << 'EOF'
import sys, collections, statistics
c = collections.defaultdict(list)
for line in open(sys.argv[1]):
    b, w, m = line.strip().split(',')
    c[(w, b)].append(float(m))
for w in ('cold-fill', 'delete-reinsert'):
    s, q = statistics.median(c[(w, 'sharded')]), statistics.median(c[(w, 'quickcache')])
    print(f'{w}: sharded {s:.2f} quick {q:.2f} ratio {s / q - 1:+.1%}')
EOF

echo "== perf stat, difference of 8M and 4M ops, per op"
PERF=$(command -v perf)
EV=cycles,instructions,branch-misses,L1-dcache-load-misses,LLC-load-misses,dTLB-load-misses,L1-icache-load-misses,iTLB-load-misses
"$PERF" stat -e "$EV" true > /dev/null 2>&1 || EV=task-clock
for w in $W; do for b in $B; do for ops in 4000000 8000000; do
    "$PERF" stat -x, -e "$EV" -o "target/stat-$b-$w-$ops.txt" "$P" "$b" "$w" 50000 "$ops" 1 64 1 0 > /dev/null
done; done; done
python3 - << 'EOF'
import collections
def load(p):
    c = {}
    for line in open(p):
        f = line.strip().split(',')
        if len(f) > 3 and f[0] and f[0][0].isdigit():
            c[f[2]] = float(f[0])
    return c
for w in ('cold-fill', 'delete-reinsert'):
    for b in ('sharded', 'quickcache'):
        a = load(f'target/stat-{b}-{w}-4000000.txt'); z = load(f'target/stat-{b}-{w}-8000000.txt')
        d = {k: (z[k] - a.get(k, 0)) / 4e6 for k in z}
        ipc = d.get('cycles') and d.get('instructions', 0) / d['cycles']
        print(w, b, ' '.join(f'{k.split(":")[0]}={v:.2f}' for k, v in d.items()), f'ipc={ipc:.2f}' if ipc else '')
EOF

echo "== perf record, top symbols and addresses (ASLR off)"
REC="-e cycles:u -c 20011"
"$PERF" record $REC -o /dev/null true > /dev/null 2>&1 || REC="-e cpu-clock -F 20000"
for w in $W; do for b in $B; do
    setarch -R "$PERF" record $REC -o "target/p-$b-$w.data" "$P" "$b" "$w" 50000 8000000 1 64 1 0 > /dev/null 2>&1
    echo "-- $b $w symbols"
    "$PERF" report -i "target/p-$b-$w.data" --no-children --sort dso,sym --stdio 2> /dev/null \
        | grep -v '^#' | grep -v '^$' | head -25 | tee "target/compare-sym-$b-$w.csv"
    "$PERF" script -i "target/p-$b-$w.data" -F ip,sym 2> /dev/null > "target/ips-$b-$w.txt"
    python3 - "target/ips-$b-$w.txt" "$P" << 'EOF' | tee "target/compare-ips-$b-$w.csv"
import sys, collections, subprocess
ips = collections.Counter(); syms = {}
total = 0
for line in open(sys.argv[1]):
    f = line.split(None, 1)
    if not f: continue
    total += 1
    ip = int(f[0], 16)
    if 0x555555554000 <= ip < 0x555600000000:
        ips[ip] += 1; syms[ip] = f[1].strip()[:60] if len(f) > 1 else ''
base = 0x555555554000
for ip, n in ips.most_common(60):
    off = ip - base
    src = subprocess.run(['addr2line', '-i', '-e', sys.argv[2], hex(off)], capture_output=True, text=True).stdout
    chain = ' < '.join(s.split('/src/')[-1].split('/library/')[-1] for s in src.split('\n') if s)[:150]
    dis = subprocess.run(['objdump', '-d', '--no-show-raw-insn', f'--start-address={off}', f'--stop-address={off + 16}', sys.argv[2]], capture_output=True, text=True).stdout
    ins = [l for l in dis.split('\n') if l.strip().startswith(f'{off:x}:')]
    ins = ins[0].split(':', 1)[1].strip() if ins else '?'
    print(f'{n:5d} {100 * n / total:5.2f}% {off:x} {ins[:36]:36s} {chain}')
EOF
done; done

echo "== cachegrind, difference of 800k and 400k ops, per op"
for w in $W; do for b in $B; do for ops in 400000 800000; do
    valgrind --tool=cachegrind --cache-sim=yes --cachegrind-out-file="target/compare-cg-$b-$w-$ops.csv" \
        "$P" "$b" "$w" 50000 "$ops" 1 64 1 0 > /dev/null 2>&1 &
done; done; done
wait
for w in $W; do for b in $B; do
    echo "-- $b $w"
    python3 - "target/compare-cg-$b-$w-400000.csv" "target/compare-cg-$b-$w-800000.csv" 400000 << 'EOF'
import sys, collections, re
def load(p):
    fns = collections.defaultdict(lambda: None); lines = collections.Counter(); ev = None; fn = fl = None
    tot = None
    for line in open(p):
        if line.startswith('events:'): ev = line.split()[1:]; tot = collections.Counter()
        elif line.startswith('fl='): fl = line[3:].strip()
        elif line.startswith('fn='): fn = line[3:].strip()
        elif line[:1].isdigit():
            f = line.split(); v = [int(x) for x in f[1:]] + [0] * (len(ev) - len(f) + 1)
            k = (fn,)
            cur = fns[k] or [0] * len(ev); fns[k] = [a + b for a, b in zip(cur, v)]
            lines[(fl.split('/src/')[-1], int(f[0]), fn)] += v[0]
            for e, x in zip(ev, v): tot[e] += x
    return ev, fns, lines, tot
ev, fa, la, ta = load(sys.argv[1]); _, fb, lb, tb = load(sys.argv[2]); n = int(sys.argv[3])
print('total/op', ' '.join(f'{e}={(tb[e] - ta[e]) / n:.2f}' for e in ev))
clean = lambda s: re.sub(r'<[^<>]*>', '', re.sub(r'<[^<>]*>', '', s or ''))[-70:]
d = sorted(((([y - x for x, y in zip(fa.get(k) or [0] * len(ev), fb[k])]), k) for k in fb), reverse=True)
print('by function (Ir/op, D1 misses/op):')
for v, k in d[:22]:
    if v[0] / n >= 1:
        dm = sum(v[i] for i, e in enumerate(ev) if e in ('D1mr', 'D1mw')) / n
        print(f'  {v[0] / n:7.1f} {dm:6.3f}  {clean(k[0])}')
print('by source line (Ir/op):')
dl = collections.Counter({k: lb[k] - la.get(k, 0) for k in lb})
for k, v in dl.most_common(30):
    if v / n >= 2: print(f'  {v / n:7.1f}  {k[0]}:{k[1]}  [{clean(k[2])[-40:]}]')
EOF
done; done
