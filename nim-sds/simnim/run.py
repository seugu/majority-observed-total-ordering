#!/usr/bin/env python3
"""Parallel runner and summary for finality_sim.

Comma-separated values sweep a parameter; all swept parameters are combined
(cartesian product). Every (configuration, seed) pair is one run.

  python3 simnim/run.py --n 50 --loss 10,20 --variant A,B --seeds 10

Per configuration it reports, counted over runs:
  safety    no two nodes finalized conflicting orders
  complete  every node finalized all k mains
  order     the agreed order equals the release order 1>2>...>k
"""
import argparse, itertools, json, os, signal, statistics as st, subprocess, sys, threading
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_BIN = os.path.join(HERE, "..", "build", "finality_sim")

# (flag, type, default, help). Every one of these accepts a comma-separated sweep.
SWEPT = [
    ("n", int, "50", "nodes"),
    ("loss", float, "10", "packet loss %%"),
    ("max-delay", float, "30", "max one-way delay (s)"),
    ("delay", str, "uniform", "delay model: uniform | tail | geo"),
    ("interval", float, "60", "seconds between MAIN releases"),
    ("variant", str, "A", "A = spec, B = A + re-gossip"),
    ("sync", float, "30", "SYNC period (s), 0 = off"),
    ("senders", str, "random", "random | single"),
    ("payload", int, "0", "MAIN payload bytes"),
    ("bad-frac", float, "0", "fraction of nodes with a bad link"),
    ("offline-frac", float, "0", "fraction of nodes offline for --offline-dur"),
    ("votes", str, "received", "received | delivered"),
    ("admit", str, "received", "received | delivered"),
    ("causal", int, "0", "SDS maxCausalHistory, 0 = max(11, 2N)"),
    ("regossip", float, "20", "re-gossip period for B (s)"),
]
# (flag, type, default, help). Passed through unchanged.
FIXED = [
    ("k", int, 5, "number of MAIN messages"),
    ("min-delay", float, 0.05, "min one-way delay (s)"),
    ("bad-loss", float, 50, "loss %% on bad links"),
    ("offline-at", float, -1, "offline start (s), -1 = interval"),
    ("offline-dur", float, 120, "offline duration (s)"),
    ("t-end", float, 0, "simulated time limit (s), 0 = (k-1)*interval+900"),
]

procs, procs_lock = set(), threading.Lock()


def run_one(binary, cfg, fixed, flags, seed, timeout):
    """Run one seed; returns the parsed JSON line, or None if killed."""
    cmd = [binary] + [f"--{k}={v}" for k, v in {**cfg, **fixed}.items()]
    cmd += [f"--{f}" for f in flags] + [f"--seed={seed}", "--seeds=1"]
    if timeout:
        cmd.append(f"--wall={timeout}")   # sim stops itself and reports a partial result
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, text=True)
    with procs_lock:
        procs.add(p)
    try:
        out, _ = p.communicate(timeout=timeout * 2 + 60 if timeout else None)  # hard backstop
    except subprocess.TimeoutExpired:
        p.kill(); out = ""
    finally:
        with procs_lock:
            procs.discard(p)
    lines = out.strip().splitlines()
    return json.loads(lines[-1]) if p.returncode == 0 and lines else None


def kill_all(*_):
    with procs_lock:
        for p in procs:
            p.kill()
    print("\ninterrupted: all simulator processes killed", file=sys.stderr)
    os._exit(130)


def fmt_cfg(cfg, swept_keys):
    return " ".join(f"{k}={cfg[k]}" for k in swept_keys)


def summarize(cfg, swept_keys, rs, k):
    ideal = list(range(1, k + 1))
    n = len(rs)
    print(f"[{fmt_cfg(cfg, swept_keys) or 'default'}]  {n} runs")
    if not n:
        print("  no results\n"); return
    complete = [r for r in rs if r["all_done"]]
    in_order = [r for r in complete if r["order"] == ideal]
    print(f"  safety    {sum(r['safety'] for r in rs):>3}/{n}  no conflicting orders")
    print(f"  complete  {len(complete):>3}/{n}  every node finalized all {k} mains")
    print(f"  order     {len(in_order):>3}/{n}  agreed order == release order")
    # completed runs that agreed on a non-release order
    other = defaultdict(list)
    for r in complete:
        if r["order"] != ideal:
            other[">".join(map(str, r["order"]))].append(r["seed"])
    for o, seeds in other.items():
        print(f"            {len(seeds)} run(s): all nodes agreed on {o}  (seeds {seeds})")
    for r in [r for r in rs if not r["all_done"]][:3]:
        why = "wall-clock limit" if r["wall_timeout"] else f"sim time limit, stall={r['stall']}"
        stuck = ", ".join(f"n{i}@{p}" for i, p in r["stuck"])
        print(f"  not done  seed {r['seed']}: {r['nodes_done']}/{cfg['n']} nodes done ({why}); "
              f"node@positions: {stuck}")
    l1 = [r["lat"][0] for r in rs if r["lat"] and r["lat"][0] is not None]
    la = [x for r in rs for x in r["lat"] if x is not None]
    med = lambda xs: f"{st.median(xs):.0f}s" if xs else "-"
    print(f"  latency   median release -> last node: first main {med(l1)}, all mains {med(la)}")
    print(f"  traffic   mean {st.mean(r['sent'] for r in rs):.0f} packets, "
          f"{st.mean(r['bytes'] for r in rs) / 1e6:.2f} MB sent per run")
    miss = sum(1 for r in rs if r["fin_missing"])
    print(f"  content   {miss}/{n} runs: some node finalized a main it had not received (at run end)\n")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    for f, _, d, h in SWEPT:
        ap.add_argument(f"--{f}", default=d, help=f"{h} (default {d})")
    for f, t, d, h in FIXED:
        ap.add_argument(f"--{f}", type=t, default=d, help=f"{h} (default {d})")
    ap.add_argument("--strip", action="store_true", help="strip stale repair requests from SDS-R rebroadcasts")
    ap.add_argument("--run-to-end", action="store_true", help="keep simulating after everyone finalized")
    ap.add_argument("--seeds", type=int, default=10, help="runs per configuration (default 10)")
    ap.add_argument("--seed", type=int, default=0, help="first seed (default 0)")
    ap.add_argument("--jobs", type=int, default=os.cpu_count() or 2, help="parallel runs")
    ap.add_argument("--timeout", type=float, default=0, help="wall-clock limit per run (s); partial result kept")
    ap.add_argument("--out", help="append raw per-run JSON lines to this file")
    ap.add_argument("--bin", default=DEFAULT_BIN, help="simulator binary")
    a = ap.parse_args()
    if not os.path.exists(a.bin):
        sys.exit(f"binary not found: {a.bin} (run simnim/build.sh first)")
    signal.signal(signal.SIGINT, kill_all)

    values = {f: [t(x) for x in getattr(a, f.replace("-", "_")).split(",")] for f, t, _, _ in SWEPT}
    swept_keys = [f for f in values if len(values[f]) > 1]
    configs = [dict(zip(values, combo)) for combo in itertools.product(*values.values())]
    fixed = {f: getattr(a, f.replace("-", "_")) for f, _, _, _ in FIXED}
    flags = [f for f in ("strip", "run-to-end") if getattr(a, f.replace("-", "_"))]
    seeds = range(a.seed, a.seed + a.seeds)
    print(f"{len(configs)} config(s) x {a.seeds} seed(s) = {len(configs) * a.seeds} runs, {a.jobs} parallel",
          file=sys.stderr)
    common = {k: v for k, v in configs[0].items() if k not in swept_keys}
    print("fixed: " + " ".join(f"{k}={v}" for k, v in {**common, **fixed}.items()) + "\n")

    res, done = defaultdict(list), 0
    outf = open(a.out, "a") if a.out else None
    with ThreadPoolExecutor(a.jobs) as ex:
        futs = {ex.submit(run_one, a.bin, c, fixed, flags, s, a.timeout): i
                for i, c in enumerate(configs) for s in seeds}
        for fut in as_completed(futs):
            r = fut.result()
            done += 1
            print(f"\r{done}/{len(futs)}", end="", file=sys.stderr, flush=True)
            if r is None:
                continue
            i = futs[fut]
            res[i].append(r)
            if outf:
                outf.write(json.dumps({**configs[i], **fixed, "r": r}) + "\n"); outf.flush()
    print(file=sys.stderr)
    for i, c in enumerate(configs):
        summarize(c, swept_keys, sorted(res[i], key=lambda r: r["seed"]), a.k)


if __name__ == "__main__":
    main()
