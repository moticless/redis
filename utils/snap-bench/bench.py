#!/usr/bin/env python3
"""
Hybrid BGSAVE vs stock BGSAVE: save time, peak memory, latency, idle vs traffic,
per dataset shape. One row per (config, dataset, write rate) in a CSV.

  bench.py run  --name <cfg> --port P --datasets small,mixed,big --rates 0,20000 --csv out.csv [--scale 1.0]
  bench.py report out.csv          # markdown tables

The server must already be running on --port (see run.sh, which starts the two
binaries with identical flags, pinned to their own cores). Needs memtier_benchmark,
python redis, same host (reads /proc/<pid>).

Datasets (each <= ~400 MB at scale 1.0):
  small   1.2 M strings 100 B + 200 k listpack hashes (10 x 40 B)      only SNAP_NOFORK
  mixed   800 k strings 50 B + 200 k 1-elem lists + 200 k 1-elem sets
          + 16 k hashtable hashes (64 fields x 96 B, ~9 KB)               ~55% of bytes in 2% big keys
  big     22 hashes x 100 k fields x 64 B (~10 MB) + 2 zsets x 300 k     only SNAP_FORK
  big-grow  same data; traffic inserts random fields/members instead of overwriting f0

Traffic = memtier same-size in-place writes spread over the dataset's classes in
proportion to the bytes each class occupies, started 3 s before BGSAVE and kept
past its end, plus a 5 ms PING probe. Sampled every 100 ms during the save:
server RssAnon/VmSwap growth, child Private_Dirty, used_memory, blocked clients.
"""
import argparse, csv, json, multiprocessing as mp, os, random, statistics, string, subprocess, sys, threading, time
import redis

ALPHA = (string.ascii_letters + string.digits).encode()
def rnd_bytes(rnd, n): return bytes(rnd.choices(ALPHA, k=n))

# (prefix, count, kind, params, memtier command, data size)
DATASETS = {
    "small": [
        ("s:",  1_200_000, "string", {"size": 100},                   "SET __key__ __data__",     100),
        ("h:",    200_000, "hash",   {"fields": 10, "size": 40},      "HSET __key__ f0 __data__", 40),
    ],
    "mixed": [
        ("s:",    800_000, "string", {"size": 50},                    "SET __key__ __data__",     50),
        ("l:",    200_000, "list",   {"size": 50},                    "LSET __key__ 0 __data__",  50),
        ("st:",   200_000, "set",    {"size": 8},                     "SADD __key__ __data__",    8),
        ("bh:",    16_000, "hash",   {"fields": 64, "size": 96},      "HSET __key__ f0 __data__", 96),
    ],
    "big": [
        ("bh:",        22, "hash",   {"fields": 100_000, "size": 64}, "HSET __key__ f0 __data__", 64),
        ("bz:",         2, "zset",   {"members": 300_000},            "ZADD __key__ 1 0",         8),
    ],
    # same data, but the traffic inserts new fields/members (random names) into the big items
    "big-grow": [
        ("bh:",        22, "hash",   {"fields": 100_000, "size": 64}, "HSET __key__ __data__ v",  24),
        ("bz:",         2, "zset",   {"members": 300_000},            "ZADD __key__ 1 __data__",  16),
    ],
}

def fill(args):
    port, prefix, kind, params, lo, hi, seed = args
    r = redis.Redis(port=port); rnd = random.Random(f"{seed}:{prefix}:{lo}")
    p = r.pipeline(transaction=False); n = 0
    for i in range(lo, hi):
        k = f"{prefix}{i}"
        if kind == "string": p.set(k, rnd_bytes(rnd, params["size"])); n += 1
        elif kind == "list": p.rpush(k, rnd_bytes(rnd, params["size"])); n += 1
        elif kind == "set": p.sadd(k, rnd_bytes(rnd, params["size"])); n += 1
        elif kind == "hash":
            for f0 in range(0, params["fields"], 500):
                p.hset(k, mapping={f"f{j}": rnd_bytes(rnd, params["size"]) for j in range(f0, min(f0 + 500, params["fields"]))}); n += 1
        elif kind == "zset":
            for m0 in range(0, params["members"], 5000):
                p.zadd(k, {str(m): float(m) for m in range(m0, min(m0 + 5000, params["members"]))}); n += 1
        if n >= 1000: p.execute(); n = 0
    if n: p.execute()

def populate(port, dataset, scale, workers=6, seed=42):
    r = redis.Redis(port=port); r.flushall(); time.sleep(0.5)
    jobs = []
    for prefix, count, kind, params, _, _ in DATASETS[dataset]:
        count = max(1, int(count * scale))
        step = max(1, count // (workers * 4))
        jobs += [(port, prefix, kind, params, lo, min(lo + step, count + 1), seed) for lo in range(1, count + 1, step)]
    t0 = time.time()
    with mp.Pool(workers) as pool: pool.map(fill, jobs)
    return time.time() - t0

def scaled(dataset, scale):
    return [(p, max(1, int(c * scale)), k, pr, cmd, d) for p, c, k, pr, cmd, d in DATASETS[dataset]]

# ------------------------------------------------------------------ /proc
def read_file(path):
    try:
        with open(path) as f: return f.read()
    except OSError: return ""

class Server:
    def __init__(self, pid): self.pid = pid
    def status(self):
        d = {}
        for line in read_file(f"/proc/{self.pid}/status").splitlines():
            k, _, v = line.partition(":")
            if k in ("RssAnon", "VmSwap", "VmRSS", "Threads"): d[k] = int(v.split()[0]) // 1024 if k != "Threads" else int(v)
        return d
    def children(self):
        out = []
        for t in os.listdir(f"/proc/{self.pid}/task") if os.path.isdir(f"/proc/{self.pid}/task") else []:
            out += [int(c) for c in read_file(f"/proc/{self.pid}/task/{t}/children").split()]
        return out
    def child_mem(self):
        pd = rss = 0
        for c in self.children():
            for line in read_file(f"/proc/{c}/smaps_rollup").splitlines():
                if line.startswith("Private_Dirty:"): pd += int(line.split()[1]) // 1024
            for line in read_file(f"/proc/{c}/status").splitlines():
                if line.startswith("RssAnon:"): rss += int(line.split()[1]) // 1024
        return pd, rss
    def cpu_ticks(self):
        """(main thread utime+stime, whole process utime+stime, children utime+stime) in clock ticks."""
        def ticks(path):
            f = read_file(path)
            if not f: return 0
            fields = f.rsplit(")", 1)[1].split()
            return int(fields[11]) + int(fields[12])   # utime, stime
        main = ticks(f"/proc/{self.pid}/task/{self.pid}/stat"); proc = ticks(f"/proc/{self.pid}/stat")
        child = sum(ticks(f"/proc/{c}/stat") for c in self.children())
        return main, proc, child

class Sampler(threading.Thread):
    def __init__(self, srv, port, period=0.1):
        super().__init__(daemon=True); self.srv = srv; self.port = port; self.period = period
        self.rows = []; self.stop_ev = threading.Event(); self.child_cpu = 0
    def run(self):
        r = redis.Redis(port=self.port, socket_timeout=2)
        while not self.stop_ev.is_set():
            t = time.time(); st = self.srv.status(); pd, crss = self.srv.child_mem()
            row = {"t": t, "rss_anon_mb": st.get("RssAnon", 0), "swap_mb": st.get("VmSwap", 0), "child_pd_mb": pd, "child_rss_mb": crss}
            try:
                info = r.info("memory"); row["used_memory_mb"] = info["used_memory"] // 2**20
                row["blocked"] = r.info("clients").get("blocked_clients", 0)
            except redis.RedisError: pass
            _, _, cc = self.srv.cpu_ticks(); self.child_cpu = max(self.child_cpu, cc)
            self.rows.append(row)
            time.sleep(max(0, self.period - (time.time() - t)))

class PingProbe(threading.Thread):
    def __init__(self, port, period=0.005):
        super().__init__(daemon=True); self.port = port; self.period = period; self.samples = []; self.stop_ev = threading.Event()
    def run(self):
        r = redis.Redis(port=self.port, socket_timeout=5)
        while not self.stop_ev.is_set():
            t0 = time.time()
            try: r.ping()
            except redis.RedisError: pass
            self.samples.append((t0, (time.time() - t0) * 1000)); time.sleep(self.period)

def pct(xs, p):
    if not xs: return None
    xs = sorted(xs); k = min(len(xs) - 1, int(round(p / 100 * (len(xs) - 1)))); return round(xs[k], 3)

# ------------------------------------------------------------------ load
def memtier(port, cls, rate, secs, out_prefix, cpus):
    prefix, count, _, _, cmd, dsize = cls
    conns = max(2, min(8, round(8 * rate / 30000))); threads = min(4, conns); clients = max(1, conns // threads)
    per_conn = max(1, rate // (threads * clients))
    c = ["taskset", "-c", cpus, "memtier_benchmark", "-s", "127.0.0.1", "-p", str(port), "--protocol", "redis", "-t", str(threads), "-c", str(clients),
         "--test-time", str(secs), "--rate-limiting", str(per_conn), f"--command={cmd}", "--command-key-pattern=R",
         "--key-prefix", prefix, "--key-minimum", "1", "--key-maximum", str(count), "-d", str(dsize), "-R",
         "--hide-histogram", "--print-percentiles", "50,99,99.9,100", f"--json-out-file={out_prefix}.json", "--distinct-client-seed"]
    return subprocess.Popen(c, stdout=open(out_prefix + ".out", "w"), stderr=subprocess.STDOUT)

def parse_memtier(path):
    try:
        d = json.load(open(path))["ALL STATS"]["Totals"]
        pc = {x["Percentile"]: x["Latency"] for x in d.get("Percentile Latencies", {}).get("Totals", [])} if isinstance(d.get("Percentile Latencies"), dict) else {}
        # memtier >= 1.4 stores percentiles per-command; fall back to the flat keys
        pl = d.get("Percentile Latencies", {}) if isinstance(d.get("Percentile Latencies"), dict) else {}
        out = {"ops_per_sec": d.get("Ops/sec"), "avg_ms": (d.get("Accumulated Latency") or 0) / max(1, d.get("Count") or 1) / 1000,
               "p50.00": pl.get("p50.00"), "p99.00": pl.get("p99.00"), "p99.90": pl.get("p99.90"), "p100.00": pl.get("p100.0") or d.get("Max Latency")}
        ts = d.get("Time-Serie", {})
        return out, ts
    except (OSError, KeyError, json.JSONDecodeError, TypeError) as e:
        return {"err": str(e)}, {}

def class_weights(r, classes):
    rnd = random.Random(3); w = []
    for prefix, count, *_ in classes:
        keys = [f"{prefix}{rnd.randint(1, count)}" for _ in range(min(20, count))]
        p = r.pipeline(transaction=False)
        for k in keys: p.memory_usage(k, samples=0)
        mem = [m or 0 for m in p.execute()]
        w.append(sum(mem) / len(mem) * count)
    return w

def save_mode(r):
    """Which save path this build uses: ours (bgsave-hybrid), the teammate POC (bgsave_mode) or valkey (bgsave-default-method)."""
    cfg = r.config_get("bgsave-hybrid")
    if "bgsave-hybrid" in cfg: return "hybrid" if cfg["bgsave-hybrid"] == "yes" else "fork"
    p = r.info("persistence")
    if "bgsave_mode" in p: return p["bgsave_mode"]                      # teammate POC: fork | hybrid | forkless
    cfg = r.config_get("bgsave-default-method")
    if "bgsave-default-method" in cfg: return "valkey-" + cfg["bgsave-default-method"]
    return "fork"

IMPL_STATS = ["snap_last_save_retained_peak", "snap_hard_mark_aborts", "snap_save_main_keys",
              "forkless_retained_peak", "forkless_snapshot_peak", "forkless_limit_aborts", "forkless_copied_values",
              "forkless_retained_strings", "forkless_priority_keys", "forkless_copy_usec", "forkless_reader_wait_usec",
              "rdb_last_bgsave_type"]

def wait_save(r, poll=0.02):
    while True:
        p = r.info("persistence")
        if not p.get("rdb_bgsave_in_progress") and not p.get("snap_save_in_progress", 0): return time.time()
        time.sleep(poll)

# ------------------------------------------------------------------ one cell
def run_cell(a, dataset, rate, est_save):
    r = redis.Redis(port=a.port, socket_timeout=600)
    pid = r.info("server")["process_id"]; srv = Server(pid)
    classes = scaled(dataset, a.scale)
    mem0 = r.info("memory")["used_memory"]
    row = {"name": a.name, "ts": time.strftime("%Y-%m-%d %H:%M:%S"), "sha": r.info("server").get("redis_git_sha1"),
           "dataset": dataset, "rate": rate, "dbsize": r.dbsize(), "used_mb": mem0 // 2**20,
           "hybrid": int(r.config_get("bgsave-hybrid").get("bgsave-hybrid", "no") == "yes") if "bgsave-hybrid" in str(r.config_get("bgsave-hybrid")) else 0}
    row["mode"] = save_mode(r)
    r.execute_command("LATENCY", "RESET")
    secs = int(a.warm + est_save * 1.6 + 8)
    procs, outs = [], {}
    if rate > 0:
        weights = class_weights(r, classes); tot = sum(weights)
        row["shares"] = " ".join(f"{c[0]}{w/tot:.2f}" for c, w in zip(classes, weights))
        wd = os.path.join(a.workdir, f"{a.name}-{dataset}-r{rate}"); os.makedirs(wd, exist_ok=True)
        for cls, wgt in zip(classes, weights):
            cr = max(100, round(rate * wgt / tot)); out = os.path.join(wd, "memtier-" + cls[0].strip(":"))
            procs.append(memtier(a.port, cls, cr, secs, out, a.load_cpus)); outs[cls[0]] = (out + ".json", cr)
    t_mt = time.time(); time.sleep(a.warm if procs else 0.5)

    sampler = Sampler(srv, a.port); ping = PingProbe(a.port); sampler.start(); ping.start()
    time.sleep(0.3)
    st0 = srv.status(); cpu0 = srv.cpu_ticks()
    t0 = time.time(); r.bgsave(); t1 = wait_save(r); time.sleep(0.8)
    cpu1 = srv.cpu_ticks(); sampler.stop_ev.set(); ping.stop_ev.set(); sampler.join(); ping.join()
    p1 = r.info("persistence"); st = r.info("stats"); tick = os.sysconf("SC_CLK_TCK")
    row.update(save_sec=round(t1 - t0, 2), status=p1.get("rdb_last_bgsave_status"),
               fork_ms=round(st.get("latest_fork_usec", 0) / 1000, 2), cow_mb=p1.get("rdb_last_cow_size", 0) // 2**20,
               cpu_main_s=round((cpu1[0] - cpu0[0]) / tick, 2),
               cpu_threads_s=round(((cpu1[1] - cpu0[1]) - (cpu1[0] - cpu0[0])) / tick, 2),
               cpu_child_s=round(sampler.child_cpu / tick, 2),
               snap_retained_peak_mb=round(p1.get("snap_last_save_retained_peak", 0) / 2**20, 1) if "snap_last_save_retained_peak" in p1 else None,
               snap_hard_mark_aborts=p1.get("snap_hard_mark_aborts"),
               impl_stats=" ".join(f"{k}={p1[k]}" for k in IMPL_STATS if k in p1))
    base = st0.get("RssAnon", 0) + st0.get("VmSwap", 0)
    rows = [x for x in sampler.rows if t0 - 0.2 <= x["t"] <= t1 + 0.5]
    if rows:
        row["rss_base_mb"] = base
        row["extra_mem_peak_mb"] = max((x["rss_anon_mb"] + x["swap_mb"] - base) + x["child_pd_mb"] for x in rows)
        row["parent_growth_peak_mb"] = max(x["rss_anon_mb"] + x["swap_mb"] - base for x in rows)
        row["child_pd_peak_mb"] = max(x["child_pd_mb"] for x in rows)
        row["abs_peak_mb"] = max(x["rss_anon_mb"] + x["swap_mb"] + x["child_pd_mb"] for x in rows)
        row["used_mem_growth_mb"] = max(x.get("used_memory_mb", 0) for x in rows) - mem0 // 2**20
        row["blocked_peak"] = max(x.get("blocked", 0) for x in rows)
    ms = [m for t, m in ping.samples if t0 <= t <= t1]
    row.update(ping_p50_ms=pct(ms, 50), ping_p99_ms=pct(ms, 99), ping_max_ms=pct(ms, 100))
    lat = r.execute_command("LATENCY", "HISTORY", "command") or []
    row["cmd_events_ge20ms"] = len(lat)
    try:
        ev = r.execute_command("LATENCY", "LATEST") or []
        row["latency_events"] = " ".join(f"{e[0].decode() if isinstance(e[0], bytes) else e[0]}:{e[3]}ms" for e in ev if isinstance(e, (list, tuple)) and len(e) >= 4)
    except redis.RedisError: pass

    for p in procs: p.wait(timeout=secs + 60)
    ops = p99 = p999 = mx = stall = 0; missed = False; avg = []
    for prefix, (path, cr) in outs.items():
        m, ts = parse_memtier(path)
        if "err" in m: row["load_err"] = m["err"]; continue
        ops += m.get("ops_per_sec") or 0; avg.append(m.get("avg_ms") or 0)
        p99 = max(p99, m.get("p99.00") or 0); p999 = max(p999, m.get("p99.90") or 0); mx = max(mx, m.get("p100.00") or 0)
        if (m.get("ops_per_sec") or 0) < 0.95 * cr: missed = True
        stall = max(stall, sum(1 for s, rr in ts.items() if t0 <= t_mt + int(s) <= t1 and rr.get("Count", 0) < 0.9 * cr))
    if procs:
        row.update(ops=round(ops), load_avg_ms=round(statistics.fmean(avg), 3) if avg else None, load_p99_ms=p99, load_p999_ms=p999, load_max_ms=mx, stall_s=stall, target_missed=missed)
    return row

COLUMNS = ["name", "ts", "sha", "hybrid", "mode", "dataset", "rate", "shares", "dbsize", "used_mb", "rss_base_mb",
           "save_sec", "status", "fork_ms", "extra_mem_peak_mb", "parent_growth_peak_mb", "child_pd_peak_mb", "abs_peak_mb", "cow_mb",
           "used_mem_growth_mb", "cpu_main_s", "cpu_threads_s", "cpu_child_s", "ping_p50_ms", "ping_p99_ms", "ping_max_ms",
           "cmd_events_ge20ms", "ops", "load_avg_ms", "load_p99_ms", "load_p999_ms", "load_max_ms", "stall_s", "target_missed",
           "blocked_peak", "snap_retained_peak_mb", "snap_hard_mark_aborts", "impl_stats", "latency_events", "load_err"]

def cmd_run(a):
    r = redis.Redis(port=a.port); r.ping()
    new = not os.path.exists(a.csv)
    with open(a.csv, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=COLUMNS, extrasaction="ignore")
        if new: w.writeheader()
        for ds in a.datasets.split(","):
            secs = populate(a.port, ds, a.scale)
            mb = r.info("memory")["used_memory"] // 2**20
            print(f"[{a.name}] {ds}: {r.dbsize()} keys, {mb} MB, populated in {secs:.0f}s", file=sys.stderr)
            t0 = time.time(); r.bgsave(); wait_save(r); est = time.time() - t0; time.sleep(2)   # untimed warm-up save
            for rate in [int(x) for x in a.rates.split(",")]:
                for rep in range(a.reps):
                    row = run_cell(a, ds, rate, est); w.writerow(row); f.flush()
                    print(f"[{a.name}] {ds} r{rate} #{rep}: save {row['save_sec']}s fork {row['fork_ms']}ms extra_mem {row.get('extra_mem_peak_mb')}MB "
                          f"(parent +{row.get('parent_growth_peak_mb')}, child {row.get('child_pd_peak_mb')}) cow {row['cow_mb']}MB "
                          f"ping p99 {row.get('ping_p99_ms')} max {row.get('ping_max_ms')} load p99 {row.get('load_p99_ms')} max {row.get('load_max_ms')} stall {row.get('stall_s')}", file=sys.stderr)
                    time.sleep(a.cooldown)

def cmd_report(a):
    rows = list(csv.DictReader(open(a.csv)))
    def num(x):
        try: return float(x)
        except (TypeError, ValueError): return None
    groups = {}
    for r in rows: groups.setdefault((r["dataset"], r["rate"], r["name"]), []).append(r)
    def agg(g, k):
        xs = [num(r.get(k)) for r in g]; xs = [x for x in xs if x is not None]
        if not xs: return "-"
        return f"{statistics.median(xs):g}" if len(xs) == 1 else f"{statistics.median(xs):g} [{min(xs):g}-{max(xs):g}]"
    cols = [("save_sec", "save s"), ("fork_ms", "fork ms"), ("extra_mem_peak_mb", "extra mem peak MB"), ("parent_growth_peak_mb", "parent growth MB"),
            ("child_pd_peak_mb", "child dirty MB"), ("abs_peak_mb", "abs peak MB"), ("cow_mb", "rdb_last_cow MB"),
            ("cpu_main_s", "main cpu s"), ("cpu_threads_s", "threads cpu s"), ("cpu_child_s", "child cpu s"),
            ("ping_p99_ms", "ping p99 ms"), ("ping_max_ms", "ping max ms"), ("ops", "load ops/s"), ("load_p99_ms", "load p99 ms"), ("load_max_ms", "load max ms"), ("stall_s", "stall s")]
    print("| dataset | rate | config | n | " + " | ".join(c[1] for c in cols) + " |")
    print("|" + "---|" * (4 + len(cols)))
    for key in sorted(groups, key=lambda k: (k[0], int(k[1]), k[2])):
        g = groups[key]
        print(f"| {key[0]} | {key[1]} | {key[2]} | {len(g)} | " + " | ".join(agg(g, c[0]) for c in cols) + " |")

def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("run"); p.add_argument("--name", required=True); p.add_argument("--port", type=int, required=True)
    p.add_argument("--datasets", default="small,mixed,big"); p.add_argument("--rates", default="0,20000"); p.add_argument("--reps", type=int, default=1)
    p.add_argument("--csv", required=True); p.add_argument("--scale", type=float, default=1.0); p.add_argument("--warm", type=float, default=3)
    p.add_argument("--cooldown", type=float, default=3); p.add_argument("--workdir", default="/tmp/snap-bench-runs"); p.add_argument("--load-cpus", default="8-15")
    p.set_defaults(fn=cmd_run)
    p = sub.add_parser("report"); p.add_argument("csv"); p.set_defaults(fn=cmd_report)
    a = ap.parse_args(); a.fn(a)

if __name__ == "__main__":
    main()
