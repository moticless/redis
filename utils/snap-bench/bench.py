#!/usr/bin/env python3
"""
Hybrid BGSAVE vs stock BGSAVE: save time, peak memory, latency, idle vs traffic,
per dataset shape. One row per (config, dataset, write rate) in a CSV.

  bench.py run  --name <cfg> --port P --datasets small,mixed,big --rates 0,20000 --csv out.csv [--scale 1.0]
  bench.py report out.csv          # markdown tables

The server must already be running on --port (see run.sh, which starts the two
binaries with identical flags, pinned to their own cores). Needs
python redis, same host (reads /proc/<pid>).

Datasets (each <= ~400 MB at scale 1.0):
  small   1.2 M strings 100 B + 200 k listpack hashes (10 x 40 B)      only SNAP_NOFORK
  mixed   800 k strings 50 B + 200 k 1-elem lists + 200 k 1-elem sets
          + 16 k hashtable hashes (64 fields x 96 B, ~9 KB)               ~55% of bytes in 2% big keys
  big     22 hashes x 100 k fields x 64 B (~10 MB) + 2 zsets x 300 k     only SNAP_FORK
  big-grow  same data; traffic inserts random new fields/members instead of overwriting existing ones

Traffic = snapload.py (asyncio, raw RESP, rate limited) same-size writes spread
over the dataset's classes in proportion to the bytes each class occupies, keys
uniform over the first --hot fraction of each class (default 1.0 = all keys),
random EXISTING field/member for hashes and zsets, started 3 s before BGSAVE and
kept past its end, plus a 5 ms PING probe (the latency instrument; load latency is
per pipelined batch). A populated dataset is cached as an RDB under --cache and
loaded with DEBUG RELOAD NOSAVE on later runs (same bytes for every binary). Sampled every 100 ms during the save:
server RssAnon/VmSwap growth, child Private_Dirty, used_memory, blocked clients.
"""
import argparse, csv, json, multiprocessing as mp, os, random, shutil, statistics, string, subprocess, sys, threading, time
import redis

ALPHA = (string.ascii_letters + string.digits).encode()
def rnd_bytes(rnd, n): return bytes(rnd.choices(ALPHA, k=n))

# (prefix, count, kind, params, write mode, write data size)
DATASETS = {
    "small": [
        ("s:",  1_200_000, "string", {"size": 100},                   "overwrite", 100),
        ("h:",    200_000, "hash",   {"fields": 10, "size": 40},      "overwrite", 40),
    ],
    "mixed": [
        ("s:",    800_000, "string", {"size": 50},                    "overwrite", 50),
        ("l:",    200_000, "list",   {"size": 50},                    "overwrite", 50),
        ("st:",   200_000, "set",    {"size": 8},                     "grow",      8),
        ("bh:",    16_000, "hash",   {"fields": 64, "size": 96},      "overwrite", 96),
    ],
    "big": [   # run 12+: one class of 1M-field hashes (about 100 MB each) beside the 10 MB ones; runs 4-11 had 22 x 100k-field hashes only
        ("hh:",         1, "hash",   {"fields": 1_000_000, "size": 64}, "overwrite", 64),
        ("bh:",        12, "hash",   {"fields": 100_000, "size": 64}, "overwrite", 64),
        ("bz:",         2, "zset",   {"members": 300_000},            "overwrite", 8),
    ],
    # run 13: the big component is 1M-field hashes (about 90 MB each, hashtable); counts scale with --scale (int(c*scale))
    "mixed1m": [
        ("s:",    800_000, "string", {"size": 50},                      "overwrite", 50),
        ("l:",    200_000, "list",   {"size": 50},                      "overwrite", 50),
        ("st:",   200_000, "set",    {"size": 8},                       "grow",      8),
        ("hh:",       1.5, "hash",   {"fields": 1_000_000, "size": 64}, "overwrite", 64),
    ],
    "big1m": [
        ("hh:",      2.25, "hash",   {"fields": 1_000_000, "size": 64}, "overwrite", 64),
        ("bz:",         2, "zset",   {"members": 300_000},              "overwrite", 8),
    ],
    # same data, but the traffic inserts new fields/members (random names) into the big items
    "big-grow": [
        ("bh:",        22, "hash",   {"fields": 100_000, "size": 64}, "grow",      24),
        ("bz:",         2, "zset",   {"members": 300_000},            "grow",      16),
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

def cpu_mhz(cpus):
    """Mean current frequency (MHz) of the server's CPUs; 0 if unavailable. Throttling shows up here."""
    vals = []
    for c in cpus:
        v = read_file(f"/sys/devices/system/cpu/cpu{c}/cpufreq/scaling_cur_freq").strip()
        if v.isdigit(): vals.append(int(v) / 1000)
    return round(sum(vals) / len(vals)) if vals else 0

def cpu_temp():
    """Package temperature (C) from the TCPU/x86_pkg_temp thermal zone; 0 if unavailable."""
    for z in sorted(os.listdir("/sys/class/thermal")) if os.path.isdir("/sys/class/thermal") else []:
        if read_file(f"/sys/class/thermal/{z}/type").strip() in ("TCPU", "x86_pkg_temp"):
            v = read_file(f"/sys/class/thermal/{z}/temp").strip()
            return int(v) // 1000 if v.isdigit() else 0
    return 0

def parse_cpus(spec):
    out = []
    for part in spec.split(","):
        a, _, b = part.partition("-"); out += list(range(int(a), int(b or a) + 1))
    return out

class Sampler(threading.Thread):
    def __init__(self, srv, port, period=0.1, cpus=()):
        super().__init__(daemon=True); self.srv = srv; self.port = port; self.period = period; self.cpus = cpus
        self.rows = []; self.stop_ev = threading.Event(); self.child_cpu = 0
    def run(self):
        r = redis.Redis(port=self.port, socket_timeout=2)
        while not self.stop_ev.is_set():
            t = time.time(); st = self.srv.status(); pd, crss = self.srv.child_mem()
            row = {"t": t, "rss_anon_mb": st.get("RssAnon", 0), "swap_mb": st.get("VmSwap", 0), "child_pd_mb": pd, "child_rss_mb": crss,
                   "mhz": cpu_mhz(self.cpus), "temp": cpu_temp()}
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
SNAPLOAD = os.path.join(os.path.dirname(os.path.abspath(__file__)), "snapload.py")

def snapload(port, cls, rate, secs, out_prefix, cpus, hot):
    prefix, count, kind, params, mode, dsize = cls
    conns = max(2, min(8, round(8 * rate / 30000)))
    c = ["taskset", "-c", cpus, sys.executable, SNAPLOAD, "--port", str(port), "--seconds", str(secs), "--rate", str(rate),
         "--kind", kind, "--prefix", prefix, "--count", str(count), "--fields", str(params.get("fields", 1)),
         "--members", str(params.get("members", 1)), "--size", str(dsize), "--mode", mode, "--hot", str(hot),
         "--conns", str(conns), "--pipeline", "16", "--out", out_prefix + ".json"]
    return subprocess.Popen(c, stdout=open(out_prefix + ".out", "w"), stderr=subprocess.STDOUT)

def parse_load(path):
    try:
        d = json.load(open(path))
        out = {"ops_per_sec": d.get("ops_per_sec"), "avg_ms": d.get("avg_ms"), "p50.00": d.get("p50.00"), "p99.00": d.get("p99.00"),
               "p99.90": d.get("p99.90"), "p100.00": d.get("p100.00")}
        if d.get("errors"): out["err"] = "; ".join(d["errors"])
        return out, d.get("ts", {})
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

IMPL_STATS = ["snap_last_save_retained_peak", "snap_last_save_queued_peak", "snap_last_save_spool_bytes", "snap_last_save_spool_peak", "snap_hard_mark_aborts", "snap_save_main_keys",
              "forkless_retained_peak", "forkless_snapshot_peak", "forkless_limit_aborts", "forkless_copied_values",
              "forkless_retained_strings", "forkless_priority_keys", "forkless_copy_usec", "forkless_reader_wait_usec",
              "rdb_last_bgsave_type"]

def wait_save(r, poll=0.02):
    while True:
        p = r.info("persistence")
        if not p.get("rdb_bgsave_in_progress") and not p.get("snap_save_in_progress", 0): return time.time()
        time.sleep(poll)

# ------------------------------------------------------------------ full sync (TASK-1.14)
class Replica:
    """A throwaway replica process on the load CPUs, one per sync cell, same binary as the primary."""
    def __init__(self, a):
        self.a = a; self.port = a.sync_port
        self.dir = os.path.join(a.workdir, f"replica-{self.port}"); os.makedirs(self.dir, exist_ok=True)
        for f in os.listdir(self.dir):
            if f.endswith(".rdb"): os.unlink(os.path.join(self.dir, f))
        cmd = ["taskset", "-c", a.load_cpus, a.sync_bin, "--port", str(self.port), "--bind", "127.0.0.1", "--save", "",
               "--appendonly", "no", "--dir", self.dir, "--logfile", os.path.join(self.dir, "replica.log"),
               "--protected-mode", "no"] + (a.sync_replica_args.split() if a.sync_replica_args else [])
        self.proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.r = redis.Redis(port=self.port, socket_timeout=600)
        for _ in range(100):
            try: self.r.ping(); break
            except redis.RedisError: time.sleep(0.1)
        else: raise RuntimeError("replica did not start")
        self.srv = Server(self.proc.pid)
    def info(self, section):
        try: return self.r.info(section)
        except redis.RedisError: return {}
    def stop(self):
        try: self.r.execute_command("REPLICAOF", "NO", "ONE"); self.r.shutdown(nosave=True)
        except redis.RedisError: pass
        try: self.proc.wait(timeout=30)
        except subprocess.TimeoutExpired: self.proc.kill()

# ------------------------------------------------------------------ one cell
def cool_down(a):
    """Thermal gate: this laptop throttles (package 85-89 C, cores at 0.8-1.6 GHz of 4.7) after minutes of load and
    save times then track the clock, not the code. Wait until the package is below --cool-below (0 = off)."""
    if not a.cool_below: return 0
    t0 = time.time()
    while cpu_temp() > a.cool_below and time.time() - t0 < a.cool_wait: time.sleep(1)
    return round(time.time() - t0)

def run_cell(a, dataset, rate, est_save):
    cooled = cool_down(a)
    r = redis.Redis(port=a.port, socket_timeout=600)
    pid = r.info("server")["process_id"]; srv = Server(pid)
    classes = scaled(dataset, a.scale)
    mem0 = r.info("memory")["used_memory"]
    row = {"name": a.name, "ts": time.strftime("%Y-%m-%d %H:%M:%S"), "sha": r.info("server").get("redis_git_sha1"),
           "dataset": dataset, "rate": rate, "hot": a.hot, "dbsize": r.dbsize(), "used_mb": mem0 // 2**20,
           "cooled_s": cooled, "temp_c_start": cpu_temp(),
           "hybrid": int(r.config_get("bgsave-hybrid").get("bgsave-hybrid", "no") == "yes") if "bgsave-hybrid" in str(r.config_get("bgsave-hybrid")) else 0}
    row["mode"] = save_mode(r)
    r.execute_command("LATENCY", "RESET")
    secs = int(a.warm + est_save * (3.0 if a.sync_bin else 1.6) + 8)
    procs, outs = [], {}
    if rate > 0:
        weights = class_weights(r, classes); tot = sum(weights)
        row["shares"] = " ".join(f"{c[0]}{w/tot:.2f}" for c, w in zip(classes, weights))
        wd = os.path.join(a.workdir, f"{a.name}-{dataset}-r{rate}"); os.makedirs(wd, exist_ok=True)
        for cls, wgt in zip(classes, weights):
            cr = max(100, round(rate * wgt / tot)); out = os.path.join(wd, "load-" + cls[0].strip(":"))
            procs.append(snapload(a.port, cls, cr, secs, out, a.load_cpus, a.hot)); outs[cls[0]] = (out + ".json", cr)
    t_mt = time.time(); time.sleep(a.warm if procs else 0.5)

    sampler = Sampler(srv, a.port, cpus=parse_cpus(a.server_cpus)); ping = PingProbe(a.port); sampler.start(); ping.start()
    time.sleep(0.3)
    st0 = srv.status(); cpu0 = srv.cpu_ticks()
    rep = None; sync = {}
    if a.sync_bin:
        # Full sync instead of a BGSAVE: the primary's child (or forkless thread) streams a fresh replica.
        for k, v in (("repl-diskless-sync", "yes"), ("repl-diskless-sync-delay", "0")):
            try: r.config_set(k, v)
            except redis.RedisError: pass
        rep = Replica(a); mem_master_cob = []; mem_rep = []; rep_buf_peak = 0; rep_mem0 = rep.info("memory").get("used_memory", 0)
        sync_full0 = r.info("stats").get("sync_full", 0)
        t0 = time.time(); rep.r.execute_command("REPLICAOF", "127.0.0.1", str(a.port))
        t_child = None; started = False
        while True:
            p = r.info("persistence")
            busy = p.get("rdb_bgsave_in_progress") or p.get("snap_save_in_progress", 0) or p.get("rdb_current_bgsave_type") not in (None, "none", "", 0)
            if busy: started = True
            elif started and t_child is None: t_child = time.time()
            m = r.info("memory"); mem_master_cob.append(m.get("mem_clients_slaves", 0))
            rm = rep.info("memory"); mem_rep.append(rm.get("used_memory", 0))
            rr = rep.info("replication"); rep_buf_peak = max(rep_buf_peak, rr.get("replica_full_sync_buffer_peak", 0) or 0, rm.get("mem_replica_full_sync_buffer", 0) or 0)
            if rr.get("master_link_status") == "up": t1 = time.time(); break
            if time.time() - t0 > 1800: t1 = time.time(); sync["sync_err"] = "timeout"; break
            time.sleep(0.05)
        if t_child is None: t_child = t1
        sync.update(transfer_sec=round(t_child - t0, 2), master_cob_peak_mb=max(mem_master_cob, default=0) // 2**20,
                    replica_buf_peak_mb=rep_buf_peak // 2**20, replica_mem_growth_mb=(max(mem_rep, default=rep_mem0) - rep_mem0) // 2**20,
                    sync_full=r.info("stats").get("sync_full", 0) - sync_full0)
        time.sleep(0.8)
    else:
        t0 = time.time(); r.bgsave(); t1 = wait_save(r); time.sleep(0.8)
    cpu1 = srv.cpu_ticks(); sampler.stop_ev.set(); ping.stop_ev.set(); sampler.join(); ping.join()
    if rep is not None:
        try: sync["replica_dbsize_equal"] = int(rep.r.dbsize() == r.dbsize())
        except redis.RedisError: pass
        rep.stop()
    p1 = r.info("persistence"); st = r.info("stats"); tick = os.sysconf("SC_CLK_TCK")
    row.update(save_sec=round(t1 - t0, 2), status=p1.get("rdb_last_bgsave_status"),
               fork_ms=round(st.get("latest_fork_usec", 0) / 1000, 2), cow_mb=p1.get("rdb_last_cow_size", 0) // 2**20,
               cpu_main_s=round((cpu1[0] - cpu0[0]) / tick, 2),
               cpu_threads_s=round(((cpu1[1] - cpu0[1]) - (cpu1[0] - cpu0[0])) / tick, 2),
               cpu_child_s=round(sampler.child_cpu / tick, 2),
               snap_retained_peak_mb=round(p1.get("snap_last_save_retained_peak", 0) / 2**20, 1) if "snap_last_save_retained_peak" in p1 else None,
               snap_hard_mark_aborts=p1.get("snap_hard_mark_aborts"),
               impl_stats=" ".join(f"{k}={p1[k]}" for k in IMPL_STATS if k in p1))
    row.update(sync)
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
        mhz = [x["mhz"] for x in rows if x.get("mhz")]
        row["cpu_mhz_min"] = min(mhz) if mhz else None; row["cpu_mhz_avg"] = round(statistics.fmean(mhz)) if mhz else None
        row["temp_c_max"] = max((x.get("temp", 0) for x in rows), default=None)
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
        m, ts = parse_load(path)
        if "err" in m: row["load_err"] = m["err"]; continue
        ops += m.get("ops_per_sec") or 0; avg.append(m.get("avg_ms") or 0)
        p99 = max(p99, m.get("p99.00") or 0); p999 = max(p999, m.get("p99.90") or 0); mx = max(mx, m.get("p100.00") or 0)
        if (m.get("ops_per_sec") or 0) < 0.95 * cr: missed = True
        stall = max(stall, sum(1 for s, rr in ts.items() if t0 <= t_mt + int(s) <= t1 and rr.get("Count", 0) < 0.9 * cr))
    if procs:
        row.update(ops=round(ops), load_avg_ms=round(statistics.fmean(avg), 3) if avg else None, load_p99_ms=p99, load_p999_ms=p999, load_max_ms=mx, stall_s=stall, target_missed=missed)
    return row

COLUMNS = ["name", "ts", "sha", "hybrid", "mode", "dataset", "rate", "hot", "shares", "dbsize", "used_mb", "rss_base_mb",
           "save_sec", "status", "fork_ms", "extra_mem_peak_mb", "parent_growth_peak_mb", "child_pd_peak_mb", "abs_peak_mb", "cow_mb",
           "used_mem_growth_mb", "cpu_main_s", "cpu_threads_s", "cpu_child_s", "ping_p50_ms", "ping_p99_ms", "ping_max_ms",
           "cmd_events_ge20ms", "ops", "load_avg_ms", "load_p99_ms", "load_p999_ms", "load_max_ms", "stall_s", "target_missed",
           "blocked_peak", "cpu_mhz_min", "cpu_mhz_avg", "temp_c_start", "temp_c_max", "cooled_s", "snap_retained_peak_mb", "snap_hard_mark_aborts", "impl_stats", "latency_events", "load_err",
           "transfer_sec", "master_cob_peak_mb", "replica_buf_peak_mb", "replica_mem_growth_mb", "sync_full", "replica_dbsize_equal", "sync_err"]

def cmd_run(a):
    r = redis.Redis(port=a.port); r.ping()
    new = not os.path.exists(a.csv)
    with open(a.csv, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=COLUMNS, extrasaction="ignore")
        if new: w.writeheader()
        dump = os.path.join(r.config_get("dir")["dir"], r.config_get("dbfilename")["dbfilename"])
        # The cache is keyed by the RDB format version this binary writes (an
        # older tree cannot load a newer format): learn it from an empty SAVE.
        r.flushall(); r.save(); rdbver = open(dump, "rb").read(9)[5:9].decode()
        for ds in a.datasets.split(","):
            cache = os.path.join(a.cache, f"{ds}-s{a.scale}-seed42-rdb{rdbver}.rdb") if a.cache else None
            if cache and os.path.exists(cache):
                t0 = time.time(); shutil.copyfile(cache, dump); r.execute_command("DEBUG", "RELOAD", "NOSAVE"); secs = time.time() - t0; how = "loaded from cache"
            else:
                secs = populate(a.port, ds, a.scale); how = "populated"
            mb = r.info("memory")["used_memory"] // 2**20
            print(f"[{a.name}] {ds}: {r.dbsize()} keys, {mb} MB, {how} in {secs:.0f}s", file=sys.stderr)
            t0 = time.time(); r.bgsave(); wait_save(r); est = time.time() - t0; time.sleep(2)   # untimed warm-up save
            if cache and not os.path.exists(cache) and os.path.exists(dump):
                os.makedirs(a.cache, exist_ok=True); shutil.copyfile(dump, cache)
            for rate in [int(x) for x in a.rates.split(",")]:
                for rep in range(a.reps):
                    row = run_cell(a, ds, rate, est); w.writerow(row); f.flush()
                    print(f"[{a.name}] {ds} r{rate} #{rep}: save {row['save_sec']}s fork {row['fork_ms']}ms extra_mem {row.get('extra_mem_peak_mb')}MB "
                          f"(parent +{row.get('parent_growth_peak_mb')}, child {row.get('child_pd_peak_mb')}) cow {row['cow_mb']}MB "
                          f"ping p99 {row.get('ping_p99_ms')} max {row.get('ping_max_ms')} load p99 {row.get('load_p99_ms')} max {row.get('load_max_ms')} stall {row.get('stall_s')} "
                          f"cpu {row.get('cpu_mhz_min')}-{row.get('cpu_mhz_avg')} MHz {row.get('temp_c_max')}C", file=sys.stderr)
                    time.sleep(a.cooldown)

def cmd_report(a):
    rows = list(csv.DictReader(open(a.csv)))
    def num(x):
        try: return float(x)
        except (TypeError, ValueError): return None
    groups = {}
    for r in rows: groups.setdefault((r["dataset"], r["rate"], r.get("hot", "1.0"), r["name"]), []).append(r)
    def agg(g, k):
        xs = [num(r.get(k)) for r in g]; xs = [x for x in xs if x is not None]
        if not xs: return "-"
        return f"{statistics.median(xs):g}" if len(xs) == 1 else f"{statistics.median(xs):g} [{min(xs):g}-{max(xs):g}]"
    cols = [("save_sec", "save s"), ("fork_ms", "fork ms"), ("extra_mem_peak_mb", "extra mem peak MB"), ("parent_growth_peak_mb", "parent growth MB"),
            ("child_pd_peak_mb", "child dirty MB"), ("abs_peak_mb", "abs peak MB"), ("cow_mb", "rdb_last_cow MB"),
            ("cpu_main_s", "main cpu s"), ("cpu_threads_s", "threads cpu s"), ("cpu_child_s", "child cpu s"),
            ("ping_p99_ms", "ping p99 ms"), ("ping_max_ms", "ping max ms"), ("ops", "load ops/s"), ("load_p99_ms", "load p99 ms"), ("load_max_ms", "load max ms"), ("stall_s", "stall s")]
    print("| dataset | rate | hot | config | n | " + " | ".join(c[1] for c in cols) + " |")
    print("|" + "---|" * (5 + len(cols)))
    for key in sorted(groups, key=lambda k: (k[0], int(k[1]), k[2], k[3])):
        g = groups[key]
        print(f"| {key[0]} | {key[1]} | {key[2]} | {key[3]} | {len(g)} | " + " | ".join(agg(g, c[0]) for c in cols) + " |")

def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("run"); p.add_argument("--name", required=True); p.add_argument("--port", type=int, required=True)
    p.add_argument("--datasets", default="small,mixed,big"); p.add_argument("--rates", default="0,20000"); p.add_argument("--reps", type=int, default=1)
    p.add_argument("--csv", required=True); p.add_argument("--scale", type=float, default=1.0); p.add_argument("--warm", type=float, default=3)
    p.add_argument("--cooldown", type=float, default=3); p.add_argument("--workdir", default="/tmp/snap-bench-runs"); p.add_argument("--load-cpus", default="8-15")
    p.add_argument("--server-cpus", default="0-7", help="CPUs the server is pinned to (frequency sampling)")
    p.add_argument("--cool-below", type=int, default=0, help="before each cell wait until the package temperature (C) is below this (0 = off)")
    p.add_argument("--cool-wait", type=int, default=120, help="max seconds to wait for the thermal gate")
    p.add_argument("--hot", type=float, default=1.0, help="fraction of each class's keys that receive the writes (1.0 = uniform over all keys)")
    p.add_argument("--cache", default="/dev/shm/snap-bench-runs/cache", help="dir of cached dataset RDBs ('' to disable)")
    p.add_argument("--sync-bin", default="", help="measure a full sync to a fresh replica started from this binary instead of a BGSAVE (save_sec = time to link up)")
    p.add_argument("--sync-port", type=int, default=7601)
    p.add_argument("--sync-replica-args", default="", help="extra command-line arguments for the replica")
    p.set_defaults(fn=cmd_run)
    p = sub.add_parser("report"); p.add_argument("csv"); p.set_defaults(fn=cmd_report)
    a = ap.parse_args(); a.fn(a)

if __name__ == "__main__":
    main()
