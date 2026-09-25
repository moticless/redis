#!/usr/bin/env python3
"""
Where does the fork child's copy-on-write come from, besides big values?

Runs the 'mixed' dataset with write traffic on the SMALL classes only (strings, lists, sets; the
9 KB hashes are left alone), so every dirtied page is a parent-side mechanism other than big-value
CoW: client buffers, replication backlog / replica output blocks, AOF buffer, expiry structures,
jemalloc metadata, dict entries... Variants add a replica, AOF, or many pipelined clients.

  cow-sources.py --binary /tmp/redis-hyb/src/redis-server [--hybrid yes|no] [--rate 20000] [--seconds 12]
                 [--variants base,replica,aof,clients] [--reps 1]

Prints, per variant: child Private_Dirty peak, rdb_last_cow_size, parent RssAnon growth, and the INFO
memory fields that name the suspects (mem_clients_normal, mem_replication_backlog, mem_aof_buffer,
mem_clients_slaves). Server on cores 0-7, load on 8-15 (same as run.sh); dump dir on /dev/shm.
"""
import argparse, os, subprocess, sys, time
import redis
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import bench

D = "/dev/shm/snap-bench-runs/cow-sources"
CLI = None

def start(binary, port, name, extra):
    os.makedirs(D, exist_ok=True)
    cmd = ["taskset", "-c", "0-7", binary, "--port", str(port), "--bind", "127.0.0.1", "--save", "", "--appendonly", "no",
           "--activedefrag", "no", "--disable-thp", "yes", "--io-threads", "1", "--daemonize", "no", "--enable-debug-command", "yes",
           "--dir", D, "--dbfilename", f"{name}.rdb", "--appenddirname", f"{name}-aof", "--logfile", f"{D}/{name}.log",
           "--repl-diskless-sync", "yes", "--repl-diskless-sync-delay", "0"] + extra
    p = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    r = redis.Redis(port=port, socket_timeout=600)
    for _ in range(100):
        try:
            r.ping(); return p, r
        except redis.RedisError: time.sleep(0.1)
    raise RuntimeError("server did not start")

def stop(p, r):
    try: r.shutdown(nosave=True)
    except redis.RedisError: pass
    p.wait(timeout=30)

def load(port, cls, rate, secs, hot, conns, out):
    prefix, count, kind, params, mode, dsize = cls
    c = ["taskset", "-c", "8-15", sys.executable, bench.SNAPLOAD, "--port", str(port), "--seconds", str(secs), "--rate", str(rate),
         "--kind", kind, "--prefix", prefix, "--count", str(count), "--fields", str(params.get("fields", 1)), "--members", str(params.get("members", 1)),
         "--size", str(dsize), "--mode", mode, "--hot", str(hot), "--conns", str(conns), "--pipeline", "16", "--out", out]
    return subprocess.Popen(c, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

def run_variant(a, variant):
    extra = ["--bgsave-hybrid", a.hybrid]
    p, r = start(a.binary, a.port, f"cow-{variant}", extra)
    rep = None
    try:
        bench.populate(a.port, "mixed", 1.0)
        classes = [c for c in bench.scaled("mixed", 1.0) if c[0] != "bh:"]      # traffic on small classes only
        conns = 64 if variant == "clients" else 4
        if variant == "aof":
            r.config_set("appendonly", "yes")
            while r.info("persistence")["aof_rewrite_in_progress"]: time.sleep(0.1)
            time.sleep(1)
        if variant == "replica":
            rp, rr = start(a.binary, a.port + 1, "cow-replica", ["--bgsave-hybrid", a.hybrid])
            rr.replicaof("127.0.0.1", a.port)
            for _ in range(600):
                if rr.info("replication")["master_link_status"] == "up": break
                time.sleep(0.1)
            time.sleep(1); rep = (rp, rr)
        # untimed warm-up save, then the measured one under traffic
        r.bgsave(); bench.wait_save(r); time.sleep(1)
        pid = r.info("server")["process_id"]; srv = bench.Server(pid)
        weights = bench.class_weights(r, classes); tot = sum(weights)
        procs = [load(a.port, c, max(100, round(a.rate * w / tot)), a.seconds, 1.0, conns, f"{D}/load-{variant}-{c[0].strip(':')}.json") for c, w in zip(classes, weights)]
        time.sleep(3)
        st0 = srv.status(); m0 = r.info("memory")
        sampler = bench.Sampler(srv, a.port, cpus=range(0, 8)); sampler.start()
        r.bgsave(); t1 = bench.wait_save(r); time.sleep(0.5)
        sampler.stop_ev.set(); sampler.join()
        for q in procs: q.wait(timeout=a.seconds + 30)
        p1 = r.info("persistence"); m1 = r.info("memory")
        child_pd = max((x["child_pd_mb"] for x in sampler.rows), default=0)
        parent_growth = max((x["rss_anon_mb"] + x["swap_mb"] for x in sampler.rows), default=0) - (st0.get("RssAnon", 0) + st0.get("VmSwap", 0))
        fields = {k: m1.get(k, 0) for k in ("mem_clients_normal", "mem_clients_slaves", "mem_replication_backlog", "mem_aof_buffer", "used_memory")}
        print(f"{variant:8s} hybrid={a.hybrid}: child Private_Dirty peak {child_pd} MB, rdb_last_cow {p1.get('rdb_last_cow_size', 0) // 2**20} MB, "
              f"parent growth {parent_growth} MB, save {p1.get('rdb_last_bgsave_time_sec')} s | clients {fields['mem_clients_normal'] // 1024} KB, "
              f"slaves {fields['mem_clients_slaves'] // 1024} KB, backlog {fields['mem_replication_backlog'] // 1024} KB, aof_buf {fields['mem_aof_buffer'] // 1024} KB, "
              f"used {fields['used_memory'] // 2**20} MB")
    finally:
        if rep: stop(*rep)
        stop(p, r)

def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--binary", default="/tmp/redis-hyb/src/redis-server"); ap.add_argument("--hybrid", default="yes")
    ap.add_argument("--port", type=int, default=7610); ap.add_argument("--rate", type=int, default=20000); ap.add_argument("--seconds", type=int, default=12)
    ap.add_argument("--variants", default="base,replica,aof,clients"); ap.add_argument("--reps", type=int, default=1)
    a = ap.parse_args()
    for v in a.variants.split(","):
        for _ in range(a.reps): run_variant(a, v)

if __name__ == "__main__":
    main()
