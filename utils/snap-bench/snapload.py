#!/usr/bin/env python3
"""
Rate-limited write load for one key class, with full control of the distribution
(memtier cannot derive a random EXISTING field or member from the key).

  snapload.py --port P --seconds S --rate R --kind hash --prefix h: --count 200000 \
              --fields 10 --size 40 --mode overwrite --hot 0.2 --out result.json

Keys: <prefix><i>, i uniform in [1, count*hot]. Per kind:
  string           SET key <size random bytes>                    (same-size overwrite)
  hash overwrite   HSET key f<rand(fields)> <size>                 (existing field, filled as f0..f{fields-1})
  hash grow        HSET key g<random> <size>                       (new field)
  list             LSET key 0 <size>                               (1-element lists)
  set              SADD key <random 8 bytes>                       (new member, as memtier did)
  zset overwrite   ZINCRBY key 1 <rand(members)>                   (existing member, filled as 0..members-1)
  zset grow        ZADD key 1 <random>                             (new member)

Raw RESP over asyncio sockets, N connections, pipeline depth P, per-connection token
bucket. Latency is measured per pipelined batch (send of the batch to its last reply),
so it is coarser than a per-request client: the PING probe in bench.py is the latency
instrument, these numbers are for sanity and stall detection. Output JSON has ops/sec,
batch-latency percentiles and per-second completed counts.
"""
import argparse, asyncio, json, random, string, sys, time

ALPHA = (string.ascii_letters + string.digits).encode()

class Conn:
    def __init__(self): self.r = self.w = None
    async def open(self, port):
        self.r, self.w = await asyncio.open_connection("127.0.0.1", port); return self
    async def close(self):
        self.w.close()
        try: await self.w.wait_closed()
        except Exception: pass
    async def reply(self):
        line = await self.r.readline()
        if not line: raise ConnectionError("closed")
        c = line[:1]
        if c == b"-": raise RuntimeError(line.decode(errors="replace").strip())
        if c == b"$":
            n = int(line[1:]);  return None if n < 0 else (await self.r.readexactly(n + 2))[:-2]
        if c == b"*": return [await self.reply() for _ in range(int(line[1:]))]
        return line[1:-2]

def enc(*parts):
    out = [b"*%d\r\n" % len(parts)]
    for p in parts:
        if not isinstance(p, bytes): p = str(p).encode()
        out.append(b"$%d\r\n%s\r\n" % (len(p), p))
    return b"".join(out)

def make_cmd(a, rnd, hot_keys):
    key = f"{a.prefix}{1 + rnd.randrange(hot_keys)}".encode()
    if a.kind == "string": return enc(b"SET", key, bytes(rnd.choices(ALPHA, k=a.size)))
    if a.kind == "hash":
        field = (f"f{rnd.randrange(a.fields)}" if a.mode == "overwrite" else "g" + "".join(rnd.choices("abcdefghijklmnopqrstuvwxyz0123456789", k=10))).encode()
        return enc(b"HSET", key, field, bytes(rnd.choices(ALPHA, k=a.size)))
    if a.kind == "list": return enc(b"LSET", key, b"0", bytes(rnd.choices(ALPHA, k=a.size)))
    if a.kind == "set": return enc(b"SADD", key, bytes(rnd.choices(ALPHA, k=max(1, a.size))))
    if a.kind == "zset":
        if a.mode == "overwrite": return enc(b"ZINCRBY", key, b"1", str(rnd.randrange(a.members)).encode())
        return enc(b"ZADD", key, b"1", bytes(rnd.choices(ALPHA, k=16)))
    raise ValueError(a.kind)

async def worker(a, ident, rate, stats, start, stop_at):
    c = await Conn().open(a.port)
    rnd = random.Random(f"{a.seed}:{a.prefix}:{ident}")
    hot_keys = max(1, int(a.count * a.hot))
    tokens = 0.0; last = time.monotonic()
    try:
        while True:
            now = time.monotonic()
            if now >= stop_at: break
            tokens = min(tokens + (now - last) * rate, a.pipeline * 4.0); last = now
            n = min(a.pipeline, int(tokens))
            if n < 1:
                await asyncio.sleep(min(0.005, (1 - tokens) / rate)); continue
            tokens -= n
            batch = b"".join(make_cmd(a, rnd, hot_keys) for _ in range(n))
            t0 = time.monotonic()
            c.w.write(batch); await c.w.drain()
            for _ in range(n): await c.reply()
            t1 = time.monotonic()
            stats["lat"].append((t1 - t0) * 1000.0)
            stats["completed"] += n
            sec = int(t1 - start); stats["ts"][sec] = stats["ts"].get(sec, 0) + n
    finally:
        await c.close()

async def main_async(a):
    stats = {"lat": [], "completed": 0, "ts": {}, "errors": []}
    start = time.monotonic(); stop_at = start + a.seconds
    per_conn = a.rate / a.conns
    tasks = [asyncio.create_task(worker(a, i, per_conn, stats, start, stop_at)) for i in range(a.conns)]
    res = await asyncio.gather(*tasks, return_exceptions=True)
    stats["errors"] = [repr(x) for x in res if isinstance(x, BaseException)]
    secs = time.monotonic() - start
    lat = sorted(stats["lat"])
    def pct(p):
        if not lat: return None
        return round(lat[min(len(lat) - 1, int(round(p / 100 * (len(lat) - 1))))], 3)
    out = {"ops_per_sec": stats["completed"] / secs, "completed": stats["completed"], "seconds": secs,
           "avg_ms": (sum(lat) / len(lat)) if lat else None, "p50.00": pct(50), "p99.00": pct(99), "p99.90": pct(99.9), "p100.00": pct(100),
           "batches": len(lat), "pipeline": a.pipeline, "conns": a.conns, "hot": a.hot, "kind": a.kind, "mode": a.mode,
           "target_rate": a.rate, "ts": {str(k): {"Count": v} for k, v in sorted(stats["ts"].items())}, "errors": stats["errors"][:5]}
    with open(a.out, "w") as f: json.dump(out, f)
    print(f"{a.prefix} {a.kind}/{a.mode} hot={a.hot}: {out['ops_per_sec']:.0f} ops/s (target {a.rate}), batch p99 {out['p99.00']} ms, errors {len(stats['errors'])}", file=sys.stderr)
    return 0 if not stats["errors"] else 1

def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--port", type=int, required=True); ap.add_argument("--seconds", type=float, required=True); ap.add_argument("--rate", type=float, required=True)
    ap.add_argument("--kind", required=True, choices=["string", "hash", "list", "set", "zset"]); ap.add_argument("--prefix", required=True)
    ap.add_argument("--count", type=int, required=True); ap.add_argument("--fields", type=int, default=1); ap.add_argument("--members", type=int, default=1)
    ap.add_argument("--size", type=int, default=8); ap.add_argument("--mode", default="overwrite", choices=["overwrite", "grow"])
    ap.add_argument("--hot", type=float, default=1.0, help="fraction of the keys that receive the writes (first N keys)")
    ap.add_argument("--conns", type=int, default=4); ap.add_argument("--pipeline", type=int, default=16); ap.add_argument("--seed", default="42")
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    sys.exit(asyncio.run(main_async(a)))

if __name__ == "__main__":
    main()
