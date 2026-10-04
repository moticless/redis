#!/usr/bin/env python3
"""Markdown rows for RESULTS.md from one or more snap-bench CSVs.

    table.py run34.csv [run34m.csv ...] [--label up="stock upstream" ...]

Groups rows by (dataset, rate, hot, name) in CSV order and prints one row per
group with the reps as "a / b" (one value when the reps agree within 5%).
Columns match the run 31 tables.
"""
import csv, sys
from collections import OrderedDict

COLS = [("save_sec", "save s", 1), ("fork_ms", "fork ms", 0), ("extra_mem_peak_mb", "extra MB", 0),
        ("cow_mb", "CoW MB", 0), ("cpu_main_s", "main cpu s", 1), ("cpu_threads_s", "threads cpu s", 1),
        ("cpu_child_s", "child cpu s", 1), ("ping_max_ms", "ping max ms", 1), ("load_p99_ms", "write p99 ms", 1),
        ("load_max_ms", "write max ms", 0), ("ops", "achieved/s", 0), ("stall_s", "stall s", 1), ("blocked_peak", "blocked", 0)]
LABELS = {"up": "stock upstream", "hybrid": "hybrid", "valkey-forkless": "Valkey forkless", "base": "stock base"}

def fmt(v, nd):
    try:
        f = float(v)
    except (TypeError, ValueError):
        return "-"
    return f"{f:.{nd}f}" if nd else f"{f:.0f}"

def cell(vals, nd):
    nums = []
    for v in vals:
        try: nums.append(float(v))
        except (TypeError, ValueError): return "-"
    if not nums: return "-"
    if len(nums) == 1 or (max(nums) - min(nums)) <= 0.05 * max(abs(max(nums)), 1e-9):
        return fmt(sum(nums) / len(nums), nd)
    return " / ".join(fmt(n, nd) for n in nums)

def main(argv):
    files, labels = [], dict(LABELS)
    args = list(argv)
    while args:
        a = args.pop(0)
        if a == "--label":                      # --label name=text
            k, v = args.pop(0).split("=", 1)
            labels[k.strip()] = v
        elif a.startswith("--label="):           # --label=name=text
            k, v = a[len("--label="):].split("=", 1)
            labels[k.strip()] = v
        elif a.startswith("--label "):           # one argument "--label name=text"
            k, v = a[len("--label "):].split("=", 1)
            labels[k.strip()] = v
        else:
            files.append(a)
    groups = OrderedDict()
    for fn in files:
        with open(fn) as f:
            for r in csv.DictReader(f):
                key = (r["dataset"], r["rate"], r.get("hot", "1.0"), r["name"])
                groups.setdefault(key, []).append(r)
    print("| dataset | rate | config | " + " | ".join(h for _, h, _ in COLS) + " |")
    print("|" + "---|" * (3 + len(COLS)))
    for (ds, rate, hot, name), rows in groups.items():
        rate_s = "idle" if rate in ("0", "0.0") else f"{int(float(rate))//1000}k"
        if hot not in ("1.0", "1", ""): rate_s += f" hot {hot}"
        statuses = {r.get("status", "") for r in rows}
        mark = "" if statuses <= {"ok", ""} else " (" + ",".join(sorted(statuses)) + ")"
        cells = [cell([r.get(c) for r in rows], nd) if not (rate in ("0", "0.0") and c.startswith(("load_", "ops", "stall", "blocked")))
                 else ("-" if c != "blocked_peak" else cell([r.get(c) for r in rows], nd)) for c, _, nd in COLS]
        print(f"| {ds} | {rate_s} | {labels.get(name, name)}{mark} | " + " | ".join(cells) + " |")

if __name__ == "__main__":
    main(sys.argv[1:])
