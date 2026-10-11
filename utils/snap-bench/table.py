#!/usr/bin/env python3
"""Markdown rows for RESULTS.md from one or more snap-bench CSVs.

    table.py run34.csv [run34m.csv ...] [--label up="stock upstream" ...] [--sync | --asm]

--sync prints the full-sync column set of the run 18 tables (sync time, stream
buffers on either side) instead of the BGSAVE one; --asm the slot-migration set
(migration time, snapshot phase, hand-off write pause, redirects).

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
SYNC_COLS = [("save_sec", "sync s", 1), ("fork_ms", "fork ms", 0), ("extra_mem_peak_mb", "extra MB", 0),
             ("parent_growth_peak_mb", "parent +MB", 0), ("cow_mb", "CoW MB", 0), ("master_cob_peak_mb", "primary COB MB", 0),
             ("replica_buf_peak_mb", "replica buf MB", 0), ("cpu_main_s", "main cpu s", 1), ("cpu_threads_s", "scan cpu s", 1),
             ("cpu_child_s", "child cpu s", 1), ("ops", "ops", 0), ("load_p99_ms", "wp99 ms", 2), ("load_max_ms", "wmax ms", 1),
             ("sync_full", "full syncs", 0)]
ASM_COLS = [("save_sec", "migration s", 1), ("transfer_sec", "snapshot s", 1), ("fork_ms", "fork ms", 0), ("extra_mem_peak_mb", "extra MB", 0),
            ("parent_growth_peak_mb", "parent +MB", 0), ("child_pd_peak_mb", "child dirty MB", 0), ("cow_mb", "CoW MB", 0),
            ("snap_retained_peak_mb", "retained MB", 1), ("cpu_main_s", "main cpu s", 1), ("cpu_threads_s", "scan cpu s", 1),
            ("cpu_child_s", "child cpu s", 1), ("ping_max_ms", "ping max ms", 1), ("ops", "ops", 0), ("load_p99_ms", "wp99 ms", 2),
            ("load_max_ms", "wmax ms", 1), ("asm_write_pause_ms", "pause ms", 0), ("load_redirects", "redirects", 0)]
LABELS = {"up": "stock upstream", "hybrid": "hybrid", "hybrid-off": "stock (hybrid off)", "valkey-forkless": "Valkey forkless", "base": "stock base"}

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
    files, labels, cols = [], dict(LABELS), COLS
    args = list(argv)
    while args:
        a = args.pop(0)
        if a == "--sync":
            cols = SYNC_COLS
        elif a == "--asm":
            cols = ASM_COLS
        elif a == "--label":                      # --label name=text
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
    print("| dataset | rate | config | " + " | ".join(h for _, h, _ in cols) + " |")
    print("|" + "---|" * (3 + len(cols)))
    for (ds, rate, hot, name), rows in groups.items():
        rate_s = "idle" if rate in ("0", "0.0") else f"{int(float(rate))//1000}k"
        if hot not in ("1.0", "1", ""): rate_s += f" hot {hot}"
        statuses = {r.get("status", "") for r in rows}
        mark = "" if statuses <= {"ok", ""} else " (" + ",".join(sorted(statuses)) + ")"
        statuses |= {("err: " + r["asm_err"]) for r in rows if r.get("asm_err")}
        cells = [cell([r.get(c) for r in rows], nd) if not (rate in ("0", "0.0") and c.startswith(("load_", "ops", "stall", "blocked")))
                 else ("-" if c != "blocked_peak" else cell([r.get(c) for r in rows], nd)) for c, _, nd in cols]
        print(f"| {ds} | {rate_s} | {labels.get(name, name)}{mark} | " + " | ".join(cells) + " |")

if __name__ == "__main__":
    main(sys.argv[1:])
