#!/usr/bin/env python3
"""bot-strategy#70 spread-std entry filter study + robustness validation.

Hypothesis from the loss-concentration analysis: entries during high-spread-std
regimes might escape the 5 bps fee tail because the absolute spread move is
larger relative to the fee. Tests this on the same six top ez=2.0 cells, then
runs four robustness checks before drawing any conclusion.

Quartile cutoffs are derived from the reference cell (036 by default) and
applied across the cell set.

Outputs:
  - BTC/ETH realized vol quartile × cell (sanity, expected to be flat)
  - spread_std quartile × cell (the candidate signal)
  - filter sims: ss_top50, ss_top25, ss_top10
  - leave-one-out / leave-two-out stability of Q4 sum
  - top-2 winner concentration per cell
  - per-quartile median (does median ever turn positive?)
  - window split (does the signal hold across temporal halves?)

Usage:
  scripts/phase_a_vol_filter.py [CELL_NUMBERS...]
      [--grid-dir DIR] [--dump-glob PATTERN] [--ref-cell CELL] [--fee-bps N]

Defaults to the six top-ranked ez=2.0 cells from the 2026-04-30 sweep
(032, 034, 036, 028, 038, 040) under /tmp/phase_a_grid/, with dumps under
/tmp/bt/.
"""

import argparse
import glob
import json
import math
import re
import statistics
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal

ENTRY_RE = re.compile(
    r"\[ENTRY\]\s+pair=\S+\s+direction=(?P<dir>\S+)\s+"
    r"size_a=(?P<sa>\S+)\s+price_a=(?P<pa>\S+)\s+"
    r"size_b=(?P<sb>\S+)\s+price_b=(?P<pb>\S+)\s+"
    r"z=(?P<z>\S+).*?ts=(?P<ts>\d+)"
)
EXIT_RE = re.compile(
    r"\[EXIT\]\s+pair=\S+\s+direction=(?P<dir>\S+)\s+"
    r"size_a=(?P<sa>\S+)\s+price_a=(?P<pa>\S+)\s+"
    r"size_b=(?P<sb>\S+)\s+price_b=(?P<pb>\S+)\s+"
    r"z=\S+\s+beta=\S+\s+force=(?P<force>\S+)\s+"
    r"pnl=(?P<pnl>\S+)\s+ts=(?P<ts>\d+)"
)
ZCHECK_RE = re.compile(
    r"\[ZCHECK\].*?bucket_ts=(\d+).*?std=(\S+).*?z=([-+]?\d+\.\d+)"
)
WINDOW_SEC = 1800  # 30 min realized vol window

def find_first_ge(arr, ts, key=lambda x: x[0]):
    lo, hi = 0, len(arr)
    while lo < hi:
        mid = (lo + hi) // 2
        if key(arr[mid]) < ts:
            lo = mid + 1
        else:
            hi = mid
    return lo

def realized_vol(ticks, end_ts, window):
    start_idx = find_first_ge(ticks, end_ts - window)
    end_idx = find_first_ge(ticks, end_ts)
    if end_idx - start_idx < 5:
        return None, None
    btc_logs = []
    eth_logs = []
    for i in range(start_idx + 1, end_idx):
        prev_btc = ticks[i-1][1]; prev_eth = ticks[i-1][2]
        cur_btc = ticks[i][1]; cur_eth = ticks[i][2]
        if prev_btc > 0 and cur_btc > 0:
            btc_logs.append(math.log(cur_btc / prev_btc))
        if prev_eth > 0 and cur_eth > 0:
            eth_logs.append(math.log(cur_eth / prev_eth))
    def std(xs):
        if len(xs) < 2: return None
        m = sum(xs) / len(xs)
        v = sum((x - m) ** 2 for x in xs) / (len(xs) - 1)
        return math.sqrt(v)
    return std(btc_logs), std(eth_logs)

def load_dump(dump_glob):
    ticks = []
    for path in sorted(glob.glob(dump_glob)):
        with open(path) as f:
            for line in f:
                try:
                    d = json.loads(line)
                    ts = d["timestamp"] / 1000.0
                    btc = float(d["prices"]["BTC"]["price"])
                    eth = float(d["prices"]["ETH"]["price"])
                    ticks.append((ts, btc, eth))
                except (KeyError, ValueError):
                    continue
    ticks.sort(key=lambda x: x[0])
    return ticks

def extract_trades(log_path, ticks, fee):
    open_pos = None
    trades = []
    zcheck_history = []
    with open(log_path) as f:
        for line in f:
            m = ZCHECK_RE.search(line)
            if m:
                zcheck_history.append((int(m.group(1)), float(m.group(2)), float(m.group(3))))
                continue
            m = ENTRY_RE.search(line)
            if m:
                ts = int(m["ts"])
                vb, ve = realized_vol(ticks, ts, WINDOW_SEC)
                idx = find_first_ge(zcheck_history, ts) - 1
                spread_std = zcheck_history[idx][1] if idx >= 0 else None
                open_pos = {
                    "dir": m["dir"], "z": float(m["z"]),
                    "sa": Decimal(m["sa"]), "pa": Decimal(m["pa"]),
                    "sb": Decimal(m["sb"]), "pb": Decimal(m["pb"]),
                    "ts": ts, "vol_btc": vb, "vol_eth": ve, "spread_std": spread_std,
                }
                continue
            m = EXIT_RE.search(line)
            if m and open_pos:
                en = open_pos["sa"] * open_pos["pa"] + open_pos["sb"] * open_pos["pb"]
                ex = open_pos["sa"] * Decimal(m["pa"]) + open_pos["sb"] * Decimal(m["pb"])
                pnl_0 = Decimal(m["pnl"])
                trades.append({
                    **open_pos,
                    "exit_ts": int(m["ts"]),
                    "hold_s": int(m["ts"]) - open_pos["ts"],
                    "force": m["force"] == "true",
                    "pnl_0bp": float(pnl_0),
                    "pnl_fee": float(pnl_0 - (en + ex) * fee),
                })
                open_pos = None
    return trades

def quartile_cuts(xs):
    if not xs: return [0, 0, 0]
    n = len(xs)
    return [xs[n//4], xs[n//2], xs[3*n//4]]

def label(v, cuts):
    if v is None: return "n/a"
    if v < cuts[0]: return "Q1"
    if v < cuts[1]: return "Q2"
    if v < cuts[2]: return "Q3"
    return "Q4"

def fmt(p, n):
    return f"{p:>+5.2f}/{n:<2}"

def top_n_by_ss(trades, n):
    return sorted([t for t in trades if t["spread_std"] is not None],
                  key=lambda t: -t["spread_std"])[:n]

def topk_by_frac(trades, frac):
    n = max(1, int(len(trades) * frac))
    top = top_n_by_ss(trades, n)
    return sum(t["pnl_fee"] for t in top), len(top)

def analyze(cells, grid_dir, dump_glob, ref_cell, fee_bps):
    fee = Decimal(str(fee_bps / 10000.0))
    print("Loading dump files...")
    ticks = load_dump(dump_glob)
    print(f"  {len(ticks)} ticks loaded")

    print("Extracting trades + computing vol per cell...")
    all_trades = {}
    for cell in cells:
        log = f"{grid_dir}/cell_{int(cell):03d}.log"
        trades = extract_trades(log, ticks, fee)
        all_trades[cell] = trades
        n_ok = sum(1 for t in trades if t["vol_btc"] is not None)
        print(f"  cell {cell}: {len(trades)} trades, {n_ok} with vol metric")

    if ref_cell not in all_trades:
        ref_cell = next(iter(all_trades))
    ref = all_trades[ref_cell]
    btc_q = quartile_cuts(sorted(t["vol_btc"] for t in ref if t["vol_btc"] is not None))
    eth_q = quartile_cuts(sorted(t["vol_eth"] for t in ref if t["vol_eth"] is not None))
    ss_q = quartile_cuts(sorted(t["spread_std"] for t in ref if t["spread_std"] is not None))

    print(f"\nQuartile cutoffs (ref cell {ref_cell}, n={sum(1 for t in ref if t['spread_std'] is not None)}):")
    print(f"  BTC vol(30m):     q25={btc_q[0]:.5f}  q50={btc_q[1]:.5f}  q75={btc_q[2]:.5f}")
    print(f"  ETH vol(30m):     q25={eth_q[0]:.5f}  q50={eth_q[1]:.5f}  q75={eth_q[2]:.5f}")
    print(f"  spread_std (240): q25={ss_q[0]:.4f}  q50={ss_q[1]:.4f}  q75={ss_q[2]:.4f}")

    def bucket_table(title, key_fn):
        print(f"\n=== {title} ===")
        keys = ["Q1", "Q2", "Q3", "Q4"]
        print(f"{'cell':>4}  " + "  ".join(f"{k:>11}" for k in keys))
        for cell, trades in all_trades.items():
            b = defaultdict(list)
            for t in trades:
                b[key_fn(t)].append(t)
            row = []
            for k in keys:
                ts_ = b.get(k, [])
                row.append(fmt(sum(t["pnl_fee"] for t in ts_), len(ts_)) if ts_ else f"{'-':>9}")
            print(f"{cell:>4}  " + "  ".join(f"{v:>11}" for v in row))

    bucket_table("BTC vol quartile × cell (pnl_fee/n)", lambda t: label(t["vol_btc"], btc_q))
    bucket_table("ETH vol quartile × cell (pnl_fee/n)", lambda t: label(t["vol_eth"], eth_q))
    bucket_table("spread_std quartile × cell (pnl_fee/n)",
                 lambda t: label(t["spread_std"], ss_q))

    # Filter sims
    print("\n=== Top-K by spread_std filter sim (sum/n) ===")
    print(f"{'cell':>4}  {'baseline':>11}  {'top50':>11}  {'top30':>11}  {'top20':>11}  {'top10':>11}")
    for cell, trades in all_trades.items():
        b = sum(t["pnl_fee"] for t in trades), len(trades)
        rows = [topk_by_frac(trades, f) for f in [0.50, 0.30, 0.20, 0.10]]
        print(f"{cell:>4}  {fmt(*b):>11}  " + "  ".join(f"{fmt(*r):>11}" for r in rows))

    # ROBUSTNESS: leave-N-out on Q4 (top-25%)
    print("\n=== Robustness 1: Q4 (top-25%) leave-N-out stability ===")
    print(f"  {'cell':>4}  {'n_q4':>4}  {'sum':>7}  {'mean':>7}  "
          f"{'lo1_min':>8}  {'lo2_min':>8}")
    for cell, trades in all_trades.items():
        n_q4 = max(1, len(trades) // 4)
        q4 = top_n_by_ss(trades, n_q4)
        pnls = [t["pnl_fee"] for t in q4]
        s = sum(pnls)
        m = s / len(pnls)
        lo1 = [s - p for p in pnls]
        lo2 = [s - pnls[i] - pnls[j] for i in range(len(pnls)) for j in range(i+1, len(pnls))]
        print(f"  {cell:>4}  {len(q4):>4}  {s:>+7.2f}  {m:>+7.2f}  "
              f"{min(lo1):>+8.2f}  {min(lo2):>+8.2f}")

    # ROBUSTNESS: top-2 winner concentration
    print("\n=== Robustness 2: top-2 winners concentration (Q4) ===")
    for cell, trades in all_trades.items():
        n_q4 = max(1, len(trades) // 4)
        q4 = sorted(top_n_by_ss(trades, n_q4), key=lambda t: -t["pnl_fee"])
        s = sum(t["pnl_fee"] for t in q4)
        top2_sum = sum(t["pnl_fee"] for t in q4[:2])
        rest = q4[2:]
        rest_sum = sum(t["pnl_fee"] for t in rest)
        print(f"  cell {cell}: Q4 sum={s:+.2f}, top2={top2_sum:+.2f}, "
              f"rest({len(rest)})={rest_sum:+.2f}")

    # ROBUSTNESS: median by quartile
    print("\n=== Robustness 3: median pnl_fee by spread_std quartile ===")
    print(f"  {'cell':>4}  {'q':>2}  {'n':>3}  {'sum':>7}  {'median':>7}  {'win%':>5}")
    for cell, trades in all_trades.items():
        by_q = defaultdict(list)
        for t in trades:
            by_q[label(t["spread_std"], ss_q)].append(t)
        for q in ["Q1", "Q2", "Q3", "Q4"]:
            sub = by_q.get(q, [])
            if not sub: continue
            pnls = [t["pnl_fee"] for t in sub]
            print(f"  {cell:>4}  {q:>2}  {len(sub):>3}  {sum(pnls):>+7.2f}  "
                  f"{statistics.median(pnls):>+7.2f}  "
                  f"{sum(1 for p in pnls if p>0)/len(pnls)*100:>4.1f}%")

    # ROBUSTNESS: window split (uses ref cell's midpoint timestamp)
    print(f"\n=== Robustness 4: window split (mid-trade of ref cell {ref_cell}) ===")
    ref_sorted = sorted(all_trades[ref_cell], key=lambda t: t["ts"])
    midpoint_ts = ref_sorted[len(ref_sorted) // 2]["ts"]
    mid_dt = datetime.fromtimestamp(midpoint_ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
    print(f"  Midpoint: {mid_dt}")
    print(f"  {'cell':>4}  {'half':>5}  {'n':>3}  {'baseline':>11}  {'ss_top50':>11}  {'ss_top25':>11}")
    for cell, trades in all_trades.items():
        ts_list = sorted(trades, key=lambda t: t["ts"])
        for half_name, half in [("early", [t for t in ts_list if t["ts"] < midpoint_ts]),
                                 ("late", [t for t in ts_list if t["ts"] >= midpoint_ts])]:
            half_ss = sorted(t["spread_std"] for t in half if t["spread_std"] is not None)
            if len(half_ss) < 4:
                continue
            q50 = half_ss[len(half_ss)//2]
            q75 = half_ss[3*len(half_ss)//4]
            base = sum(t["pnl_fee"] for t in half), len(half)
            top50 = [t for t in half if t["spread_std"] is not None and t["spread_std"] >= q50]
            top25 = [t for t in half if t["spread_std"] is not None and t["spread_std"] >= q75]
            s50 = sum(t["pnl_fee"] for t in top50), len(top50)
            s25 = sum(t["pnl_fee"] for t in top25), len(top25)
            print(f"  {cell:>4}  {half_name:>5}  {len(half):>3}  {fmt(*base):>11}  "
                  f"{fmt(*s50):>11}  {fmt(*s25):>11}")

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("cells", nargs="*",
                   help="Cell numbers (1..100). Default: 032 034 036 028 038 040.")
    p.add_argument("--grid-dir", default="/tmp/phase_a_grid",
                   help="Directory holding cell_NNN.log files (default: %(default)s).")
    p.add_argument("--dump-glob", default="/tmp/bt/market_data_btceth_*.jsonl",
                   help="Glob for dump files used to compute realized vol "
                        "(default: %(default)s).")
    p.add_argument("--ref-cell", default="036",
                   help="Reference cell whose trade set defines the quartile "
                        "cutoffs (default: %(default)s).")
    p.add_argument("--fee-bps", type=float, default=5.0,
                   help="Fee in bps applied per leg per side (default: %(default)s).")
    args = p.parse_args()
    cells = args.cells or ["032", "034", "036", "028", "038", "040"]
    analyze(cells, args.grid_dir, args.dump_glob, args.ref_cell, args.fee_bps)
