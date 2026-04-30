#!/usr/bin/env python3
"""bot-strategy#70 Phase A trade-level loss-concentration analyzer.

Companion to phase_a_analyze.py. Where that script summarizes the grid CSV,
this one parses per-cell BT logs to produce trade-level rows + bucket tables
(force vs natural exit, |entry_z| band, hold band, direction split) plus
single-feature filter simulation.

Usage:
  scripts/phase_a_trade_buckets.py [CELL_NUMBERS...] [--grid-dir DIR] [--fee-bps N]

Defaults to the six top-ranked ez=2.0 cells from the 2026-04-30 sweep
(032, 034, 036, 028, 038, 040) under /tmp/phase_a_grid/.
"""

import argparse
import re
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
    r"z=(?P<z>\S+)\s+beta=\S+\s+force=(?P<force>\S+)\s+"
    r"pnl=(?P<pnl>\S+)\s+ts=(?P<ts>\d+)"
)

# Grid layout from phase_a_grid.sh (must stay in sync).
ENTRY_Z_VALUES = [1.5, 2.0, 2.5, 3.0, 3.5]
FC_VALUES = [3600, 5400, 7200, 10800, 14400]
EXIT_Z_VALUES = [0.2, 0.3]
SL_Z_VALUES = [4.0, 6.0]

def cell_params(cell):
    """Map cell number (1..100) → (ez, fc, xz, sl)."""
    idx = cell - 1
    sl_i = idx % 2; idx //= 2
    xz_i = idx % 2; idx //= 2
    fc_i = idx % 5; idx //= 5
    ez_i = idx
    return (ENTRY_Z_VALUES[ez_i], FC_VALUES[fc_i],
            EXIT_Z_VALUES[xz_i], SL_Z_VALUES[sl_i])

def extract_trades(log_path, fee_bps):
    fee = Decimal(str(fee_bps / 10000.0))
    open_pos = None
    trades = []
    with open(log_path) as f:
        for line in f:
            m = ENTRY_RE.search(line)
            if m:
                open_pos = {
                    "dir": m["dir"],
                    "sa": Decimal(m["sa"]), "pa": Decimal(m["pa"]),
                    "sb": Decimal(m["sb"]), "pb": Decimal(m["pb"]),
                    "z": float(m["z"]), "ts": int(m["ts"]),
                }
                continue
            m = EXIT_RE.search(line)
            if m and open_pos:
                en = open_pos["sa"] * open_pos["pa"] + open_pos["sb"] * open_pos["pb"]
                ex = open_pos["sa"] * Decimal(m["pa"]) + open_pos["sb"] * Decimal(m["pb"])
                pnl_0 = Decimal(m["pnl"])
                trades.append({
                    "entry_ts": open_pos["ts"], "exit_ts": int(m["ts"]),
                    "dir": open_pos["dir"], "entry_z": open_pos["z"],
                    "exit_z": float(m["z"]), "hold_s": int(m["ts"]) - open_pos["ts"],
                    "force": m["force"] == "true",
                    "entry_notional": float(en),
                    "pnl_0bp": float(pnl_0),
                    "pnl_fee": float(pnl_0 - (en + ex) * fee),
                })
                open_pos = None
    return trades

def fmt_bucket(buckets, keys):
    out = []
    for k in keys:
        ts = buckets.get(k, [])
        n = len(ts)
        if n == 0:
            out.append(f"{'-':>10}")
        else:
            p_fee = sum(t["pnl_fee"] for t in ts)
            out.append(f"{p_fee:>+5.2f}/{n:<2}")
    return out

def analyze(cells, grid_dir, fee_bps):
    print(f"=== Trade-level summary (fee={fee_bps}bp) ===\n")
    hdr = (f"{'cell':>4} {'cfg':<32} {'n':>3} {'fc#':>3} {'fc%':>5} "
           f"{'pnl_0bp':>8} {'pnl_fee':>8} {'natural':>8} {'force':>8}")
    print(hdr)
    print("-" * len(hdr))
    all_trades = {}
    for cell in cells:
        ez, fc, xz, sl = cell_params(int(cell))
        log = f"{grid_dir}/cell_{int(cell):03d}.log"
        trades = extract_trades(log, fee_bps)
        all_trades[cell] = (ez, fc, xz, sl, trades)
        n = len(trades)
        fc_n = sum(1 for t in trades if t["force"])
        fc_pct = fc_n / n * 100 if n else 0
        p0 = sum(t["pnl_0bp"] for t in trades)
        p_fee = sum(t["pnl_fee"] for t in trades)
        nat = sum(t["pnl_fee"] for t in trades if not t["force"])
        frc = sum(t["pnl_fee"] for t in trades if t["force"])
        cfg = f"ez={ez} fc={fc} xz={xz} sl={sl}"
        print(f"{cell:>4} {cfg:<32} {n:>3} {fc_n:>3} {fc_pct:>4.1f}% "
              f"{p0:>+8.2f} {p_fee:>+8.2f} {nat:>+8.2f} {frc:>+8.2f}")

    def bucket(key_fn, sorted_keys):
        return [(c, defaultdict(list)) for c in [None]] and \
               {c: {k: [] for k in sorted_keys} for c in all_trades}

    # |entry_z| band
    print("\n=== |entry_z| band × cell (pnl_fee / count) ===")
    keys = ["<2.5", "2.5-3.5", "3.5-4.5", ">=4.5"]
    print(f"{'cell':>4}  " + "  ".join(f"{k:>8}" for k in keys))
    for cell, (ez, fc, xz, sl, trades) in all_trades.items():
        b = defaultdict(list)
        for t in trades:
            z = abs(t["entry_z"])
            k = ("<2.5" if z < 2.5 else "2.5-3.5" if z < 3.5
                 else "3.5-4.5" if z < 4.5 else ">=4.5")
            b[k].append(t)
        print(f"{cell:>4}  " + "  ".join(fmt_bucket(b, keys)))

    # hold band
    print("\n=== hold band × cell (pnl_fee / count) ===")
    keys = ["0-30m", "30-60m", "1-2h", "2-3h", ">=3h"]
    print(f"{'cell':>4}  " + "  ".join(f"{k:>8}" for k in keys))
    for cell, (ez, fc, xz, sl, trades) in all_trades.items():
        b = defaultdict(list)
        for t in trades:
            h = t["hold_s"]
            k = ("0-30m" if h < 1800 else "30-60m" if h < 3600
                 else "1-2h" if h < 7200 else "2-3h" if h < 10800 else ">=3h")
            b[k].append(t)
        print(f"{cell:>4}  " + "  ".join(fmt_bucket(b, keys)))

    # filter simulation
    print(f"\n=== Filter simulation across cells (pnl_fee / count) ===")
    print(f"{'cell':>4}  {'baseline':>11} {'drop_force':>11} {'drop_|z|<3':>11} "
          f"{'short_only':>11} {'natural+|z|>=3.5':>17}")
    for cell, (ez, fc, xz, sl, trades) in all_trades.items():
        def fp(pred):
            keep = [t for t in trades if pred(t)]
            return (sum(t["pnl_fee"] for t in keep), len(keep)) if keep else (0.0, 0)
        b, bn = (sum(t["pnl_fee"] for t in trades), len(trades))
        nf, nfc = fp(lambda t: not t["force"])
        nz, nzc = fp(lambda t: abs(t["entry_z"]) >= 3.0)
        ss, ssc = fp(lambda t: t["dir"] == "ShortSpread")
        nz5, nz5c = fp(lambda t: not t["force"] and abs(t["entry_z"]) >= 3.5)
        print(f"{cell:>4}  {b:>+6.2f}/{bn:<2}  {nf:>+6.2f}/{nfc:<2}    "
              f"{nz:>+6.2f}/{nzc:<2}    {ss:>+6.2f}/{ssc:<2}    {nz5:>+6.2f}/{nz5c:<2}")

    # direction split
    print(f"\n=== Direction split (pnl_fee / count) ===")
    print(f"{'cell':>4}  {'Long':>11} {'Short':>11}")
    for cell, (ez, fc, xz, sl, trades) in all_trades.items():
        L = [t for t in trades if t["dir"] == "LongSpread"]
        S = [t for t in trades if t["dir"] == "ShortSpread"]
        Lp = sum(t["pnl_fee"] for t in L)
        Sp = sum(t["pnl_fee"] for t in S)
        print(f"{cell:>4}  {Lp:>+5.2f}/{len(L):<3}  {Sp:>+5.2f}/{len(S):<2}")

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("cells", nargs="*",
                   help="Cell numbers (1..100). Default: 032 034 036 028 038 040.")
    p.add_argument("--grid-dir", default="/tmp/phase_a_grid",
                   help="Directory holding cell_NNN.log files (default: %(default)s).")
    p.add_argument("--fee-bps", type=float, default=5.0,
                   help="Fee in bps applied per leg per side (default: %(default)s).")
    args = p.parse_args()
    cells = args.cells or ["032", "034", "036", "028", "038", "040"]
    analyze(cells, args.grid_dir, args.fee_bps)
