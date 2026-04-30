#!/usr/bin/env python3
"""bot-strategy#70 Phase B precursor: force-close hold-extension diagnostic.

For each force_close trade in a BT cell log, computes hypothetical pnl@fee
under different post-force hold extensions H ∈ {60, 180, 360, 720, 1440} m,
plus the omniscient best price within a 12 h window.

Method (price-based, NOT z-based): the bot recalibrates beta after force_close
so post-force-close ZCHECK z reflects new beta and is invalid for "what would
the held position have done" reasoning. Mid prices reconstructed from
ZCHECK close_a/close_b (log) are unaffected by that recalibration.

Usage:
  scripts/phase_a_hold_extension.py [CELL_NUMBERS...] [--grid-dir DIR] [--fee-bps N]

Defaults to the six top-ranked ez=2.0 cells from the 2026-04-30 sweep
(032, 034, 036, 028, 038, 040) under /tmp/phase_a_grid/.

Findings on the 2026-04-12..29 window (per #70 comment):
  - Static dynamic fc (always extend by H min) consistently worsens vs force_close
    for the top ez=2.0 cells. Confirms the static fc grid sweep result.
  - "Best within 12 h" carries large upside ($28-129 across cells) but is
    omniscient — only realisable with a recovery indicator that doesn't exist yet.
  - Implication: dynamic fc is not a quick win. Smart variant needs feature
    engineering + BT engine extension. Lower priority than Extended (#123)
    or pair diversification (#8).
"""

import argparse
import math
import re
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
ZCHECK_PRICES_RE = re.compile(
    r"\[ZCHECK\].*?bucket_ts=(\d+).*?close_a=(\S+)\s+close_b=(\S+)"
)

ENTRY_Z_VALUES = [1.5, 2.0, 2.5, 3.0, 3.5]
FC_VALUES = [3600, 5400, 7200, 10800, 14400]
EXIT_Z_VALUES = [0.2, 0.3]
SL_Z_VALUES = [4.0, 6.0]
HOLD_EXTENSIONS_MIN = [60, 180, 360, 720, 1440]
BEST_WINDOW_MIN = 720

def cell_params(cell):
    idx = cell - 1
    sl_i = idx % 2; idx //= 2
    xz_i = idx % 2; idx //= 2
    fc_i = idx % 5; idx //= 5
    ez_i = idx
    return (ENTRY_Z_VALUES[ez_i], FC_VALUES[fc_i],
            EXIT_Z_VALUES[xz_i], SL_Z_VALUES[sl_i])

def parse(log_path):
    open_pos = None
    fc_trades = []
    prices = []
    with open(log_path) as f:
        for line in f:
            m = ZCHECK_PRICES_RE.search(line)
            if m:
                prices.append((int(m.group(1)),
                               math.exp(float(m.group(2))),
                               math.exp(float(m.group(3)))))
                continue
            m = ENTRY_RE.search(line)
            if m:
                open_pos = {
                    "dir": m["dir"], "entry_z": float(m["z"]),
                    "sa": Decimal(m["sa"]), "pa": Decimal(m["pa"]),
                    "sb": Decimal(m["sb"]), "pb": Decimal(m["pb"]),
                    "ts": int(m["ts"]),
                }
                continue
            m = EXIT_RE.search(line)
            if m and open_pos:
                if m["force"] == "true":
                    fc_trades.append({
                        **open_pos,
                        "force_exit_ts": int(m["ts"]),
                        "force_exit_pa": Decimal(m["pa"]),
                        "force_exit_pb": Decimal(m["pb"]),
                        "force_pnl_0bp": float(Decimal(m["pnl"])),
                    })
                open_pos = None
    prices.sort(key=lambda x: x[0])
    return fc_trades, prices

def find_strict_after(prices, ts):
    lo, hi = 0, len(prices)
    while lo < hi:
        mid = (lo + hi) // 2
        if prices[mid][0] <= ts:
            lo = mid + 1
        else:
            hi = mid
    return lo

def pnl_at(trade, exit_pa, exit_pb, fee):
    pa = Decimal(str(exit_pa))
    pb = Decimal(str(exit_pb))
    if trade["dir"] == "LongSpread":
        pnl_a = (pa - trade["pa"]) * trade["sa"]
        pnl_b = (trade["pb"] - pb) * trade["sb"]
    else:
        pnl_a = (trade["pa"] - pa) * trade["sa"]
        pnl_b = (pb - trade["pb"]) * trade["sb"]
    pnl_0 = pnl_a + pnl_b
    en = trade["pa"] * trade["sa"] + trade["pb"] * trade["sb"]
    ex = pa * trade["sa"] + pb * trade["sb"]
    return float(pnl_0), float(pnl_0 - (en + ex) * fee)

def best_in_window(trade, prices, start_idx, end_ts, fee):
    best = None
    best_ts = None
    for i in range(start_idx, len(prices)):
        ts, pa, pb = prices[i]
        if ts > end_ts:
            break
        _, p_fee = pnl_at(trade, pa, pb, fee)
        if best is None or p_fee > best:
            best = p_fee
            best_ts = ts
    return best_ts, best

def analyze(cells, grid_dir, fee_bps):
    fee = Decimal(str(fee_bps / 10000.0))
    print(f"=== Force-close hold-extension diagnostic (price-based, fee={fee_bps}bp) ===\n")
    print(f"For each cell: pnl summed over force_close trades, evaluated at force_exit and at +H min after.\n")
    hdr = (f"{'cell':>4} {'cfg':<25} {'fc_n':>4} {'force':>8} "
           + " ".join(f"+{h:>4}m" for h in HOLD_EXTENSIONS_MIN)
           + f"  {'best/12h':>9}")
    print(hdr)
    print("-" * len(hdr))

    for cell in cells:
        ez, fc, xz, sl = cell_params(int(cell))
        log = f"{grid_dir}/cell_{int(cell):03d}.log"
        fc_trades, prices = parse(log)
        n = len(fc_trades)
        base_sum = 0.0
        held_sums = {h: 0.0 for h in HOLD_EXTENSIONS_MIN}
        best_sum = 0.0
        for t in fc_trades:
            _, fp = pnl_at(t, float(t["force_exit_pa"]), float(t["force_exit_pb"]), fee)
            base_sum += fp
            idx = find_strict_after(prices, t["force_exit_ts"])
            for h in HOLD_EXTENSIONS_MIN:
                target_ts = t["force_exit_ts"] + h * 60
                j = find_strict_after(prices, target_ts) - 1
                if j < idx:
                    held_sums[h] += fp
                else:
                    _, p = pnl_at(t, prices[j][1], prices[j][2], fee)
                    held_sums[h] += p
            _, bp = best_in_window(t, prices, idx,
                                   t["force_exit_ts"] + BEST_WINDOW_MIN * 60, fee)
            best_sum += bp if bp is not None else fp
        cfg = f"ez={ez} fc={fc} xz={xz}"
        held_str = " ".join(f"{held_sums[h]:>+6.2f}" for h in HOLD_EXTENSIONS_MIN)
        print(f"{cell:>4} {cfg:<25} {n:>4} {base_sum:>+8.2f}  {held_str}  {best_sum:>+9.2f}")

    if len(cells) == 1:
        cell = cells[0]
        log = f"{grid_dir}/cell_{int(cell):03d}.log"
        fc_trades, prices = parse(log)
        print(f"\n--- Cell {cell} per-trade hold extension (pnl_fee) ---")
        cols = " ".join(f"+{h}m" for h in HOLD_EXTENSIONS_MIN)
        print(f"  {'#':>2}  date_utc          dir         |z|en  fc_pnl  {cols}  best12h  best_dt_min")
        for i, t in enumerate(fc_trades, 1):
            _, fp = pnl_at(t, float(t["force_exit_pa"]), float(t["force_exit_pb"]), fee)
            idx = find_strict_after(prices, t["force_exit_ts"])
            cells_p = []
            for h in HOLD_EXTENSIONS_MIN:
                target_ts = t["force_exit_ts"] + h * 60
                j = find_strict_after(prices, target_ts) - 1
                if j < idx:
                    cells_p.append(fp)
                else:
                    _, p = pnl_at(t, prices[j][1], prices[j][2], fee)
                    cells_p.append(p)
            bts, bp = best_in_window(t, prices, idx,
                                     t["force_exit_ts"] + BEST_WINDOW_MIN * 60, fee)
            bp = bp if bp is not None else fp
            bdt = (bts - t["force_exit_ts"]) / 60.0 if bts else 0.0
            dts = datetime.fromtimestamp(t["ts"], tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
            print(f"  {i:>2}  {dts}  {t['dir']:<11}  {abs(t['entry_z']):>4.2f}   "
                  f"{fp:>+5.2f} " + " ".join(f"{p:>+5.2f}" for p in cells_p)
                  + f"   {bp:>+5.2f}    {bdt:>5.0f}")

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("cells", nargs="*",
                   help="Cell numbers (1..100). Default: 032 034 036 028 038 040. "
                        "Pass a single cell to also see per-trade detail.")
    p.add_argument("--grid-dir", default="/tmp/phase_a_grid",
                   help="Directory holding cell_NNN.log files (default: %(default)s).")
    p.add_argument("--fee-bps", type=float, default=5.0,
                   help="Fee in bps applied per leg per side (default: %(default)s).")
    args = p.parse_args()
    cells = args.cells or ["032", "034", "036", "028", "038", "040"]
    analyze(cells, args.grid_dir, args.fee_bps)
