#!/usr/bin/env python3
"""bot-strategy#70 Phase A grid analyzer.

Reads results.csv from phase_a_grid.sh and emits:
  - Top-N configs by net_pnl @ 5bps (filter: trades >= MIN_TRADES)
  - Per-dim sensitivity (mean pnl_5bp marginalised over other dims)
  - Baseline comparison (live A: entry_z=1.5 fc=7200 exit=0.2 sl=4.0)

Phase A is for directional signal, not final choice — see issue body.
"""

import csv
import sys
from collections import defaultdict
from statistics import mean

CSV_PATH = sys.argv[1] if len(sys.argv) > 1 else "/tmp/phase_a_grid/results.csv"
TOP_N = int(sys.argv[2]) if len(sys.argv) > 2 else 5
MIN_TRADES = 10

with open(CSV_PATH) as f:
    rows = list(csv.DictReader(f))

for r in rows:
    for k in ("entry_z", "exit_z", "sl_z", "pnl_0bp", "pnl_5bp",
              "win_rate_5bp", "sharpe_5bp", "maxdd_5bp", "calmar_5bp", "fc_rate"):
        r[k] = float(r[k])
    for k in ("fc_secs", "trades"):
        r[k] = int(r[k])

eligible = [r for r in rows if r["trades"] >= MIN_TRADES]
print(f"=== Phase A grid: {len(rows)} cells, {len(eligible)} eligible (trades>={MIN_TRADES}) ===\n")

# Top-N by pnl_5bp.
print(f"--- Top {TOP_N} by net_pnl @ 5bps ---")
hdr = "rank cell  ez   fc    xz   sl   pnl_5bp  pnl_0bp  trades  win%  sharpe  maxdd  calmar  fc%"
print(hdr)
top = sorted(eligible, key=lambda r: r["pnl_5bp"], reverse=True)[:TOP_N]
for i, r in enumerate(top, 1):
    print(f"{i:4d} {r['cell']} {r['entry_z']:.1f}  {r['fc_secs']:>5}  {r['exit_z']:.1f}  {r['sl_z']:.1f}  "
          f"{r['pnl_5bp']:>+7.2f}  {r['pnl_0bp']:>+7.2f}  {r['trades']:>4}  "
          f"{r['win_rate_5bp']:>4.1f}  {r['sharpe_5bp']:>+5.2f}  {r['maxdd_5bp']:>5.2f}  "
          f"{r['calmar_5bp']:>+6.2f}  {r['fc_rate']*100:>4.1f}")

# Sensitivity: mean pnl_5bp varying one dim (others marginalised) over eligible only.
def sens_table(field, label):
    buckets = defaultdict(list)
    for r in eligible:
        buckets[r[field]].append(r["pnl_5bp"])
    print(f"\n--- Sensitivity: {label} (mean pnl_5bp over eligible cells) ---")
    for v in sorted(buckets.keys()):
        vals = buckets[v]
        ranking = "best" if v == max(buckets, key=lambda k: mean(buckets[k])) else ""
        print(f"  {label}={v}: n={len(vals):>3}  mean={mean(vals):>+8.2f}  "
              f"min={min(vals):>+7.2f}  max={max(vals):>+7.2f}  {ranking}")

sens_table("entry_z", "entry_z")
sens_table("fc_secs", "fc_secs")
sens_table("exit_z", "exit_z")
sens_table("sl_z", "sl_z")

# Baseline comparison.
baseline = next((r for r in rows
                 if r["entry_z"] == 1.5 and r["fc_secs"] == 7200
                 and r["exit_z"] == 0.2 and r["sl_z"] == 4.0), None)
if baseline:
    print(f"\n--- Baseline (live Bot A): cell {baseline['cell']} ---")
    print(f"  pnl_5bp={baseline['pnl_5bp']:+.2f}  trades={baseline['trades']}  "
          f"win={baseline['win_rate_5bp']:.1f}%  calmar={baseline['calmar_5bp']:+.2f}  "
          f"fc%={baseline['fc_rate']*100:.1f}")
    if top:
        delta = top[0]["pnl_5bp"] - baseline["pnl_5bp"]
        print(f"  Best beats baseline by ${delta:+.2f}")

# Verdict heuristic: any cell positive at 5bps?
positive = [r for r in eligible if r["pnl_5bp"] > 0]
print(f"\n--- Verdict ---")
print(f"  Cells with pnl_5bp > 0: {len(positive)} / {len(eligible)} eligible")
if positive:
    print("  → Phase A signal: at least one config flips fee_5bp positive on this window.")
else:
    print("  → Phase A signal: no config in this grid flips fee_5bp positive on this window.")
    print("    Per #70 fallback: parameter tuning alone is insufficient for this pair/fee combo;")
    print("    next moves are entry filters / fee-competitive DEX / pair diversification.")
