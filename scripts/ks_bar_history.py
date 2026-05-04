#!/usr/bin/env python3
"""
Cross-host bar-history KS distance check (bot-strategy#276).

Compares two pairtrade snapshot JSONs (e.g. Frankfurt vs Tokyo) and reports
the Kolmogorov-Smirnov 2-sample distance per price symbol and per pair
spread.

Acceptance criteria (#276):
  Phase 1 (ms-precision exchange_ts): KS <= 0.05
  Phase 2 (push-based BarBuilder):    KS ~ 0 (numerical equality)

Usage:
  scripts/ks_bar_history.py <snapshot_a.json> <snapshot_b.json>
  scripts/ks_bar_history.py --strict <a> <b>     # require Phase 2 (~0)

Snapshot fetch (deploy-time):
  scp debot:/opt/debot/pairtrade_history_BTC_ETH.json /tmp/fra.json
  scp debot-tokyo:/opt/debot/pairtrade_history_BTC_ETH.json /tmp/tyo.json
  scripts/ks_bar_history.py /tmp/fra.json /tmp/tyo.json

Exit code: 0 on PASS, 1 on FAIL, 2 on input error.
"""
import argparse
import json
import sys
from pathlib import Path

PHASE1_THRESHOLD = 0.05
PHASE2_THRESHOLD = 0.001


def load_snapshot(path):
    with open(path) as f:
        snap = json.load(f)
    v = snap.get("_v", 1)
    prices = snap.get("prices", {})
    spreads = snap.get("spread_histories", {})
    # Normalize ts to ms regardless of snapshot version.
    # v1/v2 stored ts in seconds; v3+ in ms.
    if v < 3:
        prices = {sym: [(lp, ts * 1000) for (lp, ts) in series]
                  for sym, series in prices.items()}
    else:
        prices = {sym: [(lp, ts) for (lp, ts) in series]
                  for sym, series in prices.items()}
    return v, prices, spreads


def ks_2samp(a, b):
    """KS 2-sample distance. Pure-python, no numpy."""
    if not a or not b:
        return float("nan")
    a = sorted(a)
    b = sorted(b)
    n1, n2 = len(a), len(b)
    i = j = 0
    d = 0.0
    while i < n1 and j < n2:
        x1, x2 = a[i], b[j]
        if x1 <= x2:
            i += 1
        if x2 <= x1:
            j += 1
        d = max(d, abs(i / n1 - j / n2))
    return d


def align_by_ts(series_a, series_b):
    """Inner join by ts. Returns (values_a, values_b, n_aligned)."""
    map_a = {ts: lp for (lp, ts) in series_a}
    map_b = {ts: lp for (lp, ts) in series_b}
    common = sorted(map_a.keys() & map_b.keys())
    return [map_a[t] for t in common], [map_b[t] for t in common], len(common)


def tail_align(a, b):
    """Spread series carries no ts; align by trailing N entries."""
    n = min(len(a), len(b))
    return a[-n:], b[-n:], n


def fmt_verdict(d, threshold):
    if d != d:  # NaN
        return "N/A"
    return "PASS" if d <= threshold else "FAIL"


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("snap_a", type=Path)
    ap.add_argument("snap_b", type=Path)
    ap.add_argument("--strict", action="store_true",
                    help="Use Phase 2 threshold (~0) instead of Phase 1 (0.05)")
    ap.add_argument("--label-a", default="A")
    ap.add_argument("--label-b", default="B")
    args = ap.parse_args()

    if not args.snap_a.exists() or not args.snap_b.exists():
        print(f"ERROR: snapshot file missing", file=sys.stderr)
        return 2

    threshold = PHASE2_THRESHOLD if args.strict else PHASE1_THRESHOLD
    phase = "Phase 2 (~0)" if args.strict else "Phase 1 (<= 0.05)"

    v_a, prices_a, spreads_a = load_snapshot(args.snap_a)
    v_b, prices_b, spreads_b = load_snapshot(args.snap_b)

    print(f"== Snapshots ==")
    print(f"  {args.label_a}: {args.snap_a}  (_v={v_a})")
    print(f"  {args.label_b}: {args.snap_b}  (_v={v_b})")
    print(f"  threshold: {threshold}  ({phase})")
    print()

    failed = False

    print(f"== Per-symbol log_price KS (ts-aligned) ==")
    print(f"{'symbol':<8} {'n_aligned':>10} {'KS':>10}  verdict")
    syms = sorted(set(prices_a) | set(prices_b))
    for sym in syms:
        sa = prices_a.get(sym, [])
        sb = prices_b.get(sym, [])
        va, vb, n = align_by_ts(sa, sb)
        d = ks_2samp(va, vb)
        v = fmt_verdict(d, threshold)
        print(f"{sym:<8} {n:>10} {d:>10.4f}  {v}")
        if v == "FAIL":
            failed = True

    print()
    print(f"== Per-pair spread KS (tail-aligned) ==")
    print(f"{'pair':<10} {'n_aligned':>10} {'KS':>10}  verdict")
    pairs = sorted(set(spreads_a) | set(spreads_b))
    for pair in pairs:
        va, vb, n = tail_align(spreads_a.get(pair, []), spreads_b.get(pair, []))
        d = ks_2samp(va, vb)
        v = fmt_verdict(d, threshold)
        print(f"{pair:<10} {n:>10} {d:>10.4f}  {v}")
        if v == "FAIL":
            failed = True

    print()
    if failed:
        print(f"OVERALL: FAIL ({phase})")
        return 1
    print(f"OVERALL: PASS ({phase})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
