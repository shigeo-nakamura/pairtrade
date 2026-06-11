#!/usr/bin/env python3
"""bot-strategy#373 P1: out-of-sample revalidation of the defensive regime
filters (rv_eth_4h drop_low, beta_drift drop_high) on a byte-exact window
fully after the #283 execution-accounting fix (live 2026-05-11).

Inputs: a single-variant byte-exact BT work dir (bt.log + combined.jsonl)
produced by bt_live_data.sh with live-A params.

Method (per #373 P1 spec):
  - walk-forward threshold selection: K time folds; for each fold the
    drop-quantile q is chosen on the other folds (max kept-PnL), then
    applied to the fold. No look-ahead.
  - report total PnL, MaxDD, kept trade count, loss-tail recall,
    natural-win kill rate, leave-N-out sensitivity, per-fold thresholds.

Loss tail definition: pnl_0bp <= -$3.00 (= -30 bps on the $1000
equity_usd_reference used by the BT cell), matching the #19/#315 framing.
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from phase_a_cluster_features import (  # noqa: E402
    add_market_features, load_ticks, parse_cell,
)

LOSS_TAIL_USD = -3.0
Q_CANDIDATES = [0.10, 0.20, 0.25, 0.33]


def max_dd(pnls):
    peak = run = dd = 0.0
    for p in pnls:
        run += p
        peak = max(peak, run)
        dd = max(dd, peak - run)
    return dd


def summarize(rows):
    pnls = [r["pnl_0bp"] for r in rows]
    return {
        "n": len(rows),
        "pnl": sum(pnls),
        "maxdd": max_dd(pnls),
        "wins": sum(1 for p in pnls if p > 0),
        "tail": sum(1 for p in pnls if p <= LOSS_TAIL_USD),
    }


def apply_filter(rows, key, q_or_thr, drop_low, threshold=None):
    """Return (kept, dropped). Threshold from quantile q on `rows` unless an
    explicit threshold is given (walk-forward: threshold comes from train)."""
    valid = [r for r in rows if r.get(key) is not None]
    if threshold is None:
        ordered = sorted(v[key] for v in valid)
        idx = int(len(ordered) * q_or_thr) if drop_low else int(len(ordered) * (1 - q_or_thr))
        idx = min(max(idx, 0), len(ordered) - 1)
        threshold = ordered[idx]
    if drop_low:
        kept = [r for r in valid if r[key] >= threshold]
    else:
        kept = [r for r in valid if r[key] <= threshold]
    dropped = [r for r in valid if r not in kept]
    return kept, dropped, threshold


def walk_forward(rows, key, drop_low, n_folds=4):
    rows = sorted((r for r in rows if r.get(key) is not None),
                  key=lambda r: r["entry_ts"])
    t0, t1 = rows[0]["entry_ts"], rows[-1]["entry_ts"] + 1
    span = (t1 - t0) / n_folds
    folds = [[r for r in rows if t0 + i * span <= r["entry_ts"] < t0 + (i + 1) * span]
             for i in range(n_folds)]
    kept_all, dropped_all, fold_info = [], [], []
    for i, test in enumerate(folds):
        train = [r for j, f in enumerate(folds) if j != i for r in f]
        if not test or len(train) < 8:
            kept_all += test
            fold_info.append({"fold": i, "q": None, "thr": None, "n_test": len(test)})
            continue
        best_q, best_pnl, best_thr = None, -1e18, None
        for q in Q_CANDIDATES:
            kept, _, thr = apply_filter(train, key, q, drop_low)
            pnl = sum(r["pnl_0bp"] for r in kept)
            if pnl > best_pnl:
                best_q, best_pnl, best_thr = q, pnl, thr
        kept, dropped, _ = apply_filter(test, key, None, drop_low, threshold=best_thr)
        kept_all += kept
        dropped_all += dropped
        fold_info.append({"fold": i, "q": best_q, "thr": best_thr,
                          "n_test": len(test), "n_dropped": len(dropped)})
    return kept_all, dropped_all, fold_info


def leave_n_out(base, kept, dropped, n):
    """Remove the n most favorable drops (biggest avoided losses) and ask
    whether the remaining lift is still positive."""
    drops_sorted = sorted(dropped, key=lambda r: r["pnl_0bp"])
    readded = drops_sorted[:n]
    kept_adj = kept + readded
    return sum(r["pnl_0bp"] for r in kept_adj) - base["pnl"]


def report(name, rows, key, drop_low):
    base = summarize(rows)
    kept, dropped, folds = walk_forward(rows, key, drop_low)
    filt = summarize(kept)
    print(f"\n=== {name} ({'drop_low' if drop_low else 'drop_high'}, walk-forward 4-fold) ===")
    print(f"baseline : n={base['n']}  pnl=${base['pnl']:.2f}  maxdd=${base['maxdd']:.2f}  "
          f"wins={base['wins']}  tail(<=-$3)={base['tail']}")
    print(f"filtered : n={filt['n']}  pnl=${filt['pnl']:.2f}  maxdd=${filt['maxdd']:.2f}  "
          f"wins={filt['wins']}  tail={filt['tail']}")
    print(f"lift     : ${filt['pnl'] - base['pnl']:+.2f}")
    tail_dropped = sum(1 for r in dropped if r["pnl_0bp"] <= LOSS_TAIL_USD)
    win_dropped = sum(1 for r in dropped if r["pnl_0bp"] > 0)
    tail_total = base["tail"]
    wins_total = base["wins"]
    print(f"loss-tail recall      : {tail_dropped}/{tail_total}")
    print(f"natural-win kill rate : {win_dropped}/{wins_total}")
    for n in (1, 2):
        print(f"leave-{n}-out lift     : ${leave_n_out(base, kept, dropped, n):+.2f}")
    print("fold thresholds       :",
          [(f['fold'], f['q'], round(f['thr'], 6) if f['thr'] is not None else None)
           for f in folds])
    print("dropped trades        :",
          [(r['entry_ts'], round(r['pnl_0bp'], 2)) for r in sorted(dropped, key=lambda x: x['entry_ts'])])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("workdir", help="BT work dir (bt.log + combined.jsonl)")
    args = ap.parse_args()
    wd = Path(args.workdir)

    rows = parse_cell(wd / "bt.log")
    # dedupe identical (entry_ts, exit_ts) rows (defensive; single-variant
    # logs are clean but variant configs double-log)
    seen, uniq = set(), []
    for r in rows:
        k = (r["entry_ts"], r["exit_ts"])
        if k not in seen:
            seen.add(k)
            uniq.append(r)
    rows = uniq
    ticks = load_ticks(str(wd / "combined.jsonl"))
    ts_keys = [t[0] for t in ticks]
    add_market_features(rows, ticks, ts_keys)

    miss_rv = sum(1 for r in rows if r.get("rv_eth_4h") is None)
    miss_bd = sum(1 for r in rows if r.get("beta_drift") is None)
    print(f"trades={len(rows)}  missing rv_eth_4h={miss_rv}  missing beta_drift={miss_bd}")

    report("rv_eth_4h", rows, "rv_eth_4h", drop_low=True)
    report("beta_drift", rows, "beta_drift", drop_low=False)


if __name__ == "__main__":
    main()
