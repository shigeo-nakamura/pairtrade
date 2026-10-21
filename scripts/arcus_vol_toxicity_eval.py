#!/usr/bin/env python3
"""Evaluate the pre-registered toxicity gate rules (bot-strategy#1093).

Joins `toxicity_*.jsonl` fill rows (features at the fill's venue time) with
`fills.jsonl` `markout` rows (5/30/60 s, primary = 60 s) and the log's own
`markout_tox` rows (1/10/60/300 s vs own mid and reference mid), then scores
the rule grid frozen in `deploy/arcus_vol/TOXICITY_PREREG.md`.

    arcus_vol_toxicity_eval.py --state DIR [--state DIR ...] --split 2026-11-04 --oos-end 2026-11-18 --out OUTDIR
    arcus_vol_toxicity_eval.py ... --oos --open-oos     # the ONE out-of-sample look

`--state DIR` is an instance state dir holding `toxicity_*.jsonl` and
`fills.jsonl` (several instances = several markets). IS = fills before the
split date (UTC), OOS = from it up to (excluding) `--oos-end`, the
pre-registered end; later fills are never scored. The OOS run needs `--open-oos`, which writes
`OUTDIR/OOS_OPENED.json` and is refused when that file already exists. No
model is fitted: the grid is the gate family; the IS run picks one rule, the
OOS run scores only that rule (`OUTDIR/is_selected.json`).
Stdlib only.
"""
import argparse
import glob
import hashlib
import json
import math
import os
import statistics
import sys
from datetime import datetime, timezone

GRID_BP = [1.0, 2.0, 3.0, 5.0]
GRID_SECS = [60, 300, 600]
GRID_FLOW = [5_000.0, 25_000.0, 100_000.0]
MIN_KEEP = 0.5
MIN_T = 2.0
MIN_DELTA_OOS = 1.0
MIN_DAYS = 5
MIN_IS_FILLS = 150  # PREREG: fewer → extend the IS window by whole weeks (addendum), no selection
MIN_FILLS_PER_DAY_GROUP = 3


def utc_day(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")


def load_state(d):
    """Return (fills: {fill_id: row}, markouts: {fill_id: {h: bps}}, tox_markouts: {fill_id: {h: (mid, ref)}})."""
    fills, mk, tmk = {}, {}, {}
    for f in sorted(glob.glob(os.path.join(d, "toxicity_*.jsonl"))):
        with open(f) as fh:
            for line in fh:
                try:
                    r = json.loads(line)
                except json.JSONDecodeError:
                    continue
                k = r.get("kind")
                if k == "fill" and r.get("fill_id"):
                    fills[r["fill_id"]] = r
                elif k == "markout_tox" and r.get("fill_id"):
                    tmk.setdefault(r["fill_id"], {})[int(r["horizon_s"])] = (
                        r.get("markout_mid_bps"),
                        r.get("markout_ref_bps"),
                    )
    p = os.path.join(d, "fills.jsonl")
    if os.path.exists(p):
        with open(p) as fh:
            for line in fh:
                try:
                    r = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if r.get("kind") == "markout" and r.get("fill_id"):
                    try:
                        mk.setdefault(r["fill_id"], {})[int(r["horizon_s"])] = float(r["bps"])
                    except (TypeError, ValueError):
                        pass
    return fills, mk, tmk


def rules():
    """The frozen gate family: (id, label, predicate(fill_row) -> bool|None)."""
    out = []

    def ge(key, x):
        def f(r):
            v = r.get(key)
            return None if v is None else v >= x

        return f

    for x in GRID_BP:
        out.append((f"R1_jump5_{x:g}", f"ref_jump_5s >= {x:g} bp", ge("ref_jump_5s", x)))
    for x in GRID_BP:
        out.append((f"R2_jump1_{x:g}", f"ref_jump_1s >= {x:g} bp", ge("ref_jump_1s", x)))
    for x in GRID_BP:

        def against(r, x=x):
            v = r.get("ref_ret_5s")
            if v is None:
                return None
            return v <= -x if r.get("side") == "buy" else v >= x

        out.append((f"R3_against5_{x:g}", f"ref moved >= {x:g} bp against the quote in 5 s", against))
    for s in GRID_SECS:

        def near(r, s=s):
            a, b = r.get("secs_to_next"), r.get("secs_since_prev")
            if a is None and b is None:
                return None
            return (a is not None and a <= s) or (b is not None and b <= s)

        out.append((f"R4_boundary_{s}", f"within {s} s of a session boundary", near))
    for f_ in GRID_FLOW:

        def flow(r, f_=f_):
            v = r.get("arc_flow_5s")
            if v is None:
                return None
            # taker buys hit our ask (sell fill); taker sells hit our bid (buy fill)
            return v >= f_ if r.get("side") == "sell" else v <= -f_

        out.append((f"R5_flow5_{f_:g}", f"taker flow >= ${f_:g} into the quote in 5 s", flow))
    return out


def score(rows, pred, outcome):
    """rows: list of (day, fill_row, outcome_bps). Returns dict or None."""
    allv = [o for _, _, o in rows]
    if not allv:
        return None
    kept = [(d, o) for d, r, o in rows if not pred(r)]
    if not kept:
        return None
    mean_all = statistics.fmean(allv)
    mean_kept = statistics.fmean(o for _, o in kept)
    by_day_all, by_day_kept = {}, {}
    for d, r, o in rows:
        by_day_all.setdefault(d, []).append(o)
        if not pred(r):
            by_day_kept.setdefault(d, []).append(o)
    deltas = []
    for d, a in by_day_all.items():
        k = by_day_kept.get(d, [])
        flagged = len(a) - len(k)
        if len(k) >= MIN_FILLS_PER_DAY_GROUP and flagged >= MIN_FILLS_PER_DAY_GROUP:
            deltas.append(statistics.fmean(k) - statistics.fmean(a))
    t = None
    if len(deltas) >= MIN_DAYS and len(deltas) > 1:
        sd = statistics.stdev(deltas)
        t = (statistics.fmean(deltas) / (sd / math.sqrt(len(deltas)))) if sd > 0 else None
    return {
        "n_all": len(allv),
        "n_kept": len(kept),
        "keep": len(kept) / len(allv),
        "mean_all_bp": mean_all,
        "mean_kept_bp": mean_kept,
        "delta_bp": mean_kept - mean_all,
        "n_days": len(deltas),
        "t": t,
        "outcome": outcome,
    }


def join(fills, mk, tmk, outcome, horizon):
    """Rows (day, fill_row, outcome) for maker fills with that outcome present."""
    rows = []
    for fid, r in fills.items():
        if r.get("role") != "maker":
            continue
        if outcome == "markout60":
            o = mk.get(fid, {}).get(horizon)
        elif outcome == "markout_mid":
            o = tmk.get(fid, {}).get(horizon, (None, None))[0]
        else:
            o = tmk.get(fid, {}).get(horizon, (None, None))[1]
        if o is None:
            continue
        rows.append((utc_day(int(r["t"])), r, float(o)))
    return rows


def passes_is(s):
    return s and s["keep"] >= MIN_KEEP and s["t"] is not None and s["t"] >= MIN_T


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--state", action="append", required=True, help="instance state dir (repeatable)")
    ap.add_argument("--split", required=True, help="OOS start date, UTC (YYYY-MM-DD)")
    ap.add_argument("--oos-end", required=True, help="OOS end date, UTC, exclusive (YYYY-MM-DD), as pre-registered")
    ap.add_argument("--out", required=True, help="output dir")
    ap.add_argument("--oos", action="store_true", help="score the IS-selected rule on OOS (one look)")
    ap.add_argument("--open-oos", action="store_true", help="required with --oos; writes OOS_OPENED.json once")
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)
    split_ms = int(datetime.strptime(a.split, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000)
    end_ms = int(datetime.strptime(a.oos_end, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000)
    if end_ms <= split_ms:
        print("--oos-end must be after --split", file=sys.stderr)
        return 2

    fills, mk, tmk = {}, {}, {}
    for d in a.state:
        f, m, t = load_state(d)
        fills.update(f)
        mk.update(m)
        tmk.update(t)
    is_fills = {k: v for k, v in fills.items() if int(v["t"]) < split_ms}
    oos_fills = {k: v for k, v in fills.items() if split_ms <= int(v["t"]) < end_ms}
    print(f"fills: {len(fills)} total, {len(is_fills)} IS, {len(oos_fills)} OOS (to {a.oos_end}); markout60 rows {sum(1 for v in mk.values() if 60 in v)}")

    if not a.oos:
        rows = join(is_fills, mk, tmk, "markout60", 60)
        if len(rows) < MIN_IS_FILLS:
            print(f"IS has {len(rows)} maker fills with a 60 s markout (< {MIN_IS_FILLS}): underpowered — extend the IS window by whole weeks (PREREG addendum) before selecting", file=sys.stderr)
            with open(os.path.join(a.out, "is_selected.json"), "w") as fh:
                json.dump({"split": a.split, "oos_end": a.oos_end, "selected": None, "verdict": f"underpowered ({len(rows)} < {MIN_IS_FILLS} IS fills)"}, fh, indent=1)
            return 2
        table = []
        for rid, label, pred in rules():
            s = score(rows, pred, "markout60")
            if s:
                s.update(id=rid, label=label)
                table.append(s)
        table.sort(key=lambda s: -s["delta_bp"])
        for s in table:
            tt = "-" if s["t"] is None else f"{s['t']:.2f}"
            print(f"{s['id']:<22} keep {s['keep']:.2f}  n {s['n_kept']}/{s['n_all']}  Δ {s['delta_bp']:+.2f} bp  t {tt}  days {s['n_days']}")
        eligible = [s for s in table if passes_is(s)]
        sel = eligible[0] if eligible else None
        with open(os.path.join(a.out, "is_table.json"), "w") as fh:
            json.dump(table, fh, indent=1)
        with open(os.path.join(a.out, "is_selected.json"), "w") as fh:
            json.dump({"split": a.split, "oos_end": a.oos_end, "n_is": len(rows), "selected": sel, "verdict": "KILL (no rule passes IS)" if sel is None else "candidate"}, fh, indent=1)
        # side tables (no gate): other outcomes, per market
        side = {}
        for outcome, h in [("markout_mid", 1), ("markout_mid", 10), ("markout_mid", 300), ("markout_ref", 60)]:
            r2 = join(is_fills, mk, tmk, outcome, h)
            side[f"{outcome}_{h}"] = {s[0]: s[1] for s in ((rid, score(r2, pred, outcome)) for rid, _, pred in rules()) if s[1]}
        with open(os.path.join(a.out, "is_side_tables.json"), "w") as fh:
            json.dump(side, fh, indent=1)
        print("IS selected:", sel["id"] if sel else "none → KILL")
        return 0

    marker = os.path.join(a.out, "OOS_OPENED.json")
    sel_path = os.path.join(a.out, "is_selected.json")
    if not os.path.exists(sel_path):
        print("run the IS evaluation first (is_selected.json missing)", file=sys.stderr)
        return 2
    selj = json.load(open(sel_path))
    sel = selj["selected"]
    if selj.get("split") != a.split or selj.get("oos_end") != a.oos_end:
        print(f"IS selection was made for split {selj.get('split')} / end {selj.get('oos_end')}: pass the same dates", file=sys.stderr)
        return 2
    if sel is None:
        print("IS selected no rule: KILL; OOS is not opened", file=sys.stderr)
        return 2
    if os.path.exists(marker):
        print(f"OOS already opened: {marker} — one look only", file=sys.stderr)
        return 2
    if not a.open_oos:
        print("OOS needs --open-oos (one look)", file=sys.stderr)
        return 2
    inputs = sorted(p for d in a.state for p in glob.glob(os.path.join(d, "toxicity_*.jsonl")) + [os.path.join(d, "fills.jsonl")] if os.path.exists(p))
    h = hashlib.sha256()
    for p in inputs:
        h.update(open(p, "rb").read())
    with open(marker, "w") as fh:
        json.dump({"opened_at": datetime.now(timezone.utc).isoformat(), "split": a.split, "oos_end": a.oos_end, "rule": sel["id"], "inputs_sha256": h.hexdigest(), "inputs": inputs}, fh, indent=1)
    pred = next(p for rid, _, p in rules() if rid == sel["id"])
    rows = join(oos_fills, mk, tmk, "markout60", 60)
    s = score(rows, pred, "markout60")
    verdict = "KILL"
    if s and s["delta_bp"] >= MIN_DELTA_OOS and s["t"] is not None and s["t"] >= MIN_T and s["keep"] >= MIN_KEEP:
        verdict = "PASS"
    out = {"rule": sel["id"], "oos": s, "verdict": verdict}
    with open(os.path.join(a.out, "oos_verdict.json"), "w") as fh:
        json.dump(out, fh, indent=1)
    print(json.dumps(out, indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main())
