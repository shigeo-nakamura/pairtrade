#!/usr/bin/env python3
"""bot-strategy#70 cluster-level entry-feature pre-work for Phase B.

Companion to phase_a_vol_filter.py. While that script tests one feature
(spread_std) on a fixed six-cell set, this one ranks a wider feature menu
on byte-exact cells and cross-checks against live-cluster boundaries.

Phase B (~2026-05-30, gated on bot-strategy#255 30d S3 archive) plans to
re-run the 100-cell grid byte-exact. This script's job is to short-list
entry-time observables to compute on that 30d sweep so the regime question
posed in #70 (within-window regime shifts) can be tackled in one pass.

Features computed at each ENTRY ts:
  rv_btc / rv_eth (30m, 4h)   — realized log-return std
  corr (1h, 4h, 24h)           — Pearson corr of BTC/ETH log-returns
  fr_btc / fr_eth / fr_diff    — funding rates from dump
  spread_std                   — bot's own 240-bucket rolling std (ZCHECK)
  beta_eff / beta_drift        — beta_eff_now and (now - 1h_ago) / |1h_ago|
  hour_utc / weekday           — calendar buckets

For each feature: drop-bottom-quartile and drop-top-quartile filter sims,
quartile medians, leave-1-out / leave-2-out worst case. Plus, if a live
trade JSONL is provided, classify each BT entry by the live-cluster window
it falls in (LOSS / WIN / NONE) and report mean feature value per bucket.

Caveat: on a 5.5d window the byte-exact harness yields ~15 trades per
top cell. Even strongly positive filter results here die under leave-2-out.
Use this as a candidate ranking for the 30d run, not a deployment signal.

Usage:
  scripts/phase_a_cluster_features.py [CELL_NUMBERS...]
      [--grid-dir DIR] [--dump-glob PATTERN]
      [--live-pnl-glob PATTERN] [--start-utc TS] [--end-utc TS]
      [--fee-bps N]

Defaults: cells 010 012 (top byte-exact survivors from the 5.5d run); grid
dir /tmp/phase_a_grid_be/; dumps /tmp/bt/market_data_btceth_*.jsonl.
"""
import argparse, csv, glob, json, math, re, statistics
from bisect import bisect_left
from datetime import datetime, timezone

ENTRY_RE = re.compile(
    r"\[ENTRY\]\s+pair=\S+\s+direction=(?P<dir>\S+)\s+"
    r"size_a=\S+\s+price_a=\S+\s+size_b=\S+\s+price_b=\S+\s+"
    r"z=(?P<z>\S+).*?ts=(?P<ts>\d+)"
)
EXIT_RE = re.compile(
    r"\[EXIT\]\s+.*?force=(?P<force>\S+)\s+pnl=(?P<pnl>\S+)\s+ts=(?P<ts>\d+)"
)
ZCHECK_RE = re.compile(
    r"\[ZCHECK\]\s+\S+\s+bucket_ts=(?P<bts>\d+).*?"
    r"beta_eff=(?P<be>\S+).*?std=(?P<std>\S+)"
)


def load_ticks(dump_glob):
    ticks = []
    for f in sorted(glob.glob(dump_glob)):
        with open(f) as fh:
            for line in fh:
                try:
                    r = json.loads(line)
                except Exception:
                    continue
                ts = r["timestamp"] // 1000
                p = r["prices"]
                try:
                    ticks.append((ts,
                                  float(p["BTC"]["price"]),
                                  float(p["ETH"]["price"]),
                                  float(p["BTC"]["funding_rate"]),
                                  float(p["ETH"]["funding_rate"])))
                except Exception:
                    continue
    ticks.sort(key=lambda x: x[0])
    return ticks


def realized_vol_log(ticks, ts_keys, end_ts, window_sec):
    end_i = bisect_left(ts_keys, end_ts)
    end_i = min(end_i, len(ts_keys) - 1)
    start_i = bisect_left(ts_keys, end_ts - window_sec)
    if end_i - start_i < 5:
        return None, None
    btc_logs, eth_logs = [], []
    for k in range(start_i + 1, end_i + 1):
        pa, pb = ticks[k - 1][1], ticks[k][1]
        ea, eb = ticks[k - 1][2], ticks[k][2]
        if pa > 0 and pb > 0:
            btc_logs.append(math.log(pb / pa))
        if ea > 0 and eb > 0:
            eth_logs.append(math.log(eb / ea))
    def std(xs):
        if len(xs) < 2: return None
        m = sum(xs) / len(xs)
        v = sum((x - m) ** 2 for x in xs) / (len(xs) - 1)
        return math.sqrt(v)
    return std(btc_logs), std(eth_logs)


def corr(ticks, ts_keys, end_ts, window_sec):
    end_i = bisect_left(ts_keys, end_ts)
    end_i = min(end_i, len(ts_keys) - 1)
    start_i = bisect_left(ts_keys, end_ts - window_sec)
    if end_i - start_i < 10:
        return None
    btc_logs, eth_logs = [], []
    for k in range(start_i + 1, end_i + 1):
        pa, pb = ticks[k - 1][1], ticks[k][1]
        ea, eb = ticks[k - 1][2], ticks[k][2]
        if pa > 0 and pb > 0 and ea > 0 and eb > 0:
            btc_logs.append(math.log(pb / pa))
            eth_logs.append(math.log(eb / ea))
    n = len(btc_logs)
    if n < 10: return None
    mb = sum(btc_logs) / n; me = sum(eth_logs) / n
    num = sum((b - mb) * (e - me) for b, e in zip(btc_logs, eth_logs))
    db = math.sqrt(sum((b - mb) ** 2 for b in btc_logs))
    de = math.sqrt(sum((e - me) ** 2 for e in eth_logs))
    if db == 0 or de == 0: return None
    return num / (db * de)


def funding_at(ticks, ts_keys, ts):
    i = bisect_left(ts_keys, ts)
    i = min(i, len(ts_keys) - 1)
    return ticks[i][3], ticks[i][4]


def parse_cell(log_path):
    """Extract per-trade rows. Returns list of dicts with entry_ts, exit_ts,
    direction, z_entry, force, pnl, plus zcheck-derived spread_std/beta_eff
    and beta_drift."""
    entries, exits = [], []
    zchecks = []
    with open(log_path) as f:
        for line in f:
            m = ENTRY_RE.search(line)
            if m:
                entries.append({"entry_ts": int(m["ts"]),
                                "direction": m["dir"],
                                "z_entry": float(m["z"])})
                continue
            m = EXIT_RE.search(line)
            if m:
                exits.append({"exit_ts": int(m["ts"]),
                              "force": m["force"].lower() == "true",
                              "pnl_0bp": float(m["pnl"])})
                continue
            m = ZCHECK_RE.search(line)
            if m:
                zchecks.append((int(m["bts"]) // 1000, float(m["be"]), float(m["std"])))
    entries.sort(key=lambda x: x["entry_ts"])
    exits.sort(key=lambda x: x["exit_ts"])
    zchecks.sort(key=lambda x: x[0])
    z_keys = [z[0] for z in zchecks]

    def z_at(ts):
        i = bisect_left(z_keys, ts) - 1
        if i < 0: i = 0
        return zchecks[i] if zchecks else (None, None, None)

    rows = []
    for ent, ex in zip(entries, exits):
        bts, be, std = z_at(ent["entry_ts"])
        _, be1h, _ = z_at(ent["entry_ts"] - 3600)
        beta_drift = (be - be1h) / abs(be1h) if (be is not None and be1h not in (None, 0)) else None
        rows.append({**ent, **ex, "spread_std": std, "beta_eff": be,
                     "beta_drift": beta_drift})
    return rows


def add_market_features(rows, ticks, ts_keys):
    for r in rows:
        ts = r["entry_ts"]
        rv_btc_30, rv_eth_30 = realized_vol_log(ticks, ts_keys, ts, 1800)
        rv_btc_4h, rv_eth_4h = realized_vol_log(ticks, ts_keys, ts, 14400)
        r["rv_btc_30m"] = rv_btc_30; r["rv_eth_30m"] = rv_eth_30
        r["rv_btc_4h"] = rv_btc_4h; r["rv_eth_4h"] = rv_eth_4h
        r["corr_1h"] = corr(ticks, ts_keys, ts, 3600)
        r["corr_4h"] = corr(ticks, ts_keys, ts, 14400)
        r["corr_24h"] = corr(ticks, ts_keys, ts, 86400)
        fr_btc, fr_eth = funding_at(ticks, ts_keys, ts)
        r["fr_btc"] = fr_btc; r["fr_eth"] = fr_eth; r["fr_diff"] = fr_btc - fr_eth
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        r["hour_utc"] = dt.hour; r["weekday"] = dt.weekday()


def find_clusters(live_pnl_glob, start, end,
                  window_h=24, min_n=3, ratio=2):
    """Identify dense loss / win clusters in live trades within window.
    A cluster: contiguous trades within 24h where one side outnumbers the
    other by `ratio`, and minority side has at most n/(ratio+1) entries.
    Returns list of {kind, start_ts, end_ts}."""
    trades = []
    for f in sorted(glob.glob(live_pnl_glob)):
        with open(f) as fh:
            for line in fh:
                try:
                    r = json.loads(line)
                except Exception:
                    continue
                if r.get("source") != "exit_fill": continue
                entry_ts = int(r["ts"] - r["hold_secs"])
                if start <= entry_ts <= end:
                    r["entry_ts"] = entry_ts
                    trades.append(r)
    trades.sort(key=lambda x: x["entry_ts"])

    def find_clusters_of(want_loss):
        clusters = []
        i = 0
        while i < len(trades):
            head_ts = trades[i]["entry_ts"]
            end_ts_w = head_ts + window_h * 3600
            j = i + 1; last_idx = i
            while j < len(trades) and trades[j]["entry_ts"] <= end_ts_w:
                sub = trades[i:j+1]
                a = sum(1 for t in sub if (t["pnl"] < 0) == want_loss)
                b = len(sub) - a
                if a >= min_n and a >= ratio * b:
                    last_idx = j
                j += 1
            cluster = trades[i:last_idx+1]
            if len(cluster) >= min_n:
                a = sum(1 for t in cluster if (t["pnl"] < 0) == want_loss)
                if a >= min_n and a >= ratio * (len(cluster) - a):
                    clusters.append({
                        "kind": "LOSS" if want_loss else "WIN",
                        "start_ts": cluster[0]["entry_ts"],
                        "end_ts": cluster[-1]["entry_ts"],
                        "n_trades": len(cluster),
                        "majority_n": a,
                    })
                    i = last_idx + 1
                    continue
            i += 1
        return clusters
    return find_clusters_of(want_loss=True) + find_clusters_of(want_loss=False), trades


def membership(ts, clusters, slack_s=7200):
    for c in clusters:
        if c["start_ts"] - slack_s <= ts <= c["end_ts"] + slack_s:
            return c["kind"]
    return "NONE"


def quantile_filter(rows, key, drop_low=False, drop_high=False, q=0.25):
    valid = [r for r in rows if r.get(key) is not None]
    if len(valid) < 4: return None, 0
    valid.sort(key=lambda r: r[key])
    cut = int(len(valid) * q)
    if drop_low and drop_high:
        kept = valid[cut:len(valid)-cut]
    elif drop_low:
        kept = valid[cut:]
    elif drop_high:
        kept = valid[:len(valid)-cut]
    else:
        kept = valid
    return sum(r["pnl_5bp"] for r in kept), len(kept)


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("cells", nargs="*", default=["010", "012"])
    ap.add_argument("--grid-dir", default="/tmp/phase_a_grid_be")
    ap.add_argument("--dump-glob",
                    default="/tmp/bt/market_data_btceth_*.jsonl")
    ap.add_argument("--live-pnl-glob", default=None,
                    help="Optional glob for live pnl jsonl files. "
                         "If set, classifies each BT entry by live-cluster.")
    ap.add_argument("--start-utc", type=int, default=None)
    ap.add_argument("--end-utc", type=int, default=None)
    ap.add_argument("--fee-per-trade", type=float, default=0.357,
                    help="Approximate USD fee per round-trip trade. "
                         "Default 0.357 matches cells 010/012 grid CSV "
                         "pnl_0bp/pnl_5bp delta on the 5.5d run.")
    args = ap.parse_args()

    print(f"Loading dumps from {args.dump_glob}")
    ticks = load_ticks(args.dump_glob)
    ts_keys = [t[0] for t in ticks]
    print(f"  {len(ticks)} ticks; "
          f"{datetime.fromtimestamp(ticks[0][0], tz=timezone.utc):%Y-%m-%d %H:%M} → "
          f"{datetime.fromtimestamp(ticks[-1][0], tz=timezone.utc):%Y-%m-%d %H:%M} UTC")

    if not args.start_utc:
        args.start_utc = ticks[0][0]
    if not args.end_utc:
        args.end_utc = ticks[-1][0]

    cells = {}
    for cell in args.cells:
        path = f"{args.grid_dir}/cell_{cell}.log"
        rows = parse_cell(path)
        for r in rows:
            r["pnl_5bp"] = r["pnl_0bp"] - args.fee_per_trade
            r["cell"] = cell
        add_market_features(rows, ticks, ts_keys)
        cells[cell] = rows
        print(f"  cell {cell}: {len(rows)} trades, sum_pnl_5bp={sum(r['pnl_5bp'] for r in rows):+.2f}")

    # Per-cell feature filter ranking
    feats = ["z_entry", "spread_std", "beta_eff", "beta_drift",
             "rv_btc_30m", "rv_eth_30m", "rv_btc_4h", "rv_eth_4h",
             "corr_1h", "corr_4h", "corr_24h",
             "fr_btc", "fr_eth", "fr_diff", "hour_utc"]

    for cell, rows in cells.items():
        baseline = sum(r["pnl_5bp"] for r in rows)
        print()
        print(f"=== cell {cell}: per-feature single-gate filter sweep ===")
        print(f"{'feature':<14} | {'baseline':>9} | {'drop_low':>10} | {'drop_high':>10}")
        ranked = []
        for feat in feats:
            lo = quantile_filter(rows, feat, drop_low=True)
            hi = quantile_filter(rows, feat, drop_high=True)
            if lo[0] is None or hi[0] is None: continue
            print(f"{feat:<14} | {baseline:>+8.2f} | "
                  f"{lo[0]:>+7.2f}({lo[1]:>2}) | {hi[0]:>+7.2f}({hi[1]:>2})")
            best = max(lo[0], hi[0])
            best_dir = "drop_low" if lo[0] >= hi[0] else "drop_high"
            ranked.append({"feat": feat, "best_dir": best_dir,
                           "best_pnl": best,
                           "lift": best - baseline})

        ranked.sort(key=lambda r: -r["lift"])
        print(f"\n=== cell {cell}: top features by lift over baseline ({baseline:+.2f}) ===")
        for r in ranked[:5]:
            print(f"  {r['feat']:<14} {r['best_dir']:<10} "
                  f"pnl={r['best_pnl']:+.2f} lift={r['lift']:+.2f}")

        # Leave-N-out on top 3
        print(f"\n=== cell {cell}: leave-N-out on top 3 ===")
        for r in ranked[:3]:
            feat = r["feat"]
            valid = [x for x in rows if x.get(feat) is not None]
            valid.sort(key=lambda x: x[feat])
            cut = int(len(valid) * 0.25)
            kept = valid[cut:] if r["best_dir"] == "drop_low" else valid[:len(valid)-cut]
            base = sum(x["pnl_5bp"] for x in kept)
            sk = sorted(kept, key=lambda x: -x["pnl_5bp"])
            l1 = sum(x["pnl_5bp"] for x in sk[1:])
            l2 = sum(x["pnl_5bp"] for x in sk[2:])
            print(f"  {feat:<14} ({r['best_dir']}, n={len(kept)}) "
                  f"baseline={base:+.2f} -1best={l1:+.2f} -2best={l2:+.2f}")

    # Live cluster cross-check (if requested)
    if args.live_pnl_glob:
        print()
        print(f"=== Live-cluster cross-check from {args.live_pnl_glob} ===")
        clusters, live_trades = find_clusters(args.live_pnl_glob,
                                              args.start_utc, args.end_utc)
        print(f"Live trades in window: {len(live_trades)}")
        for c in clusters:
            s = datetime.fromtimestamp(c["start_ts"], tz=timezone.utc)
            e = datetime.fromtimestamp(c["end_ts"], tz=timezone.utc)
            print(f"  {c['kind']}  {s:%Y-%m-%d %H:%M} → {e:%H:%M}  "
                  f"({c['majority_n']}/{c['n_trades']})")
        for cell, rows in cells.items():
            print(f"\n  cell {cell}: BT entries by live cluster")
            agg = {"LOSS": [], "WIN": [], "NONE": []}
            for r in rows:
                agg[membership(r["entry_ts"], clusters)].append(r)
            for kind in ["LOSS", "WIN", "NONE"]:
                rs = agg[kind]
                if not rs:
                    print(f"    {kind:<5} n=0")
                    continue
                sum5 = sum(r["pnl_5bp"] for r in rs)
                def mean(key):
                    xs = [r[key] for r in rs if r.get(key) is not None]
                    return sum(xs) / len(xs) if xs else None
                mc = mean("corr_1h"); ms = mean("spread_std")
                mc_s = f"{mc:+.3f}" if mc is not None else "  na "
                ms_s = f"{ms:.3f}"  if ms is not None else " na "
                print(f"    {kind:<5} n={len(rs):>2} sum_pnl5={sum5:>+7.2f}  "
                      f"mean_corr_1h={mc_s}  mean_spread_std={ms_s}")


if __name__ == "__main__":
    main()
