#!/usr/bin/env python3
"""
rehedge_attribution.py — attribute variant C's effect to either #461
(substitution mode) or #463+#465 (re-hedge with NRV gate) via post-hoc
data analysis (bot-strategy#466).

Approach
--------
For each closed trade on variant C, count how many `[REHEDGE_EXECUTED]`
events fired during the hold. Trades with zero re-hedges behave like A
on the rehedge axis (#461 entry-side shrink only); trades with >= 1
re-hedge let us see the marginal rebalance effect.

Compare against A's trades in the same window:

  baseline (A)            : Round-4 config, no #461, no #463
  C bucket "no_rehedge"   : #461 active, but rehedge gate didn't fire
                            → isolates the entry-side shrink effect
  C bucket "rehedge>=1"   : #461 + at least one rebalance fired
                            → adds the rebalance marginal effect

Usage
-----
    # Pull artefacts from Frankfurt first, then run locally:
    mkdir -p /tmp/r5_attribution
    scp 'debot:/opt/debot/debot_pnl/pnl-debot-pair-btceth-*.jsonl' /tmp/r5_attribution/
    ssh debot "sudo journalctl -u debot-pair-btceth --since '2026-05-21' \
        --no-pager | grep REHEDGE_EXECUTED" > /tmp/r5_attribution/rehedge.log

    scripts/rehedge_attribution.py \
        --pnl-dir /tmp/r5_attribution \
        --rehedge-log /tmp/r5_attribution/rehedge.log \
        --since 2026-05-21T19:24:00Z   # variant C live opt-in time

Output
------
- Bucketed per-trade table (CSV to stdout when --csv, else markdown)
- Summary row: count, mean PnL, win %, MaxDD per bucket
- Two-sample t-style summary (no inferential test — small N — but the
  raw delta is what we care about)
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median, stdev

REHEDGE_RE = re.compile(
    r"(?P<wall>[0-9-]+T[0-9:+.]+)\s+\[INFO\]\s+-\s+\[REHEDGE_EXECUTED\]"
    r"\s+variant=(?P<variant>\S+)\s+pair=(?P<pair>\S+)"
)


@dataclass
class RehedgeEvent:
    """One `[REHEDGE_EXECUTED]` event from the journal log."""
    ts: int                # unix seconds
    variant: str
    pair: str


@dataclass
class Trade:
    """One closed pairtrade — from the pnl jsonl."""
    variant: str
    entry_ts: int
    exit_ts: int            # = ts in the jsonl (close ts)
    hold_secs: float
    direction: str
    pnl: float
    z_entry: float
    z_exit: float
    entry_price_b: float
    exit_price_b: float
    beta_at_exit: float
    funding_carry_usd: float
    # Filled in by `attach_rehedges`:
    rehedge_count: int = 0
    rehedge_events: list[RehedgeEvent] = field(default_factory=list)


def parse_pnl_files(paths: list[Path]) -> list[Trade]:
    """Parse all pnl jsonl files into a flat list of Trades."""
    trades: list[Trade] = []
    for p in paths:
        # Filename: pnl-debot-pair-btceth-<variant>-YYYYMMDD.jsonl
        m = re.search(r"pnl-debot-pair-btceth-([a-z])-\d{8}\.jsonl", p.name)
        if not m:
            continue
        variant = m.group(1)
        with open(p) as fh:
            for line in fh:
                if not line.strip():
                    continue
                d = json.loads(line)
                exit_ts = int(d["ts"])
                hold = float(d["hold_secs"])
                trades.append(
                    Trade(
                        variant=variant,
                        entry_ts=exit_ts - int(hold),
                        exit_ts=exit_ts,
                        hold_secs=hold,
                        direction=d["direction"],
                        pnl=float(d["pnl"]),
                        z_entry=float(d["z_entry"]),
                        z_exit=float(d["z_exit"]),
                        entry_price_b=float(d["entry_price_b"]),
                        exit_price_b=float(d["exit_price_b"]),
                        beta_at_exit=float(d["beta"]),
                        funding_carry_usd=float(d.get("funding_carry_usd", 0.0)),
                    )
                )
    trades.sort(key=lambda t: (t.variant, t.entry_ts))
    return trades


def parse_rehedge_events(log_path: Path) -> list[RehedgeEvent]:
    """Parse REHEDGE_EXECUTED events from a journal log fragment."""
    events: list[RehedgeEvent] = []
    if not log_path.exists():
        return events
    for line in log_path.read_text().splitlines():
        m = REHEDGE_RE.search(line)
        if not m:
            continue
        # The journal wall timestamp is the bot's stamp (e.g. CET +0100).
        # Parse to unix seconds.
        wall = m.group("wall")
        try:
            dt = datetime.fromisoformat(wall)
        except ValueError:
            continue
        events.append(
            RehedgeEvent(
                ts=int(dt.timestamp()),
                variant=m.group("variant"),
                pair=m.group("pair"),
            )
        )
    return events


def attach_rehedges(trades: list[Trade], events: list[RehedgeEvent]) -> None:
    """For each trade, count rehedge events whose ts falls in [entry, exit]."""
    by_variant: dict[str, list[RehedgeEvent]] = defaultdict(list)
    for e in events:
        by_variant[e.variant].append(e)
    for v in by_variant:
        by_variant[v].sort(key=lambda e: e.ts)
    for t in trades:
        candidates = by_variant.get(t.variant, [])
        for e in candidates:
            if t.entry_ts <= e.ts <= t.exit_ts:
                t.rehedge_count += 1
                t.rehedge_events.append(e)


def bucket_stats(trades: list[Trade], label: str) -> dict:
    """Aggregate stats for a single bucket of trades."""
    if not trades:
        return {"label": label, "n": 0}
    pnls = [t.pnl for t in trades]
    wins = sum(1 for p in pnls if p > 0)
    # Rolling drawdown
    running, peak, mdd = 0.0, 0.0, 0.0
    for p in pnls:
        running += p
        if running > peak:
            peak = running
        mdd = max(mdd, peak - running)
    return {
        "label": label,
        "n": len(pnls),
        "pnl_total": sum(pnls),
        "pnl_mean": mean(pnls),
        "pnl_median": median(pnls),
        "pnl_stdev": stdev(pnls) if len(pnls) > 1 else 0.0,
        "win_pct": 100 * wins / len(pnls),
        "max_dd": mdd,
        "median_hold_h": median(t.hold_secs / 3600 for t in trades),
        "median_rehedges": median(t.rehedge_count for t in trades),
    }


def render_markdown(rows: list[dict]) -> str:
    cols = ["label", "n", "pnl_total", "pnl_mean", "pnl_median",
            "win_pct", "max_dd", "median_hold_h", "median_rehedges"]
    out = []
    out.append("| " + " | ".join(cols) + " |")
    out.append("|" + "|".join("---" for _ in cols) + "|")
    for r in rows:
        if r.get("n", 0) == 0:
            out.append(f"| {r['label']} | 0 | — | — | — | — | — | — | — |")
            continue
        out.append(
            f"| {r['label']} | {r['n']} "
            f"| ${r['pnl_total']:+.2f} | ${r['pnl_mean']:+.3f} "
            f"| ${r['pnl_median']:+.3f} | {r['win_pct']:.0f}% "
            f"| ${r['max_dd']:.2f} | {r['median_hold_h']:.2f} "
            f"| {r['median_rehedges']:.1f} |"
        )
    return "\n".join(out)


def render_per_trade_csv(trades: list[Trade]) -> str:
    out = ["variant,entry_ts,exit_ts,hold_secs,direction,z_entry,z_exit,beta_at_exit,pnl,rehedge_count"]
    for t in trades:
        out.append(
            f"{t.variant},{t.entry_ts},{t.exit_ts},{t.hold_secs:.0f},"
            f"{t.direction},{t.z_entry:.3f},{t.z_exit:.3f},"
            f"{t.beta_at_exit:.4f},{t.pnl:.4f},{t.rehedge_count}"
        )
    return "\n".join(out)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--pnl-dir", required=True, type=Path,
                        help="Directory holding pnl-debot-pair-btceth-*.jsonl")
    parser.add_argument("--rehedge-log", required=True, type=Path,
                        help="File with [REHEDGE_EXECUTED] lines (grep output of journalctl)")
    parser.add_argument("--since", default=None,
                        help="ISO timestamp; ignore trades that closed before this (default: all)")
    parser.add_argument("--csv", action="store_true",
                        help="Also print per-trade CSV after the summary")
    args = parser.parse_args()

    since_ts: int | None = None
    if args.since:
        since_ts = int(datetime.fromisoformat(args.since.replace("Z", "+00:00")).timestamp())

    pnl_files = sorted(args.pnl_dir.glob("pnl-debot-pair-btceth-*.jsonl"))
    if not pnl_files:
        print(f"No pnl jsonl found under {args.pnl_dir}", file=sys.stderr)
        return 1
    trades = parse_pnl_files(pnl_files)
    if since_ts is not None:
        trades = [t for t in trades if t.exit_ts >= since_ts]

    events = parse_rehedge_events(args.rehedge_log)
    if since_ts is not None:
        events = [e for e in events if e.ts >= since_ts]
    attach_rehedges(trades, events)

    # Buckets
    a_trades = [t for t in trades if t.variant == "a"]
    b_trades = [t for t in trades if t.variant == "b"]
    c_trades = [t for t in trades if t.variant == "c"]
    c_no_rehedge = [t for t in c_trades if t.rehedge_count == 0]
    c_with_rehedge = [t for t in c_trades if t.rehedge_count >= 1]

    rows = [
        bucket_stats(a_trades, "A (Round-4 baseline)"),
        bucket_stats(b_trades, "B (Round-4 baseline + fc=10800)"),
        bucket_stats(c_trades, "C (overall: #461 + #463+#465)"),
        bucket_stats(c_no_rehedge, "  └─ C, no rehedge (isolates #461)"),
        bucket_stats(c_with_rehedge, "  └─ C, rehedge≥1 (adds #463+#465)"),
    ]
    print(f"# Round-5 Variant-C Attribution — bot-strategy#466\n")
    print(f"Window: {'since ' + args.since if args.since else 'all data'} "
          f"({len(trades)} trades, {len(events)} REHEDGE_EXECUTED events)\n")
    print(render_markdown(rows))
    print()

    # Interpretation hints
    def safe_mean(label_substr: str) -> float | None:
        for r in rows:
            if label_substr in r["label"] and r.get("n", 0) > 0:
                return r["pnl_mean"]
        return None
    a_pnl = safe_mean("A (Round-4")
    c_pnl = safe_mean("C (overall")
    c_nor = safe_mean("no rehedge")
    c_reh = safe_mean("rehedge≥1")
    def fmt_delta(left: float | None, right: float | None) -> str:
        if left is None or right is None:
            return "n/a (empty bucket)"
        return f"${left - right:+.3f}"
    print("## Interpretation\n")
    print(f"- C overall vs A: per-trade mean PnL delta = {fmt_delta(c_pnl, a_pnl)}")
    print(f"- C no-rehedge vs A: {fmt_delta(c_nor, a_pnl)}  (= #461 substitution-only effect)")
    print(f"- C rehedge≥1 vs C no-rehedge: {fmt_delta(c_reh, c_nor)}  "
          "(= #463+#465 marginal rebalance effect, holding #461 constant)\n")
    print("Caveats: small N, selection bias (rehedge trades may have deeper entry-z), "
          "and per-trade variance is high — treat sub-bucket deltas as directional, "
          "not as significance tests.")

    if args.csv:
        print("\n--- per-trade CSV ---")
        print(render_per_trade_csv(trades))

    return 0


if __name__ == "__main__":
    sys.exit(main())
