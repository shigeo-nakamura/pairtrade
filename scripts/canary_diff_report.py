#!/usr/bin/env python3
"""
canary_diff_report.py — weekly canary-vs-main divergence report
(bot-strategy#376 Phase 5 stage A — skeleton).

Pulls the last 7 days of status / equity history for the Frankfurt canary
and main Pair-A from the debot-dashboard S3 mirror, computes divergence
metrics, and emits a markdown report on stdout. The
canary-weekly-review.yml workflow then posts the stdout as a comment on
the bot-strategy#404 tracking issue.

Stage A: script runs end-to-end and produces a populated table, but the
ALERT / WATCH thresholds are placeholders. Stage C (~2026-05-28) sets the
thresholds from the first 2-3 weeks of baseline data and enables the
auto-issue trigger on ALERT.

Usage:
    canary_diff_report.py [--days 7]

Environment:
    AWS_REGION (default: eu-central-1)
    The script reads S3 via the GitHub Actions OIDC role configured for
    the bot-strategy repo (same as the existing error-watch workflow).
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import boto3

BUCKET = "debot-dashboard"
MAIN_PREFIX = "debot/status/frankfurt"
CANARY_PREFIX = "debot/status/frankfurt-canary"
MAIN_AGENT = "debot-pair-btceth-a"   # we benchmark canary against Pair-A
CANARY_AGENT = "debot-pair-canary"


@dataclass
class WindowSnapshot:
    """7-day window summary for one bot."""
    agent: str
    trade_count: int
    win_count: int
    cumulative_pnl: float
    median_per_trade_pnl: float
    funding_carry: float
    equity_start: float
    equity_end: float

    @property
    def win_rate(self) -> float:
        return self.win_count / self.trade_count if self.trade_count else 0.0


def s3_get_json(client, key: str) -> dict | None:
    try:
        resp = client.get_object(Bucket=BUCKET, Key=key)
        return json.loads(resp["Body"].read())
    except client.exceptions.NoSuchKey:
        return None


def s3_get_jsonl(client, key: str) -> list[dict]:
    try:
        resp = client.get_object(Bucket=BUCKET, Key=key)
        body = resp["Body"].read().decode()
        return [json.loads(line) for line in body.splitlines() if line.strip()]
    except client.exceptions.NoSuchKey:
        return []


def filter_window(samples: list[dict], cutoff_ts: float) -> list[dict]:
    return [s for s in samples if s.get("ts", 0) >= cutoff_ts]


def build_snapshot(client, prefix: str, agent: str, cutoff_ts: float) -> WindowSnapshot:
    status = s3_get_json(client, f"{prefix}/{agent}.json") or {}
    history = s3_get_jsonl(client, f"{prefix}/{agent}.equity_history.jsonl")
    history_in_window = filter_window(history, cutoff_ts)

    trade_stats = status.get("trade_stats", {}) or {}
    trade_count = int(trade_stats.get("trades", 0))
    win_count = int(trade_stats.get("wins", 0))
    cumulative_pnl = float(trade_stats.get("pnl", 0.0))
    funding_carry = float(status.get("funding_carry_today", 0.0) or 0.0)

    equity_start = history_in_window[0]["equity"] if history_in_window else float(status.get("pnl_total", 0.0))
    equity_end = float(status.get("pnl_total", equity_start))

    # Stage A placeholder: median_per_trade_pnl approximated from
    # cumulative / count. Stage B/C will load per-cycle pnl from the
    # debot_pnl/ S3 prefix once the schema is wired in.
    median_per_trade_pnl = cumulative_pnl / trade_count if trade_count else 0.0

    return WindowSnapshot(
        agent=agent,
        trade_count=trade_count,
        win_count=win_count,
        cumulative_pnl=cumulative_pnl,
        median_per_trade_pnl=median_per_trade_pnl,
        funding_carry=funding_carry,
        equity_start=equity_start,
        equity_end=equity_end,
    )


def compute_verdict(canary: WindowSnapshot, main: WindowSnapshot) -> tuple[str, str]:
    """
    Stage A: returns PASS unconditionally with a TODO note.
    Stage C: real thresholds (rate ratio range, per-trade PnL sign
    flip detection, KS test on z-distribution).
    """
    rate_ratio = (canary.trade_count / main.trade_count) if main.trade_count else 0.0
    notes = []
    notes.append(f"trade_rate_ratio={rate_ratio:.2f} (target ≥ 2.0 with entry_z=1.0)")
    notes.append(f"main_cumulative={main.cumulative_pnl:.4f} canary_cumulative={canary.cumulative_pnl:.4f}")
    notes.append("Stage A: thresholds are placeholders, verdict always PASS until Stage C")
    return "PASS", " | ".join(notes)


def render_markdown(canary: WindowSnapshot, main: WindowSnapshot,
                    window_days: int, verdict: str, notes: str) -> str:
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    rate_ratio = (canary.trade_count / main.trade_count) if main.trade_count else float("nan")

    lines = [
        f"## Weekly canary review — generated {now}",
        "",
        f"**Window**: trailing {window_days} d / **Verdict**: **{verdict}**",
        "",
        "| Metric | Main (Pair-A Frankfurt) | Canary (Frankfurt) | Δ / Ratio |",
        "|---|---|---|---|",
        f"| Trades | {main.trade_count} | {canary.trade_count} | ratio {rate_ratio:.2f} |",
        f"| Wins | {main.win_count} | {canary.win_count} | — |",
        f"| Win rate | {main.win_rate*100:.1f}% | {canary.win_rate*100:.1f}% | Δ {(canary.win_rate-main.win_rate)*100:+.1f} pp |",
        f"| Cumulative PnL | ${main.cumulative_pnl:.4f} | ${canary.cumulative_pnl:.4f} | Δ ${canary.cumulative_pnl-main.cumulative_pnl:+.4f} |",
        f"| Median per-trade PnL | ${main.median_per_trade_pnl:.4f} | ${canary.median_per_trade_pnl:.4f} | — |",
        f"| Funding carry (today) | ${main.funding_carry:.4f} | ${canary.funding_carry:.4f} | — |",
        f"| Equity (end of window) | ${main.equity_end:.4f} | ${canary.equity_end:.4f} | — |",
        "",
        f"**Verdict notes**: {notes}",
        "",
        "_Generated by `pairtrade/scripts/canary_diff_report.py` via_ ",
        "`bot-strategy/.github/workflows/canary-weekly-review.yml`. ",
        "See bot-strategy#376 Phase 5 for the full design.",
    ]
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=int, default=7, help="window length in days")
    args = parser.parse_args()

    cutoff = (datetime.now(timezone.utc) - timedelta(days=args.days)).timestamp()
    client = boto3.client("s3", region_name="eu-central-1")

    main_snap = build_snapshot(client, MAIN_PREFIX, MAIN_AGENT, cutoff)
    canary_snap = build_snapshot(client, CANARY_PREFIX, CANARY_AGENT, cutoff)

    verdict, notes = compute_verdict(canary_snap, main_snap)
    print(render_markdown(canary_snap, main_snap, args.days, verdict, notes))
    return 0


if __name__ == "__main__":
    sys.exit(main())
