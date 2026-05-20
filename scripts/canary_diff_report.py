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


# Heuristic threshold for treating an equity-history step as a discrete
# trade-close event vs continuous drift (funding + unrealised PnL on an
# open position). 5 bps of starting equity matches canary's observed
# trade cadence (≈0.46 trades/h × 168h ≈ 77, heuristic count 81 at
# 5 bps for canary on the 5/13-5/20 window). Main A over-counts at this
# threshold because $1000 notional × intraday volatility produces 5-bps
# equity steps from unrealised PnL during holds, but the metric stays
# consistent week-over-week so it still serves regression detection.
# Refined under bot-strategy#376 Phase 5 Stage B baseline review.
TRADE_EVENT_BPS_OF_EQUITY = 5.0


def filter_window(samples: list[dict], cutoff_ts_ms: float) -> list[dict]:
    """Filter equity-history samples to those at or after the cutoff.

    `equity_history.jsonl` writes `ts` in milliseconds; callers pass the
    cutoff in milliseconds too. The earlier seconds-vs-milliseconds
    mismatch silently let every sample through (lifetime accumulation
    instead of a real window) — see bot-strategy#376 Phase 5 Stage B
    fix.
    """
    return [s for s in samples if s.get("ts", 0) >= cutoff_ts_ms]


def build_snapshot(client, prefix: str, agent: str, cutoff_ts_ms: float) -> WindowSnapshot:
    """Build a 7-day window summary from S3-mirrored bot state.

    Source of truth is `equity_history.jsonl`, which persists across
    process restarts. The earlier draft read `status.json.trade_stats.*`
    directly, but `trade_stats` is per-process and resets when systemd
    restarts the bot (canary auto-restarts on every CI deploy per
    bot-strategy#376 design), making the report show `trades=0` whenever
    the workflow happens to run shortly after a deploy. Equity history
    is the only S3-side feed that survives restarts today; bringing the
    debot_pnl/ jsonl mirror up to S3 is a follow-up.
    """
    status = s3_get_json(client, f"{prefix}/{agent}.json") or {}
    history = s3_get_jsonl(client, f"{prefix}/{agent}.equity_history.jsonl")
    history_in_window = filter_window(history, cutoff_ts_ms)

    funding_carry = float(status.get("funding_carry_today", 0.0) or 0.0)

    if not history_in_window:
        equity_end = float(status.get("pnl_total", 0.0))
        return WindowSnapshot(
            agent=agent,
            trade_count=0,
            win_count=0,
            cumulative_pnl=0.0,
            median_per_trade_pnl=0.0,
            funding_carry=funding_carry,
            equity_start=equity_end,
            equity_end=equity_end,
        )

    equity_start = float(history_in_window[0]["equity"])
    equity_end = float(history_in_window[-1]["equity"])
    cumulative_pnl = equity_end - equity_start

    # Trade count heuristic: count equity-history transitions where
    # |Δ equity| ≥ TRADE_EVENT_BPS_OF_EQUITY bps of starting equity.
    # This is a coarse proxy — Stage B/C follow-up will replace it with
    # an authoritative debot_pnl/ jsonl read once that prefix is
    # mirrored to S3. Until then the heuristic gives a directionally
    # correct trade-rate ratio (the metric the verdict cares about).
    threshold = abs(equity_start) * TRADE_EVENT_BPS_OF_EQUITY / 10_000.0
    trade_count = 0
    win_count = 0
    deltas: list[float] = []
    for i in range(1, len(history_in_window)):
        delta = history_in_window[i]["equity"] - history_in_window[i - 1]["equity"]
        if abs(delta) >= threshold:
            trade_count += 1
            if delta > 0:
                win_count += 1
            deltas.append(delta)
    median_per_trade_pnl = statistics.median(deltas) if deltas else 0.0

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
    notes.append(
        f"activity_event_ratio={rate_ratio:.2f} "
        f"(heuristic at ≥{TRADE_EVENT_BPS_OF_EQUITY:.0f} bps Δ; main A over-counts from "
        f"unrealised-PnL mid-hold so the absolute target ≥ 2.0 from #376 needs "
        f"recalibration in Stage C)"
    )
    notes.append(
        f"main_cumulative={main.cumulative_pnl:+.4f} "
        f"canary_cumulative={canary.cumulative_pnl:+.4f} "
        f"(equity_history Δ over window, restart-resilient)"
    )
    notes.append(
        "Stage A: thresholds are placeholders, verdict always PASS until Stage C"
    )
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
        f"| Activity events (equity-step heuristic, ≥{TRADE_EVENT_BPS_OF_EQUITY:.0f} bps Δ) | {main.trade_count} | {canary.trade_count} | ratio {rate_ratio:.2f} |",
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

    cutoff_ms = (datetime.now(timezone.utc) - timedelta(days=args.days)).timestamp() * 1000
    client = boto3.client("s3", region_name="eu-central-1")

    main_snap = build_snapshot(client, MAIN_PREFIX, MAIN_AGENT, cutoff_ms)
    canary_snap = build_snapshot(client, CANARY_PREFIX, CANARY_AGENT, cutoff_ms)

    verdict, notes = compute_verdict(canary_snap, main_snap)
    print(render_markdown(canary_snap, main_snap, args.days, verdict, notes))
    return 0


if __name__ == "__main__":
    sys.exit(main())
