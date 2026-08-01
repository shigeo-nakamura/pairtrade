#!/usr/bin/env python3
"""
canary_diff_report.py — weekly canary-vs-main divergence report
(bot-strategy#376 Phase 5).

Pulls the last 7 days of status / equity history for the Frankfurt canary
and main Pair-A from the debot-dashboard S3 mirror, computes divergence
metrics, and emits a markdown report on stdout. The
canary-weekly-review.yml workflow then posts the stdout as a comment on
the bot-strategy#404 tracking issue.

A partial mirrored history produces WATCH because a main-bot restart can
truncate the local equity-history file before it is uploaded to S3. Full
windows use conservative activity-ratio and normalized-return thresholds.

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

BUCKET = "debot-dashboard"
MAIN_PREFIX = "debot/status/frankfurt"
CANARY_PREFIX = "debot/status/frankfurt-canary"
MAIN_AGENT = "debot-pair-btceth-a"   # we benchmark canary against Pair-A
CANARY_AGENT = "debot-pair-canary"
MIN_WINDOW_COVERAGE = 0.80
ACTIVITY_RATIO_WATCH_MIN = 0.50
ACTIVITY_RATIO_WATCH_MAX = 4.00
ACTIVITY_RATIO_ALERT_MIN = 0.25
ACTIVITY_RATIO_ALERT_MAX = 8.00
RETURN_GAP_WATCH_BPS = -500.0
RETURN_GAP_ALERT_BPS = -1_000.0
# bot-strategy#412 signal #5: alert when the canary loses more than ~2 sigma
# over 7d while main-A is profitable. -1.4 USD is 2 x sigma_7d ~= 0.116 USD/trade
# x sqrt(36 trades/7d) at the 2026-07 baseline (canary ref $50, entry_z=1.0);
# recompute when the canary config changes. Only applied to 7-day windows.
CANARY_PNL_ALERT_USD_7D = -1.4
CANARY_PNL_ALERT_WINDOW_DAYS = 7


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
    history_start_ts_ms: float
    history_end_ts_ms: float
    window_coverage: float
    # bot-strategy#765 round2: a capital-adjusted return, computed by
    # rebasing to the post-event equity at each capital-event boundary and
    # compounding each segment's own return (see build_snapshot_from_data).
    # Plain cumulative_pnl / equity_start (the previous return_bps formula)
    # divides post-event trading PnL by the *pre-event* starting balance,
    # which is wrong whenever a capital event changed the capital actually
    # at risk. With no capital events in the window there is only one
    # segment, so this is numerically identical to the old formula.
    return_bps: float

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

# bot-strategy#765: a single-step |Δequity| this large relative to the
# window's starting equity is a deposit/withdrawal capital event, not
# trading PnL — round rollouts routinely move >50% of a variant's equity
# in one step (e.g. bot-strategy#751 withdrew Pair-A to ~$0 then
# redeposited to $6,000, which the naive equity_end - equity_start diff
# misread as an $3,834.95 trading gain in bot-strategy#762). No observed
# real trade-close has come close to this fraction of equity_start.
# Capital-event steps are excluded entirely from cumulative_pnl,
# trade_count/win_count, and median_per_trade_pnl.
CAPITAL_EVENT_FRACTION_OF_EQUITY = 0.50

# bot-strategy#765 round4: below this absolute USD balance, a segment has
# no meaningful capital at risk, so ordinary cent-scale drift against it
# is not a percentage return or a trade — it is the tail of an in-progress
# withdrawal/redeposit. Without this floor, a withdrawal leaving a small
# nonzero residual (e.g. $1,000 -> $0.01) rebases the segment onto that
# dust: a further $0.01 -> $0.014 drift is only a 40% move relative to the
# $0.01 base, under CAPITAL_EVENT_FRACTION_OF_EQUITY's 50% cutoff, so it
# is treated as an ordinary trade and compounds into a +4,000bps segment
# return once the eventual redeposit closes it out — a false ALERT against
# an unchanged canary. Canary's own reference notional is ~$50 (see
# CANARY_PNL_ALERT_USD_7D above); $1 is comfortably below any real
# position size while covering realistic post-withdrawal residuals.
DUST_EQUITY_USD_FLOOR = 1.0


def filter_window(samples: list[dict], cutoff_ts_ms: float) -> list[dict]:
    """Filter equity-history samples to those at or after the cutoff.

    `equity_history.jsonl` writes `ts` in milliseconds; callers pass the
    cutoff in milliseconds too. The earlier seconds-vs-milliseconds
    mismatch silently let every sample through (lifetime accumulation
    instead of a real window) — see bot-strategy#376 Phase 5 Stage B
    fix.
    """
    return [s for s in samples if s.get("ts", 0) >= cutoff_ts_ms]


def build_snapshot_from_data(agent: str, status: dict, history: list[dict],
                             cutoff_ts_ms: float, now_ts_ms: float) -> WindowSnapshot:
    """Pure computation over already-fetched status/history data.

    Split out from `build_snapshot` so the verdict logic can be exercised
    with fixture data (bot-strategy#765) without mocking S3.
    """
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
            history_start_ts_ms=0.0,
            history_end_ts_ms=0.0,
            window_coverage=0.0,
            return_bps=0.0,
        )

    history_start_ts_ms = float(history_in_window[0]["ts"])
    history_end_ts_ms = float(history_in_window[-1]["ts"])
    requested_span_ms = max(1.0, now_ts_ms - cutoff_ts_ms)
    covered_span_ms = max(0.0, history_end_ts_ms - history_start_ts_ms)
    equity_start = float(history_in_window[0]["equity"])
    equity_end = float(history_in_window[-1]["equity"])

    # Trade count heuristic: count equity-history transitions where
    # |Δ equity| ≥ TRADE_EVENT_BPS_OF_EQUITY bps of the current segment's
    # starting equity (rebased at each capital event alongside
    # segment_start_equity below — bot-strategy#765 round3: a fixed,
    # window-original threshold over-counts noise as trades after a
    # deposit raises the capital base, or suppresses real trades after a
    # withdrawal lowers it). A single segment (no capital events) keeps
    # this fixed at equity_start, i.e. numerically identical to the
    # pre-#765-round3 formula.
    trade_count = 0
    win_count = 0
    deltas: list[float] = []
    trading_pnl = 0.0

    # bot-strategy#765 round2: a capital event closes the current segment
    # and starts a new one based at the post-event equity, so a
    # capital-adjusted return can be computed by compounding each
    # segment's own (local-base) return instead of dividing all
    # post-event trading PnL by the window's original (possibly
    # since-withdrawn) starting balance. A single segment (no capital
    # events) reduces to plain segment_pnl / segment_start_equity, i.e.
    # numerically identical to the pre-#765-round2 formula.
    segment_start_equity = equity_start
    segment_pnl = 0.0
    growth_factor = 1.0

    def close_segment() -> None:
        nonlocal growth_factor
        if abs(segment_start_equity) >= DUST_EQUITY_USD_FLOOR:
            growth_factor *= 1.0 + segment_pnl / abs(segment_start_equity)
        # else: no meaningful capital was at risk in this segment (it
        # started at, or dropped to, dust), so it contributes no return;
        # a move large enough to matter is itself caught as the next
        # capital event below, not misread as a huge percentage gain.

    for i in range(1, len(history_in_window)):
        prev_equity = history_in_window[i - 1]["equity"]
        curr_equity = history_in_window[i]["equity"]
        delta = curr_equity - prev_equity
        # The capital-event reference is the pre-step (not the larger of
        # the two endpoints) balance: the fraction is defined relative to
        # what was already there before this move, so using the larger
        # endpoint instead raises the effective threshold above the
        # configured fraction for any deposit that increases the balance
        # by less than 100% — e.g. a $1,000 -> $1,600 deposit (a 60% move)
        # would compare its $600 delta against an $800 threshold
        # (max(1000, 1600) * 50%) and be missed, misread as $600 of
        # trading PnL. A step whose pre-event balance is exactly zero has
        # no such reference, so the post-event balance is used instead
        # purely to still catch that deposit (bot-strategy#765 round2).
        step_reference = abs(prev_equity) if prev_equity != 0.0 else abs(curr_equity)
        capital_event_threshold = step_reference * CAPITAL_EVENT_FRACTION_OF_EQUITY
        if step_reference > 0.0 and abs(delta) >= capital_event_threshold:
            # Deposit/withdrawal, not a trade — excluded from PnL and
            # trade-count accounting (bot-strategy#765), and rebases the
            # capital-adjusted return's segment boundary.
            close_segment()
            segment_start_equity = curr_equity
            segment_pnl = 0.0
            continue
        if abs(segment_start_equity) < DUST_EQUITY_USD_FLOOR:
            # Still resolving a withdrawal: this segment has no meaningful
            # capital at risk, so ordinary cent-scale drift here (below
            # the capital-event fraction of the tiny dust base, e.g. a
            # further $0.01 -> $0.014 move) is neither a trade nor a
            # return-bearing move. Keep tracking the running balance so
            # the eventual redeposit is still measured from the correct
            # (low) base, without counting this step as a trade or
            # compounding it into the return.
            segment_start_equity = curr_equity
            continue
        trading_pnl += delta
        segment_pnl += delta
        trade_threshold = abs(segment_start_equity) * TRADE_EVENT_BPS_OF_EQUITY / 10_000.0
        if abs(delta) >= trade_threshold:
            trade_count += 1
            if delta > 0:
                win_count += 1
            deltas.append(delta)
    close_segment()
    median_per_trade_pnl = statistics.median(deltas) if deltas else 0.0
    return_bps = (growth_factor - 1.0) * 10_000.0

    return WindowSnapshot(
        agent=agent,
        trade_count=trade_count,
        win_count=win_count,
        cumulative_pnl=trading_pnl,
        median_per_trade_pnl=median_per_trade_pnl,
        funding_carry=funding_carry,
        equity_start=equity_start,
        equity_end=equity_end,
        history_start_ts_ms=history_start_ts_ms,
        history_end_ts_ms=history_end_ts_ms,
        window_coverage=min(1.0, covered_span_ms / requested_span_ms),
        return_bps=return_bps,
    )


def build_snapshot(client, prefix: str, agent: str, cutoff_ts_ms: float,
                   now_ts_ms: float) -> WindowSnapshot:
    """Build a 7-day window summary from S3-mirrored bot state.

    Source of truth is `equity_history.jsonl`. It is more durable than
    status.json trade_stats, but a state reset or manual main-bot restart
    can truncate it. Window coverage therefore gates the verdict.
    """
    status = s3_get_json(client, f"{prefix}/{agent}.json") or {}
    history = s3_get_jsonl(client, f"{prefix}/{agent}.equity_history.jsonl")
    return build_snapshot_from_data(agent, status, history, cutoff_ts_ms, now_ts_ms)


def compute_verdict(canary: WindowSnapshot, main: WindowSnapshot,
                    window_days: int = CANARY_PNL_ALERT_WINDOW_DAYS) -> tuple[str, str]:
    notes: list[str] = []
    if (
        main.window_coverage < MIN_WINDOW_COVERAGE
        or canary.window_coverage < MIN_WINDOW_COVERAGE
    ):
        notes.append(
            "insufficient history coverage "
            f"(main={main.window_coverage:.0%}, canary={canary.window_coverage:.0%}, "
            f"required={MIN_WINDOW_COVERAGE:.0%}); a restart may have truncated "
            "the mirrored equity history"
        )
        return "WATCH", " | ".join(notes)

    # bot-strategy#412 signal #5: absolute-USD divergence, calibrated for the
    # 7-day window only. Independent of the activity ratio, so it is evaluated
    # before the zero-activity early return below.
    pnl_divergence = (
        window_days == CANARY_PNL_ALERT_WINDOW_DAYS
        and canary.cumulative_pnl < CANARY_PNL_ALERT_USD_7D
        and main.cumulative_pnl > 0
    )
    if window_days == CANARY_PNL_ALERT_WINDOW_DAYS:
        notes.append(
            f"pnl_7d canary=${canary.cumulative_pnl:+.2f} vs "
            f"main=${main.cumulative_pnl:+.2f} "
            f"(alert if canary < ${CANARY_PNL_ALERT_USD_7D} and main > 0)"
        )

    if main.trade_count == 0:
        notes.append("main has zero activity events; activity ratio is undefined")
        return ("ALERT" if pnl_divergence else "WATCH"), " | ".join(notes)

    rate_ratio = canary.trade_count / main.trade_count
    return_gap_bps = canary.return_bps - main.return_bps
    notes.append(
        f"activity_event_ratio={rate_ratio:.2f} "
        f"(heuristic at >= {TRADE_EVENT_BPS_OF_EQUITY:.0f} bps delta)"
    )
    notes.append(
        f"return_gap={return_gap_bps:+.1f}bps "
        f"(main={main.return_bps:+.1f}bps, canary={canary.return_bps:+.1f}bps)"
    )

    if (
        rate_ratio < ACTIVITY_RATIO_ALERT_MIN
        or rate_ratio > ACTIVITY_RATIO_ALERT_MAX
        or return_gap_bps < RETURN_GAP_ALERT_BPS
        or pnl_divergence
    ):
        return "ALERT", " | ".join(notes)
    if (
        rate_ratio < ACTIVITY_RATIO_WATCH_MIN
        or rate_ratio > ACTIVITY_RATIO_WATCH_MAX
        or return_gap_bps < RETURN_GAP_WATCH_BPS
    ):
        return "WATCH", " | ".join(notes)
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
        f"| Return | {main.return_bps:+.1f} bps | {canary.return_bps:+.1f} bps | delta {canary.return_bps-main.return_bps:+.1f} bps |",
        f"| Median per-trade PnL | ${main.median_per_trade_pnl:.4f} | ${canary.median_per_trade_pnl:.4f} | — |",
        f"| Funding carry (today) | ${main.funding_carry:.4f} | ${canary.funding_carry:.4f} | — |",
        f"| Equity (end of window) | ${main.equity_end:.4f} | ${canary.equity_end:.4f} | — |",
        f"| History coverage | {main.window_coverage:.0%} | {canary.window_coverage:.0%} | required >= {MIN_WINDOW_COVERAGE:.0%} |",
        "",
        f"**Verdict notes**: {notes}",
        "",
        "_Generated by `pairtrade/scripts/canary_diff_report.py` via_ ",
        "`debot-dashboard/.github/workflows/canary-weekly-review.yml`. ",
        "See bot-strategy#376 Phase 5 for the full design.",
    ]
    return "\n".join(lines)


def main() -> int:
    import boto3

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=int, default=7, help="window length in days")
    args = parser.parse_args()

    now_ms = datetime.now(timezone.utc).timestamp() * 1000
    cutoff_ms = now_ms - timedelta(days=args.days).total_seconds() * 1000
    client = boto3.client("s3", region_name="eu-central-1")

    main_snap = build_snapshot(client, MAIN_PREFIX, MAIN_AGENT, cutoff_ms, now_ms)
    canary_snap = build_snapshot(client, CANARY_PREFIX, CANARY_AGENT, cutoff_ms, now_ms)

    verdict, notes = compute_verdict(canary_snap, main_snap, args.days)
    print(render_markdown(canary_snap, main_snap, args.days, verdict, notes))
    return 0


if __name__ == "__main__":
    sys.exit(main())
