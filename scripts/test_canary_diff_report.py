#!/usr/bin/env python3
"""Regression tests for canary_diff_report.py (bot-strategy#765).

No external dependencies (stdlib unittest only, unlike the script itself
which needs boto3 for the S3-fetching `main()` path) — these tests only
exercise the pure `build_snapshot_from_data` / `compute_verdict` layer.

Run with: python3 scripts/test_canary_diff_report.py
"""
from __future__ import annotations

import unittest

from canary_diff_report import (
    TRADE_EVENT_BPS_OF_EQUITY,
    build_snapshot_from_data,
    compute_verdict,
)

DAY_MS = 24 * 60 * 60 * 1000
WINDOW_DAYS = 7


def _ts(day_offset: float) -> float:
    """Millisecond timestamp `day_offset` days into a 7-day window starting at 0."""
    return day_offset * DAY_MS


class CapitalEventExclusionTests(unittest.TestCase):
    """bot-strategy#765 acceptance criteria."""

    def test_762_sequence_excludes_capital_event_from_pnl(self):
        # Replays the bot-strategy#762 sequence: Pair-A window opens at
        # $2,132.78, drifts with ordinary small trading PnL, gets
        # withdrawn to ~$0 at the Round-9 boundary, then redeposited to
        # $6,000 a few hours later, then drifts again with ordinary
        # trading PnL for the rest of the window.
        main_history = [
            {"ts": _ts(0.0), "equity": 2132.78},
            {"ts": _ts(2.0), "equity": 2142.30},      # +9.52 ordinary trade win
            {"ts": _ts(4.0), "equity": 2152.78},      # +10.48 ordinary trade win
            {"ts": _ts(6.0), "equity": 2153.00},      # +0.22 ordinary drift
            {"ts": _ts(6.5), "equity": 0.009267},     # withdraw: capital event
            {"ts": _ts(6.7), "equity": 6000.009267},  # redeposit: capital event
            {"ts": _ts(6.9), "equity": 6010.50},      # +10.49 ordinary trade win
            {"ts": _ts(7.0), "equity": 6005.00},      # -5.50 ordinary trade loss
        ]
        canary_history = [
            {"ts": _ts(0.0), "equity": 17.90},
            {"ts": _ts(2.0), "equity": 17.60},
            {"ts": _ts(4.0), "equity": 17.40},
            {"ts": _ts(6.0), "equity": 17.30},
            {"ts": _ts(7.0), "equity": 17.28},
        ]
        cutoff_ms = _ts(0.0)
        now_ms = _ts(WINDOW_DAYS)

        main = build_snapshot_from_data("debot-pair-btceth-a", {}, main_history, cutoff_ms, now_ms)
        canary = build_snapshot_from_data("debot-pair-canary", {}, canary_history, cutoff_ms, now_ms)

        # cumulative_pnl must reflect only the ordinary trading deltas
        # (9.52 + 10.48 + 0.22 + 10.49 - 5.50 = 25.21), not the ~-2132.77
        # withdrawal or the ~+6000.00 redeposit.
        expected_trading_pnl = 9.52 + 10.48 + 0.22 + 10.49 - 5.50
        self.assertAlmostEqual(main.cumulative_pnl, expected_trading_pnl, places=2)

        # The two capital-event steps must not be counted as trades.
        for delta in (0.009267 - 2153.00, 6000.009267 - 0.009267):
            self.assertGreater(
                abs(delta), abs(main.equity_start) * 0.50,
                "fixture delta is not actually large enough to exercise the capital-event path",
            )
        self.assertEqual(main.trade_count, 4)

        verdict, notes = compute_verdict(canary, main, WINDOW_DAYS)
        self.assertEqual(
            verdict, "PASS",
            f"expected PASS once the capital event is excluded, got {verdict}: {notes}",
        )

    def test_capital_event_step_not_counted_as_trade(self):
        history = [
            {"ts": _ts(0.0), "equity": 1000.0},
            {"ts": _ts(1.0), "equity": 1005.0},   # ordinary trade
            {"ts": _ts(2.0), "equity": 0.01},     # withdraw (capital event)
            {"ts": _ts(3.0), "equity": 3000.01},  # redeposit (capital event)
            {"ts": _ts(4.0), "equity": 2995.0},   # ordinary trade
        ]
        snap = build_snapshot_from_data("x", {}, history, _ts(0.0), _ts(WINDOW_DAYS))
        # Only the two ordinary trades should be counted; the two capital
        # events must be excluded even though they dwarf the 5bps
        # trade-event threshold.
        self.assertEqual(snap.trade_count, 2)

    def test_non_capital_event_window_is_unchanged(self):
        # A normal week with only ordinary trade-sized moves: cumulative_pnl
        # must equal the old equity_end - equity_start formula exactly
        # (acceptance criterion: existing PASS reports must not change).
        history = [
            {"ts": _ts(0.0), "equity": 1000.0},
            {"ts": _ts(1.0), "equity": 1008.0},
            {"ts": _ts(2.0), "equity": 1003.0},
            {"ts": _ts(3.0), "equity": 1011.0},
            {"ts": _ts(7.0), "equity": 1015.0},
        ]
        snap = build_snapshot_from_data("x", {}, history, _ts(0.0), _ts(WINDOW_DAYS))
        self.assertAlmostEqual(snap.cumulative_pnl, history[-1]["equity"] - history[0]["equity"])

    def test_ordinary_trade_sized_delta_still_counted(self):
        # A delta comfortably above the trade-event threshold but well
        # below the capital-event threshold must still be treated as an
        # ordinary trade, not silently dropped.
        equity_start = 1000.0
        trade_delta = equity_start * (TRADE_EVENT_BPS_OF_EQUITY * 2) / 10_000.0
        self.assertLess(trade_delta, equity_start * 0.50)
        history = [
            {"ts": _ts(0.0), "equity": equity_start},
            {"ts": _ts(1.0), "equity": equity_start + trade_delta},
        ]
        snap = build_snapshot_from_data("x", {}, history, _ts(0.0), _ts(WINDOW_DAYS))
        self.assertEqual(snap.trade_count, 1)
        self.assertAlmostEqual(snap.cumulative_pnl, trade_delta)


if __name__ == "__main__":
    unittest.main()
