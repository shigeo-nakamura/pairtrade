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

    def test_return_bps_is_rebased_after_a_capital_event(self):
        # bot-strategy#765 round2: a window starting at $1,000, deposited
        # to $6,000, then earning $120 of ordinary trading PnL must report
        # a return close to 120/6000 = 200bps (against the new capital
        # base), not 120/1000 = 1,200bps (against the pre-deposit base) —
        # the latter could cross the ALERT threshold against a canary
        # comparison that would otherwise pass.
        history = [
            {"ts": _ts(0.0), "equity": 1000.0},
            {"ts": _ts(1.0), "equity": 6000.0},   # deposit: capital event
            {"ts": _ts(2.0), "equity": 6120.0},   # +120 ordinary trade win
        ]
        snap = build_snapshot_from_data("x", {}, history, _ts(0.0), _ts(WINDOW_DAYS))
        self.assertAlmostEqual(snap.cumulative_pnl, 120.0)
        self.assertAlmostEqual(snap.return_bps, 120.0 / 6000.0 * 10_000.0, places=2)
        self.assertLess(
            snap.return_bps, 1_000.0,
            "return_bps must not be computed against the pre-deposit equity_start",
        )

    def test_return_bps_matches_the_old_formula_with_no_capital_event(self):
        history = [
            {"ts": _ts(0.0), "equity": 1000.0},
            {"ts": _ts(1.0), "equity": 1008.0},
            {"ts": _ts(2.0), "equity": 1003.0},
            {"ts": _ts(3.0), "equity": 1011.0},
            {"ts": _ts(7.0), "equity": 1015.0},
        ]
        snap = build_snapshot_from_data("x", {}, history, _ts(0.0), _ts(WINDOW_DAYS))
        expected = (history[-1]["equity"] - history[0]["equity"]) / abs(history[0]["equity"]) * 10_000.0
        self.assertAlmostEqual(snap.return_bps, expected)

    def test_deposit_from_a_zero_starting_balance_is_detected(self):
        # bot-strategy#765 round2: when the window's first sample is
        # exactly zero, abs(equity_start) * 0.50 is itself zero, which
        # previously disabled capital-event detection for the rest of the
        # window entirely — a $0 -> $6,000 redeposit was then recorded as
        # $6,000 of trading PnL and one trade.
        history = [
            {"ts": _ts(0.0), "equity": 0.0},
            {"ts": _ts(1.0), "equity": 6000.0},   # redeposit: capital event
            {"ts": _ts(2.0), "equity": 6010.0},   # +10 ordinary trade win
        ]
        snap = build_snapshot_from_data("x", {}, history, _ts(0.0), _ts(WINDOW_DAYS))
        self.assertAlmostEqual(snap.cumulative_pnl, 10.0)
        self.assertEqual(snap.trade_count, 1)

    def test_moderate_deposit_is_measured_against_the_pre_event_balance(self):
        # bot-strategy#765 round3: a $1,000 -> $1,600 deposit is a 60% move
        # relative to the $1,000 that was already there, so it must be
        # detected as a capital event. Comparing it against the *larger*
        # endpoint instead (max(1000, 1600) = 1600) raises the effective
        # threshold to $800, which a $600 delta does not clear, and the
        # deposit would be misread as $600 of trading PnL.
        history = [
            {"ts": _ts(0.0), "equity": 1000.0},
            {"ts": _ts(1.0), "equity": 1600.0},   # deposit: capital event
            {"ts": _ts(2.0), "equity": 1610.0},   # +10 ordinary trade win
        ]
        snap = build_snapshot_from_data("x", {}, history, _ts(0.0), _ts(WINDOW_DAYS))
        self.assertAlmostEqual(snap.cumulative_pnl, 10.0)
        self.assertEqual(snap.trade_count, 1)

    def test_trade_event_threshold_is_rebased_after_a_capital_event(self):
        # bot-strategy#765 round3: after a $1,000 -> $6,000 deposit, an
        # ordinary trade must be judged against the new $6,000 base (5bps
        # = $3.00), not the window's original $1,000 (5bps = $0.50) — a
        # stale threshold would count noise well under a real trade size
        # as a trade once the capital base has grown this much.
        history = [
            {"ts": _ts(0.0), "equity": 1000.0},
            {"ts": _ts(1.0), "equity": 6000.0},   # deposit: capital event
            {"ts": _ts(2.0), "equity": 6001.0},   # $1 drift: not a trade at the new base
        ]
        snap = build_snapshot_from_data("x", {}, history, _ts(0.0), _ts(WINDOW_DAYS))
        self.assertEqual(snap.trade_count, 0)
        self.assertAlmostEqual(snap.cumulative_pnl, 1.0)

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
