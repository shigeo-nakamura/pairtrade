#!/usr/bin/env python3

import importlib.util
import json
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

SCRIPT = Path(__file__).with_name("arcus_activity_ledger.py")
SPEC = importlib.util.spec_from_file_location("arcus_activity_ledger", SCRIPT)
assert SPEC and SPEC.loader
ledger_tool = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = ledger_tool
SPEC.loader.exec_module(ledger_tool)

CEILING = ledger_tool.DEFAULT_CEILING_USD_PER_1K
NO_GAS = Decimal(0)
WEI = "5227436861554050"


def stamp(when):
    return when.isoformat().replace("+00:00", "Z")


def raw(quantity, decimals=18):
    return str(int(Decimal(quantity) * (Decimal(10) ** decimals)))


# The runtime pins the resolved ERC-20 contract on both the plan and the
# intent, so every real record carries them and the matcher requires them.
TOKENS = {
    "SPY": "0x1111111111111111111111111111111111111111",
    "QQQ": "0x2222222222222222222222222222222222222222",
    "NVDA": "0x3333333333333333333333333333333333333333",
    "AMD": "0x4444444444444444444444444444444444444444",
}


def would_rotate_event(sequence, when, *, trigger, sell, buy, sell_quantity,
                       buy_quantity, spy_mark, qqq_mark, decimals=18):
    return {
        "sequence": sequence,
        "observed_at": stamp(when),
        "pair": "SPY/QQQ",
        "relative_log_price": 0.07,
        "z_score": -2.6,
        "token_a_reference_price_usd": str(spy_mark),
        "token_b_reference_price_usd": str(qqq_mark),
        "decision": {
            "action": "would_rotate",
            "plan": {
                "trigger": trigger,
                "venue": "rialto",
                "sell_symbol": sell,
                "buy_symbol": buy,
                "sell_quantity": str(sell_quantity),
                "buy_quantity": str(buy_quantity),
                "sell_amount_raw": raw(sell_quantity, decimals),
                "buy_amount_raw": raw(buy_quantity, decimals),
                "sell_token_address": TOKENS[sell],
                "buy_token_address": TOKENS[buy],
            },
        },
    }


def observe_event(sequence, when):
    return {
        "sequence": sequence,
        "observed_at": stamp(when),
        "pair": "SPY/QQQ",
        "relative_log_price": 0.07,
        "z_score": None,
        "token_a_reference_price_usd": "770",
        "token_b_reference_price_usd": "718",
        "decision": {"action": "observe"},
    }


def attempt(sequence, when, *, sell, buy, sell_quantity, buy_quantity,
            phase="reconciled", decimals=18, gas_before=WEI, gas_after=WEI,
            sell_before="10", buy_before="10", dispatched=None):
    sold = Decimal(sell_before) - Decimal(sell_quantity)
    bought = Decimal(buy_before) + Decimal(buy_quantity)
    return {
        "sequence": sequence,
        "prepared_at": stamp(when),
        "dispatched_at": stamp(dispatched or when),
        "phase": phase,
        "intent": {
            "venue": "rialto",
            "sell_symbol": sell,
            "buy_symbol": buy,
            "sell_amount_raw": raw(sell_quantity, decimals),
            "sell_token": TOKENS[sell],
            "buy_token": TOKENS[buy],
        },
        "pre_balances": {
            "sell_balance_raw": raw(sell_before, decimals),
            "buy_balance_raw": raw(buy_before, decimals),
            "gas_balance_wei": gas_before,
        },
        "post_balances": {
            "sell_balance_raw": raw(sold, decimals),
            "buy_balance_raw": raw(bought, decimals),
            "gas_balance_wei": gas_after,
        },
    }


def report_for(events, history, active=None, **kwargs):
    ledger = {"history": history, "active": active}
    return ledger_tool.build_report(ledger, events, CEILING, NO_GAS, **kwargs)


ENTRY_AT = datetime(2026, 9, 4, 14, 17, tzinfo=timezone.utc)
EXIT_AT = datetime(2026, 9, 5, 13, 2, tzinfo=timezone.utc)


def baseline_round_trip():
    """The one completed SPY/QQQ rotation the owner priced by hand on #938."""
    events = [
        would_rotate_event(1, ENTRY_AT, trigger="entry_signal", sell="QQQ", buy="SPY",
                           sell_quantity="0.347094", buy_quantity="0.323269",
                           spy_mark="771.27", qqq_mark="720.265"),
        would_rotate_event(2, EXIT_AT, trigger="mean_reversion_exit", sell="SPY", buy="QQQ",
                           sell_quantity="0.323269", buy_quantity="0.346345",
                           spy_mark="773.50", qqq_mark="721.00"),
    ]
    history = [
        attempt(8, ENTRY_AT, sell="QQQ", buy="SPY",
                sell_quantity="0.347094", buy_quantity="0.323269"),
        attempt(9, EXIT_AT, sell="SPY", buy="QQQ",
                sell_quantity="0.323269", buy_quantity="0.346345"),
    ]
    return events, history


class ActivityLedgerTests(unittest.TestCase):
    def test_a_closed_rotation_is_priced_at_its_exit_marks(self):
        events, history = baseline_round_trip()
        report = report_for(events, history)

        # 0.347094 QQQ out, 0.346345 QQQ back: the 0.000749 QQQ that did not
        # come back is what the volume cost, marked at the exit tick.
        expected_loss = Decimal("0.000749") * Decimal("721.00")
        self.assertAlmostEqual(report["totals"]["cost_usd"], float(expected_loss), places=4)
        # Volume is both legs, so the KPI denominator is 2 x notional.
        self.assertEqual(report["totals"]["round_trips"], 1)
        self.assertGreater(report["totals"]["round_trip_volume_usd"], 490)
        # Well under the owner's 0.3% ceiling, and reported as such.
        self.assertLess(report["totals"]["cost_per_1k_usd"], 2)
        self.assertFalse(report["totals"]["over_ceiling"])

    def test_cost_lands_on_the_day_the_rotation_closed(self):
        events, history = baseline_round_trip()
        days = {day["date"]: day for day in report_for(events, history)["days"]}

        # Entry day traded real volume but priced nothing: a rotation that is
        # still open has no cost yet, and pretending otherwise would make the
        # KPI swing on the calendar rather than on execution.
        self.assertEqual(days["2026-09-04"]["swaps"], 1)
        self.assertEqual(days["2026-09-04"]["round_trips"], 0)
        self.assertIsNone(days["2026-09-04"]["cost_per_1k_usd"])
        self.assertEqual(days["2026-09-05"]["round_trips"], 1)
        self.assertIsNotNone(days["2026-09-05"]["cost_per_1k_usd"])

    def test_an_entry_still_open_is_reported_unpaired_never_priced(self):
        events, history = baseline_round_trip()
        report = report_for(events[:1], history[:1])
        self.assertEqual(report["totals"]["round_trips"], 0)
        self.assertIsNone(report["totals"]["cost_per_1k_usd"])
        self.assertEqual(report["days"][0]["open_legs"], [8])

    def test_actual_balance_deltas_win_over_the_planned_amounts(self):
        """The plan is an intent; only the wallet says what was swapped."""
        events, history = baseline_round_trip()
        # Half the buy leg actually landed, while the plan still says the full
        # amount. A ledger priced off the plan would report no loss at all.
        history[1] = attempt(9, EXIT_AT, sell="SPY", buy="QQQ",
                             sell_quantity="0.323269", buy_quantity="0.173172")
        report = report_for(events, history)
        self.assertGreater(report["totals"]["cost_usd"], 100)

    def test_a_swap_outside_the_event_window_is_named_not_dropped(self):
        events, history = baseline_round_trip()
        old = attempt(5, datetime(2026, 9, 3, 11, tzinfo=timezone.utc), sell="NVDA",
                      buy="AMD", sell_quantity="0.1", buy_quantity="0.2")
        report = report_for(events, [old] + history)
        self.assertEqual(report["ledger_swaps_outside_window"], [5])
        self.assertEqual(report["totals"]["swaps"], 2)

    def test_a_swap_dispatched_after_its_own_last_event_is_still_priced(self):
        """Coverage follows the marks, not the dispatch clock.

        live-tick commits the would-rotate event and only then dispatches the
        swap, so when the export ends on that event the dispatch is always
        later than the window's end. Gating on `dispatched_at` dropped the
        closing swap -- and with it the whole round trip -- even though the
        exact pricing event was right there in the verified stream.
        """
        events, history = baseline_round_trip()
        history[1] = attempt(9, EXIT_AT, sell="SPY", buy="QQQ",
                             sell_quantity="0.323269", buy_quantity="0.346345",
                             dispatched=EXIT_AT + timedelta(seconds=3))
        report = report_for(events, history)

        self.assertEqual(report["ledger_swaps_outside_window"], [])
        self.assertEqual(report["totals"]["swaps"], 2)
        self.assertEqual(report["totals"]["round_trips"], 1)

    def test_a_reconciled_attempt_still_in_active_is_counted(self):
        """The runtime can durably reconcile and exit before archiving.

        `resume_status_and_reconcile` documents that seam: the swap is on
        chain and reconciled while the attempt is still in `active`. Reading
        only `history` would report the closing leg as if it never happened.
        """
        events, history = baseline_round_trip()
        report = report_for(events, history[:1], active=history[1])

        self.assertEqual(report["totals"]["swaps"], 2)
        self.assertEqual(report["totals"]["round_trips"], 1)
        self.assertGreater(report["totals"]["cost_usd"], 0)

    def test_a_non_reconciled_active_attempt_is_ignored(self):
        events, history = baseline_round_trip()
        pending = attempt(9, EXIT_AT, sell="SPY", buy="QQQ", sell_quantity="0.323269",
                          buy_quantity="0.346345", phase="submitted")
        report = report_for(events, history[:1], active=pending)

        self.assertEqual(report["totals"]["swaps"], 1)
        self.assertEqual(report["totals"]["round_trips"], 0)

    def test_a_swap_inside_the_window_with_no_matching_event_is_an_error(self):
        events, history = baseline_round_trip()
        history[1]["intent"]["sell_amount_raw"] = raw("0.999999")
        with self.assertRaisesRegex(ledger_tool.ActivityLedgerError, "no would-rotate event"):
            report_for(events, history)

    def test_a_rotation_unwound_in_two_legs_is_one_round_trip(self):
        """A partial exit does not close a rotation.

        When the recorder's fixed-notional quote comes back smaller than the
        open quantity, the runtime keeps the remainder tracked as open and
        sells it on a later tick. Closing on the first exit priced the round
        trip from half its unwind and reported the other half as unpaired.
        """
        second_exit_at = EXIT_AT + timedelta(hours=1)
        events = [
            would_rotate_event(1, ENTRY_AT, trigger="entry_signal", sell="QQQ", buy="SPY",
                               sell_quantity="0.347094", buy_quantity="0.323269",
                               spy_mark="771.27", qqq_mark="720.265"),
            would_rotate_event(2, EXIT_AT, trigger="mean_reversion_exit", sell="SPY", buy="QQQ",
                               sell_quantity="0.161634", buy_quantity="0.173172",
                               spy_mark="773.50", qqq_mark="721.00"),
            would_rotate_event(3, second_exit_at, trigger="mean_reversion_exit", sell="SPY",
                               buy="QQQ", sell_quantity="0.161635", buy_quantity="0.173173",
                               spy_mark="773.50", qqq_mark="721.00"),
        ]
        history = [
            attempt(8, ENTRY_AT, sell="QQQ", buy="SPY", sell_quantity="0.347094",
                    buy_quantity="0.323269"),
            attempt(9, EXIT_AT, sell="SPY", buy="QQQ", sell_quantity="0.161634",
                    buy_quantity="0.173172", sell_before="0.323269", buy_before="0"),
            attempt(10, second_exit_at, sell="SPY", buy="QQQ", sell_quantity="0.161635",
                    buy_quantity="0.173173", sell_before="0.161635", buy_before="0.173172"),
        ]
        report = report_for(events, history)

        self.assertEqual(report["totals"]["round_trips"], 1)
        self.assertEqual(report["totals"]["swaps"], 3)
        # Nothing left over: both exit legs belong to the rotation.
        self.assertEqual([day["open_legs"] for day in report["days"]], [[], []])
        # Volume is all three legs, and the loss is the same 0.000749 QQQ
        # shortfall the single-exit case reports -- split across two exits,
        # netted before pricing rather than measured from the first leg only.
        expected_loss = Decimal("0.000749") * Decimal("721.00")
        self.assertAlmostEqual(report["totals"]["cost_usd"], float(expected_loss), places=4)
        # The same tokens moved, so the KPI denominator is the same as the
        # single-exit baseline -- splitting the unwind neither inflates nor
        # loses volume.
        one_leg = report_for(*baseline_round_trip())
        self.assertAlmostEqual(report["totals"]["round_trip_volume_usd"],
                               one_leg["totals"]["round_trip_volume_usd"], places=2)

    def test_a_rotation_left_partly_open_is_never_priced(self):
        events, history = baseline_round_trip()
        # The exit unwinds only half of what the entry bought, so the
        # rotation is still open when the window ends.
        events[1] = would_rotate_event(2, EXIT_AT, trigger="mean_reversion_exit", sell="SPY",
                                       buy="QQQ", sell_quantity="0.161634",
                                       buy_quantity="0.173172", spy_mark="773.50",
                                       qqq_mark="721.00")
        history[1] = attempt(9, EXIT_AT, sell="SPY", buy="QQQ", sell_quantity="0.161634",
                             buy_quantity="0.173172", sell_before="0.323269", buy_before="0")
        report = report_for(events, history)

        self.assertEqual(report["totals"]["round_trips"], 0)
        self.assertIsNone(report["totals"]["cost_per_1k_usd"])
        self.assertEqual(sorted(sum((day["open_legs"] for day in report["days"]), [])),
                         [8, 9])

    def test_two_indistinguishable_events_are_refused_not_guessed_between(self):
        """Recency is not proof of which plan was dispatched.

        The runtime keeps a config+plan digest precisely because
        venue/symbols/amount do not identify a plan, and that digest cannot
        be recomputed from the event stream. Two candidates inside the
        dispatch freshness bound therefore mean the marks are unprovable.
        """
        events, history = baseline_round_trip()
        events.insert(1, would_rotate_event(
            99, ENTRY_AT - timedelta(seconds=20), trigger="max_hold_exit", sell="QQQ", buy="SPY",
            sell_quantity="0.347094", buy_quantity="0.323269",
            spy_mark="999.00", qqq_mark="111.00"))
        with self.assertRaisesRegex(ledger_tool.ActivityLedgerError, "refusing rather than"):
            report_for(events, history)

    def test_an_older_identical_plan_outside_the_freshness_bound_is_not_a_candidate(self):
        """The runtime will not dispatch a plan older than 60s, so neither
        will this match one."""
        events, history = baseline_round_trip()
        events.insert(1, would_rotate_event(
            99, ENTRY_AT - timedelta(minutes=15), trigger="max_hold_exit", sell="QQQ", buy="SPY",
            sell_quantity="0.347094", buy_quantity="0.323269",
            spy_mark="999.00", qqq_mark="111.00"))
        report = report_for(events, history)
        self.assertEqual(report["totals"]["round_trips"], 1)

    def test_a_remapped_token_contract_is_not_the_same_swap(self):
        """Same symbols, different ERC-20: the runtime pins the address."""
        events, history = baseline_round_trip()
        events[1]["decision"]["plan"]["sell_token_address"] = (
            "0x9999999999999999999999999999999999999999")
        with self.assertRaisesRegex(ledger_tool.ActivityLedgerError, "no would-rotate event"):
            report_for(events, history)

    def test_a_rotation_that_opened_before_since_is_still_priced_on_its_close_day(self):
        """A reporting boundary must not delete the rotation it cuts.

        Round trips are assigned to the day they closed, so asking about
        that day has to carry the entry leg in with it. Filtering the entry
        out before pairing left the exit unpaired and took the whole loss
        and round-trip volume out of the very day being asked about --
        understating the ceiling and stop metric at every boundary.
        """
        events, history = baseline_round_trip()
        since = ENTRY_AT + timedelta(hours=1)
        report = report_for(events, history, since=since)

        self.assertEqual(report["totals"]["round_trips"], 1)
        expected_loss = Decimal("0.000749") * Decimal("721.00")
        self.assertAlmostEqual(report["totals"]["cost_usd"], float(expected_loss), places=4)
        # Both legs' notional is the denominator, even though only one leg
        # was dispatched inside the window.
        self.assertGreater(report["totals"]["round_trip_volume_usd"], 490)
        # The entry itself is not reported as a swap of the window, and it
        # is not reported as uncovered either -- the stream priced it.
        self.assertEqual(report["totals"]["swaps"], 1)
        self.assertEqual(report["ledger_swaps_outside_window"], [])
        self.assertEqual([day["open_legs"] for day in report["days"]], [[]])

    def test_the_markdown_total_puts_each_number_under_its_own_heading(self):
        events, history = baseline_round_trip()
        report = report_for(events, history)
        table = ledger_tool.render_markdown(report)
        header, _, *body = table.splitlines()
        columns = [c.strip() for c in header.strip("|").split("|")]
        total = [c.strip() for c in body[-1].strip("|").split("|")]
        self.assertEqual(len(total), len(columns))
        # The cost belongs under "cost $", not under "gas $": this run has
        # no gas at all, and reporting the trading loss there read as if
        # every dollar of it were gas.
        self.assertEqual(float(total[columns.index("gas $")]), 0.0)
        self.assertAlmostEqual(float(total[columns.index("cost $")]),
                               report["totals"]["cost_usd"], places=4)
        self.assertAlmostEqual(float(total[columns.index("RT loss $")]),
                               report["totals"]["round_trip_loss_usd"], places=4)

    def test_a_requested_bound_selects_on_the_dispatch_it_dates_rows_by(self):
        """Every date this report prints comes from the dispatch.

        Live-tick commits the pricing event and then dispatches, so the two
        can straddle midnight. Selecting on the observation while dating the
        row from the dispatch made `--since` drop a swap the report itself
        files under the requested day.
        """
        just_before_midnight = datetime(2026, 9, 4, 23, 59, 59, tzinfo=timezone.utc)
        just_after = datetime(2026, 9, 5, 0, 0, 1, tzinfo=timezone.utc)
        events = [would_rotate_event(1, just_before_midnight, trigger="entry_signal",
                                     sell="QQQ", buy="SPY", sell_quantity="0.347094",
                                     buy_quantity="0.323269", spy_mark="771.27",
                                     qqq_mark="720.265")]
        history = [attempt(8, just_before_midnight, sell="QQQ", buy="SPY",
                           sell_quantity="0.347094", buy_quantity="0.323269",
                           dispatched=just_after)]
        report = report_for(events, history,
                            since=datetime(2026, 9, 5, tzinfo=timezone.utc))

        self.assertEqual(report["totals"]["swaps"], 1)
        self.assertEqual([day["date"] for day in report["days"]], ["2026-09-05"])

    def test_gas_on_a_leg_that_closed_nothing_is_still_counted(self):
        """An open rotation's gas is money the wallet already paid."""
        events, history = baseline_round_trip()
        spent = str(int(WEI) - 10**15)
        history[0] = attempt(8, ENTRY_AT, sell="QQQ", buy="SPY", sell_quantity="0.347094",
                             buy_quantity="0.323269", gas_before=WEI, gas_after=spent)
        # Only the entry: the rotation is still open at the end of the window.
        report = ledger_tool.build_report({"history": history[:1]}, events[:1], CEILING,
                                          Decimal("4000"))

        day = report["days"][0]
        self.assertEqual(day["open_legs"], [8])
        # Kept, but as open-leg gas: it has no closed rotation to be part
        # of, so it must not enter the rate's numerator.
        self.assertEqual(day["open_leg_gas_wei"], "1000000000000000")
        self.assertAlmostEqual(day["open_leg_gas_usd"], 4.0, places=6)
        self.assertAlmostEqual(day["spent_usd"], 4.0, places=6)
        self.assertEqual(day["gas_wei"], "0")
        self.assertEqual(day["cost_usd"], 0.0)
        self.assertAlmostEqual(report["totals"]["spent_usd"], 4.0, places=6)
        # No rotation closed, so there is no volume to divide it by.
        self.assertIsNone(day["cost_per_1k_usd"])

    def test_a_stream_beginning_mid_rotation_refuses_a_stop_verdict(self):
        """An exit whose entry the stream lacks is a hole, not a zero.

        The rotation really closed and really cost something; its loss is in
        none of the figures. Publishing a KPI and a stop verdict anyway --
        the number that decides whether to keep funding the bot -- would be
        deciding on a window known to be short a rotation.
        """
        events, history = baseline_round_trip()
        # The export starts after the entry: only the closing observation
        # and only the closing attempt are present.
        report = report_for(events[1:], history[1:])

        self.assertFalse(report["coverage"]["complete"])
        self.assertEqual(report["coverage"]["exits_without_entry"], [9])
        self.assertTrue(report["stop_rule"]["undecidable"])
        self.assertFalse(report["stop_rule"]["stop_candidate"])
        self.assertIn("no stop verdict is given", ledger_tool.render_markdown(report))

    def test_a_streak_that_would_trip_the_stop_rule_is_withheld_while_incomplete(self):
        """The guard has to bind where it matters: on a firing streak.

        A window missing a rotation can still run a streak of over-ceiling
        days. Publishing `stop_candidate` from it would retire the bot on
        an arithmetic the report itself knows is short a trade.
        """
        # An exit with no entry ahead of an ordinary, complete rotation.
        orphan_at = ENTRY_AT - timedelta(days=1)
        events = [would_rotate_event(0, orphan_at, trigger="mean_reversion_exit", sell="SPY",
                                     buy="QQQ", sell_quantity="0.1", buy_quantity="0.1",
                                     spy_mark="770", qqq_mark="720")]
        history = [attempt(7, orphan_at, sell="SPY", buy="QQQ", sell_quantity="0.1",
                           buy_quantity="0.1")]
        baseline_events, baseline_history = baseline_round_trip()
        events += baseline_events
        history += baseline_history

        # Ceiling of zero makes the one closing day over-ceiling, and a
        # one-day rule makes that streak enough to fire.
        original = ledger_tool.STOP_RULE_CONSECUTIVE_DAYS
        ledger_tool.STOP_RULE_CONSECUTIVE_DAYS = 1
        try:
            report = ledger_tool.build_report({"history": history}, events,
                                              Decimal(0), NO_GAS)
            complete = ledger_tool.build_report({"history": baseline_history},
                                                baseline_events, Decimal(0), NO_GAS)
        finally:
            ledger_tool.STOP_RULE_CONSECUTIVE_DAYS = original

        # The same streak fires when the window is whole ...
        self.assertGreaterEqual(complete["stop_rule"]["longest_consecutive_days_over_ceiling"], 1)
        self.assertTrue(complete["stop_rule"]["stop_candidate"])
        # ... and is withheld when it is not.
        self.assertGreaterEqual(report["stop_rule"]["longest_consecutive_days_over_ceiling"], 1)
        self.assertFalse(report["stop_rule"]["stop_candidate"])
        self.assertTrue(report["stop_rule"]["undecidable"])

    def test_a_rotation_closing_after_until_is_reported_open_not_lost(self):
        """A bound that cuts a rotation must not delete its entry.

        Pairing runs over the whole stream, so the entry belongs to a trip
        the `--until` filter then removes. Asking the pairing pass for the
        open set would not know that, and the leg vanished from the report
        entirely -- neither priced nor reported open, with its gas gone.
        """
        events, history = baseline_round_trip()
        spent = str(int(WEI) - 10**15)
        history[0] = attempt(8, ENTRY_AT, sell="QQQ", buy="SPY", sell_quantity="0.347094",
                             buy_quantity="0.323269", gas_before=WEI, gas_after=spent)
        report = ledger_tool.build_report(
            {"history": history}, events, CEILING, Decimal("4000"),
            until=ENTRY_AT + timedelta(hours=1))

        self.assertEqual(report["totals"]["round_trips"], 0)
        self.assertEqual([day["date"] for day in report["days"]], ["2026-09-04"])
        day = report["days"][0]
        self.assertEqual(day["open_legs"], [8])
        self.assertAlmostEqual(day["open_leg_gas_usd"], 4.0, places=6)
        self.assertTrue(report["coverage"]["complete"])

    def test_an_open_leg_gas_never_enters_a_completed_trip_rate(self):
        """A day with both a close and a fresh open must not mix them.

        Dividing the open leg's gas by the closed rotation's volume can push
        an arm over the ceiling on a day that earned no such thing, and the
        stop rule is built on exactly that comparison.
        """
        events, history = baseline_round_trip()
        reopen_at = EXIT_AT + timedelta(hours=2)
        events.append(would_rotate_event(3, reopen_at, trigger="entry_signal", sell="QQQ",
                                         buy="SPY", sell_quantity="0.347094",
                                         buy_quantity="0.323269", spy_mark="773.50",
                                         qqq_mark="721.00"))
        spent = str(int(WEI) - 10**16)
        history.append(attempt(10, reopen_at, sell="QQQ", buy="SPY",
                               sell_quantity="0.347094", buy_quantity="0.323269",
                               gas_before=WEI, gas_after=spent))
        with_gas = ledger_tool.build_report({"history": history}, events, CEILING,
                                            Decimal("4000"))
        no_gas = report_for(*baseline_round_trip())

        close_day = [d for d in with_gas["days"] if d["date"] == "2026-09-05"][0]
        self.assertEqual(close_day["open_legs"], [10])
        self.assertGreater(close_day["open_leg_gas_usd"], 39)
        # The rate is the same as if the fresh entry had not happened.
        self.assertAlmostEqual(close_day["cost_per_1k_usd"],
                               no_gas["days"][1]["cost_per_1k_usd"], places=6)
        self.assertFalse(close_day["over_ceiling"])
        # But the money is not lost.
        self.assertGreater(close_day["spent_usd"], close_day["cost_usd"])

    def test_a_bound_reaching_past_the_events_withholds_the_verdict(self):
        """A swap inside the question the stream cannot price is a hole.

        Naming it in `ledger_swaps_outside_window` was never enough on its
        own: the caller asked about that day, the swap's cost is in none of
        the figures, and a definitive stop verdict was published anyway.
        """
        events, history = baseline_round_trip()
        later = EXIT_AT + timedelta(days=1)
        # A reconciled swap a day past the last observation, so nothing can
        # price it -- but `--until` reaches over it.
        history.append(attempt(10, later, sell="QQQ", buy="SPY", sell_quantity="0.5",
                               buy_quantity="0.4"))
        report = report_for(events, history, until=later + timedelta(hours=1))

        self.assertEqual(report["ledger_swaps_outside_window"], [10])
        self.assertEqual(report["coverage"]["requested_but_unpriceable"], [10])
        self.assertFalse(report["coverage"]["complete"])
        self.assertTrue(report["stop_rule"]["undecidable"])
        self.assertIn("cannot price at all", ledger_tool.render_markdown(report))

        # The same unpriceable swap outside the requested bounds is simply
        # not part of the question, and does not spoil the verdict.
        bounded = report_for(events, history, until=EXIT_AT + timedelta(hours=1))
        self.assertEqual(bounded["ledger_swaps_outside_window"], [10])
        self.assertTrue(bounded["coverage"]["complete"])
        self.assertFalse(bounded["stop_rule"]["undecidable"])

    def test_an_unpriceable_swap_outside_the_reported_window_is_not_a_hole(self):
        """With no bounds given, the question is the window actually reported.

        The execution ledger outlives each event export, so an unmatched
        attempt from an older probe is always sitting in it. Testing those
        against `(None, None)` admitted every one of them, and the routine
        no-bounds invocation therefore reported incomplete coverage and an
        `undecidable` stop verdict over a swap the report does not cover.
        """
        events, history = baseline_round_trip()
        old = attempt(5, ENTRY_AT - timedelta(days=30), sell="NVDA", buy="AMD",
                      sell_quantity="0.1", buy_quantity="0.2")
        report = report_for(events, [old] + history)

        # Still named, because it is a swap the ledger holds and cannot price.
        self.assertEqual(report["ledger_swaps_outside_window"], [5])
        # But it is outside what this report covers, so it is not a hole in it.
        self.assertEqual(report["coverage"]["requested_but_unpriceable"], [])
        self.assertTrue(report["coverage"]["complete"])
        self.assertFalse(report["stop_rule"]["undecidable"])

        # An explicit bound reaching back over it still makes it part of the
        # question, exactly as an `--until` past the events does.
        asked = report_for(events, [old] + history,
                           since=ENTRY_AT - timedelta(days=60))
        self.assertEqual(asked["coverage"]["requested_but_unpriceable"], [5])
        self.assertTrue(asked["stop_rule"]["undecidable"])

    def test_an_unreconciled_active_attempt_withholds_the_verdict(self):
        """`Confirmed` is persisted before reconciliation starts.

        A crash or provider failure in between leaves a transaction that
        may already be on chain in that phase; ignoring it left its leg
        out of every figure while the report still published a clean
        coverage flag and a definitive stop result.
        """
        events, history = baseline_round_trip()
        pending = attempt(11, ENTRY_AT + timedelta(minutes=1), sell="SPY", buy="QQQ",
                          sell_quantity="0.1", buy_quantity="0.2", phase="confirmed")
        report = report_for(events, history, active=pending)

        self.assertEqual(report["coverage"]["unresolved_attempts"],
                         [{"sequence": 11, "phase": "confirmed"}])
        self.assertFalse(report["coverage"]["complete"])
        self.assertTrue(report["stop_rule"]["undecidable"])
        self.assertIn("may be on chain but is not reconciled",
                      ledger_tool.render_markdown(report))

        # `failed` counts too: the ledger's own recovery contract says it
        # can hold a dispatched-but-unconfirmed transaction.
        failed = report_for(events, history, active=dict(pending, phase="failed"))
        self.assertEqual(failed["coverage"]["unresolved_attempts"],
                         [{"sequence": 11, "phase": "failed"}])

        # A phase that sent nothing is not a hole. `rejected` is the
        # router refusing before any transaction went out.
        for phase in ("prepared", "rejected", "operator_hold"):
            clean = report_for(events, history, active=dict(pending, phase=phase))
            self.assertEqual(clean["coverage"]["unresolved_attempts"], [], phase)
            self.assertTrue(clean["coverage"]["complete"], phase)

    def test_a_pending_attempt_outside_the_window_is_named_not_fatal(self):
        """A historical export must not be spoiled by activity after it.

        An attempt dispatched after the last observation could not have
        been priced by this export, so it belongs to the next report --
        but the operator is still told the ledger holds one.
        """
        events, history = baseline_round_trip()
        pending = attempt(11, EXIT_AT + timedelta(days=1), sell="SPY", buy="QQQ",
                          sell_quantity="0.1", buy_quantity="0.2", phase="confirmed")
        report = report_for(events, history, active=pending)

        self.assertEqual(report["coverage"]["unresolved_attempts"], [])
        self.assertEqual(report["coverage"]["pending_outside_window"],
                         [{"sequence": 11, "phase": "confirmed"}])
        self.assertTrue(report["coverage"]["complete"])
        self.assertFalse(report["stop_rule"]["undecidable"])
        self.assertIn("unfinished attempt outside this window",
                      ledger_tool.render_markdown(report))

    def test_plan_freshness_is_measured_from_the_quote_not_the_observation(self):
        """`validate_plan_age` measures from `plan.quote_received_at`.

        An event observed recently whose quote was already too old to
        dispatch cannot have produced the attempt, so matching on the
        observation admits a candidate the runtime would have refused.
        """
        events, history = baseline_round_trip()
        stale = events[0]["decision"]["plan"]
        # Observed 1s before preparation (fresh), quoted 10 minutes before
        # (far past HARD_MAX_PLAN_AGE_SECS).
        stale["quote_received_at"] = stamp(ENTRY_AT - timedelta(minutes=10))
        with self.assertRaises(ledger_tool.ActivityLedgerError) as caught:
            report_for(events, history)
        self.assertIn("no would-rotate event", str(caught.exception))

        # A quote inside the bound still matches.
        stale["quote_received_at"] = stamp(ENTRY_AT - timedelta(seconds=1))
        report = report_for(events, history)
        self.assertEqual(report["totals"]["round_trips"], 1)

    def test_plan_freshness_is_measured_at_dispatch(self):
        """The runtime re-validates the plan immediately before dispatch.

        A quote inside the bound at preparation but outside it by the
        time the attempt was dispatched was refused by the runtime, so an
        event carrying it cannot be the one that produced this swap.
        """
        events, history = baseline_round_trip()
        plan = events[0]["decision"]["plan"]
        plan["quote_received_at"] = stamp(ENTRY_AT - timedelta(seconds=30))
        # Prepared while the quote was 30s old (inside the bound), then
        # dispatched much later, by which time it was not.
        history[0] = attempt(8, ENTRY_AT, sell="QQQ", buy="SPY",
                             sell_quantity="0.347094", buy_quantity="0.323269",
                             dispatched=ENTRY_AT + timedelta(minutes=5))
        with self.assertRaises(ledger_tool.ActivityLedgerError):
            report_for(events, history)

    def test_an_unmatched_attempt_outside_the_requested_bounds_is_not_fatal(self):
        """A stream can span more than the report the caller asked for.

        A manual or offline attempt with no `would_rotate` event, outside
        the requested bounds, is something `--since` promises to ignore --
        it must not refuse to produce the report.
        """
        events, history = baseline_round_trip()
        orphan_at = ENTRY_AT + timedelta(minutes=5)
        history.append(attempt(20, orphan_at, sell="NVDA", buy="AMD",
                               sell_quantity="0.1", buy_quantity="0.2"))
        # Inside the stream and inside the question: still a hard error.
        with self.assertRaises(ledger_tool.ActivityLedgerError):
            report_for(events, history)
        # Inside the stream but outside the requested bounds: reported,
        # not fatal.
        report = report_for(events, history, since=orphan_at + timedelta(minutes=1))
        self.assertIn(20, report["ledger_swaps_outside_window"])

    def test_a_gas_top_up_across_a_swap_is_not_a_negative_cost(self):
        """A wallet inflow between the two snapshots is not cheaper gas.

        Reconciliation validates only the sell and buy legs, so an attempt
        whose native balance rose is still accepted; the delta then went
        into `cost_usd` as a credit and could suppress a real stop signal.
        """
        events, history = baseline_round_trip()
        # The wallet gains 1 native token across the closing swap.
        history[1] = attempt(9, EXIT_AT, sell="SPY", buy="QQQ",
                             sell_quantity="0.323269", buy_quantity="0.346345",
                             gas_after=str(int(WEI) + 10**18))

        report = report_for(events, history)
        self.assertEqual(report["coverage"]["gas_unmeasurable"], [9])
        self.assertFalse(report["coverage"]["complete"])
        self.assertTrue(report["stop_rule"]["undecidable"])
        # No credit: the unknown gas is zero, never negative.
        self.assertGreaterEqual(report["totals"]["gas_usd"], 0)
        self.assertIn("topped up across them", ledger_tool.render_markdown(report))

    def test_a_carry_in_leg_with_unreadable_gas_still_withholds_the_verdict(self):
        """A rotation carries its entry in even when `--since` excludes it.

        The entry leg is filtered out of `swaps` but stays in the
        retained round trip, so its zeroed gas is in the cost the report
        publishes -- and the scan that withholds the verdict has to see
        it too.
        """
        events, history = baseline_round_trip()
        # The entry's wallet gains a native token; `--since` then starts
        # after that entry, so only the close is an in-window swap.
        history[0] = attempt(8, ENTRY_AT, sell="QQQ", buy="SPY",
                             sell_quantity="0.347094", buy_quantity="0.323269",
                             gas_after=str(int(WEI) + 10**18))
        report = report_for(events, history, since=ENTRY_AT + timedelta(hours=1))

        self.assertEqual(report["totals"]["round_trips"], 1,
                         "the rotation still closes inside the window")
        self.assertEqual(report["coverage"]["gas_unmeasurable"], [8])
        self.assertFalse(report["coverage"]["complete"])
        self.assertTrue(report["stop_rule"]["undecidable"])

    def test_an_explicit_until_is_never_moved_forward(self):
        """`--until` before the stream must not be reported as after it."""
        events, history = baseline_round_trip()
        cutoff = ENTRY_AT - timedelta(days=1)
        report = report_for(events, history, until=cutoff)
        self.assertEqual(
            report["window"]["to"], cutoff.isoformat().replace("+00:00", "Z"))
        self.assertLessEqual(report["window"]["from"], report["window"]["to"])
        self.assertEqual(report["days"], [])

    def test_a_one_sided_bound_stays_open_on_the_side_the_caller_left_open(self):
        """`--since X` with no `--until` asks about everything after X.

        Filling the missing endpoint from the stream clamps that open
        side back to the last observation, so a reconciled attempt after
        the export -- exactly what a mid-window run produces -- fell
        outside the coverage check while the report still published
        `complete` and a definitive stop verdict.
        """
        events, history = baseline_round_trip()
        later = EXIT_AT + timedelta(days=1)
        history.append(attempt(10, later, sell="QQQ", buy="SPY", sell_quantity="0.5",
                               buy_quantity="0.4"))

        open_ended = report_for(events, history, since=ENTRY_AT - timedelta(hours=1))
        self.assertEqual(open_ended["coverage"]["requested_but_unpriceable"], [10])
        self.assertFalse(open_ended["coverage"]["complete"])
        self.assertTrue(open_ended["stop_rule"]["undecidable"])

        # The mirror image: `--until` after the swap, open start.
        old = attempt(5, ENTRY_AT - timedelta(days=30), sell="NVDA", buy="AMD",
                      sell_quantity="0.1", buy_quantity="0.2")
        open_start = report_for(events, [old] + history,
                                until=later + timedelta(hours=1))
        self.assertEqual(open_start["coverage"]["requested_but_unpriceable"], [5, 10])

    def test_the_reported_window_is_never_inverted(self):
        """`--since` after the last observation is now a supported case."""
        just_before_midnight = datetime(2026, 9, 4, 23, 59, 59, tzinfo=timezone.utc)
        just_after = datetime(2026, 9, 5, 0, 0, 1, tzinfo=timezone.utc)
        events = [would_rotate_event(1, just_before_midnight, trigger="entry_signal",
                                     sell="QQQ", buy="SPY", sell_quantity="0.347094",
                                     buy_quantity="0.323269", spy_mark="771.27",
                                     qqq_mark="720.265")]
        history = [attempt(8, just_before_midnight, sell="QQQ", buy="SPY",
                           sell_quantity="0.347094", buy_quantity="0.323269",
                           dispatched=just_after)]
        report = report_for(events, history,
                            since=datetime(2026, 9, 5, tzinfo=timezone.utc))

        self.assertLessEqual(report["window"]["from"], report["window"]["to"])
        # And it covers the row it emitted, rather than ending before it.
        self.assertGreaterEqual(report["window"]["to"], "2026-09-05T00:00:01Z")

    def test_the_freshness_bound_truncates_the_way_the_runtime_does(self):
        """A swap the runtime accepted must not become unmatchable here.

        `validate_plan_age` compares `num_seconds()`, which truncates, so a
        plan prepared 60.4s after its observation clears a 60s cap and is
        persisted. Comparing exact datetimes rejected that same event and
        turned a real, already-executed swap into an unpriceable one.
        """
        events, history = baseline_round_trip()
        # Prepared 60.4s after the pricing observation: inside the runtime's
        # bound, outside an exact-datetime one.
        history[0] = attempt(8, ENTRY_AT + timedelta(seconds=60, milliseconds=400),
                             sell="QQQ", buy="SPY", sell_quantity="0.347094",
                             buy_quantity="0.323269")
        report = report_for(events, history)
        self.assertEqual(report["totals"]["round_trips"], 1)
        self.assertEqual(report["ledger_swaps_outside_window"], [])

        # 61.0s is genuinely past it, and the runtime would not have
        # dispatched: still refused.
        events, history = baseline_round_trip()
        history[0] = attempt(8, ENTRY_AT + timedelta(seconds=61), sell="QQQ", buy="SPY",
                             sell_quantity="0.347094", buy_quantity="0.323269")
        with self.assertRaisesRegex(ledger_tool.ActivityLedgerError, "no would-rotate event"):
            report_for(events, history)

    def test_an_impossible_price_or_ceiling_is_refused(self):
        """A definitive verdict must not come out of an impossible input."""
        parser = ledger_tool.build_parser()
        for argument, value in (("--gas-price-usd", "-1"), ("--ceiling-per-1k", "-0.5"),
                                ("--gas-price-usd", "NaN"), ("--gas-price-usd", "Infinity"),
                                ("--ceiling-per-1k", "cheap")):
            with self.assertRaises(SystemExit):
                parser.parse_args(["events.jsonl", "--ledger", "l.json", argument, value])
        ok = parser.parse_args(["events.jsonl", "--ledger", "l.json",
                                "--gas-price-usd", "0", "--ceiling-per-1k", "3"])
        self.assertEqual(ok.gas_price_usd, Decimal(0))
        self.assertEqual(ok.ceiling_per_1k, Decimal(3))

    def test_only_reconciled_attempts_are_counted(self):
        events, history = baseline_round_trip()
        history[1]["phase"] = "rejected"
        report = report_for(events, history)
        self.assertEqual(report["totals"]["swaps"], 1)
        self.assertEqual(report["totals"]["round_trips"], 0)

    def test_non_eighteen_decimal_tokens_are_read_from_the_plan(self):
        entry = would_rotate_event(1, ENTRY_AT, trigger="entry_signal", sell="QQQ", buy="SPY",
                                   sell_quantity="0.347094", buy_quantity="0.323269",
                                   spy_mark="771.27", qqq_mark="720.265", decimals=6)
        history = [attempt(8, ENTRY_AT, sell="QQQ", buy="SPY", sell_quantity="0.347094",
                           buy_quantity="0.323269", decimals=6)]
        report = report_for([entry], history)
        # Priced identically to the 18-decimal case: the exponent came from
        # the plan, not from an assumption.
        self.assertAlmostEqual(report["days"][0]["swap_volume_usd"],
                               float(Decimal("0.347094") * Decimal("720.265")), places=2)

    def test_a_quiet_day_neither_breaks_nor_extends_the_stop_rule_streak(self):
        expensive = ledger_tool.DailyRow(date="d", round_trips=1,
                                         round_trip_volume_usd=Decimal(1000),
                                         round_trip_loss_usd=Decimal(4))
        quiet = ledger_tool.DailyRow(date="q")
        cheap = ledger_tool.DailyRow(date="c", round_trips=1,
                                     round_trip_volume_usd=Decimal(1000),
                                     round_trip_loss_usd=Decimal(1))
        self.assertEqual(
            ledger_tool.consecutive_days_over([expensive, quiet, expensive], CEILING), 2)
        self.assertEqual(
            ledger_tool.consecutive_days_over([expensive, cheap, expensive], CEILING), 1)

    def test_seven_expensive_days_make_the_bot_a_stop_candidate(self):
        rows = [ledger_tool.DailyRow(date=f"d{index}", round_trips=1,
                                     round_trip_volume_usd=Decimal(1000),
                                     round_trip_loss_usd=Decimal(4))
                for index in range(ledger_tool.STOP_RULE_CONSECUTIVE_DAYS)]
        self.assertEqual(
            ledger_tool.consecutive_days_over(rows, CEILING),
            ledger_tool.STOP_RULE_CONSECUTIVE_DAYS)

    def test_gas_is_only_charged_when_the_wallet_actually_paid_it(self):
        events, history = baseline_round_trip()
        history[1] = attempt(9, EXIT_AT, sell="SPY", buy="QQQ", sell_quantity="0.323269",
                             buy_quantity="0.346345", gas_before="2000000000000000000",
                             gas_after="1000000000000000000")
        priced = ledger_tool.build_report({"history": history}, events, CEILING, Decimal("2500"))
        self.assertAlmostEqual(priced["days"][-1]["gas_usd"], 2500.0, places=4)
        # The router has paid gas on every swap so far, so the default refuses
        # to invent a native-token mark rather than guessing one.
        self.assertEqual(report_for(events, history)["days"][-1]["gas_usd"], 0.0)

    def test_the_report_runs_end_to_end_over_a_verified_stream(self):
        events, history = baseline_round_trip()
        stream = []
        previous = None
        renumbered = [observe_event(1, ENTRY_AT - timedelta(minutes=1))]
        for offset, item in enumerate(events, start=2):
            renumbered.append({**item, "sequence": offset})
        for item in renumbered:
            payload = json.dumps(item, separators=(",", ":"))
            event_hash = event_stream.sha256_prefixed(payload.encode())
            chain = event_stream.chain_sha256(previous, event_hash)
            stream.append(json.dumps({
                "schema_version": 1,
                "previous_chain_sha256": previous,
                "event_sha256": event_hash,
                "chain_sha256": chain,
                "event_json": payload,
            }, separators=(",", ":")))
            previous = chain
        with tempfile.TemporaryDirectory() as directory:
            stream_path = Path(directory) / "events.jsonl"
            stream_path.write_text("\n".join(stream) + "\n", encoding="utf-8")
            ledger_path = Path(directory) / "ledger.json"
            ledger_path.write_text(json.dumps({"history": history}), encoding="utf-8")
            arguments = ledger_tool.build_parser().parse_args(
                [str(stream_path), "--ledger", str(ledger_path)])
            report = ledger_tool.run(arguments)
        self.assertEqual(report["totals"]["round_trips"], 1)
        self.assertIn("| date |", ledger_tool.render_markdown(report))


event_stream = ledger_tool.event_stream


if __name__ == "__main__":
    unittest.main()
