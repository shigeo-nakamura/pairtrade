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
        self.assertEqual(report["days"][0]["unpaired_swaps"], [8])

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
