#!/usr/bin/env python3
"""Unit tests for the Engine B Step 0 absorption test (bot-strategy#988)."""

from __future__ import annotations

import importlib.util
import json
import math
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent


def load(name: str):
    spec = importlib.util.spec_from_file_location(name, SCRIPT_DIR / ("%s.py" % name))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


absorption = load("engine_b_step0_absorption")
extract = load("engine_b_step0_extract")

US = 1_000_000


def price_row(date, point, symbol, price, price_type="mid", lag=0.0):
    return {
        "status": "ok",
        "date": date,
        "point": point,
        "symbol": symbol,
        "price_type": price_type,
        "price": str(price),
        "lag_secs": lag,
    }


def prices_from(rows):
    return {
        (r["date"], r["point"], r["symbol"], r["price_type"]): r for r in rows
    }


def full_day(date, kr, us, price_type="mid", lag=0.0):
    """kr/us are (t0, t1, t2) price triples."""
    rows = []
    for symbol, triple in (("KR", kr), ("US", us)):
        for point, value in zip(absorption.POINTS, triple):
            rows.append(price_row(date, point, symbol, value, price_type, lag))
    return rows


class ChiSquareTest(unittest.TestCase):
    def test_quantiles_match_published_tables(self):
        for df, p, expected in (
            (1, 0.95, 3.8415),
            (1, 0.05, 0.003932),
            (2, 0.95, 5.9915),
            (2, 0.05, 0.10259),
            (5, 0.95, 11.0705),
            (10, 0.05, 3.9403),
            (30, 0.95, 43.7730),
        ):
            got = absorption.chi2_quantile(p, df)
            self.assertAlmostEqual(
                got, expected, delta=max(1e-4, expected * 5e-4), msg="df=%d p=%s" % (df, p)
            )

    def test_quantile_rejects_bad_input(self):
        with self.assertRaises(ValueError):
            absorption.chi2_quantile(0.0, 3)
        with self.assertRaises(ValueError):
            absorption.chi2_quantile(0.5, 0)

    def test_ci_brackets_the_point_estimate_and_widens_as_n_falls(self):
        lo_small, hi_small = absorption.sd_ci(100.0, df=2)
        lo_big, hi_big = absorption.sd_ci(100.0, df=30)
        self.assertLess(lo_small, 100.0)
        self.assertGreater(hi_small, 100.0)
        # Fewer degrees of freedom must mean a wider interval, which is the
        # whole reason the interval is reported at n=3.
        self.assertLess(lo_small, lo_big)
        self.assertGreater(hi_small, hi_big)

    def test_ci_matches_the_closed_form(self):
        sd, df = 42.0, 4
        lo, hi = absorption.sd_ci(sd, df)
        self.assertAlmostEqual(lo, sd * math.sqrt(df / absorption.chi2_quantile(0.95, df)), places=9)
        self.assertAlmostEqual(hi, sd * math.sqrt(df / absorption.chi2_quantile(0.05, df)), places=9)

    def test_ci_is_none_without_degrees_of_freedom(self):
        self.assertIsNone(absorption.sd_ci(10.0, df=0))
        self.assertIsNone(absorption.sd_ci(None, df=5))


class StatisticsTest(unittest.TestCase):
    def test_stdev_honours_ddof_and_centring(self):
        values = [1.0, 2.0, 3.0, 4.0]
        self.assertAlmostEqual(absorption.stdev(values), math.sqrt(5.0 / 3.0))
        self.assertAlmostEqual(absorption.stdev(values, ddof=2), math.sqrt(5.0 / 2.0))
        # Uncentred: residuals are already centred by the fit, so their mean
        # must not be re-estimated.
        self.assertAlmostEqual(
            absorption.stdev([2.0, -2.0], ddof=1, centred=False), math.sqrt(8.0)
        )
        self.assertIsNone(absorption.stdev([1.0], ddof=1))
        self.assertIsNone(absorption.stdev([1.0, 2.0], ddof=2))

    def test_ols_recovers_an_exact_line(self):
        x = [1.0, 2.0, 3.0, 4.0]
        y = [3.0 + 2.0 * v for v in x]
        fit = absorption.ols(y, x)
        self.assertAlmostEqual(fit["beta"], 2.0)
        self.assertAlmostEqual(fit["alpha"], 3.0)
        self.assertAlmostEqual(fit["r2"], 1.0)
        self.assertTrue(all(abs(r) < 1e-12 for r in fit["resid"]))

    def test_ols_needs_spread_and_three_points(self):
        self.assertIsNone(absorption.ols([1.0, 2.0, 3.0], [5.0, 5.0, 5.0]))
        self.assertIsNone(absorption.ols([1.0, 2.0], [1.0, 2.0]))

    def test_correlation_matches_a_known_value(self):
        self.assertAlmostEqual(
            absorption.correlation([1.0, 2.0, 3.0], [2.0, 4.0, 6.0]), 1.0
        )
        self.assertAlmostEqual(
            absorption.correlation([1.0, 2.0, 3.0], [6.0, 4.0, 2.0]), -1.0
        )
        self.assertIsNone(absorption.correlation([1.0, 2.0, 3.0], [1.0, 1.0, 1.0]))


class SessionBuildingTest(unittest.TestCase):
    def test_partial_day_still_feeds_the_statistic_it_can_support(self):
        rows = full_day("2026-09-08", (100.0, 101.0, 102.0), (200.0, 202.0, 204.0))
        # A day whose t0 is missing can still produce fwd, but not a regression
        # point -- this is what keeps n(fwd) above n(regression).
        rows += [
            price_row("2026-09-09", "t1", "US", 200.0),
            price_row("2026-09-09", "t2", "US", 210.0),
        ]
        sessions, notes = build(rows)
        by_date = {s["date"]: s for s in sessions}
        self.assertIsNotNone(by_date["2026-09-09"]["fwd"])
        self.assertIsNone(by_date["2026-09-09"]["r_kr"])
        self.assertIsNone(by_date["2026-09-09"]["r_us_conc"])
        stats = absorption.analyse(sessions)
        self.assertEqual(stats["n_fwd"], 2)
        self.assertEqual(stats["n_regression"], 1)
        self.assertTrue(any(n["date"] == "2026-09-09" for n in notes))

    def test_stale_quote_is_dropped_with_a_reason(self):
        rows = full_day("2026-09-08", (100.0, 101.0, 102.0), (200.0, 202.0, 204.0))
        rows[0]["lag_secs"] = 900.0  # KR@t0 far from the instant
        sessions, notes = build(rows)
        self.assertIsNone(sessions[0]["r_kr"])
        self.assertIsNotNone(sessions[0]["fwd"])
        self.assertIn("stale", " ".join(notes[0]["why"]))

    def test_non_positive_price_is_dropped(self):
        rows = full_day("2026-09-08", (100.0, 101.0, 102.0), (200.0, 202.0, 204.0))
        rows[3]["price"] = "0"  # US@t0
        sessions, notes = build(rows)
        self.assertIsNone(sessions[0]["r_us_conc"])
        self.assertIsNotNone(sessions[0]["fwd"])
        self.assertIn("non_positive", " ".join(notes[0]["why"]))

    def test_day_with_no_usable_leg_is_reported_not_returned(self):
        rows = [price_row("2026-09-08", "t0", "US", 100.0)]
        sessions, notes = build(rows)
        self.assertEqual(sessions, [])
        self.assertEqual(notes[0]["usable"], "none")

    def test_price_types_do_not_leak_into_each_other(self):
        rows = full_day("2026-09-08", (100.0, 101.0, 102.0), (200.0, 202.0, 204.0), "mid")
        rows += full_day("2026-09-08", (100.0, 110.0, 121.0), (200.0, 220.0, 242.0), "mark")
        mid, _ = build(rows, price_type="mid")
        mark, _ = build(rows, price_type="mark")
        self.assertAlmostEqual(mid[0]["r_kr"], math.log(101.0 / 100.0))
        self.assertAlmostEqual(mark[0]["r_kr"], math.log(110.0 / 100.0))

    def test_returns_are_log_returns_of_the_right_legs(self):
        rows = full_day("2026-09-08", (100.0, 101.0, 102.0), (200.0, 202.0, 204.0))
        sessions, _ = build(rows)
        self.assertAlmostEqual(sessions[0]["r_kr"], math.log(101.0 / 100.0))
        self.assertAlmostEqual(sessions[0]["r_us_conc"], math.log(202.0 / 200.0))
        self.assertAlmostEqual(sessions[0]["fwd"], math.log(204.0 / 202.0))


def build(rows, price_type="mid", max_lag=120.0):
    return absorption.build_sessions(prices_from(rows), "KR", "US", price_type, max_lag)


class VerdictTest(unittest.TestCase):
    @staticmethod
    def sessions_from(triples):
        """triples: [(r_kr, r_us_conc, fwd)] in raw log-return units."""
        return [
            {"date": "d%d" % i, "r_kr": a, "r_us_conc": b, "fwd": c}
            for i, (a, b, c) in enumerate(triples)
        ]

    def test_k0a_kills_when_the_forward_move_is_smaller_than_the_cost(self):
        # fwd dispersion of ~1 bp: nothing to capture.
        sessions = self.sessions_from(
            [
                (0.02, 0.01, 0.0001),
                (-0.02, -0.01, 0.0000),
                (0.01, 0.02, 0.0002),
                (-0.01, 0.00, 0.0001),
            ]
        )
        stats = absorption.analyse(sessions)
        result = absorption.verdict(stats)
        self.assertEqual(result["k0a"], "kill")
        self.assertTrue(result["killed"])
        self.assertEqual(result["decision"], absorption.DECISION_KILL)
        # A kill is only called when the whole interval is below the threshold.
        self.assertLess(stats["sd_fwd_bps_ci90"][1], absorption.KILL_SD_BPS)

    def test_k0b_kills_when_the_us_leg_has_already_absorbed_the_kr_move(self):
        # r_kr is r_us_conc plus a sub-bp wobble: R^2 ~ 1 and no residual width.
        base = [0.02, -0.015, 0.01, -0.005, 0.03, -0.02]
        wobble = [1e-5, -1e-5, 5e-6, -5e-6, 2e-6, -2e-6]
        sessions = self.sessions_from(
            [(b + w, b, 0.01 * (i % 3 - 1)) for i, (b, w) in enumerate(zip(base, wobble))]
        )
        stats = absorption.analyse(sessions)
        result = absorption.verdict(stats)
        self.assertGreaterEqual(stats["r2"], absorption.KILL_R2)
        self.assertLess(stats["sd_eps_bps"], absorption.KILL_SD_BPS)
        self.assertEqual(result["k0b"], "kill")
        self.assertTrue(result["killed"])
        self.assertTrue(result["k0b_beta_variants_agree"])

    def test_wide_residual_with_high_r2_does_not_kill(self):
        # Absorbed but still wide: K0-b needs *both* conditions.
        base = [0.02, -0.015, 0.01, -0.005, 0.03, -0.02]
        wobble = [0.008, -0.008, 0.004, -0.004, 0.002, -0.002]
        fwd = [0.012, -0.008, 0.02, -0.015, 0.01, -0.02]
        sessions = self.sessions_from(
            [(b + w, b, f) for b, w, f in zip(base, wobble, fwd)]
        )
        stats = absorption.analyse(sessions)
        result = absorption.verdict(stats)
        self.assertGreaterEqual(stats["r2"], absorption.KILL_R2)
        self.assertGreater(stats["sd_eps_bps"], absorption.KILL_SD_BPS)
        self.assertEqual(result["k0b"], "cleared")
        self.assertFalse(result["killed"])
        self.assertEqual(result["decision"], absorption.DECISION_PROCEED)

    def test_volatile_pair_proceeds_and_reports_the_ci_as_clear(self):
        sessions = self.sessions_from(
            [
                (0.02, 0.01, 0.012),
                (-0.02, -0.01, -0.008),
                (0.01, 0.02, 0.02),
                (-0.01, 0.00, -0.015),
            ]
        )
        stats = absorption.analyse(sessions)
        result = absorption.verdict(stats)
        self.assertFalse(result["killed"])
        self.assertEqual(result["k0a"], "cleared")
        self.assertEqual(result["decision"], absorption.DECISION_PROCEED)

    def test_missing_statistics_are_unresolved_not_a_pass(self):
        result = absorption.verdict(
            {"sd_fwd_bps": None, "sd_eps_bps": None, "sd_eps_issue_beta_bps": None, "r2": None}
        )
        self.assertFalse(result["killed"])
        self.assertEqual(result["k0a"], "unresolved")
        self.assertEqual(result["k0b"], "unresolved")
        # No data must never read as "proceed".
        self.assertEqual(result["decision"], absorption.DECISION_UNRESOLVED)

    def test_interval_straddling_the_threshold_is_unresolved(self):
        # Point estimate below the cutoff, interval spanning it: the sample
        # cannot tell a dead strategy from a live one, and must say so.
        stats = {
            "sd_fwd_bps": 12.0,
            "sd_fwd_bps_ci90": [6.0, 40.0],
            "sd_eps_bps": 500.0,
            "sd_eps_bps_ci90": [300.0, 900.0],
            "sd_eps_issue_beta_bps": 500.0,
            "r2": 0.2,
        }
        result = absorption.verdict(stats)
        self.assertEqual(result["k0a"], "unresolved")
        self.assertTrue(result["k0a_point_estimate_kills"])
        self.assertFalse(result["killed"])
        self.assertEqual(result["decision"], absorption.DECISION_UNRESOLVED)

    def test_k0b_needs_both_beta_conventions_to_kill(self):
        # Primary residual tiny, the issue's literal-beta residual wide: the
        # ambiguity in the formula would be deciding, not the data.
        stats = {
            "sd_fwd_bps": 200.0,
            "sd_fwd_bps_ci90": [150.0, 400.0],
            "sd_eps_bps": 2.0,
            "sd_eps_bps_ci90": [1.0, 6.0],
            "sd_eps_issue_beta_bps": 90.0,
            "r2": 0.95,
        }
        result = absorption.verdict(stats)
        self.assertFalse(result["k0b_beta_variants_agree"])
        self.assertNotEqual(result["k0b"], "kill")
        self.assertFalse(result["killed"])
        self.assertEqual(result["decision"], absorption.DECISION_UNRESOLVED)

    def test_k0b_clearance_also_needs_both_beta_conventions(self):
        # Primary residual interval wholly above the cutoff, the alternate
        # convention's wholly below it: the two disagree about the *direction*,
        # so neither answer is the data's.
        stats = {
            "sd_fwd_bps": 200.0,
            "sd_fwd_bps_ci90": [150.0, 400.0],
            "sd_eps_bps": 90.0,
            "sd_eps_bps_ci90": [40.0, 200.0],
            "sd_eps_issue_beta_bps": 3.0,
            "sd_eps_issue_beta_bps_ci90": [1.0, 8.0],
            "r2": 0.95,
        }
        result = absorption.verdict(stats)
        self.assertEqual(result["k0b_primary"], "cleared")
        self.assertEqual(result["k0b_issue_beta"], "kill")
        self.assertEqual(result["k0b"], "unresolved")
        self.assertFalse(result["killed"])
        self.assertEqual(result["decision"], absorption.DECISION_UNRESOLVED)

    def test_a_low_r2_alone_does_not_clear_k0b(self):
        # R^2 has no interval here, so only the residual's width can rule the
        # kill out; a point R^2 under the gate is not evidence at n=3.
        stats = {
            "sd_fwd_bps": 200.0,
            "sd_fwd_bps_ci90": [150.0, 400.0],
            "sd_eps_bps": 8.0,
            "sd_eps_bps_ci90": [4.0, 30.0],
            "sd_eps_issue_beta_bps": 8.0,
            "r2": 0.1,
        }
        result = absorption.verdict(stats)
        self.assertEqual(result["k0b"], "unresolved")
        self.assertEqual(result["decision"], absorption.DECISION_UNRESOLVED)

    def test_residual_dispersion_uses_the_regression_degrees_of_freedom(self):
        sessions = self.sessions_from(
            [(0.02, 0.01, 0.01), (-0.02, -0.01, -0.01), (0.01, 0.02, 0.02)]
        )
        stats = absorption.analyse(sessions)
        fit = absorption.ols(
            [s["r_kr"] for s in sessions], [s["r_us_conc"] for s in sessions]
        )
        expected = math.sqrt(sum(r * r for r in fit["resid"]) / (3 - 2)) / absorption.BPS
        self.assertAlmostEqual(stats["sd_eps_bps"], expected, places=6)


class ExtractTest(unittest.TestCase):
    def test_partition_name_is_the_utc_hour(self):
        # 2026-09-10 13:30:00 UTC -> the 13:00 partition.
        self.assertEqual(extract.partition_name(1789047000 * US), "20260910_13")
        self.assertEqual(extract.partition_name(1788998400 * US), "20260910_00")

    def test_a_windows_partitions_include_the_hour_before_the_instant(self):
        t0 = 1788998400 * US  # 2026-09-10 00:00:00 UTC, an hour *and* day edge
        self.assertEqual(
            extract.partitions_for_window(t0, 300 * US),
            ["20260909_23", "20260910_00"],
        )
        # Mid-hour instants stay in one partition.
        self.assertEqual(
            extract.partitions_for_window(1789021800 * US, 300 * US), ["20260910_06"]
        )
        # A tolerance wider than an hour cannot skip an intervening partition.
        self.assertEqual(
            extract.partitions_for_window(1789021800 * US, 3600 * US),
            ["20260910_05", "20260910_06", "20260910_07"],
        )

    def test_session_points_require_both_markets_open(self):
        calendar = {
            "sessions": {
                "2026-09-10": {
                    "krx_is_open": True,
                    "us_is_open": True,
                    "krx_open_utc_us": 1,
                    "krx_close_utc_us": 2,
                    "us_open_utc_us": 3,
                },
                # KRX open, US Labor Day. The us_open timestamp is deliberately
                # non-null here: it is the is_open flag that must decide, not
                # the presence of a field.
                "2026-09-07": {
                    "krx_is_open": True,
                    "us_is_open": False,
                    "krx_open_utc_us": 1,
                    "krx_close_utc_us": 2,
                    "us_open_utc_us": 3,
                },
                "2026-09-05": {  # weekend
                    "krx_is_open": False,
                    "us_is_open": False,
                    "krx_open_utc_us": None,
                    "krx_close_utc_us": None,
                    "us_open_utc_us": None,
                },
            }
        }
        import datetime

        self.assertEqual(
            extract.session_points(calendar, datetime.date(2026, 9, 10)),
            {"t0": 1, "t1": 2, "t2": 3},
        )
        self.assertIsNone(extract.session_points(calendar, datetime.date(2026, 9, 7)))
        self.assertIsNone(extract.session_points(calendar, datetime.date(2026, 9, 5)))
        # A date the calendar does not mention at all.
        self.assertIsNone(extract.session_points(calendar, datetime.date(2026, 9, 6)))

    def test_previous_us_close_walks_back_over_weekend_and_holiday(self):
        import datetime

        calendar = {
            "sessions": {
                "2026-09-04": {"us_is_open": True, "us_close_utc_us": 400},
                "2026-09-05": {"us_is_open": False, "us_close_utc_us": None},
                "2026-09-06": {"us_is_open": False, "us_close_utc_us": None},
                # Labor Day. The close timestamp is deliberately non-null: the
                # is_open flag must decide, not the presence of a field.
                "2026-09-07": {"us_is_open": False, "us_close_utc_us": 700},
                "2026-09-08": {
                    "krx_is_open": True,
                    "us_is_open": True,
                    "krx_open_utc_us": 1,
                    "krx_close_utc_us": 2,
                    "us_open_utc_us": 3,
                    "us_close_utc_us": 800,
                },
                "2026-09-09": {
                    "krx_is_open": True,
                    "us_is_open": True,
                    "krx_open_utc_us": 11,
                    "krx_close_utc_us": 12,
                    "us_open_utc_us": 13,
                    "us_close_utc_us": 900,
                },
            }
        }
        # Tuesday after a US holiday Monday: the control is Friday's close.
        self.assertEqual(extract.previous_us_close(calendar, datetime.date(2026, 9, 8)), 400)
        self.assertEqual(extract.previous_us_close(calendar, datetime.date(2026, 9, 9)), 800)
        # Off the calendar's front edge: unresolvable, never a guess.
        self.assertIsNone(extract.previous_us_close(calendar, datetime.date(2026, 9, 4)))
        # Only the requested instants come back, pc included on request.
        self.assertEqual(
            extract.session_points(calendar, datetime.date(2026, 9, 9)),
            {"t0": 11, "t1": 12, "t2": 13},
        )
        self.assertEqual(
            extract.session_points(calendar, datetime.date(2026, 9, 9), ["t1", "t2", "pc"]),
            {"t1": 12, "t2": 13, "pc": 800},
        )
        # A session whose previous close cannot be resolved is not a session
        # once pc is requested -- the same fail-closed rule as t0/t1/t2.
        self.assertIsNone(
            extract.session_points(calendar, datetime.date(2026, 9, 4), ["t1", "pc"])
        )

    def test_points_option_rejects_unknown_instants(self):
        with tempfile.TemporaryDirectory() as tmp:
            calendar = os.path.join(tmp, "cal.json")
            with open(calendar, "w") as handle:
                json.dump({"sessions": {}}, handle)
            argv = [
                "--calendar", calendar, "--data-dir", tmp, "--start", "2026-09-08",
                "--end", "2026-09-08", "--out", os.path.join(tmp, "o.jsonl"),
                "--workdir", tmp, "--points", "t1,t3",
            ]
            with self.assertRaises(SystemExit) as raised:
                extract.main(argv)
            self.assertNotEqual(raised.exception.code, 0)

    def test_query_takes_the_nearest_row_and_filters_venue_and_symbol(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "part.sqlite3")
            conn = sqlite3.connect(path)
            conn.execute(
                """CREATE TABLE price_observation (
                     observed_ts_us INTEGER, ts_srv_us INTEGER, venue TEXT,
                     market_id INTEGER, symbol TEXT, price_type TEXT,
                     price TEXT, source TEXT)"""
            )
            target = 1789047000 * US
            conn.executemany(
                "INSERT INTO price_observation VALUES (?,?,?,?,?,?,?,?)",
                [
                    (target - 250 * US, None, "lighter", 1, "SNDK", "mid", "100.0", "ws"),
                    (target - 1 * US, None, "lighter", 1, "SNDK", "mid", "101.0", "ws"),
                    (target + 2 * US, None, "lighter", 1, "SNDK", "mid", "102.0", "ws"),
                    (target + 250 * US, None, "lighter", 1, "SNDK", "mid", "103.0", "ws"),
                    (target + 1 * US, None, "robinhood", 1, "SNDK", "mid", "999.0", "ws"),
                    (target + 3 * US, None, "lighter", 2, "MU", "mid", "50.0", "ws"),
                    (target + 3 * US, None, "lighter", 1, "SNDK", "last", "77.0", "ws"),
                    (target + 3600 * US, None, "lighter", 1, "SNDK", "mid", "200.0", "ws"),
                ],
            )
            conn.commit()
            conn.close()

            rows = extract.query_partition(
                path,
                [("2026-09-10", "t2", target)],
                ["SNDK"],
                ["mid"],
                ["lighter", "lighter_mainnet_context"],
                300 * US,
            )
            self.assertEqual(len(rows), 1)
            row = rows[0]
            # Nearest in absolute time: neither the earliest nor the latest row
            # inside the tolerance window, and on either side of the instant.
            self.assertEqual(row["price"], "101.0")
            self.assertEqual(row["venue"], "lighter")  # robinhood excluded
            self.assertAlmostEqual(row["lag_secs"], -1.0)

    def test_a_busy_series_cannot_crowd_out_a_quiet_one(self):
        # One 1 Hz series can fill any global row cap with quotes near the
        # instant; the quiet series' only quote sits further out but well
        # inside the tolerance, and must still come back.
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "part.sqlite3")
            conn = sqlite3.connect(path)
            conn.execute(
                """CREATE TABLE price_observation (
                     observed_ts_us INTEGER, ts_srv_us INTEGER, venue TEXT,
                     market_id INTEGER, symbol TEXT, price_type TEXT,
                     price TEXT, source TEXT)"""
            )
            target = 1789047000 * US
            # 603 rows sit nearer to the instant than the quiet series' only
            # quote, which is what a global row cap would spend itself on.
            busy = [
                (target + offset * US, None, "lighter", 1, "SNDK", ptype, "100.0", "ws")
                for offset in range(-150, 151)
                for ptype in ("mid", "mark", "index")
            ]
            conn.executemany("INSERT INTO price_observation VALUES (?,?,?,?,?,?,?,?)", busy)
            conn.execute(
                "INSERT INTO price_observation VALUES (?,?,?,?,?,?,?,?)",
                (target + 100 * US, None, "lighter", 2, "SKHYNIXUSD", "mid", "55.5", "ws"),
            )
            conn.commit()
            conn.close()

            rows = extract.query_partition(
                path,
                [("2026-09-10", "t2", target)],
                ["SNDK", "SKHYNIXUSD"],
                ["mid", "mark", "index"],
                ["lighter"],
                300 * US,
            )
            quiet = [r for r in rows if r["symbol"] == "SKHYNIXUSD"]
            self.assertEqual(len(quiet), 1)
            self.assertEqual(quiet[0]["price"], "55.5")
            self.assertAlmostEqual(quiet[0]["lag_secs"], 100.0)
            self.assertEqual(len([r for r in rows if r["symbol"] == "SNDK"]), 3)

    def test_query_returns_nothing_outside_the_tolerance(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "part.sqlite3")
            conn = sqlite3.connect(path)
            conn.execute(
                """CREATE TABLE price_observation (
                     observed_ts_us INTEGER, ts_srv_us INTEGER, venue TEXT,
                     market_id INTEGER, symbol TEXT, price_type TEXT,
                     price TEXT, source TEXT)"""
            )
            target = 1789047000 * US
            conn.execute(
                "INSERT INTO price_observation VALUES (?,?,?,?,?,?,?,?)",
                (target + 400 * US, None, "lighter", 1, "SNDK", "mid", "101.0", "ws"),
            )
            conn.commit()
            conn.close()
            rows = extract.query_partition(
                path, [("2026-09-10", "t2", target)], ["SNDK"], ["mid"], ["lighter"], 300 * US
            )
            self.assertEqual(rows, [])

    def test_only_a_covered_completion_record_lets_a_rerun_skip(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = os.path.join(tmp, "prices.jsonl")
            with open(out, "w") as handle:
                # An observation row alone does not mean the partition finished.
                handle.write(
                    json.dumps(
                        dict(price_row("2026-09-10", "t0", "SNDK", 1.0), partition="20260910_00")
                    )
                    + "\n"
                )
                handle.write("not json\n")
                handle.write(
                    json.dumps({"partition": "20260910_06", "status": "missing"}) + "\n"
                )
                handle.write(
                    json.dumps(
                        {"partition": "20260910_13", "status": "done", "covered": False}
                    )
                    + "\n"
                )
                handle.write(
                    json.dumps(
                        {"partition": "20260909_13", "status": "done", "covered": True}
                    )
                    + "\n"
                )
            self.assertEqual(extract.load_done_partitions(out), {"20260909_13"})
            self.assertEqual(
                extract.load_done_partitions(os.path.join(tmp, "gone.jsonl")), set()
            )

    def test_a_live_partition_counts_as_done_only_when_every_series_answered(self):
        targets = [("2026-09-10", "t2", 1789047000 * US)]
        rows = [
            {"date": "2026-09-10", "point": "t2", "symbol": "SNDK", "price_type": "mid"}
        ]
        partial = extract.completion_record(
            "20260910_13", targets, ["SNDK", "SKHYNIXUSD"], ["mid"], rows, sealed=False
        )
        self.assertFalse(partial["covered"])
        complete = extract.completion_record(
            "20260910_13", targets, ["SNDK"], ["mid"], rows, sealed=False
        )
        self.assertTrue(complete["covered"])

    def test_a_sealed_partition_is_done_even_when_it_answered_nothing(self):
        # The archive object is immutable: an hour the feed was down will never
        # answer differently, so re-fetching it forever buys nothing.
        record = extract.completion_record(
            "20260904_06",
            [("2026-09-04", "t1", 1788503400 * US)],
            ["SNDK"],
            ["mid"],
            [],
            sealed=True,
        )
        self.assertTrue(record["covered"])
        self.assertEqual(record["rows"], 0)


class BoundaryCandidateTest(unittest.TestCase):
    def test_the_nearest_candidate_wins_across_partitions(self):
        # t0 is 00:00:00 exactly: the quote that stood for it can be the
        # previous hour's last one, emitted from the previous partition.
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "prices.jsonl")
            with open(path, "w") as handle:
                far = dict(
                    price_row("2026-09-10", "t0", "SNDK", 200.0, lag=45.0),
                    partition="20260910_00",
                    observed_ts_us=1788998445 * US,
                )
                near = dict(
                    price_row("2026-09-10", "t0", "SNDK", 199.0, lag=-1.0),
                    partition="20260909_23",
                    observed_ts_us=1788998399 * US,
                )
                handle.write(json.dumps(far) + "\n")
                handle.write(json.dumps(near) + "\n")
            prices = absorption.load_prices(path)
            chosen = prices[("2026-09-10", "t0", "SNDK", "mid")]
            self.assertEqual(chosen["price"], "199.0")
            self.assertEqual(chosen["partition"], "20260909_23")

    def test_equal_distance_candidates_resolve_deterministically(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "prices.jsonl")
            with open(path, "w") as handle:
                for lag, ts, price in ((5.0, 1788998405 * US, 1.0), (-5.0, 1788998395 * US, 2.0)):
                    handle.write(
                        json.dumps(
                            dict(
                                price_row("2026-09-10", "t0", "SNDK", price, lag=lag),
                                observed_ts_us=ts,
                            )
                        )
                        + "\n"
                    )
            first = absorption.load_prices(path)[("2026-09-10", "t0", "SNDK", "mid")]
            # Order in the file must not change the answer.
            lines = open(path).read().strip().split("\n")
            with open(path, "w") as handle:
                handle.write("\n".join(reversed(lines)) + "\n")
            second = absorption.load_prices(path)[("2026-09-10", "t0", "SNDK", "mid")]
            self.assertEqual(first["observed_ts_us"], second["observed_ts_us"])
            self.assertEqual(first["price"], "2.0")  # the earlier of the two


class LoadPricesTest(unittest.TestCase):
    def test_status_rows_are_skipped(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "prices.jsonl")
            with open(path, "w") as handle:
                handle.write(json.dumps({"partition": "20260910_00", "status": "missing"}) + "\n")
                handle.write("\n")
                handle.write(
                    json.dumps(
                        dict(price_row("2026-09-10", "t0", "SNDK", 1.5), partition="20260910_00")
                    )
                    + "\n"
                )
            prices = absorption.load_prices(path)
            self.assertEqual(list(prices), [("2026-09-10", "t0", "SNDK", "mid")])


if __name__ == "__main__":
    unittest.main(verbosity=2)
