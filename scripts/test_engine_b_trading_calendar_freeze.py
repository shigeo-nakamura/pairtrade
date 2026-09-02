#!/usr/bin/env python3
"""Tests for the offline Engine B trading-calendar freeze tool (A-7).

Only run where `exchange_calendars` is installed (see
scripts/engine_b_trading_calendar_freeze_requirements.txt) -- CI installs it
in a dedicated step, never alongside the Phase 0 observer's own dependencies.
"""

from __future__ import annotations

import importlib.util
import json
import sys
import unittest
from datetime import date
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
MODULE_PATH = SCRIPT_DIR / "engine_b_trading_calendar_freeze.py"
FROZEN_ARTIFACT_PATH = REPO_ROOT / "configs" / "engine-b" / "trading_calendar.json"

SPEC = importlib.util.spec_from_file_location("engine_b_trading_calendar_freeze", MODULE_PATH)
assert SPEC and SPEC.loader
freeze = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = freeze
SPEC.loader.exec_module(freeze)


class BuildSessionsTests(unittest.TestCase):
    def test_both_open_both_closed_and_asymmetric_days(self) -> None:
        sessions = freeze.build_sessions(date(2026, 1, 1), date(2026, 9, 7))

        new_years_day = sessions["2026-01-01"]
        self.assertFalse(new_years_day["krx_is_open"])
        self.assertFalse(new_years_day["us_is_open"])
        self.assertIsNone(new_years_day["krx_open_utc_us"])
        self.assertIsNone(new_years_day["us_open_utc_us"])

        both_open = sessions["2026-09-02"]
        self.assertTrue(both_open["krx_is_open"])
        self.assertTrue(both_open["us_is_open"])
        self.assertIsInstance(both_open["krx_open_utc_us"], int)
        self.assertIsInstance(both_open["us_open_utc_us"], int)
        self.assertLess(both_open["krx_open_utc_us"], both_open["krx_close_utc_us"])
        self.assertLess(both_open["us_open_utc_us"], both_open["us_close_utc_us"])

        us_labor_day = sessions["2026-09-07"]
        self.assertTrue(us_labor_day["krx_is_open"])
        self.assertFalse(us_labor_day["us_is_open"])

    def test_one_off_krx_closures_override_the_library(self) -> None:
        sessions = freeze.build_sessions(date(2026, 6, 1), date(2026, 7, 20))
        for iso in freeze.KRX_ONE_OFF_CLOSURES:
            entry = sessions[iso]
            self.assertFalse(entry["krx_is_open"], f"{iso} should be forced closed")
            self.assertIsNone(entry["krx_open_utc_us"])
            self.assertIsNone(entry["krx_close_utc_us"])
            # US cash market is unaffected by a Korean administrative holiday.
            self.assertTrue(entry["us_is_open"])

    def test_csat_delayed_open_shifts_krx_hours_by_one_hour(self) -> None:
        sessions = freeze.build_sessions(date(2026, 11, 1), date(2027, 12, 1))
        for iso in freeze.KRX_DELAYED_OPEN_ONE_HOUR:
            entry = sessions[iso]
            self.assertTrue(entry["krx_is_open"], f"{iso} should still be a KRX session, just delayed")
            open_seconds_of_day = (entry["krx_open_utc_us"] // 1_000_000) % 86_400
            close_seconds_of_day = (entry["krx_close_utc_us"] // 1_000_000) % 86_400
            self.assertEqual(open_seconds_of_day, 3600)  # 01:00 UTC = 10:00 KST
            self.assertEqual(close_seconds_of_day, 7 * 3600 + 1800)  # 07:30 UTC = 16:30 KST
            # US cash market is unaffected by a Korean exam-day adjustment
            # (9:30am America/New_York, EST or EDT depending on the date).
            self.assertTrue(entry["us_is_open"])
            self.assertIn((entry["us_open_utc_us"] // 1_000_000) % 86_400, (13 * 3600 + 1800, 14 * 3600 + 1800))

    def test_dst_shifts_us_open_hour_but_not_krx(self) -> None:
        sessions = freeze.build_sessions(date(2026, 1, 5), date(2026, 7, 6))
        winter_us_open = sessions["2026-01-05"]["us_open_utc_us"]
        summer_us_open = sessions["2026-07-06"]["us_open_utc_us"]
        self.assertIsNotNone(winter_us_open)
        self.assertIsNotNone(summer_us_open)
        # EST vs EDT: same 9:30am local open, but a 1-hour different UTC offset.
        winter_open_hour = (winter_us_open // 1_000_000 // 3600) % 24
        summer_open_hour = (summer_us_open // 1_000_000 // 3600) % 24
        self.assertEqual((winter_open_hour - summer_open_hour) % 24, 1)

        winter_krx_open = sessions["2026-01-05"]["krx_open_utc_us"]
        # KRX (Asia/Seoul) observes no DST: open hour-of-day stays fixed.
        self.assertEqual((winter_krx_open // 1_000_000) % 86_400, 0)


class BuildDocumentTests(unittest.TestCase):
    def test_deterministic_and_self_hashing(self) -> None:
        first = freeze.build_document(date(2026, 1, 1), date(2026, 1, 31))
        second = freeze.build_document(date(2026, 1, 1), date(2026, 1, 31))
        self.assertEqual(first, second)
        self.assertTrue(first["calendar_version"].startswith("xkrx-xnys-exchange_calendars-"))
        self.assertNotIn("generated_at", first)

    def test_krx_one_off_closures_field_is_range_filtered(self) -> None:
        containing = freeze.build_document(date(2026, 1, 1), date(2026, 12, 31))
        self.assertEqual(
            containing["krx_one_off_closures"],
            {"2026-06-03": "kr_local_election_day", "2026-07-17": "kr_constitution_day_2026_reinstatement"},
        )
        before = freeze.build_document(date(2026, 1, 1), date(2026, 5, 31))
        self.assertEqual(before["krx_one_off_closures"], {})

    def test_krx_delayed_open_field_is_range_filtered(self) -> None:
        containing = freeze.build_document(date(2026, 1, 1), date(2027, 12, 31))
        self.assertEqual(
            containing["krx_delayed_open_one_hour"],
            {"2026-11-19": "kr_csat_delayed_open", "2027-11-18": "kr_csat_delayed_open"},
        )
        only_2026 = freeze.build_document(date(2026, 1, 1), date(2026, 12, 31))
        self.assertEqual(only_2026["krx_delayed_open_one_hour"], {"2026-11-19": "kr_csat_delayed_open"})

    def test_range_shrink_changes_calendar_version(self) -> None:
        wide = freeze.build_document(date(2026, 1, 1), date(2026, 12, 31))
        narrow = freeze.build_document(date(2026, 1, 1), date(2026, 6, 30))
        self.assertNotEqual(wide["calendar_version"], narrow["calendar_version"])


class CommittedArtifactTests(unittest.TestCase):
    """Guard against a hand-edited configs/engine-b/trading_calendar.json.

    scripts/engine_b_trading_calendar_freeze_requirements.txt pins the exact
    exchange_calendars version this repeats; a version bump must regenerate
    and recommit the artifact together with the requirements pin.
    """

    def test_committed_artifact_matches_regeneration(self) -> None:
        committed = json.loads(FROZEN_ARTIFACT_PATH.read_bytes())
        start = date.fromisoformat(committed["range"]["start"])
        end = date.fromisoformat(committed["range"]["end"])
        regenerated = freeze.build_document(start, end)
        self.assertEqual(committed, regenerated)


if __name__ == "__main__":
    unittest.main()
