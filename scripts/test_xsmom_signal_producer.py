#!/usr/bin/env python3
"""Tests for xsmom_signal_producer.py (fixture ledger, no network)."""
import json
import os
import subprocess
import sys
import tempfile
import unittest
from datetime import date, datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import book_signal_file as bsf  # noqa: E402
import xsmom_signal_producer as xp  # noqa: E402

ROW = {
    "type": "rebalance", "date": "2026-09-06", "ts": 1788654050, "n_eligible": 56, "k": 11,
    "fills": [],
    "book": {"LIT": {"side": 1, "notional": 30.46}, "GRAM": {"side": -1, "notional": 45.79},
             "APT": {"side": -1, "notional": 44.96}, "ENA": {"side": 1, "notional": 45.45},
             "ZERO": {"side": 1, "notional": 0.0}},
}


def write_ledger(path, rows):
    with open(path, "w") as f:
        for r in rows:
            f.write(json.dumps({"type": "mark", "date": "2026-09-05"}) + "\n")
            f.write(json.dumps(r) + "\n")


class ProducerTests(unittest.TestCase):
    def test_grid(self):
        self.assertTrue(xp.on_grid(date(2026, 7, 3)))
        self.assertTrue(xp.on_grid(date(2026, 9, 6)))
        self.assertFalse(xp.on_grid(date(2026, 9, 7)))
        self.assertFalse(xp.on_grid(date(2026, 7, 1)))

    def test_build_weights_and_meta(self):
        sig = xp.build(ROW, 1000.0, datetime(2026, 9, 6, 0, 25, tzinfo=timezone.utc))
        self.assertEqual(sig["decision_key"], "2026-09-06")
        self.assertEqual(sig["as_of"], "2026-09-06T00:00:00Z")
        self.assertEqual(sig["producer_id"], xp.PRODUCER_ID)
        self.assertEqual(set(sig["weights"]), {"LIT", "GRAM", "APT", "ENA"})  # zero leg dropped
        self.assertAlmostEqual(sig["weights"]["LIT"], 0.03046)
        self.assertAlmostEqual(sig["weights"]["GRAM"], -0.04579)
        self.assertEqual(sig["meta"]["n_positions"], 4)
        self.assertEqual(sig["payload_sha256"], bsf.payload_sha256(
            xp.PRODUCER_ID, "2026-09-06T00:00:00Z", "2026-09-06", sig["weights"]))

    def test_caps_refuse_bad_books(self):
        bad = dict(ROW, book={"LIT": {"side": 1, "notional": 200.0}})
        with self.assertRaises(SystemExit):
            xp.build(bad, 1000.0, datetime.now(timezone.utc))
        bad = dict(ROW, book={"LIT": {"side": 1, "notional": 100.0}, "APT": {"side": 1, "notional": 100.0}})
        with self.assertRaises(SystemExit):  # net 0.2 > 0.05
            xp.build(bad, 1000.0, datetime.now(timezone.utc))
        bad = dict(ROW, book={"LIT": {"side": 2, "notional": 10.0}})
        with self.assertRaises(SystemExit):
            xp.build(bad, 1000.0, datetime.now(timezone.utc))
        bad = dict(ROW, book={"LIT": {"side": 1.5, "notional": 10.0}})
        with self.assertRaises(SystemExit):  # not truncated to 1
            xp.build(bad, 1000.0, datetime.now(timezone.utc))
        missing = {k: v for k, v in ROW.items() if k != "book"}
        with self.assertRaises(SystemExit):  # no book key is not a flat book
            xp.build(missing, 1000.0, datetime.now(timezone.utc))
        bad = dict(ROW, book=[])
        with self.assertRaises(SystemExit):
            xp.build(bad, 1000.0, datetime.now(timezone.utc))
        for bad_notional in [True, "45.45", float("nan")]:
            bad = dict(ROW, book={"LIT": {"side": 1, "notional": bad_notional}})
            with self.assertRaises(SystemExit):
                xp.build(bad, 1000.0, datetime.now(timezone.utc))
        flat = dict(ROW, book={})
        self.assertEqual(xp.build(flat, 1000.0, datetime.now(timezone.utc))["weights"], {})

    def test_non_finite_metadata_is_refused(self):
        with self.assertRaises(SystemExit):
            xp.build(dict(ROW, k=float("nan")), 1000.0, datetime.now(timezone.utc))
        with tempfile.TemporaryDirectory() as d:
            ledger = os.path.join(d, "ledger.jsonl")
            with open(ledger, "w") as f:
                f.write(json.dumps(ROW).replace('"k": 11', '"k": NaN') + "\n")
            with self.assertRaises(SystemExit):
                xp.load_rebalance_rows(ledger)

    def test_cli_writes_only_on_a_rebalance_date(self):
        with tempfile.TemporaryDirectory() as d:
            ledger = os.path.join(d, "ledger.jsonl")
            write_ledger(ledger, [ROW])
            out = os.path.join(d, "signal.json")
            cmd = [sys.executable, os.path.join(HERE, "xsmom_signal_producer.py"), "--ledger", ledger, "--out", out]
            r = subprocess.run(cmd + ["--date", "2026-09-07"], capture_output=True, text=True)
            self.assertEqual(r.returncode, 0, r.stderr)
            self.assertIn("no rebalance row", r.stdout)
            self.assertFalse(os.path.exists(out))
            r = subprocess.run(cmd + ["--date", "2026-09-06"], capture_output=True, text=True)
            self.assertEqual(r.returncode, 0, r.stderr)
            with open(out) as f:
                sig = json.load(f)
            self.assertEqual(sig["decision_key"], "2026-09-06")
            self.assertEqual(sig["schema_version"], 1)
            # off-grid (past) date with a row → refused unless overridden
            off = dict(ROW, date="2026-09-04")
            write_ledger(ledger, [off, ROW])
            r = subprocess.run(cmd + ["--date", "2026-09-04"], capture_output=True, text=True)
            self.assertEqual(r.returncode, 2)
            r = subprocess.run(cmd + ["--date", "2026-09-04", "--allow-off-grid"], capture_output=True, text=True)
            self.assertEqual(r.returncode, 0, r.stderr)
            # a negative / zero / nan gross would invert or break every weight
            for g in ["-1000", "0", "nan"]:
                r = subprocess.run(cmd + ["--date", "2026-09-06", "--gross", g], capture_output=True, text=True)
                self.assertEqual(r.returncode, 2, g)


if __name__ == "__main__":
    unittest.main()
