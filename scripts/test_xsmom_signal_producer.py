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
import xsmom_universe_pool as xu  # noqa: E402

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

    def test_universe_cross_check_refuses_symbols_the_runtime_would_reject(self):
        with tempfile.TemporaryDirectory() as d:
            cfg = os.path.join(d, "cfg.yaml")
            with open(cfg, "w") as f:
                f.write("universe:\n  symbols:\n    - LIT\n    - GRAM\n    - APT\nschedule:\n  kind: daily\n")
            self.assertEqual(xp.universe_from_config(cfg), {"LIT", "GRAM", "APT"})
            ledger = os.path.join(d, "ledger.jsonl")
            write_ledger(ledger, [ROW])
            out = os.path.join(d, "signal.json")
            cmd = [sys.executable, os.path.join(HERE, "xsmom_signal_producer.py"),
                   "--ledger", ledger, "--out", out, "--date", "2026-09-06", "--config", cfg]
            # ROW also holds ENA, which the config does not list.
            r = subprocess.run(cmd, capture_output=True, text=True)
            self.assertEqual(r.returncode, 2, r.stdout)
            self.assertIn("ENA", r.stderr)
            self.assertFalse(os.path.exists(out))
            with open(cfg, "a") as f:
                f.write("")
            with open(cfg, "w") as f:
                f.write("universe:\n  symbols:\n    - LIT\n    - GRAM\n    - APT\n    - ENA\nschedule:\n  kind: daily\n")
            r = subprocess.run(cmd, capture_output=True, text=True)
            self.assertEqual(r.returncode, 0, r.stderr)
            self.assertTrue(os.path.exists(out))
            # A config with no universe block is an error, not an empty set.
            bad = os.path.join(d, "bad.yaml")
            with open(bad, "w") as f:
                f.write("schedule:\n  kind: daily\n")
            with self.assertRaises(SystemExit):
                xp.universe_from_config(bad)

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


class PoolTest(unittest.TestCase):
    """The pool predicate must match the watcher's screen exactly: every
    exclusion the watcher applies before its liquidity filters, and no
    other, or the config either misses a name (runtime skips the key) or
    carries one the screen can never emit."""

    OBD = [
        {"symbol": "BTC", "status": "active", "market_config": {}},
        {"symbol": "ARB", "status": "active", "market_config": {"hidden": False}},
        {"symbol": "HID", "status": "active", "market_config": {"hidden": True}},
        {"symbol": "RED", "status": "active", "market_config": {"force_reduce_only": True}},
        {"symbol": "TSLA", "status": "active", "market_config": {"trading_hours": {"open": "13:30"}}},
        {"symbol": "OLD", "status": "inactive", "market_config": {}},
        {"symbol": "NOCFG", "status": "active"},
        {"symbol": "LONLY", "status": "active", "market_config": {}},
    ]
    INFO = {"symbols": [
        {"baseAsset": "BTC", "quoteAsset": "USDT", "status": "TRADING",
         "contractType": "PERPETUAL", "underlyingType": "COIN"},
        {"baseAsset": "ARB", "quoteAsset": "USDT", "status": "TRADING",
         "contractType": "PERPETUAL", "underlyingType": "COIN"},
        {"baseAsset": "HID", "quoteAsset": "USDT", "status": "TRADING",
         "contractType": "PERPETUAL", "underlyingType": "COIN"},
        {"baseAsset": "RED", "quoteAsset": "USDT", "status": "TRADING",
         "contractType": "PERPETUAL", "underlyingType": "COIN"},
        {"baseAsset": "TSLA", "quoteAsset": "USDT", "status": "TRADING",
         "contractType": "PERPETUAL", "underlyingType": "COIN"},
        {"baseAsset": "OLD", "quoteAsset": "USDT", "status": "TRADING",
         "contractType": "PERPETUAL", "underlyingType": "COIN"},
        {"baseAsset": "NOCFG", "quoteAsset": "USDT", "status": "TRADING",
         "contractType": "PERPETUAL", "underlyingType": "COIN"},
        # Binance-side exclusions: wrong quote, halted, dated, index underlying
        {"baseAsset": "BTC", "quoteAsset": "USDC", "status": "TRADING",
         "contractType": "PERPETUAL", "underlyingType": "COIN"},
        {"baseAsset": "ARB", "quoteAsset": "USDT", "status": "SETTLING",
         "contractType": "PERPETUAL", "underlyingType": "COIN"},
        {"baseAsset": "LONLY", "quoteAsset": "USDT", "status": "TRADING",
         "contractType": "CURRENT_QUARTER", "underlyingType": "COIN"},
        {"baseAsset": "DEFI", "quoteAsset": "USDT", "status": "TRADING",
         "contractType": "PERPETUAL", "underlyingType": "INDEX"},
    ]}

    def test_pool_is_the_watchers_screen_before_liquidity(self):
        self.assertEqual(xu.lighter_tradable(self.OBD), {"BTC", "ARB", "NOCFG", "LONLY"})
        self.assertEqual(xu.binance_coin_bases(self.INFO),
                         {"BTC", "ARB", "HID", "RED", "TSLA", "OLD", "NOCFG"})
        self.assertEqual(xu.pool(self.OBD, self.INFO), ["ARB", "BTC", "NOCFG"])

    def test_check_names_pool_symbols_the_config_lacks(self):
        cfg_text = "schema_version: 1\nuniverse:\n  symbols:\n    - BTC\n    - ZZZ\nschedule:\n  kind: daily\n"
        with tempfile.TemporaryDirectory() as d:
            cfg = os.path.join(d, "x.yaml")
            with open(cfg, "w") as f:
                f.write(cfg_text)
            real = xu.fetch_pool
            xu.fetch_pool = lambda: ["ARB", "BTC", "OP"]
            try:
                import io
                import contextlib
                err = io.StringIO()
                with contextlib.redirect_stderr(err):
                    rc = xu.main(["--check", cfg])
                self.assertEqual(rc, 1)
                self.assertIn("2 pool symbol(s) not in universe.symbols: ARB, OP", err.getvalue())
                self.assertIn("outside the current pool: ZZZ", err.getvalue())
                xu.fetch_pool = lambda: ["BTC"]
                with contextlib.redirect_stdout(io.StringIO()):
                    self.assertEqual(xu.main(["--check", cfg]), 0)
            finally:
                xu.fetch_pool = real


if __name__ == "__main__":
    unittest.main()
