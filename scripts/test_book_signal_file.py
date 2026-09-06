#!/usr/bin/env python3
"""Unit tests for book_signal_file.py. The hash vector is shared with
src/book/signal.rs::canonical_payload_matches_python_json_dumps so a drift
between the Python writer and the Rust validator fails on both sides."""
import json
import os
import sys
import tempfile
import unittest
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import book_signal_file as bsf  # noqa: E402


class SignalFileTests(unittest.TestCase):
    def test_hash_vector_matches_rust(self):
        sha = bsf.payload_sha256(
            "p", "2026-09-06T00:00:00Z", "2026-09-06", {"SOL": -0.1, "BTC": 0.1, "ETH": 1.0}
        )
        self.assertEqual(sha, "5554a39df2be5d06c9da208795db70f46379fc1dc8f5eb02351cd6ce9f010f1a")
        self.assertEqual(
            bsf.canonical_payload("p", "2026-09-06T00:00:00Z", "2026-09-06", {"SOL": -0.1, "BTC": 0.1, "ETH": 1.0}),
            '{"as_of":"2026-09-06T00:00:00Z","decision_key":"2026-09-06","producer_id":"p","weights":{"BTC":0.1,"ETH":1.0,"SOL":-0.1}}',
        )

    def test_build_and_write_roundtrip(self):
        gen = datetime(2026, 9, 6, 0, 20, 36, tzinfo=timezone.utc)
        as_of = datetime(2026, 9, 6, 0, 0, 0, tzinfo=timezone.utc)
        sig = bsf.build_signal("prod", gen, as_of, "2026-09-06", {"DOT": -0.25, "BTC": 0.25}, {"n": 2})
        self.assertEqual(sig["schema_version"], 1)
        self.assertEqual(sig["generated_at"], "2026-09-06T00:20:36Z")
        self.assertEqual(sig["as_of"], "2026-09-06T00:00:00Z")
        self.assertEqual(list(sig["weights"]), ["BTC", "DOT"])
        self.assertEqual(sig["payload_sha256"], bsf.payload_sha256("prod", "2026-09-06T00:00:00Z", "2026-09-06", sig["weights"]))
        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, "nested", "signal.json")
            bsf.write_signal(p, sig)
            with open(p) as f:
                back = json.load(f)
            self.assertEqual(back, sig)
            self.assertEqual([n for n in os.listdir(os.path.dirname(p)) if n.startswith(".signal.")], [])

    def test_rejects_look_ahead_and_bad_weights(self):
        gen = datetime(2026, 9, 6, 0, 0, 0, tzinfo=timezone.utc)
        as_of = datetime(2026, 9, 6, 0, 10, 0, tzinfo=timezone.utc)
        with self.assertRaises(ValueError):
            bsf.build_signal("p", gen, as_of, "k", {"BTC": 0.1})
        with self.assertRaises(ValueError):
            bsf.build_signal("p", as_of, gen, "k", {"BTC": float("nan")})
        with self.assertRaises(ValueError):
            bsf.build_signal("p", as_of, gen, "k", {"": 0.1})
        with self.assertRaises(ValueError):
            bsf.build_signal("p", as_of.replace(tzinfo=None), gen, "k", {"BTC": 0.1})


if __name__ == "__main__":
    unittest.main()
