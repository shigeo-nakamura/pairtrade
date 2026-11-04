#!/usr/bin/env python3
"""Tests for arcus_vol_learner_verify.py (bot-strategy#1093). Stdlib only."""
import json
import os
import sys
import tempfile
import unittest
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import arcus_vol_learner_verify as v  # noqa: E402


def ms(s):
    return int(datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc).timestamp() * 1000)


class Windows(unittest.TestCase):
    def test_same_windows_as_offset_learner_rs(self):
        # Mirrors windows_follow_new_york_time_and_dst in offset_learner.rs.
        cases = [
            ("2026-10-05T03:00:00Z", "off"), ("2026-10-05T07:59:00Z", "off"),
            ("2026-10-05T08:00:00Z", "pre"), ("2026-10-05T13:29:00Z", "pre"),
            ("2026-10-05T13:30:00Z", "cash"), ("2026-10-05T20:00:00Z", "after"),
            ("2026-10-06T00:00:00Z", "off"), ("2026-10-10T00:00:00Z", "weekend"),
            ("2026-10-11T23:59:00Z", "weekend"), ("2026-10-12T00:00:00Z", "off"),
            ("2026-11-02T14:29:00Z", "pre"), ("2026-11-02T14:30:00Z", "cash"),
        ]
        for t, w in cases:
            self.assertEqual(v.window_of(ms(t)), w, t)


def fill(t, realized, notional):
    return {"kind": "fill", "mode": "live", "ts_ms": ms(t), "realized": str(realized), "fee": "0", "notional": str(notional)}


class Readout(unittest.TestCase):
    def run_once(self, fills, live_log, t0, now):
        d = tempfile.mkdtemp()
        f, l, out = os.path.join(d, "fills.jsonl"), os.path.join(d, "learner.json"), os.path.join(d, "out")
        with open(f, "w") as fh:
            fh.write("\n".join(json.dumps(r) for r in fills) + "\n")
        with open(l, "w") as fh:
            json.dump({"live_log": live_log}, fh)
        args = ["--fills", f, "--learner", l, "--t0", t0, "--out", out, "--now", now]
        return v.main(args), out, args

    def test_refused_before_the_test_period_ends_and_on_a_second_look(self):
        t0 = "2026-11-05T10:00:00Z"  # test = 11-06 .. 11-19 (exclusive 11-20)
        rc, _, _ = self.run_once([], [], t0, "2026-11-19T23:59:00Z")
        self.assertEqual(rc, 2)
        rc, out, args = self.run_once([], [], t0, "2026-11-20T00:00:00Z")
        self.assertEqual(rc, 0)
        self.assertEqual(v.main(args), 2)  # READOUT_DONE.json exists

    def test_verdicts(self):
        t0 = "2026-11-05T10:00:00Z"
        base = [fill("2026-10-28T15:00:00Z", -1.0, 10_000)]  # baseline mean = (-1 + 0.72) / 8
        # Rotate 3 / 5 / 8 bp every 3 h through the test period, one fill per arm.
        log, t = [], ms("2026-11-05T10:00:00Z")
        arms = [3.0, 5.0, 8.0]
        for i in range(15 * 8):
            log.append([t + i * 3 * 3_600_000, arms[i % 3]])
        test = [fill("2026-11-06T00:30:00Z", 1.0, 10_000), fill("2026-11-06T03:30:00Z", 0.0, 1),
                fill("2026-11-06T06:30:00Z", 0.0, 1)]
        rc, out, _ = self.run_once(base + test, log, t0, "2026-11-21T00:00:00Z")
        self.assertEqual(rc, 0)
        r = json.load(open(os.path.join(out, "learner_verify.json")))
        self.assertTrue(r["M1_pass"] and r["M2_pass"] and r["M3_pass"], r)
        self.assertEqual(r["verdict"], "WORKS")
        # Stuck at the widest arm with no fills -> BROKEN.
        rc, out, _ = self.run_once(base, [[t, 8.0]], t0, "2026-11-21T00:00:00Z")
        r = json.load(open(os.path.join(out, "learner_verify.json")))
        self.assertFalse(r["M1_pass"])
        self.assertTrue(r["M2_stuck_windows"])
        self.assertEqual(r["verdict"], "BROKEN")
        # Mechanics fine but worse than the baseline.
        bad = [fill("2026-11-06T00:30:00Z", -5.0, 10_000), fill("2026-11-06T03:30:00Z", 0.0, 1),
               fill("2026-11-06T06:30:00Z", 0.0, 1)]
        rc, out, _ = self.run_once(base + bad, log, t0, "2026-11-21T00:00:00Z")
        r = json.load(open(os.path.join(out, "learner_verify.json")))
        self.assertEqual(r["verdict"], "MECHANICS_OK_ECONOMICS_NOT_BETTER")

    def test_reward_uses_three_dollars_a_point(self):
        self.assertAlmostEqual(v.reward((-1.0, 1_000_000)), -1.0 + 72.0)


if __name__ == "__main__":
    unittest.main()
