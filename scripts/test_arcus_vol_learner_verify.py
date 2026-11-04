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


def journal_rows(live_log, start, end, quoting):
    """Hourly kind=learner rows as the runtime writes them: the current window's
    arm hours, decayed with the half-life, +1 h on the live arm while quoting."""
    hours, rows = {}, []
    k = 0.5 ** (1 / v.HALF_LIFE_H)
    for t in range(start, end, 3_600_000):
        w = v.window_of(t)
        arm = v.arm_at(live_log, t)
        for key in hours:
            hours[key] *= k
        if arm is not None and quoting(t):
            hours[(w, arm)] = hours.get((w, arm), 0.0) + 1.0
        arms = [{"offset_bps": a, "hours": hours.get((w, a), 0.0)} for a in (2.0, 3.0, 5.0, 8.0, 12.0)]
        rows.append({"kind": "learner", "mode": "live", "window": w, "ts_ms": t + 3_600_000, "arms": arms})
    return rows


class QuotingHours(unittest.TestCase):
    H = 3_600_000

    def row(self, t, window, hours, mode="live"):
        return {"kind": "learner", "mode": mode, "window": window, "ts_ms": t,
                "arms": [{"offset_bps": a, "hours": h} for a, h in hours.items()]}

    def test_decay_alone_adds_nothing_and_quoting_adds_its_hours(self):
        k = 0.5 ** (1 / v.HALF_LIFE_H)
        rows = [self.row(0, "off", {3.0: 10.0, 5.0: 4.0}),
                self.row(self.H, "off", {3.0: 10.0 * k + 1.0, 5.0: 4.0 * k})]
        h = v.quoting_hours(rows, 0, 10 * self.H)
        self.assertAlmostEqual(h[("off", 3.0)], 1.0)
        self.assertNotIn(("off", 5.0), h)

    def test_shadow_rows_and_window_changes_add_nothing(self):
        rows = [self.row(0, "off", {3.0: 10.0}, mode="shadow"),
                self.row(self.H, "off", {3.0: 11.0}, mode="shadow"),
                self.row(2 * self.H, "off", {3.0: 1.0}),
                self.row(3 * self.H, "pre", {3.0: 30.0})]  # another window's stats
        self.assertEqual(v.quoting_hours(rows, 0, 10 * self.H), {})


class Readout(unittest.TestCase):
    def run_once(self, fills, live_log, t0, now, quoting=None):
        """`quoting(ms) -> bool`: whether the runtime was quoting that hour (default: always)."""
        d = tempfile.mkdtemp()
        f, l, j, out = (os.path.join(d, x) for x in ("fills.jsonl", "learner.json", "learner.jsonl", "out"))
        with open(f, "w") as fh:
            fh.write("\n".join(json.dumps(r) for r in fills) + "\n")
        with open(l, "w") as fh:
            json.dump({"live_log": live_log}, fh)
        with open(j, "w") as fh:
            for r in journal_rows(live_log, ms(t0), ms(now), quoting or (lambda _: True)):
                fh.write(json.dumps(r) + "\n")
        args = ["--fills", f, "--learner", l, "--journal", j, "--t0", t0, "--out", out, "--now", now]
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

    def test_halted_arm_gets_no_exposure(self):
        # 8 bp is selected but the runtime never quotes while it is live (halted /
        # down): it must not be credited quoting hours (Codex P2 on pairtrade#422).
        t0 = "2026-11-05T10:00:00Z"
        log, t = [], ms(t0)
        arms = [3.0, 5.0, 8.0]
        for i in range(15 * 8):
            log.append([t + i * 3 * 3_600_000, arms[i % 3]])
        rc, out, _ = self.run_once([], log, t0, "2026-11-21T00:00:00Z",
                                   quoting=lambda m: v.arm_at(log, m) != 8.0)
        r = json.load(open(os.path.join(out, "learner_verify.json")))
        self.assertEqual(r["M1_max_live_hours_per_arm"]["8.0"], 0.0)
        self.assertFalse(r["M1_pass"])
        self.assertEqual(r["verdict"], "BROKEN")

    def test_reward_uses_three_dollars_a_point(self):
        self.assertAlmostEqual(v.reward((-1.0, 1_000_000)), -1.0 + 72.0)


if __name__ == "__main__":
    unittest.main()
