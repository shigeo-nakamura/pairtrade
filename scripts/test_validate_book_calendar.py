#!/usr/bin/env python3
"""Tests for validate_book_calendar.py (no network, no filesystem beyond tmp)."""
import json
import os
import subprocess
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPT = os.path.join(HERE, "validate_book_calendar.py")
sys.path.insert(0, HERE)
import validate_book_calendar as vbc  # noqa: E402

GOOD = {
    "calendar_version": "t-1",
    "entries": [
        {"decision_key": "2026-09-15", "decision_at": "2026-09-15T13:29:00Z",
         "flatten_at": "2026-09-15T13:36:00Z"},
        {"decision_key": "2026-09-16", "decision_at": "2026-09-16T13:29:00Z",
         "flatten_at": "2026-09-16T13:36:00Z"},
    ],
}


def write(d):
    fd, p = tempfile.mkstemp(suffix=".json")
    with os.fdopen(fd, "w") as f:
        json.dump(d, f)
    return p


class ValidatorTests(unittest.TestCase):
    def test_accepts_a_good_calendar(self):
        self.assertEqual(len(vbc.validate(write(GOOD))["entries"]), 2)

    def test_rejects_what_the_runtime_would_reject(self):
        import copy
        cases = {
            "bare list": [GOOD["entries"][0]],
            "no entries": {"calendar_version": "t", "entries": []},
            "unknown top key": dict(GOOD, schedule="calendar"),
            "entries not a list": {"calendar_version": "t", "entries": {}},
        }
        for name, bad in cases.items():
            with self.assertRaises(SystemExit, msg=name):
                vbc.validate(write(bad))

        def mutate(fn):
            d = copy.deepcopy(GOOD)
            fn(d)
            return d

        entry_cases = {
            "unknown entry key": lambda d: d["entries"][0].update(flatten_after="x"),
            "bad decision_at": lambda d: d["entries"][0].update(decision_at="2026-09-15 13:29"),
            "non-string decision_at": lambda d: d["entries"][0].update(decision_at=1788000000),
            "empty decision_key": lambda d: d["entries"][0].update(decision_key="  "),
            "duplicate key": lambda d: d["entries"][1].update(decision_key="2026-09-15"),
            "flatten before decision": lambda d: d["entries"][0].update(
                flatten_at="2026-09-15T13:20:00Z"),
            "flatten equals decision": lambda d: d["entries"][0].update(
                flatten_at="2026-09-15T13:29:00Z"),
            "overlap": lambda d: d["entries"][1].update(
                decision_at="2026-09-15T13:30:00Z", flatten_at="2026-09-15T13:40:00Z"),
            "out of order": lambda d: d["entries"].reverse(),
            "entry not an object": lambda d: d["entries"].__setitem__(0, "2026-09-15"),
        }
        for name, fn in entry_cases.items():
            with self.assertRaises(SystemExit, msg=name):
                vbc.validate(write(mutate(fn)))

    def test_the_committed_calendar_is_loadable(self):
        cal = os.path.join(os.path.dirname(HERE), "configs", "book",
                           "exdiv-lighter.calendar.json")
        d = vbc.validate(cal)
        self.assertGreater(len(d["entries"]), 0)

    def test_cli(self):
        r = subprocess.run([sys.executable, SCRIPT, write(GOOD)], capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn("2 entries", r.stdout)
        r = subprocess.run([sys.executable, SCRIPT, write({"entries": []})],
                           capture_output=True, text=True)
        self.assertNotEqual(r.returncode, 0)
        r = subprocess.run([sys.executable, SCRIPT], capture_output=True, text=True)
        self.assertNotEqual(r.returncode, 0)


if __name__ == "__main__":
    unittest.main()
