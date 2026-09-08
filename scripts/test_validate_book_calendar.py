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
            # CalendarFile.calendar_version is a String with serde(default):
            # absent is fine, a number or an explicit null is not.
            "numeric calendar_version": dict(GOOD, calendar_version=1),
            "null calendar_version": dict(GOOD, calendar_version=None),
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
            # schedule.rs rejects a flatten inside the signal window, not
            # merely one at or before the decision.
            "flatten inside the signal window": lambda d: d["entries"][0].update(
                flatten_at="2026-09-15T13:29:30Z"),
            "overlap": lambda d: d["entries"][1].update(
                decision_at="2026-09-15T13:30:00Z", flatten_at="2026-09-15T13:40:00Z"),
            "overlap via grace, no flatten": lambda d: d["entries"].__setitem__(1, {
                "decision_key": "b", "decision_at": "2026-09-15T13:29:20Z"}),
            "entry not an object": lambda d: d["entries"].__setitem__(0, "2026-09-15"),
        }
        for name, fn in entry_cases.items():
            with self.assertRaises(SystemExit, msg=name):
                vbc.validate(write(mutate(fn)))

    def test_sub_microsecond_precision_is_rejected(self):
        """fromisoformat truncates below microseconds while chrono keeps
        nanoseconds, and every instant is compared on ceil_secs -- so the
        two would round to different seconds."""
        import copy
        d = copy.deepcopy(GOOD)
        d["entries"][0]["decision_at"] = "2026-09-15T13:29:00.0000001Z"
        with self.assertRaises(SystemExit) as cm:
            vbc.validate(write(d))
        self.assertIn("fractional digits", str(cm.exception))
        # microsecond precision is fine, and ceils to the next second
        d["entries"][0]["decision_at"] = "2026-09-15T13:29:00.000001Z"
        vbc.validate(write(d))
        t = vbc.ts("2026-09-15T13:29:00.000001Z", "x")
        self.assertEqual(vbc.ceil_secs(t), vbc.ceil_secs(vbc.ts("2026-09-15T13:29:01Z", "x")))

    def test_duplicate_json_keys_are_rejected(self):
        """json.load keeps the last value; serde rejects the file."""
        for text in (
            '{"calendar_version": "a", "calendar_version": "b", "entries": []}',
            '{"calendar_version": "a", "entries": [], "entries": '
            '[{"decision_key": "x", "decision_at": "2026-09-15T13:29:00Z"}]}',
            '{"calendar_version": "a", "entries": [{"decision_key": "x", '
            '"decision_at": "2026-09-15T13:29:00Z", "decision_at": "2026-09-16T13:29:00Z"}]}',
        ):
            fd, p = tempfile.mkstemp(suffix=".json")
            with os.fdopen(fd, "w") as f:
                f.write(text)
            with self.assertRaises(SystemExit, msg=text[:60]):
                vbc.validate(p)

    def test_calendar_version_may_be_absent(self):
        import copy
        d = copy.deepcopy(GOOD)
        del d["calendar_version"]
        self.assertEqual(len(vbc.validate(write(d))["entries"]), 2)   # serde(default)

    def test_entry_order_is_not_a_rule(self):
        """Scheduler::build sorts by decision_at before checking, so a
        merely unsorted calendar must not block an install."""
        import copy
        d = copy.deepcopy(GOOD)
        d["entries"].reverse()
        self.assertEqual(len(vbc.validate(write(d))["entries"]), 2)

    def test_grace_is_honoured(self):
        import copy
        d = copy.deepcopy(GOOD)
        d["entries"][0]["flatten_at"] = "2026-09-15T13:29:30Z"    # 30 s after decision
        vbc.validate(write(d), grace_secs=10)                      # fine with a 10 s window
        with self.assertRaises(SystemExit):                        # not with 45 s
            vbc.validate(write(d), grace_secs=45)
        with self.assertRaises(SystemExit):
            vbc.validate(write(GOOD), grace_secs=-1)

    def test_the_committed_calendar_is_loadable(self):
        cal = os.path.join(os.path.dirname(HERE), "configs", "book",
                           "exdiv-lighter.calendar.json")
        # the grace the deployed config actually uses
        import re
        cfg = open(os.path.join(os.path.dirname(HERE), "configs", "book",
                                "exdiv-lighter.yaml")).read()
        grace = int(re.search(r"^\s+signal_grace_secs:\s*(\d+)", cfg, re.M).group(1))
        d = vbc.validate(cal, grace)
        self.assertGreater(len(d["entries"]), 0)

    def test_cli(self):
        r = subprocess.run([sys.executable, SCRIPT, write(GOOD), "--grace-secs", "45"],
                           capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn("2 entries", r.stdout)
        self.assertIn("grace 45s", r.stdout)
        r = subprocess.run([sys.executable, SCRIPT, write({"entries": []})],
                           capture_output=True, text=True)
        self.assertNotEqual(r.returncode, 0)
        r = subprocess.run([sys.executable, SCRIPT], capture_output=True, text=True)
        self.assertNotEqual(r.returncode, 0)


if __name__ == "__main__":
    unittest.main()
