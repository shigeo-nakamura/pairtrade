#!/usr/bin/env python3
from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
import tempfile
import unittest

from engine_b_archive_monitor import collect, render

NOW = datetime(2026, 9, 9, 12, tzinfo=timezone.utc).timestamp()


def healthy_unit(unit):
    return {"LoadState": "loaded", "Result": "success",
            "ActiveState": "active" if unit.endswith(".timer") else "inactive"}


class MonitorTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / "data").mkdir()
        (self.root / "sealed").mkdir()

    def report(self, read_unit=healthy_unit):
        return collect(self.root, 24, now=NOW, read_unit=read_unit)

    def seal(self, name, **kwargs):
        value = {"partition": name, "sha256": "a" * 64,
                 "sealed_at": "2026-09-09T11:00:00Z"}
        value.update(kwargs)
        (self.root / "sealed" / f"{name}.json").write_text(json.dumps(value))

    def test_retention_uses_partition_end_not_mtime(self):
        for partition in ("20260908_11", "20260908_12", "20260909_12"):
            (self.root / "data" / f"engine_b_phase0_{partition}.sqlite3").write_bytes(b"123")
        (self.root / "data" / "engine_b_phase0_20260908_11.sqlite3-wal").write_bytes(b"12345")
        values = self.report()["metrics"]
        self.assertEqual(values["probe_success"], 1)
        self.assertEqual(values["eligible_partition_count"], 1)
        self.assertEqual(values["eligible_partition_bytes"], 3)
        self.assertEqual(values["retained_partition_bytes"], 9)
        self.assertEqual(values["oldest_eligible_age_seconds"], 86400)

    def test_seal_before_source_removal_is_not_success(self):
        self.seal("20260908_10")
        source = self.root / "data" / "engine_b_phase0_20260908_10.sqlite3"
        source.touch()
        self.assertEqual(self.report()["metrics"]["last_verified_removal_timestamp_seconds"], 0)
        source.unlink()
        self.assertEqual(self.report()["metrics"]["last_verified_removal_timestamp_seconds"], NOW - 3600)

    def test_empty_successful_unit_is_not_verified_archive(self):
        values = self.report()["metrics"]
        self.assertEqual(values["service_failed"], 0)
        self.assertEqual(values["last_verified_removal_timestamp_seconds"], 0)

    def test_corrupt_or_future_seal_marks_probe_failed(self):
        for overrides in ({"sha256": "bad"}, {"partition": "wrong"},
                          {"sealed_at": "2026-09-10T00:00:00Z"},
                          {"sealed_at": "2026-09-09T11:00:00"}):
            with self.subTest(overrides=overrides):
                self.seal("20260908_10", **overrides)
                report = self.report()
                self.assertEqual(report["metrics"]["probe_success"], 0)
                self.assertTrue(report["errors"])

    def test_missing_storage_does_not_hide_systemd_failure(self):
        (self.root / "data").rmdir()
        def failed(unit):
            return dict(healthy_unit(unit), Result="exit-code", ActiveState="failed")
        values = self.report(failed)["metrics"]
        self.assertEqual(values["probe_success"], 0)
        self.assertEqual(values["service_failed"], 1)
        self.assertEqual(values["timer_active"], 0)

    def test_systemd_timeout_still_exports_disk_and_probe_failure(self):
        def timeout(_unit):
            raise subprocess.TimeoutExpired("systemctl", 5)
        report = self.report(timeout)
        self.assertEqual(report["metrics"]["probe_success"], 0)
        self.assertIn("disk_available_bytes", report["metrics"])
        self.assertIn("engine_b_phase0_archive_probe_success 0\n", render(report))


if __name__ == "__main__":
    unittest.main()
