#!/usr/bin/env python3

import importlib.util
import json
import math
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

SCRIPT = Path(__file__).with_name("arcus_live_tick_signal_replay.py")
SPEC = importlib.util.spec_from_file_location("arcus_live_tick_signal_replay", SCRIPT)
assert SPEC and SPEC.loader
replay = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = replay
SPEC.loader.exec_module(replay)


def event(sequence, when, price, z_score):
    return {"sequence": sequence, "observed_at": when.isoformat().replace("+00:00", "Z"),
            "relative_log_price": price, "z_score": z_score,
            "decision": {"action": "observe"}}


class ReplayTests(unittest.TestCase):
    def test_extracts_pretty_event_among_systemd_noise(self):
        start = datetime(2026, 8, 21, tzinfo=timezone.utc)
        journal = "Starting...\n" + json.dumps(event(4, start, -0.7, 1.25), indent=2) + "\nDone\n"
        observations, ignored = replay.extract_observations(journal)
        self.assertEqual([item.sequence for item in observations], [4])
        self.assertEqual(observations[0].relative_log_price, -0.7)
        self.assertEqual(ignored, 0)

    def test_rejects_non_stale_duplicate_sequence(self):
        start = datetime(2026, 8, 21, tzinfo=timezone.utc)
        journal = "\n".join(json.dumps(item) for item in
                            (event(4, start, -0.7, 1.25), event(4, start, -0.6, 1.25)))
        with self.assertRaisesRegex(replay.ReplayError, "non-advancing sequence"):
            replay.extract_observations(journal)

    def test_accepts_non_advancing_runtime_stale_event(self):
        start = datetime(2026, 8, 21, tzinfo=timezone.utc)
        stale = event(4, start + timedelta(minutes=1), None, None)
        stale["decision"]["hold"] = {
            "code": "stale_or_duplicate_observation", "detail": "fixture"}
        journal = "\n".join(json.dumps(item) for item in
                            (event(4, start, -0.7, 1.25), stale))
        observations, _ = replay.extract_observations(journal)
        self.assertEqual([item.sequence for item in observations], [4, 4])

    def test_does_not_accept_a_recorder_snapshot_as_a_runtime_event(self):
        start = datetime(2026, 8, 21, tzinfo=timezone.utc)
        recorder = event(3, start, -0.8, 1.0)
        del recorder["decision"]
        journal = "\n".join(json.dumps(item) for item in
                            (recorder, event(4, start, -0.7, 1.25)))
        observations, ignored = replay.extract_observations(journal)
        self.assertEqual([item.sequence for item in observations], [4])
        self.assertEqual(ignored, 1)

    def test_checkpoint_tail_is_bit_exact(self):
        start = datetime(2026, 8, 21, tzinfo=timezone.utc)
        observations = [replay._event_from_object(event(1, start, 0.1, None)),
                        replay._event_from_object(event(2, start + timedelta(minutes=15), 0.2, 1.0))]
        checkpoint = {"schema_version": 1,
                      "config": {"signal_window_samples": 96, "min_signal_samples": 2},
                      "state": {"sequence": 2, "relative_log_price_history": [0.1, 0.2]}}
        self.assertTrue(replay.verify_checkpoint_tail(observations, checkpoint)["checkpoint_tail_exact"])
        checkpoint["state"]["relative_log_price_history"][-1] = 0.20000000000000004
        with self.assertRaisesRegex(replay.ReplayError, "tail differs"):
            replay.verify_checkpoint_tail(observations, checkpoint)

    def test_recomputes_population_z_after_full_exported_window(self):
        start = datetime(2026, 8, 21, tzinfo=timezone.utc)
        history, observations = [], []
        for index in range(8):
            value = index / 100
            score = replay.runtime_z_score(history[-3:], value, 2) if len(history) >= 3 else None
            observations.append(replay._event_from_object(
                event(index + 1, start + timedelta(minutes=15 * index), value, score)))
            history.append(value)
        self.assertEqual(replay.verify_recomputed_scores(observations, 3, 2)["scores_verified"], 5)

    def test_gap_resets_score_verification_window(self):
        start = datetime(2026, 8, 21, tzinfo=timezone.utc)
        observations = [replay._event_from_object(item) for item in (
            event(1, start, 0.1, None),
            event(2, start + timedelta(minutes=15), 0.2, None),
            event(4, start + timedelta(minutes=30), 0.3, 999.0),
            event(5, start + timedelta(minutes=45), 0.4, 999.0))]
        verification = replay.verify_recomputed_scores(observations, 2, 2)
        self.assertEqual(verification["scores_verified"], 0)
        self.assertEqual(verification["score_window_resets"], 1)

    def test_gap_fails_closed_but_suffix_is_authoritative(self):
        start = datetime(2026, 8, 21, tzinfo=timezone.utc)
        journal = "\n".join(json.dumps(item) for item in (
            event(1, start, 0.1, None),
            event(3, start + timedelta(minutes=15), 0.2, None),
            event(4, start + timedelta(minutes=30), 0.3, None)))
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "journal.log"
            path.write_text(journal, encoding="utf-8")
            with self.assertRaisesRegex(replay.ReplayError, "replay refused"):
                replay.run(replay.build_parser().parse_args([str(path)]))
            indicative = replay.run(replay.build_parser().parse_args(
                [str(path), "--allow-gaps"]))
            self.assertFalse(indicative["verification"]["authoritative"])
            self.assertEqual(indicative["verification"]["sequence_gaps"][0]["missing"], 1)
            suffix = replay.run(replay.build_parser().parse_args(
                [str(path), "--start-sequence", "3"]))
            self.assertTrue(suffix["verification"]["authoritative"])
            self.assertEqual(suffix["input"]["first_sequence"], 3)

    def test_replays_mean_reversion_and_max_hold_with_runtime_ordering(self):
        start = datetime(2026, 8, 21, tzinfo=timezone.utc)
        observations = [replay._event_from_object(item) for item in (
            event(1, start, 1.00, 2.6),
            event(2, start + timedelta(hours=1), 0.99, 0.2),
            event(3, start + timedelta(hours=2), 0.98, -2.7),
            event(4, start + timedelta(hours=26), 0.97, -0.1))]
        changes = [replay.ThresholdChange(datetime.min.replace(tzinfo=timezone.utc), 2.5)]
        trades, opened = replay.replay_trades(observations, changes, 0.25, 86400, 35.0)
        self.assertIsNone(opened)
        self.assertEqual([trade.exit_type for trade in trades], ["mean_reversion", "max_hold"])
        self.assertTrue(math.isclose(trades[0].gross_bps, 100.0))
        self.assertTrue(math.isclose(trades[0].net_bps, 65.0))

    def test_threshold_change_controls_entries(self):
        start = datetime(2026, 8, 20, tzinfo=timezone.utc)
        observations = [replay._event_from_object(item) for item in (
            event(1, start, 0.9, 2.2), event(2, start + timedelta(hours=1), 0.8, 0.0),
            event(3, start + timedelta(hours=2), 0.9, 2.2),
            event(4, start + timedelta(hours=3), 0.8, 0.0))]
        changes = [replay.ThresholdChange(datetime.min.replace(tzinfo=timezone.utc), 2.5),
                   replay.ThresholdChange(start + timedelta(hours=2), 2.0)]
        trades, _ = replay.replay_trades(observations, changes, 0.25, 86400, 0.0)
        self.assertEqual(len(trades), 1)
        self.assertEqual(trades[0].entry_at, observations[2].observed_at_raw)
        self.assertEqual(replay.exceedance_counts(observations, [2.5, 2.0]), {"2.5": 0, "2": 2})


if __name__ == "__main__":
    unittest.main()
