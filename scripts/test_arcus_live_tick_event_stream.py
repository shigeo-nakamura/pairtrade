#!/usr/bin/env python3

import hashlib
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).with_name("arcus_live_tick_event_stream.py")
SPEC = importlib.util.spec_from_file_location("arcus_event_stream", SCRIPT)
stream = importlib.util.module_from_spec(SPEC)
assert SPEC.loader
SPEC.loader.exec_module(stream)


def event(sequence, observed_at, *, stale=False):
    return {
        "sequence": sequence,
        "observed_at": observed_at,
        "pair": "NVDA/AMD",
        "mode": "live",
        "token_a_reference_price_usd": "200",
        "token_b_reference_price_usd": "100",
        "relative_log_price": None if stale else 0.5,
        "z_score": None if stale else 1.0,
        "inventory_before": {"token_a": "1", "token_b": "1"},
        "inventory_after": {"token_a": "1", "token_b": "1"},
        "regime_before": "neutral",
        "regime_after": "neutral",
        "risk_before": None,
        "risk_after": None,
        "decision": {
            "action": "observe",
            "hold": {
                "code": ("stale_or_duplicate_observation"
                         if stale else "no_signal"),
                "detail": "test",
            },
        },
    }


def record(item, previous=None):
    event_json = json.dumps(item, separators=(",", ":"))
    event_hash = stream.sha256_prefixed(event_json.encode())
    return {
        "schema_version": 1,
        "previous_chain_sha256": previous,
        "event_sha256": event_hash,
        "chain_sha256": stream.chain_sha256(previous, event_hash),
        "event_json": event_json,
    }


def encoded(*items):
    previous = None
    lines = []
    for item in items:
        row = record(item, previous)
        previous = row["chain_sha256"]
        lines.append(json.dumps(row, separators=(",", ":")))
    return ("\n".join(lines) + "\n").encode()


class EventStreamTests(unittest.TestCase):
    def test_verifies_hashes_chain_sequence_and_manifest(self):
        payload = encoded(
            event(41, "2026-08-24T23:47:00Z"),
            event(42, "2026-08-25T00:02:00Z"),
        )
        events, report = stream.verify_stream_bytes([("events.jsonl", payload)])
        self.assertEqual([item["sequence"] for item in events], [41, 42])
        self.assertEqual(report["records"], 2)
        self.assertEqual(report["advancing_records"], 2)
        self.assertTrue(report["hash_chain_valid"])
        self.assertEqual(
            report["stream_sha256"],
            "sha256:" + hashlib.sha256(payload).hexdigest(),
        )

    def test_accepts_stale_nonadvancing_event(self):
        payload = encoded(
            event(41, "2026-08-25T00:02:00Z"),
            event(41, "2026-08-25T00:03:00Z", stale=True),
            event(42, "2026-08-25T00:17:00Z"),
        )
        _, report = stream.verify_stream_bytes([("events.jsonl", payload)])
        self.assertEqual(report["records"], 3)
        self.assertEqual(report["advancing_records"], 2)

    def test_rejects_payload_tampering(self):
        payload = encoded(event(41, "2026-08-25T00:02:00Z"))
        value = json.loads(payload)
        value["event_json"] = value["event_json"].replace("NVDA", "SPY")
        tampered = (json.dumps(value) + "\n").encode()
        with self.assertRaisesRegex(stream.StreamError, "payload hash mismatch"):
            stream.verify_stream_bytes([("events.jsonl", tampered)])

    def test_rejects_unterminated_final_record(self):
        payload = encoded(event(41, "2026-08-25T00:02:00Z")).rstrip(b"\n")
        with self.assertRaisesRegex(stream.StreamError, "unterminated final record"):
            stream.verify_stream_bytes([("events.jsonl", payload)])

    def test_rejects_chain_break_across_files(self):
        first = encoded(event(41, "2026-08-24T23:47:00Z"))
        second = encoded(event(42, "2026-08-25T00:02:00Z"))
        with self.assertRaisesRegex(stream.StreamError, "hash-chain break"):
            stream.verify_stream_bytes([("first", first), ("second", second)])

    def test_rejects_sequence_gap(self):
        payload = encoded(
            event(41, "2026-08-25T00:02:00Z"),
            event(43, "2026-08-25T00:17:00Z"),
        )
        with self.assertRaisesRegex(stream.StreamError, "sequence discontinuity"):
            stream.verify_stream_bytes([("events.jsonl", payload)])

    def test_writes_verified_plain_events_and_manifest(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "events.jsonl"
            source.write_bytes(encoded(event(41, "2026-08-25T00:02:00Z")))
            arguments = stream.build_parser().parse_args([
                str(source),
                "--manifest-out", str(root / "manifest.json"),
                "--events-out", str(root / "plain.jsonl"),
            ])
            report = stream.run(arguments)
            self.assertEqual(json.loads((root / "manifest.json").read_text()),
                             report)
            self.assertEqual(
                json.loads((root / "plain.jsonl").read_text())["sequence"],
                41,
            )


if __name__ == "__main__":
    unittest.main()
