#!/usr/bin/env python3
"""Verify the durable hash-chained Arcus live-tick event stream."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

DOMAIN = b"arcus-live-tick-event-stream-v1\0"
SCHEMA_VERSION = 1
SHA256_PREFIX = "sha256:"
REQUIRED_EVENT_FIELDS = {
    "sequence", "observed_at", "relative_log_price", "z_score", "decision",
}


class StreamError(ValueError):
    pass


def sha256_prefixed(payload: bytes) -> str:
    return SHA256_PREFIX + hashlib.sha256(payload).hexdigest()


def chain_sha256(previous: str | None, event_sha256: str) -> str:
    digest = hashlib.sha256()
    digest.update(DOMAIN)
    digest.update((previous or "-").encode())
    digest.update(b"\0")
    digest.update(event_sha256.encode())
    return SHA256_PREFIX + digest.hexdigest()


def valid_sha256(value: Any) -> bool:
    return (
        isinstance(value, str)
        and len(value) == len(SHA256_PREFIX) + 64
        and value.startswith(SHA256_PREFIX)
        and all(character in "0123456789abcdef"
                for character in value[len(SHA256_PREFIX):])
    )


def parse_timestamp(value: Any) -> datetime:
    if not isinstance(value, str):
        raise StreamError("event observed_at must be a string")
    match = re.fullmatch(
        r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d+))?"
        r"(Z|[+-]\d{2}:\d{2})",
        value,
    )
    if not match:
        raise StreamError(f"invalid event observed_at {value!r}")
    base, fraction, offset = match.groups()
    micros = (fraction or "0")[:6].ljust(6, "0")
    normalized = f"{base}.{micros}{'+00:00' if offset == 'Z' else offset}"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as error:
        raise StreamError(f"invalid event observed_at {value!r}") from error
    return parsed.astimezone(timezone.utc)


def parse_record(line: bytes, source: str) -> tuple[dict[str, Any], dict[str, Any]]:
    try:
        record = json.loads(line)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise StreamError(f"{source}: invalid JSON record: {error}") from error
    expected = {
        "schema_version", "previous_chain_sha256", "event_sha256",
        "chain_sha256", "event_json",
    }
    if not isinstance(record, dict) or set(record) != expected:
        raise StreamError(f"{source}: unexpected event-stream record fields")
    if record["schema_version"] != SCHEMA_VERSION:
        raise StreamError(
            f"{source}: unsupported schema {record['schema_version']!r}")
    previous = record["previous_chain_sha256"]
    if previous is not None and not valid_sha256(previous):
        raise StreamError(f"{source}: invalid previous_chain_sha256")
    if not valid_sha256(record["event_sha256"]):
        raise StreamError(f"{source}: invalid event_sha256")
    if not valid_sha256(record["chain_sha256"]):
        raise StreamError(f"{source}: invalid chain_sha256")
    event_json = record["event_json"]
    if not isinstance(event_json, str):
        raise StreamError(f"{source}: event_json must be a string")
    actual_event_hash = sha256_prefixed(event_json.encode())
    if actual_event_hash != record["event_sha256"]:
        raise StreamError(
            f"{source}: event payload hash mismatch "
            f"(expected {record['event_sha256']}, got {actual_event_hash})")
    actual_chain_hash = chain_sha256(previous, record["event_sha256"])
    if actual_chain_hash != record["chain_sha256"]:
        raise StreamError(
            f"{source}: chain hash mismatch "
            f"(expected {record['chain_sha256']}, got {actual_chain_hash})")
    try:
        event = json.loads(event_json)
    except json.JSONDecodeError as error:
        raise StreamError(f"{source}: invalid event_json: {error}") from error
    if not isinstance(event, dict) or not REQUIRED_EVENT_FIELDS.issubset(event):
        raise StreamError(f"{source}: payload is not an Arcus runtime event")
    if (not isinstance(event["sequence"], int)
            or isinstance(event["sequence"], bool)
            or event["sequence"] < 1):
        raise StreamError(f"{source}: invalid event sequence")
    if not isinstance(event["decision"], dict):
        raise StreamError(f"{source}: event decision must be an object")
    parse_timestamp(event["observed_at"])
    return record, event


def is_stale_same_sequence(previous: dict[str, Any],
                           current: dict[str, Any]) -> bool:
    hold = current["decision"].get("hold")
    return (
        current["sequence"] == previous["sequence"]
        and isinstance(hold, dict)
        and hold.get("code") == "stale_or_duplicate_observation"
        and current.get("relative_log_price") is None
        and current.get("z_score") is None
    )


def verify_stream_bytes(payloads: Sequence[tuple[str, bytes]]) -> tuple[
        list[dict[str, Any]], dict[str, Any]]:
    records: list[dict[str, Any]] = []
    events: list[dict[str, Any]] = []
    combined = hashlib.sha256()
    previous_time: datetime | None = None

    for path, payload in payloads:
        if not payload.endswith(b"\n"):
            raise StreamError(f"{path}: unterminated final record")
        combined.update(payload)
        for line_number, line in enumerate(payload.splitlines(), 1):
            if not line:
                raise StreamError(f"{path}:{line_number}: empty record")
            record, event = parse_record(line, f"{path}:{line_number}")
            if records and record["previous_chain_sha256"] != records[-1]["chain_sha256"]:
                raise StreamError(f"{path}:{line_number}: hash-chain break")
            if events:
                previous = events[-1]
                if (event["sequence"] != previous["sequence"] + 1
                        and not is_stale_same_sequence(previous, event)):
                    raise StreamError(
                        f"{path}:{line_number}: sequence discontinuity "
                        f"{previous['sequence']} -> {event['sequence']}")
            observed_at = parse_timestamp(event["observed_at"])
            if previous_time is not None and observed_at < previous_time:
                raise StreamError(f"{path}:{line_number}: timestamp regression")
            previous_time = observed_at
            records.append(record)
            events.append(event)

    if not records:
        raise StreamError("event stream is empty")
    report = {
        "schema_version": SCHEMA_VERSION,
        "records": len(records),
        "advancing_records": sum(
            index == 0 or event["sequence"] > events[index - 1]["sequence"]
            for index, event in enumerate(events)
        ),
        "first_sequence": events[0]["sequence"],
        "last_sequence": events[-1]["sequence"],
        "first_observed_at": events[0]["observed_at"],
        "last_observed_at": events[-1]["observed_at"],
        "first_previous_chain_sha256": records[0]["previous_chain_sha256"],
        "first_chain_sha256": records[0]["chain_sha256"],
        "last_chain_sha256": records[-1]["chain_sha256"],
        "stream_sha256": SHA256_PREFIX + combined.hexdigest(),
        "hash_chain_valid": True,
        "sequence_continuity_valid": True,
    }
    return events, report


def verify_paths(paths: Sequence[Path]) -> tuple[
        list[dict[str, Any]], dict[str, Any]]:
    return verify_stream_bytes([
        (str(path), path.read_bytes()) for path in paths
    ])


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("stream", type=Path, nargs="+",
                        help="daily JSONL segment(s), in chain order")
    parser.add_argument("--manifest-out", type=Path)
    parser.add_argument("--events-out", type=Path,
                        help="write verified plain runtime-event JSONL")
    return parser


def run(arguments: argparse.Namespace) -> dict[str, Any]:
    events, report = verify_paths(arguments.stream)
    if arguments.events_out:
        with arguments.events_out.open("w", encoding="utf-8") as output:
            for event in events:
                output.write(json.dumps(event, separators=(",", ":")) + "\n")
    if arguments.manifest_out:
        with arguments.manifest_out.open("w", encoding="utf-8") as output:
            json.dump(report, output, indent=2, sort_keys=True)
            output.write("\n")
    return report


def main() -> int:
    parser = build_parser()
    try:
        report = run(parser.parse_args())
    except (OSError, StreamError) as error:
        parser.exit(2, f"error: {error}\n")
    json.dump(report, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
