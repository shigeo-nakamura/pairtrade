#!/usr/bin/env python3
"""Validate a book runtime calendar file (bot-strategy#937 / #948).

`book_runtime --config ... --validate` parses the YAML but does NOT load
the calendar named by `schedule.calendar_path`, so an unparsable or
self-inconsistent calendar would install cleanly and only fail when the
service next starts. This mirrors what `src/book/schedule.rs` accepts:
object shape with deny_unknown_fields at both levels, RFC 3339
timestamps, unique decision keys, flatten after decision, and no overlap
between consecutive entries.

    validate_book_calendar.py configs/book/exdiv-lighter.calendar.json

Exits 0 and prints a one-line summary when the calendar is loadable.
"""
import json
import re
import sys
from datetime import datetime, timezone

# Deliberately stricter than datetime.fromisoformat, which since 3.11
# accepts a space separator and a missing seconds field. The runtime
# deserialises these with chrono's RFC 3339 parser, which does not: a
# calendar Python accepted but Rust rejects is exactly the failure this
# script exists to prevent.
RFC3339 = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$")


def ts(v, what):
    if not isinstance(v, str):
        raise SystemExit(f"{what} must be a string timestamp, got {v!r}")
    if not RFC3339.match(v):
        raise SystemExit(
            f"{what} is not an RFC 3339 timestamp (YYYY-MM-DDTHH:MM:SSZ): {v!r}")
    try:
        return datetime.fromisoformat(v.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError as e:
        raise SystemExit(f"{what} is not a valid timestamp: {v!r} ({e})")


def validate(path):
    with open(path) as f:
        d = json.load(f)
    if not isinstance(d, dict) or not isinstance(d.get("entries"), list):
        raise SystemExit("calendar must be an object with an 'entries' list")
    extra_top = set(d) - {"calendar_version", "entries"}
    if extra_top:
        raise SystemExit(f"unknown top-level calendar key(s): {sorted(extra_top)}")
    if not d["entries"]:
        raise SystemExit("calendar has no entries; the instance would never decide")
    seen, prev_end, prev_key = set(), None, None
    for e in d["entries"]:
        if not isinstance(e, dict):
            raise SystemExit(f"calendar entry must be an object, got {e!r}")
        extra = set(e) - {"decision_key", "decision_at", "flatten_at"}
        if extra:
            raise SystemExit(f"unknown key(s) {sorted(extra)} in calendar entry {e!r}")
        key = e.get("decision_key")
        if not isinstance(key, str) or not key.strip():
            raise SystemExit(f"decision_key must be a non-empty string in {e!r}")
        if key in seen:
            raise SystemExit(f"duplicate decision_key {key!r}; the runtime applies a key once")
        seen.add(key)
        dec = ts(e.get("decision_at"), f"{key}: decision_at")
        end = dec
        if e.get("flatten_at") is not None:
            fl = ts(e["flatten_at"], f"{key}: flatten_at")
            if fl <= dec:
                raise SystemExit(f"{key}: flatten_at {e['flatten_at']} is not after decision_at")
            end = fl
        if prev_end is not None and dec <= prev_end:
            raise SystemExit(
                f"{key} starts at {e['decision_at']} but {prev_key} runs through "
                f"{prev_end:%Y-%m-%dT%H:%M:%SZ} (inclusive); calendar entries must not "
                "overlap and must be in order")
        prev_end, prev_key = end, key
    return d


def main():
    if len(sys.argv) != 2:
        raise SystemExit("usage: validate_book_calendar.py <calendar.json>")
    d = validate(sys.argv[1])
    print(f"ok {sys.argv[1]}: {len(d['entries'])} entries, "
          f"version {d.get('calendar_version', '(none)')}")


if __name__ == "__main__":
    main()
