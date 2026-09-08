#!/usr/bin/env python3
"""Validate a book runtime calendar file (bot-strategy#937 / #948).

`book_runtime --config ... --validate` parses the YAML but does NOT load
the calendar named by `schedule.calendar_path`, so an unparsable or
self-inconsistent calendar would install cleanly and only fail when the
service next starts. This mirrors what `src/book/schedule.rs` accepts:
object shape with deny_unknown_fields at both levels, RFC 3339
timestamps, unique decision keys, flatten strictly after the entry's
signal window, and no overlap between entries.

Both window rules depend on `schedule.signal_grace_secs`, so pass the
deployed value with `--grace-secs` (default 45, the exdiv-lighter
setting); with the wrong value this would accept calendars the runtime
bails on. Entry ORDER is not a rule: `Scheduler::build` sorts by
`decision_at` before checking, so this sorts a copy too rather than
rejecting a calendar the runtime would happily load.

    validate_book_calendar.py configs/book/exdiv-lighter.calendar.json \
        --grace-secs 45

Exits 0 and prints a one-line summary when the calendar is loadable.
"""
import argparse
import json
import math
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


DEFAULT_GRACE_SECS = 45


def ceil_secs(t):
    """The runtime rounds every schedule instant UP to a whole second
    (`signal::ceil_secs`) before comparing, so mirror that here."""
    return math.ceil(t.timestamp())


def _no_duplicate_keys(pairs):
    """serde rejects a repeated struct field; json.load would silently keep
    the last one and promote a calendar the runtime cannot parse."""
    seen = set()
    for k, _ in pairs:
        if k in seen:
            raise SystemExit(f"duplicate JSON key {k!r} in the calendar")
        seen.add(k)
    return dict(pairs)


def validate(path, grace_secs: int = DEFAULT_GRACE_SECS):
    if grace_secs < 0:
        raise SystemExit(f"--grace-secs must not be negative, got {grace_secs}")
    with open(path) as f:
        d = json.load(f, object_pairs_hook=_no_duplicate_keys)
    if not isinstance(d, dict) or not isinstance(d.get("entries"), list):
        raise SystemExit("calendar must be an object with an 'entries' list")
    extra_top = set(d) - {"calendar_version", "entries"}
    if extra_top:
        raise SystemExit(f"unknown top-level calendar key(s): {sorted(extra_top)}")
    # CalendarFile.calendar_version is `#[serde(default)] String`: absent is
    # fine (it defaults to ""), but a number or an explicit null fails to
    # deserialize, so the installer would promote a file the runtime cannot
    # parse on its next restart.
    if "calendar_version" in d and not isinstance(d["calendar_version"], str):
        raise SystemExit(
            f"calendar_version must be a string when present, got {d['calendar_version']!r}")
    if not d["entries"]:
        raise SystemExit("calendar has no entries; the instance would never decide")
    seen, parsed = set(), []
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
        window_end = ceil_secs(dec) + grace_secs
        fl = None
        if e.get("flatten_at") is not None:
            fl = ts(e["flatten_at"], f"{key}: flatten_at")
            # schedule.rs: a flatten inside the signal window would be
            # processed before the decision that opens the leg.
            if ceil_secs(fl) <= window_end:
                raise SystemExit(
                    f"{key}: flatten_at {e['flatten_at']} is inside its signal window, which ends "
                    f"{grace_secs}s after decision_at {e['decision_at']}")
        parsed.append((dec, key, e, max(window_end, ceil_secs(fl) if fl else window_end)))
    # Scheduler::build sorts by decision_at before checking, so entry order
    # is not a rule -- only that the sorted windows do not touch.
    parsed.sort(key=lambda x: x[0])
    for (_d0, k0, _e0, end0), (d1, k1, e1, _end1) in zip(parsed, parsed[1:]):
        if end0 >= ceil_secs(d1):
            end_iso = datetime.fromtimestamp(end0, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            raise SystemExit(
                f"calendar entries {k0} and {k1} overlap: the first's window runs through "
                f"{end_iso} (inclusive) but the second starts at {e1['decision_at']}")
    return d


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("calendar")
    ap.add_argument("--grace-secs", type=int, default=DEFAULT_GRACE_SECS,
                    help="schedule.signal_grace_secs of the instance this calendar belongs to; "
                         "the flatten and overlap rules both depend on it")
    a = ap.parse_args()
    d = validate(a.calendar, a.grace_secs)
    print(f"ok {a.calendar}: {len(d['entries'])} entries, "
          f"version {d.get('calendar_version', '(none)')} (grace {a.grace_secs}s)")


if __name__ == "__main__":
    main()
