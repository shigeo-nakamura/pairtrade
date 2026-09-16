#!/usr/bin/env python3
"""Turn the points snapshot series into the per-day rows the ledger takes.

bot-strategy#938. `robinhood_points_collector.py` appends one snapshot
per arm per run; `subsidy_ledger.py --points` wants
`{"date": "YYYY-MM-DD", "arm": "freq", "points": N}` -- the points earned
*on* that UTC day. This is the conversion, and it is deliberately dumb:

    points(day, arm) = tally(last snapshot on day) - tally(last snapshot on
                       the previous calendar day)

A day whose previous calendar day has no snapshot is skipped and named
on stderr rather than priced from a delta that spans several days: the
ledger joins on the date, and a lump attributed to one day would make
that day look cheap and the missing ones free. The first day of the
series is skipped for the same reason (its opening tally is unknown).

`--tally` chooses which of the venue's tallies to difference and is
required, not defaulted: the venue credits points "in real time" and
again in a weekly drop, and which series is the one the program will
finally count is not documented. Differencing `total_points` on a
series that only moves on drop days puts a week of points on one
Friday; differencing `live_points_total` may count points the drop
later revises. The choice is the operator's, per run, and is echoed
into every output row as `tally` so the ledger's input says what it is.

A negative day (the venue revised a tally downward, T&C section 4) is
skipped and flagged on stderr rather than written: the ledger rejects a
negative points row outright (it would make the file unreadable), and a
day whose points were revoked has no honest price -- the cost stayed and
the points went. The revised tally still becomes the next day's baseline,
so the revision is not lost, only not priced.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

TALLIES = ("live_points_total", "total_points")


class PointsDailyError(Exception):
    pass


def read_snapshots(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    lines = path.read_text(encoding="utf-8").split("\n")
    # The collector appends whole lines under one `write`, so a torn tail
    # is a crash mid-append; interior damage is not tolerated (same rule
    # as the ledger's read_jsonl).
    if lines and lines[-1] == "":
        lines.pop()
    for number, line in enumerate(lines, start=1):
        try:
            row = json.loads(line)
        except json.JSONDecodeError as exc:
            if number == len(lines):
                print(f"{path}:{number}: torn last line ignored", file=sys.stderr)
                continue
            raise PointsDailyError(f"{path}:{number}: unreadable snapshot: {exc}") from exc
        if not isinstance(row, dict):
            raise PointsDailyError(f"{path}:{number}: snapshot is not an object")
        rows.append(row)
    return rows


def utc_date_of(ts_unix: int) -> str:
    return datetime.fromtimestamp(ts_unix, tz=timezone.utc).strftime("%Y-%m-%d")


def previous_day(date: str) -> str:
    return (datetime.strptime(date, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")


def last_snapshot_per_day(rows: list[dict[str, Any]], tally: str
                          ) -> dict[str, dict[str, tuple[int, float]]]:
    """`{arm: {date: (ts_unix, value)}}`, keeping the latest snapshot of
    each day. A row missing the arm, the timestamp or the chosen tally
    is an error: these rows are produced by one script, so a hole is
    damage, not variety."""
    latest: dict[str, dict[str, tuple[int, float]]] = defaultdict(dict)
    for row in rows:
        arm, ts_unix, value = row.get("arm"), row.get("ts_unix"), row.get(tally)
        if not isinstance(arm, str) or not arm:
            raise PointsDailyError(f"snapshot without an arm: {row!r}")
        if not isinstance(ts_unix, int) or isinstance(ts_unix, bool):
            raise PointsDailyError(f"snapshot without an integer ts_unix: {row!r}")
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise PointsDailyError(f"snapshot for {arm} has no numeric {tally}: {row!r}")
        date = utc_date_of(ts_unix)
        current = latest[arm].get(date)
        if current is None or ts_unix >= current[0]:
            latest[arm][date] = (ts_unix, float(value))
    return latest


def daily_points(latest: dict[str, dict[str, tuple[int, float]]], tally: str
                 ) -> tuple[list[dict[str, Any]], list[str]]:
    """Per-(date, arm) deltas, plus the reasons for every skipped day."""
    out: list[dict[str, Any]] = []
    notes: list[str] = []
    for arm in sorted(latest):
        by_date = latest[arm]
        for date in sorted(by_date):
            prev = previous_day(date)
            if prev not in by_date:
                notes.append(f"{arm} {date}: no snapshot on {prev}; day skipped")
                continue
            points = by_date[date][1] - by_date[prev][1]
            if points < 0:
                # The ledger refuses a negative points row (a hand-typed
                # file's typo, in its world), and a day whose points were
                # revoked has no honest price anyway: the cost stayed, the
                # points went. Skip it and say so; the closing tally still
                # carries the revision into the next day's baseline.
                notes.append(
                    f"{arm} {date}: {tally} fell by {-points} (venue revision); day skipped")
                continue
            out.append({"date": date, "arm": arm, "points": points, "tally": tally,
                        "closing_ts_unix": by_date[date][0]})
    return out, notes


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("history", type=Path, help="points_history.jsonl from the collector")
    parser.add_argument("--tally", choices=TALLIES, required=True,
                        help="which venue tally to difference (see module doc)")
    parser.add_argument("--out", type=Path, default=None,
                        help="write the ledger's --points file here (default: stdout)")
    args = parser.parse_args(argv)

    try:
        latest = last_snapshot_per_day(read_snapshots(args.history), args.tally)
    except PointsDailyError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    rows, notes = daily_points(latest, args.tally)
    for note in notes:
        print(note, file=sys.stderr)
    text = "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows)
    if args.out is None:
        sys.stdout.write(text)
    else:
        args.out.write_text(text, encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
