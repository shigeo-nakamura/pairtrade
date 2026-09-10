#!/usr/bin/env python3
"""Extract session-boundary prices for the Engine B Step 0 absorption test.

bot-strategy#988. Runs *on the Tokyo observer host* (`i-0095af4fe0efbc5dd`),
where the Phase 0A SQLite lives. The database is hourly-partitioned and the
hot window on disk is only ~24 h (bot-strategy#915 retention), so older hours
are pulled back from the S3 archive one partition at a time, queried, and
deleted again — the full corpus is never materialised.

For each KRX business day the observer's frozen calendar marks as *both* KRX
open and US cash open, three instants matter:

    t0 = KRX open   (00:00 UTC)
    t1 = KRX close  (06:30 UTC)  -- Engine B's entry instant
    t2 = US cash open (13:30 UTC) -- Engine B's exit instant

and for each we take the `price_observation` row closest to the instant, for
every requested symbol and price type. Output is JSONL, one row per
(date, point, symbol, price_type), so the statistics step
(`engine_b_step0_absorption.py`) can run anywhere.

The script is append-only and idempotent per partition: rerunning it with the
same `--out` file skips partitions already present unless `--force` is given.

Example (as root on the observer host):

    python3 engine_b_step0_extract.py \
        --calendar /opt/engine-b-phase0/trading_calendar.json \
        --data-dir /var/lib/engine-b-phase0/data \
        --start 2026-08-26 --end 2026-09-10 \
        --out /var/tmp/engine-b-step0/prices.jsonl
"""

from __future__ import annotations

import argparse
import datetime as dt
import gzip
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
from typing import Iterable, Iterator, Optional

# The observer wrote `lighter_mainnet_context` before the single-venue pivot
# (pairtrade#244) and `lighter` after it. Both are the same mainnet venue;
# Robinhood rows must never be mixed in (bot-strategy#872).
DEFAULT_VENUES = ("lighter", "lighter_mainnet_context")
DEFAULT_SYMBOLS = ("SKHYNIXUSD", "SKHY", "SNDK", "MU")
DEFAULT_PRICE_TYPES = ("mid", "mark", "index")
DEFAULT_S3_PREFIX = (
    "s3://debot-dashboard/debot/engine-b/phase0/raw/debot-robinhood-lighter"
)
POINTS = ("t0", "t1", "t2")
US_PER_SEC = 1_000_000


def parse_date(value: str) -> dt.date:
    return dt.datetime.strptime(value, "%Y-%m-%d").date()


def daterange(start: dt.date, end: dt.date) -> Iterator[dt.date]:
    day = start
    while day <= end:
        yield day
        day += dt.timedelta(days=1)


def partition_name(ts_us: int) -> str:
    """Hourly partition key (UTC) a timestamp belongs to."""
    moment = dt.datetime.fromtimestamp(ts_us / US_PER_SEC, dt.timezone.utc)
    return moment.strftime("%Y%m%d_%H")


def session_points(calendar: dict, day: dt.date) -> Optional[dict]:
    """t0/t1/t2 for a day, or None when the day is not a full session.

    A day only qualifies when KRX *and* US cash are both open: t2 is the US
    cash open, so a US holiday (e.g. 2026-09-07 Labor Day) has no exit instant
    even though KRX traded.
    """
    entry = calendar.get("sessions", {}).get(day.isoformat())
    if entry is None:
        return None
    if not entry.get("krx_is_open") or not entry.get("us_is_open"):
        return None
    t0 = entry.get("krx_open_utc_us")
    t1 = entry.get("krx_close_utc_us")
    t2 = entry.get("us_open_utc_us")
    if t0 is None or t1 is None or t2 is None:
        return None
    return {"t0": int(t0), "t1": int(t1), "t2": int(t2)}


def local_partition_path(data_dir: str, name: str) -> str:
    return os.path.join(data_dir, "engine_b_phase0_%s.sqlite3" % name)


def fetch_partition(name: str, s3_prefix: str, workdir: str) -> Optional[str]:
    """Download and decompress one archived partition. None when absent."""
    year, month = name[0:4], name[4:6]
    key = "%s/%s/%s/engine_b_phase0_%s.sqlite3.gz" % (s3_prefix, year, month, name)
    gz_path = os.path.join(workdir, "%s.sqlite3.gz" % name)
    db_path = os.path.join(workdir, "%s.sqlite3" % name)
    result = subprocess.run(
        ["aws", "s3", "cp", "--quiet", key, gz_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    if result.returncode != 0:
        sys.stderr.write(
            "partition %s not fetched: %s\n"
            % (name, result.stdout.decode("utf-8", "replace").strip())
        )
        return None
    try:
        with gzip.open(gz_path, "rb") as src, open(db_path, "wb") as dst:
            shutil.copyfileobj(src, dst, length=1 << 20)
    finally:
        os.unlink(gz_path)
    return db_path


def query_partition(
    db_path: str,
    targets: Iterable[tuple],
    symbols: Iterable[str],
    price_types: Iterable[str],
    venues: Iterable[str],
    tolerance_us: int,
) -> list:
    """Nearest observation to each target instant, per symbol and price type.

    Opened read-only: the current hour's partition may still have the observer
    writing into it.
    """
    symbols = list(symbols)
    price_types = list(price_types)
    venues = list(venues)
    uri = "file:%s?mode=ro" % db_path.replace("?", "%3f").replace("#", "%23")
    rows = []
    conn = sqlite3.connect(uri, uri=True)
    try:
        # One query per series. A single combined query with a row cap would
        # let one busy series (12 symbols x 3 price types at 1 Hz) fill the cap
        # with rows near the instant and silently drop a quieter series whose
        # nearest quote is further out but still inside the tolerance.
        sql = (
            "SELECT observed_ts_us, price, venue FROM price_observation "
            "WHERE observed_ts_us BETWEEN ? AND ? "
            "  AND symbol = ? AND price_type = ? AND venue IN (%s) "
            "ORDER BY ABS(observed_ts_us - ?) LIMIT 1"
            % ",".join("?" * len(venues))
        )
        for day, point, ts_us in targets:
            for symbol in symbols:
                for price_type in price_types:
                    params = (
                        [ts_us - tolerance_us, ts_us + tolerance_us, symbol, price_type]
                        + venues
                        + [ts_us]
                    )
                    hit = conn.execute(sql, params).fetchone()
                    if hit is None:
                        continue
                    observed_us, price, venue = hit
                    rows.append(
                        {
                            "date": day,
                            "point": point,
                            "target_ts_us": ts_us,
                            "observed_ts_us": observed_us,
                            "lag_secs": (observed_us - ts_us) / US_PER_SEC,
                            "symbol": symbol,
                            "price_type": price_type,
                            "price": price,
                            "venue": venue,
                        }
                    )
    finally:
        conn.close()
    return rows


def completion_record(
    name: str,
    targets: Iterable[tuple],
    symbols: Iterable[str],
    price_types: Iterable[str],
    rows: Iterable[dict],
    sealed: bool,
) -> dict:
    """The record that lets a rerun skip this partition, and why.

    An archived partition is sealed and immutable, so whatever it answered is
    final even when that answer is "nothing" -- the mainnet feed being down for
    the whole hour, say. A partition read out of the live data directory is a
    different matter: it may still be filling (today's t2 partition queried
    before 13:30 is the documented case), so it only counts as done once every
    requested series has answered.
    """
    targets = list(targets)
    rows = list(rows)
    wanted = {
        (day, point, symbol, price_type)
        for day, point, _ in targets
        for symbol in symbols
        for price_type in price_types
    }
    got = {(r["date"], r["point"], r["symbol"], r["price_type"]) for r in rows}
    return {
        "partition": name,
        "status": "done",
        "covered": bool(sealed or wanted <= got),
        "sealed": bool(sealed),
        "targets": len(targets),
        "rows": len(rows),
    }


def load_done_partitions(out_path: str) -> set:
    """Partitions a rerun may skip.

    Only an explicit completion record counts, and only one that covered every
    instant it was asked for. An interrupted write, a transient S3 failure, an
    hour the archive has not uploaded yet, or the current day's t2 partition
    queried before 13:30 all leave the partition eligible again — otherwise a
    rerun of the documented command would paper over the hole for good.
    """
    done = set()
    if not os.path.exists(out_path):
        return done
    with open(out_path) as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except ValueError:
                continue
            if record.get("status") == "done" and record.get("covered") is True:
                done.add(record.get("partition"))
    done.discard(None)
    return done


def main(argv: Optional[list] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--calendar", default="/opt/engine-b-phase0/trading_calendar.json")
    parser.add_argument("--data-dir", default="/var/lib/engine-b-phase0/data")
    parser.add_argument("--s3-prefix", default=DEFAULT_S3_PREFIX)
    parser.add_argument("--start", required=True, type=parse_date)
    parser.add_argument("--end", required=True, type=parse_date)
    parser.add_argument("--symbols", default=",".join(DEFAULT_SYMBOLS))
    parser.add_argument("--price-types", default=",".join(DEFAULT_PRICE_TYPES))
    parser.add_argument("--venues", default=",".join(DEFAULT_VENUES))
    parser.add_argument(
        "--tolerance-secs",
        type=float,
        default=300.0,
        help="how far from the instant an observation may sit and still count",
    )
    parser.add_argument("--out", required=True)
    parser.add_argument(
        "--force",
        action="store_true",
        help="re-query every partition, including ones already completed in --out",
    )
    parser.add_argument("--workdir", default="/var/tmp/engine-b-step0/work")
    args = parser.parse_args(argv)

    with open(args.calendar) as handle:
        calendar = json.load(handle)

    symbols = [s for s in args.symbols.split(",") if s]
    price_types = [p for p in args.price_types.split(",") if p]
    venues = [v for v in args.venues.split(",") if v]
    tolerance_us = int(args.tolerance_secs * US_PER_SEC)

    # Group every needed instant by the hourly partition that holds it, so a
    # partition is fetched and decompressed at most once.
    by_partition = {}
    skipped_days = []
    for day in daterange(args.start, args.end):
        points = session_points(calendar, day)
        if points is None:
            skipped_days.append(day.isoformat())
            continue
        for point in POINTS:
            ts_us = points[point]
            by_partition.setdefault(partition_name(ts_us), []).append(
                (day.isoformat(), point, ts_us)
            )

    os.makedirs(os.path.dirname(os.path.abspath(args.out)) or ".", exist_ok=True)
    os.makedirs(args.workdir, exist_ok=True)
    done = set() if args.force else load_done_partitions(args.out)

    print(
        "sessions=%d partitions=%d already_done=%d not_a_session=%d"
        % (
            len({d for targets in by_partition.values() for d, _, _ in targets}),
            len(by_partition),
            len(done & set(by_partition)),
            len(skipped_days),
        )
    )

    written = 0
    with open(args.out, "a") as out:
        for name in sorted(by_partition):
            if name in done:
                continue
            targets = by_partition[name]
            path = local_partition_path(args.data_dir, name)
            fetched = None
            if not os.path.exists(path):
                fetched = fetch_partition(name, args.s3_prefix, args.workdir)
                path = fetched
            if path is None:
                print("partition %s MISSING (no local file, no archive)" % name)
                out.write(
                    json.dumps({"partition": name, "status": "missing", "targets": len(targets)})
                    + "\n"
                )
                out.flush()
                continue
            try:
                rows = query_partition(
                    path, targets, symbols, price_types, venues, tolerance_us
                )
            finally:
                if fetched is not None:
                    os.unlink(fetched)
            for row in rows:
                row["partition"] = name
                row["status"] = "ok"
                out.write(json.dumps(row, sort_keys=True) + "\n")
                written += 1
            # The completion record is written last and on its own line, so a
            # run killed mid-partition leaves that partition eligible again.
            record = completion_record(
                name, targets, symbols, price_types, rows, sealed=fetched is not None
            )
            covered = record["covered"]
            out.write(json.dumps(record, sort_keys=True) + "\n")
            out.flush()
            print(
                "partition %s targets=%d rows=%d%s"
                % (name, len(targets), len(rows), "" if covered else " (incomplete, will retry)")
            )

    print("wrote %d observation rows to %s" % (written, args.out))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
