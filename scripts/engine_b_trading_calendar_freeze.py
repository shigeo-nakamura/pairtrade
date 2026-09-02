#!/usr/bin/env python3
"""Freeze a KRX/US-cash trading-session calendar for Engine B (bot-strategy#866, A-7).

Offline tool only. It is never installed on the Phase 0 observer host and the
observer (`engine_b_phase0.py`) never imports `exchange_calendars` itself --
that keeps the read-only collector's dependency surface at just `websockets`
(see scripts/engine_b_phase0_requirements.txt and
scripts/test_engine_b_phase0.py::ReadOnlyBoundaryTests). This script resolves
KRX (XKRX) and US cash-market (XNYS) trading sessions -- business days,
holidays, and DST-aware open/close times -- into a static JSON artifact that
the observer loads with stdlib `json` at startup.

Run locally, review the diff, and commit the output alongside a code change.
CI regenerates it from the committed --start/--end range and diffs against
the committed file to catch drift or hand edits (see
.github/workflows/deploy-configs.yml, "Verify Engine B trading calendar
freeze").

Re-run this only as a deliberate, reviewed step: to extend the covered date
range, to pick up an exchange_calendars release, or after cross-checking a
specific year against KRX's own published holiday notice (rule-based
generation can miss one-off administrative holidays -- see
docs/engine-b-phase0-operations.md).
"""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import date, timedelta
from importlib.metadata import version as pkg_version
from pathlib import Path
from typing import Any

import exchange_calendars as xcals

KRX_CALENDAR = "XKRX"
US_CALENDAR = "XNYS"

# One-off KRX closures that exchange_calendars' rule-based/recurring-holiday
# data does not capture -- administrative closures the exchange announces
# separately, sometimes years in advance of the recurring-holiday table being
# updated. Each entry needs a citable primary source (the exchange's own
# announcement, not just an aggregator) recorded here; cross-check a given
# year against KRX's own published notice before trusting rule-only output
# for it (see docs/engine-b-phase0-operations.md).
KRX_ONE_OFF_CLOSURES: dict[str, str] = {
    # 9th nationwide local elections (지방선거): government-designated public
    # holiday; KRX announced closure of securities/derivatives/commodities
    # markets on 2026-05-20.
    # https://en.sedaily.com/finance/2026/05/20/korea-exchange-to-close-on-local-election-day-constitution
    "2026-06-03": "kr_local_election_day",
    # Constitution Day (제헌절) was reinstated as a public holiday for 2026;
    # KRX announced the same closure alongside Local Election Day.
    # https://en.sedaily.com/finance/2026/05/20/korea-exchange-to-close-on-local-election-day-constitution
    "2026-07-17": "kr_constitution_day_2026_reinstatement",
}

# KRX delays the cash-market open (and close) by one hour on the day of the
# national CSAT/College Scholastic Ability Test (수능) each year, to reduce
# noise/traffic near exam sites -- an annual practice exchange_calendars does
# not encode. Korean CSAT naming is offset by academic year (a "2027학년도"
# exam is administered in November 2026); each entry here is keyed by the
# calendar date the exam -- and the delay -- actually falls on, with a
# citable primary/government source.
KRX_DELAYED_OPEN_ONE_HOUR: dict[str, str] = {
    # 2027학년도 CSAT, administered 2026-11-19 (Thu).
    # https://ko.wikipedia.org/wiki/2027학년도_대학수학능력시험
    "2026-11-19": "kr_csat_delayed_open",
    # 2028학년도 CSAT, administered 2027-11-18 (Thu); government-confirmed.
    # https://www.korea.kr/briefing/pressReleaseView.do?newsId=156692078
    "2027-11-18": "kr_csat_delayed_open",
}
_ONE_HOUR_US = 3_600_000_000


def _daterange(start: date, end: date):
    current = start
    one_day = timedelta(days=1)
    while current <= end:
        yield current
        current += one_day


def _to_utc_us(timestamp: Any) -> int:
    # exchange_calendars returns tz-aware pandas Timestamps normalized to UTC.
    return int(timestamp.value // 1_000)


# exchange_calendars' default (unparameterized) calendar instance only spans
# a library-chosen window around "now", and is_session() raises
# DateOutOfBounds -- rather than returning False -- for any date outside the
# instance's actual first/last *session*, which can be later/earlier than the
# start=/end= passed to get_calendar() once holidays are excluded. Padding
# comfortably past our requested range keeps every iterated date, including
# non-session ones, inside the instance's true bounds.
_QUERY_PADDING_DAYS = 14


def build_sessions(start: date, end: date) -> dict[str, dict[str, Any]]:
    query_start = start - timedelta(days=_QUERY_PADDING_DAYS)
    query_end = end + timedelta(days=_QUERY_PADDING_DAYS)
    krx = xcals.get_calendar(KRX_CALENDAR, start=query_start.isoformat(), end=query_end.isoformat())
    us = xcals.get_calendar(US_CALENDAR, start=query_start.isoformat(), end=query_end.isoformat())
    krx_bound_min, krx_bound_max = krx.bound_min(), krx.bound_max()
    if krx_bound_min is not None and query_start < krx_bound_min.date():
        raise SystemExit(f"--start {start} is too close to {KRX_CALENDAR}'s earliest supported date {krx_bound_min.date()}")
    if krx_bound_max is not None and query_end > krx_bound_max.date():
        raise SystemExit(f"--end {end} is too close to {KRX_CALENDAR}'s latest supported date {krx_bound_max.date()}")

    sessions: dict[str, dict[str, Any]] = {}
    for day in _daterange(start, end):
        iso = day.isoformat()
        krx_is_open = bool(krx.is_session(iso)) and iso not in KRX_ONE_OFF_CLOSURES
        us_is_open = bool(us.is_session(iso))
        entry: dict[str, Any] = {
            "krx_is_open": krx_is_open,
            "krx_open_utc_us": None,
            "krx_close_utc_us": None,
            "us_is_open": us_is_open,
            "us_open_utc_us": None,
            "us_close_utc_us": None,
        }
        if krx_is_open:
            entry["krx_open_utc_us"] = _to_utc_us(krx.session_open(iso))
            entry["krx_close_utc_us"] = _to_utc_us(krx.session_close(iso))
            if iso in KRX_DELAYED_OPEN_ONE_HOUR:
                entry["krx_open_utc_us"] += _ONE_HOUR_US
                entry["krx_close_utc_us"] += _ONE_HOUR_US
        if us_is_open:
            entry["us_open_utc_us"] = _to_utc_us(us.session_open(iso))
            entry["us_close_utc_us"] = _to_utc_us(us.session_close(iso))
        sessions[iso] = entry
    return sessions


def build_document(start: date, end: date) -> dict[str, Any]:
    sessions = build_sessions(start, end)
    sessions_json = json.dumps(sessions, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(sessions_json.encode("utf-8")).hexdigest()[:12]
    package_version = pkg_version("exchange_calendars")
    # No generation timestamp: this document is deterministic in
    # (start, end, exchange_calendars version) so CI can regenerate it and
    # diff byte-for-byte against the committed file. Provenance (when/who)
    # comes from git history instead.
    def _filter_to_range(overrides: dict[str, str]) -> dict[str, str]:
        return {iso: reason for iso, reason in overrides.items() if start.isoformat() <= iso <= end.isoformat()}

    return {
        "schema_version": 1,
        "calendar_version": f"xkrx-xnys-exchange_calendars-{package_version}-{digest}",
        "source_package": f"exchange_calendars=={package_version}",
        "krx_calendar": KRX_CALENDAR,
        "us_calendar": US_CALENDAR,
        "range": {"start": start.isoformat(), "end": end.isoformat()},
        # Not part of `digest` (that hashes only `sessions`), but still part
        # of what CI's byte-for-byte diff verifies -- an audit trail for why
        # a date disagrees with the library's own recurring-holiday rules.
        "krx_one_off_closures": _filter_to_range(KRX_ONE_OFF_CLOSURES),
        "krx_delayed_open_one_hour": _filter_to_range(KRX_DELAYED_OPEN_ONE_HOUR),
        "sessions": sessions,
    }


def write_document(document: dict[str, Any], out: Path) -> None:
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(document, sort_keys=True, indent=2) + "\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--start", required=True, type=date.fromisoformat, help="YYYY-MM-DD, inclusive")
    parser.add_argument("--end", required=True, type=date.fromisoformat, help="YYYY-MM-DD, inclusive")
    parser.add_argument("--out", required=True, type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.end < args.start:
        raise SystemExit("--end must not be before --start")
    document = build_document(args.start, args.end)
    write_document(document, args.out)
    print(
        f"wrote {args.out} calendar_version={document['calendar_version']} "
        f"sessions={len(document['sessions'])}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
