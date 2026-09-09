#!/usr/bin/env python3
"""Daily subsidy KPI ledger for the Robinhood Chain Lighter arms.

bot-strategy#938. These bots are subsidy-capture: they pay fees, slippage
and adverse selection to earn points, so the number that matters is the
price per unit of subsidy, not the PnL (`docs/return-source-taxonomy.md`
§4.2). This aggregates the two ledgers the bot already writes into one
row per (UTC date, arm).

What the row means
------------------
`cost_usd` is money given up, so it is **positive when the arm lost** and
negative when it came out ahead. It is `-(realized_pnl + funding)`.

Slippage is deliberately **not** subtracted on top of that. Realized PnL
is computed from actual fill prices and already carries it; subtracting
it again would double-count the same dollars. The slippage column is a
diagnostic — it says where the cost came from — and is never added into
`cost_usd`.

Fees are a known gap rather than a decision: **no fee field is recorded
anywhere** in this bot's ledgers (checked across the execution ledger for
2026-08-14..09-08). If the venue charges a taker fee that the PnL row's
price arithmetic does not already capture, every cost below understates
it by that amount, uniformly. Closing this needs the bot to record
`filled_fee` from the fill response; it cannot be recovered from what is
written today, and is not guessed at here.

The denominator
---------------
The points a program awards are not readable from any endpoint reachable
here (`api.rh.lighter.xyz` answers 403 on the points routes), so points
are an operator-supplied input: export them and pass `--points`. Rows
without them keep `points: null` and no cost-per-point.

A cycle that crosses UTC midnight is the one attribution the two
ledgers genuinely disagree on: `load_execution` files each fill under
the day it happened, `load_pnl` files the whole realized cycle under the
day it closed. The entry notional therefore sits on a day with no cost
(reported as uncosted volume) while the whole cost lands on the next,
with only the exit side to divide by. A `leg_fill` carries no cycle id,
so the fills cannot be tied back and re-attributed from what is written
today; the close day is marked instead (`cross_day_cycles`) and its
per-volume rate suppressed, the same treatment an unvalued fill gets.
Closing it properly needs the bot to stamp a cycle id on both ledgers.

`cost_per_musd_volume` needs no points at all and is the KPI to steer by
in the meantime: points programs are volume-weighted, so the conversion
from "cost per $1M traded" to "cost per point" is one multiplier applied
at the end, and every lever that lowers one lowers the other.

Coverage is never assumed
-------------------------
A day with volume but no cost source reports `cost_usd: null` and is left
out of the totals rather than counted as free. The execution ledger and
the PnL ledger do not cover the same days (on the live host: execution
from 2026-08-14, PnL only from 2026-09-01), and treating the difference
as zero cost would make the arms' worst stretch look like their cheapest.
`--equity` fills that gap from an equity_history series, marked as such,
since an equity delta also carries mark-to-market that a realized-cycle
ledger does not.

The rule behind that is one invariant, applied everywhere rather than
per known failure: **a day is costed from the PnL ledger only if every
row it holds is a realized, live-money close.** A single row that is not
-- an explicit `pnl_available: false`, a DRY_RUN close, a placeholder
from recovery, or any `source` this script has not been taught to read
-- makes that day's PnL coverage incomplete, and the day falls through
to the equity delta or stays uncosted. The allowlist fails safe in the
direction that matters: an unrecognised source makes a day *uncosted and
visibly so*, never free.
"""

from __future__ import annotations

import argparse
import glob
import json
import math
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable


def utc_date(ts_seconds: float) -> str:
    return datetime.fromtimestamp(ts_seconds, timezone.utc).strftime("%Y-%m-%d")


@dataclass
class ExecDay:
    fills: int = 0
    volume_usd: float = 0.0
    slippage_usd: float = 0.0
    # A fill that moved quantity but reported no value. Its volume is
    # missing from the denominator, so the day's volume is a lower bound
    # rather than the number it looks like.
    fills_without_value: int = 0
    # Fills whose slippage diagnostic could not be read (unparseable, or
    # a non-finite token). The column is a diagnostic, not a cost, so
    # this only qualifies that column.
    slippage_unreadable: int = 0


@dataclass
class PnlDay:
    cycles: int = 0
    realized_pnl_usd: float = 0.0
    funding_usd: float = 0.0
    funding_seen: bool = False
    # Set by any row that is not a realized, live-money close. A day with
    # this set is never costed from the PnL ledger, however many good rows
    # it also holds: a partial sum presented as the day's cost is a wrong
    # number, not a smaller one.
    incomplete: bool = False
    incomplete_reasons: set[str] = field(default_factory=set)
    # Cycles that opened on an earlier UTC date than the one they closed
    # on. Their entry fills are counted as *that* day's volume, while all
    # of their cost lands here, so this day's denominator is short by the
    # entry side (Codex, PR #297).
    cross_day_cycles: int = 0


@dataclass
class Row:
    date: str
    arm: str
    fills: int = 0
    volume_usd: float = 0.0
    fills_without_value: int = 0
    slippage_usd: float | None = None
    cycles: int | None = None
    realized_pnl_usd: float | None = None
    funding_usd: float | None = None
    cost_usd: float | None = None
    cost_source: str | None = None
    pnl_coverage: str | None = None
    pnl_incomplete_reasons: list[str] | None = None
    points: float | None = None
    cost_per_point: float | None = None
    cost_per_musd_volume: float | None = None
    cross_day_cycles: int = 0
    slippage_unreadable: int = 0

    def as_json(self) -> dict:
        return {k: v for k, v in self.__dict__.items()}


class SubsidyLedgerError(ValueError):
    """A ledger this script cannot read honestly."""


def read_jsonl(path: Path) -> Iterable[dict]:
    """Tolerate a torn *final* line, and nothing else.

    These files are appended to by a running bot, so the last row can be
    half-written at any moment -- but only the last, and only when the
    file does not end in a newline. Interior corruption (a damaged
    concatenation, an interrupted recovery) is a different thing
    entirely: skipping it drops real fills or real closes while the
    report still presents the day as fully covered, which is the one
    outcome this KPI must never produce. Those are raised.
    """
    text = path.read_text(encoding="utf-8", errors="replace")
    lines = text.splitlines()
    tail_may_be_torn = bool(text) and not text.endswith("\n")
    for number, line in enumerate(lines, start=1):
        stripped = line.strip()
        if not stripped:
            continue
        try:
            yield json.loads(stripped)
        except json.JSONDecodeError as error:
            if number == len(lines) and tail_may_be_torn:
                return
            raise SubsidyLedgerError(
                f"{path}:{number}: malformed JSON in the middle of the ledger "
                f"({error.msg}); this is not a torn trailing write, so records "
                "after it cannot be assumed present") from error


def quantity_moved(record: dict) -> bool:
    """Did this leg_fill actually fill anything?"""
    for key in ("filled_qty", "fill_qty", "submitted_qty"):
        value = record.get(key)
        if value is None:
            continue
        try:
            return abs(float(value)) > 0
        except (TypeError, ValueError):
            return True
    return True


def load_execution(paths: Iterable[Path]) -> dict[tuple[str, str], ExecDay]:
    """Volume and execution slippage per (date, arm) from `leg_fill` rows.

    Notional is taken per filled leg. A pair trade's two legs are both real
    volume to a points program, so they are not halved the way a position
    count would be.
    """
    days: dict[tuple[str, str], ExecDay] = defaultdict(ExecDay)
    for path in paths:
        for record in read_jsonl(path):
            if record.get("event") != "leg_fill":
                continue
            ts_ms = record.get("ts_ms")
            arm = record.get("variant")
            if ts_ms is None or not arm:
                # Same rule the PnL loader already applies to a row with no
                # `ts`: a fill that cannot be attributed to a day and an arm
                # cannot be counted, and cannot be blamed on a day either,
                # so it would leave an understated denominator presented as
                # complete. Every archived leg_fill carries both fields, so
                # this is a broken writer rather than a gap.
                raise SubsidyLedgerError(
                    f"{path}: a leg_fill has no {'ts_ms' if ts_ms is None else 'variant'}, "
                    "so the day and arm it belongs to cannot be determined")
            try:
                key = (utc_date(float(ts_ms) / 1000.0), str(arm))
            except (TypeError, ValueError) as error:
                raise SubsidyLedgerError(f"{path}: unreadable `ts_ms` {ts_ms!r}") from error
            day = days[key]
            # `fill_value` is the field the bot actually writes (175/175
            # leg_fill rows in the archived ledgers carry it, and none
            # carry `notional_usd`); the other name is accepted only so a
            # differently-shaped export is not silently read as zero.
            notional = record.get("fill_value")
            if notional is None:
                notional = record.get("notional_usd")
            if notional is None:
                # Quantity moved with no value attached: the volume is
                # real but unmeasured. Ignoring it would leave the day
                # looking fully covered on an understated denominator,
                # which inflates every per-volume cost.
                if quantity_moved(record):
                    day.fills_without_value += 1
            else:
                try:
                    value = abs(float(notional))
                except (TypeError, ValueError):
                    day.fills_without_value += 1
                else:
                    # Zero notional on a fill that moved quantity is the
                    # same gap as no notional at all: the volume happened
                    # and its value is missing. Counting it as a valued
                    # fill of $0 leaves the day marked complete on a
                    # denominator that is short by that fill.
                    #
                    # `float()` also accepts "Infinity" and "NaN", which
                    # are not values either: an infinite denominator
                    # reports a real cost as $0.00 per $1M, and a NaN
                    # spreads into every total and out of `--out` as
                    # non-standard JSON. Both are the same gap.
                    if not math.isfinite(value):
                        day.fills_without_value += 1
                    elif value == 0.0 and quantity_moved(record):
                        day.fills_without_value += 1
                    else:
                        day.volume_usd += value
                        day.fills += 1
            slip = record.get("slippage_usd_vs_decision")
            if slip is not None:
                try:
                    slip_value = float(slip)
                except (TypeError, ValueError):
                    slip_value = None
                # Same finite test as the notional: a NaN here is only a
                # diagnostic, but it still leaves `--out` holding a token
                # no strict JSON reader will accept.
                if slip_value is not None and math.isfinite(slip_value):
                    day.slippage_usd += slip_value
                else:
                    day.slippage_unreadable += 1
    return dict(days)


# Sources this script knows describe a realized close of real money. Seen
# across every pnl ledger archived locally (645 rows): `exit_fill` is the
# live close; `exit_dry_run` is a simulated one and is NOT money; and
# `recovery_no_pnl` is a placeholder that carries `pnl: 0` with
# `pnl_available: false` because the result only exists in the venue's
# ledger. An unlisted source is treated like the last two rather than the
# first, because the cost of being wrong is asymmetric: an unknown source
# read as a realized close silently changes the KPI, while an unknown
# source read as incomplete only makes a day visibly uncosted.
REALIZED_PNL_SOURCES = frozenset({"exit_fill"})

# Lighter funds hourly. The writer omits `funding_carry_usd` when nothing
# accrued rather than writing a zero, and the archived ledgers agree
# exactly: of 514 `exit_fill` rows, all 101 without the field were held
# under an hour, and none of the 413 with it observed zero ticks. So an
# absent field on a hold that never met a funding timestamp is a real
# zero, while a hold that spans one with no funding coverage is a feed
# gap -- and a gap read as zero understates the subsidy cost silently.
# What decides that is the interval the position spanned, not its
# duration: a hold of ten minutes across the hour met a tick.
FUNDING_INTERVAL_SECS = 3600


def pnl_row_defect(record: dict) -> str | None:
    """Why this row cannot stand as a realized live close, or None."""
    if record.get("pnl_available") is False:
        return "pnl_available_false"
    source = record.get("source")
    if source is None:
        return "missing_source"
    if str(source) not in REALIZED_PNL_SOURCES:
        return f"source:{source}"
    if record.get("pnl") is None:
        return "missing_pnl"
    return None


def load_pnl(paths: Iterable[Path]) -> dict[tuple[str, str], PnlDay]:
    """Realized PnL and funding per (date, arm), with coverage tracked.

    The arm comes from the filename (`pnl-<service>-<arm>-<YYYYMMDD>.jsonl`)
    because the rows themselves do not carry it, and the date from each
    row's own timestamp rather than the filename, so a cycle that closes
    after a UTC rollover lands on the day it actually closed.

    Rows that are not realized live closes are not skipped -- skipping is
    what let a day of placeholders report as free, and what let a day
    holding one good close beside one placeholder report that close as if
    it were the whole day. They mark their (date, arm) incomplete, and
    `build_rows` then refuses to cost that day from this ledger at all.
    """
    days: dict[tuple[str, str], PnlDay] = defaultdict(PnlDay)
    for path in paths:
        arm = arm_from_pnl_filename(path.name)
        if arm is None:
            continue
        for record in read_jsonl(path):
            ts = record.get("ts")
            if ts is None:
                # Not attributable to any day, so it cannot be counted and
                # cannot be blamed on a day either. A row carrying a PnL
                # without a timestamp is a broken writer, not a gap.
                if record.get("pnl") is not None:
                    raise SubsidyLedgerError(
                        f"{path}: a PnL row has no `ts`, so the day it belongs to "
                        "cannot be determined")
                continue
            try:
                key = (utc_date(float(ts)), arm)
            except (TypeError, ValueError) as error:
                raise SubsidyLedgerError(f"{path}: unreadable `ts` {ts!r}") from error
            day = days[key]
            defect = pnl_row_defect(record)
            if defect is not None:
                day.incomplete = True
                day.incomplete_reasons.add(defect)
                continue
            try:
                pnl = float(record["pnl"])
            except (TypeError, ValueError):
                day.incomplete = True
                day.incomplete_reasons.add("unreadable_pnl")
                continue
            # "NaN" and "Infinity" parse. Neither is a cost: a NaN
            # spreads through every total and out of `--out` as
            # non-standard JSON, and an infinity swamps the day.
            if not math.isfinite(pnl):
                day.incomplete = True
                day.incomplete_reasons.add("unreadable_pnl")
                continue
            day.realized_pnl_usd += pnl
            day.cycles += 1
            if opened_on_an_earlier_day(record, key[0]):
                day.cross_day_cycles += 1
            funding = record.get("funding_carry_usd")
            if funding is None:
                # A positive `funding_ticks_observed` is the row's own
                # evidence that funding happened, and it beats any
                # inference from timestamps: rounded or stale `hold_secs`
                # / `ts` can make a row that met a tick look like it
                # stayed inside one hour, and the missing carry would
                # then be read as a real zero (Codex, PR #297).
                if funding_ticks_seen(record) or spans_a_funding_interval(record):
                    day.incomplete = True
                    day.incomplete_reasons.add("funding_gap")
                continue
            try:
                carry = float(funding)
            except (TypeError, ValueError):
                day.incomplete = True
                day.incomplete_reasons.add("unreadable_funding")
                continue
            if not math.isfinite(carry):
                day.incomplete = True
                day.incomplete_reasons.add("unreadable_funding")
                continue
            day.funding_usd += carry
            day.funding_seen = True
            if record.get("funding_ticks_observed") == 0 and spans_a_funding_interval(record):
                day.incomplete = True
                day.incomplete_reasons.add("funding_gap")
    return dict(days)


def opened_on_an_earlier_day(record: dict, close_date: str) -> bool:
    """Did this cycle open on a UTC date before the one it closed on?

    The two ledgers are keyed differently: `load_execution` files a fill
    under the date it happened, `load_pnl` files a whole realized cycle
    under the date it *closed*. An overnight round trip therefore leaves
    its entry notional on one day -- with no cost, so that day is
    uncosted -- while its entire cost lands on the next, divided by the
    exit side alone. With equal legs that doubles `cost_per_musd_volume`.

    The fills cannot be tied back to their cycle from what is written
    (no cycle id on a `leg_fill`), so the day is not re-attributed: it is
    marked, and its rate suppressed the way an unvalued fill already
    suppresses it. An unreadable hold is treated as a crossing, since it
    cannot be shown not to be one.
    """
    hold = record.get("hold_secs")
    ts = record.get("ts")
    if hold is None or ts is None:
        return True
    try:
        opened = float(ts) - float(hold)
    except (TypeError, ValueError):
        return True
    if not math.isfinite(opened):
        return True
    return utc_date(opened) != close_date


def funding_ticks_seen(record: dict) -> bool:
    """Does the row itself say a funding tick landed inside its life?"""
    ticks = record.get("funding_ticks_observed")
    if ticks is None:
        return False
    try:
        return float(ticks) > 0
    except (TypeError, ValueError):
        # An unreadable tick count is not evidence of zero either.
        return True


def spans_a_funding_interval(record: dict) -> bool:
    """Could a funding tick have landed inside this position's life?

    A full hour of hold is sufficient but not necessary: a position opened
    five minutes before an hourly boundary and closed five minutes after it
    was charged that tick while `hold_secs` reads 600. Testing the duration
    alone accepts such a row as a real zero, so the interval the position
    actually spans is what is tested -- `[close - hold, close]` against the
    hourly grid -- and the duration test remains as the answer when the
    close timestamp is unreadable.
    """
    hold = record.get("hold_secs")
    if hold is None:
        # Unknown hold, so the absence of funding cannot be read as a real
        # zero either.
        return True
    try:
        hold_secs = float(hold)
    except (TypeError, ValueError):
        return True
    if hold_secs >= FUNDING_INTERVAL_SECS:
        return True
    close = record.get("ts")
    if close is None:
        return True
    try:
        close_secs = float(close)
    except (TypeError, ValueError):
        return True
    if hold_secs < 0:
        return True
    opened = close_secs - hold_secs
    return (close_secs // FUNDING_INTERVAL_SECS) != (opened // FUNDING_INTERVAL_SECS)


def arm_from_pnl_filename(name: str) -> str | None:
    """`pnl-debot-pair-robinhood-lighter-freq-20260908.jsonl` -> `freq`."""
    if not name.startswith("pnl-") or not name.endswith(".jsonl"):
        return None
    stem = name[len("pnl-") : -len(".jsonl")]
    parts = stem.rsplit("-", 1)
    if len(parts) != 2 or not parts[1].isdigit():
        return None
    service_and_arm = parts[0]
    arm = service_and_arm.rsplit("-", 1)[-1]
    return arm or None


def equity_daily_costs(rows: Iterable[dict]) -> dict[str, float]:
    """Cost per UTC date from an equity_history series.

    The cost of a day is the fall in equity across it: last point of the
    previous day to last point of this one. The first day of the series
    has no previous close to measure against and is skipped rather than
    measured from its own first sample, which would understate whatever
    happened before the series started.

    A gap in the series is skipped for the same reason. If the last close
    before the 7th is the 5th's, the change across those two days is not
    the 7th's cost, and charging it to the 7th alone -- then dividing by
    only the 7th's volume -- reports a day that never happened. The 7th
    is left uncosted, which `summarize` already reports as uncovered
    volume.

    A row whose `equity` cannot be read is the same kind of gap rather
    than a row to step over. Skipping it leaves the last *readable*
    sample standing as that day's close, which is not the day's close if
    the unreadable row came later -- so the day is invalidated, and so is
    the following day, whose cost is measured from that same baseline. A
    row with no readable `ts` cannot be blamed on a day at all and is a
    broken writer, so it raises, as an unattributable row does in the
    other two loaders.
    """
    last_by_day: dict[str, float] = {}
    invalid_days: set[str] = set()
    for row in rows:
        ts = row.get("ts")
        if ts is None:
            raise SubsidyLedgerError(
                "an equity_history row has no `ts`, so the day it belongs to "
                f"cannot be determined: {row!r}")
        try:
            # equity_history stamps milliseconds.
            day = utc_date(float(ts) / 1000.0)
        except (TypeError, ValueError) as error:
            raise SubsidyLedgerError(
                f"unreadable `ts` {ts!r} in an equity_history row") from error
        equity = row.get("equity")
        if equity is None:
            invalid_days.add(day)
            continue
        try:
            value = float(equity)
        except (TypeError, ValueError):
            invalid_days.add(day)
        else:
            # "NaN"/"Infinity" parse but are not a close: the next
            # consecutive-day delta would be non-finite and accepted as a
            # known `equity_delta` cost.
            if math.isfinite(value):
                last_by_day[day] = value
            else:
                invalid_days.add(day)
    costs: dict[str, float] = {}
    previous_day: str | None = None
    for day in sorted(last_by_day):
        if (previous_day is not None and is_next_calendar_day(previous_day, day)
                and day not in invalid_days and previous_day not in invalid_days):
            costs[day] = -(last_by_day[day] - last_by_day[previous_day])
        previous_day = day
    return costs


def is_next_calendar_day(earlier: str, later: str) -> bool:
    fmt = "%Y-%m-%d"
    delta = datetime.strptime(later, fmt) - datetime.strptime(earlier, fmt)
    return delta.days == 1


def load_points(path: Path | None) -> dict[tuple[str, str], float]:
    """Operator-supplied points, one JSON object per line:
    `{"date": "2026-09-08", "arm": "freq", "points": 1234.5}`."""
    if path is None:
        return {}
    points: dict[tuple[str, str], float] = {}
    for record in read_jsonl(path):
        date, arm, value = record.get("date"), record.get("arm"), record.get("points")
        if not date or not arm or value is None:
            # This file is written by hand for exactly this report. A line
            # that cannot be attributed is an operator slip worth seeing,
            # not points to drop quietly -- dropping them moves the price
            # per point without saying so.
            raise SubsidyLedgerError(
                f"{path}: a points row needs date, arm and points; got {record!r}")
        try:
            parsed = float(value)
        except (TypeError, ValueError) as error:
            raise SubsidyLedgerError(
                f"{path}: unreadable points value {value!r} for {date}/{arm}") from error
        # A negative or non-finite count is a typo in a hand-written
        # file, and a silent one: the row's cost is excluded from
        # `cost_per_point` (the numerator requires points > 0) while its
        # points still moved the denominator, so $100/1000 beside
        # $100/-500 reported $0.20 per point -- a number about neither
        # day. Zero stays legal: it means "no points that day"
        # (Codex, PR #297).
        if not math.isfinite(parsed) or parsed < 0:
            raise SubsidyLedgerError(
                f"{path}: points must be a finite, non-negative number; "
                f"got {value!r} for {date}/{arm}")
        points[(str(date), str(arm))] = parsed
    return points


def build_rows(
    execution: dict[tuple[str, str], ExecDay],
    pnl: dict[tuple[str, str], PnlDay],
    equity_costs: dict[str, dict[str, float]] | None = None,
    points: dict[tuple[str, str], float] | None = None,
) -> list[Row]:
    equity_costs = equity_costs or {}
    points = points or {}
    # The four inputs are selected independently, so a date can exist in
    # any one of them alone. Unioning only the two ledgers dropped an
    # equity-only cost outright, and hid a points-only day from
    # `uncosted_points` -- both of which the row-alignment rule is
    # supposed to report rather than discard.
    keys = set(execution) | set(pnl) | set(points)
    keys |= {(date, arm) for arm, days in equity_costs.items() for date in days}
    rows: list[Row] = []
    for date, arm in sorted(keys):
        row = Row(date=date, arm=arm)
        if (date, arm) in execution:
            day = execution[(date, arm)]
            row.fills = day.fills
            row.volume_usd = round(day.volume_usd, 6)
            row.fills_without_value = day.fills_without_value
            row.slippage_usd = round(day.slippage_usd, 6)
            row.slippage_unreadable = day.slippage_unreadable
        day = pnl.get((date, arm))
        if day is not None:
            row.cycles = day.cycles
            row.realized_pnl_usd = round(day.realized_pnl_usd, 6)
            row.funding_usd = round(day.funding_usd, 6) if day.funding_seen else None
            row.pnl_coverage = "incomplete" if day.incomplete else "complete"
            row.cross_day_cycles = day.cross_day_cycles
            if day.incomplete:
                row.pnl_incomplete_reasons = sorted(day.incomplete_reasons)
        if day is not None and not day.incomplete:
            row.cost_usd = round(-(day.realized_pnl_usd + day.funding_usd), 6)
            row.cost_source = "pnl_ledger"
        elif date in equity_costs.get(arm, {}):
            row.cost_usd = round(equity_costs[arm][date], 6)
            row.cost_source = "equity_delta"
        row.points = points.get((date, arm))
        # A cross-midnight cycle short-changes *both* denominators: the
        # entry day carries its own points (and its volume) while all of
        # the cost lands here, so the price per point is overstated in
        # exactly the same way as the price per $1M. Points cannot be
        # re-attributed either -- they are supplied per day by an
        # operator, with no cycle to key them to (Codex, PR #297).
        if (row.points is not None and row.points > 0 and row.cost_usd is not None
                and not row.cross_day_cycles):
            row.cost_per_point = round(row.cost_usd / row.points, 8)
        # Same rule as the aggregate, and it has to live here too: this row
        # is what `--out` writes and what the daily table prints, so a rate
        # suppressed only in the totals would still be published per day.
        # A cycle that opened yesterday leaves its entry notional on
        # yesterday's row while all of its cost lands here, so this
        # denominator is short by the entry side -- the same kind of
        # known-short denominator as an unvalued fill, and suppressed the
        # same way (Codex, PR #297).
        if (row.cost_usd is not None and row.volume_usd > 0
                and not row.fills_without_value and not row.cross_day_cycles):
            row.cost_per_musd_volume = round(row.cost_usd / (row.volume_usd / 1e6), 4)
        rows.append(row)
    return rows


def summarize(rows: list[Row]) -> dict:
    """Totals per arm, plus how much volume has no cost source at all.

    `uncosted_volume_usd` is the honest caveat on every ratio below it: a
    day the cost is unknown for still traded, so the totals describe less
    than the whole program.

    Every ratio here obeys one rule: **it is computed over exactly the
    rows that carry all of its inputs.** A cost is admitted to a ratio's
    numerator only if that same row also supplied the denominator. The
    two inputs are independently selectable (`--exec-glob`, `--pnl-glob`,
    `--points`) and cover different day ranges on the live host, so
    without that rule a day costed but not measured -- or measured but
    with no points supplied -- lands its whole cost on some other day's
    denominator. That does not make the ratio slightly wrong; it makes it
    a number about no real period. Costs excluded this way are reported
    rather than dropped, as `cost_usd_without_volume` and
    `cost_usd_without_points`.
    """
    by_arm: dict[str, dict] = {}
    for row in rows:
        arm = by_arm.setdefault(
            row.arm,
            {
                "arm": row.arm,
                "days": 0,
                "fills": 0,
                "volume_usd": 0.0,
                "costed_volume_usd": 0.0,
                "uncosted_volume_usd": 0.0,
                "cost_usd": 0.0,
                "cost_usd_on_measured_volume": 0.0,
                "cost_usd_without_volume": 0.0,
                "cost_usd_on_pointed_days": 0.0,
                "cost_usd_without_points": 0.0,
                "points": 0.0,
                "uncosted_points": 0.0,
                "points_seen": False,
                "cost_days": 0,
                "fills_without_value": 0,
                "incomplete_volume_days": 0,
                "cross_day_cycles": 0,
            },
        )
        arm["days"] += 1
        arm["fills"] += row.fills
        arm["volume_usd"] += row.volume_usd
        arm["fills_without_value"] += row.fills_without_value or 0
        arm["cross_day_cycles"] += row.cross_day_cycles
        if row.fills_without_value or row.cross_day_cycles:
            arm["incomplete_volume_days"] += 1
        if row.cost_usd is None:
            arm["uncosted_volume_usd"] += row.volume_usd
        else:
            arm["cost_usd"] += row.cost_usd
            arm["cost_days"] += 1
            # Volume ratio: this cost counts only if this row measured the
            # volume it was spent on.
            if (row.volume_usd > 0 and not row.fills_without_value
                    and not row.cross_day_cycles):
                arm["cost_usd_on_measured_volume"] += row.cost_usd
                arm["costed_volume_usd"] += row.volume_usd
            else:
                arm["cost_usd_without_volume"] += row.cost_usd
            # Points ratio: likewise, only if this row supplied points
            # *and* its cost belongs to the day those points were earned.
            if (row.points is not None and row.points > 0
                    and not row.cross_day_cycles):
                arm["cost_usd_on_pointed_days"] += row.cost_usd
            else:
                arm["cost_usd_without_points"] += row.cost_usd
        if row.points is not None:
            arm["points_seen"] = True
            if row.cost_usd is None or row.cross_day_cycles:
                # Points earned on a day whose cost is unknown, or one
                # whose cost belongs partly to a cycle that opened
                # yesterday. Counting them would divide a numerator and a
                # denominator drawn from different days (Codex, PR #297).
                arm["uncosted_points"] += row.points
            else:
                arm["points"] += row.points
    for arm in by_arm.values():
        for key in ("volume_usd", "costed_volume_usd", "uncosted_volume_usd", "cost_usd",
                    "cost_usd_on_measured_volume", "cost_usd_without_volume",
                    "cost_usd_on_pointed_days", "cost_usd_without_points"):
            arm[key] = round(arm[key], 6)
        arm["cost_per_musd_volume"] = (
            round(arm["cost_usd_on_measured_volume"] / (arm["costed_volume_usd"] / 1e6), 4)
            if arm["costed_volume_usd"] > 0
            else None
        )
        arm["cost_per_point"] = (
            round(arm["cost_usd_on_pointed_days"] / arm["points"], 8)
            if arm["points_seen"] and arm["points"] > 0
            else None
        )
        if not arm["points_seen"]:
            arm["points"] = None
            arm["uncosted_points"] = None
            arm["cost_usd_without_points"] = None
        else:
            arm["uncosted_points"] = round(arm["uncosted_points"], 6)
        del arm["points_seen"]
    return {"arms": [by_arm[a] for a in sorted(by_arm)]}


def render_table(rows: list[Row], summary: dict) -> str:
    out = [
        f"{'date':11s} {'arm':6s} {'fills':>5s} {'volume_usd':>12s} "
        f"{'cost_usd':>9s} {'src':>12s} {'$/M vol':>9s} {'points':>9s}"
    ]
    for row in rows:
        out.append(
            f"{row.date:11s} {row.arm:6s} {row.fills:5d} {row.volume_usd:12.2f} "
            f"{'-' if row.cost_usd is None else format(row.cost_usd, '9.2f'):>9s} "
            f"{row.cost_source or '-':>12s} "
            f"{'-' if row.cost_per_musd_volume is None else format(row.cost_per_musd_volume, '9.2f'):>9s} "
            f"{'-' if row.points is None else format(row.points, '9.1f'):>9s}"
        )
    out.append("")
    out.append("Totals (cost is positive when the arm gave money up):")
    for arm in summary["arms"]:
        head = f"  {arm['arm']:6s} volume ${arm['volume_usd']:,.0f} over {arm['days']}d"
        if arm["cost_days"] == 0:
            # Nothing priced this arm at all. Saying "cost $0.00" here would
            # read as free rather than as unmeasured.
            out.append(
                f"{head}, cost unknown (no PnL ledger or equity series covers these days)")
            continue
        # The cost is known even when no rate can be built from it, so it is
        # stated on its own line rather than folded into an equation. Every
        # equation below prints the numerator it actually divided -- an
        # equation that shows a larger total beside a rate computed from a
        # subset is simply false, and a caveat further down does not repair
        # it.
        out.append(
            f"{head}, cost ${arm['cost_usd']:,.2f} across "
            f"{arm['cost_days']} costed day(s)")
        if arm["cost_per_musd_volume"] is None:
            out.append(
                "         no costed day has fully measured volume, so the per-$1M rate "
                "is unavailable"
            )
        else:
            out.append(
                f"         ${arm['cost_usd_on_measured_volume']:,.2f} of it fell on "
                f"${arm['costed_volume_usd']:,.0f} of measured volume"
                f" = ${arm['cost_per_musd_volume']:,.2f} per $1M traded"
            )
        if arm["uncosted_volume_usd"] > 0:
            out.append(
                f"         ${arm['uncosted_volume_usd']:,.0f} of that volume has no cost "
                f"source and is excluded above"
            )
        if arm["cost_usd_without_volume"]:
            out.append(
                f"         ${arm['cost_usd_without_volume']:,.2f} of cost fell on days whose "
                f"volume is unmeasured and is excluded from that rate"
            )
        if arm["fills_without_value"]:
            out.append(
                f"         {arm['fills_without_value']} fill(s) across "
                f"{arm['incomplete_volume_days']} day(s) reported no value, so those days' "
                f"volume is a lower bound"
            )
        if arm["cost_per_point"] is not None:
            out.append(
                f"         ${arm['cost_usd_on_pointed_days']:,.2f} of it over "
                f"{arm['points']:,.1f} points = ${arm['cost_per_point']:.6f} per point"
            )
            if arm["uncosted_points"]:
                out.append(
                    f"         {arm['uncosted_points']:,.1f} points earned on uncosted days "
                    f"are excluded from that price"
                )
            if arm["cost_usd_without_points"]:
                out.append(
                    f"         ${arm['cost_usd_without_points']:,.2f} of cost fell on days with "
                    f"no points supplied and is excluded from that price"
                )
    return "\n".join(out)


def expand(patterns: list[str]) -> list[Path]:
    """Every file the patterns name, each exactly once.

    `--exec-glob` and `--pnl-glob` accumulate, so a broad history pattern
    beside a single-day one is a natural way to call this -- and without
    deduplication the overlap is read twice, counting its volume, PnL and
    funding twice with nothing to show that it happened. Resolved paths are
    compared, so two patterns reaching the same file by different spellings
    still collapse to one.
    """
    seen: set[Path] = set()
    paths: list[Path] = []
    for pattern in patterns:
        for match in sorted(glob.glob(pattern)):
            resolved = Path(match).resolve()
            if resolved in seen:
                continue
            seen.add(resolved)
            paths.append(Path(match))
    return paths


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--exec-glob", action="append", default=[], required=True)
    parser.add_argument("--pnl-glob", action="append", default=[])
    parser.add_argument(
        "--equity",
        action="append",
        default=[],
        metavar="ARM=PATH",
        help="equity_history.jsonl for an arm, used only for days the PnL ledger misses",
    )
    parser.add_argument("--points", type=Path, default=None)
    parser.add_argument("--out", type=Path, default=None, help="write the rows as JSONL")
    args = parser.parse_args(argv)

    execution = load_execution(expand(args.exec_glob))
    pnl = load_pnl(expand(args.pnl_glob))
    equity_costs: dict[str, dict[str, float]] = {}
    for spec in args.equity:
        arm, _, path = spec.partition("=")
        if not path:
            parser.error(f"--equity expects ARM=PATH, got {spec!r}")
        equity_costs[arm] = equity_daily_costs(read_jsonl(Path(path)))

    rows = build_rows(execution, pnl, equity_costs, load_points(args.points))
    summary = summarize(rows)
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        with args.out.open("w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row.as_json(), sort_keys=True) + "\n")
    print(render_table(rows, summary))
    return 0


if __name__ == "__main__":
    sys.exit(main())
