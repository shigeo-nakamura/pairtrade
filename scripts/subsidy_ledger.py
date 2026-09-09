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
today; both days are marked instead -- the close day (`cross_day_cycles`)
and the day the entry landed on (`cross_day_entries`), whose own volume
holds that leg while none of the cost does -- and their rates are
suppressed, the same treatment an unvalued fill gets.

Those two are the cases a *realized close* can reveal: its own hold names
the day it opened on. An entry with no realized close in the report at
all -- still open when the window ends, or closed in a PnL file the
caller did not pass -- names nothing, and the PnL rows carry no size or
notional (checked against the archived ledgers: `ts`, prices, `pnl`,
`hold_secs`, funding), so there is no way to tell that a day's fills
include such a leg. Its notional therefore sits in the denominator with
no cost beside it, and the rate for that day reads cheaper than the truth
by however much of it was unpaired.

All three are one missing link: a `leg_fill` cannot be tied to the cycle
it belongs to. Closing it needs the bot to stamp a cycle id on both
ledgers -- at which point the volume denominator can be built from the
cycles that actually closed, and none of these three cases exists.
Estimating it instead (pairing by symbol and time, or assuming two legs
per cycle) would put a constructed number where a measured one is
supposed to be, which is the failure this KPI exists to avoid.

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
import codecs
import glob
import json
import math
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


def add_or_none(total: float | None, addend: float | None) -> float | None:
    """Running total for an aggregate, or `None` once it stops being one.

    The per-day loaders already refuse a value, or a sum, that leaves the
    reals. A roll-up across days is a *second* level of accumulation and
    can overflow where every day was finite, after which the printed
    rates are `NaN` or a misleading zero. `None` is sticky: once a total
    is unknown, nothing later makes it known again (Codex, PR #297).
    """
    if total is None or addend is None:
        return None
    combined = total + addend
    return combined if math.isfinite(combined) else None


def finite_or_none(value: float, places: int) -> float | None:
    """A derived number, or `None` when the arithmetic left the reals.

    Every *input* is guarded, but a value derived from two guarded inputs
    can still overflow -- `1e308 + 1e308`, or the difference between two
    equity closes of opposite sign. `None` is the module's existing word
    for "not known", so an unrepresentable result reports as unknown
    rather than as `Infinity` in `--out` (Codex, PR #297).
    """
    return round(value, places) if math.isfinite(value) else None


def overflows(total: float, addend: float) -> bool:
    """Would adding this to a finite running total make it non-finite?

    Every individual value is checked for finiteness before it is added,
    for a stated reason: a non-finite number spreads through the totals
    and reaches `--out` as the non-standard JSON token `Infinity`, and an
    infinite denominator reports a real cost as `$0.00` per $1M. A *sum*
    of finite values can overflow to the same thing, so the guarantee
    only holds if the running total is checked too (Codex, PR #297).
    """
    return not math.isfinite(total + addend)


def is_bare_arm(value: object) -> bool:
    """Is this exactly an arm name the other loaders could have produced?

    The arm is half of the `(date, arm)` join key, and the two machine
    sources -- `arm_from_pnl_filename` and the execution ledger's
    `variant` -- cannot emit a padded, empty or non-string value. So an
    operator-supplied arm that is any of those can only ever be a key
    matching nothing, and it fails silently: the intended arm is left
    uncosted while a separate zero-volume arm appears beside it
    (Codex, PR #297).

    Deliberately *not* a check that the arm exists elsewhere: points and
    equity may legitimately be supplied for an arm whose ledger has not
    been exported yet, which `uncosted_points` reports.

    `arm_from_pnl_filename` uses it too. A filename is operator-supplied,
    so the only genuinely machine-written arm is the execution ledger's
    `variant` (Codex, PR #297).
    """
    if not isinstance(value, str) or not value:
        return False
    # `value == value.strip()` was the wrong rule twice over: `strip()`
    # only looks at the ends, so `"fr\neq"` passed and then broke the
    # fixed-width table as well as the join, and it does not remove
    # zero-width format characters, so a copied `"freq\u200b"` stayed a
    # distinct key while the real arm got nothing. The rule is what an
    # arm name actually is -- printable, and no whitespace anywhere
    # (Codex, PR #297).
    return value.isprintable() and not any(char.isspace() for char in value)


def is_canonical_date(value: object) -> bool:
    """Is this exactly a date `utc_date` could have produced?

    A round-trip rather than a shape test, because the two failures are
    different and both matter. `strptime` rejects an impossible date --
    `2026-02-31` matches `\\d{4}-\\d{2}-\\d{2}` but is not a day, and
    `utc_date` can never emit it (Codex, PR #297). Re-formatting then
    rejects a real date spelled differently -- `2026-9-08` parses, but
    is not the key the other loaders build.

    It matters because this is a *join key*: a value that misses either
    way silently becomes a separate points-only row, leaving the day the
    operator meant to price without points and its rate unproducible.
    """
    if not isinstance(value, str):
        return False
    try:
        parsed = datetime.strptime(value, "%Y-%m-%d")
    except ValueError:
        return False
    return parsed.strftime("%Y-%m-%d") == value


def utc_date(ts_seconds: float) -> str:
    return datetime.fromtimestamp(ts_seconds, timezone.utc).strftime("%Y-%m-%d")


def utc_date_or_none(ts_seconds: float) -> str | None:
    """`utc_date`, or `None` when the epoch is outside what it can render.

    `float()` accepts numbers `datetime.fromtimestamp` cannot: a finite
    but enormous value raises `OverflowError` or `OSError` depending on
    the platform. Every caller here has to handle that -- the loaders by
    refusing the row, `opening_date` by taking its documented fail-safe --
    so the conversion is expressed once, and a new call site cannot
    forget it (Codex, PR #297).
    """
    try:
        return utc_date(ts_seconds)
    except (OverflowError, OSError, ValueError):
        return None


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
    # At least one close carried a readable `funding_carry_usd`. A
    # diagnostic only: it deliberately does not gate `funding_usd`'s
    # serialization, because it goes true on the *first* good carry and
    # would publish a partial sum for a day whose coverage is incomplete
    # (Codex, PR #297).
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
    # Cycles whose *entry* fell on this date but which closed on a later
    # one. Their entry notional is in this day's execution volume while
    # every dollar of their cost is filed under the close date, so this
    # day's denominator is inflated by exactly that leg -- the mirror of
    # `cross_day_cycles`, and just as unusable as a rate (Codex, PR #297).
    cross_day_entries: int = 0


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
    cross_day_entries: int = 0
    slippage_unreadable: int = 0

    def cost_spans_two_days(self) -> bool:
        """Is this row's cost partly earned on a day it is not filed under?

        Only when it came from the PnL ledger: that files a whole realized
        cycle under its close date. An `equity_delta` is measured between
        two daily closes, so it is already aligned with this date's own
        volume and points and must not be suppressed (Codex, PR #297).
        """
        return (
            bool(self.cross_day_cycles or self.cross_day_entries)
            and self.cost_source == "pnl_ledger"
        )

    def as_json(self) -> dict:
        return {k: v for k, v in self.__dict__.items()}


class SubsidyLedgerError(ValueError):
    """A ledger this script cannot read honestly."""


def read_jsonl(path: Path, tolerate_torn_tail: bool = True) -> Iterable[dict]:
    """Tolerate a torn *final* line, and nothing else.

    These files are appended to by a running bot, so the last row can be
    half-written at any moment -- but only the last, and only when the
    file does not end in a newline. Interior corruption (a damaged
    concatenation, an interrupted recovery) is a different thing
    entirely: skipping it drops real fills or real closes while the
    report still presents the day as fully covered, which is the one
    outcome this KPI must never produce. Those are raised.

    `tolerate_torn_tail=False` for a file no bot appends to. The points
    export is written by hand for this report, so a truncated last line
    is a damaged file, not a race, and letting it through drops that
    day's points silently (Codex, PR #297).

    Decoding is strict. `errors="replace"` turned invalid UTF-8 inside an
    otherwise complete record into U+FFFD and handed on JSON that still
    parses -- corruption inside an execution `variant` becomes a new arm
    key, splitting its volume from its costs, with a 0 exit. A torn tail
    can be an incomplete multi-byte sequence, so that one case is
    retried leniently and only the last line is kept from it
    (Codex, PR #297).
    """
    raw = path.read_bytes()
    # An incremental decoder separates the two failures exactly, which
    # neither of the earlier attempts did: replacement-decoding the final
    # line let a bad byte through as a real record (round 44), and
    # dropping any final line that fails to decode silently discarded a
    # *complete* record that merely lacked a trailing newline (round 45).
    # `final=False` raises only on a genuinely invalid byte; a multibyte
    # sequence truncated at EOF is buffered and raises only on the
    # closing `final=True` (Codex, PR #297).
    decoder = codecs.getincrementaldecoder("utf-8")()
    try:
        text = decoder.decode(raw, final=False)
    except UnicodeDecodeError as error:
        raise SubsidyLedgerError(
            f"{path}: invalid UTF-8 at byte {error.start}; this is not an incomplete "
            "character at the end of the file, so it is not a torn trailing write, "
            "and a substituted character can parse as valid JSON and silently become "
            "a different key") from error
    try:
        text += decoder.decode(b"", final=True)
    except UnicodeDecodeError as error:
        # A character cut in half by the write that is still in flight.
        # The bytes are simply absent; the truncated line then fails to
        # parse and the torn-tail path drops it.
        if not tolerate_torn_tail:
            raise SubsidyLedgerError(
                f"{path}: the file ends mid-character; nothing appends to this file, "
                "so that is a damaged export rather than a write in progress"
            ) from error
    lines = text.splitlines()
    tail_may_be_torn = tolerate_torn_tail and bool(text) and not text.endswith("\n")
    for number, line in enumerate(lines, start=1):
        stripped = line.strip()
        if not stripped:
            continue
        try:
            yield json.loads(stripped, object_pairs_hook=_no_duplicate_keys)
        except SubsidyLedgerError as error:
            raise SubsidyLedgerError(f"{path}:{number}: {error}") from error
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
        if is_not_a_number(value):
            # Including a boolean: read as `0.0` it would claim the fill
            # moved nothing, quietly excusing a missing notional
            # (Codex, PR #297).
            return True
        quantity = abs(float(value))
        # A non-finite quantity is not "no movement": read as zero it
        # leaves an unvalued fill counted as nothing at all, so the day's
        # denominator looks complete while this fill's volume is unknown
        # (Codex, PR #297).
        if not math.isfinite(quantity):
            return True
        return quantity > 0
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
            # The last unvalidated arm source. Round 33 called this "the
            # only genuinely machine-written arm" and left it alone; it
            # is still a field read out of a file, and `"freq "` here
            # creates an execution-only arm whose volume reports as
            # uncosted while the real cost reports as having no volume
            # (Codex, PR #297).
            if arm is not None and not is_bare_arm(arm):
                raise SubsidyLedgerError(
                    f"{path}: a leg_fill needs a bare `variant` arm name; got {arm!r}")
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
            day_key = (
                None if is_not_a_number(ts_ms) else utc_date_or_none(float(ts_ms) / 1000.0)
            )
            if day_key is None:
                raise SubsidyLedgerError(f"{path}: unreadable `ts_ms` {ts_ms!r}")
            key = (day_key, str(arm))
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
                if is_not_a_number(notional):
                    day.fills_without_value += 1
                else:
                    value = abs(float(notional))
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
                    elif overflows(day.volume_usd, value):
                        # Real volume whose value cannot be carried in the
                        # running total: the same gap as an unvalued fill,
                        # and the denominator is a lower bound either way.
                        day.fills_without_value += 1
                    else:
                        day.volume_usd += value
                        day.fills += 1
            # `in`, not a `None` test: the writer omits this diagnostic
            # when it has none, so a present `null` is a value that could
            # not be read. Treating the two alike published
            # `slippage_usd: 0` with no coverage warning
            # (Codex, PR #297).
            if "slippage_usd_vs_decision" in record:
                slip = record["slippage_usd_vs_decision"]
                slip_value = None if is_not_a_number(slip) else float(slip)
                # Same finite test as the notional: a NaN here is only a
                # diagnostic, but it still leaves `--out` holding a token
                # no strict JSON reader will accept.
                if slip_value is None or not math.isfinite(slip_value):
                    # Present but unusable -- including an explicit
                    # `null`. The column is a diagnostic, so this
                    # qualifies the column rather than the day.
                    day.slippage_unreadable += 1
                elif overflows(day.slippage_usd, slip_value):
                    day.slippage_unreadable += 1
                else:
                    day.slippage_usd += slip_value
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
    # `is False` caught only the one shape a correct writer emits. The
    # field is a *claim that the PnL is real*, so anything present that
    # is not exactly `true` fails to establish it -- `0`, `null`,
    # `"false"`, `"true"` as a string. Absent is different and stays
    # fine: most rows do not carry the field at all (Codex, PR #297).
    if "pnl_available" in record and record["pnl_available"] is not True:
        return "pnl_available_not_true"
    source = record.get("source")
    if source is None:
        return "missing_source"
    if str(source) not in REALIZED_PNL_SOURCES:
        return f"source:{source}"
    if record.get("pnl") is None:
        return "missing_pnl"
    return None


def load_pnl(paths: Iterable[Path],
             service: str | None = None) -> dict[tuple[str, str], PnlDay]:
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
        arm = arm_from_pnl_filename(path.name, service)
        if arm is None:
            # The arm comes from the basename because the rows do not
            # carry it. Skipping such a file dropped every realized cost
            # in it and still exited 0 -- the report then says no PnL
            # source covers those days, which is a different claim from
            # "one was supplied and could not be read" (Codex, PR #297).
            detail = ""
            if service is not None:
                detail = f"; --pnl-service {service!r} does not prefix this name"
            elif "-" in (_pnl_service_and_arm(path.name) or ""):
                detail = (
                    "; if this arm's name contains a hyphen, pass --pnl-service to say "
                    "where the service name ends -- the filename alone cannot"
                )
            raise SubsidyLedgerError(
                f"{path}: the arm cannot be read from this filename; a PnL export must be "
                "named pnl-<service>-<arm>-<YYYYMMDD>.jsonl, since the rows do not carry "
                f"the arm themselves{detail}")
        for record in read_jsonl(path):
            ts = record.get("ts")
            if ts is None:
                # Not attributable to any day, so it cannot be counted and
                # cannot be blamed on a day either -- which also means the
                # days it might belong to cannot be shown to be complete.
                # Every archived row carries `ts`, so this is a broken
                # writer whatever else the row holds, and it is fatal
                # regardless of whether a `pnl` came with it
                # (Codex, PR #297).
                raise SubsidyLedgerError(
                    f"{path}: a PnL row has no `ts`, so the day it belongs to "
                    f"cannot be determined: {record!r}")
            # Before the conversion, not after: `float(False)` is `0.0`,
            # so a boolean `ts` attributed the row to 1970-01-01 and
            # published a complete realized cost there instead of taking
            # the fatal unreadable-timestamp path (Codex, PR #297).
            day_key = None if is_not_a_number(ts) else utc_date_or_none(float(ts))
            if day_key is None:
                raise SubsidyLedgerError(f"{path}: unreadable `ts` {ts!r}")
            key = (day_key, arm)
            day = days[key]
            defect = pnl_row_defect(record)
            # Derived *before* any rejection below: the entry leg of an
            # overnight cycle sits in the execution ledger whatever this
            # row's PnL turned out to be, so the opening day's denominator
            # is contaminated even when this close cannot be costed. A
            # simulated close is the one kind that moved no real quantity
            # (Codex, PR #297).
            if str(record.get("source")) != "exit_dry_run":
                opened_on = opening_date(record)
                if opened_on is None:
                    # Whether it crossed midnight is unknowable, so the
                    # fail-safe is to treat this day's denominator as
                    # spanning two -- the same direction every other
                    # unreadable input takes.
                    day.cross_day_cycles += 1
                elif opened_on != key[0]:
                    day.cross_day_cycles += 1
                    days[(opened_on, arm)].cross_day_entries += 1
            if defect is not None:
                day.incomplete = True
                day.incomplete_reasons.add(defect)
                continue
            # `is_not_a_number` first: `float(False)` is `0.0`, so
            # `"pnl": false` was counted as a cycle with a verified zero
            # cost and left the day complete (Codex, PR #297).
            if is_not_a_number(record.get("pnl")):
                day.incomplete = True
                day.incomplete_reasons.add("unreadable_pnl")
                continue
            pnl = float(record["pnl"])
            # "NaN" and "Infinity" parse. Neither is a cost: a NaN
            # spreads through every total and out of `--out` as
            # non-standard JSON, and an infinity swamps the day.
            if not math.isfinite(pnl):
                day.incomplete = True
                day.incomplete_reasons.add("unreadable_pnl")
                continue
            if overflows(day.realized_pnl_usd, pnl):
                day.incomplete = True
                day.incomplete_reasons.add("unreadable_pnl")
                continue
            day.realized_pnl_usd += pnl
            day.cycles += 1
            # `in`, not `.get()`, for the same reason as the tick count:
            # the documented writer omits this field for a genuine zero,
            # so a present `null` is a malformed claim, not that. Round 34
            # asserted a present null "still takes the conservative
            # branch" and tested it on a boundary-spanning row -- which
            # is the one shape where that happens to be true. On a short
            # same-hour hold with no tick claim the missing-carry branch
            # marks no gap, and the day published `funding_usd: 0` as a
            # verified zero (Codex, PR #297).
            if "funding_carry_usd" in record and record["funding_carry_usd"] is None:
                day.incomplete = True
                day.incomplete_reasons.add("unreadable_funding")
                continue
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
            # Same trap on the funding side: a boolean carry is not a
            # zero carry (Codex, PR #297).
            if is_not_a_number(funding):
                day.incomplete = True
                day.incomplete_reasons.add("unreadable_funding")
                continue
            carry = float(funding)
            if not math.isfinite(carry):
                day.incomplete = True
                day.incomplete_reasons.add("unreadable_funding")
                continue
            if overflows(day.funding_usd, carry):
                day.incomplete = True
                day.incomplete_reasons.add("unreadable_funding")
                continue
            day.funding_usd += carry
            day.funding_seen = True
            # A supplied carry bypasses `funding_ticks_seen` entirely, so
            # this is the only place a malformed count is looked at on
            # this path. Without it a row spanning a boundary with
            # `funding_carry_usd: 0` and an impossible count (-1, NaN,
            # false) stayed complete, while the same row with a readable
            # `0` correctly produced a gap (Codex, PR #297).
            claim = funding_tick_claim(record)
            if claim == FUNDING_TICKS_MALFORMED:
                day.incomplete = True
                day.incomplete_reasons.add("unreadable_funding")
            elif claim == FUNDING_TICKS_NONE and spans_a_funding_interval(record):
                day.incomplete = True
                day.incomplete_reasons.add("funding_gap")
    return dict(days)


def opening_date(record: dict) -> str | None:
    """The UTC date this cycle opened on, or `None` when it cannot be read.

    `None` is not "it did not cross": the caller treats an unknown
    opening date as a crossing, because a row whose own opening date is
    unreadable cannot be shown to be aligned with its day (Codex,
    PR #297).

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
        return None
    # A boolean is a valid `float()` and would read as a 0s or 1s hold,
    # marking the cycle same-day on a row whose real opening date is
    # unknowable (Codex, PR #297).
    if is_not_a_number(hold) or is_not_a_number(ts):
        return None
    hold_secs, close_secs = float(hold), float(ts)
    # A negative hold puts the open *after* the close, which usually
    # lands on the same date and would clear the marker on a row whose
    # real opening date is unknowable -- the same fail-safe
    # `spans_a_funding_interval` already applies (Codex, PR #297).
    if not math.isfinite(hold_secs) or not math.isfinite(close_secs) or hold_secs < 0:
        return None
    opened = close_secs - hold_secs
    if not math.isfinite(opened):
        return None
    # Finite is still not renderable: a normal close with `hold_secs:
    # 1e300` derives an epoch `datetime.fromtimestamp` refuses, and this
    # call sits outside the loaders' handlers -- so it came out of the
    # CLI as a traceback instead of this function's documented fail-safe
    # (Codex, PR #297).
    return utc_date_or_none(opened)


# What a row's `funding_ticks_observed` field claims, classified once.
# Three call sites used to read the field with three different ad-hoc
# parses, and every round of review found another value one of them
# disagreed about -- `"0"`, `False`, `-1`, `NaN`. There is one parse now,
# and the callers differ only in what they do with the answer
# (Codex, PR #297).
FUNDING_TICKS_ABSENT = "absent"
FUNDING_TICKS_NONE = "none"
FUNDING_TICKS_SOME = "some"
FUNDING_TICKS_MALFORMED = "malformed"


def funding_tick_claim(record: dict) -> str:
    """`absent` / `none` / `some` / `malformed`.

    `malformed` is anything present that cannot be a count: a boolean, an
    unparseable token, a non-finite number, or a negative one. It is
    deliberately distinct from `none`, because a row that cannot say how
    many ticks it saw is not a row saying it saw none.
    """
    # `in`, not `.get()`: an explicit `null` is a *present* value that is
    # not a count, and collapsing it into `absent` is the one direction
    # that loses safety here. For every other field a missing value takes
    # the conservative branch anyway -- a missing `funding_carry_usd`
    # runs the tick/interval fail-safe, a missing `pnl` or `source` is a
    # defect -- but a missing tick count is the row making *no claim*,
    # which marks no gap. So this is the only field where present-null
    # and absent must differ (Codex, PR #297).
    if "funding_ticks_observed" not in record:
        return FUNDING_TICKS_ABSENT
    if is_not_a_number(record["funding_ticks_observed"]):
        return FUNDING_TICKS_MALFORMED
    ticks = record["funding_ticks_observed"]
    count = float(ticks)
    # Finite is not the same as possible, and neither is non-negative: a
    # tick count is how many hourly funding charges landed, so it is a
    # whole number. `0.5` is not "some ticks" any more than `-1` is "no
    # ticks" -- both are rows that cannot say what they saw
    # (Codex, PR #297).
    if not math.isfinite(count) or count < 0 or count != int(count):
        return FUNDING_TICKS_MALFORMED
    return FUNDING_TICKS_NONE if count == 0.0 else FUNDING_TICKS_SOME


def funding_ticks_seen(record: dict) -> bool:
    """Does the row itself say a funding tick landed inside its life?

    A malformed count answers yes, because it is not evidence of zero:
    read as "no ticks" it would turn a missing carry into a verified one.
    """
    return funding_tick_claim(record) in (FUNDING_TICKS_SOME, FUNDING_TICKS_MALFORMED)


def funding_ticks_are_zero(record: dict) -> bool:
    """Does the row *readably* claim that no funding tick landed?

    Only a value that is present and parses to exactly zero. An exact
    `== 0` used to let an export that writes its numbers as strings
    through: `"0"` is not `0`, so a row spanning an hourly boundary with a
    zero carry was accepted as a complete day and its zero went into the
    cost (Codex, PR #297).
    """
    return funding_tick_claim(record) == FUNDING_TICKS_NONE


def is_not_a_number(value: object) -> bool:
    """Is this field something `float()` would silently misread?

    `float(False)` is `0.0` and `float(True)` is `1.0`, so a boolean in a
    money or count field parsed cleanly and was published as a verified
    zero -- a malformed row becoming evidence rather than a gap. `None`
    is not a number either, and neither is a value `float()` refuses.
    Non-finiteness is deliberately *not* tested here: several callers
    distinguish "unreadable" from "infinite" in their own way, and the
    ones that do not test it themselves right after (Codex, PR #297).
    """
    if value is None or isinstance(value, bool):
        return True
    try:
        float(value)
    except (TypeError, ValueError, OverflowError):
        # `json.loads` keeps an integer literal at arbitrary precision,
        # so a field like 10**400 survives parsing and then raises
        # OverflowError here. Catching only TypeError/ValueError let that
        # reach the caller as a traceback instead of the documented
        # unreadable-value handling (Codex, PR #297).
        return True
    return False


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
    # Same fail-safe as an unreadable hold, and for the same reason a
    # boolean needs it: read as a 0s or 1s hold it would claim the row
    # stayed inside one funding interval, turning a missing carry into a
    # verified zero (Codex, PR #297).
    if is_not_a_number(hold):
        return True
    hold_secs = float(hold)
    if not math.isfinite(hold_secs):
        return True
    if hold_secs >= FUNDING_INTERVAL_SECS:
        return True
    close = record.get("ts")
    if is_not_a_number(close):
        return True
    close_secs = float(close)
    if not math.isfinite(close_secs):
        return True
    if hold_secs < 0:
        return True
    opened = close_secs - hold_secs
    return (close_secs // FUNDING_INTERVAL_SECS) != (opened // FUNDING_INTERVAL_SECS)


def _pnl_service_and_arm(name: str) -> str | None:
    """The `<service>-<arm>` part of `pnl-<service>-<arm>-<date>.jsonl`."""
    if not name.startswith("pnl-") or not name.endswith(".jsonl"):
        return None
    stem = name[len("pnl-") : -len(".jsonl")]
    service_and_arm, _, date = stem.rpartition("-")
    if not service_and_arm or not date.isdigit():
        return None
    return service_and_arm


def arm_from_pnl_filename(name: str, service: str | None = None) -> str | None:
    """`pnl-debot-pair-robinhood-lighter-freq-20260908.jsonl` -> `freq`.

    `pnl-<service>-<arm>-<date>` is ambiguous whenever both parts may
    contain hyphens: nothing in `pnl-a-b-c-20260908.jsonl` says whether
    the arm is `c` or `b-c`. Three rounds of this review tried to infer
    it -- the last token, then the longest arm known from another input,
    then refusing when two known arms fit -- and each inference had its
    own way of attributing one arm's realized PnL to another, which is
    the failure the inference existed to prevent. The last of them still
    accepted a sole suffix match, which is not evidence either: the
    other inputs may simply not mention the real arm (Codex, PR #297).

    So it is not inferred. `--pnl-service` names the prefix and the arm
    is exactly what follows it, which is decidable. Without it the last
    token is used -- correct for every filename this project produces,
    and documented as not supporting a hyphenated arm, which needs
    `--pnl-service`. Either way `None` makes the caller refuse the file
    rather than file it under a guess.
    """
    service_and_arm = _pnl_service_and_arm(name)
    if service_and_arm is None:
        return None
    if service is not None:
        prefix = f"{service}-"
        if not service_and_arm.startswith(prefix):
            return None
        arm = service_and_arm[len(prefix):]
    else:
        arm = service_and_arm.rsplit("-", 1)[-1]
    return arm if is_bare_arm(arm) else None


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
    # Keyed by the *latest* timestamp seen for the day, not by arrival
    # order: an export written newest-first would otherwise leave an
    # earlier intraday sample standing as the close, and the resulting
    # delta is published as a known cost (Codex, PR #297).
    last_by_day: dict[str, tuple[float, float]] = {}
    invalid_days: set[str] = set()
    # Days whose *latest* stamp carries two different equity values. Not
    # merged into `invalid_days` as they are found, because a later,
    # strictly greater sample settles the day and clears the tie
    # (Codex, PR #297).
    tied_closes: set[str] = set()
    # The latest instant at which a day had an unreadable sample. A day is
    # only spoiled if that instant is at or after its best readable close:
    # a missing equity at 10:00 followed by a good 23:00 close leaves the
    # day's close perfectly well known, and marking it invalid discarded
    # that day's delta *and* the next day's for nothing. Same rule the
    # tie handling already follows -- a strictly later valid sample
    # settles it (Codex, PR #297).
    last_invalid_us: dict[str, float] = {}
    for row in rows:
        ts = row.get("ts")
        if ts is None:
            raise SubsidyLedgerError(
                "an equity_history row has no `ts`, so the day it belongs to "
                f"cannot be determined: {row!r}")
        # equity_history stamps milliseconds.
        day = None if is_not_a_number(ts) else utc_date_or_none(float(ts) / 1000.0)
        if day is None:
            raise SubsidyLedgerError(f"unreadable `ts` {ts!r} in an equity_history row")
        stamp = float(ts)
        equity = row.get("equity")
        if equity is None or is_not_a_number(equity):
            last_invalid_us[day] = max(last_invalid_us.get(day, stamp), stamp)
            continue
        else:
            value = float(equity)
            # "NaN"/"Infinity" parse but are not a close: the next
            # consecutive-day delta would be non-finite and accepted as a
            # known `equity_delta` cost.
            if math.isfinite(value) and math.isfinite(stamp):
                seen = last_by_day.get(day)
                if seen is None or stamp > seen[0]:
                    last_by_day[day] = (stamp, value)
                    # A strictly later sample is the close, whatever the
                    # earlier instants disagreed about.
                    tied_closes.discard(day)
                elif stamp == seen[0] and value != seen[1]:
                    # Two different closes claiming the same instant.
                    # `>=` made whichever row came last win, so merely
                    # reversing an equivalent export changed the next
                    # day's `equity_delta` cost. Neither is the day's
                    # close (Codex, PR #297).
                    tied_closes.add(day)
            else:
                last_invalid_us[day] = max(last_invalid_us.get(day, stamp), stamp)
    # A day is spoiled only when its unreadable sample is at or after the
    # best close it does have: anything earlier is settled by that close.
    # With no readable close at all the day has nothing to be settled by.
    for day, bad_us in last_invalid_us.items():
        seen = last_by_day.get(day)
        if seen is None or bad_us >= seen[0]:
            invalid_days.add(day)
    invalid_days |= tied_closes
    costs: dict[str, float] = {}
    previous_day: str | None = None
    for day in sorted(last_by_day):
        if (previous_day is not None and is_next_calendar_day(previous_day, day)
                and day not in invalid_days and previous_day not in invalid_days):
            delta = -(last_by_day[day][1] - last_by_day[previous_day][1])
            # Two finite closes of opposite sign can differ by more than a
            # float holds. An unrepresentable delta is not a known cost,
            # so the day stays uncosted rather than becoming `Infinity`
            # in `--out` (Codex, PR #297).
            if math.isfinite(delta):
                costs[day] = delta
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
    for record in read_jsonl(path, tolerate_torn_tail=False):
        date, arm, value = record.get("date"), record.get("arm"), record.get("points")
        if not date or not arm or value is None:
            # This file is written by hand for exactly this report. A line
            # that cannot be attributed is an operator slip worth seeing,
            # not points to drop quietly -- dropping them moves the price
            # per point without saying so.
            raise SubsidyLedgerError(
                f"{path}: a points row needs date, arm and points; got {record!r}")
        # The date is a *join key* against the execution and PnL ledgers,
        # both of which produce it from `utc_date`. A noncanonical but
        # truthy spelling -- "2026-9-08", or a trailing space -- keys a
        # separate points-only row instead, so the day the operator meant
        # to price reports no points and its rate cannot be produced, with
        # a 0 exit (Codex, PR #297).
        # `(date, arm)` is one compound join key and both halves have to
        # survive it. The date is checked below; the arm is compared
        # against `arm_from_pnl_filename` / the execution ledger's
        # `variant`, neither of which can produce a padded or non-string
        # value, so `"freq "` would have become a points-only arm of its
        # own -- the costed day left without points and its rate silently
        # suppressed (Codex, PR #297).
        if not is_bare_arm(arm):
            raise SubsidyLedgerError(
                f"{path}: a points row needs a bare arm name; got {arm!r}")
        if not is_canonical_date(date):
            raise SubsidyLedgerError(
                f"{path}: a points row needs a real YYYY-MM-DD date; got {date!r}")
        if is_not_a_number(value):
            raise SubsidyLedgerError(
                f"{path}: unreadable points value {value!r} for {date}/{arm}")
        parsed = float(value)
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
        # A repeated (date, arm) -- concatenated exports, or a correction
        # appended after the original -- silently kept only the last
        # value, dividing the day's cost by an order-dependent subset of
        # its points. Whether the intent was "replace" or "add" cannot be
        # read from the file, and both change the KPI, so it is refused
        # (Codex, PR #297).
        if (str(date), str(arm)) in points:
            raise SubsidyLedgerError(
                f"{path}: {date}/{arm} appears more than once; a day's points must be a single "
                "row, since neither replacing nor summing can be inferred from the file")
        points[(str(date), str(arm))] = parsed
    return points


def match_coverage(day: PnlDay) -> str | None:
    """`"complete"`, `"incomplete"`, or `None` when the day holds no rows.

    The third case exists because a cross-day entry marker can create a
    `PnlDay` for a date whose own PnL file was never supplied.
    """
    if day.incomplete:
        return "incomplete"
    if day.cycles > 0:
        return "complete"
    return None


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
            # Same contract as `funding_usd` below, and for the same
            # reason: `load_pnl` keeps the good closes' subtotal while
            # marking the day incomplete, so serializing it unconditionally
            # published a partial sum in a field a consumer reads as the
            # day's realized PnL. `cost_usd` was already withheld (or taken
            # from equity) in that case; this makes the two agree
            # (Codex, PR #297).
            row.realized_pnl_usd = (
                round(day.realized_pnl_usd, 6)
                if day.cycles > 0 and not day.incomplete
                else None
            )
            # `null` means "not known", and on a complete day it is not
            # true: every close there either carried a carry or was shown
            # to have met no funding tick, and the cost above was
            # computed with that zero. Reporting it as unknown left a
            # JSON consumer unable to tell a verified zero from a gap --
            # and the gap case is exactly what `incomplete` marks
            # (Codex, PR #297).
            #
            # `funding_seen` is deliberately *not* part of this test. It
            # goes true on the first readable carry, so a day holding one
            # good close and one whose carry is missing, unparseable or
            # non-finite would have published the known subtotal as if it
            # were the day's total. Every path that marks a day incomplete
            # also skips that row's carry -- `unreadable_pnl` and the
            # excluded-source defect both `continue` before funding is
            # read -- so an incomplete day's sum is partial whatever the
            # reason, and the honest value is `null` (Codex, PR #297).
            row.funding_usd = (
                round(day.funding_usd, 6)
                if day.cycles > 0 and not day.incomplete
                else None
            )
            # A day created only to carry a cross-day entry marker has no
            # PnL rows at all, so "complete" would claim a coverage it
            # never had. Its coverage is simply unknown (Codex, PR #297).
            row.pnl_coverage = match_coverage(day)
            row.cross_day_cycles = day.cross_day_cycles
            row.cross_day_entries = day.cross_day_entries
            if day.incomplete:
                row.pnl_incomplete_reasons = sorted(day.incomplete_reasons)
        # Derived first, so a PnL cost that cannot be represented behaves
        # like every other PnL day this ledger cannot cost: it falls
        # through to the equity series. Selecting the branch and *then*
        # discovering the value is unknown left the day uncosted with a
        # usable fallback sitting right there (Codex, PR #297). Each
        # accumulator is guarded on its own, but their sum is a third
        # value and can overflow where neither did.
        pnl_cost = (
            finite_or_none(-(day.realized_pnl_usd + day.funding_usd), 6)
            if day is not None and not day.incomplete and day.cycles > 0
            else None
        )
        if pnl_cost is not None:
            row.cost_usd = pnl_cost
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
                and not row.cost_spans_two_days()):
            row.cost_per_point = finite_or_none(row.cost_usd / row.points, 8)
        # Same rule as the aggregate, and it has to live here too: this row
        # is what `--out` writes and what the daily table prints, so a rate
        # suppressed only in the totals would still be published per day.
        # A cycle that opened yesterday leaves its entry notional on
        # yesterday's row while all of its cost lands here, so this
        # denominator is short by the entry side -- the same kind of
        # known-short denominator as an unvalued fill, and suppressed the
        # same way (Codex, PR #297).
        if (row.cost_usd is not None and row.volume_usd > 0
                and not row.fills_without_value and not row.cost_spans_two_days()):
            row.cost_per_musd_volume = finite_or_none(
                row.cost_usd / (row.volume_usd / 1e6), 4)
        rows.append(row)
    return rows


def summarize(rows: list[Row], points_input: bool | None = None) -> dict:
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
                # Cost excluded from the volume rate because its day's
                # volume and its own cost belong to different days --
                # kept apart from the unmeasured-volume bucket, or the
                # table reports the same rows as both measured and
                # unmeasured (Codex, PR #297).
                "cost_usd_cross_day_volume": 0.0,
                # Row counts, not dollar truthiness: excluded rows whose
                # signed costs cancel to exactly 0.0 still have to be
                # disclosed, and a net of zero is not "nothing was
                # excluded" (Codex, PR #297).
                "days_cross_day_volume": 0,
                "cross_day_volume_from_yesterday": 0,
                "cross_day_volume_closes_later": 0,
                "days_without_volume": 0,
                "cost_usd_on_pointed_days": 0.0,
                "cost_usd_without_points": 0.0,
                # The points mirror: this day *did* supply points, they
                # are simply not the points its cost was earned against.
                "cost_usd_cross_day_points": 0.0,
                # The points classification excludes a different set of
                # rows than the volume one, so it counts its own
                # directions. Sharing them let the points diagnostic
                # claim a direction that only a volume-excluded row had
                # (Codex, PR #297).
                "days_cross_day_points": 0,
                "cross_day_points_from_yesterday": 0,
                "cross_day_points_closes_later": 0,
                "days_without_points": 0,
                # A PnL ledger that was supplied and *rejected* is not an
                # absent one, and the reasons live only on the row, which
                # the table does not print. Aggregated here so the report
                # can name what to go and fix (Codex, PR #297).
                "pnl_rejected_days": 0,
                "pnl_incomplete_reasons": [],
                # Days that supplied a real zero. Kept apart from the
                # ones that supplied nothing (Codex, PR #297).
                "days_zero_points": 0,
                "cost_usd_on_zero_point_days": 0.0,
                "points": 0.0,
                "uncosted_points": 0.0,
                "points_seen": False,
                "cost_days": 0,
                # Costed days whose volume *is* measured but whose cost
                # belongs partly to a cycle that opened the day before.
                # Excluded from the rate for a third reason, and the
                # reader has to be told which one (Codex, PR #297).

                "fills_without_value": 0,
                "incomplete_volume_days": 0,
                "cross_day_cycles": 0,
            },
        )
        arm["days"] += 1
        arm["fills"] += row.fills
        if row.pnl_coverage == "incomplete":
            arm["pnl_rejected_days"] += 1
            arm["pnl_incomplete_reasons"] = sorted(
                set(arm["pnl_incomplete_reasons"]) | set(row.pnl_incomplete_reasons or ())
            )
        # The per-day loaders guard this exact failure; the per-arm roll-up
        # is a second level of accumulation and needs the same guard, or
        # two finite days total to `Infinity` and the printed rates become
        # NaN or a misleading zero (Codex, PR #297).
        arm["volume_usd"] = add_or_none(arm["volume_usd"], row.volume_usd)
        arm["fills_without_value"] += row.fills_without_value or 0
        arm["cross_day_cycles"] += row.cross_day_cycles
        # Only days that actually reported an unvalued fill: the renderer
        # says "N fill(s) across M day(s) reported no value", and a
        # cross-day day's volume *is* measured. Sharing the counter made
        # that sentence describe days it does not apply to
        # (Codex, PR #297).
        if row.fills_without_value:
            arm["incomplete_volume_days"] += 1
        if row.cost_usd is None:
            arm["uncosted_volume_usd"] = add_or_none(arm["uncosted_volume_usd"], row.volume_usd)
        else:
            arm["cost_usd"] = add_or_none(arm["cost_usd"], row.cost_usd)
            arm["cost_days"] += 1
            # Volume ratio: this cost counts only if this row measured the
            # volume it was spent on.
            if (row.volume_usd > 0 and not row.fills_without_value
                    and not row.cost_spans_two_days()):
                arm["cost_usd_on_measured_volume"] = add_or_none(
                    arm["cost_usd_on_measured_volume"], row.cost_usd)
                arm["costed_volume_usd"] = add_or_none(
                    arm["costed_volume_usd"], row.volume_usd)
            elif row.volume_usd > 0 and not row.fills_without_value:
                # Measured, but misaligned: a separate reason and a
                # separate bucket, so no row is described as both.
                arm["days_cross_day_volume"] += 1
                if row.cross_day_cycles:
                    arm["cross_day_volume_from_yesterday"] += 1
                if row.cross_day_entries:
                    arm["cross_day_volume_closes_later"] += 1
                arm["cost_usd_cross_day_volume"] = add_or_none(
                    arm["cost_usd_cross_day_volume"], row.cost_usd)
            else:
                arm["days_without_volume"] += 1
                arm["cost_usd_without_volume"] = add_or_none(
                    arm["cost_usd_without_volume"], row.cost_usd)
            # Points ratio: likewise, only if this row supplied points
            # *and* its cost belongs to the day those points were earned.
            if (row.points is not None and row.points > 0
                    and not row.cost_spans_two_days()):
                arm["cost_usd_on_pointed_days"] = add_or_none(
                    arm["cost_usd_on_pointed_days"], row.cost_usd)
            elif row.points is not None and row.points > 0:
                # Points were supplied; they are just not the points this
                # cost was earned against (Codex, PR #297).
                arm["days_cross_day_points"] += 1
                if row.cross_day_cycles:
                    arm["cross_day_points_from_yesterday"] += 1
                if row.cross_day_entries:
                    arm["cross_day_points_closes_later"] += 1
                arm["cost_usd_cross_day_points"] = add_or_none(
                    arm["cost_usd_cross_day_points"], row.cost_usd)
            else:
                # An explicit `points: 0` is a verified award of nothing,
                # not missing coverage. Grouping the two made the report
                # say "no points supplied" for a day whose own table row
                # shows 0.0 (Codex, PR #297).
                if row.points is None:
                    arm["days_without_points"] += 1
                    arm["cost_usd_without_points"] = add_or_none(
                        arm["cost_usd_without_points"], row.cost_usd)
                else:
                    arm["days_zero_points"] += 1
                    arm["cost_usd_on_zero_point_days"] = add_or_none(
                        arm["cost_usd_on_zero_point_days"], row.cost_usd)
        if row.points is not None:
            arm["points_seen"] = True
            if row.cost_usd is None or row.cost_spans_two_days():
                # Points earned on a day whose cost is unknown, or one
                # whose cost belongs partly to a cycle that opened
                # yesterday. Counting them would divide a numerator and a
                # denominator drawn from different days (Codex, PR #297).
                arm["uncosted_points"] = add_or_none(arm["uncosted_points"], row.points)
            else:
                arm["points"] = add_or_none(arm["points"], row.points)
    for arm in by_arm.values():
        for key in ("volume_usd", "costed_volume_usd", "uncosted_volume_usd", "cost_usd",
                    "cost_usd_on_measured_volume", "cost_usd_without_volume",
                    "cost_usd_cross_day_volume", "cost_usd_cross_day_points",
                    "cost_usd_on_pointed_days", "cost_usd_without_points",
                    "cost_usd_on_zero_point_days"):
            arm[key] = None if arm[key] is None else round(arm[key], 6)
        # A rate needs both of its terms. `None` on either is the total
        # saying it is not known, and a rate over an unknown total is not
        # a number to print (Codex, PR #297).
        arm["cost_per_musd_volume"] = (
            finite_or_none(
                arm["cost_usd_on_measured_volume"] / (arm["costed_volume_usd"] / 1e6), 4)
            if arm["cost_usd_on_measured_volume"] is not None
            and arm["costed_volume_usd"] is not None
            and arm["costed_volume_usd"] > 0
            else None
        )
        arm["cost_per_point"] = (
            finite_or_none(arm["cost_usd_on_pointed_days"] / arm["points"], 8)
            if arm["points_seen"]
            and arm["cost_usd_on_pointed_days"] is not None
            and arm["points"] is not None
            and arm["points"] > 0
            else None
        )
        # A missing rate means two different things and the table has to
        # tell them apart: the coverage genuinely was not there, or it
        # was and a total could not be represented. Derived per rate from
        # the inputs that rate actually needs -- round 37 flagged only
        # the points totals, so an overflow in `costed_volume_usd` or in
        # `cost_usd_on_pointed_days` was still reported as absent
        # coverage (Codex, PR #297).
        arm["volume_rate_unrepresentable"] = (
            arm["cost_usd_on_measured_volume"] is None or arm["costed_volume_usd"] is None
        )
        arm["points_rate_unrepresentable"] = bool(arm["points_seen"]) and (
            arm["points"] is None
            or arm["uncosted_points"] is None
            or arm["cost_usd_on_pointed_days"] is None
        )
        # Whether a points input existed at all, which is different from
        # every points total being unknown. Without it the renderer keyed
        # the point-coverage lines off a day count that is non-zero on
        # the ordinary `--points`-less run, and printed "unknown of cost
        # fell on days with no points supplied" under a known total cost
        # (Codex, PR #297).
        # `points_input` is whether the *caller* gave a points file;
        # `points_seen` is whether this arm had a row in it. They differ
        # exactly when the export omits an arm, and that omission is
        # something the operator needs told rather than hidden by
        # suppressing the section (Codex, PR #297). `None` means the
        # caller did not say, so fall back to the per-arm fact.
        arm["points_supplied"] = (
            bool(arm["points_seen"]) if points_input is None else points_input
        )
        # And whether *this arm* had a row, which is not the same as its
        # totals being non-zero: `points: 0` is a legal award of nothing,
        # and reading it through `uncosted_points`'s truthiness reported
        # a supplied zero as an omitted export (Codex, PR #297).
        arm["points_for_this_arm"] = bool(arm["points_seen"])
        if not arm["points_seen"]:
            # `points` and `uncosted_points` are genuinely absent for
            # this arm, so `None` is the honest value. The *cost*
            # subtotals are not absent -- they are real dollars that fell
            # on days with no points -- and nulling them printed
            # "unknown of cost fell on days with no points supplied"
            # whenever the section was shown. Hiding them was a display
            # concern, and `points_supplied` handles that now
            # (Codex, PR #297).
            arm["points"] = None
            arm["uncosted_points"] = None
        elif arm["uncosted_points"] is not None:
            arm["uncosted_points"] = round(arm["uncosted_points"], 6)
        del arm["points_seen"]
    return {"arms": [by_arm[a] for a in sorted(by_arm)]}


def cross_day_reason(arm: dict, which: str) -> str:
    """Which direction(s) the misalignment ran, for one classification.

    Two different things suppress a rate and the original wording
    described only the first: a cost that opened the day before, and an
    entry made today that closes tomorrow. A day can carry both.

    `which` is `"volume"` or `"points"` because the two classifications
    exclude different sets of rows -- a row can be misaligned for the
    volume rate while supplying no points at all -- and sharing one pair
    of counters let the points diagnostic claim a direction that only a
    volume-excluded row had (Codex, PR #297).
    """
    parts = []
    if arm[f"cross_day_{which}_from_yesterday"]:
        parts.append(f"{arm[f'cross_day_{which}_from_yesterday']} opened the day before")
    if arm[f"cross_day_{which}_closes_later"]:
        parts.append(f"{arm[f'cross_day_{which}_closes_later']} close on a later day")
    return " and ".join(parts) if parts else "cost and volume span two days"


def _no_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict:
    """Refuse an object that names the same field twice.

    `json.loads` keeps the last value silently, so a points row holding
    both `"points": 100` and `"points": 1000` prices the day at 1000 and
    exits 0 -- and a repeated field in a machine ledger can move volume,
    PnL or a join key the same way. Which value a reader takes is a
    parser detail; a record that says two things is not a record
    (Codex, PR #297).
    """
    seen: dict[str, Any] = {}
    for key, value in pairs:
        if key in seen:
            raise SubsidyLedgerError(
                f"the field {key!r} appears twice in one record; which value applies "
                "is not decidable from the file"
            )
        seen[key] = value
    return seen


def _path_key(path: Path) -> tuple:
    """An identity for a path that survives symlinks and spellings.

    `st_dev`/`st_ino` when the file exists, so a symlink, a hard link and
    a `./` spelling all compare equal; the resolved path otherwise, so a
    not-yet-created `--out` still compares against inputs by name
    (Codex, PR #297).
    """
    try:
        stat = path.stat()
        return ("inode", stat.st_dev, stat.st_ino)
    except OSError:
        return ("path", str(path.resolve()))


def money(value: float | None, places: int = 2) -> str:
    """A dollar amount for the table, or the word for "not known".

    Every total can now be `None` -- a roll-up that overflowed says so
    rather than printing `Infinity` -- so the renderer has to be able to
    say it too. Without this the CLI raised
    `unsupported format string passed to NoneType.__format__` instead of
    producing the report (Codex, PR #297).
    """
    return "unknown" if value is None else f"${value:,.{places}f}"


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
        head = (f"  {arm['arm']:6s} volume {money(arm['volume_usd'], 0)} "
                f"over {arm['days']}d")
        if arm["cost_days"] == 0:
            # Nothing priced this arm at all. Saying "cost $0.00" here would
            # read as free rather than as unmeasured.
            if arm["pnl_rejected_days"]:
                # Supplied and refused, which is a different instruction
                # to the reader than "nothing covers these days".
                out.append(
                    f"{head}, cost unknown: a PnL ledger covers "
                    f"{arm['pnl_rejected_days']} of these day(s) but every row in them "
                    f"was rejected ({', '.join(arm['pnl_incomplete_reasons'])}), and no "
                    "equity series covers them either"
                )
            else:
                out.append(
                    f"{head}, cost unknown (no PnL ledger or equity series covers "
                    "these days)")
            # Still say what was supplied and excluded: an all-uncosted
            # points export otherwise printed "cost unknown" and nothing
            # about the points it was given (Codex, PR #297).
            if arm["points_rate_unrepresentable"]:
                out.append(
                    "         the points total could not be represented, so no price per "
                    "point can be computed"
                )
            elif arm["points_for_this_arm"]:
                out.append(
                    f"         {arm['uncosted_points']:,.1f} points were supplied for days with "
                    f"no usable cost, so no price per point can be computed"
                )
            elif arm["points_supplied"]:
                # A points file was given and holds nothing for this arm.
                # This branch returns before the diagnostic further down,
                # so without it the omission is silent on exactly the
                # arms that have no cost either (Codex, PR #297).
                out.append(
                    "         the points file supplied none for this arm, so no price "
                    "per point can be computed"
                )
            if arm["fills_without_value"]:
                out.append(
                    f"         {arm['fills_without_value']} fill(s) across "
                    f"{arm['incomplete_volume_days']} day(s) reported no value, so that volume "
                    f"is a lower bound"
                )
            continue
        # The cost is known even when no rate can be built from it, so it is
        # stated on its own line rather than folded into an equation. Every
        # equation below prints the numerator it actually divided -- an
        # equation that shows a larger total beside a rate computed from a
        # subset is simply false, and a caveat further down does not repair
        # it.
        out.append(
            f"{head}, cost {money(arm['cost_usd'])} across "
            f"{arm['cost_days']} costed day(s)")
        if arm["pnl_rejected_days"]:
            # Some days were costed and others' PnL rows were refused.
            # The reasons are on the rows, which this table does not
            # print, so an operator had no way to see what to fix
            # (Codex, PR #297).
            out.append(
                f"         {arm['pnl_rejected_days']} further day(s) had a PnL ledger "
                f"whose rows were all rejected ({', '.join(arm['pnl_incomplete_reasons'])})"
            )
        if arm["volume_rate_unrepresentable"]:
            out.append(
                "         the measured-volume totals could not be represented, so the "
                "per-$1M rate is unavailable"
            )
        elif arm["cost_per_musd_volume"] is None and arm["days_cross_day_volume"]:
            # The volume is measured; it is misaligned with the cost.
            # Saying "no costed day has fully measured volume" here sent
            # the reader looking for missing fill values that are not
            # missing. And the misalignment runs in two directions -- a
            # cost that opened yesterday, or an entry today that closes
            # tomorrow -- so the line names the one(s) that occurred
            # instead of assuming the close-day case (Codex, PR #297).
            out.append(
                f"         {arm['days_cross_day_volume']} costed day(s) measured their "
                f"volume but it is not aligned with the cost "
                f"({cross_day_reason(arm, 'volume')}), so the per-$1M rate is unavailable"
            )
        elif arm["cost_per_musd_volume"] is None:
            out.append(
                "         no costed day has fully measured volume, so the per-$1M rate "
                "is unavailable"
            )
        else:
            out.append(
                f"         {money(arm['cost_usd_on_measured_volume'])} of it fell on "
                f"{money(arm['costed_volume_usd'], 0)} of measured volume"
                f" = ${arm['cost_per_musd_volume']:,.2f} per $1M traded"
            )
        if arm["uncosted_volume_usd"] is None or arm["uncosted_volume_usd"] > 0:
            out.append(
                f"         {money(arm['uncosted_volume_usd'], 0)} of that volume has no cost "
                f"source and is excluded above"
            )
        if arm["days_without_volume"]:
            out.append(
                f"         {money(arm['cost_usd_without_volume'])} of cost fell on days whose "
                f"volume is unmeasured and is excluded from that rate"
            )
        if arm["days_cross_day_volume"]:
            # A separate line from the one above, because these days'
            # volume *is* measured. Printing them under the same heading
            # made the table say measured and unmeasured of the same
            # rows (Codex, PR #297).
            out.append(
                f"         {money(arm['cost_usd_cross_day_volume'])} of cost fell on days "
                f"whose volume is measured but not aligned with it "
                f"({cross_day_reason(arm, 'volume')}), and is excluded from that rate"
            )
        if arm["fills_without_value"]:
            out.append(
                f"         {arm['fills_without_value']} fill(s) across "
                f"{arm['incomplete_volume_days']} day(s) reported no value, so those days' "
                f"volume is a lower bound"
            )
        if not arm["points_supplied"]:
            # No points input at all: the whole point-coverage section is
            # about a KPI the caller did not ask for (Codex, PR #297).
            continue
        if arm["cost_per_point"] is not None:
            out.append(
                f"         {money(arm['cost_usd_on_pointed_days'])} of it over "
                f"{arm['points']:,.1f} points = ${arm['cost_per_point']:.6f} per point"
            )
        elif arm["points_rate_unrepresentable"]:
            out.append(
                "         the points-day totals could not be represented, so no price "
                "per point can be computed"
            )
        elif arm["days_cross_day_points"]:
            # Both were supplied on the same row; they are misaligned.
            # "No day supplied both" is a different fact and was the
            # wrong one to print here (Codex, PR #297).
            out.append(
                f"         no price per point: the day(s) that supplied points carry a "
                f"cost earned against another day ({cross_day_reason(arm, 'points')})"
            )
        elif not arm["points_for_this_arm"]:
            # A points file was given (or the section would have been
            # skipped) and it holds nothing for this arm. Saying nothing
            # would hide that the export omitted it (Codex, PR #297).
            out.append(
                "         no price per point: the points file supplied none for this arm"
            )
        else:
            # No rate is exactly when the reader most needs to be told
            # why: points and cost landing on different days is the
            # normal cause, and suppressing the diagnostics with the rate
            # left the requested KPI missing with no explanation
            # (Codex, PR #297).
            out.append(
                "         no price per point: no day supplied both a cost and points"
            )
        # These say which side was missing, and are worth printing
        # whether or not a rate came out of what remained.
        if arm["points_rate_unrepresentable"]:
            out.append(
                "         the points total could not be represented, so it is excluded "
                "from that price"
            )
        elif arm["uncosted_points"]:
            out.append(
                f"         {arm['uncosted_points']:,.1f} points earned on days with no usable "
                f"cost are excluded from that price"
            )
        if arm["days_cross_day_points"]:
            out.append(
                f"         {money(arm['cost_usd_cross_day_points'])} of cost fell on days "
                f"that did supply points, but not the points it was earned against "
                f"({cross_day_reason(arm, 'points')}), and is excluded from that price"
            )
        if arm["days_zero_points"]:
            out.append(
                f"         {money(arm['cost_usd_on_zero_point_days'])} of cost fell on "
                f"{arm['days_zero_points']} day(s) that supplied a verified zero points, "
                f"and is excluded from that price"
            )
        if arm["days_without_points"]:
            out.append(
                f"         {money(arm['cost_usd_without_points'])} of cost fell on days with "
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
    seen: set[tuple] = set()
    paths: list[Path] = []
    for pattern in patterns:
        for match in sorted(glob.glob(pattern)):
            # Filesystem identity, not the resolved name: two hard links
            # to one ledger resolve to two distinct pathnames, so a broad
            # archive glob holding both read every record twice and
            # doubled that arm's volume or PnL, exit 0. Same helper the
            # `--out` guard uses, for the same reason (Codex, PR #297).
            key = _path_key(Path(match))
            if key in seen:
                continue
            seen.add(key)
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
    parser.add_argument(
        "--pnl-service",
        default=None,
        help="the <service> part of pnl-<service>-<arm>-<YYYYMMDD>.jsonl; required "
             "for an arm whose own name contains a hyphen, since the filename alone "
             "cannot say where the service ends",
    )
    parser.add_argument("--points", type=Path, default=None)
    parser.add_argument("--out", type=Path, default=None, help="write the rows as JSONL")
    args = parser.parse_args(argv)

    # `--exec-glob` is required, so matching nothing is a mistyped pattern
    # or a missing export -- not an empty period. Left alone the command
    # exited 0, wrote an empty `--out`, and printed no arm-level warning,
    # making a missing ledger indistinguishable from a real empty report.
    #
    # Checked *per pattern*, not on the combined expansion: the option
    # repeats, and one good pattern beside a mistyped one still leaves
    # `exec_paths` non-empty while a whole arm or date silently drops out
    # of the execution denominator (Codex, PR #297).
    unmatched = [pattern for pattern in args.exec_glob if not expand([pattern])]
    if unmatched:
        parser.error(
            "--exec-glob matched no files: "
            + ", ".join(repr(pattern) for pattern in unmatched)
            + "; a missing export cannot be told apart from an empty period"
        )
    # `--pnl-glob` is optional -- a run with no PnL ledger is supported,
    # and falls back to `equity_delta`. But a pattern the operator *did*
    # supply that matches nothing is the same mistake as above, and its
    # silent effect is worse: the costs it would have carried reappear as
    # an equity delta or as uncovered days, and the command still exits 0
    # (Codex, PR #297).
    unmatched_pnl = [pattern for pattern in args.pnl_glob if not expand([pattern])]
    if unmatched_pnl:
        parser.error(
            "--pnl-glob matched no files: "
            + ", ".join(repr(pattern) for pattern in unmatched_pnl)
            + "; omit the option entirely to run without a PnL ledger"
        )
    exec_paths = expand(args.exec_glob)
    pnl_paths = expand(args.pnl_glob)
    equity_paths = [Path(spec.partition("=")[2]) for spec in args.equity
                    if spec.partition("=")[2]]
    inputs = exec_paths + pnl_paths + equity_paths
    if args.points is not None:
        inputs.append(args.points)
    # Before anything is read, and by resolved path so a symlink or a
    # `./` spelling cannot slip through. Every record is read before the
    # write, so `--out` naming an input would truncate a live
    # append-only ledger and replace it with the report -- irreversible,
    # and the command would still exit 0 (Codex, PR #297).
    if args.out is not None:
        out_key = _path_key(args.out)
        clash = [path for path in inputs if _path_key(path) == out_key]
        if clash:
            parser.error(
                f"--out {args.out} is also an input ({clash[0]}); writing it would "
                "destroy that ledger"
            )
    execution = load_execution(exec_paths)
    points = load_points(args.points)
    pnl = load_pnl(pnl_paths, args.pnl_service)
    equity_costs: dict[str, dict[str, float]] = {}
    equity_sources: dict[tuple, str] = {}
    for spec in args.equity:
        arm, sep, path = spec.partition("=")
        # Both sides, not just the path: `--equity =PATH` used to load the
        # costs under the arm `""` and emit them as a separate blank-arm
        # series, leaving the arm the operator meant to cost uncosted --
        # and exiting 0. The arm goes through the same `is_bare_arm` the
        # points file uses, because it is the same join key and
        # `--equity 'freq =h.jsonl'` failed the same silent way
        # (Codex, PR #297).
        if not sep or not path or not is_bare_arm(arm):
            parser.error(f"--equity expects ARM=PATH, got {spec!r}")
        # `--equity` repeats, and a second file for the same arm replaced
        # the first silently: costs the operator did supply would vanish,
        # or turn into uncosted days. Merging two series is not safe
        # either -- the deltas are computed between consecutive daily
        # closes, so two partial exports do not concatenate into one
        # history -- so it is refused (Codex, PR #297).
        if arm in equity_costs:
            parser.error(
                f"--equity given twice for {arm!r}; pass one history file per arm "
                "(the daily deltas are computed across the series, so two files "
                "cannot simply be merged)"
            )
        # And one history may not stand in for two arms. An
        # equity_history is an account-level series, so attributing its
        # daily deltas to two arms doubles the total cost and fabricates
        # a per-arm cost for each -- with a 0 exit. By filesystem
        # identity, so an alias cannot get around it (Codex, PR #297).
        history_key = _path_key(Path(path))
        if history_key in equity_sources:
            parser.error(
                f"--equity {arm}={path} names the same history already given for "
                f"{equity_sources[history_key]!r}; an equity series is account-level, "
                "so attributing its deltas to two arms would double the cost"
            )
        equity_sources[history_key] = arm
        equity_costs[arm] = equity_daily_costs(read_jsonl(Path(path)))

    rows = build_rows(execution, pnl, equity_costs, points)
    summary = summarize(rows, points_input=args.points is not None)
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        with args.out.open("w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row.as_json(), sort_keys=True) + "\n")
    print(render_table(rows, summary))
    return 0


if __name__ == "__main__":
    sys.exit(main())
