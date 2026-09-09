#!/usr/bin/env python3
"""Daily Arcus Spot activity/cost ledger for the subsidy KPI (bot-strategy#938).

The Arcus Spot bot is not run for alpha: it buys genuine-risk Spot volume in
the hope of unlocking Perps access. So the number that decides whether to keep
running it is not PnL but **cost per $1k of volume**, against an owner-declared
ceiling of 0.3% ($3 per $1k, #938).

Both inputs are already immutable and independently verifiable, and each is
used only for what it is authoritative about:

* the execution ledger says what was actually swapped -- pre/post wallet
  balances and gas, never an intent or a quote;
* the hash-chained live-tick event stream says what the marks were at that
  moment, and which rotation the swap belongs to.

A swap that cannot be matched to exactly one event is reported and refused
rather than silently priced at some other tick's marks: a KPI that quietly
drops or misprices trades is worse than no KPI.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable, Sequence

sys.path.insert(0, str(Path(__file__).resolve().parent))
import arcus_live_tick_event_stream as event_stream

DEFAULT_CEILING_USD_PER_1K = Decimal("3")
STOP_RULE_CONSECUTIVE_DAYS = 7
WEI_PER_ETHER = Decimal(10) ** 18
# `HARD_MAX_PLAN_AGE_SECS` in src/arcus_spot/live_executor.rs: the runtime
# refuses to dispatch a plan older than this, whatever the config says.
HARD_MAX_PLAN_AGE_SECS = 60


class ActivityLedgerError(ValueError):
    pass


@dataclass(frozen=True)
class Swap:
    """One reconciled on-chain swap, priced at its own tick's marks."""

    sequence: int
    at: datetime
    date: str
    venue: str
    trigger: str
    sell_symbol: str
    buy_symbol: str
    sell_quantity: Decimal
    buy_quantity: Decimal
    sell_mark_usd: Decimal
    buy_mark_usd: Decimal
    gas_wei: Decimal
    # True when the wallet's native balance *rose* across the swap -- a
    # top-up landing between the two snapshots -- so the transaction's own
    # gas cannot be read from the delta. `gas_wei` is then zero rather
    # than a credit (PR #298 Codex review).
    gas_unmeasurable: bool
    event_sequence: int
    # When the marks this swap was priced at were observed. Every window
    # question is answered with this rather than with `at` (the dispatch),
    # because live-tick commits the event before dispatching against it.
    event_at: datetime

    @property
    def notional_usd(self) -> Decimal:
        return self.sell_quantity * self.sell_mark_usd


@dataclass(frozen=True)
class RoundTrip:
    """One entry and every exit leg that unwinds it.

    A rotation is not always closed by a single exit. When the recorder's
    fixed-notional quote comes back smaller than the open quantity, the
    runtime takes it as an ordinary partial exit and keeps the remainder
    tracked as still open (`src/arcus_spot/runtime.rs`), so the unwind can
    take several legs across several days.
    """

    entry: Swap
    exits: tuple[Swap, ...]

    @property
    def exit(self) -> Swap:
        """The leg that actually closed the rotation."""
        return self.exits[-1]

    @property
    def date(self) -> str:
        """The day the cost became real, i.e. the day the rotation closed."""
        return self.exit.date

    @property
    def volume_usd(self) -> Decimal:
        return self.entry.notional_usd + sum(
            (leg.notional_usd for leg in self.exits), Decimal(0))

    @property
    def loss_usd(self) -> Decimal:
        """Realized round-trip loss, marked at the closing tick.

        Both net token deltas are valued at the marks of the leg that closed
        the rotation, so the number includes whatever the pair drifted while
        it was held. That drift is not an execution cost, but it is money
        that the volume cost us, and pricing each leg at its own mark would
        hide it. With one exit this is exactly the two-leg calculation; with
        several it nets every leg first and prices the result once.
        """
        sold = sum((leg.sell_quantity for leg in self.exits), Decimal(0))
        bought = sum((leg.buy_quantity for leg in self.exits), Decimal(0))
        held = self.entry.buy_quantity - sold
        returned = bought - self.entry.sell_quantity
        pnl = held * self.exit.sell_mark_usd + returned * self.exit.buy_mark_usd
        return -pnl

    @property
    def gas_wei(self) -> Decimal:
        return self.entry.gas_wei + sum((leg.gas_wei for leg in self.exits), Decimal(0))


@dataclass
class DailyRow:
    """One day, with the closed rotations kept apart from the open ones.

    The KPI is a price per unit of volume, so its numerator and denominator
    must describe the same trades. A completed rotation supplies both. An
    open leg supplies gas but no volume it can be charged against, so its
    gas is accounted separately: adding it to `cost_usd` would divide it by
    some other rotation's volume, and on a quiet day that alone could push
    the arm over the ceiling and raise a stop signal nothing earned.
    """

    date: str
    swaps: int = 0
    swap_volume_usd: Decimal = Decimal(0)
    round_trips: int = 0
    round_trip_volume_usd: Decimal = Decimal(0)
    round_trip_loss_usd: Decimal = Decimal(0)
    gas_wei: Decimal = Decimal(0)
    gas_usd: Decimal = Decimal(0)
    open_leg_gas_wei: Decimal = Decimal(0)
    open_leg_gas_usd: Decimal = Decimal(0)
    open_legs: list[int] = field(default_factory=list)

    @property
    def cost_usd(self) -> Decimal:
        """What the closed rotations of this day cost -- the KPI numerator."""
        return self.round_trip_loss_usd + self.gas_usd

    @property
    def spent_usd(self) -> Decimal:
        """Every dollar the wallet gave up on this day, priced or not."""
        return self.cost_usd + self.open_leg_gas_usd

    def cost_per_1k(self) -> Decimal | None:
        """None on a day that closed no rotation.

        A day with nothing to price is not a cheap day and not an expensive
        one; folding it in either direction would move the KPI on days the bot
        did not trade.
        """
        if self.round_trips == 0 or self.round_trip_volume_usd == 0:
            return None
        return self.cost_usd / self.round_trip_volume_usd * 1000


def parse_decimal(value: Any, what: str) -> Decimal:
    try:
        return Decimal(str(value))
    except Exception as error:  # noqa: BLE001 - reported with context below
        raise ActivityLedgerError(f"{what}: not a number: {value!r}") from error


def token_decimals(raw: str, quantity: Decimal, what: str) -> int:
    """Recover a token's decimals from the plan's own raw/decimal pair.

    `live_executor::require_raw_matches_decimal_quantity` already enforces
    that these two describe the same amount, so this reads the exponent the
    runtime itself used rather than assuming every Arcus token is 18-decimal.
    """
    if quantity <= 0:
        raise ActivityLedgerError(f"{what}: non-positive quantity {quantity}")
    scaled = parse_decimal(raw, what) / quantity
    exponent = scaled.log10() if scaled > 0 else Decimal(0)
    decimals = int(exponent.to_integral_value())
    if parse_decimal(raw, what) != quantity * (Decimal(10) ** decimals):
        raise ActivityLedgerError(
            f"{what}: raw amount {raw} is not {quantity} at 10^{decimals}")
    return decimals


def marks_for(event: dict[str, Any], sell_symbol: str,
              buy_symbol: str) -> tuple[Decimal, Decimal]:
    """Map the event's token_a/token_b marks onto this swap's two legs."""
    pair = str(event.get("pair", ""))
    parts = pair.split("/")
    if len(parts) != 2:
        raise ActivityLedgerError(f"event {event.get('sequence')}: unreadable pair {pair!r}")
    token_a, token_b = parts
    marks = {
        token_a: parse_decimal(event.get("token_a_reference_price_usd"),
                               f"event {event.get('sequence')} token_a mark"),
        token_b: parse_decimal(event.get("token_b_reference_price_usd"),
                               f"event {event.get('sequence')} token_b mark"),
    }
    for symbol in (sell_symbol, buy_symbol):
        if symbol not in marks:
            raise ActivityLedgerError(
                f"event {event.get('sequence')}: pair {pair!r} has no mark for {symbol}")
    return marks[sell_symbol], marks[buy_symbol]


def would_rotate_index(events: Iterable[dict[str, Any]]) -> dict[tuple, list[dict[str, Any]]]:
    index: dict[tuple, list[dict[str, Any]]] = {}
    for event in events:
        decision = event.get("decision") or {}
        if decision.get("action") != "would_rotate":
            continue
        plan = decision.get("plan") or {}
        key = (
            str(plan.get("venue", "")).lower(),
            plan.get("sell_symbol"),
            plan.get("buy_symbol"),
            plan.get("sell_amount_raw"),
        )
        index.setdefault(key, []).append(event)
    return index


def find_event(attempt: dict[str, Any], index: dict[tuple, list[dict[str, Any]]],
               ) -> dict[str, Any] | None:
    """The would-rotate observation this dispatch was built from, if present.

    Venue, symbols and `sell_amount_raw` do not prove plan identity -- the
    runtime says so itself and keeps a plan digest for exact identity
    (`require_intent_matches_plan_shape` in
    `src/arcus_spot/live_executor.rs`). The digest is over config+plan and
    the config is not in the event stream, so it cannot be recomputed from
    this side. Two things are available instead, and both come from the
    runtime rather than from taste:

    * the resolved token addresses, the same extra proof the runtime adds on
      top of the coarse shape for the caller that has no digest; and
    * the dispatch-time freshness bound. `validate_plan_age` refuses any
      plan older than `max_plan_age_secs`, itself hard-capped at
      `HARD_MAX_PLAN_AGE_SECS` = 60s, so the observation a dispatch was
      built from is always within 60s of it. Live-tick runs on a 15-minute
      timer (`deploy/arcus-spot-live-tick.timer`), so exactly one candidate
      falls in that bound on the live path.

    More than one is therefore not a tie to break by recency -- picking the
    newest could price a swap at an unrelated tick's marks and trigger, e.g.
    an offline execution of an older approved plan. It is refused.
    """
    intent = attempt.get("intent") or {}
    key = (
        str(intent.get("venue", "")).lower(),
        intent.get("sell_symbol"),
        intent.get("buy_symbol"),
        intent.get("sell_amount_raw"),
    )
    prepared_at = event_stream.parse_timestamp(attempt["prepared_at"])
    candidates = []
    for event in index.get(key, []):
        observed_at = event_stream.parse_timestamp(event["observed_at"])
        if not within_plan_age(observed_at, prepared_at):
            continue
        plan = event["decision"]["plan"]
        if not token_addresses_match(intent, plan):
            continue
        candidates.append(event)
    if not candidates:
        return None
    if len(candidates) > 1:
        raise ActivityLedgerError(
            f"ledger sequence {attempt.get('sequence')}: {len(candidates)} would-rotate events "
            f"within {HARD_MAX_PLAN_AGE_SECS}s before {attempt['prepared_at']} match this swap's "
            "venue/symbols/amount/token addresses, so which marks and trigger priced it cannot "
            "be proven -- refusing rather than guessing at "
            + ", ".join(str(event["sequence"]) for event in candidates))
    return candidates[0]


def within_plan_age(observed_at: datetime, prepared_at: datetime) -> bool:
    """The freshness bound the runtime actually applies, to the second.

    `validate_plan_age` compares `plan_age.num_seconds()` against
    `max_plan_age_secs`, and `num_seconds()` truncates toward zero -- so a
    plan prepared 60.4s after its observation is accepted under a 60s cap.
    Comparing exact datetimes here rejected the same event the runtime had
    already accepted and persisted, which turned a real swap into an
    unmatched one: reported unpriceable, or a hard error. Truncating the
    same way keeps the two in step.
    """
    age = int((prepared_at - observed_at).total_seconds())
    return 0 <= age <= HARD_MAX_PLAN_AGE_SECS


def token_addresses_match(intent: dict[str, Any], plan: dict[str, Any]) -> bool:
    """Same ERC-20 contracts on both sides, not merely the same symbols.

    A symbol registry can resolve the same symbol to a different contract
    later than it did when the intent was signed; the runtime checks the
    pinned addresses for exactly this reason.
    """
    pairs = (
        (intent.get("sell_token"), plan.get("sell_token_address")),
        (intent.get("buy_token"), plan.get("buy_token_address")),
    )
    for on_intent, on_plan in pairs:
        if on_intent is None or on_plan is None:
            return False
        if str(on_intent).lower() != str(on_plan).lower():
            return False
    return True


def swap_from_attempt(attempt: dict[str, Any], event: dict[str, Any]) -> Swap:
    intent = attempt["intent"]
    pre = attempt.get("pre_balances")
    post = attempt.get("post_balances")
    if not pre or not post:
        raise ActivityLedgerError(
            f"ledger sequence {attempt['sequence']}: reconciled attempt without both balance "
            "snapshots")
    plan = event["decision"]["plan"]

    sell_decimals = token_decimals(
        plan["sell_amount_raw"], parse_decimal(plan["sell_quantity"], "plan sell_quantity"),
        f"ledger sequence {attempt['sequence']} sell leg")
    buy_decimals = token_decimals(
        plan["buy_amount_raw"], parse_decimal(plan["buy_quantity"], "plan buy_quantity"),
        f"ledger sequence {attempt['sequence']} buy leg")

    # What the wallet actually did, not what the plan asked for.
    sold_raw = (parse_decimal(pre["sell_balance_raw"], "pre sell balance")
                - parse_decimal(post["sell_balance_raw"], "post sell balance"))
    bought_raw = (parse_decimal(post["buy_balance_raw"], "post buy balance")
                  - parse_decimal(pre["buy_balance_raw"], "pre buy balance"))
    if sold_raw <= 0 or bought_raw <= 0:
        raise ActivityLedgerError(
            f"ledger sequence {attempt['sequence']}: reconciled balances do not show a swap "
            f"(sold {sold_raw}, bought {bought_raw})")

    sell_quantity = sold_raw / (Decimal(10) ** sell_decimals)
    buy_quantity = bought_raw / (Decimal(10) ** buy_decimals)
    sell_mark, buy_mark = marks_for(event, intent["sell_symbol"], intent["buy_symbol"])
    gas_wei = (parse_decimal(pre["gas_balance_wei"], "pre gas balance")
               - parse_decimal(post["gas_balance_wei"], "post gas balance"))
    # A negative delta means the wallet gained native tokens across the
    # swap -- a gas top-up between the pre-swap snapshot and the delayed
    # reconciliation, which the runtime allows because it validates only
    # the sell and buy legs. That inflow is not a negative transaction
    # cost: counted as one it credits an unrelated deposit against a
    # round trip's loss and can suppress a stop signal that is really
    # there. The gas is unknown, so it is zero and said to be unknown
    # (PR #298 Codex review).
    gas_unmeasurable = gas_wei < 0
    if gas_unmeasurable:
        gas_wei = Decimal(0)
    at = event_stream.parse_timestamp(attempt["dispatched_at"])
    return Swap(
        sequence=int(attempt["sequence"]),
        at=at,
        date=at.date().isoformat(),
        venue=str(intent["venue"]).lower(),
        trigger=str(plan.get("trigger", "")),
        sell_symbol=intent["sell_symbol"],
        buy_symbol=intent["buy_symbol"],
        sell_quantity=sell_quantity,
        buy_quantity=buy_quantity,
        sell_mark_usd=sell_mark,
        buy_mark_usd=buy_mark,
        gas_wei=gas_wei,
        gas_unmeasurable=gas_unmeasurable,
        event_sequence=int(event["sequence"]),
        event_at=event_stream.parse_timestamp(event["observed_at"]),
    )


def reconciled_attempts(ledger: dict[str, Any]) -> list[dict[str, Any]]:
    """Every attempt whose swap is on chain and reconciled, wherever it sits.

    `active` is not only a work-in-progress slot. `reconcile_confirmed` can
    durably persist Reconciled and the process then exit before the runtime
    commit archives the attempt into `history`, which is exactly why the
    Reconciled arm of `resume_status_and_reconcile` exists
    (`src/arcus_spot/live_executor.rs`). That swap really happened, so reading
    only `history` would drop a completed rotation from the totals *and* from
    the not-covered list -- silently understating activity and cost at the one
    seam the runtime documents. An attempt is in exactly one of the two slots
    (archiving moves it), so this cannot double-count.
    """
    history = ledger.get("history")
    if not isinstance(history, list):
        raise ActivityLedgerError("execution ledger has no history array")
    attempts = list(history)
    active = ledger.get("active")
    if isinstance(active, dict):
        attempts.append(active)
    return [attempt for attempt in attempts if attempt.get("phase") == "reconciled"]


def reconciled_swaps(ledger: dict[str, Any], index: dict[tuple, list[dict[str, Any]]],
                     window: tuple[datetime, datetime],
                     ) -> tuple[list[Swap], list[tuple[int, datetime]]]:
    """Price every reconciled swap the event window actually covers.

    Coverage is decided by the *pricing event*, not by the dispatch clock.
    live-tick commits a would-rotate event before dispatching the swap that
    event describes (`src/bin/arcus_spot_execute_once.rs`), so `dispatched_at`
    is always later than the marks the swap was priced at. Gating on the
    dispatch time therefore always excluded the final swap of any export whose
    last record is the very event that produced it -- the exact marks were
    present, and the swap was dropped anyway.

    The ledger outlives any one window -- it still holds the NVDA/AMD probe's
    swaps -- and a swap whose marks are not in the given segments cannot be
    priced. Those are returned by sequence instead of being dropped, so a
    report always says which part of the ledger it does not cover; a swap
    dispatched *inside* the window that matches no event stays a hard error,
    because then something really is missing.
    """
    start, end = window
    swaps: list[Swap] = []
    out_of_window: list[tuple[int, datetime]] = []
    for attempt in reconciled_attempts(ledger):
        dispatched_at = event_stream.parse_timestamp(attempt["dispatched_at"])
        event = find_event(attempt, index)
        if event is None:
            if start <= dispatched_at <= end:
                raise ActivityLedgerError(
                    f"ledger sequence {attempt.get('sequence')}: no would-rotate event at or "
                    f"before {attempt['prepared_at']} matches venue/symbols/sell_amount_raw -- "
                    "the event window probably does not cover this swap")
            out_of_window.append((int(attempt["sequence"]), dispatched_at))
            continue
        swap = swap_from_attempt(attempt, event)
        if not start <= swap.event_at <= end:
            out_of_window.append((int(attempt["sequence"]), dispatched_at))
            continue
        swaps.append(swap)
    swaps.sort(key=lambda swap: (swap.at, swap.sequence))
    # The dispatch time rides along so the caller can tell an unpriceable
    # swap it was asked about from one it was not: the first is a hole in
    # the answer, the second is simply outside the question.
    return swaps, sorted(out_of_window)


def pair_round_trips(swaps: Sequence[Swap]) -> tuple[list[RoundTrip], list[Swap]]:
    """Pair each entry with the exit legs that unwind it.

    `trigger` is the runtime's own word for what a rotation was: only
    `entry_signal` opens one, and every other trigger (mean-reversion exit,
    max-hold exit) sells against it.

    A rotation closes when its exits have sold the whole quantity the entry
    acquired, not on the first exit. That is the runtime's own rule, and it
    is exact: the entry sets `rotated_quantity` to the quantity actually
    bought, each exit subtracts the quantity actually sold, and the rotation
    returns to Neutral only when the remainder reaches zero
    (`src/arcus_spot/runtime.rs`). Closing on the first exit instead priced a
    round trip from a fraction of its unwind -- understating both the loss
    and the volume it was earned on -- and reported every later leg as
    unpaired.

    An entry whose exits never finish unwinding it is still open at the end
    of the window, and is not returned here at all: `build_report` derives
    the open set as "in the window and in no round trip it kept", which
    stays exhaustive whatever the reporting bounds remove.

    What *is* returned alongside the round trips is the exits whose entry
    the stream does not hold -- a stream that begins mid-rotation. Those are
    a hole in the report rather than an open position, and the caller
    refuses to publish a stop verdict on a window containing one.
    """
    round_trips: list[RoundTrip] = []
    orphan_exits: list[Swap] = []
    open_entry: Swap | None = None
    open_exits: list[Swap] = []
    remaining = Decimal(0)

    for swap in swaps:
        if swap.trigger == "entry_signal":
            open_entry, open_exits = swap, []
            remaining = swap.buy_quantity
            continue
        if open_entry is None:
            # An exit whose entry the stream does not hold. The rotation it
            # closes really happened and really cost something, but its
            # entry marks are not here, so the loss cannot be computed.
            orphan_exits.append(swap)
            continue
        open_exits.append(swap)
        remaining -= swap.sell_quantity
        if remaining <= 0:
            round_trips.append(RoundTrip(entry=open_entry, exits=tuple(open_exits)))
            open_entry, open_exits = None, []
    return round_trips, orphan_exits


def daily_rows(swaps: Sequence[Swap], round_trips: Sequence[RoundTrip],
               open_legs: Sequence[Swap], gas_price_usd: Decimal) -> list[DailyRow]:
    rows: dict[str, DailyRow] = {}

    def row_for(date: str) -> DailyRow:
        return rows.setdefault(date, DailyRow(date=date))

    def gas_usd(wei: Decimal) -> Decimal:
        return wei / WEI_PER_ETHER * gas_price_usd

    for swap in swaps:
        row = row_for(swap.date)
        row.swaps += 1
        row.swap_volume_usd += swap.notional_usd
    for trip in round_trips:
        row = row_for(trip.date)
        row.round_trips += 1
        row.round_trip_volume_usd += trip.volume_usd
        row.round_trip_loss_usd += trip.loss_usd
        row.gas_wei += trip.gas_wei
        row.gas_usd += gas_usd(trip.gas_wei)
    for swap in open_legs:
        # Gas on a leg that has closed no rotation is money the wallet paid
        # and is never dropped -- but it is kept out of `cost_usd`, because
        # the rate divides that by the *closed* rotations' volume. Charging
        # an open leg's gas against another rotation's volume is how a quiet
        # day gets pushed over the ceiling by a swap that has not yet cost
        # anything measurable.
        row = row_for(swap.date)
        row.open_legs.append(swap.sequence)
        row.open_leg_gas_wei += swap.gas_wei
        row.open_leg_gas_usd += gas_usd(swap.gas_wei)
    return [rows[date] for date in sorted(rows)]


def consecutive_days_over(rows: Sequence[DailyRow], ceiling: Decimal) -> int:
    """Longest run of *priced* days above the ceiling.

    Days that closed no rotation are skipped rather than breaking the run: the
    stop rule in #938 is about a bot that keeps buying volume too expensively,
    and a quiet day is neither evidence for nor against that.
    """
    longest = 0
    current = 0
    for row in rows:
        cost = row.cost_per_1k()
        if cost is None:
            continue
        if cost > ceiling:
            current += 1
            longest = max(longest, current)
        else:
            current = 0
    return longest


def as_number(value: Decimal | None, places: str = "0.0001") -> float | None:
    if value is None:
        return None
    return float(value.quantize(Decimal(places)))


def within(when: datetime, since: datetime | None, until: datetime | None) -> bool:
    """Inside the caller's bounds, each of which is optional."""
    if since is not None and when < since:
        return False
    if until is not None and when > until:
        return False
    return True


def event_window(events: Sequence[dict[str, Any]], since: datetime | None,
                 until: datetime | None) -> tuple[datetime, datetime]:
    """The span of observations available, optionally narrowed.

    `build_report` calls this with no bounds, for the range the marks can
    price at all. The bounds are kept for callers that want the clamped
    span.
    """
    if not events:
        raise ActivityLedgerError("event stream is empty")
    start = event_stream.parse_timestamp(events[0]["observed_at"])
    end = event_stream.parse_timestamp(events[-1]["observed_at"])
    if since is not None:
        start = max(start, since)
    if until is not None:
        end = min(end, until)
    if end < start:
        raise ActivityLedgerError("--since is after --until (or after the event stream ends)")
    return start, end


def build_report(ledger: dict[str, Any], events: Sequence[dict[str, Any]],
                 ceiling: Decimal, gas_price_usd: Decimal,
                 since: datetime | None = None,
                 until: datetime | None = None) -> dict[str, Any]:
    index = would_rotate_index(events)
    # Two different windows, and conflating them dropped whole rotations.
    # The stream window is what the marks can price at all; the report
    # window is what the caller asked to see. Pairing must happen over the
    # first, because a rotation that closes inside the reporting window can
    # have opened before it -- discarding the entry there left the exit
    # unpaired and took the rotation's whole loss and volume out of the very
    # day it was asked about, at every boundary.
    stream = event_window(events, None, None)
    # What the caller asked for, reported as asked. This is no longer
    # clamped to the stream: a bound can legitimately sit outside it, since
    # a swap dispatched just after the stream's last observation still
    # belongs to the day the caller named.
    if since is not None and until is not None and since > until:
        raise ActivityLedgerError("--since is after --until")
    swaps, out_of_window = reconciled_swaps(ledger, index, stream)
    # Pair over everything the stream priced, so a rotation that spans a
    # requested bound is still recognised as one rotation.
    round_trips, orphan_exits = pair_round_trips(swaps)

    # The two bounds answer different questions and so read different
    # clocks. Whether the stream can price a swap is about its marks, so
    # that is `event_at` (above). Whether a caller asked to see it is about
    # the swap, and every date this report prints -- `Swap.date`, and the
    # day a round trip is filed under -- comes from the dispatch. Filtering
    # on the observation instead put a swap dispatched at 00:00:01 outside a
    # report starting at midnight, and could emit a row dated past
    # `--until`. Only an explicitly requested bound narrows anything; with
    # neither given the report covers everything the stream priced.
    swaps = [swap for swap in swaps if within(swap.at, since, until)]
    # A round trip belongs to the day it closed, so that is what the
    # reporting window selects on -- carrying its entry leg in with it.
    round_trips = [trip for trip in round_trips if within(trip.exit.at, since, until)]

    # Everything else in the window is an open leg, derived here rather than
    # carried from `pair_round_trips`. A leg whose rotation closes after
    # `--until` was paired and then dropped with its trip; asking the
    # pairing pass for the open set would not know that, and the leg went
    # missing from the report entirely -- neither priced nor reported open,
    # with its gas gone. Defining "open" as "in the window and not in a
    # round trip this report kept" makes the two sets exhaustive by
    # construction, whatever the bounds do.
    closed = {id(leg) for trip in round_trips for leg in (trip.entry, *trip.exits)}
    open_legs = [swap for swap in swaps if id(swap) not in closed]

    rows = daily_rows(swaps, round_trips, open_legs, gas_price_usd)

    total_volume = sum((trip.volume_usd for trip in round_trips), Decimal(0))
    total_cost = sum((row.cost_usd for row in rows), Decimal(0))
    total_open_gas = sum((row.open_leg_gas_usd for row in rows), Decimal(0))
    overall = (total_cost / total_volume * 1000) if total_volume else None
    streak = consecutive_days_over(rows, ceiling)
    # An implicit endpoint is taken from what the report actually covers,
    # not from the stream alone: with `--since` after the last observation
    # but before its dispatch -- the midnight straddle live-tick produces --
    # `stream[1]` as the end sits *before* the start, and the report then
    # described a real row with an inverted interval.
    #
    # Only the *derived* endpoint may move: clamping both was the mirror
    # bug -- an `--until` before the stream pushed `window.to` forward to
    # the stream start, so a report answering "nothing up to Sep 3"
    # described its window as `from = to = Sep 4`, after the cutoff it was
    # asked about (PR #298 Codex review). A caller who supplies both
    # bounds gets exactly those bounds back, empty intersection included.
    dispatches = [swap.at for swap in swaps]
    window_from = since if since is not None else min([stream[0], *dispatches])
    window_to = until if until is not None else max([stream[1], *dispatches])
    if until is None:
        window_to = max(window_to, window_from)
    if since is None:
        window_from = min(window_from, window_to)
    report_window = (window_from, window_to)

    # Two ways this window can be short a rotation, and both make the stop
    # verdict undecidable. The report still says what it can measure, but it
    # will not hand back a verdict computed on a window it knows is
    # incomplete -- this is what decides whether to keep funding the bot.
    #
    # An exit whose entry the stream does not hold: a rotation really closed
    # here and its loss is in none of the figures.
    # What "the caller asked about" means, in one rule: a bound the caller
    # supplied is used as given -- including the open side of a one-sided
    # request, which really does extend past the events. Only when *no*
    # bound is given at all is the question the window this report
    # covers; clamping a missing endpoint to the stream in the one-sided
    # case dropped requested attempts and published a definitive verdict
    # anyway (PR #298 Codex review, rounds 1 and 2).
    asked = report_window if since is None and until is None else (since, until)
    orphaned = sorted(swap.sequence for swap in orphan_exits
                      if within(swap.at, *asked))
    # And a ledger swap the caller asked about that the stream cannot price
    # at all -- a bound reaching past the events supplied. Naming it in
    # `ledger_swaps_outside_window` was never enough on its own: the swap is
    # inside the question, so the answer is missing a piece.
    #
    # With no bounds given -- the documented invocation -- this is judged
    # against the reported window: the execution ledger outlives each
    # event export, so testing against `(None, None)` admitted every
    # unmatched attempt the ledger has ever held, including probes from
    # before the stream begins, and a routine weekly report came back
    # `undecidable` over a swap outside the period it reports on.
    unpriceable = sorted(sequence for sequence, dispatched_at in out_of_window
                         if within(dispatched_at, *asked))
    # A swap whose gas could not be read is a hole of the same kind: its
    # cost is understated by an unknown amount, so no definitive stop
    # verdict is given for a window containing one.
    unmeasured_gas = sorted(swap.sequence for swap in swaps if swap.gas_unmeasurable)
    complete = not orphaned and not unpriceable and not unmeasured_gas
    return {
        "schema_version": 1,
        "window": {
            "from": report_window[0].isoformat().replace("+00:00", "Z"),
            "to": report_window[1].isoformat().replace("+00:00", "Z"),
        },
        "event_stream": {
            "from": stream[0].isoformat().replace("+00:00", "Z"),
            "to": stream[1].isoformat().replace("+00:00", "Z"),
        },
        "ledger_swaps_outside_window": [sequence for sequence, _ in out_of_window],
        "ceiling_usd_per_1k": as_number(ceiling),
        "gas_price_usd": as_number(gas_price_usd),
        "days": [
            {
                "date": row.date,
                "swaps": row.swaps,
                "swap_volume_usd": as_number(row.swap_volume_usd, "0.01"),
                "round_trips": row.round_trips,
                "round_trip_volume_usd": as_number(row.round_trip_volume_usd, "0.01"),
                "round_trip_loss_usd": as_number(row.round_trip_loss_usd),
                "gas_wei": str(row.gas_wei),
                "gas_usd": as_number(row.gas_usd),
                "cost_usd": as_number(row.cost_usd),
                "open_leg_gas_wei": str(row.open_leg_gas_wei),
                "open_leg_gas_usd": as_number(row.open_leg_gas_usd),
                "spent_usd": as_number(row.spent_usd),
                "cost_per_1k_usd": as_number(row.cost_per_1k()),
                "over_ceiling": (row.cost_per_1k() is not None
                                 and row.cost_per_1k() > ceiling),
                "open_legs": row.open_legs,
            }
            for row in rows
        ],
        "totals": {
            "swaps": sum(row.swaps for row in rows),
            "round_trips": len(round_trips),
            "round_trip_volume_usd": as_number(total_volume, "0.01"),
            "round_trip_loss_usd": as_number(
                sum((row.round_trip_loss_usd for row in rows), Decimal(0))),
            "gas_usd": as_number(sum((row.gas_usd for row in rows), Decimal(0))),
            "cost_usd": as_number(total_cost),
            "open_leg_gas_usd": as_number(total_open_gas),
            "spent_usd": as_number(total_cost + total_open_gas),
            "cost_per_1k_usd": as_number(overall),
            "over_ceiling": overall is not None and overall > ceiling,
        },
        "coverage": {
            "complete": complete,
            "exits_without_entry": orphaned,
            "requested_but_unpriceable": unpriceable,
            "gas_unmeasurable": unmeasured_gas,
        },
        "stop_rule": {
            "consecutive_days_required": STOP_RULE_CONSECUTIVE_DAYS,
            "longest_consecutive_days_over_ceiling": streak,
            "stop_candidate": complete and streak >= STOP_RULE_CONSECUTIVE_DAYS,
            "undecidable": not complete,
        },
    }


def render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "| date | swaps | swap volume $ | round trips | RT volume $ | RT loss $ | gas $ "
        "| cost $ | $/1k |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for day in report["days"]:
        rate = day["cost_per_1k_usd"]
        marker = " ⚠️" if day["over_ceiling"] else ""
        lines.append(
            f"| {day['date']} | {day['swaps']} | {day['swap_volume_usd']:.2f} | "
            f"{day['round_trips']} | {day['round_trip_volume_usd']:.2f} | "
            f"{day['round_trip_loss_usd']:.4f} | {day['gas_usd']:.4f} | "
            f"{day['cost_usd']:.4f} | "
            f"{'—' if rate is None else f'{rate:.2f}{marker}'} |")
    totals = report["totals"]
    overall = totals["cost_per_1k_usd"]
    # Each total under its own heading. Putting the cost in the gas column
    # (and leaving the loss blank) read as "all of this was gas".
    lines.append(
        f"| **total** | {totals['swaps']} | | {totals['round_trips']} | "
        f"{totals['round_trip_volume_usd']:.2f} | {totals['round_trip_loss_usd']:.4f} | "
        f"{totals['gas_usd']:.4f} | {totals['cost_usd']:.4f} | "
        f"{'—' if overall is None else f'{overall:.2f}'} |")
    coverage = report["coverage"]
    if not coverage["complete"]:
        lines.append("")
        reasons = []
        if coverage["exits_without_entry"]:
            reasons.append(
                "closes rotations whose entry the event stream does not hold (ledger sequences "
                + ", ".join(str(s) for s in coverage["exits_without_entry"]) + ")")
        if coverage["requested_but_unpriceable"]:
            reasons.append(
                "covers swaps the event stream cannot price at all (ledger sequences "
                + ", ".join(str(s) for s in coverage["requested_but_unpriceable"]) + ")")
        if coverage.get("gas_unmeasurable"):
            reasons.append(
                "holds swaps whose gas cannot be read because the wallet was topped up across "
                "them (ledger sequences "
                + ", ".join(str(s) for s in coverage["gas_unmeasurable"]) + ")")
        lines.append(
            "⚠️ This window " + " and ".join(reasons)
            + ", so their cost is in none of the figures above and no stop verdict is given. "
              "Re-run with the missing event segment included.")
    open_gas = report["totals"]["open_leg_gas_usd"]
    if open_gas:
        lines.append("")
        lines.append(
            f"Gas on rotations still open: ${open_gas:.4f}. It is real spend and is in "
            f"`spent_usd` (${report['totals']['spent_usd']:.4f}), but not in the $/1k rate "
            "above, which has no volume to divide it by until those rotations close.")
    skipped = report["ledger_swaps_outside_window"]
    if skipped:
        lines.append("")
        lines.append(
            f"Not priceable from this event stream ({report['event_stream']['from']} .. "
            f"{report['event_stream']['to']}): ledger sequences "
            + ", ".join(str(sequence) for sequence in skipped))
    return "\n".join(lines)


def non_negative_decimal(text: str) -> Decimal:
    """A price or a threshold, refused when it cannot be one.

    A negative `--gas-price-usd` turns gas the wallet paid into a credit
    and lowers the KPI; a negative `--ceiling-per-1k` puts every ordinary
    day over the threshold. Either produces a definitive-looking verdict
    from an impossible input, and this report's whole premise is that it
    does not publish numbers it cannot stand behind.
    """
    try:
        value = Decimal(text)
    except InvalidOperation as error:
        raise argparse.ArgumentTypeError(f"{text!r} is not a number") from error
    if not value.is_finite():
        raise argparse.ArgumentTypeError(f"{text!r} is not a finite number")
    if value < 0:
        raise argparse.ArgumentTypeError(f"{text!r} must not be negative")
    return value


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("stream", type=Path, nargs="+",
                        help="hash-chained live-tick event segment(s), in chain order")
    parser.add_argument("--ledger", type=Path, required=True,
                        help="execution ledger JSON (ledger.json)")
    parser.add_argument("--ceiling-per-1k", type=non_negative_decimal,
                        default=DEFAULT_CEILING_USD_PER_1K,
                        help="owner-declared cost ceiling per $1k of volume (default: 3 = 0.3%%)")
    parser.add_argument("--gas-price-usd", type=non_negative_decimal, default=Decimal(0),
                        help="USD price of one native gas token; the router has paid gas so far, "
                             "so the default leaves gas at $0 rather than inventing a mark")
    parser.add_argument("--since", type=event_stream.parse_timestamp,
                        help="ignore swaps before this RFC3339 instant")
    parser.add_argument("--until", type=event_stream.parse_timestamp,
                        help="ignore swaps after this RFC3339 instant")
    parser.add_argument("--markdown", action="store_true",
                        help="print the daily table instead of JSON")
    parser.add_argument("--json-out", type=Path)
    return parser


def run(arguments: argparse.Namespace) -> dict[str, Any]:
    events, _ = event_stream.verify_paths(arguments.stream)
    ledger = json.loads(arguments.ledger.read_text(encoding="utf-8"))
    report = build_report(ledger, events, arguments.ceiling_per_1k,
                          arguments.gas_price_usd, arguments.since, arguments.until)
    if arguments.json_out:
        arguments.json_out.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return report


def main() -> int:
    arguments = build_parser().parse_args()
    try:
        report = run(arguments)
    except (ActivityLedgerError, event_stream.StreamError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1
    print(render_markdown(report) if arguments.markdown else json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
