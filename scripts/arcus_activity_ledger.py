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
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable, Sequence

sys.path.insert(0, str(Path(__file__).resolve().parent))
import arcus_live_tick_event_stream as event_stream

DEFAULT_CEILING_USD_PER_1K = Decimal("3")
STOP_RULE_CONSECUTIVE_DAYS = 7
WEI_PER_ETHER = Decimal(10) ** 18


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
    event_sequence: int

    @property
    def notional_usd(self) -> Decimal:
        return self.sell_quantity * self.sell_mark_usd


@dataclass(frozen=True)
class RoundTrip:
    entry: Swap
    exit: Swap

    @property
    def date(self) -> str:
        """The day the cost became real, i.e. the day the rotation closed."""
        return self.exit.date

    @property
    def volume_usd(self) -> Decimal:
        return self.entry.notional_usd + self.exit.notional_usd

    @property
    def loss_usd(self) -> Decimal:
        """Realized round-trip loss, marked at the exit tick.

        Both net token deltas are valued at the *exit* marks, so the number
        includes whatever the pair drifted while the rotation was held. That
        drift is not an execution cost, but it is money that the volume cost
        us, and pricing each leg at its own entry mark would hide it.
        """
        base = self.exit.buy_quantity - self.entry.sell_quantity
        quote = self.entry.buy_quantity - self.exit.sell_quantity
        pnl = base * self.exit.buy_mark_usd + quote * self.exit.sell_mark_usd
        return -pnl

    @property
    def gas_wei(self) -> Decimal:
        return self.entry.gas_wei + self.exit.gas_wei


@dataclass
class DailyRow:
    date: str
    swaps: int = 0
    swap_volume_usd: Decimal = Decimal(0)
    round_trips: int = 0
    round_trip_volume_usd: Decimal = Decimal(0)
    round_trip_loss_usd: Decimal = Decimal(0)
    gas_wei: Decimal = Decimal(0)
    gas_usd: Decimal = Decimal(0)
    unpaired_entries: list[int] = field(default_factory=list)

    @property
    def cost_usd(self) -> Decimal:
        return self.round_trip_loss_usd + self.gas_usd

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
    """The would-rotate observation this dispatch was built from, if present."""
    intent = attempt.get("intent") or {}
    key = (
        str(intent.get("venue", "")).lower(),
        intent.get("sell_symbol"),
        intent.get("buy_symbol"),
        intent.get("sell_amount_raw"),
    )
    prepared_at = event_stream.parse_timestamp(attempt["prepared_at"])
    candidates = [
        event for event in index.get(key, [])
        if event_stream.parse_timestamp(event["observed_at"]) <= prepared_at
    ]
    if not candidates:
        return None
    # The plan a dispatch was built from is the newest matching observation,
    # the same rule live-tick's own staleness check applies.
    return max(candidates, key=lambda event: event_stream.parse_timestamp(event["observed_at"]))


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
        event_sequence=int(event["sequence"]),
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
                     ) -> tuple[list[Swap], list[int]]:
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
    out_of_window: list[int] = []
    for attempt in reconciled_attempts(ledger):
        event = find_event(attempt, index)
        if event is None:
            dispatched_at = event_stream.parse_timestamp(attempt["dispatched_at"])
            if start <= dispatched_at <= end:
                raise ActivityLedgerError(
                    f"ledger sequence {attempt.get('sequence')}: no would-rotate event at or "
                    f"before {attempt['prepared_at']} matches venue/symbols/sell_amount_raw -- "
                    "the event window probably does not cover this swap")
            out_of_window.append(int(attempt["sequence"]))
            continue
        if not start <= event_stream.parse_timestamp(event["observed_at"]) <= end:
            out_of_window.append(int(attempt["sequence"]))
            continue
        swaps.append(swap_from_attempt(attempt, event))
    swaps.sort(key=lambda swap: (swap.at, swap.sequence))
    return swaps, sorted(out_of_window)


def pair_round_trips(swaps: Sequence[Swap]) -> tuple[list[RoundTrip], list[Swap]]:
    """Pair each entry with the exit that unwinds it.

    `trigger` is the runtime's own word for what a rotation was: only
    `entry_signal` opens one, and every other trigger (mean-reversion exit,
    max-hold exit) closes it. An entry still open at the end of the window is
    reported separately rather than being priced against nothing.
    """
    round_trips: list[RoundTrip] = []
    open_entry: Swap | None = None
    unpaired: list[Swap] = []
    for swap in swaps:
        if swap.trigger == "entry_signal":
            if open_entry is not None:
                unpaired.append(open_entry)
            open_entry = swap
            continue
        if open_entry is None:
            unpaired.append(swap)
            continue
        round_trips.append(RoundTrip(entry=open_entry, exit=swap))
        open_entry = None
    if open_entry is not None:
        unpaired.append(open_entry)
    return round_trips, unpaired


def daily_rows(swaps: Sequence[Swap], round_trips: Sequence[RoundTrip],
               unpaired: Sequence[Swap], gas_price_usd: Decimal) -> list[DailyRow]:
    rows: dict[str, DailyRow] = {}

    def row_for(date: str) -> DailyRow:
        return rows.setdefault(date, DailyRow(date=date))

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
        row.gas_usd += trip.gas_wei / WEI_PER_ETHER * gas_price_usd
    for swap in unpaired:
        row_for(swap.date).unpaired_entries.append(swap.sequence)
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


def event_window(events: Sequence[dict[str, Any]], since: datetime | None,
                 until: datetime | None) -> tuple[datetime, datetime]:
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
    window = event_window(events, since, until)
    swaps, out_of_window = reconciled_swaps(ledger, index, window)
    round_trips, unpaired = pair_round_trips(swaps)
    rows = daily_rows(swaps, round_trips, unpaired, gas_price_usd)

    total_volume = sum((trip.volume_usd for trip in round_trips), Decimal(0))
    total_cost = sum((row.cost_usd for row in rows), Decimal(0))
    overall = (total_cost / total_volume * 1000) if total_volume else None
    streak = consecutive_days_over(rows, ceiling)
    return {
        "schema_version": 1,
        "window": {
            "from": window[0].isoformat().replace("+00:00", "Z"),
            "to": window[1].isoformat().replace("+00:00", "Z"),
        },
        "ledger_swaps_outside_window": out_of_window,
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
                "cost_per_1k_usd": as_number(row.cost_per_1k()),
                "over_ceiling": (row.cost_per_1k() is not None
                                 and row.cost_per_1k() > ceiling),
                "unpaired_swaps": row.unpaired_entries,
            }
            for row in rows
        ],
        "totals": {
            "swaps": sum(row.swaps for row in rows),
            "round_trips": len(round_trips),
            "round_trip_volume_usd": as_number(total_volume, "0.01"),
            "cost_usd": as_number(total_cost),
            "cost_per_1k_usd": as_number(overall),
            "over_ceiling": overall is not None and overall > ceiling,
        },
        "stop_rule": {
            "consecutive_days_required": STOP_RULE_CONSECUTIVE_DAYS,
            "longest_consecutive_days_over_ceiling": streak,
            "stop_candidate": streak >= STOP_RULE_CONSECUTIVE_DAYS,
        },
    }


def render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "| date | swaps | swap volume $ | round trips | RT volume $ | RT loss $ | gas $ | $/1k |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for day in report["days"]:
        cost = day["cost_per_1k_usd"]
        marker = " ⚠️" if day["over_ceiling"] else ""
        lines.append(
            f"| {day['date']} | {day['swaps']} | {day['swap_volume_usd']:.2f} | "
            f"{day['round_trips']} | {day['round_trip_volume_usd']:.2f} | "
            f"{day['round_trip_loss_usd']:.4f} | {day['gas_usd']:.4f} | "
            f"{'—' if cost is None else f'{cost:.2f}{marker}'} |")
    totals = report["totals"]
    overall = totals["cost_per_1k_usd"]
    lines.append(
        f"| **total** | {totals['swaps']} | | {totals['round_trips']} | "
        f"{totals['round_trip_volume_usd']:.2f} | | {totals['cost_usd']:.4f} | "
        f"{'—' if overall is None else f'{overall:.2f}'} |")
    skipped = report["ledger_swaps_outside_window"]
    if skipped:
        lines.append("")
        lines.append(
            f"Not covered by this window ({report['window']['from']} .. "
            f"{report['window']['to']}): ledger sequences "
            + ", ".join(str(sequence) for sequence in skipped))
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("stream", type=Path, nargs="+",
                        help="hash-chained live-tick event segment(s), in chain order")
    parser.add_argument("--ledger", type=Path, required=True,
                        help="execution ledger JSON (ledger.json)")
    parser.add_argument("--ceiling-per-1k", type=Decimal,
                        default=DEFAULT_CEILING_USD_PER_1K,
                        help="owner-declared cost ceiling per $1k of volume (default: 3 = 0.3%%)")
    parser.add_argument("--gas-price-usd", type=Decimal, default=Decimal(0),
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
