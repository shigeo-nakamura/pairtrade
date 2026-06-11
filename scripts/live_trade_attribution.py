#!/usr/bin/env python3
"""
Build a live pairtrade attribution report from exchange accounting first.

The DEX export is treated as the accounting source of truth. Bot pnl JSONL and
journal logs are optional enrichment inputs; missing fields are reported as
gaps instead of being guessed.

Examples:
  scripts/live_trade_attribution.py \
    --dex-zip ~/bot/logs/extended-202697.zip \
    --venue extended \
    --since 2026-06-04T00:00:00Z \
    --until 2026-06-11T06:00:00Z \
    --report-out /tmp/extended-0604-0611-attribution.md \
    --csv-out /tmp/extended-0604-0611-attribution.csv

  scripts/live_trade_attribution.py \
    --dex-path ~/bot/logs/lighter-trade-export-2026-06-11T07_14_03.801Z-UTC.csv \
    --venue lighter \
    --since 2026-06-10T00:00:00Z \
    --until 2026-06-11T08:00:00Z
"""
from __future__ import annotations

import argparse
import csv
import glob
import io
import json
import re
import sys
import zipfile
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Iterable


BASE_MARKET_SUFFIX = "-USD"
DEFAULT_GROUP_WINDOW_SECS = 300.0
DEFAULT_PNL_MATCH_SECS = 300.0
DEFAULT_JOURNAL_MATCH_SECS = 600.0
FILL_MATCH_LOOKBACK_SECS = 12 * 3600.0
FILL_QTY_TOLERANCE = Decimal("0.00000001")
REALIZED_LEG_AGGREGATE_SECS = 2.0

LOG_PREFIX_RE = re.compile(
    r"(?P<wall>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[+-]\d{4})"
    r"\s+\[(?P<level>[A-Z]+)\]\s+-\s+(?P<msg>.*)"
)
KV_RE = re.compile(r"\b(?P<key>[A-Za-z_][A-Za-z0-9_]*)=(?P<value>\"[^\"]*\"|[^\s,]+)")
ENTRY_RE = re.compile(r"\[ENTRY\].*?(?P<direction>LongSpread|ShortSpread)", re.IGNORECASE)
EXIT_REASON_RE = re.compile(r"(?:reason|close_reason)=([A-Za-z0-9_:/.-]+)")
MARKER_WORDS = ("FILL_DETECTION", "partial", "reissue", "timeout", "amend", "ENTRY_CAP")


@dataclass
class DexFill:
    market: str
    side: str
    price: Decimal
    qty: Decimal
    fee: Decimal
    role: str
    time: datetime


@dataclass
class RealizedLeg:
    market: str
    position_side: str
    size: Decimal
    entry_price: Decimal | None
    exit_price: Decimal
    realized_pnl: Decimal
    trade_pnl: Decimal | None
    funding: Decimal | None
    trading_fees: Decimal | None
    close_type: str
    closed_at: datetime
    entry_at: datetime | None = None
    exit_at: datetime | None = None
    entry_role: str = ""
    exit_role: str = ""
    gaps: list[str] = field(default_factory=list)


@dataclass
class PnlRecord:
    ts: datetime
    pair: str
    direction: str
    pnl: Decimal
    source: str
    pnl_available: bool
    close_reason: str
    recovery_reason: str
    z_entry: Decimal | None
    z_exit: Decimal | None
    beta: Decimal | None
    hold_secs: Decimal | None
    funding_carry_usd: Decimal | None


@dataclass
class JournalEvent:
    ts: datetime
    kind: str
    direction: str = ""
    reason: str = ""
    z: Decimal | None = None
    beta: Decimal | None = None
    exit_z: Decimal | None = None
    message: str = ""


@dataclass
class AttributedTrade:
    index: int
    venue: str
    variant: str
    pair: str
    base_market: str
    quote_market: str
    legs: list[RealizedLeg]
    pnl_record: PnlRecord | None = None
    journal_entry: JournalEvent | None = None
    journal_exit: JournalEvent | None = None
    markers: list[JournalEvent] = field(default_factory=list)
    gaps: list[str] = field(default_factory=list)

    @property
    def close_at(self) -> datetime:
        return max(leg.closed_at for leg in self.legs)

    @property
    def entry_at(self) -> datetime | None:
        times = [leg.entry_at for leg in self.legs if leg.entry_at is not None]
        if times:
            return min(times)
        if self.journal_entry:
            return self.journal_entry.ts
        return None

    @property
    def full_entry_at(self) -> datetime | None:
        times = [leg.entry_at for leg in self.legs if leg.entry_at is not None]
        if len(times) >= 2:
            return max(times)
        return None

    @property
    def exit_at(self) -> datetime:
        times = [leg.exit_at for leg in self.legs if leg.exit_at is not None]
        return max(times) if times else self.close_at

    @property
    def direction(self) -> str:
        sides = {leg.market: leg.position_side for leg in self.legs}
        base_side = sides.get(self.base_market)
        quote_side = sides.get(self.quote_market)
        if base_side == "LONG" and quote_side == "SHORT":
            return "LongSpread"
        if base_side == "SHORT" and quote_side == "LONG":
            return "ShortSpread"
        if base_side:
            return f"{base_side.title()}Base"
        if self.pnl_record and self.pnl_record.direction:
            return self.pnl_record.direction
        if self.journal_entry and self.journal_entry.direction:
            return self.journal_entry.direction
        return "unknown"

    @property
    def realized_pnl(self) -> Decimal:
        return sum_decimal(leg.realized_pnl for leg in self.legs)

    @property
    def trade_pnl(self) -> Decimal | None:
        values = [leg.trade_pnl for leg in self.legs if leg.trade_pnl is not None]
        if len(values) != len(self.legs):
            return None
        return sum_decimal(values)

    @property
    def funding(self) -> Decimal | None:
        values = [leg.funding for leg in self.legs if leg.funding is not None]
        if len(values) != len(self.legs):
            return None
        return sum_decimal(values)

    @property
    def trading_fees(self) -> Decimal | None:
        values = [leg.trading_fees for leg in self.legs if leg.trading_fees is not None]
        if len(values) != len(self.legs):
            return None
        return sum_decimal(values)

    @property
    def hold_secs(self) -> float | None:
        start = self.full_entry_at or self.entry_at
        if start is None:
            return None
        return max(0.0, (self.exit_at - start).total_seconds())

    @property
    def entry_sync_secs(self) -> float | None:
        times = [leg.entry_at for leg in self.legs if leg.entry_at is not None]
        if len(times) < 2:
            return None
        return (max(times) - min(times)).total_seconds()

    @property
    def exit_sync_secs(self) -> float | None:
        times = [leg.exit_at for leg in self.legs if leg.exit_at is not None]
        if len(times) < 2:
            return None
        return (max(times) - min(times)).total_seconds()

    @property
    def bot_pnl(self) -> Decimal | None:
        if not self.pnl_record or not self.pnl_record.pnl_available:
            return None
        return self.pnl_record.pnl

    @property
    def execution_leakage(self) -> Decimal | None:
        if self.bot_pnl is None:
            return None
        return self.realized_pnl - self.bot_pnl

    @property
    def close_reason(self) -> str:
        if self.journal_exit and self.journal_exit.reason:
            return self.journal_exit.reason
        close_types = sorted({leg.close_type for leg in self.legs if leg.close_type})
        return "/".join(close_types) if close_types else "n/a"

    def all_gaps(self) -> list[str]:
        gaps = list(self.gaps)
        for leg in self.legs:
            gaps.extend(leg.gaps)
        if self.trade_pnl is None:
            gaps.append("trade_pnl split unavailable")
        if self.funding is None:
            gaps.append("funding split unavailable")
        if self.trading_fees is None:
            gaps.append("trading fee split unavailable")
        if self.pnl_record is None:
            gaps.append("bot pnl jsonl unmatched")
        elif not self.pnl_record.pnl_available and not any(
            gap.startswith("bot pnl unavailable:") for gap in gaps
        ):
            gaps.append("bot pnl unavailable")
        if self.journal_entry is None:
            gaps.append("journal ENTRY unmatched")
        if self.journal_exit is None:
            gaps.append("journal exit reason unmatched")
        return sorted(set(gaps))


def sum_decimal(values: Iterable[Decimal]) -> Decimal:
    total = Decimal("0")
    for value in values:
        total += value
    return total


def parse_decimal(raw: object, default: Decimal | None = None) -> Decimal:
    if raw is None:
        if default is not None:
            return default
        raise ValueError("missing decimal")
    text = str(raw).strip()
    if not text or text == "-":
        if default is not None:
            return default
        raise ValueError("missing decimal")
    try:
        return Decimal(text)
    except InvalidOperation as exc:
        raise ValueError(f"invalid decimal {raw!r}") from exc


def optional_decimal(raw: object) -> Decimal | None:
    try:
        return parse_decimal(raw)
    except ValueError:
        return None


def parse_bool(raw: object, default: bool = False) -> bool:
    if raw is None:
        return default
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, (int, float)):
        return raw != 0
    text = str(raw).strip().lower()
    if text in {"1", "true", "yes", "y"}:
        return True
    if text in {"0", "false", "no", "n"}:
        return False
    return default


def parse_ts(raw: str) -> datetime:
    text = raw.strip()
    if not text:
        raise ValueError("empty timestamp")
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        text = f"{text}T00:00:00Z"
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    if re.fullmatch(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", text):
        text = text.replace(" ", "T") + "+00:00"
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def fmt_ts(dt: datetime | None) -> str:
    if dt is None:
        return ""
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def fmt_decimal(value: Decimal | None, places: int = 6) -> str:
    if value is None:
        return ""
    quant = Decimal("1").scaleb(-places)
    return str(value.quantize(quant))


def fmt_float(value: float | None, places: int = 1) -> str:
    if value is None:
        return ""
    return f"{value:.{places}f}"


def parse_pair(pair: str) -> tuple[str, str]:
    parts = [part for part in pair.replace("-", "/").replace("_", "/").split("/") if part]
    if len(parts) != 2:
        raise ValueError(f"pair must look like BTC/ETH, got {pair!r}")
    return parts[0].upper(), parts[1].upper()


def market_for(symbol: str) -> str:
    symbol = symbol.upper()
    return symbol if "-" in symbol else f"{symbol}{BASE_MARKET_SUFFIX}"


def infer_venue(path: Path, explicit: str) -> str:
    if explicit != "auto":
        return explicit
    if path.suffix.lower() == ".zip" or "extended" in path.name.lower():
        return "extended"
    return "lighter"


def read_csv_from_zip(path: Path, member: str) -> list[dict[str, str]]:
    with zipfile.ZipFile(path) as archive:
        with archive.open(member) as raw:
            text = io.TextIOWrapper(raw, encoding="utf-8")
            return list(csv.DictReader(text))


def read_plain_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def load_extended(path: Path) -> tuple[list[RealizedLeg], list[DexFill], list[str]]:
    warnings: list[str] = []
    realized_rows = read_csv_from_zip(path, "realized_pnl.csv")
    trade_rows = read_csv_from_zip(path, "trades.csv")

    legs: list[RealizedLeg] = []
    for row in realized_rows:
        fees = parse_decimal(row.get("trading_fees"), default=Decimal("0"))
        legs.append(
            RealizedLeg(
                market=row["market"].upper(),
                position_side=row["side"].upper(),
                size=parse_decimal(row["size"]),
                entry_price=parse_decimal(row["entry_price"]),
                exit_price=parse_decimal(row["exit_price"]),
                realized_pnl=parse_decimal(row["realised_pnl"]),
                trade_pnl=parse_decimal(row["trade_pnl"]),
                funding=parse_decimal(row["funding_fees"]),
                trading_fees=fees,
                close_type=row.get("exit_type", ""),
                closed_at=parse_ts(row["closed_at"]),
                gaps=[],
            )
        )

    fills: list[DexFill] = []
    for row in trade_rows:
        fills.append(
            DexFill(
                market=row["market"].upper(),
                side=row["side"].upper(),
                price=parse_decimal(row["price"]),
                qty=parse_decimal(row["qty"]),
                fee=parse_decimal(row.get("fee"), default=Decimal("0")),
                role=row.get("trade_type", ""),
                time=parse_ts(row["time"]),
            )
        )

    match_extended_fills(legs, fills)
    legs = aggregate_realized_legs(legs, REALIZED_LEG_AGGREGATE_SECS)
    return sorted(legs, key=lambda leg: leg.closed_at), sorted(fills, key=lambda fill: fill.time), warnings


def load_lighter(path: Path) -> tuple[list[RealizedLeg], list[DexFill], list[str]]:
    warnings = [
        "Lighter CSV has no separate funding/trade PnL split; Closed PnL is used as realized and trade PnL",
        "Lighter CSV reconstructs entry price by FIFO matching Open rows to Close rows",
    ]
    rows = read_plain_csv(path)
    rows.sort(key=lambda row: parse_ts(row["Date"]))
    open_lots: dict[tuple[str, str], deque[dict[str, object]]] = defaultdict(deque)
    legs: list[RealizedLeg] = []
    fills: list[DexFill] = []

    for row in rows:
        market = market_for(row["Market"])
        side_text = row["Side"].strip()
        parts = side_text.split()
        if len(parts) != 2 or parts[0] not in {"Open", "Close"}:
            warnings.append(f"unrecognized Lighter side {side_text!r}")
            continue
        action, position = parts[0].upper(), parts[1].upper()
        ts = parse_ts(row["Date"])
        size = parse_decimal(row["Size"])
        price = parse_decimal(row["Price"])
        fee = parse_decimal(row.get("Fee"), default=Decimal("0"))
        role = row.get("Role", "")
        trade_side = "BUY" if (action == "OPEN" and position == "LONG") or (action == "CLOSE" and position == "SHORT") else "SELL"
        fills.append(DexFill(market=market, side=trade_side, price=price, qty=size, fee=fee, role=role, time=ts))

        key = (market, position)
        if action == "OPEN":
            open_lots[key].append(
                {
                    "remaining": size,
                    "price": price,
                    "time": ts,
                    "fee": fee,
                    "role": role,
                }
            )
            continue

        closed_pnl = parse_decimal(row.get("Closed PnL"), default=Decimal("0"))
        remaining_close = size
        matched_entry_value = Decimal("0")
        matched_entry_fee = Decimal("0")
        entry_times: list[datetime] = []
        entry_roles: list[str] = []
        gaps: list[str] = []

        while remaining_close > Decimal("0") and open_lots[key]:
            lot = open_lots[key][0]
            lot_remaining = lot["remaining"]
            take = min(remaining_close, lot_remaining)
            matched_entry_value += take * lot["price"]
            matched_entry_fee += lot["fee"] * (take / lot_remaining) if lot_remaining else Decimal("0")
            entry_times.append(lot["time"])
            if lot["role"]:
                entry_roles.append(str(lot["role"]))
            lot["remaining"] = lot_remaining - take
            remaining_close -= take
            if lot["remaining"] <= FILL_QTY_TOLERANCE:
                open_lots[key].popleft()

        if remaining_close > FILL_QTY_TOLERANCE:
            gaps.append(f"unmatched Lighter open lot qty={remaining_close}")

        entry_price = matched_entry_value / (size - remaining_close) if size > remaining_close else None
        total_fees = matched_entry_fee + fee
        legs.append(
            RealizedLeg(
                market=market,
                position_side=position,
                size=size,
                entry_price=entry_price,
                exit_price=price,
                realized_pnl=closed_pnl,
                trade_pnl=closed_pnl,
                funding=None,
                trading_fees=total_fees,
                close_type=row.get("Type", ""),
                closed_at=ts,
                entry_at=min(entry_times) if entry_times else None,
                exit_at=ts,
                entry_role="+".join(sorted(set(entry_roles))),
                exit_role=role,
                gaps=gaps,
            )
        )

    legs = aggregate_realized_legs(legs, REALIZED_LEG_AGGREGATE_SECS)
    return sorted(legs, key=lambda leg: leg.closed_at), sorted(fills, key=lambda fill: fill.time), warnings


def aggregate_realized_legs(legs: list[RealizedLeg], window_secs: float) -> list[RealizedLeg]:
    grouped: list[list[RealizedLeg]] = []
    for leg in sorted(legs, key=lambda item: (item.market, item.position_side, item.close_type, item.closed_at)):
        if not grouped:
            grouped.append([leg])
            continue
        current = grouped[-1]
        first = current[0]
        same_key = (
            leg.market == first.market
            and leg.position_side == first.position_side
            and leg.close_type == first.close_type
        )
        close_delta = abs((leg.closed_at - first.closed_at).total_seconds())
        if same_key and close_delta <= window_secs:
            current.append(leg)
        else:
            grouped.append([leg])

    result: list[RealizedLeg] = []
    for group in grouped:
        if len(group) == 1:
            result.append(group[0])
            continue
        size = sum_decimal(leg.size for leg in group)
        entry_price = None
        if size and all(leg.entry_price is not None for leg in group):
            entry_value = sum_decimal(leg.entry_price * leg.size for leg in group if leg.entry_price is not None)
            entry_price = entry_value / size
        exit_price = sum_decimal(leg.exit_price * leg.size for leg in group) / size if size else group[0].exit_price
        result.append(
            RealizedLeg(
                market=group[0].market,
                position_side=group[0].position_side,
                size=size,
                entry_price=entry_price,
                exit_price=exit_price,
                realized_pnl=sum_decimal(leg.realized_pnl for leg in group),
                trade_pnl=values_or_none([leg.trade_pnl for leg in group]),
                funding=values_or_none([leg.funding for leg in group]),
                trading_fees=values_or_none([leg.trading_fees for leg in group]),
                close_type=group[0].close_type,
                closed_at=max(leg.closed_at for leg in group),
                entry_at=min((leg.entry_at for leg in group if leg.entry_at is not None), default=None),
                exit_at=max((leg.exit_at for leg in group if leg.exit_at is not None), default=None),
                entry_role="+".join(sorted({leg.entry_role for leg in group if leg.entry_role})),
                exit_role="+".join(sorted({leg.exit_role for leg in group if leg.exit_role})),
                gaps=sorted({gap for leg in group for gap in leg.gaps}),
            )
        )
    return sorted(result, key=lambda leg: leg.closed_at)


def side_for_entry(position_side: str) -> str:
    return "BUY" if position_side == "LONG" else "SELL"


def side_for_exit(position_side: str) -> str:
    return "SELL" if position_side == "LONG" else "BUY"


def match_extended_fills(legs: list[RealizedLeg], fills: list[DexFill]) -> None:
    used: set[int] = set()
    for leg in sorted(legs, key=lambda item: item.closed_at):
        entry = find_fill(
            fills=fills,
            used=used,
            market=leg.market,
            side=side_for_entry(leg.position_side),
            qty=leg.size,
            price=leg.entry_price,
            end=leg.closed_at,
            kind="entry",
        )
        if entry is not None:
            idx, fill = entry
            used.add(idx)
            leg.entry_at = fill.time
            leg.entry_role = fill.role
        else:
            leg.gaps.append("entry fill unmatched")

        exit_fill = find_fill(
            fills=fills,
            used=used,
            market=leg.market,
            side=side_for_exit(leg.position_side),
            qty=leg.size,
            price=leg.exit_price,
            end=leg.closed_at,
            kind="exit",
        )
        if exit_fill is not None:
            idx, fill = exit_fill
            used.add(idx)
            leg.exit_at = fill.time
            leg.exit_role = fill.role
        else:
            leg.gaps.append("exit fill unmatched")


def find_fill(
    fills: list[DexFill],
    used: set[int],
    market: str,
    side: str,
    qty: Decimal,
    price: Decimal | None,
    end: datetime,
    kind: str,
) -> tuple[int, DexFill] | None:
    candidates: list[tuple[float, int, DexFill]] = []
    for idx, fill in enumerate(fills):
        if idx in used or fill.market != market or fill.side != side:
            continue
        if abs(fill.qty - qty) > FILL_QTY_TOLERANCE:
            continue
        if price is not None and fill.price != price:
            continue
        delta = (end - fill.time).total_seconds()
        if kind == "entry":
            if delta < 0 or delta > FILL_MATCH_LOOKBACK_SECS:
                continue
            score = delta
        else:
            score = abs(delta)
            if score > DEFAULT_GROUP_WINDOW_SECS:
                continue
        candidates.append((score, idx, fill))
    if not candidates:
        return None
    _, idx, fill = min(candidates, key=lambda item: item[0])
    return idx, fill


def group_trades(
    legs: list[RealizedLeg],
    since: datetime,
    until: datetime,
    venue: str,
    variant: str,
    pair: str,
    base_market: str,
    quote_market: str,
    window_secs: float,
) -> list[AttributedTrade]:
    expected = {base_market, quote_market}
    scoped = [leg for leg in legs if since <= leg.closed_at < until and leg.market in expected]
    scoped.sort(key=lambda leg: leg.closed_at)

    groups: list[list[RealizedLeg]] = []
    current: list[RealizedLeg] = []
    for leg in scoped:
        if not current:
            current = [leg]
            continue
        first = current[0]
        markets = {item.market for item in current}
        close_delta = abs((leg.closed_at - first.closed_at).total_seconds())
        if close_delta <= window_secs and leg.market not in markets:
            current.append(leg)
            groups.append(current)
            current = []
        else:
            groups.append(current)
            current = [leg]
    if current:
        groups.append(current)

    trades: list[AttributedTrade] = []
    for index, group in enumerate(groups, start=1):
        trade = AttributedTrade(
            index=index,
            venue=venue,
            variant=variant,
            pair=pair,
            base_market=base_market,
            quote_market=quote_market,
            legs=group,
        )
        markets = {leg.market for leg in group}
        missing = sorted(expected - markets)
        if missing:
            trade.gaps.append("missing DEX leg(s): " + ",".join(missing))
        if len(group) != 2:
            trade.gaps.append(f"DEX group has {len(group)} leg(s)")
        trades.append(trade)
    return trades


def load_pnl_records(patterns: list[str]) -> list[PnlRecord]:
    records: list[PnlRecord] = []
    paths: list[Path] = []
    for pattern in patterns:
        matches = glob.glob(str(Path(pattern).expanduser()))
        paths.extend(Path(match) for match in matches)
    for path in sorted(set(paths)):
        with path.open(encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                raw = json.loads(line)
                records.append(
                    PnlRecord(
                        ts=parse_pnl_ts(raw.get("ts")),
                        pair=str(raw.get("pair", "")),
                        direction=str(raw.get("direction", "")),
                        pnl=parse_decimal(raw.get("pnl"), default=Decimal("0")),
                        source=str(raw.get("source", "")),
                        pnl_available=parse_bool(raw.get("pnl_available"), default=True),
                        close_reason=str(raw.get("close_reason", "")),
                        recovery_reason=str(raw.get("recovery_reason", "")),
                        z_entry=optional_decimal(raw.get("z_entry")),
                        z_exit=optional_decimal(raw.get("z_exit")),
                        beta=optional_decimal(raw.get("beta")),
                        hold_secs=optional_decimal(raw.get("hold_secs")),
                        funding_carry_usd=optional_decimal(raw.get("funding_carry_usd")),
                    )
                )
    records.sort(key=lambda record: record.ts)
    return records


def parse_pnl_ts(raw: object) -> datetime:
    if isinstance(raw, (int, float)):
        return datetime.fromtimestamp(raw, tz=timezone.utc)
    text = str(raw)
    if re.fullmatch(r"\d+", text):
        return datetime.fromtimestamp(int(text), tz=timezone.utc)
    return parse_ts(text)


def match_pnl_records(trades: list[AttributedTrade], records: list[PnlRecord], max_secs: float) -> None:
    used: set[int] = set()
    for trade in trades:
        candidates: list[tuple[float, int, PnlRecord]] = []
        for idx, record in enumerate(records):
            if idx in used:
                continue
            if record.pair and record.pair.replace("_", "/").upper() != trade.pair.upper():
                continue
            distance = abs((record.ts - trade.close_at).total_seconds())
            if distance <= max_secs:
                candidates.append((distance, idx, record))
        if not candidates:
            continue
        _, idx, record = min(candidates, key=lambda item: item[0])
        used.add(idx)
        trade.pnl_record = record
        if not record.pnl_available:
            reason = record.recovery_reason or record.close_reason or record.source or "unknown"
            trade.gaps.append(f"bot pnl unavailable: {reason}")


def load_journal(paths: list[str]) -> list[JournalEvent]:
    events: list[JournalEvent] = []
    expanded: list[Path] = []
    for pattern in paths:
        expanded.extend(Path(match) for match in glob.glob(str(Path(pattern).expanduser())))
    for path in sorted(set(expanded)):
        with path.open(encoding="utf-8", errors="replace") as handle:
            for line in handle:
                event = parse_journal_line(line)
                if event:
                    events.append(event)
    events.sort(key=lambda event: event.ts)
    return events


def parse_journal_line(line: str) -> JournalEvent | None:
    match = LOG_PREFIX_RE.search(line)
    if not match:
        return None
    message = match.group("msg")
    values = {m.group("key"): m.group("value").strip('"') for m in KV_RE.finditer(message)}
    ts = parse_log_ts(match.group("wall"), values)
    if "[ENTRY]" in message:
        direction = ""
        entry_match = ENTRY_RE.search(message)
        if entry_match:
            direction = entry_match.group("direction")
        return JournalEvent(
            ts=ts,
            kind="entry",
            direction=direction,
            z=optional_decimal(values.get("z")),
            beta=optional_decimal(values.get("beta")),
            message=message,
        )
    if "[EXIT_CHECK]" in message or "[EXIT]" in message or "[CLOSE" in message:
        reason = ""
        reason_match = EXIT_REASON_RE.search(message)
        if reason_match:
            reason = reason_match.group(1)
        return JournalEvent(
            ts=ts,
            kind="exit",
            reason=reason,
            z=optional_decimal(values.get("z")),
            exit_z=optional_decimal(values.get("exit_z")),
            message=message,
        )
    if any(word.lower() in message.lower() for word in MARKER_WORDS):
        return JournalEvent(ts=ts, kind="marker", message=message)
    return None


def parse_log_ts(wall: str, values: dict[str, str]) -> datetime:
    if "ts" in values:
        try:
            return datetime.fromtimestamp(int(values["ts"]), tz=timezone.utc)
        except ValueError:
            pass
    return datetime.strptime(wall, "%Y-%m-%dT%H:%M:%S%z").astimezone(timezone.utc)


def match_journal(trades: list[AttributedTrade], events: list[JournalEvent], max_secs: float) -> None:
    entries = [event for event in events if event.kind == "entry"]
    exits = [event for event in events if event.kind == "exit"]
    markers = [event for event in events if event.kind == "marker"]
    for trade in trades:
        entry_anchor = trade.entry_at or trade.close_at
        entry_lookback_secs = max_secs if trade.entry_at else FILL_MATCH_LOOKBACK_SECS
        before = [
            event
            for event in entries
            if event.ts <= entry_anchor and (entry_anchor - event.ts).total_seconds() <= entry_lookback_secs
        ]
        if before:
            trade.journal_entry = max(before, key=lambda event: event.ts)
        close_candidates = [
            event
            for event in exits
            if abs((event.ts - trade.close_at).total_seconds()) <= max_secs
        ]
        if close_candidates:
            trade.journal_exit = min(close_candidates, key=lambda event: abs((event.ts - trade.close_at).total_seconds()))
        trade.markers = [
            event
            for event in markers
            if trade.entry_at
            and trade.entry_at <= event.ts <= trade.close_at
        ]



def marker_count(trade: AttributedTrade, needle: str) -> int:
    lower = needle.lower()
    return sum(1 for event in trade.markers if lower in event.message.lower())


def bucket_stats(label: str, trades: list[AttributedTrade], selected: list[AttributedTrade]) -> dict[str, object]:
    selected_ids = {id(trade) for trade in selected}
    kept = [trade for trade in trades if id(trade) not in selected_ids]
    blocked_pnl = sum_decimal(trade.realized_pnl for trade in selected)
    kept_pnl = sum_decimal(trade.realized_pnl for trade in kept)
    fees = values_or_none([trade.trading_fees for trade in selected])
    losses_total = sum_decimal(-trade.realized_pnl for trade in trades if trade.realized_pnl < 0)
    blocked_losses = sum_decimal(-trade.realized_pnl for trade in selected if trade.realized_pnl < 0)
    wins_total = sum(1 for trade in trades if trade.realized_pnl > 0)
    blocked_wins = sum(1 for trade in selected if trade.realized_pnl > 0)
    loss_recall = float(blocked_losses / losses_total * Decimal("100")) if losses_total > 0 else 0.0
    win_kill = (blocked_wins / wins_total * 100.0) if wins_total else 0.0
    score = loss_recall - win_kill
    wins = sum(1 for trade in selected if trade.realized_pnl > 0)
    return {
        "label": label,
        "n": len(selected),
        "pnl": blocked_pnl,
        "win_rate": (wins / len(selected) * 100.0) if selected else 0.0,
        "avg_pnl": (blocked_pnl / Decimal(len(selected))) if selected else Decimal("0"),
        "fees": fees,
        "loss_recall": loss_recall,
        "win_kill": win_kill,
        "kept_n": len(kept),
        "kept_pnl": kept_pnl,
        "score": score,
    }


def feature_matrix(trades: list[AttributedTrade]) -> list[dict[str, object]]:
    scoped = [trade for trade in trades if len(trade.legs) == 2]
    if not scoped:
        return []

    def by(label: str, predicate) -> dict[str, object]:
        return bucket_stats(label, scoped, [trade for trade in scoped if predicate(trade)])

    candidates = [
        by("close_reason=force_close", lambda trade: trade.close_reason == "force_close"),
        by("close_reason=ineligible", lambda trade: trade.close_reason == "ineligible"),
        by("close_reason=partial_fill", lambda trade: trade.close_reason == "partial_fill"),
        by("close_reason=stop_loss_z", lambda trade: trade.close_reason == "stop_loss_z"),
        by(
            "partial/reissue markers present",
            lambda trade: marker_count(trade, "partial") > 0 or marker_count(trade, "reissu") > 0,
        ),
        by("reissue markers present", lambda trade: marker_count(trade, "reissu") > 0),
        by("amend markers present", lambda trade: marker_count(trade, "amend") > 0),
        by("bot pnl unavailable", lambda trade: trade.pnl_record is not None and not trade.pnl_record.pnl_available),
        by(
            "abs(entry_z) >= 3.0",
            lambda trade: trade.journal_entry is not None
            and trade.journal_entry.z is not None
            and abs(trade.journal_entry.z) >= Decimal("3.0"),
        ),
        by(
            "abs(entry_z) >= 3.5",
            lambda trade: trade.journal_entry is not None
            and trade.journal_entry.z is not None
            and abs(trade.journal_entry.z) >= Decimal("3.5"),
        ),
        by(
            "abs(entry_z) >= 4.0",
            lambda trade: trade.journal_entry is not None
            and trade.journal_entry.z is not None
            and abs(trade.journal_entry.z) >= Decimal("4.0"),
        ),
        by(
            "beta < 0.9",
            lambda trade: trade.journal_entry is not None
            and trade.journal_entry.beta is not None
            and trade.journal_entry.beta < Decimal("0.9"),
        ),
        by(
            "beta >= 0.9",
            lambda trade: trade.journal_entry is not None
            and trade.journal_entry.beta is not None
            and trade.journal_entry.beta >= Decimal("0.9"),
        ),
        by("entry_z missing", lambda trade: trade.journal_entry is None or trade.journal_entry.z is None),
        by("hold >= 2h", lambda trade: trade.hold_secs is not None and trade.hold_secs >= 7200.0),
        by("exit_sync >= 30s", lambda trade: trade.exit_sync_secs is not None and trade.exit_sync_secs >= 30.0),
        by("entry_sync >= 5s", lambda trade: trade.entry_sync_secs is not None and trade.entry_sync_secs >= 5.0),
    ]
    return sorted(
        [row for row in candidates if row["n"]],
        key=lambda row: (row["score"], -float(row["pnl"])),
        reverse=True,
    )

def summarize(trades: list[AttributedTrade], warnings: list[str], since: datetime, until: datetime) -> str:
    total = sum_decimal(trade.realized_pnl for trade in trades)
    trade_split = values_or_none([trade.trade_pnl for trade in trades])
    funding = values_or_none([trade.funding for trade in trades])
    fees = values_or_none([trade.trading_fees for trade in trades])
    wins = sum(1 for trade in trades if trade.realized_pnl > 0)
    complete = sum(1 for trade in trades if len(trade.legs) == 2)
    gap_count = sum(1 for trade in trades if trade.all_gaps())

    by_direction: dict[str, list[AttributedTrade]] = defaultdict(list)
    for trade in trades:
        by_direction[trade.direction].append(trade)

    lines: list[str] = []
    lines.append("# Live Trade Attribution")
    lines.append("")
    lines.append(f"Window: `{since.isoformat()}` to `{until.isoformat()}`")
    lines.append("")
    lines.append("## Summary")
    lines.append("")
    lines.append("| Metric | Value |")
    lines.append("|---|---:|")
    lines.append(f"| Trades | {len(trades)} |")
    lines.append(f"| Complete two-leg trades | {complete} |")
    lines.append(f"| Win rate | {(wins / len(trades) * 100.0) if trades else 0.0:.1f}% |")
    lines.append(f"| DEX realised PnL | ${fmt_decimal(total, 6)} |")
    lines.append(f"| Trade PnL split | {money_or_gap(trade_split)} |")
    lines.append(f"| Funding split | {money_or_gap(funding)} |")
    lines.append(f"| Trading fees split | {money_or_gap(fees)} |")
    lines.append(f"| Trades with explicit gaps | {gap_count} |")
    lines.append("")

    if by_direction:
        lines.append("## Direction")
        lines.append("")
        lines.append("| Direction | N | DEX realised PnL | Win rate |")
        lines.append("|---|---:|---:|---:|")
        for direction, items in sorted(by_direction.items()):
            pnl = sum_decimal(trade.realized_pnl for trade in items)
            direction_wins = sum(1 for trade in items if trade.realized_pnl > 0)
            lines.append(
                f"| {direction} | {len(items)} | ${fmt_decimal(pnl, 6)} | "
                f"{(direction_wins / len(items) * 100.0):.1f}% |"
            )
        lines.append("")

    matrix = feature_matrix(trades)
    if matrix:
        scoped_count = sum(1 for trade in trades if len(trade.legs) == 2)
        lines.append("## Feature Matrix")
        lines.append("")
        lines.append(f"Scope: complete two-leg trades only (`N={scoped_count}`). `Loss recall` is the share of realised losses captured by the bucket. `Win kill` is the share of winning trades captured by the bucket.")
        lines.append("")
        lines.append("| Rank | Candidate bucket | N | Bucket PnL | Win rate | Avg PnL | Fees | Loss recall | Win kill | Kept N | Kept PnL |")
        lines.append("|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
        for rank, row in enumerate(matrix, start=1):
            lines.append(
                f"| {rank} | {row['label']} | {row['n']} | ${fmt_decimal(row['pnl'], 6)} | "
                f"{row['win_rate']:.1f}% | ${fmt_decimal(row['avg_pnl'], 6)} | "
                f"{money_or_gap(row['fees'])} | {row['loss_recall']:.1f}% | {row['win_kill']:.1f}% | "
                f"{row['kept_n']} | ${fmt_decimal(row['kept_pnl'], 6)} |"
            )
        lines.append("")

    lines.append("## Trades")
    lines.append("")
    lines.append(
        "| # | Close UTC | Direction | Legs | Realised | Trade | Funding | Fees | "
        "Entry sync s | Exit sync s | Hold min | Close reason | Gaps |"
    )
    lines.append("|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|---|")
    for trade in trades:
        gaps = "; ".join(trade.all_gaps())
        lines.append(
            f"| {trade.index} | {fmt_ts(trade.close_at)} | {trade.direction} | {len(trade.legs)} | "
            f"${fmt_decimal(trade.realized_pnl, 6)} | {money_or_gap(trade.trade_pnl)} | "
            f"{money_or_gap(trade.funding)} | {money_or_gap(trade.trading_fees)} | "
            f"{fmt_float(trade.entry_sync_secs)} | {fmt_float(trade.exit_sync_secs)} | "
            f"{fmt_float((trade.hold_secs / 60.0) if trade.hold_secs is not None else None)} | "
            f"{trade.close_reason} | {gaps} |"
        )
    lines.append("")

    if warnings:
        lines.append("## Input Warnings")
        lines.append("")
        for warning in sorted(set(warnings)):
            lines.append(f"- {warning}")
        lines.append("")

    return "\n".join(lines)


def values_or_none(values: list[Decimal | None]) -> Decimal | None:
    if not values or any(value is None for value in values):
        return None
    return sum_decimal(value for value in values if value is not None)


def money_or_gap(value: Decimal | None) -> str:
    if value is None:
        return "n/a"
    return f"${fmt_decimal(value, 6)}"


def write_csv(path: Path, trades: list[AttributedTrade]) -> None:
    fields = [
        "index",
        "venue",
        "variant",
        "pair",
        "direction",
        "entry_at_utc",
        "close_at_utc",
        "legs",
        "realized_pnl",
        "trade_pnl",
        "funding",
        "trading_fees",
        "pnl_source",
        "pnl_available",
        "pnl_close_reason",
        "pnl_recovery_reason",
        "bot_pnl",
        "execution_leakage",
        "entry_sync_secs",
        "exit_sync_secs",
        "hold_secs",
        "journal_entry_z",
        "journal_entry_beta",
        "journal_exit_z",
        "journal_exit_target_z",
        "journal_marker_count",
        "partial_marker_count",
        "reissue_marker_count",
        "amend_marker_count",
        "close_reason",
        "gaps",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for trade in trades:
            writer.writerow(
                {
                    "index": trade.index,
                    "venue": trade.venue,
                    "variant": trade.variant,
                    "pair": trade.pair,
                    "direction": trade.direction,
                    "entry_at_utc": fmt_ts(trade.entry_at),
                    "close_at_utc": fmt_ts(trade.close_at),
                    "legs": len(trade.legs),
                    "realized_pnl": fmt_decimal(trade.realized_pnl, 10),
                    "trade_pnl": fmt_decimal(trade.trade_pnl, 10),
                    "funding": fmt_decimal(trade.funding, 10),
                    "trading_fees": fmt_decimal(trade.trading_fees, 10),
                    "pnl_source": trade.pnl_record.source if trade.pnl_record else "",
                    "pnl_available": str(trade.pnl_record.pnl_available).lower() if trade.pnl_record else "",
                    "pnl_close_reason": trade.pnl_record.close_reason if trade.pnl_record else "",
                    "pnl_recovery_reason": trade.pnl_record.recovery_reason if trade.pnl_record else "",
                    "bot_pnl": fmt_decimal(trade.bot_pnl, 10),
                    "execution_leakage": fmt_decimal(trade.execution_leakage, 10),
                    "entry_sync_secs": fmt_float(trade.entry_sync_secs, 3),
                    "exit_sync_secs": fmt_float(trade.exit_sync_secs, 3),
                    "hold_secs": fmt_float(trade.hold_secs, 3),
                    "journal_entry_z": fmt_decimal(trade.journal_entry.z if trade.journal_entry else None, 10),
                    "journal_entry_beta": fmt_decimal(trade.journal_entry.beta if trade.journal_entry else None, 10),
                    "journal_exit_z": fmt_decimal(trade.journal_exit.z if trade.journal_exit else None, 10),
                    "journal_exit_target_z": fmt_decimal(trade.journal_exit.exit_z if trade.journal_exit else None, 10),
                    "journal_marker_count": len(trade.markers),
                    "partial_marker_count": sum(1 for event in trade.markers if "partial" in event.message.lower()),
                    "reissue_marker_count": sum(1 for event in trade.markers if "reissu" in event.message.lower()),
                    "amend_marker_count": sum(1 for event in trade.markers if "amend" in event.message.lower()),
                    "close_reason": trade.close_reason,
                    "gaps": "; ".join(trade.all_gaps()),
                }
            )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dex-path",
        "--dex-zip",
        dest="dex_path",
        required=True,
        type=Path,
        help="DEX export ZIP/CSV path",
    )
    parser.add_argument("--venue", choices=["auto", "extended", "lighter"], default="auto")
    parser.add_argument("--variant", default="n/a")
    parser.add_argument("--pair", default="BTC/ETH")
    parser.add_argument("--since", required=True)
    parser.add_argument("--until", required=True)
    parser.add_argument("--group-window-secs", type=float, default=DEFAULT_GROUP_WINDOW_SECS)
    parser.add_argument("--pnl-jsonl", action="append", default=[], help="Optional pnl jsonl glob")
    parser.add_argument("--journal-log", action="append", default=[], help="Optional journal log glob")
    parser.add_argument("--csv-out", type=Path)
    parser.add_argument("--report-out", type=Path)
    args = parser.parse_args()

    dex_path = args.dex_path.expanduser()
    since = parse_ts(args.since)
    until = parse_ts(args.until)
    venue = infer_venue(dex_path, args.venue)
    base, quote = parse_pair(args.pair)
    base_market = market_for(base)
    quote_market = market_for(quote)

    if venue == "extended":
        legs, _fills, warnings = load_extended(dex_path)
    elif venue == "lighter":
        legs, _fills, warnings = load_lighter(dex_path)
    else:
        raise AssertionError(f"unexpected venue {venue}")

    trades = group_trades(
        legs=legs,
        since=since,
        until=until,
        venue=venue,
        variant=args.variant,
        pair=f"{base}/{quote}",
        base_market=base_market,
        quote_market=quote_market,
        window_secs=args.group_window_secs,
    )

    if args.pnl_jsonl:
        pnl_records = load_pnl_records(args.pnl_jsonl)
        match_pnl_records(trades, pnl_records, DEFAULT_PNL_MATCH_SECS)
    if args.journal_log:
        journal_events = load_journal(args.journal_log)
        match_journal(trades, journal_events, DEFAULT_JOURNAL_MATCH_SECS)

    report = summarize(trades, warnings, since, until)
    if args.report_out:
        args.report_out.expanduser().write_text(report + "\n", encoding="utf-8")
    else:
        print(report)
    if args.csv_out:
        write_csv(args.csv_out.expanduser(), trades)

    return 0


if __name__ == "__main__":
    sys.exit(main())
