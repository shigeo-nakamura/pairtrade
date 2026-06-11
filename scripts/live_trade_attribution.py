#!/usr/bin/env python3
"""
Build a live pairtrade attribution report from exchange accounting first.

The DEX-side realized_pnl.csv is treated as the accounting source of truth.
Bot pnl jsonl and journal fragments are optional enrichment inputs; when they
are absent or cannot be matched, the report keeps the gap explicit instead of
guessing.

Example:
    scripts/live_trade_attribution.py \
        --dex-zip ~/bot/logs/extended-202697.zip \
        --since 2026-06-04T00:00:00Z \
        --until 2026-06-11T06:00:00Z \
        --venue extended \
        --journal-log /tmp/extended-0604-0611.log \
        --pnl-jsonl '/tmp/extended-pnl/pnl-*.jsonl' \
        --report-out /tmp/extended-0604-0611-attribution.md \
        --csv-out /tmp/extended-0604-0611-attribution.csv
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
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Iterable


BASE_MARKET_SUFFIX = "-USD"
ENTRY_EXIT_MATCH_SECS = 300.0
JOURNAL_ENTRY_LOOKBACK_SECS = 6 * 3600.0
JOURNAL_EXIT_WINDOW_SECS = 10 * 60.0
FILL_BUNDLE_SECS = 2.0
FILL_QTY_TOLERANCE_RATIO = Decimal("0.025")

LOG_PREFIX_RE = re.compile(
    r"(?P<wall>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[+-]\d{4})"
    r"\s+\[(?P<level>[A-Z]+)\]\s+-\s+(?P<msg>.*)"
)
KV_RE = re.compile(r"\b(?P<key>[A-Za-z_][A-Za-z0-9_]*)=(?P<value>\"[^\"]*\"|[^\s,]+)")
MARKER_PATTERNS = ("FILL_DETECTION", "partial", "reissue", "timeout")


@dataclass
class RealizedLeg:
    market: str
    side: str
    size: Decimal
    entry_price: Decimal
    realised_pnl: Decimal
    trade_pnl: Decimal
    funding_fees: Decimal
    trading_fees: Decimal
    exit_price: Decimal
    exit_type: str
    closed_at: datetime


@dataclass
class Fill:
    market: str
    side: str
    price: Decimal
    qty: Decimal
    total_value: Decimal
    fee: Decimal
    trade_type: str
    time: datetime


@dataclass
class PnlRecord:
    ts: datetime
    pair: str
    direction: str
    pnl: Decimal
    source: str
    beta: Decimal | None
    z_entry: Decimal | None
    z_exit: Decimal | None
    hold_secs: Decimal | None
    funding_carry_usd: Decimal | None


@dataclass
class JournalEntry:
    ts: datetime
    pair: str | None
    direction: str | None
    size_a: Decimal | None
    price_a: Decimal | None
    size_b: Decimal | None
    price_b: Decimal | None
    z: Decimal | None
    beta: Decimal | None


@dataclass
class JournalExit:
    ts: datetime
    pair: str | None
    reason: str | None
    raw_type: str


@dataclass
class JournalMarker:
    ts: datetime
    label: str


@dataclass
class JournalData:
    entries: list[JournalEntry] = field(default_factory=list)
    exits: list[JournalExit] = field(default_factory=list)
    markers: list[JournalMarker] = field(default_factory=list)
    line_count: int = 0


@dataclass
class AttributedTrade:
    index: int
    venue: str
    variant: str
    pair: str
    legs: list[RealizedLeg]
    base_market: str
    quote_market: str
    direction: str
    close_at: datetime
    entry_fills: dict[str, Fill] = field(default_factory=dict)
    exit_fills: dict[str, Fill] = field(default_factory=dict)
    pnl_record: PnlRecord | None = None
    journal_entry: JournalEntry | None = None
    journal_exit: JournalExit | None = None
    journal_markers: list[JournalMarker] = field(default_factory=list)
    gaps: list[str] = field(default_factory=list)

    @property
    def realised_pnl(self) -> Decimal:
        return sum_decimal(leg.realised_pnl for leg in self.legs)

    @property
    def trade_pnl(self) -> Decimal:
        return sum_decimal(leg.trade_pnl for leg in self.legs)

    @property
    def funding_fees(self) -> Decimal:
        return sum_decimal(leg.funding_fees for leg in self.legs)

    @property
    def trading_fees(self) -> Decimal:
        return sum_decimal(leg.trading_fees for leg in self.legs)

    @property
    def entry_at(self) -> datetime | None:
        if not self.entry_fills:
            return self.journal_entry.ts if self.journal_entry else None
        return min(fill.time for fill in self.entry_fills.values())

    @property
    def full_entry_at(self) -> datetime | None:
        if len(self.entry_fills) < 2:
            return None
        return max(fill.time for fill in self.entry_fills.values())

    @property
    def exit_at(self) -> datetime | None:
        if not self.exit_fills:
            return self.close_at
        return max(fill.time for fill in self.exit_fills.values())

    @property
    def entry_sync_secs(self) -> float | None:
        if len(self.entry_fills) < 2:
            return None
        times = [fill.time for fill in self.entry_fills.values()]
        return (max(times) - min(times)).total_seconds()

    @property
    def exit_sync_secs(self) -> float | None:
        if len(self.exit_fills) < 2:
            return None
        times = [fill.time for fill in self.exit_fills.values()]
        return (max(times) - min(times)).total_seconds()

    @property
    def dex_hold_secs(self) -> float | None:
        start = self.full_entry_at or self.entry_at
        end = self.exit_at
        if start is None or end is None:
            return None
        return max(0.0, (end - start).total_seconds())

    @property
    def model_pnl(self) -> Decimal | None:
        return self.pnl_record.pnl if self.pnl_record else None

    @property
    def execution_leakage(self) -> Decimal | None:
        if self.model_pnl is None:
            return None
        return self.realised_pnl - self.model_pnl

    @property
    def close_reason(self) -> str:
        if self.journal_exit and self.journal_exit.reason:
            return self.journal_exit.reason
        exit_types = sorted({leg.exit_type for leg in self.legs if leg.exit_type})
        return "/".join(exit_types) if exit_types else "n/a"


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
    try:
        return Decimal(str(raw).strip())
    except (InvalidOperation, AttributeError) as exc:
        raise ValueError(f"invalid decimal {raw!r}") from exc


def optional_decimal(raw: object) -> Decimal | None:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    try:
        return Decimal(text)
    except InvalidOperation:
        return None


def parse_ts(raw: str) -> datetime:
    text = raw.strip()
    if not text:
        raise ValueError("empty timestamp")
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        text = f"{text}T00:00:00Z"
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def parse_log_ts(wall: str, values: dict[str, str]) -> datetime:
    if values.get("ts"):
        try:
            return datetime.fromtimestamp(int(values["ts"]), tz=timezone.utc)
        except ValueError:
            pass
    return datetime.strptime(wall, "%Y-%m-%dT%H:%M:%S%z").astimezone(timezone.utc)


def parse_pair(pair: str) -> tuple[str, str]:
    normalized = pair.replace("_", "/").replace("-", "/")
    parts = [p for p in normalized.split("/") if p]
    if len(parts) != 2:
        raise ValueError(f"pair must look like BTC/ETH, got {pair!r}")
    return parts[0].upper(), parts[1].upper()


def market_for(symbol: str) -> str:
    symbol = symbol.upper()
    return symbol if "-" in symbol else f"{symbol}{BASE_MARKET_SUFFIX}"


def expand_path(path: Path) -> Path:
    return Path(str(path)).expanduser()


def zip_csv_reader(dex_zip: Path, name: str) -> list[dict[str, str]]:
    with zipfile.ZipFile(dex_zip) as archive:
        try:
            with archive.open(name) as raw:
                text = io.TextIOWrapper(raw, encoding="utf-8")
                return list(csv.DictReader(text))
        except KeyError as exc:
            raise FileNotFoundError(f"{name} not found in {dex_zip}") from exc


def load_realized_legs(dex_zip: Path) -> list[RealizedLeg]:
    rows = zip_csv_reader(dex_zip, "realized_pnl.csv")
    legs: list[RealizedLeg] = []
    for row in rows:
        legs.append(
            RealizedLeg(
                market=row["market"],
                side=row["side"].upper(),
                size=parse_decimal(row["size"]),
                entry_price=parse_decimal(row["entry_price"]),
                realised_pnl=parse_decimal(row["realised_pnl"]),
                trade_pnl=parse_decimal(row["trade_pnl"]),
                funding_fees=parse_decimal(row["funding_fees"]),
                trading_fees=parse_decimal(row["trading_fees"]),
                exit_price=parse_decimal(row["exit_price"]),
                exit_type=row.get("exit_type", ""),
                closed_at=parse_ts(row["closed_at"]),
            )
        )
    legs.sort(key=lambda leg: leg.closed_at)
    return legs


def load_fills(dex_zip: Path) -> list[Fill]:
    rows = zip_csv_reader(dex_zip, "trades.csv")
    fills: list[Fill] = []
    for row in rows:
        fills.append(
            Fill(
                market=row["market"],
                side=row["side"].upper(),
                price=parse_decimal(row["price"]),
                qty=parse_decimal(row["qty"]),
                total_value=parse_decimal(row["total_value"]),
                fee=parse_decimal(row["fee"], default=Decimal("0")),
                trade_type=row.get("trade_type", ""),
                time=parse_ts(row["time"]),
            )
        )
    fills.sort(key=lambda fill: fill.time)
    return fills


def infer_venue(dex_zip: Path, explicit: str | None) -> str:
    if explicit:
        return explicit
    name = dex_zip.name.lower()
    if "extended" in name:
        return "extended"
    if "bot-a" in name or "-a" in name:
        return "lighter-a"
    if "bot-b" in name or "-b" in name:
        return "lighter-b"
    if "bot-c" in name or "-c" in name:
        return "lighter-c"
    return dex_zip.stem


def infer_variant(explicit: str | None, venue: str) -> str:
    if explicit:
        return explicit
    for suffix in ("-a", "-b", "-c"):
        if venue.endswith(suffix):
            return suffix[-1]
    return "n/a"


def direction_from_legs(legs: list[RealizedLeg], base_market: str, quote_market: str) -> str:
    by_market = {leg.market: leg.side for leg in legs}
    base_side = by_market.get(base_market)
    quote_side = by_market.get(quote_market)
    if base_side == "LONG" and quote_side == "SHORT":
        return "LongSpread"
    if base_side == "SHORT" and quote_side == "LONG":
        return "ShortSpread"
    if base_side:
        return f"{base_side.title()}Base"
    return "unknown"


def group_realized_trades(
    legs: list[RealizedLeg],
    since: datetime,
    until: datetime,
    venue: str,
    variant: str,
    pair: str,
    base_market: str,
    quote_market: str,
    group_window_secs: float,
) -> list[AttributedTrade]:
    expected = {base_market, quote_market}
    scoped = [
        leg
        for leg in legs
        if since <= leg.closed_at < until and leg.market in expected
    ]
    groups: list[list[RealizedLeg]] = []
    current: list[RealizedLeg] = []
    for leg in scoped:
        if not current:
            current = [leg]
            continue
        first = current[0]
        has_market = {item.market for item in current}
        close_delta = (leg.closed_at - first.closed_at).total_seconds()
        if close_delta <= group_window_secs and leg.market not in has_market:
            current.append(leg)
            groups.append(current)
            current = []
        else:
            groups.append(current)
            current = [leg]
    if current:
        groups.append(current)

    trades: list[AttributedTrade] = []
    for idx, group in enumerate(groups, start=1):
        close_at = max(leg.closed_at for leg in group)
        direction = direction_from_legs(group, base_market, quote_market)
        trade = AttributedTrade(
            index=idx,
            venue=venue,
            variant=variant,
            pair=pair,
            legs=group,
            base_market=base_market,
            quote_market=quote_market,
            direction=direction,
            close_at=close_at,
        )
        markets = {leg.market for leg in group}
        missing = sorted(expected - markets)
        if missing:
            trade.gaps.append("missing DEX leg(s): " + ",".join(missing))
        if len(group) != 2:
            trade.gaps.append(f"DEX group has {len(group)} leg(s)")
        trades.append(trade)
    return trades


def entry_side_for_leg(leg: RealizedLeg) -> str:
    if leg.side == "LONG":
        return "BUY"
    if leg.side == "SHORT":
        return "SELL"
    return "UNKNOWN"


def exit_side_for_leg(leg: RealizedLeg) -> str:
    if leg.side == "LONG":
        return "SELL"
    if leg.side == "SHORT":
        return "BUY"
    return "UNKNOWN"


def fill_matches_exact(fill: Fill, leg: RealizedLeg, side: str, price: Decimal) -> bool:
    return (
        fill.market == leg.market
        and fill.side == side
        and fill.qty == leg.size
        and fill.price == price
    )


def qty_tolerance(target: Decimal) -> Decimal:
    return max(abs(target) * FILL_QTY_TOLERANCE_RATIO, Decimal("0.00000001"))


def aggregate_fills(items: list[Fill]) -> Fill:
    if not items:
        raise ValueError("cannot aggregate empty fill list")
    total_qty = sum_decimal(fill.qty for fill in items)
    total_value = sum_decimal(fill.total_value for fill in items)
    fee = sum_decimal(fill.fee for fill in items)
    if total_qty != Decimal("0"):
        price = total_value / total_qty
    else:
        price = items[0].price
    return Fill(
        market=items[0].market,
        side=items[0].side,
        price=price,
        qty=total_qty,
        total_value=total_value,
        fee=fee,
        trade_type="+".join(sorted({fill.trade_type for fill in items if fill.trade_type})),
        time=max(fill.time for fill in items),
    )


def match_fill_bundle(
    fills: list[Fill],
    used: set[int],
    leg: RealizedLeg,
    target_side: str,
    target_price: Decimal,
    close_at: datetime,
    kind: str,
    match_window_secs: float,
) -> tuple[list[int], Fill | None, bool]:
    candidates: list[tuple[float, int, Fill]] = []
    for idx, fill in enumerate(fills):
        if idx in used:
            continue
        if fill.market != leg.market or fill.side != target_side or fill.price != target_price:
            continue
        if kind == "entry":
            if fill.time > close_at:
                continue
            score = -(close_at - fill.time).total_seconds()
        else:
            distance = abs((fill.time - close_at).total_seconds())
            if distance > match_window_secs:
                continue
            score = -distance
        candidates.append((score, idx, fill))
    if not candidates:
        return [], None, False

    exact = [
        (score, idx, fill)
        for score, idx, fill in candidates
        if fill_matches_exact(fill, leg, target_side, target_price)
    ]
    if exact:
        _, idx, fill = max(exact, key=lambda item: item[0])
        return [idx], fill, False

    tolerance = qty_tolerance(leg.size)
    preferred = sorted(candidates, key=lambda item: item[0], reverse=True)
    for _, seed_idx, seed in preferred:
        bundle: list[tuple[int, Fill]] = []
        for _, idx, fill in candidates:
            if abs((fill.time - seed.time).total_seconds()) <= FILL_BUNDLE_SECS:
                bundle.append((idx, fill))
        aggregate = aggregate_fills([fill for _, fill in bundle])
        if abs(aggregate.qty - leg.size) <= tolerance:
            return [idx for idx, _ in bundle], aggregate, aggregate.qty != leg.size

    # Last resort: take the closest single fill if its quantity is near the
    # realized leg size. This keeps leg-sync timing available while surfacing
    # the quantity mismatch as a report gap.
    _, idx, fill = preferred[0]
    if abs(fill.qty - leg.size) <= tolerance:
        return [idx], fill, fill.qty != leg.size
    return [], None, False


def attach_fills(trades: list[AttributedTrade], fills: list[Fill], match_window_secs: float) -> None:
    used: set[int] = set()
    for trade in trades:
        for leg in trade.legs:
            entry_indices, entry_fill, entry_qty_mismatch = match_fill_bundle(
                fills=fills,
                used=used,
                leg=leg,
                target_side=entry_side_for_leg(leg),
                target_price=leg.entry_price,
                close_at=trade.close_at,
                kind="entry",
                match_window_secs=match_window_secs,
            )
            if entry_fill:
                used.update(entry_indices)
                trade.entry_fills[leg.market] = entry_fill
                if entry_qty_mismatch:
                    trade.gaps.append(f"entry fill qty mismatch {leg.market}")
            else:
                trade.gaps.append(f"unmatched entry fill {leg.market}")

            exit_indices, exit_fill, exit_qty_mismatch = match_fill_bundle(
                fills=fills,
                used=used,
                leg=leg,
                target_side=exit_side_for_leg(leg),
                target_price=leg.exit_price,
                close_at=trade.close_at,
                kind="exit",
                match_window_secs=match_window_secs,
            )
            if exit_fill:
                used.update(exit_indices)
                trade.exit_fills[leg.market] = exit_fill
                if exit_qty_mismatch:
                    trade.gaps.append(f"exit fill qty mismatch {leg.market}")
            else:
                trade.gaps.append(f"unmatched exit fill {leg.market}")


def resolve_pnl_paths(patterns: list[str]) -> list[Path]:
    paths: list[Path] = []
    for pattern in patterns:
        expanded = str(Path(pattern).expanduser())
        matches = [Path(p) for p in glob.glob(expanded)]
        if not matches:
            candidate = Path(expanded)
            if candidate.is_dir():
                matches = sorted(candidate.glob("pnl-*.jsonl"))
            elif candidate.exists():
                matches = [candidate]
        for path in matches:
            if path.is_dir():
                paths.extend(sorted(path.glob("pnl-*.jsonl")))
            elif path.exists():
                paths.append(path)
    return sorted(dict.fromkeys(paths))


def load_pnl_records(patterns: list[str], since: datetime, until: datetime) -> list[PnlRecord]:
    records: list[PnlRecord] = []
    for path in resolve_pnl_paths(patterns):
        with path.open() as handle:
            for line in handle:
                if not line.strip():
                    continue
                data = json.loads(line)
                ts = datetime.fromtimestamp(int(data["ts"]), tz=timezone.utc)
                if not (since <= ts < until):
                    continue
                records.append(
                    PnlRecord(
                        ts=ts,
                        pair=str(data.get("pair", "")),
                        direction=str(data.get("direction", "")),
                        pnl=parse_decimal(data.get("pnl", "0")),
                        source=str(data.get("source", "")),
                        beta=optional_decimal(data.get("beta")),
                        z_entry=optional_decimal(data.get("z_entry")),
                        z_exit=optional_decimal(data.get("z_exit")),
                        hold_secs=optional_decimal(data.get("hold_secs")),
                        funding_carry_usd=optional_decimal(data.get("funding_carry_usd")),
                    )
                )
    records.sort(key=lambda record: record.ts)
    return records


def normalize_direction(value: str | None) -> str:
    text = (value or "").replace("_", "").replace("-", "").lower()
    if text in {"longspread", "long"}:
        return "longspread"
    if text in {"shortspread", "short"}:
        return "shortspread"
    return text


def attach_pnl_records(
    trades: list[AttributedTrade],
    records: list[PnlRecord],
    match_window_secs: float,
) -> None:
    if not records:
        for trade in trades:
            trade.gaps.append("no matched pnl jsonl")
        return
    used: set[int] = set()
    for trade in trades:
        candidates: list[tuple[float, int, PnlRecord]] = []
        target_dir = normalize_direction(trade.direction)
        for idx, record in enumerate(records):
            if idx in used:
                continue
            if target_dir and normalize_direction(record.direction) != target_dir:
                continue
            distance = abs((record.ts - trade.close_at).total_seconds())
            if distance > match_window_secs:
                continue
            candidates.append((-distance, idx, record))
        if not candidates:
            trade.gaps.append("no matched pnl jsonl")
            continue
        _, idx, record = max(candidates, key=lambda item: item[0])
        used.add(idx)
        trade.pnl_record = record


def parse_kv_pairs(msg: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for match in KV_RE.finditer(msg):
        value = match.group("value")
        if value.startswith('"') and value.endswith('"'):
            value = value[1:-1]
        values[match.group("key")] = value
    return values


def marker_label(line: str) -> str | None:
    lower = line.lower()
    for pattern in MARKER_PATTERNS:
        if pattern == "FILL_DETECTION" and pattern in line:
            return pattern
        if pattern != "FILL_DETECTION" and pattern in lower:
            return pattern
    return None


def load_journal(path: Path | None, since: datetime, until: datetime) -> JournalData:
    data = JournalData()
    if path is None:
        return data
    path = expand_path(path)
    if not path.exists():
        raise FileNotFoundError(f"journal log not found: {path}")
    with path.open(errors="replace") as handle:
        for line in handle:
            data.line_count += 1
            m = LOG_PREFIX_RE.search(line)
            if not m:
                continue
            msg = m.group("msg")
            values = parse_kv_pairs(msg)
            ts = parse_log_ts(m.group("wall"), values)
            if not (since <= ts < until):
                continue
            if "[ENTRY]" in msg:
                data.entries.append(
                    JournalEntry(
                        ts=ts,
                        pair=values.get("pair"),
                        direction=values.get("direction"),
                        size_a=optional_decimal(values.get("size_a")),
                        price_a=optional_decimal(values.get("price_a")),
                        size_b=optional_decimal(values.get("size_b")),
                        price_b=optional_decimal(values.get("price_b")),
                        z=optional_decimal(values.get("z")),
                        beta=optional_decimal(values.get("beta")),
                    )
                )
            elif "[EXIT_CHECK]" in msg:
                data.exits.append(
                    JournalExit(
                        ts=ts,
                        pair=values.get("pair"),
                        reason=values.get("reason"),
                        raw_type="EXIT_CHECK",
                    )
                )
            elif "[EXIT]" in msg:
                data.exits.append(
                    JournalExit(
                        ts=ts,
                        pair=values.get("pair"),
                        reason=values.get("reason") or "exit",
                        raw_type="EXIT",
                    )
                )
            label = marker_label(line)
            if label:
                data.markers.append(JournalMarker(ts=ts, label=label))
    data.entries.sort(key=lambda item: item.ts)
    data.exits.sort(key=lambda item: item.ts)
    data.markers.sort(key=lambda item: item.ts)
    return data


def attach_journal(trades: list[AttributedTrade], journal: JournalData) -> None:
    if journal.line_count == 0:
        for trade in trades:
            trade.gaps.append("no journal log")
        return

    used_entries: set[int] = set()
    used_exits: set[int] = set()
    for trade in trades:
        target_dir = normalize_direction(trade.direction)
        ref_entry = trade.entry_at or trade.close_at
        entry_candidates: list[tuple[float, int, JournalEntry]] = []
        for idx, entry in enumerate(journal.entries):
            if idx in used_entries:
                continue
            if target_dir and normalize_direction(entry.direction) != target_dir:
                continue
            if entry.ts > trade.close_at:
                continue
            if (trade.close_at - entry.ts).total_seconds() > JOURNAL_ENTRY_LOOKBACK_SECS:
                continue
            distance = abs((entry.ts - ref_entry).total_seconds())
            entry_candidates.append((-distance, idx, entry))
        if entry_candidates:
            _, idx, entry = max(entry_candidates, key=lambda item: item[0])
            used_entries.add(idx)
            trade.journal_entry = entry
        else:
            trade.gaps.append("no matched journal ENTRY")

        exit_candidates: list[tuple[int, float, int, JournalExit]] = []
        for idx, exit_event in enumerate(journal.exits):
            if idx in used_exits:
                continue
            distance = abs((exit_event.ts - trade.close_at).total_seconds())
            if distance > JOURNAL_EXIT_WINDOW_SECS:
                continue
            priority = 1 if exit_event.raw_type == "EXIT_CHECK" else 0
            exit_candidates.append((priority, -distance, idx, exit_event))
        if exit_candidates:
            _, _, idx, exit_event = max(exit_candidates, key=lambda item: (item[0], item[1]))
            used_exits.add(idx)
            trade.journal_exit = exit_event
        else:
            trade.gaps.append("no matched journal close reason")

        window_start = trade.journal_entry.ts if trade.journal_entry else (trade.entry_at or trade.close_at)
        window_end = trade.exit_at or trade.close_at
        trade.journal_markers = [
            marker for marker in journal.markers if window_start <= marker.ts <= window_end
        ]


def fmt_dt(dt: datetime | None) -> str:
    if dt is None:
        return "n/a"
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def fmt_money(value: Decimal | None) -> str:
    if value is None:
        return "n/a"
    return f"${value:+.4f}"


def fmt_decimal(value: Decimal | None, places: int = 3) -> str:
    if value is None:
        return "n/a"
    return f"{value:.{places}f}"


def fmt_secs(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.1f}"


def fmt_hours(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value / 3600.0:.2f}"


def marker_summary(markers: list[JournalMarker]) -> str:
    if not markers:
        return "none"
    counts: dict[str, int] = {}
    for marker in markers:
        counts[marker.label] = counts.get(marker.label, 0) + 1
    return ",".join(f"{key}:{counts[key]}" for key in sorted(counts))


def leg_for(trade: AttributedTrade, market: str) -> RealizedLeg | None:
    for leg in trade.legs:
        if leg.market == market:
            return leg
    return None


def fmt_leg_detail(leg: RealizedLeg | None) -> str:
    if leg is None:
        return "n/a"
    return f"{leg.side} {leg.size} {leg.entry_price}->{leg.exit_price}"


def leg_csv_value(leg: RealizedLeg | None, field: str) -> str:
    if leg is None:
        return ""
    value = getattr(leg, field)
    return str(value)


def gap_summary(trade: AttributedTrade) -> str:
    if not trade.gaps:
        return "ok"
    seen: list[str] = []
    for gap in trade.gaps:
        if gap not in seen:
            seen.append(gap)
    return "; ".join(seen)


def render_markdown(
    trades: list[AttributedTrade],
    dex_zip: Path,
    since: datetime,
    until: datetime,
    journal: JournalData,
    pnl_records: list[PnlRecord],
) -> str:
    complete = [t for t in trades if len(t.legs) == 2]
    legs = [leg for trade in trades for leg in trade.legs]
    total_realized = sum_decimal(leg.realised_pnl for leg in legs)
    total_trade = sum_decimal(leg.trade_pnl for leg in legs)
    total_funding = sum_decimal(leg.funding_fees for leg in legs)
    total_fees = sum_decimal(leg.trading_fees for leg in legs)
    matched_model = [trade for trade in trades if trade.model_pnl is not None]
    total_model = sum_decimal(t.model_pnl for t in matched_model if t.model_pnl is not None)
    total_leakage = sum_decimal(
        t.execution_leakage for t in matched_model if t.execution_leakage is not None
    )

    out: list[str] = []
    out.append("# Pairtrade Live Trade Attribution")
    out.append("")
    out.append("## Inputs")
    out.append("")
    out.append(f"- DEX ZIP: `{dex_zip}`")
    out.append(f"- Window UTC: `{fmt_dt(since)}` to `{fmt_dt(until)}`")
    out.append(f"- Journal lines parsed: {journal.line_count}")
    out.append(f"- PnL jsonl records parsed in window: {len(pnl_records)}")
    out.append("")
    out.append("## Summary")
    out.append("")
    out.append("| Metric | Value |")
    out.append("|---|---:|")
    out.append(f"| DEX legs | {len(legs)} |")
    out.append(f"| Complete round trips | {len(complete)} |")
    out.append(f"| Incomplete DEX groups | {len(trades) - len(complete)} |")
    out.append(f"| DEX realised PnL | {fmt_money(total_realized)} |")
    out.append(f"| DEX trade PnL | {fmt_money(total_trade)} |")
    out.append(f"| DEX funding | {fmt_money(total_funding)} |")
    out.append(f"| DEX trading fees | {fmt_money(total_fees)} |")
    if matched_model:
        out.append(f"| Matched bot/model PnL | {fmt_money(total_model)} |")
        out.append(f"| Realised minus model | {fmt_money(total_leakage)} |")
    else:
        out.append("| Matched bot/model PnL | n/a |")
        out.append("| Realised minus model | n/a |")
    out.append("")
    out.append("## Per Trade")
    out.append("")
    columns = [
        "#",
        "close_utc",
        "direction",
        "base_leg",
        "quote_leg",
        "dex_realized",
        "trade",
        "funding",
        "fees",
        "model",
        "leakage",
        "hold_h",
        "entry_sync_s",
        "exit_sync_s",
        "close_reason",
        "markers",
        "gaps",
    ]
    out.append("| " + " | ".join(columns) + " |")
    out.append("|" + "|".join("---" for _ in columns) + "|")
    for trade in trades:
        base_leg = leg_for(trade, trade.base_market)
        quote_leg = leg_for(trade, trade.quote_market)
        out.append(
            "| "
            + " | ".join(
                [
                    str(trade.index),
                    fmt_dt(trade.close_at),
                    trade.direction,
                    fmt_leg_detail(base_leg),
                    fmt_leg_detail(quote_leg),
                    fmt_money(trade.realised_pnl),
                    fmt_money(trade.trade_pnl),
                    fmt_money(trade.funding_fees),
                    fmt_money(trade.trading_fees),
                    fmt_money(trade.model_pnl),
                    fmt_money(trade.execution_leakage),
                    fmt_hours(trade.dex_hold_secs),
                    fmt_secs(trade.entry_sync_secs),
                    fmt_secs(trade.exit_sync_secs),
                    trade.close_reason,
                    marker_summary(trade.journal_markers),
                    gap_summary(trade),
                ]
            )
            + " |"
        )
    out.append("")
    out.append("## Explicit Gaps")
    out.append("")
    gaps = sorted({gap for trade in trades for gap in trade.gaps})
    if gaps:
        for gap in gaps:
            out.append(f"- {gap}")
    else:
        out.append("- none")
    if not matched_model:
        out.append("- Signal/model PnL is not populated because no pnl jsonl or BT model row matched this window.")
    if journal.line_count == 0:
        out.append("- Close reason and partial-fill markers require a journal fragment.")
    out.append("")
    out.append("## Notes")
    out.append("")
    out.append("- DEX realised PnL is the live accounting source of truth.")
    out.append("- `model` is populated only from matched pairtrade pnl jsonl records; it is not minimum-exec BT.")
    out.append("- `leakage` is `DEX realised - model` and remains n/a when model PnL is unavailable.")
    return "\n".join(out)


def write_csv(path: Path, trades: list[AttributedTrade]) -> None:
    fields = [
        "index",
        "venue",
        "variant",
        "pair",
        "direction",
        "entry_utc",
        "close_utc",
        "base_market",
        "base_side",
        "base_size",
        "base_entry_price",
        "base_exit_price",
        "quote_market",
        "quote_side",
        "quote_size",
        "quote_entry_price",
        "quote_exit_price",
        "dex_realized",
        "trade_pnl",
        "funding_fees",
        "trading_fees",
        "model_pnl",
        "execution_leakage",
        "hold_secs",
        "entry_sync_secs",
        "exit_sync_secs",
        "close_reason",
        "markers",
        "gaps",
    ]
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for trade in trades:
            base_leg = leg_for(trade, trade.base_market)
            quote_leg = leg_for(trade, trade.quote_market)
            writer.writerow(
                {
                    "index": trade.index,
                    "venue": trade.venue,
                    "variant": trade.variant,
                    "pair": trade.pair,
                    "direction": trade.direction,
                    "entry_utc": fmt_dt(trade.entry_at),
                    "close_utc": fmt_dt(trade.close_at),
                    "base_market": trade.base_market,
                    "base_side": leg_csv_value(base_leg, "side"),
                    "base_size": leg_csv_value(base_leg, "size"),
                    "base_entry_price": leg_csv_value(base_leg, "entry_price"),
                    "base_exit_price": leg_csv_value(base_leg, "exit_price"),
                    "quote_market": trade.quote_market,
                    "quote_side": leg_csv_value(quote_leg, "side"),
                    "quote_size": leg_csv_value(quote_leg, "size"),
                    "quote_entry_price": leg_csv_value(quote_leg, "entry_price"),
                    "quote_exit_price": leg_csv_value(quote_leg, "exit_price"),
                    "dex_realized": str(trade.realised_pnl),
                    "trade_pnl": str(trade.trade_pnl),
                    "funding_fees": str(trade.funding_fees),
                    "trading_fees": str(trade.trading_fees),
                    "model_pnl": "" if trade.model_pnl is None else str(trade.model_pnl),
                    "execution_leakage": ""
                    if trade.execution_leakage is None
                    else str(trade.execution_leakage),
                    "hold_secs": "" if trade.dex_hold_secs is None else f"{trade.dex_hold_secs:.3f}",
                    "entry_sync_secs": ""
                    if trade.entry_sync_secs is None
                    else f"{trade.entry_sync_secs:.3f}",
                    "exit_sync_secs": ""
                    if trade.exit_sync_secs is None
                    else f"{trade.exit_sync_secs:.3f}",
                    "close_reason": trade.close_reason,
                    "markers": marker_summary(trade.journal_markers),
                    "gaps": gap_summary(trade),
                }
            )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--dex-zip", required=True, type=Path, help="DEX export zip with trades.csv and realized_pnl.csv")
    parser.add_argument("--since", required=True, help="UTC ISO start, inclusive")
    parser.add_argument("--until", required=True, help="UTC ISO end, exclusive")
    parser.add_argument("--pair", default="BTC/ETH", help="Pair symbol, default BTC/ETH")
    parser.add_argument("--venue", default=None, help="Venue label for the report")
    parser.add_argument("--variant", default=None, help="Variant label for the report")
    parser.add_argument("--journal-log", type=Path, default=None, help="Optional journalctl grep output")
    parser.add_argument(
        "--pnl-jsonl",
        action="append",
        default=[],
        help="Optional pnl jsonl file, directory, or glob. Repeatable.",
    )
    parser.add_argument("--group-window-secs", type=float, default=10.0)
    parser.add_argument("--match-window-secs", type=float, default=ENTRY_EXIT_MATCH_SECS)
    parser.add_argument("--report-out", type=Path, default=None)
    parser.add_argument("--csv-out", type=Path, default=None)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    dex_zip = expand_path(args.dex_zip)
    since = parse_ts(args.since)
    until = parse_ts(args.until)
    if since >= until:
        print("--since must be earlier than --until", file=sys.stderr)
        return 2

    base, quote = parse_pair(args.pair)
    base_market = market_for(base)
    quote_market = market_for(quote)
    venue = infer_venue(dex_zip, args.venue)
    variant = infer_variant(args.variant, venue)

    legs = load_realized_legs(dex_zip)
    fills = load_fills(dex_zip)
    trades = group_realized_trades(
        legs=legs,
        since=since,
        until=until,
        venue=venue,
        variant=variant,
        pair=f"{base}/{quote}",
        base_market=base_market,
        quote_market=quote_market,
        group_window_secs=args.group_window_secs,
    )
    attach_fills(trades, fills, args.match_window_secs)

    pnl_records = load_pnl_records(args.pnl_jsonl, since, until)
    attach_pnl_records(trades, pnl_records, args.match_window_secs)

    journal = load_journal(args.journal_log, since, until)
    attach_journal(trades, journal)

    report = render_markdown(trades, dex_zip, since, until, journal, pnl_records)
    if args.report_out:
        report_path = expand_path(args.report_out)
        report_path.write_text(report + "\n")
    print(report)

    if args.csv_out:
        write_csv(expand_path(args.csv_out), trades)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
