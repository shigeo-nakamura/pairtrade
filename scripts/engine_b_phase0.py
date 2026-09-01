#!/usr/bin/env python3
"""Engine B Phase 0A public-data observer (bot-strategy#866).

This process has no account authentication, signing, or order capability.  Its
only outbound WebSocket messages are public-data subscriptions and keepalives.
It records Robinhood Lighter (the intended future execution venue) alongside a
complete standard-Lighter context feed because Robinhood currently lacks EWY
and USDKRW, two controls required by requirements v0.3.

The absence of those same-venue controls and the unresolved exchange calendar
are recorded as fail-closed session-invalid reasons.  Collection can begin,
but these rows must not be counted as valid Phase 0A samples yet.
"""

from __future__ import annotations

import argparse
import asyncio
import fcntl
import hashlib
import json
import logging
import os
import signal
import sqlite3
import sys
import time
import uuid
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date, datetime, time as datetime_time, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from urllib.error import URLError
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from websockets.asyncio.client import connect


LOG = logging.getLogger("engine_b_phase0")
UTC = timezone.utc
ALLOWED_MESSAGE_TYPES = frozenset({"subscribe", "unsubscribe", "ping", "pong"})
ALLOWED_CHANNEL_PREFIXES = frozenset({"order_book", "trade", "market_stats"})
SCHEMA_VERSION = 3
OHLCV_FINALIZE_GRACE_US = 120_000_000


SCHEMA = """
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS schema_metadata (
  schema_version INTEGER PRIMARY KEY,
  created_ts_us INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS collector_manifest (
  collector_run_id TEXT PRIMARY KEY,
  started_ts_us INTEGER NOT NULL,
  document_version TEXT NOT NULL,
  collector_version TEXT NOT NULL,
  code_commit TEXT NOT NULL,
  config_hash TEXT NOT NULL,
  dependency_lock_hash TEXT NOT NULL,
  api_schema_version TEXT NOT NULL,
  order_capability INTEGER NOT NULL CHECK(order_capability = 0),
  config_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ws_connection (
  connection_session_id TEXT PRIMARY KEY,
  venue TEXT NOT NULL,
  channel TEXT NOT NULL,
  started_ts_recv_us INTEGER NOT NULL,
  ended_ts_recv_us INTEGER,
  api_schema_version TEXT NOT NULL,
  end_reason TEXT
);

CREATE TABLE IF NOT EXISTS book_event (
  book_event_id INTEGER PRIMARY KEY AUTOINCREMENT,
  connection_session_id TEXT NOT NULL,
  venue TEXT NOT NULL,
  market_id INTEGER NOT NULL,
  symbol TEXT NOT NULL,
  event_kind TEXT NOT NULL,
  exchange_sequence TEXT,
  begin_sequence TEXT,
  exchange_offset TEXT,
  local_sequence INTEGER NOT NULL,
  ts_recv_us INTEGER NOT NULL,
  ts_srv_us INTEGER,
  is_complete_snapshot INTEGER NOT NULL,
  api_schema_version TEXT NOT NULL,
  UNIQUE(connection_session_id, market_id, local_sequence),
  FOREIGN KEY(connection_session_id)
    REFERENCES ws_connection(connection_session_id)
);

CREATE INDEX IF NOT EXISTS idx_book_event_venue_symbol_time
  ON book_event(venue, symbol, ts_recv_us);

CREATE TABLE IF NOT EXISTS book_level (
  book_event_id INTEGER NOT NULL,
  side TEXT NOT NULL,
  level INTEGER NOT NULL,
  price TEXT NOT NULL,
  size TEXT NOT NULL,
  PRIMARY KEY(book_event_id, side, level),
  FOREIGN KEY(book_event_id) REFERENCES book_event(book_event_id)
);

CREATE TABLE IF NOT EXISTS trade (
  trade_row_id INTEGER PRIMARY KEY AUTOINCREMENT,
  connection_session_id TEXT NOT NULL,
  venue TEXT NOT NULL,
  market_id INTEGER NOT NULL,
  symbol TEXT NOT NULL,
  exchange_trade_id TEXT,
  exchange_sequence TEXT,
  local_sequence INTEGER NOT NULL,
  ts_recv_us INTEGER NOT NULL,
  ts_srv_us INTEGER,
  price TEXT NOT NULL,
  size TEXT NOT NULL,
  aggressor_side TEXT,
  raw_public_json TEXT NOT NULL,
  UNIQUE(connection_session_id, market_id, local_sequence),
  UNIQUE(venue, market_id, exchange_trade_id),
  FOREIGN KEY(connection_session_id)
    REFERENCES ws_connection(connection_session_id)
);

CREATE INDEX IF NOT EXISTS idx_trade_venue_symbol_time
  ON trade(venue, symbol, ts_recv_us);

CREATE TABLE IF NOT EXISTS late_trade (
  late_trade_row_id INTEGER PRIMARY KEY AUTOINCREMENT,
  connection_session_id TEXT NOT NULL,
  venue TEXT NOT NULL,
  market_id INTEGER NOT NULL,
  symbol TEXT NOT NULL,
  exchange_trade_id TEXT NOT NULL,
  exchange_sequence TEXT,
  local_sequence INTEGER NOT NULL,
  ts_recv_us INTEGER NOT NULL,
  ts_srv_us INTEGER,
  event_ts_us INTEGER NOT NULL,
  sealed_partition TEXT NOT NULL,
  price TEXT NOT NULL,
  size TEXT NOT NULL,
  aggressor_side TEXT,
  raw_public_json TEXT NOT NULL,
  UNIQUE(venue, market_id, exchange_trade_id, sealed_partition),
  FOREIGN KEY(connection_session_id)
    REFERENCES ws_connection(connection_session_id)
);

CREATE INDEX IF NOT EXISTS idx_late_trade_venue_symbol_time
  ON late_trade(venue, symbol, ts_recv_us);

CREATE TABLE IF NOT EXISTS ohlcv_1m (
  bucket_start_us INTEGER NOT NULL,
  venue TEXT NOT NULL,
  market_id INTEGER NOT NULL,
  symbol TEXT NOT NULL,
  source TEXT NOT NULL,
  open TEXT,
  high TEXT,
  low TEXT,
  close TEXT,
  volume TEXT NOT NULL,
  trade_count INTEGER NOT NULL,
  is_complete INTEGER NOT NULL,
  first_trade_ts_us INTEGER NOT NULL,
  last_trade_ts_us INTEGER NOT NULL,
  PRIMARY KEY(bucket_start_us, venue, market_id, source)
);

CREATE TABLE IF NOT EXISTS funding (
  observed_ts_us INTEGER NOT NULL,
  effective_ts_us INTEGER,
  venue TEXT NOT NULL,
  market_id INTEGER NOT NULL,
  symbol TEXT NOT NULL,
  rate TEXT NOT NULL,
  source TEXT NOT NULL,
  PRIMARY KEY(observed_ts_us, venue, market_id, source)
);

CREATE TABLE IF NOT EXISTS price_observation (
  observed_ts_us INTEGER NOT NULL,
  ts_srv_us INTEGER,
  venue TEXT NOT NULL,
  market_id INTEGER NOT NULL,
  symbol TEXT NOT NULL,
  price_type TEXT NOT NULL,
  price TEXT NOT NULL,
  source TEXT NOT NULL,
  PRIMARY KEY(observed_ts_us, venue, market_id, price_type, source)
);

CREATE TABLE IF NOT EXISTS data_gap (
  gap_id INTEGER PRIMARY KEY AUTOINCREMENT,
  connection_session_id TEXT,
  venue TEXT NOT NULL,
  market_id INTEGER,
  symbol TEXT,
  channel TEXT NOT NULL,
  ts_start_us INTEGER NOT NULL,
  ts_end_us INTEGER,
  expected_sequence TEXT,
  observed_sequence TEXT,
  reason TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS market_status (
  observed_ts_us INTEGER NOT NULL,
  venue TEXT NOT NULL,
  market_id INTEGER NOT NULL,
  symbol TEXT NOT NULL,
  status TEXT NOT NULL,
  force_reduce_only INTEGER NOT NULL,
  daily_volume_usd TEXT NOT NULL,
  open_interest TEXT,
  is_eligible INTEGER NOT NULL,
  eligibility_reason TEXT NOT NULL,
  PRIMARY KEY(observed_ts_us, venue, market_id)
);

CREATE TABLE IF NOT EXISTS market_metadata (
  observed_ts_us INTEGER NOT NULL,
  venue TEXT NOT NULL,
  market_id INTEGER NOT NULL,
  symbol TEXT NOT NULL,
  public_json TEXT NOT NULL,
  PRIMARY KEY(observed_ts_us, venue, market_id)
);

CREATE TABLE IF NOT EXISTS trading_session (
  session_id TEXT PRIMARY KEY,
  krx_business_date TEXT NOT NULL,
  t0_us INTEGER NOT NULL,
  t1_us INTEGER NOT NULL,
  t2_us INTEGER NOT NULL,
  krx_is_open INTEGER NOT NULL,
  us_cash_is_open INTEGER NOT NULL,
  calendar_version TEXT NOT NULL,
  validity_reason TEXT
);

CREATE TABLE IF NOT EXISTS daily_data_quality (
  session_id TEXT NOT NULL,
  venue TEXT NOT NULL,
  market_id INTEGER NOT NULL,
  event_count INTEGER NOT NULL,
  missing_duration_us INTEGER NOT NULL,
  out_of_order_count INTEGER NOT NULL,
  duplicate_count INTEGER NOT NULL,
  sequence_gap_count INTEGER NOT NULL,
  stale_quote_duration_us INTEGER NOT NULL,
  crossed_book_duration_us INTEGER NOT NULL,
  reconnect_count INTEGER NOT NULL,
  max_clock_offset_us INTEGER,
  is_valid INTEGER NOT NULL,
  invalid_reason TEXT,
  PRIMARY KEY(session_id, venue, market_id)
);

CREATE TABLE IF NOT EXISTS analysis_run (
  analysis_run_id TEXT PRIMARY KEY,
  created_ts_us INTEGER NOT NULL,
  phase_role TEXT NOT NULL,
  model_version TEXT NOT NULL,
  code_commit TEXT NOT NULL,
  config_hash TEXT NOT NULL,
  dataset_hash TEXT NOT NULL,
  calendar_version TEXT NOT NULL,
  start_session_id TEXT NOT NULL,
  end_session_id TEXT NOT NULL
);
"""


def now_us() -> int:
    return time.time_ns() // 1_000


def partition_for_us(timestamp_us: int) -> str:
    return datetime.fromtimestamp(timestamp_us / 1_000_000, UTC).strftime("%Y%m%d_%H")


def normalize_exchange_timestamp_us(value: Any) -> int | None:
    """Normalize documented millisecond/microsecond timestamps to microseconds."""
    if value is None:
        return None
    try:
        raw = int(value)
    except (TypeError, ValueError):
        return None
    if raw <= 0:
        return None
    if raw < 100_000_000_000_000:
        return raw * 1_000
    return raw


def canonical_decimal(value: Any) -> str:
    if value is None:
        raise ValueError("decimal value is missing")
    try:
        parsed = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError(f"invalid decimal: {value!r}") from exc
    if not parsed.is_finite():
        raise ValueError(f"non-finite decimal: {value!r}")
    text = format(parsed, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return "0" if text == "-0" else text


def parse_market_id(channel: str) -> int:
    tail = channel.rsplit(":", 1)[-1] if ":" in channel else channel.rsplit("/", 1)[-1]
    return int(tail)


@dataclass(frozen=True)
class MarketConfig:
    symbol: str
    market_id: int
    role: str


@dataclass(frozen=True)
class VenueConfig:
    name: str
    rest_url: str
    ws_url: str
    role: str
    required_same_venue_symbols: tuple[str, ...]
    known_missing_symbols: tuple[str, ...]
    markets: tuple[MarketConfig, ...]

    @property
    def market_by_id(self) -> dict[int, MarketConfig]:
        return {market.market_id: market for market in self.markets}


@dataclass(frozen=True)
class AppConfig:
    raw: dict[str, Any]
    raw_json: str
    config_hash: str
    dependency_lock_hash: str
    document_version: str
    collector_version: str
    api_schema_version: str
    database_dir: Path
    health_file: Path
    metrics_host: str
    metrics_port: int
    top_levels: int
    reconstructed_snapshot_interval_ms: int
    rest_poll_seconds: int
    min_daily_volume_usd: Decimal
    queue_maxsize: int
    db_batch_max: int
    db_flush_interval_ms: int
    venues: tuple[VenueConfig, ...]


def load_config(path: Path, dependency_lock_path: Path) -> AppConfig:
    raw_bytes = path.read_bytes()
    raw = json.loads(raw_bytes)
    lock_bytes = dependency_lock_path.read_bytes()
    venues = []
    seen_venue_names: set[str] = set()
    for item in raw["venues"]:
        markets = tuple(MarketConfig(**market) for market in item["markets"])
        market_ids = [market.market_id for market in markets]
        symbols = [market.symbol for market in markets]
        if len(set(market_ids)) != len(market_ids) or len(set(symbols)) != len(symbols):
            raise ValueError(f"duplicate market id/symbol in venue {item['name']}")
        if item["name"] in seen_venue_names:
            raise ValueError(f"duplicate venue name: {item['name']}")
        seen_venue_names.add(item["name"])
        required = set(item["required_same_venue_symbols"])
        configured = set(symbols)
        missing = required - configured
        if missing != set(item["known_missing_symbols"]):
            raise ValueError(
                f"{item['name']} missing-symbol declaration differs: "
                f"computed={sorted(missing)} declared={sorted(item['known_missing_symbols'])}"
            )
        venues.append(
            VenueConfig(
                name=item["name"],
                rest_url=item["rest_url"].rstrip("/"),
                ws_url=item["ws_url"],
                role=item["role"],
                required_same_venue_symbols=tuple(item["required_same_venue_symbols"]),
                known_missing_symbols=tuple(item["known_missing_symbols"]),
                markets=markets,
            )
        )
    if int(raw["top_levels"]) < 5:
        raise ValueError("top_levels must be at least 5")
    host, port_text = raw["metrics_listen"].rsplit(":", 1)
    return AppConfig(
        raw=raw,
        raw_json=json.dumps(raw, sort_keys=True, separators=(",", ":")),
        config_hash=hashlib.sha256(raw_bytes).hexdigest(),
        dependency_lock_hash=hashlib.sha256(lock_bytes).hexdigest(),
        document_version=str(raw["document_version"]),
        collector_version=str(raw["collector_version"]),
        api_schema_version=str(raw["api_schema_version"]),
        database_dir=Path(raw["database_dir"]),
        health_file=Path(raw["health_file"]),
        metrics_host=host,
        metrics_port=int(port_text),
        top_levels=int(raw["top_levels"]),
        reconstructed_snapshot_interval_ms=int(raw["reconstructed_snapshot_interval_ms"]),
        rest_poll_seconds=int(raw["rest_poll_seconds"]),
        min_daily_volume_usd=Decimal(str(raw["min_daily_volume_usd"])),
        queue_maxsize=int(raw["queue_maxsize"]),
        db_batch_max=int(raw["db_batch_max"]),
        db_flush_interval_ms=int(raw["db_flush_interval_ms"]),
        venues=tuple(venues),
    )


@dataclass
class BookState:
    bids: dict[str, str] = field(default_factory=dict)
    asks: dict[str, str] = field(default_factory=dict)
    last_nonce: str | None = None
    synced: bool = False
    last_reconstructed_us: int = 0

    def apply_snapshot(self, payload: dict[str, Any]) -> None:
        self.bids = self._levels_to_map(payload.get("bids", []))
        self.asks = self._levels_to_map(payload.get("asks", []))
        self.last_nonce = self._sequence(payload.get("nonce"))
        self.synced = True

    def apply_delta(self, payload: dict[str, Any]) -> tuple[bool, str | None, str | None]:
        begin_nonce = self._sequence(payload.get("begin_nonce"))
        expected = self.last_nonce
        if not self.synced or (begin_nonce is not None and expected is not None and begin_nonce != expected):
            self.synced = False
            self.bids.clear()
            self.asks.clear()
            return False, expected, begin_nonce
        self._apply_levels(self.bids, payload.get("bids", []))
        self._apply_levels(self.asks, payload.get("asks", []))
        self.last_nonce = self._sequence(payload.get("nonce")) or self.last_nonce
        return True, expected, begin_nonce

    def reconstructed_levels(self, top_levels: int) -> list[tuple[str, int, str, str]]:
        bids = sorted(self.bids.items(), key=lambda item: Decimal(item[0]), reverse=True)[:top_levels]
        asks = sorted(self.asks.items(), key=lambda item: Decimal(item[0]))[:top_levels]
        rows = [("bid", index, price, size) for index, (price, size) in enumerate(bids)]
        rows.extend(("ask", index, price, size) for index, (price, size) in enumerate(asks))
        return rows

    @staticmethod
    def raw_levels(payload: dict[str, Any]) -> list[tuple[str, int, str, str]]:
        rows: list[tuple[str, int, str, str]] = []
        for side_key, side in (("bids", "bid"), ("asks", "ask")):
            for index, level in enumerate(payload.get(side_key, [])):
                rows.append(
                    (side, index, canonical_decimal(level["price"]), canonical_decimal(level["size"]))
                )
        return rows

    @staticmethod
    def _sequence(value: Any) -> str | None:
        return None if value is None else str(value)

    @staticmethod
    def _levels_to_map(levels: list[dict[str, Any]]) -> dict[str, str]:
        state: dict[str, str] = {}
        BookState._apply_levels(state, levels)
        return state

    @staticmethod
    def _apply_levels(state: dict[str, str], levels: list[dict[str, Any]]) -> None:
        for level in levels:
            price = canonical_decimal(level["price"])
            size = canonical_decimal(level["size"])
            if Decimal(size) == 0:
                state.pop(price, None)
            else:
                state[price] = size



class Metrics:
    def __init__(self) -> None:
        self.started_us = now_us()
        self.counters: defaultdict[tuple[str, tuple[tuple[str, str], ...]], int] = defaultdict(int)
        self.gauges: dict[tuple[str, tuple[tuple[str, str], ...]], float] = {}
        self.last_message_us: dict[str, int] = {}
        self.feed_connected: dict[str, bool] = {}
        self.book_synced: dict[tuple[str, int], bool] = {}
        self.last_rest_success_us: dict[str, int] = {}

    @staticmethod
    def _key(name: str, labels: dict[str, Any]) -> tuple[str, tuple[tuple[str, str], ...]]:
        return name, tuple(sorted((key, str(value)) for key, value in labels.items()))

    def inc(self, name: str, labels: dict[str, Any], amount: int = 1) -> None:
        self.counters[self._key(name, labels)] += amount

    def set_gauge(self, name: str, labels: dict[str, Any], value: float) -> None:
        self.gauges[self._key(name, labels)] = value

    @staticmethod
    def _format(name: str, labels: tuple[tuple[str, str], ...], value: Any) -> str:
        label_text = ""
        if labels:
            escaped = [f'{key}="{val.replace(chr(92), chr(92) * 2).replace(chr(34), chr(92) + chr(34))}"' for key, val in labels]
            label_text = "{" + ",".join(escaped) + "}"
        return f"{name}{label_text} {value}"

    def render(self, queue_size: int) -> str:
        current_us = now_us()
        lines = [
            "# TYPE engine_b_phase0_order_capability gauge",
            "engine_b_phase0_order_capability 0",
            "# TYPE engine_b_phase0_uptime_seconds gauge",
            f"engine_b_phase0_uptime_seconds {(current_us - self.started_us) / 1_000_000:.3f}",
            "# TYPE engine_b_phase0_db_queue_size gauge",
            f"engine_b_phase0_db_queue_size {queue_size}",
        ]
        for venue, timestamp_us in sorted(self.last_message_us.items()):
            age = max(0.0, (current_us - timestamp_us) / 1_000_000)
            lines.append(self._format("engine_b_phase0_last_message_age_seconds", (("venue", venue),), age))
        for venue, connected in sorted(self.feed_connected.items()):
            lines.append(self._format("engine_b_phase0_feed_connected", (("venue", venue),), int(connected)))
        for (venue, market_id), synced in sorted(self.book_synced.items()):
            lines.append(
                self._format(
                    "engine_b_phase0_book_synced",
                    (("market_id", str(market_id)), ("venue", venue)),
                    int(synced),
                )
            )
        for key, value in sorted(self.counters.items()):
            lines.append(self._format(key[0], key[1], value))
        for key, value in sorted(self.gauges.items()):
            lines.append(self._format(key[0], key[1], value))
        return "\n".join(lines) + "\n"


class DatabaseSink:
    def __init__(self, config: AppConfig, collector_run_id: str, code_commit: str) -> None:
        self.config = config
        self.collector_run_id = collector_run_id
        self.code_commit = code_commit
        self.queue: asyncio.Queue[tuple[str, dict[str, Any]]] = asyncio.Queue(
            maxsize=config.queue_maxsize
        )
        self._connections: dict[str, sqlite3.Connection] = {}
        self.state_dir = config.database_dir.parent
        self.sealed_dir = self.state_dir / "sealed"
        self.lock_dir = self.state_dir / "locks"
        self._task: asyncio.Task[None] | None = None
        self._stopping = False

    def start(self) -> None:
        for directory in (self.config.database_dir, self.sealed_dir, self.lock_dir):
            directory.mkdir(parents=True, exist_ok=True)
            os.chmod(directory, 0o750)
        self._task = asyncio.create_task(self._run(), name="sqlite-writer")

    async def put(self, kind: str, payload: dict[str, Any]) -> None:
        if self._stopping:
            raise RuntimeError("database sink is stopping")
        await self.queue.put((kind, payload))

    async def close(self) -> None:
        self._stopping = True
        try:
            if self._task is not None:
                if not self._task.done():
                    await self.queue.put(("__stop__", {"recv_us": now_us()}))
                await self._task
        finally:
            for connection in self._connections.values():
                connection.close()
            self._connections.clear()

    async def _run(self) -> None:
        stop_after_batch = False
        while not stop_after_batch:
            first = await self.queue.get()
            batch = [first]
            stop_after_batch = first[0] == "__stop__"
            deadline = asyncio.get_running_loop().time() + self.config.db_flush_interval_ms / 1000
            while len(batch) < self.config.db_batch_max and not stop_after_batch:
                timeout = deadline - asyncio.get_running_loop().time()
                if timeout <= 0:
                    break
                try:
                    item = await asyncio.wait_for(self.queue.get(), timeout)
                except TimeoutError:
                    break
                batch.append(item)
                stop_after_batch = item[0] == "__stop__"
            await asyncio.to_thread(self._write_batch, batch)
            for _ in batch:
                self.queue.task_done()

    def _connection(self, partition: str) -> sqlite3.Connection:
        if self._is_partition_sealed(partition):
            raise RuntimeError(f"refusing to open sealed partition: {partition}")
        connection = self._connections.get(partition)
        if connection is not None:
            return connection
        path = self.config.database_dir / f"engine_b_phase0_{partition}.sqlite3"
        connection = sqlite3.connect(path, timeout=30, check_same_thread=False)
        connection.executescript(SCHEMA)
        self._migrate(connection)
        started_us = now_us()
        connection.execute(
            "INSERT OR IGNORE INTO schema_metadata(schema_version, created_ts_us) VALUES (?, ?)",
            (SCHEMA_VERSION, started_us),
        )
        connection.execute(
            """INSERT OR IGNORE INTO collector_manifest(
                 collector_run_id, started_ts_us, document_version, collector_version,
                 code_commit, config_hash, dependency_lock_hash, api_schema_version,
                 order_capability, config_json
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?)""",
            (
                self.collector_run_id,
                started_us,
                self.config.document_version,
                self.config.collector_version,
                self.code_commit,
                self.config.config_hash,
                self.config.dependency_lock_hash,
                self.config.api_schema_version,
                self.config.raw_json,
            ),
        )
        connection.commit()
        self._connections[partition] = connection
        return connection

    def _migrate(self, connection: sqlite3.Connection) -> None:
        columns = {
            row[1] for row in connection.execute("PRAGMA table_info(ohlcv_1m)")
        }
        if "first_trade_ts_us" not in columns:
            connection.execute("ALTER TABLE ohlcv_1m ADD COLUMN first_trade_ts_us INTEGER")
        if "last_trade_ts_us" not in columns:
            connection.execute("ALTER TABLE ohlcv_1m ADD COLUMN last_trade_ts_us INTEGER")
        connection.execute(
            """UPDATE ohlcv_1m
               SET first_trade_ts_us = COALESCE(first_trade_ts_us, bucket_start_us),
                   last_trade_ts_us = COALESCE(last_trade_ts_us, bucket_start_us)
               WHERE first_trade_ts_us IS NULL OR last_trade_ts_us IS NULL"""
        )

    @staticmethod
    def _ensure_connection(connection: sqlite3.Connection, payload: dict[str, Any]) -> None:
        meta = payload.get("connection")
        if not meta:
            return
        connection.execute(
            """INSERT OR IGNORE INTO ws_connection(
                 connection_session_id, venue, channel, started_ts_recv_us,
                 api_schema_version
               ) VALUES (?, ?, ?, ?, ?)""",
            (
                meta["id"],
                meta["venue"],
                "multiplexed_public",
                meta["started_us"],
                meta["api_schema_version"],
            ),
        )

    def _is_partition_sealed(self, partition: str) -> bool:
        return (self.sealed_dir / f"{partition}.json").is_file()

    @contextmanager
    def _partition_lock(self, partition: str):
        lock_path = self.lock_dir / f"{partition}.lock"
        with lock_path.open("a+") as lock_file:
            os.chmod(lock_path, 0o640)
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)

    def _write_partition(
        self, partition: str, commands: list[tuple[str, dict[str, Any]]]
    ) -> None:
        connection = self._connection(partition)
        try:
            connection.execute("BEGIN")
            for kind, payload in commands:
                self._ensure_connection(connection, payload)
                self._apply(connection, kind, payload)
            connection.commit()
        except Exception:
            connection.rollback()
            raise

    def _write_batch(self, batch: list[tuple[str, dict[str, Any]]]) -> None:
        grouped: defaultdict[str, list[tuple[str, dict[str, Any]]]] = defaultdict(list)
        for kind, payload in batch:
            if kind == "__stop__":
                continue
            partition_us = int(payload.get("partition_us", payload["recv_us"]))
            grouped[partition_for_us(partition_us)].append((kind, payload))

        late_commands: list[tuple[str, dict[str, Any]]] = []
        for partition, commands in grouped.items():
            with self._partition_lock(partition):
                if self._is_partition_sealed(partition):
                    regular_commands = []
                    for kind, payload in commands:
                        if kind != "trade":
                            raise RuntimeError(
                                f"non-trade command targeted sealed partition {partition}: {kind}"
                            )
                        late_payload = dict(payload)
                        late_payload["sealed_partition"] = partition
                        late_commands.append(("late_trade", late_payload))
                    commands = regular_commands
                if commands:
                    self._write_partition(partition, commands)

        if late_commands:
            late_partition = partition_for_us(now_us())
            with self._partition_lock(late_partition):
                if self._is_partition_sealed(late_partition):
                    raise RuntimeError(f"active late-trade partition is sealed: {late_partition}")
                self._write_partition(late_partition, late_commands)

        for partition, connection in list(self._connections.items()):
            with self._partition_lock(partition):
                self._finalize_ohlcv(connection, now_us())
                connection.commit()
        current_partition = partition_for_us(now_us())
        for partition in list(self._connections):
            if partition < current_partition:
                self._connections.pop(partition).close()

    @staticmethod
    def _finalize_ohlcv(connection: sqlite3.Connection, observed_us: int) -> None:
        final_bucket_cutoff = observed_us - OHLCV_FINALIZE_GRACE_US - 60_000_000
        connection.execute(
            """UPDATE ohlcv_1m SET is_complete = 1
               WHERE is_complete = 0 AND bucket_start_us <= ?""",
            (final_bucket_cutoff,),
        )

    @staticmethod
    def _merge_ohlcv_trade(connection: sqlite3.Connection, payload: dict[str, Any]) -> None:
        source = "event_derived"
        key = (
            payload["bucket_start_us"],
            payload["venue"],
            payload["market_id"],
            source,
        )
        existing = connection.execute(
            """SELECT open, high, low, close, volume, trade_count,
                      first_trade_ts_us, last_trade_ts_us
               FROM ohlcv_1m
               WHERE bucket_start_us = ? AND venue = ? AND market_id = ? AND source = ?""",
            key,
        ).fetchone()
        price = Decimal(payload["price"])
        size = Decimal(payload["size"])
        event_ts_us = int(payload["event_ts_us"])
        if existing is None:
            price_text = format(price, "f")
            connection.execute(
                """INSERT INTO ohlcv_1m(
                     bucket_start_us, venue, market_id, symbol, source, open, high,
                     low, close, volume, trade_count, first_trade_ts_us,
                     last_trade_ts_us, is_complete
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, 0)""",
                (
                    payload["bucket_start_us"], payload["venue"], payload["market_id"],
                    payload["symbol"], source, price_text, price_text, price_text,
                    price_text, format(size, "f"), event_ts_us, event_ts_us,
                ),
            )
            return

        open_price, high, low, close_price, volume, trade_count, first_ts, last_ts = existing
        if event_ts_us < int(first_ts):
            open_price, first_ts = format(price, "f"), event_ts_us
        if event_ts_us >= int(last_ts):
            close_price, last_ts = format(price, "f"), event_ts_us
        connection.execute(
            """UPDATE ohlcv_1m
               SET open = ?, high = ?, low = ?, close = ?, volume = ?,
                   trade_count = ?, first_trade_ts_us = ?, last_trade_ts_us = ?,
                   is_complete = 0
               WHERE bucket_start_us = ? AND venue = ? AND market_id = ? AND source = ?""",
            (
                open_price,
                format(max(Decimal(high), price), "f"),
                format(min(Decimal(low), price), "f"),
                close_price,
                format(Decimal(volume) + size, "f"),
                int(trade_count) + 1,
                first_ts,
                last_ts,
                *key,
            ),
        )

    def _apply(self, connection: sqlite3.Connection, kind: str, payload: dict[str, Any]) -> None:
        if kind == "connection_start":
            self._ensure_connection(connection, payload)
        elif kind == "connection_end":
            connection.execute(
                """UPDATE ws_connection SET ended_ts_recv_us = ?, end_reason = ?
                   WHERE connection_session_id = ?""",
                (payload["recv_us"], payload["reason"], payload["connection"]["id"]),
            )
        elif kind == "book":
            cursor = connection.execute(
                """INSERT INTO book_event(
                     connection_session_id, venue, market_id, symbol, event_kind,
                     exchange_sequence, begin_sequence, exchange_offset, local_sequence,
                     ts_recv_us, ts_srv_us, is_complete_snapshot, api_schema_version
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    payload["connection"]["id"], payload["venue"], payload["market_id"],
                    payload["symbol"], payload["event_kind"], payload.get("exchange_sequence"),
                    payload.get("begin_sequence"), payload.get("exchange_offset"),
                    payload["local_sequence"], payload["recv_us"], payload.get("srv_us"),
                    int(payload["complete"]), self.config.api_schema_version,
                ),
            )
            event_id = cursor.lastrowid
            connection.executemany(
                "INSERT INTO book_level(book_event_id, side, level, price, size) VALUES (?, ?, ?, ?, ?)",
                [(event_id, *level) for level in payload["levels"]],
            )
        elif kind == "late_trade":
            connection.execute(
                """INSERT OR IGNORE INTO late_trade(
                     connection_session_id, venue, market_id, symbol, exchange_trade_id,
                     exchange_sequence, local_sequence, ts_recv_us, ts_srv_us,
                     event_ts_us, sealed_partition, price, size, aggressor_side,
                     raw_public_json
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    payload["connection"]["id"], payload["venue"], payload["market_id"],
                    payload["symbol"], payload["trade_id"], payload.get("exchange_sequence"),
                    payload["local_sequence"], payload["recv_us"], payload.get("srv_us"),
                    payload["event_ts_us"], payload["sealed_partition"], payload["price"],
                    payload["size"], payload.get("aggressor_side"), payload["raw_public_json"],
                ),
            )
        elif kind == "trade":
            cursor = connection.execute(
                """INSERT OR IGNORE INTO trade(
                     connection_session_id, venue, market_id, symbol, exchange_trade_id,
                     exchange_sequence, local_sequence, ts_recv_us, ts_srv_us, price, size,
                     aggressor_side, raw_public_json
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    payload["connection"]["id"], payload["venue"], payload["market_id"],
                    payload["symbol"], payload["trade_id"], payload.get("exchange_sequence"),
                    payload["local_sequence"], payload["recv_us"], payload.get("srv_us"),
                    payload["price"], payload["size"], payload.get("aggressor_side"),
                    payload["raw_public_json"],
                ),
            )
            if cursor.rowcount == 1:
                self._merge_ohlcv_trade(connection, payload)
        elif kind == "funding":
            connection.execute(
                """INSERT OR REPLACE INTO funding(
                     observed_ts_us, effective_ts_us, venue, market_id, symbol, rate, source
                   ) VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    payload["recv_us"], payload.get("effective_us"), payload["venue"],
                    payload["market_id"], payload["symbol"], payload["rate"], payload["source"],
                ),
            )
        elif kind == "price":
            connection.execute(
                """INSERT OR REPLACE INTO price_observation(
                     observed_ts_us, ts_srv_us, venue, market_id, symbol, price_type, price, source
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    payload["recv_us"], payload.get("srv_us"), payload["venue"],
                    payload["market_id"], payload["symbol"], payload["price_type"],
                    payload["price"], payload["source"],
                ),
            )
        elif kind == "gap":
            connection.execute(
                """INSERT INTO data_gap(
                     connection_session_id, venue, market_id, symbol, channel, ts_start_us,
                     ts_end_us, expected_sequence, observed_sequence, reason
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    payload.get("connection_id"), payload["venue"], payload.get("market_id"),
                    payload.get("symbol"), payload["channel"], payload["recv_us"],
                    payload.get("end_us"), payload.get("expected_sequence"),
                    payload.get("observed_sequence"), payload["reason"],
                ),
            )
        elif kind == "market_status":
            connection.execute(
                """INSERT OR REPLACE INTO market_status(
                     observed_ts_us, venue, market_id, symbol, status, force_reduce_only,
                     daily_volume_usd, open_interest, is_eligible, eligibility_reason
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    payload["recv_us"], payload["venue"], payload["market_id"],
                    payload["symbol"], payload["status"], int(payload["force_reduce_only"]),
                    payload["daily_volume_usd"], payload.get("open_interest"),
                    int(payload["is_eligible"]), payload["eligibility_reason"],
                ),
            )
        elif kind == "market_metadata":
            connection.execute(
                """INSERT OR REPLACE INTO market_metadata(
                     observed_ts_us, venue, market_id, symbol, public_json
                   ) VALUES (?, ?, ?, ?, ?)""",
                (
                    payload["recv_us"], payload["venue"], payload["market_id"],
                    payload["symbol"], payload["public_json"],
                ),
            )
        elif kind == "session":
            connection.execute(
                """INSERT OR REPLACE INTO trading_session(
                     session_id, krx_business_date, t0_us, t1_us, t2_us, krx_is_open,
                     us_cash_is_open, calendar_version, validity_reason
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    payload["session_id"], payload["krx_business_date"], payload["t0_us"],
                    payload["t1_us"], payload["t2_us"], payload["krx_is_open"],
                    payload["us_cash_is_open"], payload["calendar_version"],
                    payload["validity_reason"],
                ),
            )
        else:
            raise ValueError(f"unknown database command: {kind}")


async def send_public_control(ws: Any, message: dict[str, Any]) -> None:
    """Enforce the process's no-order outbound-message boundary."""
    message_type = message.get("type")
    if message_type not in ALLOWED_MESSAGE_TYPES:
        raise RuntimeError(f"outbound WebSocket message type is forbidden: {message_type!r}")
    channel = message.get("channel")
    if channel is not None and channel.split("/", 1)[0] not in ALLOWED_CHANNEL_PREFIXES:
        raise RuntimeError(f"outbound WebSocket channel is forbidden: {channel!r}")
    if set(message) - {"type", "channel"}:
        raise RuntimeError("outbound public control message contains unexpected fields")
    await ws.send(json.dumps(message, separators=(",", ":")))


def fetch_json(url: str) -> dict[str, Any]:
    request = Request(url, headers={"User-Agent": "engine-b-phase0-observer/1"})
    with urlopen(request, timeout=20) as response:
        if response.status != 200:
            raise RuntimeError(f"HTTP {response.status} from {url}")
        return json.load(response)


class Collector:
    def __init__(self, config: AppConfig, sink: DatabaseSink, metrics: Metrics) -> None:
        self.config = config
        self.sink = sink
        self.metrics = metrics
        self.stop_event = asyncio.Event()
        self.books: dict[tuple[str, int], BookState] = {}
        self.local_sequences: defaultdict[tuple[str, str, int], int] = defaultdict(int)
        self.last_health_error: str | None = None

    def request_stop(self) -> None:
        self.stop_event.set()

    def next_sequence(self, venue: str, channel: str, market_id: int) -> int:
        key = (venue, channel, market_id)
        self.local_sequences[key] += 1
        return self.local_sequences[key]

    async def run(self, smoke_seconds: int | None) -> None:
        tasks = [
            asyncio.create_task(self.feed_loop(venue), name=f"ws-{venue.name}")
            for venue in self.config.venues
        ]
        tasks.extend(
            [
                asyncio.create_task(self.rest_loop(), name="rest-market-status"),
                asyncio.create_task(self.session_loop(), name="provisional-calendar"),
                asyncio.create_task(self.health_loop(), name="health-file"),
                asyncio.create_task(self.metrics_server(), name="metrics-server"),
                asyncio.create_task(self.database_watchdog(), name="database-watchdog"),
            ]
        )
        if smoke_seconds is not None:
            tasks.append(asyncio.create_task(self.smoke_timer(smoke_seconds), name="smoke-timer"))
        stop_wait = asyncio.create_task(self.stop_event.wait(), name="stop-wait")
        try:
            done, _ = await asyncio.wait([*tasks, stop_wait], return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                if task is stop_wait:
                    continue
                exception = task.exception()
                if exception is not None:
                    raise exception
                if not self.stop_event.is_set() and task.get_name() != "smoke-timer":
                    raise RuntimeError(f"critical task ended unexpectedly: {task.get_name()}")
        finally:
            self.stop_event.set()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            stop_wait.cancel()
            await asyncio.gather(stop_wait, return_exceptions=True)

    async def smoke_timer(self, seconds: int) -> None:
        await asyncio.sleep(seconds)
        self.stop_event.set()

    async def database_watchdog(self) -> None:
        if self.sink._task is None:
            raise RuntimeError("database writer was not started")
        await asyncio.shield(self.sink._task)
        if not self.stop_event.is_set():
            raise RuntimeError("database writer ended unexpectedly")

    async def feed_loop(self, venue: VenueConfig) -> None:
        backoff = 1
        while not self.stop_event.is_set():
            connection = {
                "id": str(uuid.uuid4()),
                "venue": venue.name,
                "started_us": now_us(),
                "api_schema_version": self.config.api_schema_version,
            }
            await self.sink.put("connection_start", {"recv_us": connection["started_us"], "connection": connection})
            reason = "normal_stop"
            try:
                async with connect(
                    venue.ws_url,
                    ping_interval=20,
                    ping_timeout=15,
                    close_timeout=10,
                    max_queue=4096,
                    open_timeout=20,
                ) as ws:
                    LOG.info("WebSocket connected venue=%s url=%s", venue.name, venue.ws_url)
                    self.metrics.feed_connected[venue.name] = True
                    backoff = 1
                    async for raw_message in ws:
                        recv_us = now_us()
                        self.metrics.last_message_us[venue.name] = recv_us
                        message = json.loads(raw_message)
                        message_type = message.get("type", "")
                        if message_type == "connected":
                            await self.subscribe_public_channels(ws, venue)
                        elif message_type in {"subscribed/order_book", "update/order_book"}:
                            await self.handle_book(ws, venue, connection, message, recv_us)
                        elif message_type in {"subscribed/trade", "update/trade"}:
                            await self.handle_trades(venue, connection, message, recv_us)
                        elif message_type in {"subscribed/market_stats", "update/market_stats"}:
                            await self.handle_market_stats(venue, message, recv_us)
                        elif message_type == "ping":
                            await send_public_control(ws, {"type": "pong"})
                        elif message_type == "pong":
                            pass
                        else:
                            self.metrics.inc("engine_b_phase0_unhandled_message_total", {"venue": venue.name, "type": message_type or "missing"})
                            LOG.warning("Unhandled public WS message venue=%s type=%s", venue.name, message_type)
                        if self.stop_event.is_set():
                            break
            except asyncio.CancelledError:
                reason = "task_cancelled"
                raise
            except Exception as exc:
                reason = f"connection_error:{type(exc).__name__}"
                self.metrics.inc("engine_b_phase0_reconnect_total", {"venue": venue.name})
                LOG.exception("WebSocket failure venue=%s; reconnect in %ss", venue.name, backoff)
            finally:
                self.metrics.feed_connected[venue.name] = False
                ended_us = now_us()
                await self.sink.put(
                    "connection_end",
                    {"recv_us": ended_us, "connection": connection, "reason": reason},
                )
                for market in venue.markets:
                    state = self.books.setdefault((venue.name, market.market_id), BookState())
                    state.synced = False
                    self.metrics.book_synced[(venue.name, market.market_id)] = False
                    await self.sink.put(
                        "gap",
                        {
                            "recv_us": ended_us,
                            "connection_id": connection["id"],
                            "venue": venue.name,
                            "market_id": market.market_id,
                            "symbol": market.symbol,
                            "channel": "connection",
                            "reason": reason,
                        },
                    )
            if not self.stop_event.is_set():
                try:
                    await asyncio.wait_for(self.stop_event.wait(), timeout=backoff)
                except TimeoutError:
                    pass
                backoff = min(backoff * 2, 60)

    async def subscribe_public_channels(self, ws: Any, venue: VenueConfig) -> None:
        for market in venue.markets:
            for channel in ("order_book", "trade", "market_stats"):
                await send_public_control(
                    ws, {"type": "subscribe", "channel": f"{channel}/{market.market_id}"}
                )
        LOG.info("Subscribed to public channels venue=%s markets=%d", venue.name, len(venue.markets))

    async def handle_book(
        self,
        ws: Any,
        venue: VenueConfig,
        connection: dict[str, Any],
        message: dict[str, Any],
        recv_us: int,
    ) -> None:
        market_id = parse_market_id(message["channel"])
        market = venue.market_by_id.get(market_id)
        if market is None:
            return
        payload = message.get("order_book", {})
        state = self.books.setdefault((venue.name, market_id), BookState())
        message_type = message["type"]
        local_sequence = self.next_sequence(venue.name, "order_book", market_id)
        srv_us = normalize_exchange_timestamp_us(
            payload.get("last_updated_at", message.get("last_updated_at", message.get("timestamp")))
        )
        event_kind = "snapshot" if message_type == "subscribed/order_book" else "delta"
        complete = event_kind == "snapshot"
        await self.sink.put(
            "book",
            {
                "recv_us": recv_us,
                "srv_us": srv_us,
                "connection": connection,
                "venue": venue.name,
                "market_id": market_id,
                "symbol": market.symbol,
                "event_kind": event_kind,
                "exchange_sequence": str(payload["nonce"]) if payload.get("nonce") is not None else None,
                "begin_sequence": str(payload["begin_nonce"]) if payload.get("begin_nonce") is not None else None,
                "exchange_offset": str(payload["offset"]) if payload.get("offset") is not None else None,
                "local_sequence": local_sequence,
                "complete": complete,
                "levels": BookState.raw_levels(payload),
            },
        )
        if complete:
            state.apply_snapshot(payload)
        else:
            applied, expected, observed = state.apply_delta(payload)
            if not applied:
                self.metrics.inc("engine_b_phase0_sequence_gap_total", {"venue": venue.name, "symbol": market.symbol})
                await self.sink.put(
                    "gap",
                    {
                        "recv_us": recv_us,
                        "connection_id": connection["id"],
                        "venue": venue.name,
                        "market_id": market_id,
                        "symbol": market.symbol,
                        "channel": "order_book",
                        "expected_sequence": expected,
                        "observed_sequence": observed,
                        "reason": "begin_nonce_mismatch_or_unsynced_delta",
                    },
                )
                await send_public_control(ws, {"type": "unsubscribe", "channel": f"order_book/{market_id}"})
                await send_public_control(ws, {"type": "subscribe", "channel": f"order_book/{market_id}"})
                self.metrics.book_synced[(venue.name, market_id)] = False
                return
        self.metrics.book_synced[(venue.name, market_id)] = state.synced
        if state.synced and recv_us - state.last_reconstructed_us >= self.config.reconstructed_snapshot_interval_ms * 1_000:
            state.last_reconstructed_us = recv_us
            await self.sink.put(
                "book",
                {
                    "recv_us": recv_us,
                    "srv_us": srv_us,
                    "connection": connection,
                    "venue": venue.name,
                    "market_id": market_id,
                    "symbol": market.symbol,
                    "event_kind": "reconstructed",
                    "exchange_sequence": state.last_nonce,
                    "begin_sequence": None,
                    "exchange_offset": str(payload["offset"]) if payload.get("offset") is not None else None,
                    "local_sequence": self.next_sequence(venue.name, "order_book", market_id),
                    "complete": True,
                    "levels": state.reconstructed_levels(self.config.top_levels),
                },
            )
        self.metrics.inc("engine_b_phase0_book_message_total", {"venue": venue.name, "symbol": market.symbol})

    async def handle_trades(
        self,
        venue: VenueConfig,
        connection: dict[str, Any],
        message: dict[str, Any],
        recv_us: int,
    ) -> None:
        market_id = parse_market_id(message["channel"])
        market = venue.market_by_id.get(market_id)
        if market is None:
            return
        exchange_sequence = str(message["nonce"]) if message.get("nonce") is not None else None
        trades = [*message.get("trades", []), *message.get("liquidation_trades", [])]
        for trade in trades:
            price_text = canonical_decimal(trade["price"])
            size_text = canonical_decimal(trade["size"])
            srv_us = normalize_exchange_timestamp_us(trade.get("timestamp", message.get("timestamp")))
            event_ts_us = srv_us or recv_us
            bucket_start_us = event_ts_us - event_ts_us % 60_000_000
            is_maker_ask = trade.get("is_maker_ask")
            aggressor_side = None if is_maker_ask is None else ("buy" if is_maker_ask else "sell")
            raw_public_json = json.dumps(trade, sort_keys=True, separators=(",", ":"))
            raw_trade_id = trade.get("trade_id_str", trade.get("trade_id"))
            trade_id = (
                str(raw_trade_id)
                if raw_trade_id is not None
                else "synthetic:"
                + hashlib.sha256(
                    f"{venue.name}:{market_id}:".encode() + raw_public_json.encode()
                ).hexdigest()
            )
            await self.sink.put(
                "trade",
                {
                    "recv_us": recv_us,
                    "partition_us": event_ts_us,
                    "event_ts_us": event_ts_us,
                    "bucket_start_us": bucket_start_us,
                    "srv_us": srv_us,
                    "connection": connection,
                    "venue": venue.name,
                    "market_id": market_id,
                    "symbol": market.symbol,
                    "trade_id": trade_id,
                    "exchange_sequence": exchange_sequence,
                    "local_sequence": self.next_sequence(venue.name, "trade", market_id),
                    "price": price_text,
                    "size": size_text,
                    "aggressor_side": aggressor_side,
                    "raw_public_json": raw_public_json,
                },
            )
        if trades:
            self.metrics.inc(
                "engine_b_phase0_trade_total",
                {"venue": venue.name, "symbol": market.symbol},
                len(trades),
            )

    async def handle_market_stats(
        self, venue: VenueConfig, message: dict[str, Any], recv_us: int
    ) -> None:
        market_id = parse_market_id(message["channel"])
        market = venue.market_by_id.get(market_id)
        if market is None:
            return
        stats = message.get("market_stats", {})
        srv_us = normalize_exchange_timestamp_us(message.get("timestamp"))
        for field_name, price_type in (
            ("mark_price", "mark"),
            ("index_price", "index"),
            ("last_trade_price", "last"),
            ("mid_price", "mid"),
        ):
            value = stats.get(field_name)
            if value not in (None, ""):
                await self.sink.put(
                    "price",
                    {
                        "recv_us": recv_us,
                        "srv_us": srv_us,
                        "venue": venue.name,
                        "market_id": market_id,
                        "symbol": market.symbol,
                        "price_type": price_type,
                        "price": canonical_decimal(value),
                        "source": "ws_market_stats",
                    },
                )
        rate = stats.get("funding_rate")
        if rate not in (None, ""):
            await self.sink.put(
                "funding",
                {
                    "recv_us": recv_us,
                    "effective_us": normalize_exchange_timestamp_us(stats.get("funding_timestamp")),
                    "venue": venue.name,
                    "market_id": market_id,
                    "symbol": market.symbol,
                    "rate": canonical_decimal(rate),
                    "source": "ws_market_stats_last_payment",
                },
            )
        current_rate = stats.get("current_funding_rate")
        if current_rate not in (None, ""):
            await self.sink.put(
                "funding",
                {
                    "recv_us": recv_us + 1,
                    "effective_us": None,
                    "venue": venue.name,
                    "market_id": market_id,
                    "symbol": market.symbol,
                    "rate": canonical_decimal(current_rate),
                    "source": "ws_market_stats_estimate",
                },
            )
        self.metrics.inc("engine_b_phase0_market_stats_total", {"venue": venue.name, "symbol": market.symbol})

    async def rest_loop(self) -> None:
        while not self.stop_event.is_set():
            await asyncio.gather(*(self.poll_venue(venue) for venue in self.config.venues))
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=self.config.rest_poll_seconds)
            except TimeoutError:
                pass

    async def poll_venue(self, venue: VenueConfig) -> None:
        observed_us = now_us()
        try:
            response = await asyncio.to_thread(fetch_json, f"{venue.rest_url}/api/v1/orderBookDetails")
            details_by_id = {
                int(detail["market_id"]): detail for detail in response.get("order_book_details", [])
            }
            for market in venue.markets:
                detail = details_by_id.get(market.market_id)
                if detail is None or detail.get("symbol") != market.symbol:
                    raise RuntimeError(
                        f"market mapping mismatch venue={venue.name} expected={market.symbol}/{market.market_id}"
                    )
                status = str(detail.get("status", "unknown"))
                force_reduce_only = bool((detail.get("market_config") or {}).get("force_reduce_only", False))
                volume = Decimal(str(detail.get("daily_quote_token_volume", "0")))
                reasons = []
                if status != "active":
                    reasons.append(f"status={status}")
                if force_reduce_only:
                    reasons.append("force_reduce_only")
                if volume < self.config.min_daily_volume_usd:
                    reasons.append("daily_volume_below_min")
                await self.sink.put(
                    "market_status",
                    {
                        "recv_us": observed_us,
                        "venue": venue.name,
                        "market_id": market.market_id,
                        "symbol": market.symbol,
                        "status": status,
                        "force_reduce_only": force_reduce_only,
                        "daily_volume_usd": canonical_decimal(volume),
                        "open_interest": canonical_decimal(detail["open_interest"]) if detail.get("open_interest") is not None else None,
                        "is_eligible": not reasons,
                        "eligibility_reason": "eligible" if not reasons else ",".join(reasons),
                    },
                )
                await self.sink.put(
                    "market_metadata",
                    {
                        "recv_us": observed_us,
                        "venue": venue.name,
                        "market_id": market.market_id,
                        "symbol": market.symbol,
                        "public_json": json.dumps(detail, sort_keys=True, separators=(",", ":")),
                    },
                )
            self.metrics.last_rest_success_us[venue.name] = observed_us
            self.metrics.set_gauge("engine_b_phase0_rest_poll_success", {"venue": venue.name}, 1)
        except (OSError, URLError, ValueError, RuntimeError, KeyError):
            self.metrics.set_gauge("engine_b_phase0_rest_poll_success", {"venue": venue.name}, 0)
            self.metrics.inc("engine_b_phase0_rest_poll_error_total", {"venue": venue.name})
            LOG.exception("REST market metadata poll failed venue=%s", venue.name)

    async def session_loop(self) -> None:
        last_date: date | None = None
        while not self.stop_event.is_set():
            today = datetime.now(UTC).date()
            if today != last_date:
                await self.write_provisional_session(today)
                await self.write_provisional_session(today + timedelta(days=1))
                last_date = today
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=300)
            except TimeoutError:
                pass

    async def write_provisional_session(self, session_date: date) -> None:
        t0 = datetime.combine(session_date, datetime_time(0, 0), UTC)
        t1 = datetime.combine(session_date, datetime_time(6, 30), UTC)
        t2_ny = datetime.combine(session_date, datetime_time(9, 30), ZoneInfo("America/New_York"))
        missing = sorted(
            {
                f"{venue.name}:{symbol}"
                for venue in self.config.venues
                for symbol in venue.known_missing_symbols
            }
        )
        reasons = ["A7_UNRESOLVED_VERIFIED_KRX_US_CALENDAR"]
        if missing:
            reasons.append("SAME_VENUE_REQUIRED_SYMBOLS_MISSING=" + ",".join(missing))
        if session_date.weekday() >= 5:
            reasons.append("PROVISIONAL_WEEKEND")
        await self.sink.put(
            "session",
            {
                "recv_us": now_us(),
                "session_id": f"provisional-{session_date.isoformat()}",
                "krx_business_date": session_date.isoformat(),
                "t0_us": int(t0.timestamp() * 1_000_000),
                "t1_us": int(t1.timestamp() * 1_000_000),
                "t2_us": int(t2_ny.astimezone(UTC).timestamp() * 1_000_000),
                "krx_is_open": 0,
                "us_cash_is_open": 0,
                "calendar_version": "UNRESOLVED_A7_zoneinfo_only",
                "validity_reason": ";".join(reasons),
            },
        )

    async def health_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                self.write_health()
                self.last_health_error = None
            except OSError as exc:
                self.last_health_error = str(exc)
                LOG.exception("Failed to write health file")
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=10)
            except TimeoutError:
                pass

    def health_payload(self) -> dict[str, Any]:
        current_us = now_us()
        return {
            "timestamp": datetime.now(UTC).isoformat(),
            "phase": "0A_observer",
            "order_capability": False,
            "collector_version": self.config.collector_version,
            "config_hash": self.config.config_hash,
            "queue_size": self.sink.queue.qsize(),
            "venues": {
                venue.name: {
                    "connected": self.metrics.feed_connected.get(venue.name, False),
                    "last_message_age_seconds": (
                        (current_us - self.metrics.last_message_us[venue.name]) / 1_000_000
                        if venue.name in self.metrics.last_message_us
                        else None
                    ),
                    "known_missing_same_venue_symbols": list(venue.known_missing_symbols),
                }
                for venue in self.config.venues
            },
            "phase0_sample_eligible": False,
            "phase0_sample_blockers": [
                "A7 verified KRX/US calendar unresolved",
                "Robinhood Lighter lacks same-venue EWY and USDKRW",
            ],
            "last_health_error": self.last_health_error,
        }

    def write_health(self) -> None:
        path = self.config.health_file
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(path.suffix + ".tmp")
        temporary.write_text(json.dumps(self.health_payload(), sort_keys=True) + "\n")
        os.chmod(temporary, 0o640)
        temporary.replace(path)

    async def metrics_server(self) -> None:
        server = await asyncio.start_server(
            self.handle_metrics_client, self.config.metrics_host, self.config.metrics_port
        )
        LOG.info("Metrics listening on %s:%d", self.config.metrics_host, self.config.metrics_port)
        async with server:
            await server.serve_forever()

    async def handle_metrics_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            request_line = await asyncio.wait_for(reader.readline(), timeout=2)
            path = request_line.decode("ascii", errors="replace").split(" ")[1]
            while True:
                line = await asyncio.wait_for(reader.readline(), timeout=2)
                if line in {b"\r\n", b"\n", b""}:
                    break
            if path == "/metrics":
                body = self.metrics.render(self.sink.queue.qsize()).encode()
                status = b"200 OK"
                content_type = b"text/plain; version=0.0.4"
            elif path == "/healthz":
                body = (json.dumps(self.health_payload(), sort_keys=True) + "\n").encode()
                status = b"200 OK"
                content_type = b"application/json"
            else:
                body = b"not found\n"
                status = b"404 Not Found"
                content_type = b"text/plain"
            writer.write(
                b"HTTP/1.1 " + status + b"\r\nContent-Type: " + content_type
                + b"\r\nContent-Length: " + str(len(body)).encode()
                + b"\r\nConnection: close\r\n\r\n" + body
            )
            await writer.drain()
        except (IndexError, TimeoutError):
            pass
        finally:
            writer.close()
            await writer.wait_closed()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config",
        type=Path,
        default=Path(os.environ.get("ENGINE_B_PHASE0_CONFIG", "/opt/debot/configs/engine-b/phase0.json")),
    )
    parser.add_argument(
        "--requirements-lock",
        type=Path,
        default=Path(
            os.environ.get(
                "ENGINE_B_PHASE0_REQUIREMENTS_LOCK",
                "/opt/debot/scripts/engine_b_phase0_requirements.txt",
            )
        ),
    )
    parser.add_argument("--database-dir", type=Path)
    parser.add_argument("--health-file", type=Path)
    parser.add_argument("--metrics-listen")
    parser.add_argument("--smoke-seconds", type=int)
    parser.add_argument("--validate-config", action="store_true")
    return parser.parse_args()


async def async_main(args: argparse.Namespace) -> int:
    config = load_config(args.config, args.requirements_lock)
    if args.database_dir is not None:
        object.__setattr__(config, "database_dir", args.database_dir)
    if args.health_file is not None:
        object.__setattr__(config, "health_file", args.health_file)
    if args.metrics_listen is not None:
        host, port_text = args.metrics_listen.rsplit(":", 1)
        object.__setattr__(config, "metrics_host", host)
        object.__setattr__(config, "metrics_port", int(port_text))
    if args.validate_config:
        print(
            json.dumps(
                {
                    "config_hash": config.config_hash,
                    "dependency_lock_hash": config.dependency_lock_hash,
                    "order_capability": False,
                    "venues": [venue.name for venue in config.venues],
                },
                sort_keys=True,
            )
        )
        return 0
    collector_run_id = str(uuid.uuid4())
    code_commit = os.environ.get("ENGINE_B_PHASE0_CODE_COMMIT", "UNKNOWN_UNDEPLOYED")
    metrics = Metrics()
    sink = DatabaseSink(config, collector_run_id, code_commit)
    sink.start()
    collector = Collector(config, sink, metrics)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, collector.request_stop)
    LOG.info(
        "Engine B Phase 0A observer starting run=%s order_capability=false config_hash=%s code_commit=%s",
        collector_run_id,
        config.config_hash,
        code_commit,
    )
    for venue in config.venues:
        if venue.known_missing_symbols:
            LOG.warning(
                "venue=%s cannot produce v0.3-valid samples; missing same-venue symbols=%s",
                venue.name,
                ",".join(venue.known_missing_symbols),
            )
    try:
        await collector.run(args.smoke_seconds)
    finally:
        await sink.close()
    LOG.info("Engine B Phase 0A observer stopped cleanly")
    return 0


def main() -> int:
    if sys.version_info < (3, 11):
        print("Python 3.11 or newer is required", file=sys.stderr)
        return 2
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)sZ %(levelname)s %(name)s %(message)s",
    )
    args = parse_args()
    if args.smoke_seconds is not None and args.smoke_seconds <= 0:
        raise SystemExit("--smoke-seconds must be positive")
    return asyncio.run(async_main(args))


if __name__ == "__main__":
    raise SystemExit(main())
