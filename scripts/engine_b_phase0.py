#!/usr/bin/env python3
"""Engine B Phase 0A public-data observer (bot-strategy#866).

This process has no account authentication, signing, or order capability.  Its
only outbound WebSocket messages are public-data subscriptions and keepalives.
It records the mainnet Lighter venue (the intended future execution venue),
which already carries every same-venue control requirements v0.3 needs
(including EWY and USDKRW) -- there is no second, cross-venue feed to
reconcile.

The exchange calendar (A-7) is resolved from a frozen, pre-computed KRX/US
cash-market session table (see scripts/engine_b_trading_calendar_freeze.py
and configs/engine-b/trading_calendar.json) that this process loads with
stdlib `json` at startup -- it never imports a calendar library itself. If
the frozen table is missing, unreadable, or does not cover a given date, the
observer falls back to the original fail-closed placeholder for that date.
A configured venue missing a required same-venue symbol is recorded
separately (`SAME_VENUE_REQUIRED_SYMBOLS_MISSING`) and remains a fail-closed
session-invalid reason regardless of A-7 -- today's single venue has nothing
missing, so only an unresolved A-7 date produces one. Collection can begin,
but rows carrying any session-invalid reason must not be counted as valid
Phase 0A samples yet.
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
from collections import defaultdict, deque
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date, datetime, time as datetime_time, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from urllib.error import URLError
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

LOG = logging.getLogger("engine_b_phase0")
UTC = timezone.utc
ALLOWED_MESSAGE_TYPES = frozenset({"subscribe", "unsubscribe", "ping", "pong"})
ALLOWED_CHANNEL_PREFIXES = frozenset({"order_book", "trade", "market_stats"})
SCHEMA_VERSION = 9
# Ceiling for (re)subscribe WS frames per venue per rolling minute: half of
# Lighter's documented 200 client messages / minute per IP, leaving the other
# half to the trading bots sharing the IP.
WATCHDOG_MAX_FRAMES_PER_MIN = 100
# Part of that ceiling is reserved for recovering *proven* sequence breaks on
# synced books (bot-strategy#908): two frames (unsubscribe + subscribe) per
# configured market of the venue, so every book can recover once within the
# minute even if the initial burst and the watchdog have used their share.
# `load_config` rejects venues where that reserve would leave the watchdog
# fewer than WATCHDOG_MIN_SHARE_FRAMES.
WATCHDOG_MIN_SHARE_FRAMES = 20
MAX_TRADE_EVENT_AGE_US = 7 * 24 * 60 * 60 * 1_000_000
MAX_TRADE_EVENT_FUTURE_US = 5 * 60 * 1_000_000
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
  last_activity_ts_recv_us INTEGER NOT NULL,
  ended_ts_recv_us INTEGER,
  api_schema_version TEXT NOT NULL,
  end_reason TEXT,
  is_physical INTEGER NOT NULL CHECK(is_physical IN (0, 1))
);

CREATE TABLE IF NOT EXISTS sealed_session_interval (
  interval_id TEXT PRIMARY KEY,
  sealed_partition TEXT NOT NULL,
  connection_session_id TEXT NOT NULL,
  venue TEXT NOT NULL,
  ts_start_us INTEGER NOT NULL,
  ts_end_us INTEGER NOT NULL,
  api_schema_version TEXT NOT NULL,
  reason TEXT NOT NULL,
  CHECK(ts_end_us >= ts_start_us)
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

CREATE TABLE IF NOT EXISTS trade_replay_alias (
  venue TEXT NOT NULL,
  market_id INTEGER NOT NULL,
  replay_alias TEXT NOT NULL,
  exchange_trade_id TEXT NOT NULL,
  PRIMARY KEY(venue, market_id, exchange_trade_id)
);

CREATE INDEX IF NOT EXISTS idx_trade_replay_alias_lookup
  ON trade_replay_alias(venue, market_id, replay_alias);

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
  replay_alias TEXT,
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
  continuation_id TEXT,
  reason TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sealed_gap_interval (
  interval_id TEXT PRIMARY KEY,
  sealed_partition TEXT NOT NULL,
  venue TEXT NOT NULL,
  market_id INTEGER,
  symbol TEXT,
  channel TEXT NOT NULL,
  ts_start_us INTEGER NOT NULL,
  ts_end_us INTEGER NOT NULL,
  expected_sequence TEXT,
  observed_sequence TEXT,
  reason TEXT NOT NULL,
  CHECK(ts_end_us >= ts_start_us)
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


def partition_start_us(partition: str) -> int:
    return int(
        datetime.strptime(partition, "%Y%m%d_%H")
        .replace(tzinfo=UTC)
        .timestamp()
        * 1_000_000
    )


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


def validate_trade_timestamp_us(timestamp_us: int | None, recv_us: int) -> None:
    if timestamp_us is None:
        return
    if not (
        recv_us - MAX_TRADE_EVENT_AGE_US
        <= timestamp_us
        <= recv_us + MAX_TRADE_EVENT_FUTURE_US
    ):
        raise RuntimeError(
            "refusing trade with out-of-range exchange timestamp "
            f"timestamp_us={timestamp_us} recv_us={recv_us}"
        )


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


def synthetic_trade_id(
    venue: str,
    market_id: int,
    event_ts_us: int,
    stable_occurrence: int,
    raw_public_json: str,
    message_scope: str | None = None,
) -> str:
    identity_fields: dict[str, Any] = {
        "venue": venue,
        "market_id": market_id,
        "event_ts_us": event_ts_us,
        "stable_occurrence": stable_occurrence,
        "raw_public_json": raw_public_json,
    }
    if message_scope is not None:
        identity_fields["message_scope"] = message_scope
    identity = json.dumps(identity_fields, sort_keys=True, separators=(",", ":"))
    return "synthetic:v3:" + hashlib.sha256(identity.encode()).hexdigest()


def synthetic_trade_replay_alias(
    venue: str,
    market_id: int,
    event_ts_us: int,
    raw_public_json: str,
) -> str:
    identity = json.dumps(
        {
            "venue": venue,
            "market_id": market_id,
            "event_ts_us": event_ts_us,
            "raw_public_json": raw_public_json,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return "synthetic-replay:v1:" + hashlib.sha256(identity.encode()).hexdigest()


def trade_message_scope(
    message_type: str, exchange_sequence: str | None, recv_us: int
) -> str | None:
    if message_type == "subscribed/trade":
        return None
    return (
        f"{message_type}:{exchange_sequence}"
        if exchange_sequence is not None
        else f"{message_type}:missing-sequence"
    )


def build_sealed_trade_index(
    source_path: Path,
    index_path: Path,
    partition: str,
    canonical_sha256: str,
) -> None:
    source = sqlite3.connect(f"file:{source_path}?mode=ro", uri=True)
    index = sqlite3.connect(index_path)
    try:
        index.execute(
            """CREATE TABLE sealed_metadata(
                 partition TEXT PRIMARY KEY,
                 canonical_db_sha256 TEXT NOT NULL
               )"""
        )
        index.execute(
            """CREATE TABLE archived_trade_identity(
                 venue TEXT NOT NULL,
                 market_id INTEGER NOT NULL,
                 exchange_trade_id TEXT NOT NULL,
                 PRIMARY KEY(venue, market_id, exchange_trade_id)
               ) WITHOUT ROWID"""
        )
        index.execute(
            """CREATE TABLE late_trade_identity(
                 venue TEXT NOT NULL,
                 market_id INTEGER NOT NULL,
                 exchange_trade_id TEXT NOT NULL,
                 PRIMARY KEY(venue, market_id, exchange_trade_id)
               ) WITHOUT ROWID"""
        )
        index.execute(
            """CREATE TABLE archived_trade_replay_alias(
                 venue TEXT NOT NULL,
                 market_id INTEGER NOT NULL,
                 replay_alias TEXT NOT NULL,
                 exchange_trade_id TEXT NOT NULL,
                 PRIMARY KEY(venue, market_id, exchange_trade_id)
               ) WITHOUT ROWID"""
        )
        index.execute(
            """CREATE INDEX idx_archived_trade_replay_alias_lookup
               ON archived_trade_replay_alias(venue, market_id, replay_alias)"""
        )
        index.execute(
            """CREATE TABLE archived_gap_continuation(
                 continuation_id TEXT PRIMARY KEY,
                 gap_id INTEGER NOT NULL
               ) WITHOUT ROWID"""
        )
        index.execute(
            """CREATE TABLE archived_connection_session(
                 connection_session_id TEXT PRIMARY KEY,
                 started_ts_recv_us INTEGER NOT NULL,
                 ended_ts_recv_us INTEGER,
                 end_reason TEXT
               ) WITHOUT ROWID"""
        )
        tables = {
            row[0]
            for row in source.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
        if "trade" in tables:
            last_message: tuple[str, str, int, int, str | None] | None = None
            stable_occurrences: defaultdict[tuple[int, str], int] = defaultdict(int)
            rows = source.execute(
                """SELECT trade_row_id, connection_session_id, venue, market_id,
                          exchange_trade_id, exchange_sequence, local_sequence,
                          ts_recv_us, ts_srv_us, raw_public_json
                   FROM trade
                   ORDER BY connection_session_id, venue, market_id, ts_recv_us,
                            local_sequence, trade_row_id"""
            )
            for (
                _trade_row_id,
                connection_session_id,
                venue,
                market_id,
                exchange_trade_id,
                exchange_sequence,
                _local_sequence,
                ts_recv_us,
                ts_srv_us,
                raw_public_json,
            ) in rows:
                message_identity = (
                    connection_session_id,
                    venue,
                    market_id,
                    ts_recv_us,
                    exchange_sequence,
                )
                if message_identity == last_message:
                    pass
                else:
                    last_message = message_identity
                    stable_occurrences.clear()
                event_ts_us = ts_srv_us or ts_recv_us
                occurrence_key = (event_ts_us, raw_public_json)
                stable_occurrence = stable_occurrences[occurrence_key]
                stable_occurrences[occurrence_key] += 1
                if exchange_trade_id is not None and not exchange_trade_id.startswith(
                    "synthetic:"
                ):
                    stable_trade_ids = [exchange_trade_id]
                else:
                    unscoped_id = synthetic_trade_id(
                        venue,
                        market_id,
                        event_ts_us,
                        stable_occurrence,
                        raw_public_json,
                    )
                    scoped_id = synthetic_trade_id(
                        venue,
                        market_id,
                        event_ts_us,
                        stable_occurrence,
                        raw_public_json,
                        trade_message_scope(
                            "update/trade", exchange_sequence, ts_recv_us
                        ),
                    )
                    stable_trade_ids = [unscoped_id, scoped_id]
                    if exchange_trade_id is not None and exchange_trade_id.startswith(
                        "synthetic:v3:"
                    ):
                        stable_trade_ids[0] = exchange_trade_id
                index.executemany(
                    "INSERT OR IGNORE INTO archived_trade_identity VALUES (?, ?, ?)",
                    (
                        (venue, market_id, stable_trade_id)
                        for stable_trade_id in stable_trade_ids
                    ),
                )
                if exchange_trade_id is None or exchange_trade_id.startswith(
                    "synthetic:"
                ):
                    index.execute(
                        """INSERT OR IGNORE INTO archived_trade_replay_alias
                           VALUES (?, ?, ?, ?)""",
                        (
                            venue,
                            market_id,
                            synthetic_trade_replay_alias(
                                venue, market_id, event_ts_us, raw_public_json
                            ),
                            (
                                exchange_trade_id
                                if exchange_trade_id is not None
                                else stable_trade_ids[0]
                            ),
                        ),
                    )
        gap_columns = (
            {
                row[1]
                for row in source.execute("PRAGMA table_info(data_gap)")
            }
            if "data_gap" in tables
            else set()
        )
        if "continuation_id" in gap_columns:
            index.executemany(
                "INSERT OR IGNORE INTO archived_gap_continuation VALUES (?, ?)",
                source.execute(
                    """SELECT continuation_id, gap_id FROM data_gap
                       WHERE continuation_id IS NOT NULL"""
                ),
            )
        if "ws_connection" in tables:
            session_columns = {
                row[1]
                for row in source.execute("PRAGMA table_info(ws_connection)")
            }
            physical_filter = (
                " WHERE is_physical = 1"
                if "is_physical" in session_columns
                else ""
            )
            index.executemany(
                "INSERT OR IGNORE INTO archived_connection_session VALUES (?, ?, ?, ?)",
                source.execute(
                    """SELECT connection_session_id, started_ts_recv_us,
                              ended_ts_recv_us, end_reason
                       FROM ws_connection"""
                    + physical_filter
                ),
            )
        index.execute(
            "INSERT INTO sealed_metadata VALUES (?, ?)",
            (partition, canonical_sha256),
        )
        index.commit()
        if index.execute("PRAGMA integrity_check").fetchone() != ("ok",):
            raise RuntimeError(
                f"trade identity index integrity_check failed: {index_path}"
            )
    finally:
        index.close()
        source.close()


def verify_sealed_partition(
    source_path: Path,
    index_path: Path,
    seal_path: Path,
    partition: str,
) -> None:
    seal = json.loads(seal_path.read_text())
    expected_sha256 = seal.get("sha256")
    if (
        seal.get("partition") != partition
        or seal.get("trade_index") != index_path.name
        or not isinstance(expected_sha256, str)
        or len(expected_sha256) != 64
    ):
        raise RuntimeError(f"sealed partition metadata is invalid: {seal_path}")
    digest = hashlib.sha256()
    with source_path.open("rb") as source_file:
        while chunk := source_file.read(1024 * 1024):
            digest.update(chunk)
    if digest.hexdigest() != expected_sha256:
        raise RuntimeError(
            f"sealed local database does not match verified archive: {source_path}"
        )
    source = sqlite3.connect(f"file:{source_path}?mode=ro", uri=True)
    index = sqlite3.connect(f"file:{index_path}?mode=ro", uri=True)
    try:
        if source.execute("PRAGMA integrity_check").fetchone() != ("ok",):
            raise RuntimeError(f"sealed local database integrity failed: {source_path}")
        if index.execute("PRAGMA integrity_check").fetchone() != ("ok",):
            raise RuntimeError(f"sealed trade identity index integrity failed: {index_path}")
        metadata = index.execute(
            "SELECT partition, canonical_db_sha256 FROM sealed_metadata"
        ).fetchone()
        if metadata != (partition, expected_sha256):
            raise RuntimeError(f"sealed trade identity index mismatch: {index_path}")
    finally:
        index.close()
        source.close()


def reconcile_late_trade_identities(source_path: Path, sealed_dir: Path) -> int:
    """Copy the durable late-trade journal into each canonical seal sidecar."""
    source = sqlite3.connect(f"file:{source_path}?mode=ro", uri=True, timeout=5)
    try:
        tables = {
            row[0]
            for row in source.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
        if "late_trade" not in tables:
            return 0
        late_columns = {
            row[1] for row in source.execute("PRAGMA table_info(late_trade)")
        }
        replay_alias_expression = (
            "replay_alias" if "replay_alias" in late_columns else "NULL"
        )
        rows = source.execute(
            f"""SELECT DISTINCT sealed_partition, venue, market_id,
                       exchange_trade_id, {replay_alias_expression}
                FROM late_trade ORDER BY sealed_partition, venue, market_id,
                                         exchange_trade_id"""
        ).fetchall()
    finally:
        source.close()

    by_partition: defaultdict[
        str, list[tuple[str, int, str, str | None]]
    ] = defaultdict(list)
    for partition, venue, market_id, trade_id, replay_alias in rows:
        by_partition[partition].append(
            (venue, market_id, trade_id, replay_alias)
        )

    for partition, identities in by_partition.items():
        seal_path = sealed_dir / f"{partition}.json"
        index_path = sealed_dir / f"{partition}.trade_ids.sqlite3"
        try:
            seal = json.loads(seal_path.read_text())
        except (OSError, json.JSONDecodeError) as exc:
            raise RuntimeError(
                f"late-trade seal metadata is unreadable: {seal_path}"
            ) from exc
        if (
            seal.get("partition") != partition
            or seal.get("trade_index") != index_path.name
            or not isinstance(seal.get("sha256"), str)
        ):
            raise RuntimeError(f"late-trade seal metadata is invalid: {seal_path}")
        if not index_path.is_file():
            raise RuntimeError(
                f"late-trade identity index is missing: {index_path}"
            )
        index = sqlite3.connect(index_path, timeout=5)
        try:
            metadata = index.execute(
                "SELECT partition, canonical_db_sha256 FROM sealed_metadata"
            ).fetchone()
            if metadata != (partition, seal["sha256"]):
                raise RuntimeError(
                    f"late-trade identity index metadata mismatch: {index_path}"
                )
            index.execute(
                """CREATE TABLE IF NOT EXISTS archived_trade_replay_alias(
                     venue TEXT NOT NULL,
                     market_id INTEGER NOT NULL,
                     replay_alias TEXT NOT NULL,
                     exchange_trade_id TEXT NOT NULL,
                     PRIMARY KEY(venue, market_id, exchange_trade_id)
                   ) WITHOUT ROWID"""
            )
            index.execute(
                """CREATE INDEX IF NOT EXISTS idx_archived_trade_replay_alias_lookup
                   ON archived_trade_replay_alias(venue, market_id, replay_alias)"""
            )
            index.executemany(
                "INSERT OR IGNORE INTO late_trade_identity VALUES (?, ?, ?)",
                (identity[:3] for identity in identities),
            )
            index.executemany(
                """INSERT OR IGNORE INTO archived_trade_replay_alias
                   VALUES (?, ?, ?, ?)""",
                (
                    (venue, market_id, replay_alias, trade_id)
                    for venue, market_id, trade_id, replay_alias in identities
                    if replay_alias is not None
                ),
            )
            index.commit()
        except Exception:
            index.rollback()
            raise
        finally:
            index.close()
    return len(rows)


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
    trading_calendar_file: Path | None
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
    # Per-market book watchdog (bot-strategy#908). Optional keys in
    # phase0.json so an existing deployment keeps working unchanged.
    book_resubscribe_after_ms: int = 10_000
    book_stall_after_ms: int = 60_000
    book_watchdog_batch: int = 5


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
    for key, default in (
        ("book_resubscribe_after_ms", 10_000),
        ("book_stall_after_ms", 60_000),
        ("book_watchdog_batch", 5),
    ):
        # Optional keys, but a typo must not silently disable the watchdog
        # (batch <= 0) or turn it into a subscribe storm (after_ms <= 0).
        if int(raw.get(key, default)) <= 0:
            raise ValueError(f"{key} must be positive")
    # Worst case the watchdog can send: one pass per second, up to
    # `book_watchdog_batch` markets per pass, two frames (unsubscribe +
    # subscribe) per market, each market eligible again after the shorter of
    # the two intervals. Keep that under half of Lighter's documented
    # 200 client messages / minute per IP so the trading bots sharing the IP
    # keep the other half (bot-strategy#908, Codex review on pairtrade#277).
    resubscribe_after_ms = int(raw.get("book_resubscribe_after_ms", 10_000))
    stall_after_ms = int(raw.get("book_stall_after_ms", 60_000))
    if stall_after_ms < resubscribe_after_ms:
        # A stall declared before the per-market re-subscribe spacing allows
        # a frame would sit unsynced with no recovery until that spacing
        # elapses; require the stall threshold to be the longer of the two.
        raise ValueError("book_stall_after_ms must be >= book_resubscribe_after_ms")
    shortest_ms = min(resubscribe_after_ms, stall_after_ms)
    worst_frames_per_min = 2 * int(raw.get("book_watchdog_batch", 5)) * 60_000 / shortest_ms
    if worst_frames_per_min > WATCHDOG_MAX_FRAMES_PER_MIN:
        raise ValueError(
            "book watchdog settings could send "
            f"{worst_frames_per_min:.0f} WS frames/min (> {WATCHDOG_MAX_FRAMES_PER_MIN}); "
            "raise book_resubscribe_after_ms/book_stall_after_ms or lower book_watchdog_batch"
        )
    for venue in venues:
        reserve = 2 * len(venue.markets)
        if reserve > WATCHDOG_MAX_FRAMES_PER_MIN - WATCHDOG_MIN_SHARE_FRAMES:
            raise ValueError(
                f"venue {venue.name} has {len(venue.markets)} markets; the sequence-break reserve "
                f"({reserve} frames) would leave the watchdog fewer than {WATCHDOG_MIN_SHARE_FRAMES} of "
                f"{WATCHDOG_MAX_FRAMES_PER_MIN} frames/min"
            )
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
        trading_calendar_file=Path(raw["trading_calendar_file"]) if raw.get("trading_calendar_file") else None,
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
        book_resubscribe_after_ms=int(raw.get("book_resubscribe_after_ms", 10_000)),
        book_stall_after_ms=int(raw.get("book_stall_after_ms", 60_000)),
        book_watchdog_batch=int(raw.get("book_watchdog_batch", 5)),
    )


@dataclass(frozen=True)
class TradingCalendar:
    """Frozen KRX/US cash-market session table (A-7).

    Loaded once from a static JSON artifact produced offline by
    scripts/engine_b_trading_calendar_freeze.py. This module never imports a
    calendar library itself: `load` only ever calls stdlib `json`, and any
    failure to load or parse the file degrades to `None` (fail-closed,
    A-7-unresolved for every date) rather than raising, since a calendar
    load problem must not take down the observer.
    """

    calendar_version: str
    sessions: dict[str, dict[str, Any]]

    # SQLite INTEGER's positive bound (signed 64-bit); sqlite3 raises
    # OverflowError above it, which _write_partition's
    # `except Exception: rollback(); raise` propagates straight through the
    # database watchdog, terminating the whole observer.
    _SQLITE_INT_MAX = 2**63 - 1

    @classmethod
    def _valid_timestamp_us(cls, value: Any) -> bool:
        # bool is a subclass of int in Python, so isinstance(value, int)
        # alone would accept True/False here. A pre-epoch (negative)
        # microsecond value is never a legitimate KRX/US session boundary.
        return type(value) is int and 0 <= value <= cls._SQLITE_INT_MAX

    @classmethod
    def load(cls, path: Path) -> "TradingCalendar | None":
        try:
            raw = json.loads(path.read_bytes())
            calendar_version = str(raw["calendar_version"])
            sessions = dict(raw["sessions"])
            for entry in sessions.values():
                if not isinstance(entry["krx_is_open"], bool) or not isinstance(entry["us_is_open"], bool):
                    raise TypeError("session entry krx_is_open/us_is_open must be bool")
                # A valid, SQLite-safe, correctly ordered open/close pair is
                # only required -- and only meaningful -- on the side that is
                # actually open; write_provisional_session indexes these
                # fields directly once a date resolves, so anything wrong
                # here must reject the whole load rather than surface later
                # as a KeyError, a NULL into trading_session's NOT NULL
                # columns, an OverflowError from an out-of-range int, or a
                # session end before its own start.
                if entry["krx_is_open"]:
                    krx_open, krx_close = entry["krx_open_utc_us"], entry["krx_close_utc_us"]
                    if not cls._valid_timestamp_us(krx_open) or not cls._valid_timestamp_us(krx_close):
                        raise TypeError("krx_is_open session entry missing/invalid krx_open_utc_us/krx_close_utc_us")
                    if krx_open >= krx_close:
                        raise ValueError("krx_open_utc_us must be before krx_close_utc_us")
                if entry["us_is_open"] and not cls._valid_timestamp_us(entry["us_open_utc_us"]):
                    raise TypeError("us_is_open session entry missing/invalid us_open_utc_us")
            return cls(calendar_version=calendar_version, sessions=sessions)
        except (OSError, ValueError, KeyError, TypeError):
            LOG.exception("Failed to load trading calendar from %s; A-7 remains unresolved", path)
            return None

    def resolve(self, session_date: date) -> dict[str, Any] | None:
        return self.sessions.get(session_date.isoformat())


@dataclass
class BookState:
    bids: dict[str, str] = field(default_factory=dict)
    asks: dict[str, str] = field(default_factory=dict)
    last_nonce: str | None = None
    synced: bool = False
    last_reconstructed_us: int = 0

    def apply_snapshot(self, payload: dict[str, Any]) -> bool:
        nonce = self._sequence(payload.get("nonce"))
        if nonce is None:
            self.synced = False
            self.last_nonce = None
            self.bids.clear()
            self.asks.clear()
            return False
        self.bids = self._levels_to_map(payload.get("bids", []))
        self.asks = self._levels_to_map(payload.get("asks", []))
        self.last_nonce = nonce
        self.synced = True
        return True

    def apply_delta(self, payload: dict[str, Any]) -> tuple[bool, str | None, str | None]:
        begin_nonce = self._sequence(payload.get("begin_nonce"))
        next_nonce = self._sequence(payload.get("nonce"))
        expected = self.last_nonce
        if (
            not self.synced
            or begin_nonce is None
            or next_nonce is None
            or expected is None
            or begin_nonce != expected
        ):
            self.synced = False
            self.last_nonce = None
            self.bids.clear()
            self.asks.clear()
            return False, expected, begin_nonce
        self._apply_levels(self.bids, payload.get("bids", []))
        self._apply_levels(self.asks, payload.get("asks", []))
        self.last_nonce = next_nonce
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
                price = canonical_decimal(level["price"])
                size = canonical_decimal(level["size"])
                if Decimal(price) <= 0 or Decimal(size) < 0:
                    raise RuntimeError(
                        "non-positive book price or negative book size"
                    )
                rows.append((side, index, price, size))
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
    def __init__(
        self,
        config: AppConfig,
        collector_run_id: str,
        code_commit: str,
        run_started_us: int | None = None,
    ) -> None:
        self.config = config
        self.collector_run_id = collector_run_id
        self.code_commit = code_commit
        self.run_started_us = now_us() if run_started_us is None else run_started_us
        self.queue: asyncio.Queue[tuple[str, dict[str, Any]]] = asyncio.Queue(
            maxsize=config.queue_maxsize
        )
        self._connections: dict[str, sqlite3.Connection] = {}
        self.state_dir = config.database_dir.parent
        self.sealed_dir = self.state_dir / "sealed"
        self.lock_dir = self.state_dir / "locks"
        self.gap_continuation_dir = self.state_dir / "gap-continuations"
        self.gap_close_dir = self.state_dir / "gap-closes"
        self.session_continuation_dir = self.state_dir / "session-continuations"
        self._startup_recovery_pending = True
        self._task: asyncio.Task[None] | None = None
        self._stopping = False

    def start(self) -> None:
        for directory in (
            self.config.database_dir,
            self.sealed_dir,
            self.lock_dir,
            self.gap_continuation_dir,
            self.gap_close_dir,
            self.session_continuation_dir,
        ):
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
            try:
                first = await asyncio.wait_for(
                    self.queue.get(), self._rotation_timeout_seconds()
                )
            except TimeoutError:
                await asyncio.to_thread(self._write_batch, [])
                continue
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

    @staticmethod
    def _rotation_timeout_seconds() -> float:
        current_us = now_us()
        next_hour_us = (current_us // 3_600_000_000 + 1) * 3_600_000_000
        return max(0.05, (next_hour_us - current_us) / 1_000_000 + 0.05)

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
        opened_us = now_us()
        connection.execute(
            "INSERT OR IGNORE INTO schema_metadata(schema_version, created_ts_us) VALUES (?, ?)",
            (SCHEMA_VERSION, opened_us),
        )
        connection.execute(
            """INSERT OR IGNORE INTO collector_manifest(
                 collector_run_id, started_ts_us, document_version, collector_version,
                 code_commit, config_hash, dependency_lock_hash, api_schema_version,
                 order_capability, config_json
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?)""",
            (
                self.collector_run_id,
                self.run_started_us,
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
        gap_columns = {
            row[1] for row in connection.execute("PRAGMA table_info(data_gap)")
        }
        if "continuation_id" not in gap_columns:
            connection.execute("ALTER TABLE data_gap ADD COLUMN continuation_id TEXT")
        session_columns = {
            row[1] for row in connection.execute("PRAGMA table_info(ws_connection)")
        }
        if "last_activity_ts_recv_us" not in session_columns:
            connection.execute(
                "ALTER TABLE ws_connection ADD COLUMN last_activity_ts_recv_us INTEGER"
            )
            connection.execute(
                """UPDATE ws_connection
                   SET last_activity_ts_recv_us = started_ts_recv_us
                   WHERE last_activity_ts_recv_us IS NULL"""
            )
        if "is_physical" not in session_columns:
            connection.execute(
                """ALTER TABLE ws_connection
                   ADD COLUMN is_physical INTEGER NOT NULL DEFAULT 1"""
            )
        late_columns = {
            row[1] for row in connection.execute("PRAGMA table_info(late_trade)")
        }
        if "replay_alias" not in late_columns:
            connection.execute("ALTER TABLE late_trade ADD COLUMN replay_alias TEXT")
        connection.execute(
            """CREATE UNIQUE INDEX IF NOT EXISTS idx_data_gap_continuation
               ON data_gap(continuation_id) WHERE continuation_id IS NOT NULL"""
        )
        connection.execute(
            """DELETE FROM data_gap
               WHERE ts_end_us IS NULL AND channel = 'connection'
                 AND gap_id NOT IN (
                   SELECT MIN(gap_id) FROM data_gap
                   WHERE ts_end_us IS NULL AND channel = 'connection'
                   GROUP BY venue, market_id, channel
                 )"""
        )
        connection.execute(
            """CREATE UNIQUE INDEX IF NOT EXISTS idx_data_gap_open_connection
               ON data_gap(venue, market_id, channel)
               WHERE ts_end_us IS NULL AND channel = 'connection'"""
        )
        connection.execute(
            """DELETE FROM data_gap
               WHERE ts_end_us IS NULL AND channel = 'order_book'
                 AND gap_id NOT IN (
                   SELECT MIN(gap_id) FROM data_gap
                   WHERE ts_end_us IS NULL AND channel = 'order_book'
                   GROUP BY venue, market_id, channel
                 )"""
        )
        connection.execute(
            """CREATE UNIQUE INDEX IF NOT EXISTS idx_data_gap_open_order_book
               ON data_gap(venue, market_id, channel)
               WHERE ts_end_us IS NULL AND channel = 'order_book'"""
        )
        connection.execute(
            """UPDATE ohlcv_1m
               SET first_trade_ts_us = COALESCE(first_trade_ts_us, bucket_start_us),
                   last_trade_ts_us = COALESCE(last_trade_ts_us, bucket_start_us)
               WHERE first_trade_ts_us IS NULL OR last_trade_ts_us IS NULL"""
        )
        for venue, market_id, trade_id, recv_us, srv_us, raw_public_json in (
            connection.execute(
                """SELECT venue, market_id, exchange_trade_id, ts_recv_us,
                          ts_srv_us, raw_public_json
                   FROM trade
                   WHERE exchange_trade_id LIKE 'synthetic:%'"""
            )
        ):
            connection.execute(
                """INSERT OR IGNORE INTO trade_replay_alias(
                     venue, market_id, replay_alias, exchange_trade_id
                   ) VALUES (?, ?, ?, ?)""",
                (
                    venue,
                    market_id,
                    synthetic_trade_replay_alias(
                        venue, market_id, srv_us or recv_us, raw_public_json
                    ),
                    trade_id,
                ),
            )
        for row_id, venue, market_id, event_ts_us, raw_public_json in (
            connection.execute(
                """SELECT late_trade_row_id, venue, market_id, event_ts_us,
                          raw_public_json
                   FROM late_trade
                   WHERE replay_alias IS NULL
                     AND exchange_trade_id LIKE 'synthetic:%'"""
            )
        ):
            connection.execute(
                "UPDATE late_trade SET replay_alias = ? WHERE late_trade_row_id = ?",
                (
                    synthetic_trade_replay_alias(
                        venue, market_id, event_ts_us, raw_public_json
                    ),
                    row_id,
                ),
            )

    @staticmethod
    def _ensure_connection(
        connection: sqlite3.Connection, payload: dict[str, Any], partition: str
    ) -> None:
        meta = payload.get("connection")
        if not meta:
            return
        partition_started_us = partition_start_us(partition)
        partition_ended_us = partition_started_us + 3_600_000_000
        connection_started_us = int(meta["started_us"])
        payload_received_us = int(payload.get("recv_us", connection_started_us))
        is_physical = (
            connection_started_us < partition_ended_us
            and partition_started_us <= payload_received_us
        )
        segment_started_us = min(
            max(connection_started_us, partition_started_us),
            partition_ended_us,
        )
        connection.execute(
            """INSERT INTO ws_connection(
                 connection_session_id, venue, channel, started_ts_recv_us,
                 last_activity_ts_recv_us, ended_ts_recv_us,
                 api_schema_version, end_reason, is_physical
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(connection_session_id) DO UPDATE SET
                 venue = excluded.venue,
                 channel = excluded.channel,
                 started_ts_recv_us = excluded.started_ts_recv_us,
                 last_activity_ts_recv_us = excluded.last_activity_ts_recv_us,
                 ended_ts_recv_us = NULL,
                 api_schema_version = excluded.api_schema_version,
                 end_reason = NULL,
                 is_physical = 1
               WHERE ws_connection.is_physical = 0
                 AND excluded.is_physical = 1""",
            (
                meta["id"],
                meta["venue"],
                "multiplexed_public",
                segment_started_us,
                segment_started_us,
                None if is_physical else segment_started_us,
                meta["api_schema_version"],
                None if is_physical else "event_time_reference",
                int(is_physical),
            ),
        )

    def _is_partition_sealed(self, partition: str) -> bool:
        return (self.sealed_dir / f"{partition}.json").is_file()

    def _sealed_trade_index_contains(
        self, partition: str, payload: dict[str, Any]
    ) -> bool:
        index_path = self.sealed_dir / f"{partition}.trade_ids.sqlite3"
        seal_path = self.sealed_dir / f"{partition}.json"
        try:
            seal = json.loads(seal_path.read_text())
            if (
                seal.get("partition") != partition
                or seal.get("trade_index") != index_path.name
                or not isinstance(seal.get("sha256"), str)
            ):
                raise RuntimeError(f"sealed partition metadata is invalid: {seal_path}")
        except (OSError, json.JSONDecodeError) as exc:
            raise RuntimeError(
                f"sealed partition metadata is unreadable: {seal_path}"
            ) from exc
        if not index_path.is_file():
            raise RuntimeError(
                f"sealed partition trade identity index is missing: {index_path}"
            )
        connection = sqlite3.connect(
            f"file:{index_path}?mode=ro", uri=True, timeout=5
        )
        try:
            metadata = connection.execute(
                "SELECT partition, canonical_db_sha256 FROM sealed_metadata"
            ).fetchone()
            if metadata != (partition, seal["sha256"]):
                raise RuntimeError(
                    f"sealed trade identity index metadata mismatch: {index_path}"
                )
            identity = (
                payload["venue"],
                payload["market_id"],
                payload["trade_id"],
            )
            archived = connection.execute(
                """SELECT 1 FROM archived_trade_identity
                   WHERE venue = ? AND market_id = ? AND exchange_trade_id = ?""",
                identity,
            ).fetchone()
            late = connection.execute(
                """SELECT 1 FROM late_trade_identity
                   WHERE venue = ? AND market_id = ? AND exchange_trade_id = ?""",
                identity,
            ).fetchone()
            return archived is not None or late is not None
        except sqlite3.DatabaseError as exc:
            raise RuntimeError(
                f"sealed trade identity index is unreadable: {index_path}"
            ) from exc
        finally:
            connection.close()

    def _sealed_gap_continuation_gap_id(
        self, partition: str, continuation_id: str
    ) -> int | None:
        index_path = self.sealed_dir / f"{partition}.trade_ids.sqlite3"
        seal_path = self.sealed_dir / f"{partition}.json"
        if not index_path.is_file():
            raise RuntimeError(
                f"sealed partition gap index is missing: {index_path}"
            )
        try:
            seal = json.loads(seal_path.read_text())
        except (OSError, json.JSONDecodeError) as exc:
            raise RuntimeError(
                f"sealed partition metadata is unreadable: {seal_path}"
            ) from exc
        connection = sqlite3.connect(
            f"file:{index_path}?mode=ro", uri=True, timeout=5
        )
        try:
            metadata = connection.execute(
                "SELECT partition, canonical_db_sha256 FROM sealed_metadata"
            ).fetchone()
            if metadata != (partition, seal.get("sha256")):
                raise RuntimeError(
                    f"sealed trade identity index metadata mismatch: {index_path}"
                )
            table = connection.execute(
                """SELECT 1 FROM sqlite_master
                   WHERE type = 'table' AND name = 'archived_gap_continuation'"""
            ).fetchone()
            if table is None:
                raise RuntimeError(
                    f"sealed partition gap index is incompatible: {index_path}"
                )
            row = connection.execute(
                """SELECT gap_id FROM archived_gap_continuation
                   WHERE continuation_id = ?""",
                (continuation_id,),
            ).fetchone()
            return None if row is None else int(row[0])
        except sqlite3.DatabaseError as exc:
            raise RuntimeError(
                f"sealed trade identity index is unreadable: {index_path}"
            ) from exc
        finally:
            connection.close()

    def _sealed_trade_replay_alias_ids(
        self, partition: str, payload: dict[str, Any]
    ) -> set[str] | None:
        replay_alias = payload.get("replay_alias")
        if replay_alias is None or payload.get("snapshot_occurrence") is None:
            return set()
        index_path = self.sealed_dir / f"{partition}.trade_ids.sqlite3"
        seal_path = self.sealed_dir / f"{partition}.json"
        try:
            seal = json.loads(seal_path.read_text())
        except (OSError, json.JSONDecodeError) as exc:
            raise RuntimeError(
                f"sealed partition metadata is unreadable: {seal_path}"
            ) from exc
        connection = sqlite3.connect(
            f"file:{index_path}?mode=ro", uri=True, timeout=5
        )
        try:
            metadata = connection.execute(
                "SELECT partition, canonical_db_sha256 FROM sealed_metadata"
            ).fetchone()
            if metadata != (partition, seal.get("sha256")):
                raise RuntimeError(
                    f"sealed trade replay index metadata mismatch: {index_path}"
                )
            table = connection.execute(
                """SELECT 1 FROM sqlite_master
                   WHERE type = 'table'
                     AND name = 'archived_trade_replay_alias'"""
            ).fetchone()
            if table is None:
                LOG.warning(
                    "Legacy sealed trade index has no replay aliases; "
                    "discarding unverifiable ID-less snapshot partition=%s "
                    "venue=%s market_id=%s",
                    partition,
                    payload["venue"],
                    payload["market_id"],
                )
                return None
            return {
                str(row[0])
                for row in connection.execute(
                    """SELECT exchange_trade_id
                       FROM archived_trade_replay_alias
                       WHERE venue = ? AND market_id = ? AND replay_alias = ?""",
                    (payload["venue"], payload["market_id"], replay_alias),
                )
            }
        except sqlite3.DatabaseError as exc:
            raise RuntimeError(
                f"sealed trade replay index is unreadable: {index_path}"
            ) from exc
        finally:
            connection.close()

    def _sealed_connection_session_exists(
        self, partition: str, connection_session_id: str
    ) -> bool:
        index_path = self.sealed_dir / f"{partition}.trade_ids.sqlite3"
        seal_path = self.sealed_dir / f"{partition}.json"
        if not index_path.is_file():
            raise RuntimeError(
                f"sealed partition session index is missing: {index_path}"
            )
        try:
            seal = json.loads(seal_path.read_text())
        except (OSError, json.JSONDecodeError) as exc:
            raise RuntimeError(
                f"sealed partition metadata is unreadable: {seal_path}"
            ) from exc
        connection = sqlite3.connect(
            f"file:{index_path}?mode=ro", uri=True, timeout=5
        )
        try:
            metadata = connection.execute(
                "SELECT partition, canonical_db_sha256 FROM sealed_metadata"
            ).fetchone()
            if metadata != (partition, seal.get("sha256")):
                raise RuntimeError(
                    f"sealed session index metadata mismatch: {index_path}"
                )
            table = connection.execute(
                """SELECT 1 FROM sqlite_master
                   WHERE type = 'table'
                     AND name = 'archived_connection_session'"""
            ).fetchone()
            if table is None:
                raise RuntimeError(
                    f"sealed partition session index is incompatible: {index_path}"
                )
            return connection.execute(
                """SELECT 1 FROM archived_connection_session
                   WHERE connection_session_id = ?""",
                (connection_session_id,),
            ).fetchone() is not None
        except sqlite3.DatabaseError as exc:
            raise RuntimeError(
                f"sealed session index is unreadable: {index_path}"
            ) from exc
        finally:
            connection.close()

    def _retained_connection_session_exists(
        self, partition: str, connection_session_id: str
    ) -> bool:
        database_path = (
            self.config.database_dir / f"engine_b_phase0_{partition}.sqlite3"
        )
        if not database_path.is_file():
            return False
        with self._partition_lock(partition):
            connection = self._connections.get(partition)
            borrowed = connection is not None
            if connection is None:
                try:
                    connection = sqlite3.connect(
                        f"file:{database_path}?mode=ro", uri=True, timeout=5
                    )
                except sqlite3.OperationalError as exc:
                    if not database_path.exists():
                        return False
                    raise RuntimeError(
                        f"session continuation target is unreadable: {database_path}"
                    ) from exc
            try:
                table = connection.execute(
                    """SELECT 1 FROM sqlite_master
                       WHERE type = 'table' AND name = 'ws_connection'"""
                ).fetchone()
                if table is None:
                    return False
                columns = {
                    row[1]
                    for row in connection.execute("PRAGMA table_info(ws_connection)")
                }
                physical_filter = (
                    " AND is_physical = 1" if "is_physical" in columns else ""
                )
                return (
                    connection.execute(
                        """SELECT 1 FROM ws_connection
                           WHERE connection_session_id = ?"""
                        + physical_filter,
                        (connection_session_id,),
                    ).fetchone()
                    is not None
                )
            except sqlite3.DatabaseError as exc:
                raise RuntimeError(
                    f"session continuation target is unreadable: {database_path}"
                ) from exc
            finally:
                if not borrowed:
                    connection.close()

    def _local_late_trade_state(
        self, sealed_partition: str, payload: dict[str, Any]
    ) -> tuple[bool, set[str]]:
        identity = (
            sealed_partition,
            payload["venue"],
            payload["market_id"],
            payload["trade_id"],
        )
        primary_exists = False
        alias_ids: set[str] = set()
        for database_path in sorted(
            self.config.database_dir.glob("engine_b_phase0_*.sqlite3")
        ):
            try:
                connection = sqlite3.connect(
                    f"file:{database_path}?mode=ro", uri=True, timeout=5
                )
            except sqlite3.OperationalError as exc:
                if not database_path.exists():
                    continue
                raise RuntimeError(
                    f"late-trade journal is unreadable: {database_path}"
                ) from exc
            try:
                table = connection.execute(
                    """SELECT 1 FROM sqlite_master
                       WHERE type = 'table' AND name = 'late_trade'"""
                ).fetchone()
                if table is None:
                    continue
                if connection.execute(
                    """SELECT 1 FROM late_trade
                       WHERE sealed_partition = ? AND venue = ? AND market_id = ?
                         AND exchange_trade_id = ?""",
                    identity,
                ).fetchone() is not None:
                    primary_exists = True
                columns = {
                    row[1]
                    for row in connection.execute("PRAGMA table_info(late_trade)")
                }
                if (
                    payload.get("replay_alias") is not None
                    and payload.get("snapshot_occurrence") is not None
                    and "replay_alias" in columns
                ):
                    alias_ids.update(
                        str(row[0])
                        for row in connection.execute(
                            """SELECT exchange_trade_id FROM late_trade
                               WHERE sealed_partition = ? AND venue = ?
                                 AND market_id = ? AND replay_alias = ?""",
                            (
                                sealed_partition,
                                payload["venue"],
                                payload["market_id"],
                                payload["replay_alias"],
                            ),
                        )
                    )
            except sqlite3.DatabaseError as exc:
                raise RuntimeError(
                    f"late-trade journal is unreadable: {database_path}"
                ) from exc
            finally:
                connection.close()
        return primary_exists, alias_ids

    def _archived_trade_exists(self, partition: str, payload: dict[str, Any]) -> bool:
        if self._sealed_trade_index_contains(partition, payload):
            return True
        sealed_alias_ids = self._sealed_trade_replay_alias_ids(partition, payload)
        if sealed_alias_ids is None:
            return True
        local_primary, local_alias_ids = self._local_late_trade_state(
            partition, payload
        )
        if local_primary:
            return True
        snapshot_occurrence = payload.get("snapshot_occurrence")
        if (
            snapshot_occurrence is not None
            and len(sealed_alias_ids | local_alias_ids) > int(snapshot_occurrence)
        ):
            return True
        # An archiver may have reconciled and removed the local journal between
        # the local scan and this point. Recheck the sidecar to close that race.
        if self._sealed_trade_index_contains(partition, payload):
            return True
        sealed_alias_ids = self._sealed_trade_replay_alias_ids(partition, payload)
        if sealed_alias_ids is None:
            return True
        return (
            snapshot_occurrence is not None
            and len(sealed_alias_ids | local_alias_ids) > int(snapshot_occurrence)
        )

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
                self._ensure_connection(connection, payload, partition)
                self._apply(connection, kind, payload)
            connection.commit()
        except Exception:
            connection.rollback()
            raise

    @staticmethod
    def _fsync_directory(directory: Path) -> None:
        descriptor = os.open(directory, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def _persist_gap_continuation_marker(
        self, payload: dict[str, Any]
    ) -> Path:
        marker = {
            "continuation_id": payload["continuation_id"],
            "start_us": payload["start_us"],
            "venue": payload["venue"],
            "market_id": payload["market_id"],
            "symbol": payload["symbol"],
            "channel": payload["channel"],
            "expected_sequence": payload.get("expected_sequence"),
            "observed_sequence": payload.get("observed_sequence"),
            "reason": payload["reason"],
            "source_partition": payload["source_partition"],
            "source_gap_id": payload["source_gap_id"],
        }
        encoded = json.dumps(marker, sort_keys=True, separators=(",", ":")) + "\n"
        marker_name = hashlib.sha256(
            marker["continuation_id"].encode("utf-8")
        ).hexdigest()
        marker_path = self.gap_continuation_dir / f"{marker_name}.json"
        if marker_path.exists():
            if marker_path.read_text() != encoded:
                raise RuntimeError(
                    f"gap continuation marker mismatch: {marker_path}"
                )
            return marker_path
        temporary = marker_path.with_name(
            f".{marker_path.name}.{uuid.uuid4().hex}.tmp"
        )
        try:
            with temporary.open("w") as output:
                output.write(encoded)
                output.flush()
                os.fsync(output.fileno())
            os.chmod(temporary, 0o640)
            os.replace(temporary, marker_path)
            self._fsync_directory(self.gap_continuation_dir)
        finally:
            temporary.unlink(missing_ok=True)
        return marker_path

    def _persist_gap_close_marker(self, payload: dict[str, Any]) -> Path:
        marker = {
            "recv_us": int(payload["recv_us"]),
            "venue": str(payload["venue"]),
            "market_id": int(payload["market_id"]),
        }
        encoded = json.dumps(marker, sort_keys=True, separators=(",", ":")) + "\n"
        marker_name = hashlib.sha256(encoded.encode("utf-8")).hexdigest()
        self.gap_close_dir.mkdir(parents=True, exist_ok=True)
        os.chmod(self.gap_close_dir, 0o750)
        marker_path = self.gap_close_dir / f"{marker_name}.json"
        if marker_path.exists():
            if marker_path.read_text() != encoded:
                raise RuntimeError(f"gap-close marker mismatch: {marker_path}")
            return marker_path
        temporary = marker_path.with_name(
            f".{marker_path.name}.{uuid.uuid4().hex}.tmp"
        )
        try:
            with temporary.open("w") as output:
                output.write(encoded)
                output.flush()
                os.fsync(output.fileno())
            os.chmod(temporary, 0o640)
            os.replace(temporary, marker_path)
            self._fsync_directory(self.gap_close_dir)
        finally:
            temporary.unlink(missing_ok=True)
        return marker_path

    def _load_gap_closes(self) -> list[tuple[Path, dict[str, Any]]]:
        if not self.gap_close_dir.is_dir():
            return []
        recovered: list[tuple[Path, dict[str, Any]]] = []
        for marker_path in sorted(self.gap_close_dir.glob("*.json")):
            try:
                marker = json.loads(marker_path.read_text())
            except (OSError, json.JSONDecodeError) as exc:
                raise RuntimeError(
                    f"gap-close marker is unreadable: {marker_path}"
                ) from exc
            if (
                not isinstance(marker, dict)
                or set(marker) != {"recv_us", "venue", "market_id"}
                or not isinstance(marker["recv_us"], int)
                or not isinstance(marker["venue"], str)
                or not isinstance(marker["market_id"], int)
            ):
                raise RuntimeError(f"gap-close marker is invalid: {marker_path}")
            recovered.append((marker_path, marker))
        return sorted(
            recovered,
            key=lambda item: (int(item[1]["recv_us"]), item[0].name),
        )

    def _persist_session_continuation_marker(
        self, payload: dict[str, Any]
    ) -> Path:
        marker = {
            "continuation_id": payload["continuation_id"],
            "start_us": payload["start_us"],
            "source_partition": payload["source_partition"],
            "source_collector_run_id": payload.get("source_collector_run_id"),
            "connection": payload["connection"],
        }
        encoded = json.dumps(marker, sort_keys=True, separators=(",", ":")) + "\n"
        marker_name = hashlib.sha256(
            marker["continuation_id"].encode("utf-8")
        ).hexdigest()
        self.session_continuation_dir.mkdir(parents=True, exist_ok=True)
        os.chmod(self.session_continuation_dir, 0o750)
        marker_path = self.session_continuation_dir / f"{marker_name}.json"
        if marker_path.exists():
            if marker_path.read_text() != encoded:
                raise RuntimeError(
                    f"session continuation marker mismatch: {marker_path}"
                )
            return marker_path
        temporary = marker_path.with_name(
            f".{marker_path.name}.{uuid.uuid4().hex}.tmp"
        )
        try:
            with temporary.open("w") as output:
                output.write(encoded)
                output.flush()
                os.fsync(output.fileno())
            os.chmod(temporary, 0o640)
            os.replace(temporary, marker_path)
            self._fsync_directory(self.session_continuation_dir)
        finally:
            temporary.unlink(missing_ok=True)
        return marker_path

    def _recover_orphaned_sessions(self, recovered_us: int) -> None:
        current_partition = partition_for_us(recovered_us)
        for database_path in sorted(
            self.config.database_dir.glob("engine_b_phase0_*.sqlite3")
        ):
            partition = database_path.stem.removeprefix("engine_b_phase0_")
            try:
                if (
                    partition_for_us(partition_start_us(partition)) != partition
                    or partition > current_partition
                    or self._is_partition_sealed(partition)
                ):
                    continue
            except ValueError:
                continue
            with self._partition_lock(partition):
                if self._is_partition_sealed(partition) or not database_path.is_file():
                    continue
                try:
                    connection = sqlite3.connect(
                        f"file:{database_path}?mode=rw", uri=True, timeout=5
                    )
                except sqlite3.OperationalError as exc:
                    if not database_path.exists():
                        continue
                    raise RuntimeError(
                        f"orphaned session database is unreadable: {database_path}"
                    ) from exc
                try:
                    table = connection.execute(
                        """SELECT 1 FROM sqlite_master
                           WHERE type = 'table' AND name = 'ws_connection'"""
                    ).fetchone()
                    if table is None:
                        continue
                    session_columns = {
                        row[1]
                        for row in connection.execute(
                            "PRAGMA table_info(ws_connection)"
                        )
                    }
                    activity_expression = (
                        "COALESCE(last_activity_ts_recv_us, started_ts_recv_us)"
                        if "last_activity_ts_recv_us" in session_columns
                        else "started_ts_recv_us"
                    )
                    physical_filter = (
                        " AND is_physical = 1"
                        if "is_physical" in session_columns
                        else ""
                    )
                    rows = connection.execute(
                        f"""SELECT connection_session_id, venue,
                                  started_ts_recv_us, {activity_expression}
                           FROM ws_connection
                           WHERE ended_ts_recv_us IS NULL{physical_filter}"""
                    ).fetchall()
                    if not rows:
                        continue
                    connection.execute("BEGIN")
                    tables = {
                        row[0]
                        for row in connection.execute(
                            "SELECT name FROM sqlite_master WHERE type = 'table'"
                        )
                    }
                    venues = {venue.name: venue for venue in self.config.venues}
                    for session_id, venue, started_us, durable_activity_us in rows:
                        last_activity_us = max(
                            int(started_us), int(durable_activity_us)
                        )
                        if "book_event" in tables:
                            book_activity = connection.execute(
                                """SELECT MAX(ts_recv_us) FROM book_event
                                   WHERE connection_session_id = ?""",
                                (session_id,),
                            ).fetchone()[0]
                            if book_activity is not None:
                                last_activity_us = max(
                                    last_activity_us, int(book_activity)
                                )
                        if "trade" in tables:
                            trade_activity = connection.execute(
                                """SELECT MAX(ts_recv_us) FROM trade
                                   WHERE connection_session_id = ?""",
                                (session_id,),
                            ).fetchone()[0]
                            if trade_activity is not None:
                                last_activity_us = max(
                                    last_activity_us, int(trade_activity)
                                )
                        last_activity_us = min(last_activity_us, recovered_us)
                        connection.execute(
                            """UPDATE ws_connection
                               SET ended_ts_recv_us = MAX(started_ts_recv_us, ?),
                                   end_reason = 'collector_restart_recovery'
                               WHERE connection_session_id = ?
                                 AND ended_ts_recv_us IS NULL""",
                            (last_activity_us, session_id),
                        )
                        venue_config = venues.get(venue)
                        if venue_config is None:
                            # A venue rename/removal (e.g. bot-strategy#866's
                            # robinhood/lighter_mainnet_context -> lighter
                            # consolidation) leaves persisted ws_connection
                            # rows under the old venue name. The session was
                            # already closed above; there is no current
                            # market list to bookkeep a data_gap against for
                            # a venue this process no longer collects, so
                            # skip that part rather than fail the whole
                            # startup recovery over a deliberate config
                            # change.
                            LOG.warning(
                                "Orphaned session %s references venue=%s, not in current "
                                "config (renamed/removed); session closed, skipping "
                                "data_gap bookkeeping for it",
                                session_id,
                                venue,
                            )
                            continue
                        for market in venue_config.markets:
                            open_gap = connection.execute(
                                """SELECT MIN(gap_id) FROM data_gap
                                   WHERE ts_end_us IS NULL AND venue = ?
                                     AND market_id = ?
                                     AND channel = 'connection'""",
                                (venue, market.market_id),
                            ).fetchone()[0]
                            if open_gap is None:
                                connection.execute(
                                    """INSERT INTO data_gap(
                                         connection_session_id, venue, market_id,
                                         symbol, channel, ts_start_us, reason
                                       ) VALUES (?, ?, ?, ?, 'connection', ?, ?)""",
                                    (
                                        session_id,
                                        venue,
                                        market.market_id,
                                        market.symbol,
                                        last_activity_us,
                                        "collector_restart_recovery",
                                    ),
                                )
                            else:
                                connection.execute(
                                    """UPDATE data_gap
                                       SET ts_start_us = MIN(ts_start_us, ?)
                                       WHERE gap_id = ?""",
                                    (last_activity_us, open_gap),
                                )
                    connection.commit()
                except sqlite3.DatabaseError as exc:
                    connection.rollback()
                    raise RuntimeError(
                        f"orphaned session database is unreadable: {database_path}"
                    ) from exc
                finally:
                    connection.close()

    def _journal_stale_open_gaps(self, recovered_us: int) -> None:
        current_partition = partition_for_us(recovered_us)
        for database_path in sorted(
            self.config.database_dir.glob("engine_b_phase0_*.sqlite3")
        ):
            partition = database_path.stem.removeprefix("engine_b_phase0_")
            try:
                if (
                    partition_for_us(partition_start_us(partition)) != partition
                    or partition >= current_partition
                    or self._is_partition_sealed(partition)
                ):
                    continue
            except ValueError:
                continue
            with self._partition_lock(partition):
                if self._is_partition_sealed(partition) or not database_path.is_file():
                    continue
                connection = self._connections.get(partition)
                borrowed = connection is not None
                if connection is None:
                    try:
                        connection = sqlite3.connect(
                            f"file:{database_path}?mode=rw", uri=True, timeout=5
                        )
                    except sqlite3.OperationalError as exc:
                        if not database_path.exists():
                            continue
                        raise RuntimeError(
                            f"stale data-gap database is unreadable: {database_path}"
                        ) from exc
                try:
                    table = connection.execute(
                        """SELECT 1 FROM sqlite_master
                           WHERE type = 'table' AND name = 'data_gap'"""
                    ).fetchone()
                    if table is None:
                        continue
                    connection.execute("BEGIN")
                    connection.execute(
                        """DELETE FROM data_gap
                           WHERE ts_end_us IS NULL
                             AND channel IN ('connection', 'order_book')
                             AND gap_id NOT IN (
                               SELECT MIN(gap_id) FROM data_gap
                               WHERE ts_end_us IS NULL
                                 AND channel IN ('connection', 'order_book')
                               GROUP BY venue, market_id, channel
                             )"""
                    )
                    connection.commit()
                    rows = connection.execute(
                        """SELECT gap_id, venue, market_id, symbol, channel,
                                  expected_sequence, observed_sequence, reason
                           FROM data_gap WHERE ts_end_us IS NULL
                           ORDER BY gap_id"""
                    ).fetchall()
                    if not rows:
                        continue
                    boundary_us = partition_start_us(partition) + 3_600_000_000
                    for (
                        gap_id,
                        venue,
                        market_id,
                        symbol,
                        channel,
                        expected,
                        observed,
                        reason,
                    ) in rows:
                        self._persist_gap_continuation_marker(
                            {
                                "continuation_id": f"partition:{partition}:{gap_id}",
                                "start_us": boundary_us,
                                "venue": venue,
                                "market_id": market_id,
                                "symbol": symbol,
                                "channel": channel,
                                "expected_sequence": expected,
                                "observed_sequence": observed,
                                "reason": reason,
                                "source_partition": partition,
                                "source_gap_id": gap_id,
                            }
                        )
                    connection.execute("BEGIN")
                    connection.executemany(
                        """UPDATE data_gap SET ts_end_us = MAX(ts_start_us, ?)
                           WHERE gap_id = ? AND ts_end_us IS NULL""",
                        ((boundary_us, row[0]) for row in rows),
                    )
                    connection.commit()
                except sqlite3.DatabaseError as exc:
                    connection.rollback()
                    raise RuntimeError(
                        f"stale data-gap database is unreadable: {database_path}"
                    ) from exc
                finally:
                    if not borrowed:
                        connection.close()

    def _write_batch(self, batch: list[tuple[str, dict[str, Any]]]) -> None:
        grouped: defaultdict[str, list[tuple[str, dict[str, Any]]]] = defaultdict(list)
        recovered_gap_close_markers = dict(self._load_gap_closes())
        for payload in recovered_gap_close_markers.values():
            self._close_open_gaps(payload)
        for marker_path in recovered_gap_close_markers:
            marker_path.unlink(missing_ok=True)
        if recovered_gap_close_markers:
            self._fsync_directory(self.gap_close_dir)

        gap_close_markers: dict[Path, dict[str, Any]] = {}
        for kind, payload in batch:
            if kind == "gap_close":
                gap_close_markers[self._persist_gap_close_marker(payload)] = payload
        recovered_session_markers = self._load_session_continuations()
        if self._startup_recovery_pending:
            recovered_us = now_us()
            self._recover_orphaned_sessions(recovered_us)
            self._journal_stale_open_gaps(recovered_us)
            self._startup_recovery_pending = False
            recovered_session_markers.extend(
                self._load_session_continuations(
                    {
                        marker_path
                        for marker_path, _, _, _ in recovered_session_markers
                    }
                )
            )
        recovered_gap_markers = self._load_gap_continuations()
        batch = [
            *(
                command
                for _, start_command, end_command, restart_gaps in (
                    recovered_session_markers
                )
                for command in (
                    start_command,
                    end_command,
                    *(("gap", gap) for gap in restart_gaps),
                )
                if command is not None
            ),
            *(("gap", payload) for _, payload in recovered_gap_markers),
            *batch,
        ]
        ordered_gap_close_us: dict[tuple[str, int], int] = {}
        for kind, payload in batch:
            if kind == "__stop__":
                continue
            if kind == "gap_close":
                ordered_gap_close_us[
                    (payload["venue"], payload["market_id"])
                ] = int(payload["recv_us"])
                continue
            if kind == "gap" and payload.get("channel") in {
                "connection",
                "order_book",
            }:
                prior_gap_close_us = ordered_gap_close_us.get(
                    (payload["venue"], payload["market_id"])
                )
                if prior_gap_close_us is not None:
                    payload = {
                        **payload,
                        "prior_gap_close_us": prior_gap_close_us,
                    }
            partition_us = int(payload.get("partition_us", payload["recv_us"]))
            grouped[partition_for_us(partition_us)].append((kind, payload))

        late_commands: list[tuple[str, dict[str, Any]]] = []
        pending_late_primary: set[tuple[str, str, int, str]] = set()
        pending_late_aliases: defaultdict[
            tuple[str, str, int, str | None], set[str]
        ] = defaultdict(set)
        for partition, commands in grouped.items():
            with self._partition_lock(partition):
                if self._is_partition_sealed(partition):
                    regular_commands = []
                    for kind, payload in commands:
                        if kind != "trade":
                            raise RuntimeError(
                                f"non-trade command targeted sealed partition {partition}: {kind}"
                            )
                        primary_key = (
                            partition,
                            payload["venue"],
                            payload["market_id"],
                            payload["trade_id"],
                        )
                        replay_alias = payload.get("replay_alias")
                        alias_key = (
                            partition,
                            payload["venue"],
                            payload["market_id"],
                            replay_alias,
                        )
                        archived = self._archived_trade_exists(
                            partition, payload
                        )
                        pending_alias_replay = False
                        if (
                            not archived
                            and replay_alias is not None
                            and payload.get("snapshot_occurrence") is not None
                        ):
                            sealed_alias_ids = (
                                self._sealed_trade_replay_alias_ids(
                                    partition, payload
                                )
                            )
                            _, local_alias_ids = self._local_late_trade_state(
                                partition, payload
                            )
                            pending_alias_replay = (
                                sealed_alias_ids is None
                                or len(
                                    sealed_alias_ids
                                    | local_alias_ids
                                    | pending_late_aliases[alias_key]
                                )
                                > int(payload["snapshot_occurrence"])
                            )
                        if (
                            archived
                            or primary_key in pending_late_primary
                            or pending_alias_replay
                        ):
                            LOG.debug(
                                "Discarding replayed archived trade venue=%s market_id=%s "
                                "trade_id=%s sealed_partition=%s",
                                payload["venue"],
                                payload["market_id"],
                                payload["trade_id"],
                                partition,
                            )
                            continue
                        late_payload = dict(payload)
                        late_payload["sealed_partition"] = partition
                        late_commands.append(("late_trade", late_payload))
                        pending_late_primary.add(primary_key)
                        if replay_alias is not None:
                            pending_late_aliases[alias_key].add(
                                payload["trade_id"]
                            )
                    commands = regular_commands
                if commands:
                    self._write_partition(partition, commands)

        if late_commands:
            late_partition = partition_for_us(now_us())
            with self._partition_lock(late_partition):
                if self._is_partition_sealed(late_partition):
                    raise RuntimeError(f"active late-trade partition is sealed: {late_partition}")
                self._write_partition(late_partition, late_commands)

        write_us = now_us()
        current_partition = partition_for_us(write_us)
        marker_paths = [marker_path for marker_path, _ in recovered_gap_markers]
        session_marker_paths = [
            marker_path for marker_path, _, _, _ in recovered_session_markers
        ]
        rotation_queue = sorted(
            partition
            for partition in self._connections
            if partition < current_partition
        )
        rotated: set[str] = set()
        while rotation_queue:
            partition = rotation_queue.pop(0)
            if partition in rotated:
                continue
            connection = self._connections.get(partition)
            if connection is None:
                continue
            continuation_commands: list[tuple[str, dict[str, Any]]] = []
            created_gap_markers: list[Path] = []
            partition_ended_us = partition_start_us(partition) + 3_600_000_000
            with self._partition_lock(partition):
                self._finalize_ohlcv(connection, write_us)
                for session_id, venue, api_schema_version in connection.execute(
                    """SELECT connection_session_id, venue, api_schema_version
                       FROM ws_connection
                       WHERE ended_ts_recv_us IS NULL
                         AND is_physical = 1
                         AND started_ts_recv_us < ?""",
                    (partition_ended_us,),
                ):
                    session_continuation = {
                        "continuation_id": f"partition:{partition}:{session_id}",
                        "start_us": partition_ended_us,
                        "source_partition": partition,
                        "source_collector_run_id": self.collector_run_id,
                        "connection": {
                            "id": session_id,
                            "venue": venue,
                            "started_us": partition_ended_us,
                            "api_schema_version": api_schema_version,
                        },
                    }
                    session_marker_paths.append(
                        self._persist_session_continuation_marker(
                            session_continuation
                        )
                    )
                    continuation_commands.append(
                        (
                            "connection_start",
                            {
                                "recv_us": partition_ended_us,
                                "partition_us": partition_ended_us,
                                "connection": session_continuation["connection"],
                            },
                        )
                    )
                for row in connection.execute(
                    """SELECT gap_id, venue, market_id, symbol, channel,
                              expected_sequence, observed_sequence, reason
                       FROM data_gap WHERE ts_end_us IS NULL"""
                ):
                    gap_id, venue, market_id, symbol, channel, expected, observed, reason = row
                    continuation = {
                        "recv_us": write_us,
                        "start_us": partition_ended_us,
                        "partition_us": partition_ended_us,
                        "connection_id": None,
                        "venue": venue,
                        "market_id": market_id,
                        "symbol": symbol,
                        "channel": channel,
                        "expected_sequence": expected,
                        "observed_sequence": observed,
                        "continuation_id": f"partition:{partition}:{gap_id}",
                        "reason": reason,
                        "source_partition": partition,
                        "source_gap_id": gap_id,
                    }
                    created_gap_markers.append(
                        self._persist_gap_continuation_marker(continuation)
                    )
                    continuation_commands.append(("gap", continuation))
                connection.execute(
                    """UPDATE data_gap SET ts_end_us = MAX(ts_start_us, ?)
                       WHERE ts_end_us IS NULL""",
                    (partition_ended_us,),
                )
                connection.execute(
                    """UPDATE ws_connection
                       SET ended_ts_recv_us = MAX(started_ts_recv_us, ?),
                           end_reason = 'partition_rotation'
                       WHERE ended_ts_recv_us IS NULL AND is_physical = 1""",
                    (partition_ended_us,),
                )
                connection.commit()
                self._connections.pop(partition).close()

            destination_partition = partition_for_us(partition_ended_us)
            if continuation_commands:
                with self._partition_lock(destination_partition):
                    self._write_partition(destination_partition, continuation_commands)
            marker_paths.extend(created_gap_markers)
            rotated.add(partition)
            if destination_partition < current_partition:
                rotation_queue.append(destination_partition)
                rotation_queue.sort()

        for partition, connection in list(self._connections.items()):
            with self._partition_lock(partition):
                self._finalize_ohlcv(connection, write_us)
                connection.commit()

        for payload in sorted(
            gap_close_markers.values(),
            key=lambda marker: int(marker["recv_us"]),
        ):
            self._close_open_gaps(payload)

        for marker_path in gap_close_markers:
            marker_path.unlink(missing_ok=True)
        if gap_close_markers:
            self._fsync_directory(self.gap_close_dir)
        for marker_path in dict.fromkeys(marker_paths):
            marker_path.unlink(missing_ok=True)
        if marker_paths:
            self._fsync_directory(self.gap_continuation_dir)
        for marker_path in dict.fromkeys(session_marker_paths):
            marker_path.unlink(missing_ok=True)
        if session_marker_paths:
            self._fsync_directory(self.session_continuation_dir)

    def _complete_session_source_close(self, marker: dict[str, Any]) -> None:
        source_partition = marker.get("source_partition")
        connection_meta = marker.get("connection")
        if not isinstance(source_partition, str) or not isinstance(connection_meta, dict):
            raise RuntimeError("session continuation source identity is invalid")
        try:
            if partition_for_us(partition_start_us(source_partition)) != source_partition:
                raise ValueError(source_partition)
        except ValueError as exc:
            raise RuntimeError("session continuation source partition is invalid") from exc
        if self._is_partition_sealed(source_partition):
            return
        database_path = self.config.database_dir / f"engine_b_phase0_{source_partition}.sqlite3"
        if not database_path.is_file():
            return
        with self._partition_lock(source_partition):
            connection = self._connections.get(source_partition)
            borrowed = connection is not None
            if connection is None:
                connection = sqlite3.connect(
                    f"file:{database_path}?mode=rw", uri=True, timeout=5
                )
            try:
                boundary_us = int(marker["start_us"])
                connection.execute("BEGIN")
                connection.execute(
                    """UPDATE ws_connection
                       SET ended_ts_recv_us = MAX(started_ts_recv_us, ?),
                           end_reason = 'partition_rotation'
                       WHERE connection_session_id = ?
                         AND ended_ts_recv_us IS NULL""",
                    (boundary_us, connection_meta["id"]),
                )
                connection.commit()
            except sqlite3.DatabaseError as exc:
                connection.rollback()
                raise RuntimeError(
                    f"session continuation source is unreadable: {database_path}"
                ) from exc
            finally:
                if not borrowed:
                    connection.close()

    def _load_session_continuations(
        self, excluded_paths: set[Path] | None = None,
    ) -> list[
        tuple[
            Path,
            tuple[str, dict[str, Any]],
            tuple[str, dict[str, Any]] | None,
            list[dict[str, Any]],
        ]
    ]:
        if not self.session_continuation_dir.is_dir():
            return []
        recovered = []
        for marker_path in sorted(self.session_continuation_dir.glob("*.json")):
            if excluded_paths is not None and marker_path in excluded_paths:
                continue
            try:
                marker = json.loads(marker_path.read_text())
            except (OSError, json.JSONDecodeError) as exc:
                raise RuntimeError(
                    f"session continuation marker is unreadable: {marker_path}"
                ) from exc
            if (
                not isinstance(marker, dict)
                or not {"continuation_id", "start_us", "source_partition", "connection"}
                <= marker.keys()
                or not isinstance(marker["connection"], dict)
                or (
                    marker.get("source_collector_run_id") is not None
                    and not isinstance(marker["source_collector_run_id"], str)
                )
            ):
                raise RuntimeError(
                    f"session continuation marker is invalid: {marker_path}"
                )
            self._complete_session_source_close(marker)
            start_us = int(marker["start_us"])
            destination_partition = partition_for_us(start_us)
            connection_meta = dict(marker["connection"])
            preserve_open = (
                marker.get("source_collector_run_id") == self.collector_run_id
            )
            sealed_intervals: list[dict[str, Any]] = []
            sealed_restart_gap_intervals: list[dict[str, Any]] = []
            while self._is_partition_sealed(destination_partition):
                archived = self._sealed_connection_session_exists(
                    destination_partition, str(connection_meta["id"])
                )
                destination_end_us = (
                    partition_start_us(destination_partition) + 3_600_000_000
                )
                LOG.warning(
                    "Advancing recovered session continuation past sealed "
                    "partition %s archived=%s",
                    destination_partition,
                    archived,
                )
                interval = {
                    "sealed_partition": destination_partition,
                    "start_us": start_us,
                    "end_us": destination_end_us,
                }
                if preserve_open:
                    if not archived:
                        interval_id = "sealed-session-missing:" + hashlib.sha256(
                            (
                                f"{marker['continuation_id']}:"
                                f"{destination_partition}"
                            ).encode("utf-8")
                        ).hexdigest()
                        sealed_intervals.append(
                            {"interval_id": interval_id, **interval}
                        )
                else:
                    sealed_restart_gap_intervals.append(interval)
                start_us = destination_end_us
                destination_partition = partition_for_us(start_us)
            connection_meta["started_us"] = start_us
            recovered_us = now_us()
            destination_exists = (
                preserve_open
                or self._retained_connection_session_exists(
                    destination_partition, str(connection_meta["id"])
                )
            )
            recovery_reason = (
                "partition_rotation"
                if preserve_open
                else "collector_restart_recovery"
            )
            restart_gaps: list[dict[str, Any]] = []
            if not preserve_open and (
                not destination_exists or sealed_restart_gap_intervals
            ):
                venue_config = next(
                    (
                        venue
                        for venue in self.config.venues
                        if venue.name == connection_meta["venue"]
                    ),
                    None,
                )
                if venue_config is None:
                    # Same reasoning as _recover_orphaned_sessions: a venue
                    # rename/removal leaves continuation markers referencing
                    # a name no longer in the current config. The
                    # connection_start/connection_end pair below is built
                    # from connection_meta alone and does not need
                    # venue_config; only restart-gap bookkeeping per current
                    # market does, so leave restart_gaps empty for this
                    # marker instead of failing the whole startup.
                    LOG.warning(
                        "Session continuation marker %s references venue=%s, not in "
                        "current config (renamed/removed); recovering the connection "
                        "without restart-gap bookkeeping for it",
                        marker_path,
                        connection_meta["venue"],
                    )
                    venue_config = None
                else:
                    for market in venue_config.markets:
                        continuation_hash = hashlib.sha256(
                            (
                                f"{marker['continuation_id']}:restart-gap:"
                                f"{destination_partition}:{market.market_id}"
                            ).encode("utf-8")
                        ).hexdigest()
                        gap_intervals = [
                            {
                                "interval_id": "sealed-session-gap:"
                                + hashlib.sha256(
                                    (
                                        f"{marker['continuation_id']}:"
                                        f"{interval['sealed_partition']}:"
                                        f"{market.market_id}"
                                    ).encode("utf-8")
                                ).hexdigest(),
                                **interval,
                            }
                            for interval in sealed_restart_gap_intervals
                        ]
                        restart_gaps.append(
                            {
                                "recv_us": recovered_us,
                                "partition_us": start_us,
                                "connection_id": connection_meta["id"],
                                "venue": venue_config.name,
                                "market_id": market.market_id,
                                "symbol": market.symbol,
                                "channel": "connection",
                                "start_us": start_us,
                                "end_us": start_us if destination_exists else None,
                                "continuation_id": (
                                    f"session-restart:{continuation_hash}"
                                ),
                                "reason": recovery_reason,
                                "sealed_intervals": gap_intervals,
                            }
                        )
            recovered.append(
                (
                    marker_path,
                    (
                        "connection_start",
                        {
                            "recv_us": start_us,
                            "partition_us": start_us,
                            "connection": connection_meta,
                            "sealed_intervals": sealed_intervals,
                            "recovery_reason": recovery_reason,
                        },
                    ),
                    None
                    if destination_exists
                    else (
                        "connection_end",
                        {
                            "recv_us": start_us,
                            "reason": recovery_reason,
                            "connection": connection_meta,
                        },
                    ),
                    restart_gaps,
                )
            )
        return recovered

    def _complete_gap_source_close(self, marker: dict[str, Any]) -> None:
        source_partition = marker.get("source_partition")
        source_gap_id = marker.get("source_gap_id")
        if source_partition is None and source_gap_id is None:
            return
        if not isinstance(source_partition, str) or not isinstance(source_gap_id, int):
            raise RuntimeError("gap continuation source identity is invalid")
        try:
            if partition_for_us(partition_start_us(source_partition)) != source_partition:
                raise ValueError(source_partition)
        except ValueError as exc:
            raise RuntimeError("gap continuation source partition is invalid") from exc
        if self._is_partition_sealed(source_partition):
            return
        database_path = (
            self.config.database_dir
            / f"engine_b_phase0_{source_partition}.sqlite3"
        )
        if not database_path.is_file():
            return
        with self._partition_lock(source_partition):
            connection = self._connections.get(source_partition)
            borrowed = connection is not None
            if connection is None:
                try:
                    connection = sqlite3.connect(
                        f"file:{database_path}?mode=rw", uri=True, timeout=5
                    )
                except sqlite3.OperationalError as exc:
                    if not database_path.exists():
                        return
                    raise RuntimeError(
                        f"gap continuation source is unreadable: {database_path}"
                    ) from exc
            try:
                boundary_us = int(marker["start_us"])
                connection.execute("BEGIN")
                connection.execute(
                    """UPDATE data_gap SET ts_end_us = MAX(ts_start_us, ?)
                       WHERE gap_id = ? AND ts_end_us IS NULL""",
                    (boundary_us, source_gap_id),
                )
                connection.execute(
                    """UPDATE ws_connection
                       SET ended_ts_recv_us = MAX(started_ts_recv_us, ?),
                           end_reason = 'partition_rotation'
                       WHERE ended_ts_recv_us IS NULL""",
                    (boundary_us,),
                )
                connection.commit()
            except sqlite3.DatabaseError as exc:
                connection.rollback()
                raise RuntimeError(
                    f"gap continuation source is unreadable: {database_path}"
                ) from exc
            finally:
                if not borrowed:
                    connection.close()

    def _load_gap_continuations(self) -> list[tuple[Path, dict[str, Any]]]:
        if not self.gap_continuation_dir.is_dir():
            return []
        recovered: list[tuple[Path, dict[str, Any]]] = []
        required = {
            "continuation_id",
            "start_us",
            "venue",
            "market_id",
            "symbol",
            "channel",
            "reason",
        }
        for marker_path in sorted(self.gap_continuation_dir.glob("*.json")):
            try:
                marker = json.loads(marker_path.read_text())
            except (OSError, json.JSONDecodeError) as exc:
                raise RuntimeError(
                    f"gap continuation marker is unreadable: {marker_path}"
                ) from exc
            if not isinstance(marker, dict) or not required <= marker.keys():
                raise RuntimeError(
                    f"gap continuation marker is invalid: {marker_path}"
                )
            self._complete_gap_source_close(marker)
            payload = dict(marker)
            payload["recv_us"] = now_us()
            payload["connection_id"] = None
            destination_us = int(marker["start_us"])
            continuation_id = str(marker["continuation_id"])
            sealed_intervals: list[dict[str, Any]] = []
            while self._is_partition_sealed(partition_for_us(destination_us)):
                sealed_partition = partition_for_us(destination_us)
                archived_gap_id = self._sealed_gap_continuation_gap_id(
                    sealed_partition, continuation_id
                )
                LOG.warning(
                    "Advancing recovered gap continuation past sealed partition %s",
                    sealed_partition,
                )
                sealed_end_us = partition_start_us(sealed_partition) + 3_600_000_000
                if archived_gap_id is not None:
                    continuation_id = (
                        f"partition:{sealed_partition}:{archived_gap_id}"
                    )
                else:
                    interval_id = "sealed-missing:" + hashlib.sha256(
                        f"{continuation_id}:{sealed_partition}".encode("utf-8")
                    ).hexdigest()
                    sealed_intervals.append(
                        {
                            "interval_id": interval_id,
                            "sealed_partition": sealed_partition,
                            "start_us": destination_us,
                            "end_us": sealed_end_us,
                        }
                    )
                    continuation_id = "sealed-skip:" + hashlib.sha256(
                        f"{continuation_id}:{sealed_partition}".encode("utf-8")
                    ).hexdigest()
                destination_us = sealed_end_us
            payload["continuation_id"] = continuation_id
            payload["start_us"] = destination_us
            payload["partition_us"] = destination_us
            payload["sealed_intervals"] = sealed_intervals
            recovered.append((marker_path, payload))
        return recovered

    def _close_open_gaps(self, payload: dict[str, Any]) -> None:
        for database_path in sorted(
            self.config.database_dir.glob("engine_b_phase0_*.sqlite3")
        ):
            partition = database_path.stem.removeprefix("engine_b_phase0_")
            with self._partition_lock(partition):
                connection = self._connections.get(partition)
                borrowed = connection is not None
                if connection is None:
                    try:
                        connection = sqlite3.connect(
                            f"file:{database_path}?mode=rw", uri=True, timeout=5
                        )
                    except sqlite3.OperationalError as exc:
                        if not database_path.exists():
                            continue
                        raise RuntimeError(
                            f"data-gap journal is unreadable: {database_path}"
                        ) from exc
                try:
                    table = connection.execute(
                        """SELECT 1 FROM sqlite_master
                           WHERE type = 'table' AND name = 'data_gap'"""
                    ).fetchone()
                    if table is not None:
                        connection.execute(
                            """UPDATE data_gap
                               SET ts_end_us = MAX(ts_start_us, ?)
                               WHERE ts_end_us IS NULL AND venue = ?
                                 AND market_id = ?
                                 AND ts_start_us <= ?
                                 AND channel IN ('connection', 'order_book')""",
                            (
                                payload["recv_us"],
                                payload["venue"],
                                payload["market_id"],
                                payload["recv_us"],
                            ),
                        )
                        connection.commit()
                except sqlite3.DatabaseError as exc:
                    connection.rollback()
                    raise RuntimeError(
                        f"data-gap journal is unreadable: {database_path}"
                    ) from exc
                finally:
                    if not borrowed:
                        connection.close()

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
            for interval in payload.get("sealed_intervals", []):
                connection.execute(
                    """INSERT OR IGNORE INTO sealed_session_interval(
                         interval_id, sealed_partition, connection_session_id,
                         venue, ts_start_us, ts_end_us, api_schema_version, reason
                       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        interval["interval_id"],
                        interval["sealed_partition"],
                        payload["connection"]["id"],
                        payload["connection"]["venue"],
                        interval["start_us"],
                        interval["end_us"],
                        payload["connection"]["api_schema_version"],
                        payload.get(
                            "recovery_reason", "collector_restart_recovery"
                        ),
                    ),
                )
        elif kind == "connection_activity":
            connection.execute(
                """UPDATE ws_connection
                   SET last_activity_ts_recv_us = MAX(
                         last_activity_ts_recv_us, ?
                       )
                   WHERE connection_session_id = ? AND is_physical = 1""",
                (payload["recv_us"], payload["connection"]["id"]),
            )
        elif kind == "connection_end":
            connection.execute(
                """UPDATE ws_connection SET ended_ts_recv_us = ?, end_reason = ?
                   WHERE connection_session_id = ?
                     AND ended_ts_recv_us IS NULL AND is_physical = 1""",
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
                     raw_public_json, replay_alias
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    payload["connection"]["id"], payload["venue"], payload["market_id"],
                    payload["symbol"], payload["trade_id"], payload.get("exchange_sequence"),
                    payload["local_sequence"], payload["recv_us"], payload.get("srv_us"),
                    payload["event_ts_us"], payload["sealed_partition"], payload["price"],
                    payload["size"], payload.get("aggressor_side"), payload["raw_public_json"],
                    payload.get("replay_alias"),
                ),
            )
        elif kind == "trade":
            replay_alias = payload.get("replay_alias")
            snapshot_occurrence = payload.get("snapshot_occurrence")
            if replay_alias is not None and snapshot_occurrence is not None:
                existing_alias_count = connection.execute(
                    """SELECT COUNT(*) FROM trade_replay_alias
                       WHERE venue = ? AND market_id = ? AND replay_alias = ?""",
                    (payload["venue"], payload["market_id"], replay_alias),
                ).fetchone()[0]
                if int(existing_alias_count) > int(snapshot_occurrence):
                    return
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
                if replay_alias is not None:
                    connection.execute(
                        """INSERT INTO trade_replay_alias(
                             venue, market_id, replay_alias, exchange_trade_id
                           ) VALUES (?, ?, ?, ?)""",
                        (
                            payload["venue"],
                            payload["market_id"],
                            replay_alias,
                            payload["trade_id"],
                        ),
                    )
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
            for interval in payload.get("sealed_intervals", []):
                connection.execute(
                    """INSERT OR IGNORE INTO sealed_gap_interval(
                         interval_id, sealed_partition, venue, market_id, symbol,
                         channel, ts_start_us, ts_end_us, expected_sequence,
                         observed_sequence, reason
                       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        interval["interval_id"], interval["sealed_partition"],
                        payload["venue"], payload.get("market_id"),
                        payload.get("symbol"), payload["channel"],
                        interval["start_us"], interval["end_us"],
                        payload.get("expected_sequence"),
                        payload.get("observed_sequence"), payload["reason"],
                    ),
                )
            if payload.get("prior_gap_close_us") is not None:
                connection.execute(
                    """UPDATE data_gap
                       SET ts_end_us = MAX(ts_start_us, ?)
                       WHERE ts_end_us IS NULL AND venue = ?
                         AND market_id = ? AND channel = ?
                         AND ts_start_us <= ?""",
                    (
                        payload["prior_gap_close_us"],
                        payload["venue"],
                        payload["market_id"],
                        payload["channel"],
                        payload["prior_gap_close_us"],
                    ),
                )
            connection.execute(
                """INSERT OR IGNORE INTO data_gap(
                     connection_session_id, venue, market_id, symbol, channel, ts_start_us,
                     ts_end_us, expected_sequence, observed_sequence, continuation_id,
                     reason
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    payload.get("connection_id"), payload["venue"], payload.get("market_id"),
                    payload.get("symbol"), payload["channel"],
                    payload.get("start_us", payload["recv_us"]),
                    payload.get("end_us"), payload.get("expected_sequence"),
                    payload.get("observed_sequence"), payload.get("continuation_id"),
                    payload["reason"],
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
        # bot-strategy#908 watchdog state, keyed by (venue, market_id).
        self.book_subscribe_us: dict[tuple[str, int], int] = {}
        self.book_last_recv_us: dict[tuple[str, int], int] = {}
        self.market_last_activity_us: dict[tuple[str, int], int] = {}
        self.last_book_watchdog_us: dict[str, int] = {}
        # Runtime enforcement of WATCHDOG_MAX_FRAMES_PER_MIN, independent of
        # config: timestamps (us) of watchdog-sent frames per venue.
        self.watchdog_frames_us: defaultdict[str, deque[int]] = defaultdict(deque)
        # When each market last spent its reserved sequence-break recovery
        # (one unsubscribe+subscribe per market per rolling minute).
        self.break_reserve_used_us: dict[tuple[str, int], int] = {}
        # Wall clock for send-time stamps; tests pin it to 0 so stamps fall
        # back to the message clock they drive.
        self._clock = now_us
        self.market_stats_rejections: defaultdict[tuple[str, int], int] = defaultdict(int)
        self.local_sequences: defaultdict[tuple[str, str, int], int] = defaultdict(int)
        self.last_health_error: str | None = None
        self.trading_calendar = (
            TradingCalendar.load(config.trading_calendar_file)
            if config.trading_calendar_file is not None
            else None
        )
        if self.trading_calendar is None:
            LOG.warning("A-7 trading calendar not loaded; session rows remain fail-closed")

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
        from websockets.asyncio.client import connect

        backoff = 1
        while not self.stop_event.is_set():
            attempt_started_us = now_us()
            connection = {
                "id": str(uuid.uuid4()),
                "venue": venue.name,
                "api_schema_version": self.config.api_schema_version,
            }
            connected = False
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
                    connection["started_us"] = now_us()
                    await self.sink.put(
                        "connection_start",
                        {
                            "recv_us": connection["started_us"],
                            "connection": connection,
                        },
                    )
                    connected = True
                    LOG.info("WebSocket connected venue=%s url=%s", venue.name, venue.ws_url)
                    self.metrics.feed_connected[venue.name] = True
                    backoff = 1
                    async for raw_message in ws:
                        recv_us = now_us()
                        self.metrics.last_message_us[venue.name] = recv_us
                        await self.sink.put(
                            "connection_activity",
                            {"recv_us": recv_us, "connection": connection},
                        )
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
                        await self.watchdog_books(ws, venue, connection, recv_us)
                        if self.stop_event.is_set():
                            break
            except asyncio.CancelledError:
                # Attribute the cancel: a real shutdown has stop_event set;
                # anything else is a cancel from an unexpected source and is
                # labelled as such so the data_gap reason stays honest
                # (bot-strategy#908 item 3).
                if self.stop_event.is_set():
                    reason = "task_cancelled"
                else:
                    reason = "task_cancelled_unexpected"
                    LOG.warning(
                        "feed_loop cancelled without stop_event venue=%s connection=%s",
                        venue.name,
                        connection["id"],
                        exc_info=True,
                    )
                raise
            except Exception as exc:
                reason = f"connection_error:{type(exc).__name__}"
                self.metrics.inc("engine_b_phase0_reconnect_total", {"venue": venue.name})
                LOG.exception("WebSocket failure venue=%s; reconnect in %ss", venue.name, backoff)
            finally:
                self.metrics.feed_connected[venue.name] = False
                ended_us = now_us()
                if connected:
                    await self.sink.put(
                        "connection_end",
                        {
                            "recv_us": ended_us,
                            "connection": connection,
                            "reason": reason,
                        },
                    )
                for market in venue.markets:
                    state = self.books.setdefault((venue.name, market.market_id), BookState())
                    state.synced = False
                    self.metrics.book_synced[(venue.name, market.market_id)] = False
                    await self.sink.put(
                        "gap",
                        {
                            "recv_us": ended_us,
                            "start_us": (
                                ended_us if connected else attempt_started_us
                            ),
                            "partition_us": (
                                ended_us if connected else attempt_started_us
                            ),
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

    @staticmethod
    def watchdog_share(venue: VenueConfig) -> int:
        """Frames per rolling minute available to the reconnect burst and the
        silence-inferred watchdog; the rest (2 per market) is reserved for
        proven sequence breaks."""
        return WATCHDOG_MAX_FRAMES_PER_MIN - 2 * len(venue.markets)

    async def wait_for_frame_budget(self, venue: VenueConfig, frames: int) -> None:
        """Block until ``frames`` more (re)subscribe frames fit under the
        watchdog share of the rolling per-minute cap, so a run of short-lived
        connections cannot push the reconnect bursts past the exchange limit
        (bot-strategy#908)."""
        limit = self.watchdog_share(venue)
        if frames > limit:
            raise ValueError(f"{frames} frames can never fit the watchdog share of {limit}")
        while not self.stop_event.is_set():
            current_us = self._clock()
            sent = self.watchdog_frames_us[venue.name]
            while sent and current_us - sent[0] > 60_000_000:
                sent.popleft()
            room = limit - len(sent)
            if room >= frames:
                return
            # Oldest frame expires first; sleep until enough of them have
            # (`sent` is non-empty here because frames <= limit and room < frames).
            need = frames - room
            expiry_us = sent[min(need, len(sent)) - 1] + 60_000_000
            wait_s = max(0.2, min(5.0, (expiry_us - current_us) / 1_000_000))
            LOG.warning(
                "delaying %d subscribe frames venue=%s for %.1fs: %d frames already sent in the last minute",
                frames,
                venue.name,
                wait_s,
                len(sent),
            )
            self.metrics.inc("engine_b_phase0_book_watchdog_throttled_total", {"venue": venue.name})
            await asyncio.sleep(wait_s)

    async def subscribe_public_channels(self, ws: Any, venue: VenueConfig) -> None:
        sent = self.watchdog_frames_us[venue.name]
        for market in venue.markets:
            # Chunked per market (3 frames) against the rolling budget, so any
            # market count the exchange allows (500 subscriptions/connection)
            # subscribes in budget-sized steps instead of one unbounded burst
            # (bot-strategy#908).
            await self.wait_for_frame_budget(venue, 3)
            for channel in ("order_book", "trade", "market_stats"):
                await send_public_control(
                    ws, {"type": "subscribe", "channel": f"{channel}/{market.market_id}"}
                )
                # Stamp each frame as it is actually sent (a backpressured
                # send must not make the frame look older than it is), and
                # take the book's retry deadline from its own send. The
                # initial subscriptions draw from the same rolling frame
                # budget the watchdog uses, so a reconnect burst plus recovery
                # frames together stay within the observer's share of
                # Lighter's per-IP client-message limit.
                sent_us = self._clock()
                sent.append(sent_us)
                if channel == "order_book":
                    self.book_subscribe_us[(venue.name, market.market_id)] = sent_us
        LOG.info("Subscribed to public channels venue=%s markets=%d", venue.name, len(venue.markets))

    async def resubscribe_book(
        self,
        ws: Any,
        venue: VenueConfig,
        market_id: int,
        recv_us: int,
        *,
        unsubscribe_first: bool,
        reason: str,
        bypass_spacing: bool = False,
    ) -> bool:
        """Single choke point for every order_book (re)subscribe frame this
        process sends after the initial subscription (bot-strategy#908):
        spaced at least ``book_resubscribe_after_ms`` per market and capped
        at ``WATCHDOG_MAX_FRAMES_PER_MIN`` frames per venue per rolling
        minute, whichever path asks -- the watchdog or ``handle_book``'s
        sequence-break handler (a burst of already-queued deltas against an
        unsynced book must not turn into a burst of subscribe frames).
        ``bypass_spacing`` is for the *first* recovery after a proven
        sequence break on a synced book, which must go out immediately (as
        the pre-watchdog code did); follow-up attempts keep the spacing.
        Returns True when frames were sent."""
        key = (venue.name, market_id)
        last_subscribe_us = self.book_subscribe_us.get(key)
        if (
            not bypass_spacing
            and last_subscribe_us is not None
            and recv_us - last_subscribe_us < self.config.book_resubscribe_after_ms * 1_000
        ):
            LOG.debug(
                "suppressing order_book re-subscribe venue=%s market_id=%s (%s): last subscribe %.1fs ago",
                venue.name,
                market_id,
                reason,
                (recv_us - last_subscribe_us) / 1_000_000,
            )
            return False
        # Stamp at send time, not with the message's receive time: awaited
        # sink/WS calls before this point can be delayed under backpressure,
        # and a stale stamp would age out of the window (and re-arm the retry
        # spacing) too early.
        stamp_us = max(recv_us, self._clock())
        sent = self.watchdog_frames_us[venue.name]
        while sent and stamp_us - sent[0] > 60_000_000:
            sent.popleft()
        frames = 2 if unsubscribe_first else 1
        fits_shared = len(sent) + frames <= self.watchdog_share(venue)
        if not fits_shared:
            # The shared share is spent. A proven sequence break may use this
            # market's *own* reserved recovery (2 frames, once per rolling
            # minute), so one noisy market cannot consume the slots meant for
            # the others; silence-inferred retries never touch the reserve.
            reserve_used_us = self.break_reserve_used_us.get(key)
            reserve_free = reserve_used_us is None or stamp_us - reserve_used_us > 60_000_000
            if not (
                bypass_spacing
                and reserve_free
                and len(sent) + frames <= WATCHDOG_MAX_FRAMES_PER_MIN
            ):
                self.metrics.inc(
                    "engine_b_phase0_book_watchdog_throttled_total", {"venue": venue.name}
                )
                return False
            self.break_reserve_used_us[key] = stamp_us
        # Account each frame right after its own send: if the socket dies
        # between the two awaited sends, the unsubscribe that did go out is
        # still counted (and the retry deadline only moves once the subscribe
        # actually went out).
        if unsubscribe_first:
            await send_public_control(ws, {"type": "unsubscribe", "channel": f"order_book/{market_id}"})
            sent.append(self._clock_at_least(stamp_us))
        await send_public_control(ws, {"type": "subscribe", "channel": f"order_book/{market_id}"})
        subscribed_us = self._clock_at_least(stamp_us)
        sent.append(subscribed_us)
        self.book_subscribe_us[key] = subscribed_us
        return True

    def _clock_at_least(self, floor_us: int) -> int:
        """Send-time stamp: wall clock, never earlier than the message time
        that drove this call (tests pin the clock to 0)."""
        return max(floor_us, self._clock())

    def can_send_frames(self, venue: VenueConfig, recv_us: int, frames: int) -> bool:
        """True if ``frames`` more WS frames fit under the watchdog share of
        the rolling per-minute cap right now (no side effects)."""
        sent = self.watchdog_frames_us[venue.name]
        while sent and recv_us - sent[0] > 60_000_000:
            sent.popleft()
        return len(sent) + frames <= self.watchdog_share(venue)

    async def watchdog_books(
        self,
        ws: Any,
        venue: VenueConfig,
        connection: dict[str, Any],
        recv_us: int,
    ) -> None:
        """Per-market order_book liveness (bot-strategy#908 items 2 and 6).

        Runs at most once per second per venue, from the message loop, so it
        needs some other channel to still be delivering -- which is exactly
        the failure shape it targets: the socket is alive but one market's
        book is not. Two cases:

        * a market still unsynced ``book_resubscribe_after_ms`` after its last
          ``subscribe`` (the post-reconnect snapshot never arrived, e.g. the
          subscribe frame was dropped under Lighter's client-message limit)
          gets its ``order_book`` channel re-subscribed;
        * a market that is synced but whose book has been silent for
          ``book_stall_after_ms`` while its own trade / market_stats channels
          kept delivering is declared stale: unsynced, an ``order_book``
          ``data_gap`` (``reason=book_channel_stalled``) is opened, and the
          channel is re-subscribed.

        At most ``book_watchdog_batch`` subscribe frames per call, to stay
        well inside the per-IP client-message budget.
        """
        last = self.last_book_watchdog_us.get(venue.name, 0)
        if recv_us - last < 1_000_000:
            return
        self.last_book_watchdog_us[venue.name] = recv_us
        budget = self.config.book_watchdog_batch
        resubscribe_after_us = self.config.book_resubscribe_after_ms * 1_000
        stall_after_us = self.config.book_stall_after_ms * 1_000
        # Oldest subscribe deadline first, so a batch smaller than the number
        # of unsynced markets rotates through all of them instead of retrying
        # the first `book_watchdog_batch` in config order forever (a
        # re-subscribed market gets the newest timestamp and moves to the
        # back of the line).
        ordered_markets = sorted(
            venue.markets,
            key=lambda m: self.book_subscribe_us.get((venue.name, m.market_id), 0),
        )
        for market in ordered_markets:
            if budget <= 0:
                return
            key = (venue.name, market.market_id)
            state = self.books.setdefault(key, BookState())
            subscribed_us = self.book_subscribe_us.get(key)
            if subscribed_us is None:
                continue
            if not state.synced:
                if recv_us - subscribed_us < resubscribe_after_us:
                    continue
                LOG.warning(
                    "order_book still unsynced %.0fs after subscribe venue=%s symbol=%s; re-subscribing",
                    (recv_us - subscribed_us) / 1_000_000,
                    venue.name,
                    market.symbol,
                )
                if await self.resubscribe_book(
                    ws, venue, market.market_id, recv_us, unsubscribe_first=False, reason="unsynced"
                ):
                    self.metrics.inc(
                        "engine_b_phase0_book_resubscribe_total",
                        {"venue": venue.name, "symbol": market.symbol, "reason": "unsynced"},
                    )
                    budget -= 1
                else:
                    return  # per-minute cap reached; nothing more this pass
                continue
            last_book_us = self.book_last_recv_us.get(key)
            last_activity_us = self.market_last_activity_us.get(key)
            # "Stalled" needs the market's *own* other channels to be alive
            # right now, not merely once since the last book message: a
            # market that went quiet on every channel is quiet, not stalled.
            if (
                last_book_us is None
                or last_activity_us is None
                or last_activity_us <= last_book_us
                or recv_us - last_activity_us > stall_after_us // 2
                or recv_us - last_book_us < stall_after_us
                # Never invalidate a book we could not re-subscribe right now
                # (per-market spacing, or the per-minute frame cap); a stall is
                # inferred from silence, so collector-imposed downtime without
                # a recovery frame is worse than waiting one more pass.
                or recv_us - subscribed_us < resubscribe_after_us
                or not self.can_send_frames(venue, recv_us, 2)
            ):
                continue
            # The gap row is written to the partition that detects it, so an
            # hour that may already be sealed never receives a new non-trade
            # row (`_write_batch` raises on that). The part of the stall that
            # lies in earlier hours is not dropped: it is recorded, hour by
            # hour, as `sealed_gap_interval` rows in the detecting partition
            # -- the same representation the gap-continuation machinery uses
            # for intervals whose home partition is unavailable.
            partition_floor_us = partition_start_us(partition_for_us(recv_us))
            gap_start_us = max(last_book_us, partition_floor_us)
            sealed_intervals: list[dict[str, Any]] = []
            cursor_us = last_book_us
            while cursor_us < partition_floor_us:
                hour_partition = partition_for_us(cursor_us)
                hour_end_us = partition_start_us(hour_partition) + 3_600_000_000
                interval_end_us = min(hour_end_us, partition_floor_us)
                interval_id = "stalled-book:" + hashlib.sha256(
                    f"{venue.name}:{market.market_id}:{cursor_us}:{interval_end_us}".encode("utf-8")
                ).hexdigest()
                sealed_intervals.append(
                    {
                        "interval_id": interval_id,
                        "sealed_partition": hour_partition,
                        "start_us": cursor_us,
                        "end_us": interval_end_us,
                    }
                )
                cursor_us = interval_end_us
            LOG.warning(
                "order_book channel stalled venue=%s symbol=%s: no book message since %d "
                "(%.0fs) while trade/market_stats kept arriving; marking unsynced and re-subscribing"
                "%s",
                venue.name,
                market.symbol,
                last_book_us,
                (recv_us - last_book_us) / 1_000_000,
                ""
                if gap_start_us == last_book_us
                else f" (gap row starts at partition {gap_start_us}; earlier portion recorded as {len(sealed_intervals)} sealed interval(s))",
            )
            last_accepted_nonce = state.last_nonce
            state.synced = False
            state.last_nonce = None
            state.bids.clear()
            state.asks.clear()
            self.metrics.book_synced[key] = False
            # Counts detections; the re-subscribe counter below counts only
            # frames actually sent.
            self.metrics.inc(
                "engine_b_phase0_book_stall_total",
                {"venue": venue.name, "symbol": market.symbol},
            )
            await self.sink.put(
                "gap",
                {
                    "recv_us": recv_us,
                    "start_us": gap_start_us,
                    "partition_us": recv_us,
                    "connection_id": connection["id"],
                    "venue": venue.name,
                    "market_id": market.market_id,
                    "symbol": market.symbol,
                    "channel": "order_book",
                    "expected_sequence": last_accepted_nonce,
                    "observed_sequence": None,
                    "reason": "book_channel_stalled",
                    "sealed_intervals": sealed_intervals,
                },
            )
            # The market was synced, so its last subscribe is old: the
            # spacing check passes; only the per-minute cap can refuse.
            if not await self.resubscribe_book(
                ws, venue, market.market_id, recv_us, unsubscribe_first=True, reason="stalled"
            ):
                return
            self.metrics.inc(
                "engine_b_phase0_book_resubscribe_total",
                {"venue": venue.name, "symbol": market.symbol, "reason": "stalled"},
            )
            budget -= 1

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
        levels = BookState.raw_levels(payload)
        state = self.books.setdefault((venue.name, market_id), BookState())
        self.book_last_recv_us[(venue.name, market_id)] = recv_us
        message_type = message["type"]
        local_sequence = self.next_sequence(venue.name, "order_book", market_id)
        srv_us = normalize_exchange_timestamp_us(
            payload.get("last_updated_at", message.get("last_updated_at", message.get("timestamp")))
        )
        event_kind = "snapshot" if message_type == "subscribed/order_book" else "delta"
        complete = event_kind == "snapshot" and payload.get("nonce") is not None
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
                "levels": levels,
            },
        )
        was_synced = state.synced
        if event_kind == "snapshot":
            applied = state.apply_snapshot(payload)
            expected = None
            observed = (
                str(payload["nonce"]) if payload.get("nonce") is not None else None
            )
        else:
            applied, expected, observed = state.apply_delta(payload)
        if not applied:
            self.metrics.inc("engine_b_phase0_sequence_gap_total", {"venue": venue.name, "symbol": market.symbol})
            if event_kind == "snapshot":
                reason = "snapshot_missing_nonce"
            elif payload.get("begin_nonce") is None:
                reason = "delta_missing_begin_nonce"
            elif payload.get("nonce") is None:
                reason = "delta_missing_nonce"
            else:
                reason = "begin_nonce_mismatch_or_unsynced_delta"
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
                    "reason": reason,
                },
            )
            # Through the shared limiter: a queue of stale deltas against an
            # unsynced book must not become a burst of subscribe frames
            # (bot-strategy#908); the watchdog retries if this one is
            # suppressed.
            await self.resubscribe_book(
                ws,
                venue,
                market_id,
                recv_us,
                unsubscribe_first=True,
                reason=reason,
                # A break on a book that was synced a moment ago is a proven
                # sequence error: recover immediately. Deltas that keep
                # failing against an already-unsynced book are the burst the
                # spacing exists for.
                bypass_spacing=was_synced,
            )
            self.metrics.book_synced[(venue.name, market_id)] = False
            return
        self.metrics.book_synced[(venue.name, market_id)] = state.synced
        if event_kind == "snapshot" and state.synced:
            await self.sink.put(
                "gap_close",
                {
                    "recv_us": recv_us,
                    "venue": venue.name,
                    "market_id": market_id,
                },
            )
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
        self.market_last_activity_us[(venue.name, market_id)] = recv_us
        exchange_sequence = str(message["nonce"]) if message.get("nonce") is not None else None
        message_type = str(message.get("type", "update/trade"))
        trades = [*message.get("trades", []), *message.get("liquidation_trades", [])]
        if message_type != "subscribed/trade" and exchange_sequence is None and any(
            trade.get("trade_id_str", trade.get("trade_id")) is None
            for trade in trades
        ):
            raise RuntimeError(
                "refusing ID-less incremental trade message without exchange nonce"
            )
        normalized_trades = [
            (
                trade,
                normalize_exchange_timestamp_us(
                    (
                        trade.get("timestamp")
                        if trade.get("timestamp") is not None
                        else message.get("timestamp")
                    )
                ),
            )
            for trade in trades
        ]
        for _, srv_us in normalized_trades:
            validate_trade_timestamp_us(srv_us, recv_us)
        if any(srv_us is None for _, srv_us in normalized_trades):
            raise RuntimeError("refusing trade message without exchange timestamp")
        parsed_trades: list[tuple[dict[str, Any], int | None, str, str]] = []
        for trade, srv_us in normalized_trades:
            price_text = canonical_decimal(trade["price"])
            size_text = canonical_decimal(trade["size"])
            if Decimal(price_text) <= 0 or Decimal(size_text) <= 0:
                raise RuntimeError("refusing trade with non-positive price or size")
            parsed_trades.append((trade, srv_us, price_text, size_text))
        message_scope = trade_message_scope(
            message_type, exchange_sequence, recv_us
        )
        stable_occurrences: defaultdict[tuple[int, str], int] = defaultdict(int)
        for trade, srv_us, price_text, size_text in parsed_trades:
            event_ts_us = srv_us or recv_us
            bucket_start_us = event_ts_us - event_ts_us % 60_000_000
            is_maker_ask = trade.get("is_maker_ask")
            aggressor_side = None if is_maker_ask is None else ("buy" if is_maker_ask else "sell")
            raw_public_json = json.dumps(trade, sort_keys=True, separators=(",", ":"))
            occurrence_key = (event_ts_us, raw_public_json)
            stable_occurrence = stable_occurrences[occurrence_key]
            stable_occurrences[occurrence_key] += 1
            raw_trade_id = trade.get("trade_id_str", trade.get("trade_id"))
            replay_alias = (
                None
                if raw_trade_id is not None
                else synthetic_trade_replay_alias(
                    venue.name,
                    market_id,
                    event_ts_us,
                    raw_public_json,
                )
            )
            trade_id = (
                str(raw_trade_id)
                if raw_trade_id is not None
                else synthetic_trade_id(
                    venue.name,
                    market_id,
                    event_ts_us,
                    stable_occurrence,
                    raw_public_json,
                    message_scope,
                )
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
                    "replay_alias": replay_alias,
                    "snapshot_occurrence": (
                        stable_occurrence
                        if raw_trade_id is None
                        and message_type == "subscribed/trade"
                        else None
                    ),
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
        self.market_last_activity_us[(venue.name, market_id)] = recv_us
        stats = message.get("market_stats", {})
        srv_us = normalize_exchange_timestamp_us(message.get("timestamp"))
        prices: list[tuple[str, str]] = []
        for field_name, price_type in (
            ("mark_price", "mark"),
            ("index_price", "index"),
            ("last_trade_price", "last"),
            ("mid_price", "mid"),
        ):
            value = stats.get(field_name)
            if value not in (None, ""):
                price = canonical_decimal(value)
                if Decimal(price) <= 0:
                    # Fail closed on the *message*, not on the connection:
                    # raising here used to tear down the whole venue feed
                    # (all markets) over one illiquid market's zero stat and
                    # loop through reconnects until the stat turned positive
                    # (bot-strategy#908 item 1). Record the rejection as a
                    # point gap on the market_stats channel and skip the
                    # message entirely (prices and funding alike).
                    key = (venue.name, market_id)
                    self.market_stats_rejections[key] += 1
                    count = self.market_stats_rejections[key]
                    if count == 1 or count % 100 == 0:
                        LOG.warning(
                            "rejecting market_stats with non-positive %s venue=%s symbol=%s "
                            "(rejections so far: %d)",
                            field_name,
                            venue.name,
                            market.symbol,
                            count,
                        )
                    self.metrics.inc(
                        "engine_b_phase0_market_stats_rejected_total",
                        {"venue": venue.name, "symbol": market.symbol, "field": field_name},
                    )
                    await self.sink.put(
                        "gap",
                        {
                            "recv_us": recv_us,
                            "start_us": recv_us,
                            "end_us": recv_us,
                            "partition_us": recv_us,
                            "connection_id": None,
                            "venue": venue.name,
                            "market_id": market_id,
                            "symbol": market.symbol,
                            "channel": "market_stats",
                            "expected_sequence": None,
                            "observed_sequence": None,
                            "reason": f"non_positive_{field_name}",
                        },
                    )
                    return
                prices.append((price_type, price))
        for price_type, price in prices:
            await self.sink.put(
                "price",
                {
                    "recv_us": recv_us,
                    "srv_us": srv_us,
                    "venue": venue.name,
                    "market_id": market_id,
                    "symbol": market.symbol,
                    "price_type": price_type,
                    "price": price,
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
                volume_text = canonical_decimal(
                    detail.get("daily_quote_token_volume", "0")
                )
                volume = Decimal(volume_text)
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
                        "daily_volume_usd": volume_text,
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
        except (
            OSError,
            URLError,
            ValueError,
            RuntimeError,
            KeyError,
            TypeError,
            AttributeError,
        ):
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

    @staticmethod
    def _placeholder_krx_open_close_us(session_date: date) -> tuple[int, int]:
        # Standard-schedule placeholder (09:00-15:30 KST, no DST): used only
        # when the frozen calendar has no entry for this date, or the market
        # is closed and there is no real open/close to report.
        t0 = datetime.combine(session_date, datetime_time(0, 0), UTC)
        t1 = datetime.combine(session_date, datetime_time(6, 30), UTC)
        return int(t0.timestamp() * 1_000_000), int(t1.timestamp() * 1_000_000)

    @staticmethod
    def _placeholder_us_open_us(session_date: date) -> int:
        # Standard-schedule placeholder (9:30am America/New_York); tz-aware
        # so DST still shifts it correctly even without a resolved entry.
        t2_ny = datetime.combine(session_date, datetime_time(9, 30), ZoneInfo("America/New_York"))
        return int(t2_ny.astimezone(UTC).timestamp() * 1_000_000)

    async def write_provisional_session(self, session_date: date) -> None:
        missing = sorted(
            {
                f"{venue.name}:{symbol}"
                for venue in self.config.venues
                for symbol in venue.known_missing_symbols
            }
        )

        resolved = self.trading_calendar.resolve(session_date) if self.trading_calendar else None
        reasons: list[str] = []
        placeholder_t0_us, placeholder_t1_us = self._placeholder_krx_open_close_us(session_date)
        placeholder_t2_us = self._placeholder_us_open_us(session_date)
        if resolved is None:
            krx_is_open = 0
            us_cash_is_open = 0
            calendar_version = "UNRESOLVED_A7_zoneinfo_only"
            t0_us, t1_us, t2_us = placeholder_t0_us, placeholder_t1_us, placeholder_t2_us
            reasons.append("A7_UNRESOLVED_VERIFIED_KRX_US_CALENDAR")
            if session_date.weekday() >= 5:
                reasons.append("PROVISIONAL_WEEKEND")
        else:
            krx_is_open = 1 if resolved["krx_is_open"] else 0
            us_cash_is_open = 1 if resolved["us_is_open"] else 0
            calendar_version = self.trading_calendar.calendar_version
            if resolved["krx_is_open"]:
                t0_us, t1_us = resolved["krx_open_utc_us"], resolved["krx_close_utc_us"]
            else:
                t0_us, t1_us = placeholder_t0_us, placeholder_t1_us
                reasons.append("KRX_CLOSED")
            t2_us = resolved["us_open_utc_us"] if resolved["us_is_open"] else placeholder_t2_us
            if not resolved["us_is_open"]:
                reasons.append("US_CASH_CLOSED")
        if missing:
            reasons.append("SAME_VENUE_REQUIRED_SYMBOLS_MISSING=" + ",".join(missing))

        await self.sink.put(
            "session",
            {
                "recv_us": now_us(),
                "session_id": f"provisional-{session_date.isoformat()}",
                "krx_business_date": session_date.isoformat(),
                "t0_us": t0_us,
                "t1_us": t1_us,
                "t2_us": t2_us,
                "krx_is_open": krx_is_open,
                "us_cash_is_open": us_cash_is_open,
                "calendar_version": calendar_version,
                "validity_reason": ";".join(reasons) if reasons else None,
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

    def calendar_covers_upcoming_sessions(self) -> bool:
        # Object presence alone is not enough: the frozen file's committed
        # date range eventually runs out (configs/engine-b/trading_calendar.json
        # currently ends 2027-12-31), after which every new session_loop date
        # silently falls back to fail-closed even though a calendar object is
        # still loaded. Check the dates session_loop actually writes (today,
        # tomorrow) rather than just whether `self.trading_calendar` is set.
        if self.trading_calendar is None:
            return False
        today = datetime.now(UTC).date()
        return (
            self.trading_calendar.resolve(today) is not None
            and self.trading_calendar.resolve(today + timedelta(days=1)) is not None
        )

    def health_payload(self) -> dict[str, Any]:
        current_us = now_us()
        blockers = []
        if not self.calendar_covers_upcoming_sessions():
            blockers.append("A7 verified KRX/US calendar unresolved")
        for venue in self.config.venues:
            if venue.known_missing_symbols:
                blockers.append(
                    f"{venue.name} lacks same-venue {','.join(venue.known_missing_symbols)}"
                )
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
            "phase0_sample_eligible": not blockers,
            "phase0_sample_blockers": blockers,
            "trading_calendar_version": (
                self.trading_calendar.calendar_version if self.trading_calendar else None
            ),
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
    parser.add_argument(
        "--build-sealed-trade-index",
        nargs=4,
        metavar=("SOURCE_DB", "INDEX_DB", "PARTITION", "CANONICAL_SHA256"),
    )
    parser.add_argument(
        "--verify-sealed-partition",
        nargs=4,
        metavar=("SOURCE_DB", "INDEX_DB", "SEAL_JSON", "PARTITION"),
    )
    parser.add_argument(
        "--reconcile-late-trade-identities",
        nargs=2,
        metavar=("SOURCE_DB", "SEALED_DIR"),
    )
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
    run_started_us = now_us()
    collector_run_id = str(uuid.uuid4())
    code_commit = os.environ.get("ENGINE_B_PHASE0_CODE_COMMIT", "UNKNOWN_UNDEPLOYED")
    metrics = Metrics()
    sink = DatabaseSink(config, collector_run_id, code_commit, run_started_us)
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
    if args.build_sealed_trade_index is not None:
        source, index, partition, canonical_sha256 = args.build_sealed_trade_index
        build_sealed_trade_index(
            Path(source), Path(index), partition, canonical_sha256
        )
        return 0
    if args.verify_sealed_partition is not None:
        source, index, seal, partition = args.verify_sealed_partition
        verify_sealed_partition(
            Path(source), Path(index), Path(seal), partition
        )
        return 0
    if args.reconcile_late_trade_identities is not None:
        source, sealed_dir = args.reconcile_late_trade_identities
        reconcile_late_trade_identities(Path(source), Path(sealed_dir))
        return 0
    if args.smoke_seconds is not None and args.smoke_seconds <= 0:
        raise SystemExit("--smoke-seconds must be positive")
    return asyncio.run(async_main(args))


if __name__ == "__main__":
    raise SystemExit(main())
