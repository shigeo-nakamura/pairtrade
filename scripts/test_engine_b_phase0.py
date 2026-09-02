#!/usr/bin/env python3
"""Unit tests for the Engine B Phase 0A observer."""

from __future__ import annotations

import asyncio
import ast
import importlib.util
import json
import sqlite3
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock


SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
MODULE_PATH = SCRIPT_DIR / "engine_b_phase0.py"
CONFIG_PATH = REPO_ROOT / "configs" / "engine-b" / "phase0.json"
LOCK_PATH = SCRIPT_DIR / "engine_b_phase0_requirements.txt"
INSTALLER_PATH = SCRIPT_DIR / "install_engine_b_phase0.sh"
OBSERVER_UNIT_PATH = REPO_ROOT / "deploy" / "engine-b-phase0.service"
ARCHIVE_UNIT_PATH = REPO_ROOT / "deploy" / "engine-b-phase0-archive.service"
DEPLOY_WORKFLOW_PATH = REPO_ROOT / ".github" / "workflows" / "deploy-configs.yml"

SPEC = importlib.util.spec_from_file_location("engine_b_phase0", MODULE_PATH)
assert SPEC and SPEC.loader
engine_b = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = engine_b
SPEC.loader.exec_module(engine_b)


class FakeWebSocket:
    def __init__(self) -> None:
        self.messages: list[dict[str, object]] = []

    async def send(self, payload: str) -> None:
        self.messages.append(json.loads(payload))


class RecordingSink:
    def __init__(self) -> None:
        self.commands: list[tuple[str, dict[str, object]]] = []

    async def put(self, kind: str, payload: dict[str, object]) -> None:
        self.commands.append((kind, payload))


class ReadOnlyBoundaryTests(unittest.IsolatedAsyncioTestCase):
    async def test_allows_only_public_control_messages(self) -> None:
        ws = FakeWebSocket()
        await engine_b.send_public_control(
            ws, {"type": "subscribe", "channel": "order_book/37"}
        )
        await engine_b.send_public_control(ws, {"type": "pong"})
        self.assertEqual(
            ws.messages,
            [
                {"type": "subscribe", "channel": "order_book/37"},
                {"type": "pong"},
            ],
        )

    async def test_rejects_private_or_mutating_messages(self) -> None:
        ws = FakeWebSocket()
        with self.assertRaises(RuntimeError):
            await engine_b.send_public_control(ws, {"type": "transaction", "data": {}})
        with self.assertRaises(RuntimeError):
            await engine_b.send_public_control(
                ws, {"type": "subscribe", "channel": "account_all/1", "auth": "secret"}
            )
        self.assertEqual(ws.messages, [])

    def test_source_has_no_lighter_signer_dependency(self) -> None:
        source = MODULE_PATH.read_text()
        tree = ast.parse(source)
        imported_roots = {
            alias.name.split(".", 1)[0]
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
        }
        imported_roots.update(
            node.module.split(".", 1)[0]
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom) and node.module
        )
        self.assertNotIn("lighter", imported_roots)
        self.assertNotIn("LIGHTER_PRIVATE_API_KEY", source)
        self.assertNotIn("jsonapi/" + "sendtx", source)


class BookStateTests(unittest.TestCase):
    def test_snapshot_delta_and_nonce_continuity(self) -> None:
        state = engine_b.BookState()
        state.apply_snapshot(
            {
                "nonce": 10,
                "bids": [
                    {"price": "99.0", "size": "2"},
                    {"price": "98", "size": "3"},
                ],
                "asks": [
                    {"price": "101", "size": "4"},
                    {"price": "102", "size": "5"},
                ],
            }
        )
        applied, expected, observed = state.apply_delta(
            {
                "begin_nonce": 10,
                "nonce": 11,
                "bids": [
                    {"price": "99", "size": "0"},
                    {"price": "100", "size": "1.5"},
                ],
                "asks": [],
            }
        )
        self.assertTrue(applied)
        self.assertEqual((expected, observed), ("10", "10"))
        self.assertEqual(
            state.reconstructed_levels(2),
            [
                ("bid", 0, "100", "1.5"),
                ("bid", 1, "98", "3"),
                ("ask", 0, "101", "4"),
                ("ask", 1, "102", "5"),
            ],
        )

    def test_nonce_gap_invalidates_book(self) -> None:
        state = engine_b.BookState()
        state.apply_snapshot({"nonce": 10, "bids": [], "asks": []})
        applied, expected, observed = state.apply_delta(
            {"begin_nonce": 12, "nonce": 13, "bids": [], "asks": []}
        )
        self.assertFalse(applied)
        self.assertEqual((expected, observed), ("10", "12"))
        self.assertFalse(state.synced)

    def test_missing_sequence_invalidates_book(self) -> None:
        state = engine_b.BookState()
        self.assertFalse(state.apply_snapshot({"bids": [], "asks": []}))
        self.assertFalse(state.synced)

        self.assertTrue(
            state.apply_snapshot({"nonce": 10, "bids": [], "asks": []})
        )
        applied, _, observed = state.apply_delta(
            {"nonce": 11, "bids": [], "asks": []}
        )
        self.assertFalse(applied)
        self.assertIsNone(observed)
        self.assertFalse(state.synced)

        self.assertTrue(
            state.apply_snapshot({"nonce": 20, "bids": [], "asks": []})
        )
        applied, expected, observed = state.apply_delta(
            {"begin_nonce": 20, "bids": [], "asks": []}
        )
        self.assertFalse(applied)
        self.assertEqual((expected, observed), ("20", "20"))
        self.assertFalse(state.synced)


class BookHandlingTests(unittest.IsolatedAsyncioTestCase):
    async def test_missing_snapshot_nonce_never_emits_reconstruction(self) -> None:
        config = engine_b.load_config(CONFIG_PATH, LOCK_PATH)
        venue = next(item for item in config.venues if item.name == "robinhood")
        sink = RecordingSink()
        metrics = engine_b.Metrics()
        collector = engine_b.Collector(config, sink, metrics)
        ws = FakeWebSocket()
        connection = {
            "id": "missing-snapshot-nonce",
            "venue": venue.name,
            "started_us": 1_774_884_000_000_000,
            "api_schema_version": config.api_schema_version,
        }
        await collector.handle_book(
            ws,
            venue,
            connection,
            {
                "type": "subscribed/order_book",
                "channel": "order_book/37",
                "order_book": {
                    "bids": [{"price": "100", "size": "1"}],
                    "asks": [{"price": "101", "size": "1"}],
                },
            },
            1_774_884_082_400_000,
        )

        books = [payload for kind, payload in sink.commands if kind == "book"]
        gaps = [payload for kind, payload in sink.commands if kind == "gap"]
        self.assertEqual(len(books), 1)
        self.assertFalse(books[0]["complete"])
        self.assertEqual(gaps[0]["reason"], "snapshot_missing_nonce")
        self.assertFalse(metrics.book_synced[(venue.name, 37)])
        self.assertEqual(
            ws.messages,
            [
                {"type": "unsubscribe", "channel": "order_book/37"},
                {"type": "subscribe", "channel": "order_book/37"},
            ],
        )

        await collector.handle_book(
            ws,
            venue,
            connection,
            {
                "type": "subscribed/order_book",
                "channel": "order_book/37",
                "order_book": {"nonce": 10, "bids": [], "asks": []},
            },
            1_774_884_083_400_000,
        )
        closes = [payload for kind, payload in sink.commands if kind == "gap_close"]
        self.assertEqual(
            closes,
            [
                {
                    "recv_us": 1_774_884_083_400_000,
                    "venue": venue.name,
                    "market_id": 37,
                }
            ],
        )


class FeedLoopTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def websocket_modules(connect: object) -> dict[str, types.ModuleType]:
        websockets = types.ModuleType("websockets")
        websockets.__path__ = []
        asyncio_module = types.ModuleType("websockets.asyncio")
        asyncio_module.__path__ = []
        client = types.ModuleType("websockets.asyncio.client")
        client.connect = connect
        websockets.asyncio = asyncio_module
        asyncio_module.client = client
        return {
            "websockets": websockets,
            "websockets.asyncio": asyncio_module,
            "websockets.asyncio.client": client,
        }

    async def test_handshake_failure_does_not_create_websocket_session(self) -> None:
        config = engine_b.load_config(CONFIG_PATH, LOCK_PATH)
        venue = next(item for item in config.venues if item.name == "robinhood")
        sink = RecordingSink()
        collector = engine_b.Collector(config, sink, engine_b.Metrics())

        class FailedHandshake:
            async def __aenter__(self) -> object:
                collector.stop_event.set()
                raise OSError("simulated handshake failure")

            async def __aexit__(self, *args: object) -> None:
                return None

        with mock.patch.dict(
            sys.modules,
            self.websocket_modules(lambda *args, **kwargs: FailedHandshake()),
        ):
            await collector.feed_loop(venue)

        kinds = [kind for kind, _ in sink.commands]
        self.assertNotIn("connection_start", kinds)
        self.assertNotIn("connection_end", kinds)
        self.assertEqual(kinds.count("gap"), len(venue.markets))

    async def test_session_start_is_timestamped_after_handshake(self) -> None:
        config = engine_b.load_config(CONFIG_PATH, LOCK_PATH)
        venue = next(item for item in config.venues if item.name == "robinhood")
        sink = RecordingSink()
        collector = engine_b.Collector(config, sink, engine_b.Metrics())

        class EmptyWebSocket:
            def __aiter__(self) -> EmptyWebSocket:
                return self

            async def __anext__(self) -> str:
                raise StopAsyncIteration

        class ConnectedWebSocket:
            entered = False

            async def __aenter__(self) -> EmptyWebSocket:
                self.entered = True
                collector.stop_event.set()
                return EmptyWebSocket()

            async def __aexit__(self, *args: object) -> None:
                return None

        context = ConnectedWebSocket()
        timestamps = iter((1_774_884_082_400_000, 1_774_884_082_500_000))

        def connected_clock() -> int:
            self.assertTrue(context.entered)
            return next(timestamps)

        with (
            mock.patch.dict(
                sys.modules,
                self.websocket_modules(lambda *args, **kwargs: context),
            ),
            mock.patch.object(engine_b, "now_us", side_effect=connected_clock),
        ):
            await collector.feed_loop(venue)

        starts = [payload for kind, payload in sink.commands if kind == "connection_start"]
        self.assertEqual(len(starts), 1)
        self.assertEqual(starts[0]["recv_us"], 1_774_884_082_400_000)


class TradeIdentityTests(unittest.IsolatedAsyncioTestCase):
    async def test_synthetic_ids_preserve_multiset_across_overlapping_snapshots(self) -> None:
        config = engine_b.load_config(CONFIG_PATH, LOCK_PATH)
        venue = next(item for item in config.venues if item.name == "robinhood")
        sink = RecordingSink()
        collector = engine_b.Collector(config, sink, engine_b.Metrics())
        connection = {
            "id": "trade-identity-connection",
            "venue": venue.name,
            "started_us": 1_774_884_000_000_000,
            "api_schema_version": config.api_schema_version,
        }
        trade = {"price": "101", "size": "2", "is_maker_ask": True}
        other = {"price": "102", "size": "1", "is_maker_ask": False}
        first_message = {
            "type": "subscribed/trade",
            "channel": "trade/37",
            "nonce": 9001,
            "timestamp": 1_774_884_082_309,
            "trades": [trade, trade, other],
        }
        overlapping_message = {
            "type": "subscribed/trade",
            "channel": "trade/37",
            "nonce": 9002,
            "timestamp": 1_774_884_082_309,
            "trades": [other, trade, trade],
        }

        await collector.handle_trades(
            venue, connection, first_message, 1_774_884_082_400_000
        )
        await collector.handle_trades(
            venue, connection, overlapping_message, 1_774_884_083_400_000
        )

        ids = [payload["trade_id"] for kind, payload in sink.commands if kind == "trade"]
        self.assertEqual(len(ids), 6)
        self.assertNotEqual(ids[0], ids[1])
        self.assertEqual(ids[:2], ids[4:])
        self.assertEqual(ids[2], ids[3])
        self.assertTrue(all(str(value).startswith("synthetic:v3:") for value in ids))

    async def test_synthetic_ids_preserve_identical_update_multiplicity(self) -> None:
        config = engine_b.load_config(CONFIG_PATH, LOCK_PATH)
        venue = next(item for item in config.venues if item.name == "robinhood")
        sink = RecordingSink()
        collector = engine_b.Collector(config, sink, engine_b.Metrics())
        connection = {
            "id": "trade-update-connection",
            "venue": venue.name,
            "started_us": 1_774_884_000_000_000,
            "api_schema_version": config.api_schema_version,
        }
        trade = {
            "price": "101",
            "size": "2",
            "is_maker_ask": True,
            "timestamp": 1_774_884_082_309,
        }
        first = {
            "type": "update/trade",
            "channel": "trade/37",
            "nonce": 9001,
            "trades": [trade],
        }
        second = {**first, "nonce": 9002}
        await collector.handle_trades(venue, connection, first, 1_774_884_082_400_000)
        await collector.handle_trades(venue, connection, second, 1_774_884_082_500_000)
        await collector.handle_trades(venue, connection, first, 1_774_884_082_600_000)

        ids = [payload["trade_id"] for kind, payload in sink.commands if kind == "trade"]
        self.assertEqual(ids[0], ids[2])
        self.assertNotEqual(ids[0], ids[1])

    async def test_idless_update_without_nonce_fails_closed(self) -> None:
        config = engine_b.load_config(CONFIG_PATH, LOCK_PATH)
        venue = next(item for item in config.venues if item.name == "robinhood")
        sink = RecordingSink()
        collector = engine_b.Collector(config, sink, engine_b.Metrics())
        connection = {
            "id": "trade-missing-nonce",
            "venue": venue.name,
            "started_us": 1_774_884_000_000_000,
            "api_schema_version": config.api_schema_version,
        }
        with self.assertRaisesRegex(RuntimeError, "without exchange nonce"):
            await collector.handle_trades(
                venue,
                connection,
                {
                    "type": "update/trade",
                    "channel": "trade/37",
                    "trades": [{"price": "101", "size": "2"}],
                },
                1_774_884_082_400_000,
            )
        self.assertEqual(sink.commands, [])

    async def test_update_to_snapshot_replay_uses_alias_multiset(self) -> None:
        config = engine_b.load_config(CONFIG_PATH, LOCK_PATH)
        venue = next(item for item in config.venues if item.name == "robinhood")
        recording = RecordingSink()
        collector = engine_b.Collector(config, recording, engine_b.Metrics())
        recv_us = 1_774_884_082_400_000
        event_ms = 1_774_884_082_309
        connection_meta = {
            "id": "trade-update-snapshot-alias",
            "venue": venue.name,
            "started_us": recv_us,
            "api_schema_version": config.api_schema_version,
        }
        trade = {
            "price": "101",
            "size": "2",
            "is_maker_ask": True,
            "timestamp": event_ms,
        }
        for nonce in (9001, 9002):
            await collector.handle_trades(
                venue,
                connection_meta,
                {
                    "type": "update/trade",
                    "channel": "trade/37",
                    "nonce": nonce,
                    "trades": [trade],
                },
                recv_us + nonce,
            )
        await collector.handle_trades(
            venue,
            connection_meta,
            {
                "type": "subscribed/trade",
                "channel": "trade/37",
                "nonce": 9003,
                "trades": [trade, trade],
            },
            recv_us + 20_000,
        )

        with tempfile.TemporaryDirectory() as directory:
            database_dir = Path(directory) / "data"
            object.__setattr__(config, "database_dir", database_dir)
            sink = engine_b.DatabaseSink(config, "alias-run", "alias-commit")
            for path in (
                database_dir,
                sink.sealed_dir,
                sink.lock_dir,
                sink.gap_continuation_dir,
                sink.session_continuation_dir,
            ):
                path.mkdir(parents=True, exist_ok=True)
            with mock.patch.object(engine_b, "now_us", return_value=recv_us + 30_000):
                sink._write_batch(recording.commands)
            await sink.close()
            database = sqlite3.connect(
                database_dir
                / f"engine_b_phase0_{engine_b.partition_for_us(event_ms * 1_000)}.sqlite3"
            )
            try:
                self.assertEqual(
                    database.execute("SELECT COUNT(*) FROM trade").fetchone(),
                    (2,),
                )
                self.assertEqual(
                    database.execute(
                        "SELECT COUNT(*) FROM trade_replay_alias"
                    ).fetchone(),
                    (2,),
                )
                self.assertEqual(
                    database.execute(
                        "SELECT volume, trade_count FROM ohlcv_1m"
                    ).fetchone(),
                    ("4", 2),
                )
            finally:
                database.close()
            partition = engine_b.partition_for_us(event_ms * 1_000)
            database_path = (
                database_dir / f"engine_b_phase0_{partition}.sqlite3"
            )
            canonical_sha = "c" * 64
            index_path = sink.sealed_dir / f"{partition}.trade_ids.sqlite3"
            engine_b.build_sealed_trade_index(
                database_path, index_path, partition, canonical_sha
            )
            (sink.sealed_dir / f"{partition}.json").write_text(
                json.dumps(
                    {
                        "partition": partition,
                        "sha256": canonical_sha,
                        "trade_index": index_path.name,
                    }
                )
                + "\n"
            )
            snapshot_payloads = [
                payload
                for kind, payload in recording.commands[-2:]
                if kind == "trade"
            ]
            self.assertEqual(len(snapshot_payloads), 2)
            self.assertTrue(
                all(
                    sink._archived_trade_exists(partition, payload)
                    for payload in snapshot_payloads
                )
            )
            legacy_index = sqlite3.connect(index_path)
            legacy_index.execute("DROP TABLE archived_trade_replay_alias")
            legacy_index.commit()
            legacy_index.close()
            self.assertTrue(
                sink._archived_trade_exists(partition, snapshot_payloads[1])
            )

    async def test_snapshot_without_exchange_timestamp_fails_closed(self) -> None:
        config = engine_b.load_config(CONFIG_PATH, LOCK_PATH)
        venue = next(item for item in config.venues if item.name == "robinhood")
        sink = RecordingSink()
        collector = engine_b.Collector(config, sink, engine_b.Metrics())
        connection = {
            "id": "snapshot-missing-timestamp",
            "venue": venue.name,
            "started_us": 1_774_884_000_000_000,
            "api_schema_version": config.api_schema_version,
        }
        with self.assertRaisesRegex(RuntimeError, "without exchange timestamp"):
            await collector.handle_trades(
                venue,
                connection,
                {
                    "type": "subscribed/trade",
                    "channel": "trade/37",
                    "trades": [{"price": "101", "size": "2"}],
                },
                1_774_884_082_400_000,
            )
        self.assertEqual(sink.commands, [])

    async def test_incremental_trade_without_exchange_timestamp_fails_closed(
        self,
    ) -> None:
        config = engine_b.load_config(CONFIG_PATH, LOCK_PATH)
        venue = next(item for item in config.venues if item.name == "robinhood")
        sink = RecordingSink()
        collector = engine_b.Collector(config, sink, engine_b.Metrics())
        connection = {
            "id": "update-missing-timestamp",
            "venue": venue.name,
            "started_us": 1_774_884_000_000_000,
            "api_schema_version": config.api_schema_version,
        }
        with self.assertRaisesRegex(RuntimeError, "without exchange timestamp"):
            await collector.handle_trades(
                venue,
                connection,
                {
                    "type": "update/trade",
                    "channel": "trade/37",
                    "nonce": 9001,
                    "trades": [
                        {
                            "trade_id": "explicit-id-without-time",
                            "price": "101",
                            "size": "2",
                        }
                    ],
                },
                1_774_884_082_400_000,
            )
        self.assertEqual(sink.commands, [])

    async def test_out_of_range_exchange_timestamp_fails_closed(self) -> None:
        config = engine_b.load_config(CONFIG_PATH, LOCK_PATH)
        venue = next(item for item in config.venues if item.name == "robinhood")
        recv_us = 1_774_884_082_400_000
        for timestamp in (1_774_884_082, 10**30):
            with self.subTest(timestamp=timestamp):
                sink = RecordingSink()
                collector = engine_b.Collector(config, sink, engine_b.Metrics())
                connection = {
                    "id": f"out-of-range-{timestamp}",
                    "venue": venue.name,
                    "started_us": recv_us,
                    "api_schema_version": config.api_schema_version,
                }
                with self.assertRaisesRegex(
                    RuntimeError, "out-of-range exchange timestamp"
                ):
                    await collector.handle_trades(
                        venue,
                        connection,
                        {
                            "type": "subscribed/trade",
                            "channel": "trade/37",
                            "trades": [
                                {
                                    "trade_id": "invalid-time",
                                    "timestamp": timestamp,
                                    "price": "101",
                                    "size": "2",
                                }
                            ],
                        },
                        recv_us,
                    )
                self.assertEqual(sink.commands, [])

    async def test_sealed_same_batch_update_snapshot_uses_pending_alias(
        self,
    ) -> None:
        config = engine_b.load_config(CONFIG_PATH, LOCK_PATH)
        venue = next(item for item in config.venues if item.name == "robinhood")
        recording = RecordingSink()
        collector = engine_b.Collector(config, recording, engine_b.Metrics())
        event_us = 1_774_884_082_309_000
        recv_us = event_us + 7_200_000_000
        connection_meta = {
            "id": "sealed-same-batch-alias",
            "venue": venue.name,
            "started_us": recv_us,
            "api_schema_version": config.api_schema_version,
        }
        trade = {
            "price": "101",
            "size": "2",
            "timestamp": event_us // 1_000,
        }
        await collector.handle_trades(
            venue,
            connection_meta,
            {
                "type": "update/trade",
                "channel": "trade/37",
                "nonce": 9001,
                "trades": [trade],
            },
            recv_us,
        )
        await collector.handle_trades(
            venue,
            connection_meta,
            {
                "type": "subscribed/trade",
                "channel": "trade/37",
                "nonce": 9002,
                "trades": [trade],
            },
            recv_us + 1,
        )

        with tempfile.TemporaryDirectory() as directory:
            database_dir = Path(directory) / "data"
            object.__setattr__(config, "database_dir", database_dir)
            sink = engine_b.DatabaseSink(
                config, "sealed-alias-run", "sealed-alias-commit"
            )
            for path in (
                database_dir,
                sink.sealed_dir,
                sink.lock_dir,
                sink.gap_continuation_dir,
                sink.session_continuation_dir,
            ):
                path.mkdir(parents=True, exist_ok=True)
            event_partition = engine_b.partition_for_us(event_us)
            index_path = sink.sealed_dir / f"{event_partition}.trade_ids.sqlite3"
            index = sqlite3.connect(index_path)
            index.executescript(
                """CREATE TABLE sealed_metadata(
                     partition TEXT PRIMARY KEY,
                     canonical_db_sha256 TEXT NOT NULL
                   );
                   CREATE TABLE archived_trade_identity(
                     venue TEXT NOT NULL,
                     market_id INTEGER NOT NULL,
                     exchange_trade_id TEXT NOT NULL,
                     PRIMARY KEY(venue, market_id, exchange_trade_id)
                   ) WITHOUT ROWID;
                   CREATE TABLE late_trade_identity(
                     venue TEXT NOT NULL,
                     market_id INTEGER NOT NULL,
                     exchange_trade_id TEXT NOT NULL,
                     PRIMARY KEY(venue, market_id, exchange_trade_id)
                   ) WITHOUT ROWID;
                   CREATE TABLE archived_trade_replay_alias(
                     venue TEXT NOT NULL,
                     market_id INTEGER NOT NULL,
                     replay_alias TEXT NOT NULL,
                     exchange_trade_id TEXT NOT NULL,
                     PRIMARY KEY(venue, market_id, exchange_trade_id)
                   ) WITHOUT ROWID;"""
            )
            index.execute(
                "INSERT INTO sealed_metadata VALUES (?, ?)",
                (event_partition, "empty-sealed-alias"),
            )
            index.commit()
            index.close()
            (sink.sealed_dir / f"{event_partition}.json").write_text(
                json.dumps(
                    {
                        "partition": event_partition,
                        "sha256": "empty-sealed-alias",
                        "trade_index": index_path.name,
                    }
                )
                + "\n"
            )
            with mock.patch.object(engine_b, "now_us", return_value=recv_us + 2):
                sink._write_batch(recording.commands)
            await sink.close()
            active_partition = engine_b.partition_for_us(recv_us)
            active = sqlite3.connect(
                database_dir / f"engine_b_phase0_{active_partition}.sqlite3"
            )
            try:
                self.assertEqual(
                    active.execute(
                        """SELECT COUNT(*), COUNT(DISTINCT replay_alias)
                           FROM late_trade"""
                    ).fetchone(),
                    (1, 1),
                )
            finally:
                active.close()


class ConfigTests(unittest.TestCase):
    def test_config_records_robinhood_same_venue_blocker(self) -> None:
        config = engine_b.load_config(CONFIG_PATH, LOCK_PATH)
        robinhood = next(venue for venue in config.venues if venue.name == "robinhood")
        self.assertEqual(set(robinhood.known_missing_symbols), {"EWY", "USDKRW"})
        self.assertGreaterEqual(config.top_levels, 5)
        self.assertEqual(config.min_daily_volume_usd, engine_b.Decimal("100000"))

    def test_timestamp_normalization(self) -> None:
        self.assertEqual(engine_b.normalize_exchange_timestamp_us(1_773_854_156_654), 1_773_854_156_654_000)
        self.assertEqual(engine_b.normalize_exchange_timestamp_us(1_774_884_082_309_144), 1_774_884_082_309_144)


class RestPollingTests(unittest.IsolatedAsyncioTestCase):
    async def test_invalid_volume_is_recorded_as_poll_failure(self) -> None:
        config = engine_b.load_config(CONFIG_PATH, LOCK_PATH)
        venue = next(item for item in config.venues if item.name == "robinhood")
        sink = RecordingSink()
        metrics = engine_b.Metrics()
        collector = engine_b.Collector(config, sink, metrics)
        response = {
            "order_book_details": [
                {
                    "market_id": market.market_id,
                    "symbol": market.symbol,
                    "status": "active",
                    "daily_quote_token_volume": None,
                }
                for market in venue.markets
            ]
        }
        with mock.patch.object(engine_b, "fetch_json", return_value=response):
            await collector.poll_venue(venue)
        self.assertEqual(sink.commands, [])
        self.assertEqual(
            metrics.gauges[
                (
                    "engine_b_phase0_rest_poll_success",
                    (("venue", venue.name),),
                )
            ],
            0,
        )

    async def test_malformed_market_metadata_is_recorded_as_poll_failure(self) -> None:
        config = engine_b.load_config(CONFIG_PATH, LOCK_PATH)
        venue = next(item for item in config.venues if item.name == "robinhood")
        malformed_responses = (
            {"order_book_details": [{"market_id": None}]},
            {"order_book_details": None},
            [],
        )
        for response in malformed_responses:
            with self.subTest(response=response):
                sink = RecordingSink()
                metrics = engine_b.Metrics()
                collector = engine_b.Collector(config, sink, metrics)
                with mock.patch.object(engine_b, "fetch_json", return_value=response):
                    await collector.poll_venue(venue)
                self.assertEqual(sink.commands, [])
                self.assertEqual(
                    metrics.gauges[
                        (
                            "engine_b_phase0_rest_poll_success",
                            (("venue", venue.name),),
                        )
                    ],
                    0,
                )


class DeploymentTests(unittest.TestCase):
    def test_installer_records_commit_and_installs_all_units(self) -> None:
        installer = INSTALLER_PATH.read_text()
        workflow = DEPLOY_WORKFLOW_PATH.read_text()
        for unit in (
            "engine-b-phase0.service",
            "engine-b-phase0-archive.service",
            "engine-b-phase0-archive.timer",
        ):
            self.assertIn(unit, installer)
            self.assertIn(f"${{ENGINE_B_PREFIX}}/deploy/{unit}", workflow)
        self.assertIn("ENGINE_B_PHASE0_CODE_COMMIT", installer)
        self.assertIn('"$INSTALL_DIR/release.env"', installer)
        self.assertIn("ENGINE_B_PHASE0_CODE_COMMIT=${GITHUB_SHA}", workflow)
        self.assertIn("systemctl daemon-reload", installer)
        self.assertNotIn("systemctl restart", installer)
        self.assertNotIn("systemctl start", installer)

    def test_archive_deletion_is_disabled_in_unit(self) -> None:
        unit = ARCHIVE_UNIT_PATH.read_text()
        archive_script = (SCRIPT_DIR / "engine_b_phase0_archive.sh").read_text()
        self.assertIn("ENGINE_B_PHASE0_DELETE_VERIFIED_LOCAL=false", unit)
        self.assertIn("--reconcile-late-trade-identities", archive_script)

    def test_services_use_credential_isolated_identity(self) -> None:
        observer = OBSERVER_UNIT_PATH.read_text()
        archive = ARCHIVE_UNIT_PATH.read_text()
        installer = INSTALLER_PATH.read_text()
        for unit in (observer, archive):
            self.assertIn("User=engine-b-phase0", unit)
            self.assertIn("Group=engine-b-phase0", unit)
            self.assertIn("InaccessiblePaths=/opt/debot", unit)
            self.assertNotIn("User=ec2-user", unit)
        self.assertIn("ProtectProc=invisible", observer)
        self.assertIn(
            "ExecStartPre=+/usr/bin/chgrp -R engine-b-phase0 /var/lib/engine-b-phase0",
            observer,
        )
        self.assertIn(
            "ExecStartPre=+/usr/bin/chmod -R g+rwX,o-rwx /var/lib/engine-b-phase0",
            observer,
        )
        self.assertNotIn("ExecStart=/bin/bash /opt/debot/", observer)
        self.assertNotIn("ExecStart=/bin/bash /opt/debot/", archive)
        self.assertIn('useradd --system --gid "$SERVICE_GROUP"', installer)
        self.assertIn('"$INSTALL_DIR/engine_b_phase0.py"', installer)
        self.assertIn('"$INSTALL_DIR/engine_b_phase0_archive.sh"', installer)


class DatabaseTests(unittest.IsolatedAsyncioTestCase):
    async def test_normalized_public_data_is_persisted(self) -> None:
        config = engine_b.load_config(CONFIG_PATH, LOCK_PATH)
        with tempfile.TemporaryDirectory() as directory:
            database_dir = Path(directory) / "data"
            object.__setattr__(config, "database_dir", database_dir)
            sink = engine_b.DatabaseSink(config, "test-run", "test-commit")
            recv_us = engine_b.now_us()
            sink.start()
            event_us = recv_us - 5
            bucket_start_us = event_us - event_us % 60_000_000
            connection = {
                "id": "connection-1",
                "venue": "robinhood",
                "started_us": recv_us,
                "api_schema_version": config.api_schema_version,
            }
            await sink.put("connection_start", {"recv_us": recv_us, "connection": connection})
            await sink.put(
                "book",
                {
                    "recv_us": recv_us,
                    "srv_us": recv_us - 10,
                    "connection": connection,
                    "venue": "robinhood",
                    "market_id": 37,
                    "symbol": "SKHY",
                    "event_kind": "reconstructed",
                    "exchange_sequence": "10",
                    "begin_sequence": None,
                    "exchange_offset": "7",
                    "local_sequence": 1,
                    "complete": True,
                    "levels": [("bid", 0, "159.70", "2"), ("ask", 0, "159.72", "3")],
                },
            )
            await sink.put(
                "trade",
                {
                    "recv_us": recv_us,
                    "partition_us": event_us,
                    "event_ts_us": event_us,
                    "bucket_start_us": bucket_start_us,
                    "srv_us": event_us,
                    "connection": connection,
                    "venue": "robinhood",
                    "market_id": 37,
                    "symbol": "SKHY",
                    "trade_id": "123",
                    "exchange_sequence": "11",
                    "local_sequence": 1,
                    "price": "159.71",
                    "size": "1.25",
                    "aggressor_side": "buy",
                    "raw_public_json": "{}",
                },
            )
            await sink.queue.join()
            await sink.close()

            db_path = next(database_dir.glob("engine_b_phase0_*.sqlite3"))
            connection_db = sqlite3.connect(db_path)
            try:
                self.assertEqual(connection_db.execute("SELECT order_capability FROM collector_manifest").fetchone(), (0,))
                self.assertEqual(connection_db.execute("SELECT COUNT(*) FROM book_event").fetchone(), (1,))
                self.assertEqual(connection_db.execute("SELECT COUNT(*) FROM book_level").fetchone(), (2,))
                self.assertEqual(connection_db.execute("SELECT exchange_trade_id FROM trade").fetchone(), ("123",))
                self.assertEqual(
                    connection_db.execute(
                        "SELECT open, high, low, close, volume, trade_count, is_complete FROM ohlcv_1m"
                    ).fetchone(),
                    ("159.71", "159.71", "159.71", "159.71", "1.25", 1, 0),
                )
                self.assertEqual(connection_db.execute("PRAGMA integrity_check").fetchone(), ("ok",))
            finally:
                connection_db.close()

    async def test_spanning_connection_is_segmented_and_run_start_is_stable(self) -> None:
        config = engine_b.load_config(CONFIG_PATH, LOCK_PATH)
        with tempfile.TemporaryDirectory() as directory:
            database_dir = Path(directory) / "data"
            object.__setattr__(config, "database_dir", database_dir)
            hour_start_us = 1_700_000_000_000_000
            hour_start_us -= hour_start_us % 3_600_000_000
            next_hour_us = hour_start_us + 3_600_000_000
            run_started_us = hour_start_us + 1_000_000
            connection_started_us = hour_start_us + 1_800_000_000
            connection_ended_us = next_hour_us + 600_000_000
            connection = {
                "id": "connection-spanning-hours",
                "venue": "robinhood",
                "started_us": connection_started_us,
                "api_schema_version": config.api_schema_version,
            }

            def book(recv_us: int, local_sequence: int) -> dict[str, object]:
                return {
                    "recv_us": recv_us,
                    "connection": connection,
                    "venue": "robinhood",
                    "market_id": 37,
                    "symbol": "SKHY",
                    "event_kind": "raw",
                    "exchange_sequence": str(local_sequence),
                    "begin_sequence": None,
                    "exchange_offset": None,
                    "local_sequence": local_sequence,
                    "complete": False,
                    "levels": [],
                }

            sink = engine_b.DatabaseSink(
                config, "spanning-run", "spanning-commit", run_started_us
            )
            for path in (
                database_dir,
                sink.sealed_dir,
                sink.lock_dir,
                sink.gap_continuation_dir,
            ):
                path.mkdir(parents=True, exist_ok=True)
            with mock.patch.object(
                engine_b, "now_us", return_value=connection_ended_us
            ):
                sink._write_batch(
                    [
                        (
                            "connection_start",
                            {"recv_us": connection_started_us, "connection": connection},
                        ),
                        ("book", book(connection_started_us, 1)),
                        ("book", book(next_hour_us + 1_000_000, 2)),
                        (
                            "connection_end",
                            {
                                "recv_us": connection_ended_us,
                                "reason": "clean_close",
                                "connection": connection,
                            },
                        ),
                    ]
                )
            await sink.close()

            first_partition = engine_b.partition_for_us(hour_start_us)
            second_partition = engine_b.partition_for_us(next_hour_us)
            expected_sessions = {
                first_partition: (
                    connection_started_us,
                    next_hour_us,
                    "partition_rotation",
                ),
                second_partition: (
                    next_hour_us,
                    connection_ended_us,
                    "clean_close",
                ),
            }
            for partition, expected_session in expected_sessions.items():
                db = sqlite3.connect(
                    database_dir / f"engine_b_phase0_{partition}.sqlite3"
                )
                try:
                    self.assertEqual(
                        db.execute(
                            """SELECT started_ts_recv_us, ended_ts_recv_us, end_reason
                               FROM ws_connection"""
                        ).fetchone(),
                        expected_session,
                    )
                    self.assertEqual(
                        db.execute(
                            "SELECT started_ts_us FROM collector_manifest"
                        ).fetchone(),
                        (run_started_us,),
                    )
                finally:
                    db.close()

    async def test_idle_writer_rotates_partition_without_another_event(self) -> None:
        config = engine_b.load_config(CONFIG_PATH, LOCK_PATH)
        with tempfile.TemporaryDirectory() as directory:
            database_dir = Path(directory) / "data"
            object.__setattr__(config, "database_dir", database_dir)
            hour_start_us = 1_700_000_000_000_000
            hour_start_us -= hour_start_us % 3_600_000_000
            clock = [hour_start_us + 1_800_000_000]
            connection = {
                "id": "connection-idle-at-boundary",
                "venue": "robinhood",
                "started_us": clock[0],
                "api_schema_version": config.api_schema_version,
            }
            partition = engine_b.partition_for_us(clock[0])
            sink = engine_b.DatabaseSink(
                config, "idle-rotation-run", "idle-rotation-commit", clock[0]
            )
            with mock.patch.object(engine_b, "now_us", side_effect=lambda: clock[0]), \
                 mock.patch.object(
                     engine_b.DatabaseSink,
                     "_rotation_timeout_seconds",
                     return_value=0.01,
                 ):
                sink.start()
                await sink.put(
                    "connection_start", {"recv_us": clock[0], "connection": connection}
                )
                await sink.queue.join()
                self.assertIn(partition, sink._connections)
                clock[0] = hour_start_us + 3 * 3_600_000_000 + 1_000_000
                for _ in range(20):
                    if partition not in sink._connections:
                        break
                    await asyncio.sleep(0.01)
                self.assertNotIn(partition, sink._connections)
                await sink.close()

            expected_sessions = [
                (
                    hour_start_us + 1_800_000_000,
                    hour_start_us + 3_600_000_000,
                    "partition_rotation",
                ),
                (
                    hour_start_us + 3_600_000_000,
                    hour_start_us + 2 * 3_600_000_000,
                    "partition_rotation",
                ),
                (
                    hour_start_us + 2 * 3_600_000_000,
                    hour_start_us + 3 * 3_600_000_000,
                    "partition_rotation",
                ),
                (hour_start_us + 3 * 3_600_000_000, None, None),
            ]
            for offset, expected in enumerate(expected_sessions):
                partition = engine_b.partition_for_us(
                    hour_start_us + offset * 3_600_000_000
                )
                db = sqlite3.connect(
                    database_dir / f"engine_b_phase0_{partition}.sqlite3"
                )
                try:
                    self.assertEqual(
                        db.execute(
                            """SELECT started_ts_recv_us, ended_ts_recv_us, end_reason
                               FROM ws_connection"""
                        ).fetchone(),
                        expected,
                    )
                finally:
                    db.close()

    async def test_connection_retries_keep_one_open_gap(self) -> None:
        config = engine_b.load_config(CONFIG_PATH, LOCK_PATH)
        with tempfile.TemporaryDirectory() as directory:
            database_dir = Path(directory) / "data"
            object.__setattr__(config, "database_dir", database_dir)
            started_us = 1_700_000_000_000_000
            started_us -= started_us % 3_600_000_000
            started_us += 1_000_000
            retried_us = started_us + 60_000_000
            sink = engine_b.DatabaseSink(config, "retry-gap-run", "retry-gap-commit")
            for path in (
                database_dir,
                sink.sealed_dir,
                sink.lock_dir,
                sink.gap_continuation_dir,
                sink.session_continuation_dir,
            ):
                path.mkdir(parents=True, exist_ok=True)
            gaps = [
                (
                    "gap",
                    {
                        "recv_us": timestamp,
                        "connection_id": f"retry-{index}",
                        "venue": "robinhood",
                        "market_id": 37,
                        "symbol": "SKHY",
                        "channel": "connection",
                        "reason": "connection_error:TimeoutError",
                    },
                )
                for index, timestamp in enumerate((started_us, retried_us))
            ]
            with mock.patch.object(engine_b, "now_us", return_value=retried_us):
                sink._write_batch(gaps)
            await sink.close()
            partition = engine_b.partition_for_us(started_us)
            database = sqlite3.connect(
                database_dir / f"engine_b_phase0_{partition}.sqlite3"
            )
            try:
                self.assertEqual(
                    database.execute(
                        """SELECT COUNT(*), MIN(ts_start_us), MAX(ts_end_us)
                           FROM data_gap WHERE channel = 'connection'"""
                    ).fetchone(),
                    (1, started_us, None),
                )
            finally:
                database.close()

    async def test_unsynchronized_book_deltas_keep_one_open_gap(self) -> None:
        config = engine_b.load_config(CONFIG_PATH, LOCK_PATH)
        with tempfile.TemporaryDirectory() as directory:
            database_dir = Path(directory) / "data"
            object.__setattr__(config, "database_dir", database_dir)
            started_us = 1_700_000_000_000_000
            started_us -= started_us % 3_600_000_000
            started_us += 1_000_000
            retried_us = started_us + 1_000_000
            recovered_us = retried_us + 1_000_000
            failed_again_us = recovered_us + 1_000_000
            sink = engine_b.DatabaseSink(
                config, "book-gap-run", "book-gap-commit"
            )
            for path in (
                database_dir,
                sink.sealed_dir,
                sink.lock_dir,
                sink.gap_continuation_dir,
                sink.session_continuation_dir,
            ):
                path.mkdir(parents=True, exist_ok=True)

            def gap(timestamp: int) -> tuple[str, dict[str, object]]:
                return (
                    "gap",
                    {
                        "recv_us": timestamp,
                        "connection_id": "book-gap-connection",
                        "venue": "robinhood",
                        "market_id": 37,
                        "symbol": "SKHY",
                        "channel": "order_book",
                        "expected_sequence": "10",
                        "observed_sequence": "12",
                        "reason": "begin_nonce_mismatch_or_unsynced_delta",
                    },
                )

            with mock.patch.object(engine_b, "now_us", return_value=retried_us):
                sink._write_batch([gap(started_us), gap(retried_us)])
            with mock.patch.object(engine_b, "now_us", return_value=failed_again_us):
                sink._write_batch(
                    [
                        (
                            "gap_close",
                            {
                                "recv_us": recovered_us,
                                "venue": "robinhood",
                                "market_id": 37,
                            },
                        ),
                        gap(failed_again_us),
                    ]
                )
            await sink.close()
            partition = engine_b.partition_for_us(started_us)
            database = sqlite3.connect(
                database_dir / f"engine_b_phase0_{partition}.sqlite3"
            )
            try:
                self.assertEqual(
                    database.execute(
                        """SELECT ts_start_us, ts_end_us FROM data_gap
                           WHERE channel = 'order_book' ORDER BY gap_id"""
                    ).fetchall(),
                    [
                        (started_us, recovered_us),
                        (failed_again_us, None),
                    ],
                )
            finally:
                database.close()

    async def test_resynchronization_closes_retained_connection_gaps(self) -> None:
        config = engine_b.load_config(CONFIG_PATH, LOCK_PATH)
        with tempfile.TemporaryDirectory() as directory:
            database_dir = Path(directory) / "data"
            object.__setattr__(config, "database_dir", database_dir)
            hour_start_us = 1_700_000_000_000_000
            hour_start_us -= hour_start_us % 3_600_000_000
            gap_started_us = hour_start_us + 3_500_000_000
            recovered_us = hour_start_us + 3_700_000_000
            sink = engine_b.DatabaseSink(config, "gap-run", "gap-commit")
            for path in (
                database_dir,
                sink.sealed_dir,
                sink.lock_dir,
                sink.gap_continuation_dir,
            ):
                path.mkdir(parents=True, exist_ok=True)
            with mock.patch.object(engine_b, "now_us", return_value=gap_started_us):
                sink._write_batch(
                    [
                        (
                            "gap",
                            {
                                "recv_us": gap_started_us,
                                "connection_id": None,
                                "venue": "robinhood",
                                "market_id": 37,
                                "symbol": "SKHY",
                                "channel": "connection",
                                "reason": "connection_error:TimeoutError",
                            },
                        )
                    ]
                )
            with mock.patch.object(engine_b, "now_us", return_value=recovered_us):
                sink._write_batch(
                    [
                        (
                            "gap_close",
                            {
                                "recv_us": recovered_us,
                                "venue": "robinhood",
                                "market_id": 37,
                            },
                        )
                    ]
                )
            await sink.close()

            gap_partition = engine_b.partition_for_us(gap_started_us)
            recovered_partition = engine_b.partition_for_us(recovered_us)
            db = sqlite3.connect(
                database_dir / f"engine_b_phase0_{gap_partition}.sqlite3"
            )
            recovered_db = sqlite3.connect(
                database_dir / f"engine_b_phase0_{recovered_partition}.sqlite3"
            )
            try:
                self.assertEqual(
                    db.execute(
                        "SELECT ts_start_us, ts_end_us FROM data_gap"
                    ).fetchone(),
                    (gap_started_us, hour_start_us + 3_600_000_000),
                )
                self.assertEqual(
                    recovered_db.execute(
                        "SELECT ts_start_us, ts_end_us FROM data_gap"
                    ).fetchone(),
                    (hour_start_us + 3_600_000_000, recovered_us),
                )
            finally:
                db.close()
                recovered_db.close()

    async def test_gap_close_does_not_close_later_gap_in_same_batch(self) -> None:
        config = engine_b.load_config(CONFIG_PATH, LOCK_PATH)
        with tempfile.TemporaryDirectory() as directory:
            database_dir = Path(directory) / "data"
            object.__setattr__(config, "database_dir", database_dir)
            base_us = 1_700_000_000_000_000
            sink = engine_b.DatabaseSink(config, "ordered-gap-run", "ordered-gap-commit")
            for path in (
                database_dir,
                sink.sealed_dir,
                sink.lock_dir,
                sink.gap_continuation_dir,
            ):
                path.mkdir(parents=True, exist_ok=True)

            def gap(recv_us: int, reason: str) -> tuple[str, dict[str, object]]:
                return (
                    "gap",
                    {
                        "recv_us": recv_us,
                        "connection_id": None,
                        "venue": "robinhood",
                        "market_id": 37,
                        "symbol": "SKHY",
                        "channel": "connection",
                        "reason": reason,
                    },
                )

            with mock.patch.object(engine_b, "now_us", return_value=base_us + 10):
                sink._write_batch([gap(base_us + 10, "old-gap")])
            with mock.patch.object(engine_b, "now_us", return_value=base_us + 30):
                sink._write_batch(
                    [
                        (
                            "gap_close",
                            {
                                "recv_us": base_us + 20,
                                "venue": "robinhood",
                                "market_id": 37,
                            },
                        ),
                        gap(base_us + 30, "later-gap"),
                    ]
                )
            await sink.close()

            partition = engine_b.partition_for_us(base_us)
            db = sqlite3.connect(
                database_dir / f"engine_b_phase0_{partition}.sqlite3"
            )
            try:
                self.assertEqual(
                    db.execute(
                        "SELECT ts_start_us, ts_end_us, reason FROM data_gap ORDER BY gap_id"
                    ).fetchall(),
                    [
                        (base_us + 10, base_us + 20, "old-gap"),
                        (base_us + 30, None, "later-gap"),
                    ],
                )
            finally:
                db.close()

    async def test_open_gap_is_carried_into_next_partition(self) -> None:
        config = engine_b.load_config(CONFIG_PATH, LOCK_PATH)
        with tempfile.TemporaryDirectory() as directory:
            database_dir = Path(directory) / "data"
            object.__setattr__(config, "database_dir", database_dir)
            hour_start_us = 1_700_000_000_000_000
            hour_start_us -= hour_start_us % 3_600_000_000
            gap_started_us = hour_start_us + 3_500_000_000
            next_hour_us = hour_start_us + 3_600_000_000
            recovered_us = hour_start_us + 3 * 3_600_000_000 + 200_000_000
            sink = engine_b.DatabaseSink(config, "carry-run", "carry-commit")
            for path in (
                database_dir,
                sink.sealed_dir,
                sink.lock_dir,
                sink.gap_continuation_dir,
            ):
                path.mkdir(parents=True, exist_ok=True)
            gap = {
                "recv_us": gap_started_us,
                "connection_id": None,
                "venue": "robinhood",
                "market_id": 37,
                "symbol": "SKHY",
                "channel": "connection",
                "reason": "connection_error:TimeoutError",
            }
            with mock.patch.object(engine_b, "now_us", return_value=gap_started_us):
                sink._write_batch([("gap", gap)])
            with mock.patch.object(engine_b, "now_us", return_value=next_hour_us + 1):
                sink._write_batch([])
            with mock.patch.object(engine_b, "now_us", return_value=recovered_us):
                sink._write_batch(
                    [
                        (
                            "gap_close",
                            {
                                "recv_us": recovered_us,
                                "venue": "robinhood",
                                "market_id": 37,
                            },
                        )
                    ]
                )
            await sink.close()

            first_db = sqlite3.connect(
                database_dir
                / f"engine_b_phase0_{engine_b.partition_for_us(hour_start_us)}.sqlite3"
            )
            gap_rows = []
            for offset in range(4):
                partition_us = hour_start_us + offset * 3_600_000_000
                db = sqlite3.connect(
                    database_dir
                    / f"engine_b_phase0_{engine_b.partition_for_us(partition_us)}.sqlite3"
                )
                try:
                    gap_rows.append(
                        db.execute(
                            """SELECT ts_start_us, ts_end_us, continuation_id
                               FROM data_gap"""
                        ).fetchone()
                    )
                finally:
                    db.close()
            try:
                self.assertEqual(
                    first_db.execute(
                        "SELECT ts_start_us, ts_end_us FROM data_gap"
                    ).fetchone(),
                    (gap_started_us, next_hour_us),
                )
                self.assertEqual(
                    gap_rows,
                    [
                        (gap_started_us, next_hour_us, None),
                        (
                            hour_start_us + 3_600_000_000,
                            hour_start_us + 2 * 3_600_000_000,
                            f"partition:{engine_b.partition_for_us(hour_start_us)}:1",
                        ),
                        (
                            hour_start_us + 2 * 3_600_000_000,
                            hour_start_us + 3 * 3_600_000_000,
                            f"partition:{engine_b.partition_for_us(hour_start_us + 3_600_000_000)}:1",
                        ),
                        (
                            hour_start_us + 3 * 3_600_000_000,
                            recovered_us,
                            f"partition:{engine_b.partition_for_us(hour_start_us + 2 * 3_600_000_000)}:1",
                        ),
                    ],
                )
            finally:
                first_db.close()

    async def test_archive_gap_marker_is_imported_idempotently(self) -> None:
        config = engine_b.load_config(CONFIG_PATH, LOCK_PATH)
        with tempfile.TemporaryDirectory() as directory:
            database_dir = Path(directory) / "data"
            object.__setattr__(config, "database_dir", database_dir)
            recovered_us = 1_700_003_800_000_000
            start_us = recovered_us - 300_000_000
            sink = engine_b.DatabaseSink(config, "marker-run", "marker-commit")
            for path in (
                database_dir,
                sink.sealed_dir,
                sink.lock_dir,
                sink.gap_continuation_dir,
            ):
                path.mkdir(parents=True, exist_ok=True)
            marker_path = sink.gap_continuation_dir / "old-gap.json"
            marker_path.write_text(
                json.dumps(
                    {
                        "continuation_id": "archive:old:7",
                        "start_us": start_us,
                        "venue": "robinhood",
                        "market_id": 37,
                        "symbol": "SKHY",
                        "channel": "connection",
                        "expected_sequence": None,
                        "observed_sequence": None,
                        "reason": "connection_error:TimeoutError",
                    }
                )
                + "\n"
            )
            with mock.patch.object(engine_b, "now_us", return_value=recovered_us):
                sink._write_batch(
                    [
                        (
                            "gap_close",
                            {
                                "recv_us": recovered_us,
                                "venue": "robinhood",
                                "market_id": 37,
                            },
                        )
                    ]
                )
            self.assertFalse(marker_path.exists())
            await sink.close()

            db_path = database_dir / (
                f"engine_b_phase0_{engine_b.partition_for_us(recovered_us)}.sqlite3"
            )
            db = sqlite3.connect(db_path)
            try:
                self.assertEqual(
                    db.execute(
                        """SELECT ts_start_us, ts_end_us, continuation_id
                           FROM data_gap"""
                    ).fetchall(),
                    [(start_us, recovered_us, "archive:old:7")],
                )
            finally:
                db.close()

    async def test_gap_marker_advances_past_sealed_destination(self) -> None:
        config = engine_b.load_config(CONFIG_PATH, LOCK_PATH)
        with tempfile.TemporaryDirectory() as directory:
            database_dir = Path(directory) / "data"
            object.__setattr__(config, "database_dir", database_dir)
            hour_start_us = 1_700_000_000_000_000
            hour_start_us -= hour_start_us % 3_600_000_000
            sealed_start_us = hour_start_us + 3_600_000_000
            recovered_us = hour_start_us + 3 * 3_600_000_000 + 200_000_000
            sink = engine_b.DatabaseSink(config, "sealed-gap-run", "sealed-gap-commit")
            for path in (
                database_dir,
                sink.sealed_dir,
                sink.lock_dir,
                sink.gap_continuation_dir,
            ):
                path.mkdir(parents=True, exist_ok=True)
            sealed_partition = engine_b.partition_for_us(sealed_start_us)
            sealed_sha = "a" * 64
            (sink.sealed_dir / f"{sealed_partition}.json").write_text(
                json.dumps(
                    {
                        "partition": sealed_partition,
                        "sha256": sealed_sha,
                        "trade_index": f"{sealed_partition}.trade_ids.sqlite3",
                    }
                )
                + "\n"
            )
            sealed_index = sqlite3.connect(
                sink.sealed_dir / f"{sealed_partition}.trade_ids.sqlite3"
            )
            sealed_index.execute(
                """CREATE TABLE sealed_metadata(
                     partition TEXT PRIMARY KEY,
                     canonical_db_sha256 TEXT NOT NULL
                   )"""
            )
            sealed_index.execute(
                """CREATE TABLE archived_gap_continuation(
                     continuation_id TEXT PRIMARY KEY,
                     gap_id INTEGER NOT NULL
                   ) WITHOUT ROWID"""
            )
            sealed_index.execute(
                "INSERT INTO sealed_metadata VALUES (?, ?)",
                (sealed_partition, sealed_sha),
            )
            sealed_index.commit()
            sealed_index.close()
            marker_path = sink.gap_continuation_dir / "sealed-target.json"
            marker_path.write_text(
                json.dumps(
                    {
                        "continuation_id": "partition:source:1",
                        "start_us": sealed_start_us,
                        "venue": "robinhood",
                        "market_id": 37,
                        "symbol": "SKHY",
                        "channel": "connection",
                        "expected_sequence": None,
                        "observed_sequence": None,
                        "reason": "connection_error:TimeoutError",
                    }
                )
                + "\n"
            )
            with mock.patch.object(engine_b, "now_us", return_value=recovered_us):
                sink._write_batch(
                    [
                        (
                            "gap_close",
                            {
                                "recv_us": recovered_us,
                                "venue": "robinhood",
                                "market_id": 37,
                            },
                        )
                    ]
                )
            self.assertFalse(marker_path.exists())
            await sink.close()

            self.assertFalse(
                (database_dir / f"engine_b_phase0_{sealed_partition}.sqlite3").exists()
            )
            expected = []
            sealed_intervals = []
            for offset in (2, 3):
                start_us = hour_start_us + offset * 3_600_000_000
                partition = engine_b.partition_for_us(start_us)
                db = sqlite3.connect(
                    database_dir / f"engine_b_phase0_{partition}.sqlite3"
                )
                try:
                    expected.append(
                        db.execute(
                            "SELECT ts_start_us, ts_end_us FROM data_gap"
                        ).fetchone()
                    )
                    sealed_intervals.extend(
                        db.execute(
                            """SELECT sealed_partition, ts_start_us, ts_end_us
                               FROM sealed_gap_interval"""
                        ).fetchall()
                    )
                finally:
                    db.close()
            self.assertEqual(
                expected,
                [
                    (
                        hour_start_us + 2 * 3_600_000_000,
                        hour_start_us + 3 * 3_600_000_000,
                    ),
                    (hour_start_us + 3 * 3_600_000_000, recovered_us),
                ],
            )
            self.assertEqual(
                sealed_intervals,
                [
                    (
                        sealed_partition,
                        sealed_start_us,
                        hour_start_us + 2 * 3_600_000_000,
                    )
                ],
            )

    async def test_partition_gap_marker_survives_failed_continuation_write(self) -> None:
        config = engine_b.load_config(CONFIG_PATH, LOCK_PATH)
        with tempfile.TemporaryDirectory() as directory:
            database_dir = Path(directory) / "data"
            object.__setattr__(config, "database_dir", database_dir)
            hour_start_us = 1_700_000_000_000_000
            hour_start_us -= hour_start_us % 3_600_000_000
            gap_started_us = hour_start_us + 3_500_000_000
            next_hour_us = hour_start_us + 3_600_000_000
            recovered_us = next_hour_us + 200_000_000
            first_partition = engine_b.partition_for_us(hour_start_us)
            second_partition = engine_b.partition_for_us(next_hour_us)
            sink = engine_b.DatabaseSink(config, "crash-run", "crash-commit")
            for path in (
                database_dir,
                sink.sealed_dir,
                sink.lock_dir,
                sink.gap_continuation_dir,
            ):
                path.mkdir(parents=True, exist_ok=True)
            gap = {
                "recv_us": gap_started_us,
                "connection_id": None,
                "venue": "robinhood",
                "market_id": 37,
                "symbol": "SKHY",
                "channel": "connection",
                "reason": "connection_error:TimeoutError",
            }
            with mock.patch.object(engine_b, "now_us", return_value=gap_started_us):
                sink._write_batch([("gap", gap)])

            original_write_partition = sink._write_partition

            def fail_continuation_write(
                partition: str, commands: list[tuple[str, dict[str, object]]]
            ) -> None:
                if partition == second_partition:
                    self.assertEqual(
                        len(list(sink.gap_continuation_dir.glob("*.json"))), 1
                    )
                    raise RuntimeError("simulated continuation write failure")
                original_write_partition(partition, commands)

            with (
                mock.patch.object(engine_b, "now_us", return_value=next_hour_us + 1),
                mock.patch.object(sink, "_write_partition", fail_continuation_write),
                self.assertRaisesRegex(RuntimeError, "simulated continuation"),
            ):
                sink._write_batch([])
            self.assertEqual(len(list(sink.gap_continuation_dir.glob("*.json"))), 1)
            await sink.close()

            recovered_sink = engine_b.DatabaseSink(
                config, "recovered-run", "recovered-commit"
            )
            for path in (
                database_dir,
                recovered_sink.sealed_dir,
                recovered_sink.lock_dir,
                recovered_sink.gap_continuation_dir,
            ):
                path.mkdir(parents=True, exist_ok=True)
            with mock.patch.object(engine_b, "now_us", return_value=recovered_us):
                recovered_sink._write_batch(
                    [
                        (
                            "gap_close",
                            {
                                "recv_us": recovered_us,
                                "venue": "robinhood",
                                "market_id": 37,
                            },
                        )
                    ]
                )
            self.assertEqual(list(recovered_sink.gap_continuation_dir.glob("*.json")), [])
            await recovered_sink.close()

            first_db = sqlite3.connect(
                database_dir / f"engine_b_phase0_{first_partition}.sqlite3"
            )
            second_db = sqlite3.connect(
                database_dir / f"engine_b_phase0_{second_partition}.sqlite3"
            )
            try:
                self.assertEqual(
                    first_db.execute(
                        "SELECT ts_start_us, ts_end_us FROM data_gap"
                    ).fetchall(),
                    [(gap_started_us, next_hour_us)],
                )
                self.assertEqual(
                    second_db.execute(
                        """SELECT ts_start_us, ts_end_us, continuation_id
                           FROM data_gap"""
                    ).fetchall(),
                    [
                        (
                            next_hour_us,
                            recovered_us,
                            f"partition:{first_partition}:1",
                        )
                    ],
                )
            finally:
                first_db.close()
                second_db.close()

    async def test_session_marker_survives_failed_continuation_write(self) -> None:
        config = engine_b.load_config(CONFIG_PATH, LOCK_PATH)
        venue = next(item for item in config.venues if item.name == "robinhood")
        with tempfile.TemporaryDirectory() as directory:
            database_dir = Path(directory) / "data"
            object.__setattr__(config, "database_dir", database_dir)
            hour_start_us = 1_700_000_000_000_000
            hour_start_us -= hour_start_us % 3_600_000_000
            connection_started_us = hour_start_us + 3_500_000_000
            next_hour_us = hour_start_us + 3_600_000_000
            recovered_us = next_hour_us + 200_000_000
            first_partition = engine_b.partition_for_us(hour_start_us)
            second_partition = engine_b.partition_for_us(next_hour_us)
            sink = engine_b.DatabaseSink(config, "session-crash-run", "session-crash-commit")
            for path in (
                database_dir,
                sink.sealed_dir,
                sink.lock_dir,
                sink.gap_continuation_dir,
                sink.session_continuation_dir,
            ):
                path.mkdir(parents=True, exist_ok=True)
            connection_meta = {
                "id": "session-crash-connection",
                "venue": "robinhood",
                "started_us": connection_started_us,
                "api_schema_version": config.api_schema_version,
            }
            with mock.patch.object(engine_b, "now_us", return_value=connection_started_us):
                sink._write_batch(
                    [
                        (
                            "connection_start",
                            {"recv_us": connection_started_us, "connection": connection_meta},
                        )
                    ]
                )

            original_write_partition = sink._write_partition

            def fail_continuation_write(
                partition: str, commands: list[tuple[str, dict[str, object]]]
            ) -> None:
                if partition == second_partition:
                    self.assertEqual(
                        len(list(sink.session_continuation_dir.glob("*.json"))), 1
                    )
                    raise RuntimeError("simulated session continuation failure")
                original_write_partition(partition, commands)

            with (
                mock.patch.object(engine_b, "now_us", return_value=next_hour_us + 1),
                mock.patch.object(sink, "_write_partition", fail_continuation_write),
                self.assertRaisesRegex(RuntimeError, "simulated session continuation"),
            ):
                sink._write_batch([])
            self.assertEqual(
                len(list(sink.session_continuation_dir.glob("*.json"))), 1
            )
            marker = json.loads(
                next(sink.session_continuation_dir.glob("*.json")).read_text()
            )
            self.assertEqual(
                marker["source_collector_run_id"], sink.collector_run_id
            )
            await sink.close()

            recovered_sink = engine_b.DatabaseSink(
                config, "session-recovery-run", "session-recovery-commit"
            )
            for path in (
                database_dir,
                recovered_sink.sealed_dir,
                recovered_sink.lock_dir,
                recovered_sink.gap_continuation_dir,
                recovered_sink.session_continuation_dir,
            ):
                path.mkdir(parents=True, exist_ok=True)
            with mock.patch.object(engine_b, "now_us", return_value=recovered_us):
                recovered_sink._write_batch(
                    [
                        (
                            "gap_close",
                            {
                                "recv_us": recovered_us,
                                "venue": venue.name,
                                "market_id": market.market_id,
                            },
                        )
                        for market in venue.markets
                    ]
                )
            self.assertEqual(
                list(recovered_sink.session_continuation_dir.glob("*.json")), []
            )
            self.assertEqual(
                list(recovered_sink.gap_continuation_dir.glob("*.json")), []
            )
            await recovered_sink.close()

            first_db = sqlite3.connect(
                database_dir / f"engine_b_phase0_{first_partition}.sqlite3"
            )
            second_db = sqlite3.connect(
                database_dir / f"engine_b_phase0_{second_partition}.sqlite3"
            )
            try:
                self.assertEqual(
                    first_db.execute(
                        """SELECT started_ts_recv_us, ended_ts_recv_us, end_reason
                           FROM ws_connection"""
                    ).fetchone(),
                    (
                        connection_started_us,
                        next_hour_us,
                        "partition_rotation",
                    ),
                )
                self.assertEqual(
                    second_db.execute(
                        """SELECT started_ts_recv_us, ended_ts_recv_us, end_reason
                           FROM ws_connection"""
                    ).fetchone(),
                    (
                        next_hour_us,
                        next_hour_us,
                        "collector_restart_recovery",
                    ),
                )
                self.assertEqual(
                    second_db.execute(
                        """SELECT COUNT(*), MIN(ts_start_us), MAX(ts_start_us),
                                  MIN(ts_end_us), MAX(ts_end_us), COUNT(DISTINCT reason)
                           FROM data_gap WHERE venue = 'robinhood'
                             AND channel = 'connection'"""
                    ).fetchone(),
                    (
                        len(venue.markets),
                        next_hour_us,
                        next_hour_us,
                        recovered_us,
                        recovered_us,
                        1,
                    ),
                )
            finally:
                first_db.close()
                second_db.close()

    async def test_same_collector_session_handoff_stays_open(self) -> None:
        config = engine_b.load_config(CONFIG_PATH, LOCK_PATH)
        with tempfile.TemporaryDirectory() as directory:
            database_dir = Path(directory) / "data"
            object.__setattr__(config, "database_dir", database_dir)
            boundary_us = 1_700_000_000_000_000
            boundary_us -= boundary_us % 3_600_000_000
            source_partition = engine_b.partition_for_us(
                boundary_us - 3_600_000_000
            )
            sink = engine_b.DatabaseSink(
                config, "live-handoff-run", "live-handoff-commit"
            )
            for path in (
                database_dir,
                sink.sealed_dir,
                sink.lock_dir,
                sink.gap_continuation_dir,
                sink.session_continuation_dir,
            ):
                path.mkdir(parents=True, exist_ok=True)
            sink._persist_session_continuation_marker(
                {
                    "continuation_id": "live-archive-handoff",
                    "start_us": boundary_us,
                    "source_partition": source_partition,
                    "source_collector_run_id": sink.collector_run_id,
                    "connection": {
                        "id": "live-session",
                        "venue": "robinhood",
                        "started_us": boundary_us,
                        "api_schema_version": config.api_schema_version,
                    },
                }
            )

            with mock.patch.object(
                engine_b, "now_us", return_value=boundary_us + 200_000_000
            ):
                sink._write_batch([])
            await sink.close()

            destination = sqlite3.connect(
                database_dir
                / f"engine_b_phase0_{engine_b.partition_for_us(boundary_us)}.sqlite3"
            )
            try:
                self.assertEqual(
                    destination.execute(
                        """SELECT started_ts_recv_us, ended_ts_recv_us, end_reason
                           FROM ws_connection"""
                    ).fetchone(),
                    (boundary_us, None, None),
                )
            finally:
                destination.close()

    async def test_existing_session_marker_precedes_orphan_discovery(self) -> None:
        config = engine_b.load_config(CONFIG_PATH, LOCK_PATH)
        with tempfile.TemporaryDirectory() as directory:
            database_dir = Path(directory) / "data"
            object.__setattr__(config, "database_dir", database_dir)
            source_start_us = 1_700_000_000_000_000
            source_start_us -= source_start_us % 3_600_000_000
            boundary_us = source_start_us + 3_600_000_000
            recovered_us = boundary_us + 200_000_000
            source_partition = engine_b.partition_for_us(source_start_us)
            destination_partition = engine_b.partition_for_us(boundary_us)
            source_sink = engine_b.DatabaseSink(
                config, "marker-source-run", "marker-source-commit"
            )
            for path in (
                database_dir,
                source_sink.sealed_dir,
                source_sink.lock_dir,
                source_sink.gap_continuation_dir,
                source_sink.session_continuation_dir,
            ):
                path.mkdir(parents=True, exist_ok=True)
            connection = {
                "id": "marker-before-source-close",
                "venue": "robinhood",
                "started_us": source_start_us + 3_500_000_000,
                "api_schema_version": config.api_schema_version,
            }
            with mock.patch.object(
                engine_b, "now_us", return_value=connection["started_us"]
            ):
                source_sink._write_batch(
                    [
                        (
                            "connection_start",
                            {
                                "recv_us": connection["started_us"],
                                "connection": connection,
                            },
                        )
                    ]
                )
            source_sink._persist_session_continuation_marker(
                {
                    "continuation_id": (
                        f"partition:{source_partition}:{connection['id']}"
                    ),
                    "start_us": boundary_us,
                    "source_partition": source_partition,
                    "source_collector_run_id": source_sink.collector_run_id,
                    "connection": {
                        **connection,
                        "started_us": boundary_us,
                    },
                }
            )
            for database in source_sink._connections.values():
                database.close()
            source_sink._connections.clear()

            recovered_sink = engine_b.DatabaseSink(
                config, "marker-recovery-run", "marker-recovery-commit"
            )
            with mock.patch.object(engine_b, "now_us", return_value=recovered_us):
                recovered_sink._write_batch([])
            await recovered_sink.close()

            source = sqlite3.connect(
                database_dir / f"engine_b_phase0_{source_partition}.sqlite3"
            )
            destination = sqlite3.connect(
                database_dir
                / f"engine_b_phase0_{destination_partition}.sqlite3"
            )
            try:
                self.assertEqual(
                    source.execute(
                        """SELECT ended_ts_recv_us, end_reason
                           FROM ws_connection"""
                    ).fetchone(),
                    (boundary_us, "partition_rotation"),
                )
                self.assertEqual(
                    destination.execute(
                        """SELECT started_ts_recv_us, ended_ts_recv_us, end_reason
                           FROM ws_connection"""
                    ).fetchone(),
                    (
                        boundary_us,
                        boundary_us,
                        "collector_restart_recovery",
                    ),
                )
                robinhood = next(
                    item for item in config.venues if item.name == "robinhood"
                )
                self.assertEqual(
                    destination.execute(
                        """SELECT COUNT(*), MIN(ts_start_us), MAX(ts_end_us)
                           FROM data_gap WHERE venue = 'robinhood'
                             AND channel = 'connection'"""
                    ).fetchone(),
                    (len(robinhood.markets), boundary_us, None),
                )
            finally:
                source.close()
                destination.close()

    async def test_replayed_session_end_preserves_actual_close(self) -> None:
        config = engine_b.load_config(CONFIG_PATH, LOCK_PATH)
        with tempfile.TemporaryDirectory() as directory:
            database_dir = Path(directory) / "data"
            object.__setattr__(config, "database_dir", database_dir)
            started_us = 1_700_000_000_000_000
            actual_end_us = started_us + 10_000_000
            replay_end_us = actual_end_us + 300_000_000
            connection = {
                "id": "completed-before-marker-unlink",
                "venue": "robinhood",
                "started_us": started_us,
                "api_schema_version": config.api_schema_version,
            }
            sink = engine_b.DatabaseSink(
                config, "completed-handoff-run", "completed-handoff-commit"
            )
            for path in (
                database_dir,
                sink.sealed_dir,
                sink.lock_dir,
                sink.gap_continuation_dir,
                sink.session_continuation_dir,
            ):
                path.mkdir(parents=True, exist_ok=True)
            with mock.patch.object(
                engine_b, "now_us", return_value=actual_end_us
            ):
                sink._write_batch(
                    [
                        (
                            "connection_start",
                            {"recv_us": started_us, "connection": connection},
                        ),
                        (
                            "connection_end",
                            {
                                "recv_us": actual_end_us,
                                "reason": "normal_stop",
                                "connection": connection,
                            },
                        ),
                    ]
                )
            with mock.patch.object(
                engine_b, "now_us", return_value=replay_end_us
            ):
                sink._write_batch(
                    [
                        (
                            "connection_end",
                            {
                                "recv_us": replay_end_us,
                                "reason": "collector_restart_recovery",
                                "connection": connection,
                            },
                        )
                    ]
                )
            await sink.close()

            database = sqlite3.connect(
                database_dir
                / f"engine_b_phase0_{engine_b.partition_for_us(started_us)}.sqlite3"
            )
            try:
                self.assertEqual(
                    database.execute(
                        """SELECT ended_ts_recv_us, end_reason
                           FROM ws_connection"""
                    ).fetchone(),
                    (actual_end_us, "normal_stop"),
                )
            finally:
                database.close()

    async def test_startup_discovers_orphaned_session_without_archive_timer(
        self,
    ) -> None:
        config = engine_b.load_config(CONFIG_PATH, LOCK_PATH)
        venue = next(item for item in config.venues if item.name == "robinhood")
        with tempfile.TemporaryDirectory() as directory:
            database_dir = Path(directory) / "data"
            object.__setattr__(config, "database_dir", database_dir)
            hour_start_us = 1_700_000_000_000_000
            hour_start_us -= hour_start_us % 3_600_000_000
            next_hour_us = hour_start_us + 3_600_000_000
            second_hour_us = next_hour_us + 3_600_000_000
            recovered_us = second_hour_us + 300_000_000
            connection_started_us = hour_start_us + 3_000_000_000
            first_partition = engine_b.partition_for_us(hour_start_us)
            connection_meta = {
                "id": "orphaned-before-rotation",
                "venue": "robinhood",
                "started_us": connection_started_us,
                "api_schema_version": config.api_schema_version,
            }

            crashed_sink = engine_b.DatabaseSink(
                config, "orphan-source-run", "orphan-source-commit"
            )
            for path in (
                database_dir,
                crashed_sink.sealed_dir,
                crashed_sink.lock_dir,
                crashed_sink.gap_continuation_dir,
                crashed_sink.session_continuation_dir,
            ):
                path.mkdir(parents=True, exist_ok=True)
            with mock.patch.object(
                engine_b, "now_us", return_value=connection_started_us
            ):
                crashed_sink._write_batch(
                    [
                        (
                            "connection_start",
                            {
                                "recv_us": connection_started_us,
                                "connection": connection_meta,
                            },
                        )
                    ]
                )
            for connection in crashed_sink._connections.values():
                connection.close()
            crashed_sink._connections.clear()

            legacy_db = sqlite3.connect(
                database_dir / f"engine_b_phase0_{first_partition}.sqlite3"
            )
            try:
                legacy_db.execute("DROP INDEX idx_data_gap_open_connection")
                legacy_db.execute("DROP INDEX idx_data_gap_open_order_book")
                legacy_db.execute("DELETE FROM schema_metadata")
                legacy_db.execute(
                    "INSERT INTO schema_metadata VALUES (?, ?)",
                    (7, connection_started_us),
                )
                market = venue.markets[0]
                for channel in ("connection", "order_book"):
                    legacy_db.executemany(
                        """INSERT INTO data_gap(
                             connection_session_id, venue, market_id, symbol,
                             channel, ts_start_us, reason
                           ) VALUES (?, ?, ?, ?, ?, ?, ?)""",
                        [
                            (
                                connection_meta["id"],
                                venue.name,
                                market.market_id,
                                market.symbol,
                                channel,
                                connection_started_us + offset,
                                "legacy_duplicate",
                            )
                            for offset in (100, 200)
                        ],
                    )
                legacy_db.commit()
            finally:
                legacy_db.close()

            recovered_sink = engine_b.DatabaseSink(
                config, "orphan-recovery-run", "orphan-recovery-commit"
            )
            with mock.patch.object(engine_b, "now_us", return_value=recovered_us):
                recovered_sink._write_batch(
                    [
                        (
                            "gap_close",
                            {
                                "recv_us": recovered_us,
                                "venue": venue.name,
                                "market_id": market.market_id,
                            },
                        )
                        for market in venue.markets
                    ]
                )
            self.assertEqual(
                list(recovered_sink.session_continuation_dir.glob("*.json")), []
            )
            self.assertEqual(
                list(recovered_sink.gap_continuation_dir.glob("*.json")), []
            )
            await recovered_sink.close()

            first_db = sqlite3.connect(
                database_dir / f"engine_b_phase0_{first_partition}.sqlite3"
            )
            try:
                self.assertEqual(
                    first_db.execute(
                        """SELECT started_ts_recv_us, ended_ts_recv_us, end_reason
                           FROM ws_connection"""
                    ).fetchone(),
                    (
                        connection_started_us,
                        connection_started_us,
                        "collector_restart_recovery",
                    ),
                )
                self.assertEqual(
                    first_db.execute(
                        """SELECT COUNT(*), MIN(ts_start_us), MAX(ts_end_us)
                           FROM data_gap WHERE channel = 'connection'"""
                    ).fetchone(),
                    (len(venue.markets), connection_started_us, next_hour_us),
                )
                self.assertEqual(
                    first_db.execute(
                        """SELECT COUNT(*), MIN(ts_start_us), MAX(ts_end_us)
                           FROM data_gap WHERE channel = 'order_book'"""
                    ).fetchone(),
                    (1, connection_started_us + 100, next_hour_us),
                )
            finally:
                first_db.close()

            for start_us, end_us in (
                (next_hour_us, second_hour_us),
                (second_hour_us, recovered_us),
            ):
                database = sqlite3.connect(
                    database_dir
                    / (
                        "engine_b_phase0_"
                        f"{engine_b.partition_for_us(start_us)}.sqlite3"
                    )
                )
                try:
                    self.assertEqual(
                        database.execute(
                            """SELECT COUNT(*), MIN(ts_start_us), MAX(ts_end_us)
                               FROM data_gap WHERE channel = 'connection'"""
                        ).fetchone(),
                        (len(venue.markets), start_us, end_us),
                    )
                    self.assertEqual(
                        database.execute(
                            """SELECT COUNT(*), MIN(ts_start_us), MAX(ts_end_us)
                               FROM data_gap WHERE channel = 'order_book'"""
                        ).fetchone(),
                        (1, start_us, end_us),
                    )
                finally:
                    database.close()

    async def test_crash_gap_runs_from_last_activity_to_replacement_snapshot(
        self,
    ) -> None:
        config = engine_b.load_config(CONFIG_PATH, LOCK_PATH)
        venue = next(item for item in config.venues if item.name == "robinhood")
        with tempfile.TemporaryDirectory() as directory:
            database_dir = Path(directory) / "data"
            object.__setattr__(config, "database_dir", database_dir)
            started_us = 1_774_884_000_000_000
            activity_us = started_us + 10_000_000
            recovered_us = activity_us + 120_000_000
            connection = {
                "id": "crashed-after-book",
                "venue": venue.name,
                "started_us": started_us,
                "api_schema_version": config.api_schema_version,
            }
            crashed_sink = engine_b.DatabaseSink(
                config, "crash-gap-source", "crash-gap-commit"
            )
            for path in (
                database_dir,
                crashed_sink.sealed_dir,
                crashed_sink.lock_dir,
                crashed_sink.gap_continuation_dir,
                crashed_sink.session_continuation_dir,
            ):
                path.mkdir(parents=True, exist_ok=True)
            with mock.patch.object(engine_b, "now_us", return_value=activity_us):
                crashed_sink._write_batch(
                    [
                        (
                            "connection_start",
                            {"recv_us": started_us, "connection": connection},
                        ),
                        (
                            "book",
                            {
                                "recv_us": activity_us,
                                "srv_us": activity_us,
                                "connection": connection,
                                "venue": venue.name,
                                "market_id": 37,
                                "symbol": "SKHY",
                                "event_kind": "snapshot",
                                "exchange_sequence": "10",
                                "begin_sequence": None,
                                "exchange_offset": None,
                                "local_sequence": 1,
                                "complete": True,
                                "levels": [],
                            },
                        ),
                    ]
                )
            for database in crashed_sink._connections.values():
                database.close()
            crashed_sink._connections.clear()

            recovered_sink = engine_b.DatabaseSink(
                config, "crash-gap-recovery", "crash-gap-commit"
            )
            with mock.patch.object(engine_b, "now_us", return_value=recovered_us):
                recovered_sink._write_batch(
                    [
                        (
                            "gap_close",
                            {
                                "recv_us": recovered_us,
                                "venue": venue.name,
                                "market_id": 37,
                            },
                        )
                    ]
                )
            await recovered_sink.close()

            database = sqlite3.connect(
                database_dir
                / f"engine_b_phase0_{engine_b.partition_for_us(started_us)}.sqlite3"
            )
            try:
                self.assertEqual(
                    database.execute(
                        """SELECT ended_ts_recv_us, end_reason
                           FROM ws_connection"""
                    ).fetchone(),
                    (activity_us, "collector_restart_recovery"),
                )
                self.assertEqual(
                    database.execute(
                        """SELECT ts_start_us, ts_end_us, reason FROM data_gap
                           WHERE venue = 'robinhood' AND market_id = 37
                             AND channel = 'connection'"""
                    ).fetchone(),
                    (
                        activity_us,
                        recovered_us,
                        "collector_restart_recovery",
                    ),
                )
            finally:
                database.close()

    async def test_session_marker_advances_past_sealed_destination(self) -> None:
        config = engine_b.load_config(CONFIG_PATH, LOCK_PATH)
        with tempfile.TemporaryDirectory() as directory:
            database_dir = Path(directory) / "data"
            object.__setattr__(config, "database_dir", database_dir)
            source_start_us = 1_700_000_000_000_000
            source_start_us -= source_start_us % 3_600_000_000
            sealed_start_us = source_start_us + 3_600_000_000
            sealed_end_us = sealed_start_us + 3_600_000_000
            recovered_us = sealed_end_us + 120_000_000
            source_partition = engine_b.partition_for_us(source_start_us)
            sealed_partition = engine_b.partition_for_us(sealed_start_us)
            recovered_partition = engine_b.partition_for_us(sealed_end_us)
            sink = engine_b.DatabaseSink(
                config, "sealed-session-run", "sealed-session-commit"
            )
            for path in (
                database_dir,
                sink.sealed_dir,
                sink.lock_dir,
                sink.gap_continuation_dir,
                sink.session_continuation_dir,
            ):
                path.mkdir(parents=True, exist_ok=True)
            sealed_sha = "b" * 64
            (sink.sealed_dir / f"{sealed_partition}.json").write_text(
                json.dumps({"partition": sealed_partition, "sha256": sealed_sha})
                + "\n"
            )
            sealed_index = sqlite3.connect(
                sink.sealed_dir / f"{sealed_partition}.trade_ids.sqlite3"
            )
            sealed_index.execute(
                """CREATE TABLE sealed_metadata(
                     partition TEXT PRIMARY KEY,
                     canonical_db_sha256 TEXT NOT NULL
                   )"""
            )
            sealed_index.execute(
                """CREATE TABLE archived_connection_session(
                     connection_session_id TEXT PRIMARY KEY,
                     started_ts_recv_us INTEGER NOT NULL,
                     ended_ts_recv_us INTEGER,
                     end_reason TEXT
                   ) WITHOUT ROWID"""
            )
            sealed_index.execute(
                "INSERT INTO archived_connection_session VALUES (?, ?, ?, ?)",
                (
                    "sealed-session",
                    sealed_start_us,
                    sealed_end_us,
                    "partition_rotation",
                ),
            )
            sealed_index.execute(
                "INSERT INTO sealed_metadata VALUES (?, ?)",
                (sealed_partition, sealed_sha),
            )
            sealed_index.commit()
            sealed_index.close()
            sink._persist_session_continuation_marker(
                {
                    "continuation_id": (
                        f"partition:{source_partition}:sealed-session"
                    ),
                    "start_us": sealed_start_us,
                    "source_partition": source_partition,
                    "connection": {
                        "id": "sealed-session",
                        "venue": "robinhood",
                        "started_us": sealed_start_us,
                        "api_schema_version": config.api_schema_version,
                    },
                }
            )

            with mock.patch.object(engine_b, "now_us", return_value=recovered_us):
                sink._write_batch([])
            self.assertEqual(
                list(sink.session_continuation_dir.glob("*.json")), []
            )
            await sink.close()

            recovered_db = sqlite3.connect(
                database_dir / f"engine_b_phase0_{recovered_partition}.sqlite3"
            )
            try:
                self.assertEqual(
                    recovered_db.execute(
                        """SELECT started_ts_recv_us, ended_ts_recv_us, end_reason
                           FROM ws_connection"""
                    ).fetchone(),
                    (
                        sealed_end_us,
                        sealed_end_us,
                        "collector_restart_recovery",
                    ),
                )
                self.assertEqual(
                    recovered_db.execute(
                        "SELECT COUNT(*) FROM sealed_session_interval"
                    ).fetchone(),
                    (0,),
                )
                robinhood = next(
                    item for item in config.venues if item.name == "robinhood"
                )
                self.assertEqual(
                    recovered_db.execute(
                        """SELECT COUNT(*), MIN(sealed_partition),
                                  MIN(ts_start_us), MAX(ts_end_us),
                                  COUNT(DISTINCT reason)
                           FROM sealed_gap_interval"""
                    ).fetchone(),
                    (
                        len(robinhood.markets),
                        sealed_partition,
                        sealed_start_us,
                        sealed_end_us,
                        1,
                    ),
                )
                self.assertEqual(
                    recovered_db.execute(
                        """SELECT COUNT(*), MIN(ts_start_us), MAX(ts_end_us)
                           FROM data_gap WHERE venue = 'robinhood'
                             AND channel = 'connection'"""
                    ).fetchone(),
                    (len(robinhood.markets), sealed_end_us, None),
                )
            finally:
                recovered_db.close()


    async def test_deduplicates_and_merges_late_trades_in_event_partition(self) -> None:
        config = engine_b.load_config(CONFIG_PATH, LOCK_PATH)
        with tempfile.TemporaryDirectory() as directory:
            database_dir = Path(directory)
            object.__setattr__(config, "database_dir", database_dir)
            sink = engine_b.DatabaseSink(config, "late-run", "late-commit")
            sink.start()
            hour_start_us = 1_700_000_000_000_000
            hour_start_us -= hour_start_us % 3_600_000_000
            receive_us = hour_start_us + 3_605_000_000
            connection = {
                "id": "connection-late",
                "venue": "robinhood",
                "started_us": receive_us,
                "api_schema_version": config.api_schema_version,
            }

            def trade_payload(
                trade_id: str, event_offset_us: int, local_sequence: int,
                price: str, size: str,
            ) -> dict[str, object]:
                event_us = hour_start_us + event_offset_us
                return {
                    "recv_us": receive_us + local_sequence,
                    "partition_us": event_us,
                    "event_ts_us": event_us,
                    "bucket_start_us": event_us - event_us % 60_000_000,
                    "srv_us": event_us,
                    "connection": connection,
                    "venue": "robinhood",
                    "market_id": 37,
                    "symbol": "SKHY",
                    "trade_id": trade_id,
                    "exchange_sequence": str(local_sequence),
                    "local_sequence": local_sequence,
                    "price": price,
                    "size": size,
                    "aggressor_side": "buy",
                    "raw_public_json": json.dumps({"trade_id": trade_id}),
                }

            for payload in (
                trade_payload("a", 3_570_000_000, 1, "100", "1"),
                trade_payload("a", 3_570_000_000, 2, "100", "1"),
                trade_payload("b", 3_550_000_000, 3, "90", "2"),
                trade_payload("c", 3_590_000_000, 4, "110", "3"),
            ):
                await sink.put("trade", payload)
            await sink.queue.join()
            await sink.close()

            event_partition = engine_b.partition_for_us(hour_start_us)
            receive_partition = engine_b.partition_for_us(receive_us)
            db_path = database_dir / f"engine_b_phase0_{event_partition}.sqlite3"
            self.assertTrue(db_path.is_file())
            self.assertFalse(
                (database_dir / f"engine_b_phase0_{receive_partition}.sqlite3").exists()
            )
            connection_db = sqlite3.connect(db_path)
            try:
                self.assertEqual(connection_db.execute("SELECT COUNT(*) FROM trade").fetchone(), (3,))
                self.assertEqual(
                    connection_db.execute(
                        """SELECT open, high, low, close, volume, trade_count, is_complete,
                                  first_trade_ts_us, last_trade_ts_us
                           FROM ohlcv_1m"""
                    ).fetchone(),
                    (
                        "90", "110", "90", "110", "6", 3, 1,
                        hour_start_us + 3_550_000_000,
                        hour_start_us + 3_590_000_000,
                    ),
                )
                self.assertEqual(connection_db.execute("PRAGMA integrity_check").fetchone(), ("ok",))
            finally:
                connection_db.close()


    async def test_deduplicates_archive_replay_and_routes_new_sealed_trade(self) -> None:
        config = engine_b.load_config(CONFIG_PATH, LOCK_PATH)
        with tempfile.TemporaryDirectory() as directory:
            database_dir = Path(directory) / "data"
            object.__setattr__(config, "database_dir", database_dir)
            sink = engine_b.DatabaseSink(config, "sealed-run", "sealed-commit")
            sink.start()
            recv_us = engine_b.now_us()
            event_us = recv_us - 7_200_000_000
            event_partition = engine_b.partition_for_us(event_us)
            (sink.sealed_dir / f"{event_partition}.json").write_text(
                json.dumps(
                    {
                        "partition": event_partition,
                        "sha256": "canonical-sha",
                        "trade_index": f"{event_partition}.trade_ids.sqlite3",
                    }
                )
                + "\n"
            )
            trade_index = sink.sealed_dir / f"{event_partition}.trade_ids.sqlite3"
            index_connection = sqlite3.connect(trade_index)
            index_connection.execute(
                "CREATE TABLE sealed_metadata(partition TEXT PRIMARY KEY, canonical_db_sha256 TEXT NOT NULL)"
            )
            index_connection.execute(
                """CREATE TABLE archived_trade_identity(
                     venue TEXT NOT NULL,
                     market_id INTEGER NOT NULL,
                     exchange_trade_id TEXT NOT NULL,
                     PRIMARY KEY(venue, market_id, exchange_trade_id)
                   ) WITHOUT ROWID"""
            )
            index_connection.execute(
                """CREATE TABLE late_trade_identity(
                     venue TEXT NOT NULL,
                     market_id INTEGER NOT NULL,
                     exchange_trade_id TEXT NOT NULL,
                     PRIMARY KEY(venue, market_id, exchange_trade_id)
                   ) WITHOUT ROWID"""
            )
            index_connection.execute(
                """CREATE TABLE archived_trade_replay_alias(
                     venue TEXT NOT NULL,
                     market_id INTEGER NOT NULL,
                     replay_alias TEXT NOT NULL,
                     exchange_trade_id TEXT NOT NULL,
                     PRIMARY KEY(venue, market_id, exchange_trade_id)
                   ) WITHOUT ROWID"""
            )
            index_connection.execute(
                "INSERT INTO sealed_metadata VALUES (?, ?)",
                (event_partition, "canonical-sha"),
            )
            index_connection.execute(
                "INSERT INTO archived_trade_identity VALUES (?, ?, ?)",
                ("robinhood", 37, "archived-before-seal"),
            )
            index_connection.commit()
            index_connection.close()
            connection = {
                "id": "connection-sealed",
                "venue": "robinhood",
                "started_us": recv_us,
                "api_schema_version": config.api_schema_version,
            }
            def sealed_trade(trade_id: str, sequence: int) -> dict[str, object]:
                return {
                    "recv_us": recv_us,
                    "partition_us": event_us,
                    "event_ts_us": event_us,
                    "bucket_start_us": event_us - event_us % 60_000_000,
                    "srv_us": event_us,
                    "connection": connection,
                    "venue": "robinhood",
                    "market_id": 37,
                    "symbol": "SKHY",
                    "trade_id": trade_id,
                    "exchange_sequence": f"late-{sequence}",
                    "local_sequence": sequence,
                    "price": "101",
                    "size": "2",
                    "aggressor_side": "buy",
                    "raw_public_json": "{}",
                }

            await sink.put("trade", sealed_trade("archived-before-seal", 1))
            await sink.put("trade", sealed_trade("late-after-seal", 2))
            await sink.put(
                "connection_end",
                {
                    "recv_us": recv_us,
                    "connection": connection,
                    "reason": "test_complete",
                },
            )
            await sink.queue.join()
            await sink.close()

            index_connection = sqlite3.connect(trade_index)
            try:
                self.assertEqual(
                    index_connection.execute(
                        "SELECT exchange_trade_id FROM late_trade_identity"
                    ).fetchall(),
                    [],
                )
            finally:
                index_connection.close()

            second_recv_us = recv_us + 3_600_000_000
            second_partition = engine_b.partition_for_us(second_recv_us)
            sink2 = engine_b.DatabaseSink(config, "sealed-run-2", "sealed-commit")
            with mock.patch.object(engine_b, "now_us", return_value=second_recv_us):
                sink2.start()
                replay = sealed_trade("late-after-seal", 3)
                replay["recv_us"] = second_recv_us
                await sink2.put("trade", replay)
                await sink2.queue.join()
                await sink2.close()
            self.assertFalse(
                (database_dir / f"engine_b_phase0_{second_partition}.sqlite3").exists()
            )

            self.assertFalse(
                (database_dir / f"engine_b_phase0_{event_partition}.sqlite3").exists()
            )
            active_partition = engine_b.partition_for_us(recv_us)
            active_db = database_dir / f"engine_b_phase0_{active_partition}.sqlite3"
            index_connection = sqlite3.connect(trade_index)
            index_connection.execute("DROP TABLE archived_trade_replay_alias")
            index_connection.commit()
            index_connection.close()
            self.assertEqual(
                engine_b.reconcile_late_trade_identities(active_db, sink.sealed_dir),
                1,
            )
            index_connection = sqlite3.connect(trade_index)
            try:
                self.assertEqual(
                    index_connection.execute(
                        """SELECT exchange_trade_id FROM late_trade_identity
                           ORDER BY exchange_trade_id"""
                    ).fetchall(),
                    [("late-after-seal",)],
                )
                self.assertEqual(
                    index_connection.execute(
                        """SELECT COUNT(*) FROM sqlite_master
                           WHERE type = 'table'
                             AND name = 'archived_trade_replay_alias'"""
                    ).fetchone(),
                    (1,),
                )
            finally:
                index_connection.close()
            connection_db = sqlite3.connect(active_db)
            try:
                self.assertEqual(connection_db.execute("SELECT COUNT(*) FROM trade").fetchone(), (0,))
                self.assertEqual(connection_db.execute("SELECT COUNT(*) FROM ohlcv_1m").fetchone(), (0,))
                self.assertEqual(
                    connection_db.execute(
                        """SELECT exchange_trade_id, sealed_partition FROM late_trade
                           ORDER BY exchange_trade_id"""
                    ).fetchall(),
                    [("late-after-seal", event_partition)],
                )
                self.assertEqual(connection_db.execute("PRAGMA integrity_check").fetchone(), ("ok",))
            finally:
                connection_db.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)
