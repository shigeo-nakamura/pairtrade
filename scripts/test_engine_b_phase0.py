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
            "channel": "trade/37",
            "nonce": 9001,
            "timestamp": 1_774_884_082_309,
            "trades": [trade, trade, other],
        }
        overlapping_message = {
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
            self.assertIn(f"/tmp/engine-b-phase0-units/{unit}", workflow)
        self.assertIn("ENGINE_B_PHASE0_CODE_COMMIT", installer)
        self.assertIn('"$INSTALL_DIR/release.env"', installer)
        self.assertIn("ENGINE_B_PHASE0_CODE_COMMIT=${GITHUB_SHA}", workflow)
        self.assertIn("systemctl daemon-reload", installer)
        self.assertNotIn("systemctl restart", installer)
        self.assertNotIn("systemctl start", installer)

    def test_archive_deletion_is_disabled_in_unit(self) -> None:
        unit = ARCHIVE_UNIT_PATH.read_text()
        self.assertIn("ENGINE_B_PHASE0_DELETE_VERIFIED_LOCAL=false", unit)

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
        self.assertNotIn("ExecStart=/bin/bash /opt/debot/", observer)
        self.assertNotIn("ExecStart=/bin/bash /opt/debot/", archive)
        self.assertIn('useradd --system --gid "$SERVICE_GROUP"', installer)
        self.assertIn('"$INSTALL_DIR/engine_b_phase0.py"', installer)
        self.assertIn('"$INSTALL_DIR/engine_b_phase0_archive.sh"', installer)


class DatabaseTests(unittest.IsolatedAsyncioTestCase):
    async def test_normalized_public_data_is_persisted(self) -> None:
        config = engine_b.load_config(CONFIG_PATH, LOCK_PATH)
        with tempfile.TemporaryDirectory() as directory:
            object.__setattr__(config, "database_dir", Path(directory))
            sink = engine_b.DatabaseSink(config, "test-run", "test-commit")
            sink.start()
            recv_us = 1_774_884_082_309_144
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

            db_path = next(Path(directory).glob("engine_b_phase0_*.sqlite3"))
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
                    ("159.71", "159.71", "159.71", "159.71", "1.25", 1, 1),
                )
                self.assertEqual(connection_db.execute("PRAGMA integrity_check").fetchone(), ("ok",))
            finally:
                connection_db.close()


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
            await sink.queue.join()
            await sink.close()

            index_connection = sqlite3.connect(trade_index)
            try:
                self.assertEqual(
                    index_connection.execute(
                        "SELECT exchange_trade_id FROM late_trade_identity"
                    ).fetchall(),
                    [("late-after-seal",)],
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
            connection_db = sqlite3.connect(active_db)
            try:
                self.assertEqual(connection_db.execute("SELECT COUNT(*) FROM trade").fetchone(), (0,))
                self.assertEqual(connection_db.execute("SELECT COUNT(*) FROM ohlcv_1m").fetchone(), (0,))
                self.assertEqual(
                    connection_db.execute(
                        "SELECT exchange_trade_id, sealed_partition FROM late_trade"
                    ).fetchone(),
                    ("late-after-seal", event_partition),
                )
                self.assertEqual(connection_db.execute("PRAGMA integrity_check").fetchone(), ("ok",))
            finally:
                connection_db.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)
