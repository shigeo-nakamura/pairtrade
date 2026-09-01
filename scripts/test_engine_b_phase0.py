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


SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
MODULE_PATH = SCRIPT_DIR / "engine_b_phase0.py"
CONFIG_PATH = REPO_ROOT / "configs" / "engine-b" / "phase0.json"
LOCK_PATH = SCRIPT_DIR / "engine_b_phase0_requirements.txt"

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


class DatabaseTests(unittest.IsolatedAsyncioTestCase):
    async def test_normalized_public_data_is_persisted(self) -> None:
        config = engine_b.load_config(CONFIG_PATH, LOCK_PATH)
        with tempfile.TemporaryDirectory() as directory:
            object.__setattr__(config, "database_dir", Path(directory))
            sink = engine_b.DatabaseSink(config, "test-run", "test-commit")
            sink.start()
            recv_us = 1_774_884_082_309_144
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
                    "srv_us": recv_us - 5,
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
                self.assertEqual(connection_db.execute("PRAGMA integrity_check").fetchone(), ("ok",))
            finally:
                connection_db.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)
