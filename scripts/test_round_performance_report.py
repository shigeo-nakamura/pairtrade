#!/usr/bin/env python3
"""Unit tests for round_performance_report.py."""

from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from round_performance_report import EquitySample, build_report, remove_isolated_spikes


def _ms(raw: str) -> int:
    return int(datetime.fromisoformat(raw.replace("Z", "+00:00")).timestamp() * 1000)


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


class EquitySpikeTests(unittest.TestCase):
    def test_removes_only_isolated_spike(self):
        samples = [
            EquitySample(1, 1000.0),
            EquitySample(2, 1200.0),
            EquitySample(3, 1000.1),
            EquitySample(4, 1600.0),
            EquitySample(5, 1600.0),
        ]
        kept, removed = remove_isolated_spikes(
            samples,
            reference=1000.0,
            threshold_bps=1000.0,
            neighbor_tolerance_bps=5.0,
        )
        self.assertEqual([row.ts_ms for row in removed], [2])
        self.assertEqual([row.ts_ms for row in kept], [1, 3, 4, 5])


class RoundReportTests(unittest.TestCase):
    def test_pairwise_opportunity_attribution_and_flat_collateral(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            until = datetime(2026, 1, 3, tzinfo=timezone.utc)
            manifest = {
                "round": "round-test",
                "service": "debot-pair-test",
                "since": "2026-01-01T00:00:00Z",
                "daily_dd_lookback_days": 30,
                "pair_match_tolerance_secs": 20,
                "variants": {
                    "b": {
                        "agent": "debot-pair-test-b",
                        "equity_reference_usd": 1000,
                        "start_collateral_usd": 1000,
                    },
                    "c": {
                        "agent": "debot-pair-test-c",
                        "equity_reference_usd": 1000,
                        "start_collateral_usd": 1000,
                    },
                },
                "comparisons": [{"left": "b", "right": "c", "label": "B-C"}],
            }
            _write_jsonl(
                root / "b.equity_history.jsonl",
                [
                    {"ts": _ms("2026-01-01T00:00:00Z"), "equity": 1000.0},
                    {"ts": _ms("2026-01-02T00:00:00Z"), "equity": 1003.0},
                ],
            )
            _write_jsonl(
                root / "c.equity_history.jsonl",
                [
                    {"ts": _ms("2026-01-01T00:00:00Z"), "equity": 1000.0},
                    {"ts": _ms("2026-01-02T00:00:00Z"), "equity": 1001.0},
                ],
            )
            base_ts = datetime(2026, 1, 1, 1, tzinfo=timezone.utc).timestamp()
            _write_jsonl(
                root / "pnl-debot-pair-test-b-20260101.jsonl",
                [
                    {
                        "ts": base_ts + 100,
                        "hold_secs": 100,
                        "pnl": 1.0,
                        "direction": "long_spread",
                        "close_reason": "exit_z",
                    },
                    {
                        "ts": base_ts + 3700,
                        "hold_secs": 100,
                        "pnl": 2.0,
                        "direction": "short_spread",
                        "close_reason": "force_close",
                    },
                ],
            )
            _write_jsonl(
                root / "pnl-debot-pair-test-c-20260101.jsonl",
                [
                    {
                        "ts": base_ts + 105,
                        "hold_secs": 100,
                        "pnl": 1.0,
                        "direction": "long_spread",
                        "close_reason": "exit_z",
                    }
                ],
            )
            metrics = root / "metrics.prom"
            metrics.write_text(
                'pairtrade_has_position{pair="BTC/ETH",variant="b"} 0\n'
                'pairtrade_has_position{pair="BTC/ETH",variant="c"} 0\n',
                encoding="utf-8",
            )

            report, packet = build_report(manifest, root, until, metrics)
            comparison = packet["comparisons"][0]
            self.assertAlmostEqual(packet["variants"]["b"]["collateral_return_bps"], 30.0)
            self.assertAlmostEqual(packet["variants"]["c"]["collateral_return_bps"], 10.0)
            self.assertAlmostEqual(comparison["collateral_gap_bps"], 20.0)
            self.assertEqual(comparison["paired_trades"], 1)
            self.assertEqual(comparison["left_only_trades"], 1)
            self.assertEqual(comparison["right_only_trades"], 0)
            self.assertAlmostEqual(comparison["paired_gap_bps_secondary"], 0.0)
            self.assertAlmostEqual(comparison["opportunity_gap_bps_secondary"], 20.0)
            self.assertEqual(packet["flat"], {"b": True, "c": True})
            self.assertIn("B-C", report)


if __name__ == "__main__":
    unittest.main()
