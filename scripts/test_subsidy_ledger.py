#!/usr/bin/env python3
"""Regression tests for scripts/subsidy_ledger.py (bot-strategy#938)."""

from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from subsidy_ledger import (  # noqa: E402
    ExecDay,
    PnlDay,
    arm_from_pnl_filename,
    build_rows,
    equity_daily_costs,
    load_execution,
    load_pnl,
    summarize,
)

# 2026-09-08 12:00 UTC.
TS = 1788868800


def write(path: Path, records: list[dict]) -> Path:
    path.write_text("".join(json.dumps(r) + "\n" for r in records), encoding="utf-8")
    return path


def test_cost_is_pnl_and_funding_only():
    # Realized PnL already contains the fees and the slippage, so the
    # slippage column must not be added into the cost as well.
    rows = build_rows(
        {("2026-09-08", "freq"): ExecDay(fills=4, volume_usd=500_000.0, slippage_usd=15.0)},
        {("2026-09-08", "freq"): PnlDay(cycles=2, realized_pnl_usd=-40.0, funding_usd=-2.0, funding_seen=True)},
    )
    assert len(rows) == 1
    row = rows[0]
    assert row.cost_usd == 42.0, row.cost_usd
    assert row.slippage_usd == 15.0
    assert row.cost_source == "pnl_ledger"
    # $42 given up across $0.5M traded.
    assert row.cost_per_musd_volume == 84.0, row.cost_per_musd_volume
    # An arm that came out ahead reports a negative cost, not a floor at 0.
    ahead = build_rows(
        {("2026-09-08", "freq"): ExecDay(fills=1, volume_usd=1_000_000.0)},
        {("2026-09-08", "freq"): PnlDay(cycles=1, realized_pnl_usd=25.0, funding_usd=0.0, funding_seen=True)},
    )[0]
    assert ahead.cost_usd == -25.0
    assert ahead.cost_per_musd_volume == -25.0


def test_volume_without_a_cost_source_is_never_counted_as_free():
    # The two ledgers do not cover the same days on the live host: the
    # execution ledger starts 2026-08-14 and the PnL ledger 2026-09-01.
    # Reporting the earlier days at zero cost would make the arms' worst
    # stretch look like their cheapest.
    rows = build_rows(
        {
            ("2026-08-20", "freq"): ExecDay(fills=10, volume_usd=800_000.0),
            ("2026-09-08", "freq"): ExecDay(fills=4, volume_usd=200_000.0),
        },
        {("2026-09-08", "freq"): PnlDay(cycles=1, realized_pnl_usd=-10.0)},
    )
    uncosted, costed = rows[0], rows[1]
    assert uncosted.cost_usd is None and uncosted.cost_source is None
    assert uncosted.cost_per_musd_volume is None
    assert costed.cost_usd == 10.0

    summary = summarize(rows)["arms"][0]
    assert summary["volume_usd"] == 1_000_000.0
    assert summary["costed_volume_usd"] == 200_000.0
    assert summary["uncosted_volume_usd"] == 800_000.0
    # The ratio describes only the volume the cost is known for.
    assert summary["cost_per_musd_volume"] == 50.0, summary["cost_per_musd_volume"]


def test_equity_fills_the_gap_and_is_labelled_as_such():
    equity = {"freq": equity_daily_costs([
        {"ts": 1788595200000, "equity": 5000.0},   # 2026-09-05
        {"ts": 1788681600000, "equity": 4900.0},   # 2026-09-06: down 100
        {"ts": 1788768000000, "equity": 4950.0},   # 2026-09-07: up 50
    ])}
    # The first day of the series has no previous close to measure from.
    assert "2026-09-05" not in equity["freq"]
    assert equity["freq"]["2026-09-06"] == 100.0
    assert equity["freq"]["2026-09-07"] == -50.0

    rows = build_rows(
        {("2026-09-06", "freq"): ExecDay(fills=2, volume_usd=100_000.0)},
        {},
        equity,
    )
    assert rows[0].cost_usd == 100.0
    assert rows[0].cost_source == "equity_delta"

    # The PnL ledger wins where it exists: it is realized cycles, while an
    # equity delta also carries mark-to-market.
    both = build_rows(
        {("2026-09-06", "freq"): ExecDay(fills=2, volume_usd=100_000.0)},
        {("2026-09-06", "freq"): PnlDay(cycles=1, realized_pnl_usd=-7.0)},
        equity,
    )
    assert both[0].cost_usd == 7.0
    assert both[0].cost_source == "pnl_ledger"


def test_points_are_optional_and_only_divide_when_present():
    execution = {("2026-09-08", "freq"): ExecDay(fills=1, volume_usd=500_000.0)}
    pnl = {("2026-09-08", "freq"): PnlDay(cycles=1, realized_pnl_usd=-50.0)}
    without = build_rows(execution, pnl)[0]
    assert without.points is None and without.cost_per_point is None
    # The volume KPI still works with no points at all, which is the point
    # of carrying it: the venue's points routes are not readable from here.
    assert without.cost_per_musd_volume == 100.0

    with_points = build_rows(execution, pnl, None, {("2026-09-08", "freq"): 2000.0})[0]
    assert with_points.cost_per_point == 0.025
    # Zero points is not a denominator.
    zero = build_rows(execution, pnl, None, {("2026-09-08", "freq"): 0.0})[0]
    assert zero.cost_per_point is None


def test_reads_the_real_ledger_shapes():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        write(
            root / "execution-debot-pair-robinhood-lighter_20260908.jsonl",
            [
                {"event": "leg_fill", "ts_ms": TS * 1000, "variant": "freq",
                 "notional_usd": 10_000.0, "slippage_usd_vs_decision": 1.5},
                # fill_value stands in when notional_usd is absent.
                {"event": "leg_fill", "ts_ms": TS * 1000, "variant": "freq", "fill_value": 5_000.0},
                # Both legs of a pair are real volume to a points program.
                {"event": "leg_fill", "ts_ms": TS * 1000, "variant": "b", "notional_usd": -2_000.0},
                # Summaries are not fills and must not be counted twice.
                {"event": "pair_fill_summary", "ts_ms": TS * 1000, "variant": "freq",
                 "notional_usd": 99_999.0},
            ],
        )
        execution = load_execution(sorted(root.glob("execution-*.jsonl")))
        assert execution[("2026-09-08", "freq")].fills == 2
        assert execution[("2026-09-08", "freq")].volume_usd == 15_000.0
        assert execution[("2026-09-08", "freq")].slippage_usd == 1.5
        # Notional is taken as an absolute value: a short leg is volume too.
        assert execution[("2026-09-08", "b")].volume_usd == 2_000.0

        write(
            root / "pnl-debot-pair-robinhood-lighter-freq-20260908.jsonl",
            [
                {"ts": TS, "pnl": -3.0, "funding_carry_usd": -0.01},
                {"ts": TS, "pnl": 1.0, "funding_carry_usd": -0.02},
                {"ts": TS, "source": "no_pnl_row"},
            ],
        )
        pnl = load_pnl(sorted(root.glob("pnl-*.jsonl")))
        day = pnl[("2026-09-08", "freq")]
        assert day.cycles == 2
        assert round(day.realized_pnl_usd, 6) == -2.0
        assert round(day.funding_usd, 6) == -0.03

        # A torn trailing line (the bot appends while this runs) is skipped,
        # not fatal.
        torn = root / "execution-debot-pair-robinhood-lighter_20260907.jsonl"
        torn.write_text('{"event": "leg_fill", "ts_ms": 1, "variant": "freq", "notion',
                        encoding="utf-8")
        assert load_execution([torn]) == {}


def test_arm_is_taken_from_the_pnl_filename():
    assert arm_from_pnl_filename("pnl-debot-pair-robinhood-lighter-freq-20260908.jsonl") == "freq"
    assert arm_from_pnl_filename("pnl-debot-pair-robinhood-lighter-b-20260908.jsonl") == "b"
    assert arm_from_pnl_filename("execution-debot-pair-robinhood-lighter_20260908.jsonl") is None
    assert arm_from_pnl_filename("pnl-no-date.jsonl") is None


def main() -> int:
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for test in tests:
        test()
        print(f"ok  {test.__name__}")
    print(f"\n{len(tests)} passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
