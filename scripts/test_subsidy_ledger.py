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
    SubsidyLedgerError,
    equity_daily_costs,
    load_execution,
    load_pnl,
    load_points,
    expand,
    render_table,
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


def test_points_from_uncosted_days_do_not_cheapen_the_price():
    # Points earned on a day whose cost is unknown must not enter the
    # denominator: the numerator only covers the costed days, so counting
    # them would understate the price per point.
    rows = build_rows(
        {
            ("2026-08-20", "freq"): ExecDay(fills=5, volume_usd=500_000.0),
            ("2026-09-08", "freq"): ExecDay(fills=5, volume_usd=500_000.0),
        },
        {("2026-09-08", "freq"): PnlDay(cycles=1, realized_pnl_usd=-100.0)},
        None,
        {("2026-08-20", "freq"): 3000.0, ("2026-09-08", "freq"): 1000.0},
    )
    assert rows[0].cost_usd is None and rows[0].points == 3000.0
    summary = summarize(rows)["arms"][0]
    assert summary["cost_usd"] == 100.0
    assert summary["points"] == 1000.0
    assert summary["uncosted_points"] == 3000.0
    # $100 / 1000 points, not $100 / 4000.
    assert summary["cost_per_point"] == 0.1, summary["cost_per_point"]


def pnl_file(root: Path, records: list[dict], arm: str = "freq", date: str = "20260908") -> Path:
    return write(root / f"pnl-debot-pair-robinhood-lighter-{arm}-{date}.jsonl", records)


def test_a_day_holding_any_non_realized_row_is_not_costed_from_the_pnl_ledger():
    """One good close beside one placeholder is not a cheap day.

    This is the shape that matters: a day containing *only* placeholders
    was already caught, but a mixed day quietly reported the good close
    as if it were the whole day's cost, and that partial sum then
    suppressed the equity fallback. Coverage is per day, not per row.
    """
    with tempfile.TemporaryDirectory() as tmp:
        path = pnl_file(
            Path(tmp),
            [
                {"ts": TS, "source": "exit_fill", "pnl": -5.0, "hold_secs": 600},
                # Real shape, seen in the archived ledgers: a recovery
                # placeholder carries pnl 0 and an explicit unavailable flag
                # because the result only exists in the venue's ledger.
                {"ts": TS, "source": "recovery_no_pnl", "pnl": 0.0, "pnl_available": False},
            ],
        )
        day = load_pnl([path])[("2026-09-08", "freq")]
        assert day.cycles == 1
        assert day.realized_pnl_usd == -5.0
        assert day.incomplete
        assert day.incomplete_reasons == {"pnl_available_false"}

        # The day therefore takes the equity delta, not the partial sum.
        rows = build_rows(
            {("2026-09-08", "freq"): ExecDay(fills=2, volume_usd=100_000.0)},
            {("2026-09-08", "freq"): day},
            equity_costs={"freq": {"2026-09-08": 31.0}},
        )
        assert rows[0].cost_usd == 31.0
        assert rows[0].cost_source == "equity_delta"
        assert rows[0].pnl_coverage == "incomplete"

        # With no equity series to fall back on it stays uncosted, never
        # reported as the partial -(-5.0).
        bare = build_rows(
            {("2026-09-08", "freq"): ExecDay(fills=2, volume_usd=100_000.0)},
            {("2026-09-08", "freq"): day},
        )[0]
        assert bare.cost_usd is None and bare.cost_source is None


def test_a_dry_run_close_is_not_money():
    """`exit_dry_run` rows are simulated fills and price nothing.

    129 of the 645 rows in the archived ledgers are these. Costing a day
    from them would report a number that no account ever paid.
    """
    with tempfile.TemporaryDirectory() as tmp:
        path = pnl_file(Path(tmp), [{"ts": TS, "source": "exit_dry_run", "pnl": -9.0, "hold_secs": 600}])
        day = load_pnl([path])[("2026-09-08", "freq")]
        assert day.cycles == 0
        assert day.incomplete and day.incomplete_reasons == {"source:exit_dry_run"}
        assert build_rows({}, {("2026-09-08", "freq"): day})[0].cost_usd is None


def test_an_unrecognised_source_makes_a_day_uncosted_not_free():
    """The allowlist fails safe: unknown means unknown, not zero."""
    with tempfile.TemporaryDirectory() as tmp:
        path = pnl_file(Path(tmp), [{"ts": TS, "source": "some_future_close", "pnl": -4.0, "hold_secs": 600}])
        day = load_pnl([path])[("2026-09-08", "freq")]
        assert day.incomplete and day.cycles == 0
        row = build_rows({("2026-09-08", "freq"): ExecDay(fills=1, volume_usd=50_000.0)},
                         {("2026-09-08", "freq"): day})[0]
        assert row.cost_usd is None
        assert row.pnl_incomplete_reasons == ["source:some_future_close"]
        # And the volume it traded is reported as uncovered, not dropped.
        assert summarize([row])["arms"][0]["uncosted_volume_usd"] == 50_000.0


def test_interior_corruption_is_raised_while_a_torn_tail_is_tolerated():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        damaged = root / "execution-debot-pair-robinhood-lighter_20260908.jsonl"
        damaged.write_text(
            '{"event": "leg_fill", "ts_ms": 1, "variant": "freq", "notional_usd": 1}\n'
            '{"event": "leg_fill", "ts_ms"\n'
            '{"event": "leg_fill", "ts_ms": 2, "variant": "freq", "notional_usd": 2}\n',
            encoding="utf-8",
        )
        try:
            load_execution([damaged])
        except SubsidyLedgerError as error:
            assert "malformed JSON in the middle" in str(error)
        else:
            raise AssertionError("interior corruption was silently skipped")


def test_an_equity_gap_leaves_the_later_day_uncosted():
    """A two-day change is not one day's cost.

    Charging a 09-05 -> 09-07 delta entirely to 09-07, then dividing it
    by only 09-07's volume, reports a day that never happened.
    """
    series = [
        {"ts": 1788609600_000, "equity": 1000.0},   # 2026-09-05
        {"ts": 1788782400_000, "equity": 900.0},    # 2026-09-07
        {"ts": 1788868800_000, "equity": 880.0},    # 2026-09-08
    ]
    costs = equity_daily_costs(series)
    assert "2026-09-07" not in costs, costs
    assert costs["2026-09-08"] == 20.0, costs


def test_a_ratio_never_mixes_a_cost_with_another_day_denominator():
    """Numerator and denominator must come from the same rows.

    The three inputs are selected independently and cover different day
    ranges, so a cost on a day the other input does not cover has no
    denominator of its own. Charging it to the days that do have one
    produces a number about no real period.
    """
    rows = build_rows(
        # 09-07 traded but no points were supplied for it; 09-08 has both.
        {("2026-09-07", "freq"): ExecDay(fills=1, volume_usd=1_000_000.0),
         ("2026-09-08", "freq"): ExecDay(fills=1, volume_usd=1_000_000.0)},
        {("2026-09-07", "freq"): PnlDay(cycles=1, realized_pnl_usd=-100.0, funding_seen=True),
         ("2026-09-08", "freq"): PnlDay(cycles=1, realized_pnl_usd=-100.0, funding_seen=True)},
        points={("2026-09-08", "freq"): 1000.0},
    )
    arm = summarize(rows)["arms"][0]
    # $100 over the 1000 points that were actually supplied, not $200.
    assert arm["cost_per_point"] == 0.1, arm["cost_per_point"]
    assert arm["cost_usd_without_points"] == 100.0
    # The volume ratio still spans both days, because both measured volume.
    assert arm["cost_usd"] == 200.0
    assert arm["cost_per_musd_volume"] == 100.0, arm["cost_per_musd_volume"]


def test_a_cost_with_no_execution_coverage_stays_out_of_the_volume_rate():
    """A PnL day whose execution file was not passed has no volume of its own."""
    rows = build_rows(
        {("2026-09-08", "freq"): ExecDay(fills=1, volume_usd=1_000_000.0)},
        {("2026-09-07", "freq"): PnlDay(cycles=1, realized_pnl_usd=-500.0, funding_seen=True),
         ("2026-09-08", "freq"): PnlDay(cycles=1, realized_pnl_usd=-100.0, funding_seen=True)},
    )
    arm = summarize(rows)["arms"][0]
    assert arm["cost_usd"] == 600.0
    assert arm["cost_usd_without_volume"] == 500.0
    # $100 on the $1M actually measured, not $600.
    assert arm["cost_per_musd_volume"] == 100.0, arm["cost_per_musd_volume"]


def test_a_fill_without_a_value_makes_the_day_volume_a_lower_bound():
    with tempfile.TemporaryDirectory() as tmp:
        path = write(
            Path(tmp) / "execution-debot-pair-robinhood-lighter_20260908.jsonl",
            [
                {"event": "leg_fill", "ts_ms": TS * 1000, "variant": "freq",
                 "fill_value": 10_000.0, "filled_qty": 1.0},
                # Filled, but the value is missing: real volume, unmeasured.
                {"event": "leg_fill", "ts_ms": TS * 1000, "variant": "freq", "filled_qty": 2.0},
            ],
        )
        day = load_execution([path])[("2026-09-08", "freq")]
        assert day.volume_usd == 10_000.0
        assert day.fills_without_value == 1

        # That day's cost is therefore kept out of the per-volume rate,
        # rather than divided by a denominator known to be short.
        rows = build_rows({("2026-09-08", "freq"): day},
                          {("2026-09-08", "freq"): PnlDay(cycles=1, realized_pnl_usd=-50.0,
                                                          funding_seen=True)})
        arm = summarize(rows)["arms"][0]
        assert arm["cost_per_musd_volume"] is None
        assert arm["cost_usd_without_volume"] == 50.0
        assert arm["fills_without_value"] == 1


def test_a_hold_spanning_a_funding_interval_needs_funding_coverage():
    """Absent funding is a real zero on a short hold and a gap on a long one.

    In the archived ledgers all 101 exit_fill rows without the field were
    held under an hour, so requiring it on every close would strike out a
    fifth of real coverage for nothing.
    """
    with tempfile.TemporaryDirectory() as tmp:
        root_short = Path(tmp) / "short"
        root_short.mkdir()
        root_long = Path(tmp) / "long"
        root_long.mkdir()
        brief = pnl_file(root_short, [{"ts": TS, "source": "exit_fill", "pnl": -2.0,
                                       "hold_secs": 900}])
        assert not load_pnl([brief])[("2026-09-08", "freq")].incomplete

        held = pnl_file(root_long, [{"ts": TS, "source": "exit_fill", "pnl": -2.0,
                                     "hold_secs": 7200}])
        day = load_pnl([held])[("2026-09-08", "freq")]
        assert day.incomplete and day.incomplete_reasons == {"funding_gap"}


def render_for(rows):
    return render_table(rows, summarize(rows))


def test_the_daily_rate_is_suppressed_when_the_day_volume_is_a_lower_bound():
    """The rule has to hold on the row, not only in the totals.

    The row is what `--out` writes and what the daily table prints, so a
    rate excluded from the aggregate but still published per day is the
    same inflated number in the place an operator actually reads it.
    """
    incomplete = ExecDay(fills=1, volume_usd=10_000.0, fills_without_value=1)
    row = build_rows({("2026-09-08", "freq"): incomplete},
                     {("2026-09-08", "freq"): PnlDay(cycles=1, realized_pnl_usd=-50.0,
                                                     funding_seen=True)})[0]
    assert row.cost_usd == 50.0
    assert row.cost_per_musd_volume is None
    assert row.as_json()["fills_without_value"] == 1


def test_every_printed_equation_shows_the_numerator_it_divided():
    rows = build_rows(
        {("2026-09-08", "freq"): ExecDay(fills=1, volume_usd=1_000_000.0)},
        {("2026-09-07", "freq"): PnlDay(cycles=1, realized_pnl_usd=-500.0, funding_seen=True),
         ("2026-09-08", "freq"): PnlDay(cycles=1, realized_pnl_usd=-100.0, funding_seen=True)},
        points={("2026-09-08", "freq"): 1000.0},
    )
    text = render_for(rows)
    # The rate divides $100, so $100 is what the equation shows. Printing
    # "$600 ... = $100 per $1M" is a false equation that no caveat repairs.
    assert "$100.00 of it fell on $1,000,000 of measured volume = $100.00 per $1M traded" in text
    assert "$100.00 of it over 1,000.0 points = $0.100000 per point" in text
    # The full cost is still reported, on its own line.
    assert "cost $600.00 across 2 costed day(s)" in text


def test_a_known_cost_is_reported_even_when_no_rate_can_be_built():
    """Unknown rate is not unknown cost.

    Every costed day here lacks execution coverage, so no per-volume rate
    exists -- but the cost itself came from the PnL ledger and saying it is
    unknown would throw away the one number that is solid.
    """
    rows = build_rows(
        {},
        {("2026-09-08", "freq"): PnlDay(cycles=1, realized_pnl_usd=-250.0, funding_seen=True)},
    )
    text = render_for(rows)
    assert "cost $250.00 across 1 costed day(s)" in text
    assert "no costed day has fully measured volume" in text
    assert "no PnL ledger or equity series covers these days" not in text
    # And the diagnostic is not skipped past.
    assert "$250.00 of cost fell on days whose volume is unmeasured" in text


def test_an_arm_with_no_cost_source_at_all_still_says_so():
    rows = build_rows({("2026-09-08", "freq"): ExecDay(fills=1, volume_usd=5_000.0)}, {})
    text = render_for(rows)
    assert "cost unknown (no PnL ledger or equity series covers these days)" in text


def test_overlapping_globs_read_each_file_once():
    """A broad pattern beside a narrow one is a natural way to call this.

    Without deduplication the overlap is read twice and its volume, PnL and
    funding are counted twice, with nothing in the output to show it.
    """
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        write(
            root / "execution-debot-pair-robinhood-lighter_20260908.jsonl",
            [{"event": "leg_fill", "ts_ms": TS * 1000, "variant": "freq",
              "fill_value": 10_000.0, "filled_qty": 1.0}],
        )
        both = expand([str(root / "execution-*.jsonl"),
                       str(root / "execution-*_20260908.jsonl")])
        assert len(both) == 1, both
        assert load_execution(both)[("2026-09-08", "freq")].volume_usd == 10_000.0


def test_a_fill_that_cannot_be_attributed_is_an_error():
    """Same rule the PnL loader applies to a row with no `ts`.

    Dropping it leaves the day's volume understated while the day still
    presents as fully covered — the failure this KPI exists to prevent.
    """
    with tempfile.TemporaryDirectory() as tmp:
        path = write(
            Path(tmp) / "execution-debot-pair-robinhood-lighter_20260908.jsonl",
            [
                {"event": "leg_fill", "ts_ms": TS * 1000, "variant": "freq",
                 "fill_value": 10_000.0},
                {"event": "leg_fill", "variant": "freq", "fill_value": 5_000.0},
            ],
        )
        try:
            load_execution([path])
        except SubsidyLedgerError as error:
            assert "no ts_ms" in str(error), error
        else:
            raise AssertionError("an unattributable fill was silently dropped")


def test_a_malformed_points_row_is_an_error():
    with tempfile.TemporaryDirectory() as tmp:
        path = write(Path(tmp) / "points.jsonl",
                     [{"date": "2026-09-08", "arm": "freq", "points": 1000.0},
                      {"date": "2026-09-08", "points": 500.0}])
        try:
            load_points(path)
        except SubsidyLedgerError as error:
            assert "needs date, arm and points" in str(error), error
        else:
            raise AssertionError("a points row with no arm was silently dropped")


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
                {"ts": TS, "source": "exit_fill", "pnl": -3.0, "funding_carry_usd": -0.01,
                 "hold_secs": 4000, "funding_ticks_observed": 1},
                {"ts": TS, "source": "exit_fill", "pnl": 1.0, "funding_carry_usd": -0.02,
                 "hold_secs": 4000, "funding_ticks_observed": 1},
            ],
        )
        pnl = load_pnl(sorted(root.glob("pnl-*.jsonl")))
        day = pnl[("2026-09-08", "freq")]
        assert day.cycles == 2
        assert round(day.realized_pnl_usd, 6) == -2.0
        assert round(day.funding_usd, 6) == -0.03
        assert not day.incomplete

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
