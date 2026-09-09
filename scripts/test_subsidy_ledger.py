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
    render_table,
    funding_ticks_are_zero,
    funding_tick_claim,
    funding_ticks_seen,
    is_canonical_date,
    is_not_a_number,
    opening_date,
    pnl_row_defect,
    spans_a_funding_interval,
    expand,
    main as ledger_main,
    render_table,
    summarize,
    utc_date,
)

# 2026-09-08 12:30 UTC. Mid-hour on purpose: the short holds below are
# meant to be genuine no-funding rows, and a close on the hour would make
# every one of them span a funding boundary.
TS = 1788870600


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
        assert day.incomplete_reasons == {"pnl_available_not_true"}

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


def test_a_short_hold_that_crosses_a_funding_boundary_is_a_gap():
    """`hold_secs < 3600` is not evidence that no funding tick landed.

    Lighter funds on the hour, so a position opened at 11:55 and closed
    at 12:05 was charged the 12:00 tick while holding for 600 seconds.
    Reading the absent `funding_carry_usd` on that row as a real zero
    understates the day's cost and still marks it complete.
    """
    on_the_hour = 1788868800  # 2026-09-08 12:00:00 UTC
    with tempfile.TemporaryDirectory() as tmp:
        crossing = pnl_file(
            Path(tmp),
            [{"ts": on_the_hour + 300, "source": "exit_fill", "pnl": -5.0, "hold_secs": 600}],
        )
        day = load_pnl([crossing])[("2026-09-08", "freq")]
        assert day.incomplete and day.incomplete_reasons == {"funding_gap"}, day.incomplete_reasons

        # The same duration wholly inside one funding hour stays a real zero.
        inside = pnl_file(
            Path(tmp),
            [{"ts": on_the_hour + 1800, "source": "exit_fill", "pnl": -5.0, "hold_secs": 600}],
            arm="b",
        )
        clean = load_pnl([inside])[("2026-09-08", "b")]
        assert not clean.incomplete, clean.incomplete_reasons


def test_a_zero_valued_fill_that_moved_quantity_is_unvalued_volume():
    """$0 notional on a fill that moved size is a missing value, not $0 of volume.

    Counted as a valued fill it adds nothing to the denominator while
    leaving the day marked complete, so every per-volume rate computed
    from that day is divided by a number known to be short.
    """
    with tempfile.TemporaryDirectory() as tmp:
        path = write(
            Path(tmp) / "execution-debot-pair-robinhood-lighter_20260908.jsonl",
            [
                {"event": "leg_fill", "ts_ms": TS * 1000, "variant": "freq",
                 "fill_value": 10_000.0, "filled_qty": 1.0},
                {"event": "leg_fill", "ts_ms": TS * 1000, "variant": "freq",
                 "fill_value": 0.0, "filled_qty": 2.0},
            ],
        )
        day = load_execution([path])[("2026-09-08", "freq")]
        assert day.volume_usd == 10_000.0
        assert day.fills == 1
        assert day.fills_without_value == 1

        # A fill that moved nothing and is worth nothing is not a gap.
        none_moved = write(
            Path(tmp) / "execution-debot-pair-robinhood-lighter_20260907.jsonl",
            [{"event": "leg_fill", "ts_ms": TS * 1000, "variant": "b",
              "fill_value": 0.0, "filled_qty": 0.0}],
        )
        quiet = load_execution([none_moved])[("2026-09-08", "b")]
        assert quiet.fills_without_value == 0 and quiet.fills == 1


def test_a_malformed_equity_sample_invalidates_its_day_and_the_next():
    """A day whose closing sample cannot be read has no known close.

    Stepping over the bad row leaves the last readable sample standing
    as the day's close, so the day's cost -- and the next day's, which
    is measured from that same baseline -- is computed from a number
    that is not a close.
    """
    costs = equity_daily_costs([
        {"ts": 1788595200000, "equity": 5000.0},   # 2026-09-05
        {"ts": 1788681600000, "equity": 4900.0},   # 2026-09-06 morning
        {"ts": 1788703200000, "equity": "n/a"},    # 2026-09-06 close, unreadable
        {"ts": 1788768000000, "equity": 4950.0},   # 2026-09-07
        {"ts": 1788854400000, "equity": 4900.0},   # 2026-09-08
    ])
    assert "2026-09-06" not in costs           # its own close is unknown
    assert "2026-09-07" not in costs           # measured from that unknown close
    assert costs["2026-09-08"] == 50.0         # clean pair, still measured

    # A row that cannot be attributed to a day at all is a broken writer.
    try:
        equity_daily_costs([{"equity": 5000.0}])
    except SubsidyLedgerError as error:
        assert "no `ts`" in str(error), error
    else:
        raise AssertionError("an equity row with no ts should raise")


def test_an_equity_only_or_points_only_date_still_appears():
    """The four inputs are selected independently, so any one can stand alone.

    An equity delta for a date absent from both ledgers is a known cost,
    and points for such a date belong in `uncosted_points`. Unioning
    only the two ledgers dropped both without saying so.
    """
    rows = build_rows(
        {},
        {},
        equity_costs={"freq": {"2026-09-06": 31.0}},
        points={("2026-09-07", "freq"): 2000.0},
    )
    by_date = {row.date: row for row in rows}
    assert by_date["2026-09-06"].cost_usd == 31.0
    assert by_date["2026-09-06"].cost_source == "equity_delta"
    assert by_date["2026-09-07"].points == 2000.0
    assert by_date["2026-09-07"].cost_usd is None

    arm = summarize(rows)["arms"][0]
    assert arm["cost_usd"] == 31.0
    assert arm["uncosted_points"] == 2000.0
    assert arm["points"] == 0.0


def test_a_non_finite_notional_is_a_gap_not_a_value():
    """`float()` accepts "Infinity" and "NaN"; neither is a volume.

    An infinite denominator reports a real cost as $0.00 per $1M, and a
    NaN spreads into every total and leaves `--out` holding non-standard
    JSON -- while the day still presents as fully covered.
    """
    with tempfile.TemporaryDirectory() as tmp:
        path = write(
            Path(tmp) / "execution-debot-pair-robinhood-lighter_20260908.jsonl",
            [
                {"event": "leg_fill", "ts_ms": TS * 1000, "variant": "freq",
                 "fill_value": 10_000.0, "filled_qty": 1.0},
                {"event": "leg_fill", "ts_ms": TS * 1000, "variant": "freq",
                 "fill_value": "Infinity", "filled_qty": 2.0},
                {"event": "leg_fill", "ts_ms": TS * 1000, "variant": "freq",
                 "fill_value": "NaN", "filled_qty": 3.0},
            ],
        )
        day = load_execution([path])[("2026-09-08", "freq")]
        assert day.volume_usd == 10_000.0, day.volume_usd
        assert day.fills == 1
        assert day.fills_without_value == 2


def test_an_observed_funding_tick_beats_the_interval_test():
    """The row's own tick count is evidence; the timestamps are inference.

    A rounded or stale `hold_secs`/`ts` can make a row that met a tick
    look like it stayed inside one funding hour, and the missing carry
    would then be read as a real zero.
    """
    inside_the_hour = 1788868800 + 1800  # 12:30 UTC, 600s hold crosses nothing
    with tempfile.TemporaryDirectory() as tmp:
        path = pnl_file(
            Path(tmp),
            [{"ts": inside_the_hour, "source": "exit_fill", "pnl": -5.0,
              "hold_secs": 600, "funding_ticks_observed": 2}],
        )
        day = load_pnl([path])[("2026-09-08", "freq")]
        assert day.incomplete and day.incomplete_reasons == {"funding_gap"}, day.incomplete_reasons

        # Zero observed ticks on a hold that met no boundary is still a
        # real zero, not a gap.
        clean = pnl_file(
            Path(tmp),
            [{"ts": inside_the_hour, "source": "exit_fill", "pnl": -5.0,
              "hold_secs": 600, "funding_ticks_observed": 0}],
            arm="b",
        )
        assert not load_pnl([clean])[("2026-09-08", "b")].incomplete


def test_negative_points_are_refused_rather_than_mixed_into_the_denominator():
    """A typo in the hand-written points file must not move the rate.

    The numerator requires points > 0, so a negative row's cost is
    excluded while its points still shrank the denominator: $100/1000
    beside $100/-500 reported $0.20 per point, a number about neither day.
    """
    with tempfile.TemporaryDirectory() as tmp:
        path = write(Path(tmp) / "points.jsonl", [
            {"date": "2026-09-08", "arm": "freq", "points": 1000.0},
            {"date": "2026-09-07", "arm": "freq", "points": -500.0},
        ])
        try:
            load_points(path)
        except SubsidyLedgerError as error:
            assert "non-negative" in str(error), error
        else:
            raise AssertionError("a negative points row should raise")

        # Zero is legal: it means the day earned none.
        zero = write(Path(tmp) / "zero.jsonl",
                     [{"date": "2026-09-08", "arm": "freq", "points": 0.0}])
        assert load_points(zero) == {("2026-09-08", "freq"): 0.0}


def test_a_cycle_that_crossed_midnight_suppresses_the_day_rate():
    """The two ledgers file an overnight round trip on different days.

    Its entry notional is yesterday's volume (with no cost, so that day
    is uncosted) while the whole cost lands on the close day with only
    the exit side to divide by. With equal legs that doubles the rate.
    """
    close = 1788868800 + 3600  # 2026-09-08 13:00 UTC
    with tempfile.TemporaryDirectory() as tmp:
        overnight = pnl_file(
            Path(tmp),
            [{"ts": close, "source": "exit_fill", "pnl": -100.0,
              "hold_secs": 20 * 3600, "funding_carry_usd": 0.0}],
        )
        day = load_pnl([overnight])[("2026-09-08", "freq")]
        assert day.cross_day_cycles == 1
        assert not day.incomplete, "it is still a good, realized close"

        row = build_rows(
            {("2026-09-08", "freq"): ExecDay(fills=1, volume_usd=500_000.0)},
            {("2026-09-08", "freq"): day},
        )[0]
        assert row.cost_usd == 100.0
        assert row.cost_per_musd_volume is None, (
            "the denominator is short by the entry side, so no rate is published")
        arm = summarize([row])["arms"][0]
        assert arm["cross_day_cycles"] == 1
        assert arm["cost_per_musd_volume"] is None
        assert arm["cost_usd_without_volume"] == 100.0

        # Points are short by the same entry side, and cannot be
        # re-attributed either -- they are supplied per day.
        pointed = build_rows(
            {("2026-09-08", "freq"): ExecDay(fills=1, volume_usd=500_000.0)},
            {("2026-09-08", "freq"): day},
            None,
            {("2026-09-08", "freq"): 1000.0},
        )[0]
        assert pointed.cost_per_point is None
        pointed_arm = summarize([pointed])["arms"][0]
        assert pointed_arm["cost_per_point"] is None
        assert pointed_arm["uncosted_points"] == 1000.0
        assert pointed_arm["cost_usd_without_points"] == 100.0

        # A same-day cycle is unaffected.
        same_day = pnl_file(
            Path(tmp),
            [{"ts": close, "source": "exit_fill", "pnl": -100.0,
              "hold_secs": 600, "funding_carry_usd": 0.0}],
            arm="b",
        )
        clean = load_pnl([same_day])[("2026-09-08", "b")]
        assert clean.cross_day_cycles == 0
        clean_row = build_rows(
            {("2026-09-08", "b"): ExecDay(fills=1, volume_usd=1_000_000.0)},
            {("2026-09-08", "b"): clean},
        )[0]
        assert clean_row.cost_per_musd_volume == 100.0


def test_non_finite_pnl_funding_and_equity_are_not_costs():
    """`float()` accepts NaN and Infinity on every cost-bearing field."""
    with tempfile.TemporaryDirectory() as tmp:
        bad_pnl = pnl_file(
            Path(tmp),
            [{"ts": TS, "source": "exit_fill", "pnl": "NaN", "hold_secs": 600}],
        )
        day = load_pnl([bad_pnl])[("2026-09-08", "freq")]
        assert day.incomplete and "unreadable_pnl" in day.incomplete_reasons

        bad_funding = pnl_file(
            Path(tmp),
            [{"ts": TS, "source": "exit_fill", "pnl": -5.0, "hold_secs": 600,
              "funding_carry_usd": "Infinity"}],
            arm="b",
        )
        funding_day = load_pnl([bad_funding])[("2026-09-08", "b")]
        assert funding_day.incomplete
        assert "unreadable_funding" in funding_day.incomplete_reasons

    costs = equity_daily_costs([
        {"ts": 1788595200000, "equity": 5000.0},   # 2026-09-05
        {"ts": 1788681600000, "equity": "NaN"},    # 2026-09-06
        {"ts": 1788768000000, "equity": 4950.0},   # 2026-09-07
    ])
    assert costs == {}, costs


def test_a_non_finite_slippage_diagnostic_is_not_carried_into_the_output():
    """The column is a diagnostic, but a NaN still breaks `--out`."""
    with tempfile.TemporaryDirectory() as tmp:
        path = write(
            Path(tmp) / "execution-debot-pair-robinhood-lighter_20260908.jsonl",
            [
                {"event": "leg_fill", "ts_ms": TS * 1000, "variant": "freq",
                 "fill_value": 10_000.0, "filled_qty": 1.0,
                 "slippage_usd_vs_decision": 1.5},
                {"event": "leg_fill", "ts_ms": TS * 1000, "variant": "freq",
                 "fill_value": 10_000.0, "filled_qty": 1.0,
                 "slippage_usd_vs_decision": "NaN"},
            ],
        )
        day = load_execution([path])[("2026-09-08", "freq")]
        assert day.slippage_usd == 1.5
        assert day.slippage_unreadable == 1
        row = build_rows({("2026-09-08", "freq"): day}, {})[0]
        assert json.dumps(row.as_json()) == json.dumps(row.as_json())
        assert "NaN" not in json.dumps(row.as_json())


def test_a_non_finite_number_never_reads_as_absence_of_movement():
    """Every remaining `float()` comparison, swept in one pass.

    A NaN compares false against everything, so each of these read as
    "nothing happened": no quantity moved, no funding ticks, a hold
    inside the hour. All three leave a day looking complete on an input
    that is unknown.
    """
    # 1. A fill with no value and a NaN quantity is movement, not stillness.
    with tempfile.TemporaryDirectory() as tmp:
        path = write(
            Path(tmp) / "execution-debot-pair-robinhood-lighter_20260908.jsonl",
            [{"event": "leg_fill", "ts_ms": TS * 1000, "variant": "freq",
              "filled_qty": "NaN"}],
        )
        day = load_execution([path])[("2026-09-08", "freq")]
        assert day.fills_without_value == 1, "unknown movement is a gap, not a zero"

    # 2. A NaN tick count is not "no ticks observed".
    inside_the_hour = 1788868800 + 1800
    with tempfile.TemporaryDirectory() as tmp:
        path = pnl_file(
            Path(tmp),
            [{"ts": inside_the_hour, "source": "exit_fill", "pnl": -5.0,
              "hold_secs": 600, "funding_ticks_observed": "NaN"}],
        )
        pnl_day = load_pnl([path])[("2026-09-08", "freq")]
        assert pnl_day.incomplete
        assert pnl_day.incomplete_reasons == {"funding_gap"}

    # 3. An impossible value is not evidence either: a tick count cannot
    # be negative, and a hold that ends before it starts says nothing
    # about which day the cycle opened on.
    assert funding_ticks_seen({"funding_ticks_observed": -1})
    assert opening_date({"ts": inside_the_hour, "hold_secs": -600}) is None

    # 4. A NaN hold or close is not a hold inside one funding hour.
    # (This one already came out right by NaN propagation through the
    # boundary comparison; the explicit guard is there so the behaviour
    # does not depend on that, and this asserts the behaviour itself.)
    assert spans_a_funding_interval(
        {"hold_secs": "NaN", "ts": inside_the_hour})
    assert spans_a_funding_interval(
        {"hold_secs": 600, "ts": "NaN"})


def test_the_cross_day_marker_does_not_touch_an_equity_costed_day():
    """An equity delta is measured between two daily closes.

    It is therefore already aligned with this date's own volume and
    points, whatever the PnL ledger's cycles did -- suppressing its rates
    would withhold a number that is not mismatched at all.
    """
    close = 1788868800 + 3600  # 2026-09-08 13:00 UTC
    with tempfile.TemporaryDirectory() as tmp:
        # A cross-midnight cycle, *and* a placeholder that makes the PnL
        # coverage incomplete, so the day falls back to the equity delta.
        path = pnl_file(
            Path(tmp),
            [
                {"ts": close, "source": "exit_fill", "pnl": -100.0,
                 "hold_secs": 20 * 3600, "funding_carry_usd": 0.0},
                {"ts": close, "source": "recovery_no_pnl", "pnl": 0.0,
                 "pnl_available": False},
            ],
        )
        day = load_pnl([path])[("2026-09-08", "freq")]
        # 2: the overnight close, plus the placeholder whose own hold is
        # unreadable, which the fail-safe also counts as a crossing.
        assert day.cross_day_cycles == 2 and day.incomplete

        row = build_rows(
            {("2026-09-08", "freq"): ExecDay(fills=1, volume_usd=500_000.0)},
            {("2026-09-08", "freq"): day},
            {"freq": {"2026-09-08": 50.0}},
            {("2026-09-08", "freq"): 1000.0},
        )[0]
        assert row.cost_source == "equity_delta"
        assert row.cost_per_musd_volume == 100.0, row.cost_per_musd_volume
        assert row.cost_per_point == 0.05, row.cost_per_point
        arm = summarize([row])["arms"][0]
        assert arm["cost_per_musd_volume"] == 100.0
        assert arm["uncosted_points"] == 0.0


def test_a_repeated_points_row_is_refused():
    """Last-wins silently divided the day's cost by part of its points."""
    with tempfile.TemporaryDirectory() as tmp:
        path = write(Path(tmp) / "points.jsonl", [
            {"date": "2026-09-08", "arm": "freq", "points": 1000.0},
            {"date": "2026-09-08", "arm": "freq", "points": 500.0},
        ])
        try:
            load_points(path)
        except SubsidyLedgerError as error:
            assert "more than once" in str(error), error
        else:
            raise AssertionError("a repeated (date, arm) should raise")

        # Different arms on the same date are not duplicates.
        fine = write(Path(tmp) / "ok.jsonl", [
            {"date": "2026-09-08", "arm": "freq", "points": 1000.0},
            {"date": "2026-09-08", "arm": "b", "points": 500.0},
        ])
        assert len(load_points(fine)) == 2


def test_point_coverage_is_explained_even_when_no_rate_exists():
    """No rate is when the reader most needs to know why.

    Points on one day and cost on another is the ordinary cause, and the
    diagnostics that say so were suppressed along with the rate -- so the
    requested KPI was simply missing, unexplained.
    """
    rows = build_rows(
        {},
        {("2026-09-08", "freq"): PnlDay(cycles=1, realized_pnl_usd=-100.0, funding_seen=True)},
        None,
        {("2026-09-07", "freq"): 1000.0},
    )
    summary = summarize(rows)
    arm = summary["arms"][0]
    assert arm["cost_per_point"] is None
    assert arm["uncosted_points"] == 1000.0
    assert arm["cost_usd_without_points"] == 100.0

    rendered = render_table(rows, summary)
    assert "no price per point" in rendered, rendered
    assert "1,000.0 points earned on days with no usable cost" in rendered, rendered
    assert "$100.00 of cost fell on days with no points supplied" in rendered, rendered


def test_the_opening_day_of_an_overnight_cycle_is_marked_too():
    """The entry leg inflates the opening day's denominator.

    That day's execution volume holds the overnight entry while none of
    its cost does, so a *different* close on that same day is divided by
    a denominator that is too big -- the mirror of the close-day case.
    """
    close = 1788868800 + 3600  # 2026-09-08 13:00 UTC
    with tempfile.TemporaryDirectory() as tmp:
        path = pnl_file(
            Path(tmp),
            [
                # Opened 2026-09-07, closed 2026-09-08.
                {"ts": close, "source": "exit_fill", "pnl": -100.0,
                 "hold_secs": 20 * 3600, "funding_carry_usd": 0.0},
            ],
        )
        days = load_pnl([path])
        assert days[("2026-09-08", "freq")].cross_day_cycles == 1
        opening = days[("2026-09-07", "freq")]
        assert opening.cross_day_entries == 1
        assert opening.cycles == 0

        # A same-day close on the opening date is not costed against a
        # denominator that also holds the overnight entry.
        opening.cycles = 1
        opening.realized_pnl_usd = -100.0
        opening.funding_seen = True
        rows = build_rows(
            {("2026-09-07", "freq"): ExecDay(fills=2, volume_usd=1_500_000.0)},
            {("2026-09-07", "freq"): opening},
        )
        assert rows[0].cross_day_entries == 1
        assert rows[0].cost_usd == 100.0
        assert rows[0].cost_per_musd_volume is None, rows[0].cost_per_musd_volume


def test_an_entry_marker_alone_does_not_cost_a_day_at_zero():
    """The opening date may hold no realized close of its own.

    The marker creates that day in the PnL map; costing it from an empty
    `PnlDay` would report a free day rather than an uncosted one.
    """
    close = 1788868800 + 3600
    with tempfile.TemporaryDirectory() as tmp:
        path = pnl_file(
            Path(tmp),
            [{"ts": close, "source": "exit_fill", "pnl": -100.0,
              "hold_secs": 20 * 3600, "funding_carry_usd": 0.0}],
        )
        days = load_pnl([path])
        rows = build_rows(
            {("2026-09-07", "freq"): ExecDay(fills=1, volume_usd=500_000.0)},
            days,
        )
        opening = next(r for r in rows if r.date == "2026-09-07")
        assert opening.cost_usd is None, "a day with no realized close is uncosted, not free"
        assert opening.cost_source is None
        arm = summarize(rows)["arms"][0]
        assert arm["uncosted_volume_usd"] == 500_000.0


def test_the_opening_day_is_marked_even_when_the_close_cannot_be_costed():
    """The entry leg is in the execution ledger whatever the PnL says.

    An overnight close with an unreadable PnL still leaves its entry
    notional on the previous day, so that day's own rates are over a
    denominator that is too big -- and the marker used to be created
    only after every PnL rejection.
    """
    close = 1788868800 + 3600
    with tempfile.TemporaryDirectory() as tmp:
        path = pnl_file(
            Path(tmp),
            [{"ts": close, "source": "exit_fill", "pnl": "NaN",
              "hold_secs": 20 * 3600}],
        )
        days = load_pnl([path])
        assert days[("2026-09-08", "freq")].incomplete
        assert days[("2026-09-07", "freq")].cross_day_entries == 1

    # A simulated close moved no real quantity, so it marks nothing.
    with tempfile.TemporaryDirectory() as tmp:
        path = pnl_file(
            Path(tmp),
            [{"ts": close, "source": "exit_dry_run", "pnl": -5.0,
              "hold_secs": 20 * 3600}],
            arm="b",
        )
        days = load_pnl([path])
        assert ("2026-09-07", "b") not in days


def test_an_unreadable_opening_date_suppresses_the_rates():
    """`None` from `opening_date` is not "it did not cross"."""
    close = 1788868800 + 1800
    with tempfile.TemporaryDirectory() as tmp:
        path = pnl_file(
            Path(tmp),
            [{"ts": close, "source": "exit_fill", "pnl": -100.0,
              "hold_secs": -600, "funding_carry_usd": 0.0}],
        )
        day = load_pnl([path])[("2026-09-08", "freq")]
        assert day.cross_day_cycles == 1, "an unknowable crossing is treated as one"
        row = build_rows(
            {("2026-09-08", "freq"): ExecDay(fills=1, volume_usd=500_000.0)},
            {("2026-09-08", "freq"): day},
        )[0]
        assert row.cost_usd == 100.0
        assert row.cost_per_musd_volume is None


def test_a_row_without_a_timestamp_is_always_fatal():
    """Even with no `pnl`: the days it might belong to cannot be cleared."""
    with tempfile.TemporaryDirectory() as tmp:
        path = pnl_file(
            Path(tmp),
            [
                {"ts": TS, "source": "exit_fill", "pnl": -5.0, "hold_secs": 600},
                {"source": "recovery_no_pnl", "pnl_available": False},
            ],
        )
        try:
            load_pnl([path])
        except SubsidyLedgerError as error:
            assert "no `ts`" in str(error), error
        else:
            raise AssertionError("a row with no ts must raise even without a pnl")


def test_a_complete_day_with_no_funding_ticks_reports_a_known_zero():
    """`null` must mean "not known", not "known to be nothing".

    A day whose closes all stayed inside one funding interval carries no
    `funding_carry_usd` at all -- the documented shape -- and its cost is
    computed with that zero, so reporting the field as unknown left a
    consumer unable to tell it from a real coverage gap.
    """
    inside_the_hour = 1788868800 + 1800
    with tempfile.TemporaryDirectory() as tmp:
        clean = pnl_file(
            Path(tmp),
            [{"ts": inside_the_hour, "source": "exit_fill", "pnl": -5.0,
              "hold_secs": 600}],
        )
        day = load_pnl([clean])[("2026-09-08", "freq")]
        assert not day.incomplete and not day.funding_seen
        row = build_rows({}, {("2026-09-08", "freq"): day})[0]
        assert row.funding_usd == 0.0, row.funding_usd
        assert row.cost_usd == 5.0

        # A day whose funding coverage really is unknown still says so.
        gap = pnl_file(
            Path(tmp),
            [{"ts": inside_the_hour, "source": "exit_fill", "pnl": -5.0,
              "hold_secs": 20 * 3600}],
            arm="b",
        )
        gapped = load_pnl([gap])[("2026-09-08", "b")]
        assert gapped.incomplete
        gap_row = build_rows({}, {("2026-09-08", "b"): gapped})[0]
        assert gap_row.funding_usd is None


def test_a_partially_covered_day_does_not_publish_its_realized_pnl_either():
    """Same contract as `funding_usd`, same reason.

    `load_pnl` keeps the good closes' subtotal while marking the day
    incomplete, so serializing it unconditionally published a partial sum
    in the field a consumer reads as the day's realized PnL -- next to a
    `cost_usd` that was already being withheld (Codex, PR #297).
    """
    inside_the_hour = 1788868800 + 1800
    with tempfile.TemporaryDirectory() as tmp:
        mixed = pnl_file(
            Path(tmp),
            [
                {"ts": inside_the_hour, "source": "exit_fill", "pnl": -5.0,
                 "hold_secs": 600},
                # Unreadable PnL: the day is incomplete, and this row's
                # loss never reached the subtotal.
                {"ts": inside_the_hour + 60, "source": "exit_fill", "pnl": "n/a",
                 "hold_secs": 600},
            ],
        )
        day = load_pnl([mixed])[("2026-09-08", "freq")]
        assert day.incomplete and "unreadable_pnl" in day.incomplete_reasons
        assert round(day.realized_pnl_usd, 6) == -5.0, "the subtotal is still carried"
        row = build_rows({}, {("2026-09-08", "freq"): day})[0]
        assert row.realized_pnl_usd is None, row.realized_pnl_usd
        assert row.cost_source != "pnl_ledger"


def test_exec_globs_that_match_nothing_are_refused():
    """`--exec-glob` is required, so matching nothing is a mistake.

    Left alone the command exited 0 and wrote an empty `--out`, which a
    consumer cannot tell from a genuinely empty period (Codex, PR #297).
    """
    with tempfile.TemporaryDirectory() as tmp:
        missing = str(Path(tmp) / "no-such-export-*.jsonl")
        try:
            ledger_main(["--exec-glob", missing])
        except SystemExit as exit_code:
            assert exit_code.code == 2, exit_code.code
        else:
            raise AssertionError("a required glob that matches nothing must be refused")


def test_a_string_zero_tick_count_is_still_a_gap():
    """`"0"` is not `0`, and an exact comparison let it through.

    An export that writes its numbers as strings then had a row spanning
    an hourly boundary accepted as a complete day, with its zero carry in
    the cost -- while the identical row written numerically produced a
    `funding_gap` (Codex, PR #297).
    """
    on_the_hour = 1788868800 + 1800
    assert funding_ticks_are_zero({"funding_ticks_observed": 0})
    assert funding_ticks_are_zero({"funding_ticks_observed": "0"})
    assert funding_ticks_are_zero({"funding_ticks_observed": 0.0})
    # No claim, or a claim that cannot be read, is not a claim of zero.
    assert not funding_ticks_are_zero({})
    assert not funding_ticks_are_zero({"funding_ticks_observed": "n/a"})
    assert not funding_ticks_are_zero({"funding_ticks_observed": 2})
    assert not funding_ticks_are_zero({"funding_ticks_observed": "NaN"})
    # A boolean is not a count, and the two helpers must agree about it:
    # `float(False)` is `0.0`, so a `false` that read as "no ticks" here
    # while the zero test refused it slipped through both checks and left
    # a carry-less, boundary-spanning day complete (Codex, PR #297).
    assert not funding_ticks_are_zero({"funding_ticks_observed": False})
    assert funding_ticks_seen({"funding_ticks_observed": False})
    assert funding_ticks_seen({"funding_ticks_observed": True})
    with tempfile.TemporaryDirectory() as tmp2:
        boolean = pnl_file(
            Path(tmp2),
            [{"ts": 1788868800 + 1800, "source": "exit_fill", "pnl": -5.0,
              "hold_secs": 600, "funding_ticks_observed": False}],
        )
        malformed = load_pnl([boolean])[("2026-09-08", "freq")]
        assert malformed.incomplete, "a boolean tick count is not a known zero"
        assert "funding_gap" in malformed.incomplete_reasons

    with tempfile.TemporaryDirectory() as tmp:
        as_text = pnl_file(
            Path(tmp),
            [{"ts": on_the_hour, "source": "exit_fill", "pnl": -5.0,
              "hold_secs": 20 * 3600, "funding_carry_usd": 0.0,
              "funding_ticks_observed": "0"}],
        )
        day = load_pnl([as_text])[("2026-09-08", "freq")]
        assert day.incomplete, "a string zero must be read as a real zero claim"
        assert "funding_gap" in day.incomplete_reasons
        row = build_rows({}, {("2026-09-08", "freq"): day})[0]
        assert row.funding_usd is None
        assert row.cost_source != "pnl_ledger"


def test_a_boolean_is_never_a_number_anywhere_in_the_ledger():
    """`float(False)` is `0.0`, so a boolean parsed as a verified zero.

    Applied at every money- or count-bearing field rather than only the
    one reported: a malformed row must become a gap, never evidence
    (Codex, PR #297).
    """
    assert is_not_a_number(False) and is_not_a_number(True)
    assert is_not_a_number(None) and is_not_a_number("n/a")
    assert not is_not_a_number(0) and not is_not_a_number("0") and not is_not_a_number(-1.5)

    inside_the_hour = 1788868800 + 1800
    with tempfile.TemporaryDirectory() as tmp:
        # A boolean realized PnL is a gap, not a zero-cost cycle.
        bad_pnl = pnl_file(
            Path(tmp),
            [{"ts": inside_the_hour, "source": "exit_fill", "pnl": False,
              "hold_secs": 600}],
        )
        day = load_pnl([bad_pnl])[("2026-09-08", "freq")]
        assert day.incomplete and "unreadable_pnl" in day.incomplete_reasons
        assert day.cycles == 0, "a malformed row is not a counted cycle"

        # And so is a boolean funding carry.
        bad_carry = pnl_file(
            Path(tmp),
            [{"ts": inside_the_hour, "source": "exit_fill", "pnl": -5.0,
              "hold_secs": 600, "funding_carry_usd": False}],
            arm="b",
        )
        carry_day = load_pnl([bad_carry])[("2026-09-08", "b")]
        assert carry_day.incomplete
        assert "unreadable_funding" in carry_day.incomplete_reasons
        assert round(carry_day.funding_usd, 6) == 0.0, "nothing was booked from it"


def test_a_malformed_tick_count_is_a_gap_even_when_the_carry_is_present():
    """A supplied carry bypasses the `funding_ticks_seen` fail-safe.

    So a row spanning a boundary with `funding_carry_usd: 0` and an
    impossible count stayed complete, while the same row with a readable
    `0` correctly produced a gap (Codex, PR #297).
    """
    assert funding_tick_claim({}) == "absent"
    assert funding_tick_claim({"funding_ticks_observed": 0}) == "none"
    assert funding_tick_claim({"funding_ticks_observed": "0"}) == "none"
    assert funding_tick_claim({"funding_ticks_observed": 3}) == "some"
    # Every value a count cannot be: unparseable, boolean, non-finite,
    # negative, or fractional. A tick count is how many hourly charges
    # landed, so 0.5 is no more a count than -1 is (Codex, PR #297).
    for bad in (-1, -0.5, 0.5, 1.5, "0.5", float("nan"), float("inf"),
                float("-inf"), False, True, "n/a", ""):
        claim = funding_tick_claim({"funding_ticks_observed": bad})
        assert claim == "malformed", (bad, claim)
    # Integral values written as floats or strings are still counts.
    for good, expected in ((0.0, "none"), ("0.0", "none"), (2.0, "some"), ("2", "some")):
        assert funding_tick_claim({"funding_ticks_observed": good}) == expected, good

    on_the_hour = 1788868800 + 1800
    with tempfile.TemporaryDirectory() as tmp:
        for arm, ticks in (("freq", -1), ("b", "NaN"), ("c", False), ("e", 0.5)):
            path = pnl_file(
                Path(tmp),
                [{"ts": on_the_hour, "source": "exit_fill", "pnl": -5.0,
                  "hold_secs": 20 * 3600, "funding_carry_usd": 0.0,
                  "funding_ticks_observed": ticks}],
                arm=arm,
            )
            day = load_pnl([path])[("2026-09-08", arm)]
            assert day.incomplete, f"{ticks!r} is not a countable claim"
            assert "unreadable_funding" in day.incomplete_reasons, day.incomplete_reasons
            row = build_rows({}, {("2026-09-08", arm): day})[0]
            assert row.funding_usd is None
            assert row.cost_source != "pnl_ledger"

        # A readable positive count with a carry is the normal case and
        # stays complete.
        good = pnl_file(
            Path(tmp),
            [{"ts": on_the_hour, "source": "exit_fill", "pnl": -5.0,
              "hold_secs": 20 * 3600, "funding_carry_usd": -0.03,
              "funding_ticks_observed": 20}],
            arm="d",
        )
        fine = load_pnl([good])[("2026-09-08", "d")]
        assert not fine.incomplete, fine.incomplete_reasons


def test_an_earlier_bad_equity_sample_is_settled_by_a_later_good_close():
    """Asymmetry with the tie handling, and only in the wrong direction.

    A missing-equity sample at 10:00 followed by a valid 23:00 close
    leaves the day's close perfectly well known, but the day was
    invalidated permanently -- discarding that day's delta *and* the
    next day's (Codex, PR #297).
    """
    day_one = 1788825600_000            # 2026-09-08 00:00 UTC, ms
    day_two = day_one + 86_400_000
    good_one = {"ts": day_one + 82_800_000, "equity": 1000.0}   # 23:00
    good_two = {"ts": day_two + 82_800_000, "equity": 900.0}
    for bad in ({"ts": day_one + 36_000_000},                    # 10:00, no equity
                {"ts": day_one + 36_000_000, "equity": "n/a"},
                {"ts": day_one + 36_000_000, "equity": float("nan")}):
        costs = equity_daily_costs([bad, good_one, good_two])
        assert costs.get("2026-09-09") == 100.0, (bad, costs)

    # A bad sample *after* the day's best close still spoils it: the
    # close is then not known to be the close.
    late_bad = {"ts": day_one + 84_000_000, "equity": None}
    spoiled = equity_daily_costs([good_one, late_bad, good_two])
    assert "2026-09-09" not in spoiled, spoiled

    # And a day with no readable sample at all has nothing to settle it.
    only_bad = equity_daily_costs([{"ts": day_one + 100, "equity": None}, good_two])
    assert "2026-09-09" not in only_bad, only_bad


def test_a_points_date_must_be_canonical():
    """The date is a join key against dates produced by `utc_date`.

    A truthy but noncanonical spelling keyed a separate points-only row,
    so the day the operator meant to price reported no points and its
    rate could not be produced -- with a 0 exit (Codex, PR #297).
    """
    with tempfile.TemporaryDirectory() as tmp:
        # Wrong shape, and -- the round-31 addition -- right shape but
        # not a day. `2026-02-31` matches \d{4}-\d{2}-\d{2} and can never
        # be produced by `utc_date`, so it would have keyed a separate
        # points-only row (Codex, PR #297).
        for bad in ("2026-9-08", "2026-09-08 ", " 2026-09-08", "20260908", "2026-09-8",
                    "2026-02-31", "2026-13-01", "2026-00-10", "2026-09-00",
                    "2025-02-29", "not-a-date", 20260908, None):
            path = write(Path(tmp) / "points.jsonl",
                         [{"date": bad, "arm": "freq", "points": 1000}])
            try:
                load_points(path)
            except SubsidyLedgerError as error:
                # `None` is caught a line earlier by the missing-field
                # guard; everything else by the date check itself.
                assert "YYYY-MM-DD" in str(error) or "needs date" in str(error), error
            else:
                raise AssertionError(f"points date {bad!r} must be refused")
        canonical = write(Path(tmp) / "points.jsonl",
                          [{"date": "2026-09-08", "arm": "freq", "points": 1000}])
        assert load_points(canonical) == {("2026-09-08", "freq"): 1000.0}
        # A real leap day is a real date and must not be collateral.
        leap = write(Path(tmp) / "points.jsonl",
                     [{"date": "2024-02-29", "arm": "freq", "points": 5}])
        assert load_points(leap) == {("2024-02-29", "freq"): 5.0}
        # And every date `utc_date` actually produces round-trips.
        for ts in (0, 1788825600, 1788825600 + 86_399, 2_000_000_000):
            assert is_canonical_date(utc_date(ts)), ts


def test_an_availability_flag_that_is_not_true_does_not_establish_availability():
    """`is False` caught only the shape a correct writer emits.

    The field claims the PnL is real, so anything present that is not
    exactly `true` fails to establish it. Absent is different and stays
    fine -- most rows do not carry it (Codex, PR #297).
    """
    base = {"ts": TS, "source": "exit_fill", "pnl": -5.0, "hold_secs": 600}
    assert pnl_row_defect(base) is None, "absent stays fine"
    assert pnl_row_defect({**base, "pnl_available": True}) is None
    for bad in (False, 0, 1, None, "false", "true", "", []):
        assert pnl_row_defect({**base, "pnl_available": bad}) == "pnl_available_not_true", bad

    with tempfile.TemporaryDirectory() as tmp:
        path = pnl_file(Path(tmp), [{**base, "pnl_available": 0}])
        day = load_pnl([path])[("2026-09-08", "freq")]
        assert day.incomplete and day.cycles == 0
        assert day.incomplete_reasons == {"pnl_available_not_true"}
        row = build_rows({}, {("2026-09-08", "freq"): day})[0]
        assert row.cost_source != "pnl_ledger"


def test_two_equity_closes_at_the_same_instant_settle_nothing():
    """`>=` made export order decide the day's close.

    Two samples sharing the latest `ts` with different equity meant that
    merely reversing an otherwise equivalent export changed the next
    day's `equity_delta` cost (Codex, PR #297).
    """
    def history(rows):
        return equity_daily_costs(rows)

    day_one = 1788825600_000          # 2026-09-08 00:00 UTC, ms
    day_two = day_one + 86_400_000
    tie_a = {"ts": day_one + 100, "equity": 1000.0}
    tie_b = {"ts": day_one + 100, "equity": 1200.0}
    close_two = {"ts": day_two + 100, "equity": 900.0}

    forward = history([tie_a, tie_b, close_two])
    reverse = history([tie_b, tie_a, close_two])
    assert forward == reverse, "the answer must not depend on export order"
    assert "2026-09-09" not in forward, "a day measured from an ambiguous close is not a cost"

    # A strictly later sample settles the tie, so the day is usable again.
    settled = history([tie_a, tie_b, {"ts": day_one + 200, "equity": 1100.0}, close_two])
    assert settled.get("2026-09-09") == 200.0, settled

    # And an exactly repeated sample is not a conflict.
    repeated = history([tie_a, dict(tie_a), close_two])
    assert repeated.get("2026-09-09") == 100.0, repeated


def test_an_epoch_too_large_to_render_is_a_gap_not_a_traceback():
    """Finite is not the same as renderable.

    A normal close with `hold_secs: 1e300` derives an opening epoch
    `datetime.fromtimestamp` refuses with `OverflowError`/`OSError`, and
    `opening_date`'s call sits outside the loaders' handlers -- so it
    came out of the CLI as a traceback instead of the documented
    fail-safe (Codex, PR #297).
    """
    huge = {"ts": 1788868800 + 1800, "hold_secs": 1e300}
    assert opening_date(huge) is None, "unknowable, not a crash"
    # The fail-safe is the same one an unreadable hold takes: treated as a
    # crossing, so the day's rates are suppressed rather than published.
    with tempfile.TemporaryDirectory() as tmp:
        path = pnl_file(
            Path(tmp),
            [{"ts": 1788868800 + 1800, "source": "exit_fill", "pnl": -5.0,
              "hold_secs": 1e300}],
        )
        day = load_pnl([path])[("2026-09-08", "freq")]
        assert day.cross_day_cycles == 1, "an unknowable opening date counts as a crossing"

    # And a timestamp the loaders cannot render is refused rather than
    # raising something a caller has to guess at.
    with tempfile.TemporaryDirectory() as tmp:
        bad = pnl_file(
            Path(tmp),
            [{"ts": 1e300, "source": "exit_fill", "pnl": -5.0, "hold_secs": 600}],
        )
        try:
            load_pnl([bad])
        except SubsidyLedgerError as error:
            assert "unreadable `ts`" in str(error), error
        else:
            raise AssertionError("an unrenderable ts must be refused")


def test_a_boolean_timestamp_or_hold_never_reads_as_a_real_value():
    """The remaining `float()` sites: timestamps and hold durations.

    `float(False)` is `0.0`, so a boolean `ts` attributed a row to
    1970-01-01 and published a complete realized cost there, and a
    boolean `hold_secs` read as a 0s/1s hold -- same-day, inside one
    funding interval -- turning a missing carry into a verified zero
    (Codex, PR #297).
    """
    with tempfile.TemporaryDirectory() as tmp:
        # A boolean PnL timestamp is fatal, like any unreadable `ts`.
        bad_ts = pnl_file(
            Path(tmp),
            [{"ts": False, "source": "exit_fill", "pnl": -5.0, "hold_secs": 600}],
        )
        try:
            load_pnl([bad_ts])
        except SubsidyLedgerError as error:
            assert "unreadable `ts`" in str(error), error
        else:
            raise AssertionError("a boolean ts must not attribute the row to 1970-01-01")

        # And so is a boolean execution timestamp.
        bad_ts_ms = write(
            Path(tmp) / "execution-freq.jsonl",
            [{"event": "leg_fill", "ts_ms": True, "variant": "freq", "fill_value": 10_000.0}],
        )
        try:
            load_execution([bad_ts_ms])
        except SubsidyLedgerError as error:
            assert "unreadable `ts_ms`" in str(error), error
        else:
            raise AssertionError("a boolean ts_ms must not publish volume under 1970-01-01")

    # A boolean hold takes the fail-safe in both hold-reading helpers.
    boolean_hold = {"ts": 1788868800 + 1800, "hold_secs": False}
    assert opening_date(boolean_hold) is None, "the opening date is unknowable, not same-day"
    assert spans_a_funding_interval(boolean_hold), "and it is not a proven no-tick hold"
    assert opening_date({"ts": True, "hold_secs": 600}) is None
    # A real short hold still reads as one, so the fail-safe has not
    # swallowed the normal case.
    real = {"ts": 1788868800 + 1800, "hold_secs": 600}
    assert opening_date(real) == "2026-09-08"
    assert not spans_a_funding_interval(real)


def test_an_equity_spec_needs_both_an_arm_and_a_path():
    """`--equity =PATH` used to cost a blank arm and exit 0.

    The arm the operator meant to cost stayed uncosted while its equity
    series was emitted under `""` (Codex, PR #297).
    """
    with tempfile.TemporaryDirectory() as tmp:
        execution = Path(tmp) / "execution-freq.jsonl"
        execution.write_text("")
        equity = write(Path(tmp) / "equity_history.jsonl",
                       [{"ts": 1788868800_000, "equity": 1000.0}])
        for bad in (f"={equity}", "freq=", "freq", "", "="):
            try:
                ledger_main(["--exec-glob", str(execution), "--equity", bad])
            except SystemExit as exit_code:
                assert exit_code.code == 2, (bad, exit_code.code)
            else:
                raise AssertionError(f"--equity {bad!r} must be refused")
        # The documented form still works.
        assert ledger_main(["--exec-glob", str(execution),
                            "--equity", f"freq={equity}"]) == 0


def test_a_supplied_pnl_glob_that_matches_nothing_is_refused():
    """Omitting `--pnl-glob` is supported; mistyping it is not.

    A pattern that matches nothing silently moved the arm's costs to
    `equity_delta` or to uncovered days, and still exited 0
    (Codex, PR #297).
    """
    with tempfile.TemporaryDirectory() as tmp:
        execution = Path(tmp) / "execution-freq.jsonl"
        execution.write_text("")
        missing = str(Path(tmp) / "no-such-pnl-*.jsonl")
        try:
            ledger_main(["--exec-glob", str(execution), "--pnl-glob", missing])
        except SystemExit as exit_code:
            assert exit_code.code == 2, exit_code.code
        else:
            raise AssertionError("a supplied pnl pattern matching nothing must be refused")
        # Omitting it entirely is still fine.
        assert ledger_main(["--exec-glob", str(execution)]) == 0


def test_one_mistyped_exec_glob_among_several_is_refused():
    """The option repeats, so a collective check is not enough.

    One good pattern beside a mistyped one keeps the expansion non-empty
    while a whole arm or date drops out of the execution denominator
    (Codex, PR #297).
    """
    with tempfile.TemporaryDirectory() as tmp:
        good = Path(tmp) / "execution-freq.jsonl"
        good.write_text("")
        missing = str(Path(tmp) / "no-such-export-*.jsonl")
        try:
            ledger_main(["--exec-glob", str(good), "--exec-glob", missing])
        except SystemExit as exit_code:
            assert exit_code.code == 2, exit_code.code
        else:
            raise AssertionError("the unmatched pattern must still be refused")


def test_a_partially_covered_day_does_not_publish_its_known_funding_subtotal():
    """One good carry does not make the day's funding total known.

    `funding_seen` goes true on the first readable carry, so a day with
    one good close and one whose carry is missing used to serialize the
    subtotal into `funding_usd` -- a number a consumer reads as the day's
    total, next to the contract that `null` means unknown (Codex, PR #297).
    """
    inside_the_hour = 1788868800 + 1800
    with tempfile.TemporaryDirectory() as tmp:
        mixed = pnl_file(
            Path(tmp),
            [
                # Readable carry: funding_seen goes true here.
                {"ts": inside_the_hour, "source": "exit_fill", "pnl": -5.0,
                 "hold_secs": 600, "funding_carry_usd": -0.03},
                # Held across a funding interval with no carry at all.
                {"ts": inside_the_hour + 60, "source": "exit_fill", "pnl": -4.0,
                 "hold_secs": 20 * 3600},
            ],
        )
        day = load_pnl([mixed])[("2026-09-08", "freq")]
        assert day.funding_seen and day.incomplete
        assert "funding_gap" in day.incomplete_reasons
        row = build_rows({}, {("2026-09-08", "freq"): day})[0]
        assert row.funding_usd is None, row.funding_usd
        # And the day is not costed from the ledger either, so the two
        # agree about what is unknown.
        assert row.cost_source != "pnl_ledger"


def test_a_pnl_file_whose_arm_cannot_be_read_is_refused():
    """Skipping it dropped every realized cost in it and still exited 0."""
    with tempfile.TemporaryDirectory() as tmp:
        path = write(Path(tmp) / "pnl-copy.jsonl",
                     [{"ts": TS, "source": "exit_fill", "pnl": -5.0, "hold_secs": 600}])
        try:
            load_pnl([path])
        except SubsidyLedgerError as error:
            assert "arm cannot be read" in str(error), error
        else:
            raise AssertionError("an unreadable arm must raise, not skip the file")


def test_a_marker_only_day_reports_unknown_coverage():
    """It has no PnL rows, so "complete" would claim coverage it never had."""
    close = 1788868800 + 3600
    with tempfile.TemporaryDirectory() as tmp:
        path = pnl_file(
            Path(tmp),
            [{"ts": close, "source": "exit_fill", "pnl": -100.0,
              "hold_secs": 20 * 3600, "funding_carry_usd": 0.0}],
        )
        rows = build_rows(
            {("2026-09-07", "freq"): ExecDay(fills=1, volume_usd=500_000.0)},
            load_pnl([path]),
        )
        opening = next(r for r in rows if r.date == "2026-09-07")
        assert opening.cross_day_entries == 1
        assert opening.cost_usd is None
        assert opening.pnl_coverage is None, opening.pnl_coverage
        closing = next(r for r in rows if r.date == "2026-09-08")
        assert closing.pnl_coverage == "complete"


def test_an_all_uncosted_arm_still_explains_its_points():
    """"cost unknown" alone said nothing about the points supplied."""
    rows = build_rows(
        {("2026-09-08", "freq"): ExecDay(fills=1, volume_usd=500_000.0)},
        {},
        None,
        {("2026-09-08", "freq"): 1000.0},
    )
    summary = summarize(rows)
    assert summary["arms"][0]["cost_days"] == 0
    rendered = render_table(rows, summary)
    assert "cost unknown" in rendered
    assert "1,000.0 points were supplied" in rendered, rendered


def test_the_equity_close_is_the_latest_sample_not_the_last_line():
    """A newest-first export must not leave an intraday sample as the close."""
    newest_first = equity_daily_costs([
        {"ts": 1788652800000, "equity": 4900.0},   # 2026-09-06 00:00
        {"ts": 1788638400000, "equity": 5000.0},   # 2026-09-05 20:00, the close
        {"ts": 1788609600000, "equity": 4990.0},   # 2026-09-05 12:00, intraday
    ])
    # 09-06 measured against 09-05's real close (5000), not the 4990
    # sample that happened to be written last.
    assert newest_first["2026-09-06"] == 100.0, newest_first


def main() -> int:
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for test in tests:
        test()
        print(f"ok  {test.__name__}")
    print(f"\n{len(tests)} passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
