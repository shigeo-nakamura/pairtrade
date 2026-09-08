#!/usr/bin/env python3
"""Daily subsidy KPI ledger for the Robinhood Chain Lighter arms.

bot-strategy#938. These bots are subsidy-capture: they pay fees, slippage
and adverse selection to earn points, so the number that matters is the
price per unit of subsidy, not the PnL (`docs/return-source-taxonomy.md`
§4.2). This aggregates the two ledgers the bot already writes into one
row per (UTC date, arm).

What the row means
------------------
`cost_usd` is money given up, so it is **positive when the arm lost** and
negative when it came out ahead. It is `-(realized_pnl + funding)`.

Fees and slippage are deliberately **not** subtracted on top of that.
Realized PnL is computed from actual fill prices and is already net of
both; subtracting them again would double-count the same dollars. The
slippage column is carried as a diagnostic — it says where the cost came
from — and is never added into `cost_usd`.

The denominator
---------------
The points a program awards are not readable from any endpoint reachable
here (`api.rh.lighter.xyz` answers 403 on the points routes), so points
are an operator-supplied input: export them and pass `--points`. Rows
without them keep `points: null` and no cost-per-point.

`cost_per_musd_volume` needs no points at all and is the KPI to steer by
in the meantime: points programs are volume-weighted, so the conversion
from "cost per $1M traded" to "cost per point" is one multiplier applied
at the end, and every lever that lowers one lowers the other.

Coverage is never assumed
-------------------------
A day with volume but no cost source reports `cost_usd: null` and is left
out of the totals rather than counted as free. The execution ledger and
the PnL ledger do not cover the same days (on the live host: execution
from 2026-08-14, PnL only from 2026-09-01), and treating the difference
as zero cost would make the arms' worst stretch look like their cheapest.
`--equity` fills that gap from an equity_history series, marked as such,
since an equity delta also carries mark-to-market that a realized-cycle
ledger does not.
"""

from __future__ import annotations

import argparse
import glob
import json
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable


def utc_date(ts_seconds: float) -> str:
    return datetime.fromtimestamp(ts_seconds, timezone.utc).strftime("%Y-%m-%d")


@dataclass
class ExecDay:
    fills: int = 0
    volume_usd: float = 0.0
    slippage_usd: float = 0.0


@dataclass
class PnlDay:
    cycles: int = 0
    realized_pnl_usd: float = 0.0
    funding_usd: float = 0.0
    funding_seen: bool = False


@dataclass
class Row:
    date: str
    arm: str
    fills: int = 0
    volume_usd: float = 0.0
    slippage_usd: float | None = None
    cycles: int | None = None
    realized_pnl_usd: float | None = None
    funding_usd: float | None = None
    cost_usd: float | None = None
    cost_source: str | None = None
    points: float | None = None
    cost_per_point: float | None = None
    cost_per_musd_volume: float | None = None

    def as_json(self) -> dict:
        return {k: v for k, v in self.__dict__.items()}


def read_jsonl(path: Path) -> Iterable[dict]:
    """Tolerate a partially written trailing line: these files are appended
    to by a running bot, so the last row can be torn at any moment."""
    with path.open(encoding="utf-8", errors="replace") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue


def load_execution(paths: Iterable[Path]) -> dict[tuple[str, str], ExecDay]:
    """Volume and execution slippage per (date, arm) from `leg_fill` rows.

    Notional is taken per filled leg. A pair trade's two legs are both real
    volume to a points program, so they are not halved the way a position
    count would be.
    """
    days: dict[tuple[str, str], ExecDay] = defaultdict(ExecDay)
    for path in paths:
        for record in read_jsonl(path):
            if record.get("event") != "leg_fill":
                continue
            ts_ms = record.get("ts_ms")
            arm = record.get("variant")
            if ts_ms is None or not arm:
                continue
            day = days[(utc_date(ts_ms / 1000.0), str(arm))]
            notional = record.get("notional_usd")
            if notional is None:
                notional = record.get("fill_value")
            if notional is not None:
                try:
                    day.volume_usd += abs(float(notional))
                    day.fills += 1
                except (TypeError, ValueError):
                    pass
            slip = record.get("slippage_usd_vs_decision")
            if slip is not None:
                try:
                    day.slippage_usd += float(slip)
                except (TypeError, ValueError):
                    pass
    return dict(days)


def load_pnl(paths: Iterable[Path]) -> dict[tuple[str, str], PnlDay]:
    """Realized PnL and funding per (date, arm).

    The arm comes from the filename (`pnl-<service>-<arm>-<YYYYMMDD>.jsonl`)
    because the rows themselves do not carry it, and the date from each
    row's own timestamp rather than the filename, so a cycle that closes
    after a UTC rollover lands on the day it actually closed.
    """
    days: dict[tuple[str, str], PnlDay] = defaultdict(PnlDay)
    for path in paths:
        arm = arm_from_pnl_filename(path.name)
        if arm is None:
            continue
        for record in read_jsonl(path):
            if record.get("pnl") is None:
                continue
            ts = record.get("ts")
            if ts is None:
                continue
            day = days[(utc_date(float(ts)), arm)]
            try:
                day.realized_pnl_usd += float(record["pnl"])
            except (TypeError, ValueError):
                continue
            day.cycles += 1
            funding = record.get("funding_carry_usd")
            if funding is not None:
                try:
                    day.funding_usd += float(funding)
                    day.funding_seen = True
                except (TypeError, ValueError):
                    pass
    return dict(days)


def arm_from_pnl_filename(name: str) -> str | None:
    """`pnl-debot-pair-robinhood-lighter-freq-20260908.jsonl` -> `freq`."""
    if not name.startswith("pnl-") or not name.endswith(".jsonl"):
        return None
    stem = name[len("pnl-") : -len(".jsonl")]
    parts = stem.rsplit("-", 1)
    if len(parts) != 2 or not parts[1].isdigit():
        return None
    service_and_arm = parts[0]
    arm = service_and_arm.rsplit("-", 1)[-1]
    return arm or None


def equity_daily_costs(rows: Iterable[dict]) -> dict[str, float]:
    """Cost per UTC date from an equity_history series.

    The cost of a day is the fall in equity across it: last point of the
    previous day to last point of this one. The first day of the series
    has no previous close to measure against and is skipped rather than
    measured from its own first sample, which would understate whatever
    happened before the series started.
    """
    last_by_day: dict[str, float] = {}
    for row in rows:
        ts = row.get("ts")
        equity = row.get("equity")
        if ts is None or equity is None:
            continue
        try:
            # equity_history stamps milliseconds.
            day = utc_date(float(ts) / 1000.0)
            last_by_day[day] = float(equity)
        except (TypeError, ValueError):
            continue
    costs: dict[str, float] = {}
    previous: float | None = None
    for day in sorted(last_by_day):
        if previous is not None:
            costs[day] = -(last_by_day[day] - previous)
        previous = last_by_day[day]
    return costs


def load_points(path: Path | None) -> dict[tuple[str, str], float]:
    """Operator-supplied points, one JSON object per line:
    `{"date": "2026-09-08", "arm": "freq", "points": 1234.5}`."""
    if path is None:
        return {}
    points: dict[tuple[str, str], float] = {}
    for record in read_jsonl(path):
        date, arm, value = record.get("date"), record.get("arm"), record.get("points")
        if not date or not arm or value is None:
            continue
        try:
            points[(str(date), str(arm))] = float(value)
        except (TypeError, ValueError):
            continue
    return points


def build_rows(
    execution: dict[tuple[str, str], ExecDay],
    pnl: dict[tuple[str, str], PnlDay],
    equity_costs: dict[str, dict[str, float]] | None = None,
    points: dict[tuple[str, str], float] | None = None,
) -> list[Row]:
    equity_costs = equity_costs or {}
    points = points or {}
    keys = set(execution) | set(pnl)
    rows: list[Row] = []
    for date, arm in sorted(keys):
        row = Row(date=date, arm=arm)
        if (date, arm) in execution:
            day = execution[(date, arm)]
            row.fills = day.fills
            row.volume_usd = round(day.volume_usd, 6)
            row.slippage_usd = round(day.slippage_usd, 6)
        if (date, arm) in pnl:
            day = pnl[(date, arm)]
            row.cycles = day.cycles
            row.realized_pnl_usd = round(day.realized_pnl_usd, 6)
            row.funding_usd = round(day.funding_usd, 6) if day.funding_seen else None
            row.cost_usd = round(-(day.realized_pnl_usd + day.funding_usd), 6)
            row.cost_source = "pnl_ledger"
        elif date in equity_costs.get(arm, {}):
            row.cost_usd = round(equity_costs[arm][date], 6)
            row.cost_source = "equity_delta"
        row.points = points.get((date, arm))
        if row.points is not None and row.points > 0 and row.cost_usd is not None:
            row.cost_per_point = round(row.cost_usd / row.points, 8)
        if row.cost_usd is not None and row.volume_usd > 0:
            row.cost_per_musd_volume = round(row.cost_usd / (row.volume_usd / 1e6), 4)
        rows.append(row)
    return rows


def summarize(rows: list[Row]) -> dict:
    """Totals per arm, plus how much volume has no cost source at all.

    `uncosted_volume_usd` is the honest caveat on every ratio below it: a
    day the cost is unknown for still traded, so the totals describe less
    than the whole program.
    """
    by_arm: dict[str, dict] = {}
    for row in rows:
        arm = by_arm.setdefault(
            row.arm,
            {
                "arm": row.arm,
                "days": 0,
                "fills": 0,
                "volume_usd": 0.0,
                "costed_volume_usd": 0.0,
                "uncosted_volume_usd": 0.0,
                "cost_usd": 0.0,
                "points": 0.0,
                "points_seen": False,
                "cost_days": 0,
            },
        )
        arm["days"] += 1
        arm["fills"] += row.fills
        arm["volume_usd"] += row.volume_usd
        if row.cost_usd is None:
            arm["uncosted_volume_usd"] += row.volume_usd
        else:
            arm["cost_usd"] += row.cost_usd
            arm["costed_volume_usd"] += row.volume_usd
            arm["cost_days"] += 1
        if row.points is not None:
            arm["points"] += row.points
            arm["points_seen"] = True
    for arm in by_arm.values():
        for key in ("volume_usd", "costed_volume_usd", "uncosted_volume_usd", "cost_usd"):
            arm[key] = round(arm[key], 6)
        arm["cost_per_musd_volume"] = (
            round(arm["cost_usd"] / (arm["costed_volume_usd"] / 1e6), 4)
            if arm["costed_volume_usd"] > 0
            else None
        )
        arm["cost_per_point"] = (
            round(arm["cost_usd"] / arm["points"], 8)
            if arm["points_seen"] and arm["points"] > 0
            else None
        )
        if not arm["points_seen"]:
            arm["points"] = None
        del arm["points_seen"]
    return {"arms": [by_arm[a] for a in sorted(by_arm)]}


def render_table(rows: list[Row], summary: dict) -> str:
    out = [
        f"{'date':11s} {'arm':6s} {'fills':>5s} {'volume_usd':>12s} "
        f"{'cost_usd':>9s} {'src':>12s} {'$/M vol':>9s} {'points':>9s}"
    ]
    for row in rows:
        out.append(
            f"{row.date:11s} {row.arm:6s} {row.fills:5d} {row.volume_usd:12.2f} "
            f"{'-' if row.cost_usd is None else format(row.cost_usd, '9.2f'):>9s} "
            f"{row.cost_source or '-':>12s} "
            f"{'-' if row.cost_per_musd_volume is None else format(row.cost_per_musd_volume, '9.2f'):>9s} "
            f"{'-' if row.points is None else format(row.points, '9.1f'):>9s}"
        )
    out.append("")
    out.append("Totals (cost is positive when the arm gave money up):")
    for arm in summary["arms"]:
        if arm["cost_per_musd_volume"] is None:
            # No costed volume at all: saying "cost $0.00" here would read
            # as free rather than as unmeasured.
            out.append(
                f"  {arm['arm']:6s} volume ${arm['volume_usd']:,.0f} over {arm['days']}d, "
                f"cost unknown (no PnL ledger or equity series covers these days)"
            )
            continue
        out.append(
            f"  {arm['arm']:6s} volume ${arm['volume_usd']:,.0f} over {arm['days']}d, "
            f"cost ${arm['cost_usd']:,.2f} on ${arm['costed_volume_usd']:,.0f} of it"
            f" = ${arm['cost_per_musd_volume']:,.2f} per $1M traded"
        )
        if arm["uncosted_volume_usd"] > 0:
            out.append(
                f"         ${arm['uncosted_volume_usd']:,.0f} of that volume has no cost "
                f"source and is excluded above"
            )
        if arm["cost_per_point"] is not None:
            out.append(
                f"         {arm['points']:,.1f} points = ${arm['cost_per_point']:.6f} per point"
            )
    return "\n".join(out)


def expand(patterns: list[str]) -> list[Path]:
    paths: list[Path] = []
    for pattern in patterns:
        paths.extend(Path(p) for p in sorted(glob.glob(pattern)))
    return paths


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--exec-glob", action="append", default=[], required=True)
    parser.add_argument("--pnl-glob", action="append", default=[])
    parser.add_argument(
        "--equity",
        action="append",
        default=[],
        metavar="ARM=PATH",
        help="equity_history.jsonl for an arm, used only for days the PnL ledger misses",
    )
    parser.add_argument("--points", type=Path, default=None)
    parser.add_argument("--out", type=Path, default=None, help="write the rows as JSONL")
    args = parser.parse_args(argv)

    execution = load_execution(expand(args.exec_glob))
    pnl = load_pnl(expand(args.pnl_glob))
    equity_costs: dict[str, dict[str, float]] = {}
    for spec in args.equity:
        arm, _, path = spec.partition("=")
        if not path:
            parser.error(f"--equity expects ARM=PATH, got {spec!r}")
        equity_costs[arm] = equity_daily_costs(read_jsonl(Path(path)))

    rows = build_rows(execution, pnl, equity_costs, load_points(args.points))
    summary = summarize(rows)
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        with args.out.open("w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row.as_json(), sort_keys=True) + "\n")
    print(render_table(rows, summary))
    return 0


if __name__ == "__main__":
    sys.exit(main())
