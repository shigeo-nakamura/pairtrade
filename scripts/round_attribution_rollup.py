#!/usr/bin/env python3
"""
Roll up per-trade attribution CSVs into a round-level readout.

Input CSVs are produced by scripts/live_trade_attribution.py --csv-out. This
script is intentionally report-only: it aggregates realised-accounting rows for
round evaluation, but does not fetch data or change bot behaviour.

Examples:
  scripts/round_attribution_rollup.py \
    --csv /tmp/round7-a-attribution.csv \
    --csv /tmp/round7-b-attribution.csv \
    --csv /tmp/round7-c-attribution.csv \
    --execution-ledger-report /tmp/round7-execution-ledger.md \
    --report-out /tmp/round7-attribution-rollup.md
"""

from __future__ import annotations

import argparse
import csv
import glob
import statistics
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Callable, Iterable


@dataclass(frozen=True)
class TradeRow:
    source: str
    index: str
    venue: str
    variant: str
    pair: str
    direction: str
    close_at: datetime | None
    legs: int
    realized_pnl: Decimal
    trading_fees: Decimal | None
    bot_pnl: Decimal | None
    execution_leakage: Decimal | None
    entry_sync_secs: float | None
    exit_sync_secs: float | None
    hold_secs: float | None
    entry_z: Decimal | None
    entry_beta: Decimal | None
    entry_feature_source: str
    pnl_available: bool | None
    close_reason: str
    partial_marker_count: int
    reissue_marker_count: int
    amend_marker_count: int
    gaps: str

    @property
    def complete(self) -> bool:
        return self.legs == 2

    @property
    def won(self) -> bool:
        return self.realized_pnl > 0

    @property
    def bot_pnl_missing(self) -> bool:
        gaps = self.gaps.lower()
        return (
            self.pnl_available is False
            or "bot pnl unavailable" in gaps
            or "bot pnl jsonl unmatched" in gaps
        )


def parse_decimal(raw: object) -> Decimal | None:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text or text.lower() == "n/a":
        return None
    if text.startswith("$"):
        text = text[1:]
    try:
        return Decimal(text)
    except InvalidOperation:
        return None


def decimal_or_zero(raw: object) -> Decimal:
    return parse_decimal(raw) or Decimal("0")


def parse_float(raw: object) -> float | None:
    value = parse_decimal(raw)
    return float(value) if value is not None else None


def parse_int(raw: object, default: int = 0) -> int:
    try:
        text = str(raw).strip()
        if not text:
            return default
        return int(float(text))
    except (TypeError, ValueError):
        return default


def parse_bool(raw: object) -> bool | None:
    if raw is None:
        return None
    text = str(raw).strip().lower()
    if text in {"true", "1", "yes", "y"}:
        return True
    if text in {"false", "0", "no", "n"}:
        return False
    return None


def parse_ts(raw: object) -> datetime | None:
    text = str(raw or "").strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S%z"):
        try:
            dt = datetime.strptime(text, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except ValueError:
            pass
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except ValueError:
        return None


def load_csv(path: Path) -> list[TradeRow]:
    rows: list[TradeRow] = []
    with path.open(newline="", encoding="utf-8") as handle:
        for raw in csv.DictReader(handle):
            rows.append(
                TradeRow(
                    source=str(path),
                    index=raw.get("index", ""),
                    venue=raw.get("venue", ""),
                    variant=(raw.get("variant") or "n/a").lower(),
                    pair=raw.get("pair", ""),
                    direction=raw.get("direction", ""),
                    close_at=parse_ts(raw.get("close_at_utc")),
                    legs=parse_int(raw.get("legs")),
                    realized_pnl=decimal_or_zero(raw.get("realized_pnl")),
                    trading_fees=parse_decimal(raw.get("trading_fees")),
                    bot_pnl=parse_decimal(raw.get("bot_pnl")),
                    execution_leakage=parse_decimal(raw.get("execution_leakage")),
                    entry_sync_secs=parse_float(raw.get("entry_sync_secs")),
                    exit_sync_secs=parse_float(raw.get("exit_sync_secs")),
                    hold_secs=parse_float(raw.get("hold_secs")),
                    entry_z=parse_decimal(raw.get("entry_z")),
                    entry_beta=parse_decimal(raw.get("entry_beta")),
                    entry_feature_source=raw.get("entry_feature_source", ""),
                    pnl_available=parse_bool(raw.get("pnl_available")),
                    close_reason=raw.get("close_reason", "") or "n/a",
                    partial_marker_count=parse_int(raw.get("partial_marker_count")),
                    reissue_marker_count=parse_int(raw.get("reissue_marker_count")),
                    amend_marker_count=parse_int(raw.get("amend_marker_count")),
                    gaps=raw.get("gaps", ""),
                )
            )
    return rows


def sum_decimal(values: Iterable[Decimal]) -> Decimal:
    total = Decimal("0")
    for value in values:
        total += value
    return total


def sum_optional(values: Iterable[Decimal | None]) -> Decimal | None:
    values = list(values)
    if not values or any(value is None for value in values):
        return None
    return sum_decimal(value for value in values if value is not None)


def money(value: Decimal | None, places: int = 2) -> str:
    if value is None:
        return "n/a"
    quant = Decimal("1").scaleb(-places)
    return f"${value.quantize(quant)}"


def num(value: float | None, places: int = 1) -> str:
    if value is None:
        return "n/a"
    return f"{value:.{places}f}"


def pct(value: float | None, places: int = 1) -> str:
    if value is None:
        return "n/a"
    return f"{value:.{places}f}%"


def median_decimal(values: list[Decimal]) -> Decimal | None:
    if not values:
        return None
    return Decimal(str(statistics.median([float(v) for v in values])))


def aggregate(rows: list[TradeRow]) -> dict[str, object]:
    pnl = sum_decimal(row.realized_pnl for row in rows)
    wins = sum(1 for row in rows if row.won)
    complete = sum(1 for row in rows if row.complete)
    pnl_values = [row.realized_pnl for row in rows]
    return {
        "n": len(rows),
        "complete": complete,
        "wins": wins,
        "win_rate": wins / len(rows) * 100.0 if rows else None,
        "pnl": pnl,
        "avg": pnl / Decimal(len(rows)) if rows else None,
        "median": median_decimal(pnl_values),
        "fees": sum_optional(row.trading_fees for row in rows),
        "bot_pnl": sum_optional(row.bot_pnl for row in rows),
        "execution_leakage": sum_optional(row.execution_leakage for row in rows),
        "bot_pnl_missing": sum(1 for row in rows if row.bot_pnl_missing),
        "feature_coverage": sum(
            1 for row in rows if row.entry_z is not None or row.entry_beta is not None
        ),
    }


def bucket_stats(label: str, rows: list[TradeRow], selected: list[TradeRow]) -> dict[str, object]:
    selected_ids = {id(row) for row in selected}
    kept = [row for row in rows if id(row) not in selected_ids]
    selected_pnl = sum_decimal(row.realized_pnl for row in selected)
    kept_pnl = sum_decimal(row.realized_pnl for row in kept)
    total_losses = sum_decimal(-row.realized_pnl for row in rows if row.realized_pnl < 0)
    selected_losses = sum_decimal(-row.realized_pnl for row in selected if row.realized_pnl < 0)
    total_wins = sum(1 for row in rows if row.realized_pnl > 0)
    selected_wins = sum(1 for row in selected if row.realized_pnl > 0)
    loss_recall = float(selected_losses / total_losses * Decimal("100")) if total_losses > 0 else 0.0
    win_kill = selected_wins / total_wins * 100.0 if total_wins else 0.0
    return {
        "label": label,
        "n": len(selected),
        "pnl": selected_pnl,
        "win_rate": selected_wins / len(selected) * 100.0 if selected else 0.0,
        "avg": selected_pnl / Decimal(len(selected)) if selected else Decimal("0"),
        "loss_recall": loss_recall,
        "win_kill": win_kill,
        "kept_n": len(kept),
        "kept_pnl": kept_pnl,
        "score": loss_recall - win_kill,
    }


def feature_matrix(rows: list[TradeRow]) -> list[dict[str, object]]:
    scoped = [row for row in rows if row.complete]
    if not scoped:
        return []

    def by(label: str, predicate: Callable[[TradeRow], bool]) -> dict[str, object]:
        return bucket_stats(label, scoped, [row for row in scoped if predicate(row)])

    buckets = [
        by("close_reason=force_close", lambda row: row.close_reason == "force_close"),
        by("close_reason=ineligible", lambda row: row.close_reason == "ineligible"),
        by("close_reason=stop_loss_z", lambda row: row.close_reason == "stop_loss_z"),
        by("close_reason=exit_z", lambda row: row.close_reason == "exit_z"),
        by("close_reason=partial_fill", lambda row: row.close_reason == "partial_fill"),
        by(
            "partial fill (marker or reason)",
            lambda row: row.partial_marker_count > 0 or row.close_reason == "partial_fill",
        ),
        by("reissue markers present", lambda row: row.reissue_marker_count > 0),
        by("amend markers present", lambda row: row.amend_marker_count > 0),
        by("bot pnl missing/unmatched", lambda row: row.bot_pnl_missing),
        by("abs(entry_z) >= 3.0", lambda row: row.entry_z is not None and abs(row.entry_z) >= Decimal("3.0")),
        by("abs(entry_z) >= 3.5", lambda row: row.entry_z is not None and abs(row.entry_z) >= Decimal("3.5")),
        by("abs(entry_z) >= 4.0", lambda row: row.entry_z is not None and abs(row.entry_z) >= Decimal("4.0")),
        by("beta < 0.9", lambda row: row.entry_beta is not None and row.entry_beta < Decimal("0.9")),
        by("beta >= 0.9", lambda row: row.entry_beta is not None and row.entry_beta >= Decimal("0.9")),
        by("entry features missing", lambda row: row.entry_z is None and row.entry_beta is None),
        by("hold >= 2h", lambda row: row.hold_secs is not None and row.hold_secs >= 7200.0),
        by("hold >= 3h", lambda row: row.hold_secs is not None and row.hold_secs >= 10800.0),
        by("entry_sync >= 5s", lambda row: row.entry_sync_secs is not None and row.entry_sync_secs >= 5.0),
        by("exit_sync >= 30s", lambda row: row.exit_sync_secs is not None and row.exit_sync_secs >= 30.0),
    ]
    return sorted(
        [row for row in buckets if row["n"]],
        key=lambda row: (row["score"], -float(row["pnl"])),
        reverse=True,
    )


def week_start(dt: datetime) -> datetime:
    day = dt.astimezone(timezone.utc).date()
    monday = day - timedelta(days=day.weekday())
    return datetime(monday.year, monday.month, monday.day, tzinfo=timezone.utc)


def grouped_pnl(rows: list[TradeRow], key_fn: Callable[[TradeRow], object]) -> list[tuple[object, list[TradeRow]]]:
    groups: dict[object, list[TradeRow]] = defaultdict(list)
    for row in rows:
        key = key_fn(row)
        if key is not None:
            groups[key].append(row)
    return sorted(groups.items(), key=lambda item: item[0])


def emit_table(lines: list[str], header: list[str], rows: list[list[str]]) -> None:
    lines.append("| " + " | ".join(header) + " |")
    lines.append("|" + "|".join("---" for _ in header) + "|")
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")
    lines.append("")


def render_report(
    rows: list[TradeRow],
    inputs: list[Path],
    execution_ledger_reports: list[Path],
    title: str,
) -> str:
    rows = sorted(rows, key=lambda row: (row.close_at or datetime.min.replace(tzinfo=timezone.utc), row.variant, row.index))
    lines: list[str] = [f"# {title}", ""]
    if rows:
        closes = [row.close_at for row in rows if row.close_at is not None]
        if closes:
            lines.append(f"Window: `{min(closes).isoformat()}` to `{max(closes).isoformat()}`")
            lines.append("")

    lines.append("Inputs:")
    lines.append("")
    lines.append("Attribution CSVs:")
    for path in inputs:
        lines.append(f"- `{path}`")
    if execution_ledger_reports:
        lines.append("")
        lines.append("Execution-ledger reports:")
        for path in execution_ledger_reports:
            lines.append(f"- `{path}`")
    lines.append("")

    summary = aggregate(rows)
    lines.append("## Summary")
    lines.append("")
    emit_table(
        lines,
        ["Metric", "Value"],
        [
            ["Trades", str(summary["n"])],
            ["Complete two-leg trades", str(summary["complete"])],
            ["Win rate", pct(summary["win_rate"])],
            ["DEX realised PnL", money(summary["pnl"])],
            ["Avg / median trade PnL", f"{money(summary['avg'])} / {money(summary['median'])}"],
            ["Trading fees", money(summary["fees"])],
            ["Bot PnL matched", money(summary["bot_pnl"])],
            ["Execution leakage", money(summary["execution_leakage"])],
            ["Bot PnL missing rows", str(summary["bot_pnl_missing"])],
            ["Entry feature coverage", f"{summary['feature_coverage']}/{summary['n']}"],
        ],
    )

    lines.append("## By Variant")
    lines.append("")
    variant_rows: list[list[str]] = []
    for variant, items in grouped_pnl(rows, lambda row: row.variant):
        stats = aggregate(items)
        variant_rows.append(
            [
                str(variant).upper(),
                str(stats["n"]),
                str(stats["complete"]),
                money(stats["pnl"]),
                pct(stats["win_rate"]),
                money(stats["avg"]),
                money(stats["execution_leakage"]),
                f"{stats['feature_coverage']}/{stats['n']}",
            ]
        )
    emit_table(
        lines,
        ["Variant", "N", "Complete", "DEX PnL", "Win rate", "Avg PnL", "Exec leak", "Feature rows"],
        variant_rows,
    )

    lines.append("## PnL Concentration")
    lines.append("")
    week_rows = []
    for start, items in grouped_pnl(rows, lambda row: week_start(row.close_at) if row.close_at else None):
        pnl = sum_decimal(row.realized_pnl for row in items)
        week_rows.append([start.strftime("%Y-%m-%d"), str(len(items)), money(pnl)])
    emit_table(lines, ["UTC week", "N", "DEX PnL"], week_rows)

    day_groups = grouped_pnl(rows, lambda row: row.close_at.date() if row.close_at else None)
    worst_days = sorted(
        ((sum_decimal(row.realized_pnl for row in items), day, items) for day, items in day_groups),
        key=lambda item: item[0],
    )[:10]
    emit_table(
        lines,
        ["Worst UTC day", "N", "DEX PnL"],
        [[str(day), str(len(items)), money(pnl)] for pnl, day, items in worst_days],
    )

    matrix = feature_matrix(rows)
    if matrix:
        scoped_count = sum(1 for row in rows if row.complete)
        lines.append("## Cross-Variant Feature Buckets")
        lines.append("")
        lines.append(
            f"Scope: complete two-leg trades only (`N={scoped_count}`). "
            "`Loss recall` is realised-loss share captured by the bucket; "
            "`Win kill` is winning-trade share captured by the same bucket."
        )
        lines.append("")
        emit_table(
            lines,
            [
                "Rank",
                "Bucket",
                "N",
                "Bucket PnL",
                "Win rate",
                "Avg PnL",
                "Loss recall",
                "Win kill",
                "Kept N",
                "Kept PnL",
            ],
            [
                [
                    str(rank),
                    str(row["label"]),
                    str(row["n"]),
                    money(row["pnl"]),
                    pct(row["win_rate"]),
                    money(row["avg"]),
                    pct(row["loss_recall"]),
                    pct(row["win_kill"]),
                    str(row["kept_n"]),
                    money(row["kept_pnl"]),
                ]
                for rank, row in enumerate(matrix, start=1)
            ],
        )

    lines.append("## Execution Ledger Coverage")
    lines.append("")
    if execution_ledger_reports:
        lines.append(
            "The attached #613 execution-ledger reports are execution diagnostics, not the accounting source of truth."
        )
        lines.append("")
        lines.append("- Keep DEX realised PnL as the primary PnL score for A/B/C decisions.")
        lines.append(
            "- Use ledger slippage, leg-sync, partial-fill, reissue, latency, and fee buckets to separate execution artifacts from strategy signal."
        )
        lines.append(
            "- Ledger coverage only applies to windows after a #613-enabled binary was running and matching `execution-<tag>.jsonl` files existed."
        )
        lines.append(
            "- Do not treat missing ledger windows as zero execution leakage; mark them as unknown execution attribution."
        )
    else:
        lines.append("No #613 execution-ledger report was supplied for this rollup.")
        lines.append("")
        lines.append(
            "- DEX realised PnL remains usable, but execution-artifact attribution is incomplete."
        )
        lines.append(
            "- Any A/B/C decision from this report must state that ledger-backed slippage, leg-sync, partial-fill, reissue, latency, and fee coverage is missing."
        )
        lines.append(
            "- Do not promote an execution-artifact bucket to strategy edge without comparable ledger coverage or an independent non-ledger confirmation."
        )
    lines.append("")

    lines.append("## Readout Guardrails")
    lines.append("")
    lines.append("- Confirm config-drift preflight before scoring A/B/C differences.")
    lines.append("- Treat execution-artifact buckets as attribution adjustments, not strategy edge.")
    lines.append("- Promote a structural follow-up only when the bucket survives cross-variant or independent-window checks.")
    lines.append("- Keep YAML/config changes out of this rollup; this report is diagnostic input for the round decision.")
    lines.append("")
    return "\n".join(lines)


def expand_inputs(patterns: list[str]) -> list[Path]:
    paths: list[Path] = []
    for pattern in patterns:
        expanded = glob.glob(str(Path(pattern).expanduser()))
        if expanded:
            paths.extend(Path(path) for path in expanded)
        else:
            paths.append(Path(pattern).expanduser())
    return sorted(set(paths))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", action="append", required=True, help="Attribution CSV path or glob")
    parser.add_argument(
        "--execution-ledger-report",
        action="append",
        default=[],
        help="Optional #613 execution-ledger markdown report path or glob",
    )
    parser.add_argument("--title", default="Round Attribution Rollup")
    parser.add_argument("--report-out", type=Path)
    args = parser.parse_args()

    inputs = expand_inputs(args.csv)
    execution_ledger_reports = expand_inputs(args.execution_ledger_report)
    missing = [path for path in inputs if not path.exists()]
    if missing:
        for path in missing:
            print(f"missing input: {path}", file=sys.stderr)
        return 1
    missing_ledger_reports = [path for path in execution_ledger_reports if not path.exists()]
    if missing_ledger_reports:
        for path in missing_ledger_reports:
            print(f"missing execution ledger report: {path}", file=sys.stderr)
        return 1

    rows: list[TradeRow] = []
    for path in inputs:
        rows.extend(load_csv(path))
    report = render_report(rows, inputs, execution_ledger_reports, args.title)
    if args.report_out:
        args.report_out.expanduser().write_text(report + "\n", encoding="utf-8")
    else:
        print(report)
    return 0


if __name__ == "__main__":
    sys.exit(main())
