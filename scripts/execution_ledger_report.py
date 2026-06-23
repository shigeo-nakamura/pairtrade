#!/usr/bin/env python3
"""
Summarize pairtrade execution-ledger JSONL.

The input is the JSONL written by the bot's execution ledger
(`execution-<tag>.jsonl`). The report is intentionally read-only and tolerant of
partial records so it can be used on live windows while the schema evolves.

Examples:
  scripts/execution_ledger_report.py \
    --ledger '/opt/debot/debot_pnl/execution-debot-pair-btceth-*.jsonl' \
    --since 2026-06-23T00:00:00Z \
    --report-out /tmp/execution-ledger-report.md
"""

from __future__ import annotations

import argparse
import glob
import json
import math
import statistics
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable


@dataclass(frozen=True)
class Record:
    source: str
    event: str
    ts_ms: int | None
    variant: str
    pair: str
    phase: str
    close_reason: str
    leg_symbol: str
    order_type: str
    attempt: int
    slippage_bps: float | None
    slippage_usd: float | None
    latency_ms: float | None
    fee_bps: float | None
    leg_sync_gap_ms: float | None
    notional_usd: float | None
    overfill: bool
    underfill: bool


def parse_ts(raw: str) -> datetime:
    text = raw.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def ts_ms_to_dt(ts_ms: int | None) -> datetime | None:
    if ts_ms is None:
        return None
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)


def as_int(raw: object) -> int | None:
    if raw is None:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def as_float(raw: object) -> float | None:
    if raw is None:
        return None
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return None
    if math.isnan(value) or math.isinf(value):
        return None
    return value


def as_bool(raw: object) -> bool:
    if isinstance(raw, bool):
        return raw
    text = str(raw).strip().lower()
    return text in {"1", "true", "yes", "y"}


def load_records(paths: list[Path], since: datetime | None, until: datetime | None) -> list[Record]:
    records: list[Record] = []
    for path in paths:
        with path.open(encoding="utf-8", errors="replace") as handle:
            for line_no, line in enumerate(handle, start=1):
                if not line.strip():
                    continue
                try:
                    raw = json.loads(line)
                except json.JSONDecodeError as exc:
                    print(f"warning: {path}:{line_no}: invalid json: {exc}", file=sys.stderr)
                    continue
                ts_ms = as_int(raw.get("ts_ms"))
                dt = ts_ms_to_dt(ts_ms)
                if since and dt and dt < since:
                    continue
                if until and dt and dt >= until:
                    continue
                records.append(
                    Record(
                        source=str(path),
                        event=str(raw.get("event", "")),
                        ts_ms=ts_ms,
                        variant=str(raw.get("variant", "n/a")).lower(),
                        pair=str(raw.get("pair", "n/a")),
                        phase=str(raw.get("phase", "n/a")),
                        close_reason=str(raw.get("close_reason", "") or "n/a"),
                        leg_symbol=str(raw.get("leg_symbol", "")),
                        order_type=str(raw.get("order_type", "")),
                        attempt=as_int(raw.get("attempt")) or 0,
                        slippage_bps=as_float(
                            raw.get("gross_execution_slippage_bps", raw.get("slippage_bps_vs_decision"))
                        ),
                        slippage_usd=as_float(
                            raw.get("gross_execution_slippage_usd", raw.get("slippage_usd_vs_decision"))
                        ),
                        latency_ms=as_float(raw.get("latency_submit_fill_ms")),
                        fee_bps=as_float(raw.get("fee_bps")),
                        leg_sync_gap_ms=as_float(raw.get("leg_sync_gap_ms")),
                        notional_usd=as_float(raw.get("notional_usd")),
                        overfill=as_bool(raw.get("overfill_detected")),
                        underfill=as_bool(raw.get("underfill_detected")),
                    )
                )
    return records


def percentile(values: list[float], q: float) -> float | None:
    values = sorted(v for v in values if v is not None)
    if not values:
        return None
    if len(values) == 1:
        return values[0]
    pos = (len(values) - 1) * q
    lo = math.floor(pos)
    hi = math.ceil(pos)
    if lo == hi:
        return values[lo]
    weight = pos - lo
    return values[lo] * (1.0 - weight) + values[hi] * weight


def stats(values: Iterable[float | None]) -> dict[str, float | None]:
    clean = [value for value in values if value is not None]
    if not clean:
        return {"n": 0, "mean": None, "median": None, "p95": None, "sum": None}
    return {
        "n": len(clean),
        "mean": statistics.mean(clean),
        "median": statistics.median(clean),
        "p95": percentile(clean, 0.95),
        "sum": sum(clean),
    }


def fmt(value: float | None, places: int = 2) -> str:
    if value is None:
        return "n/a"
    return f"{value:.{places}f}"


def fmt_int(value: float | None) -> str:
    if value is None:
        return "n/a"
    return str(int(round(value)))


def group_by(records: Iterable[Record], key_fn: Callable[[Record], tuple[str, ...]]) -> dict[tuple[str, ...], list[Record]]:
    grouped: dict[tuple[str, ...], list[Record]] = defaultdict(list)
    for record in records:
        grouped[key_fn(record)].append(record)
    return dict(sorted(grouped.items()))


def is_reissue_like(record: Record) -> bool:
    text = f"{record.order_type} {record.close_reason}".lower()
    return record.attempt > 1 or "reissue" in text or "timeout" in text or "partial" in text


def emit_table(lines: list[str], headers: list[str], rows: list[list[str]]) -> None:
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("|" + "|".join("---" for _ in headers) + "|")
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")
    lines.append("")


def render_report(records: list[Record], inputs: list[Path], since: datetime | None, until: datetime | None) -> str:
    records = sorted(records, key=lambda r: (r.ts_ms or 0, r.variant, r.phase, r.event))
    leg_records = [r for r in records if r.event == "leg_fill"]
    pair_records = [r for r in records if r.event == "pair_fill_summary"]

    lines: list[str] = ["# Execution Ledger Report", ""]
    if since or until:
        lines.append(f"Window: `{since.isoformat() if since else ''}` to `{until.isoformat() if until else ''}`")
        lines.append("")
    lines.append("Inputs:")
    for path in inputs:
        lines.append(f"- `{path}`")
    lines.append("")

    pair_slip = stats(r.slippage_bps for r in pair_records)
    pair_drag = stats(r.slippage_usd for r in pair_records)
    sync = stats(r.leg_sync_gap_ms for r in pair_records)
    latency = stats(r.latency_ms for r in leg_records)
    lines.append("## Summary")
    lines.append("")
    emit_table(
        lines,
        ["Metric", "Value"],
        [
            ["Records", str(len(records))],
            ["Leg fills", str(len(leg_records))],
            ["Pair summaries", str(len(pair_records))],
            ["Pair slippage bps mean / median / p95", f"{fmt(pair_slip['mean'])} / {fmt(pair_slip['median'])} / {fmt(pair_slip['p95'])}"],
            ["Execution PnL drag USD sum", fmt(pair_drag["sum"])],
            ["Leg-sync gap ms median / p95", f"{fmt_int(sync['median'])} / {fmt_int(sync['p95'])}"],
            ["Submit-fill latency ms median / p95", f"{fmt_int(latency['median'])} / {fmt_int(latency['p95'])}"],
            ["Overfill summaries", str(sum(1 for r in pair_records if r.overfill))],
            ["Underfill summaries", str(sum(1 for r in pair_records if r.underfill))],
            ["Reissue/timeout-like leg fills", str(sum(1 for r in leg_records if is_reissue_like(r)))],
        ],
    )

    lines.append("## Pair Summary By Variant / Phase")
    lines.append("")
    rows: list[list[str]] = []
    for (variant, phase), items in group_by(pair_records, lambda r: (r.variant, r.phase)).items():
        slip = stats(r.slippage_bps for r in items)
        drag = stats(r.slippage_usd for r in items)
        gap = stats(r.leg_sync_gap_ms for r in items)
        notional = stats(r.notional_usd for r in items)
        rows.append(
            [
                variant.upper(),
                phase,
                str(len(items)),
                fmt(notional["sum"]),
                fmt(slip["mean"]),
                fmt(slip["median"]),
                fmt(slip["p95"]),
                fmt(drag["sum"]),
                fmt_int(gap["median"]),
                fmt_int(gap["p95"]),
                str(sum(1 for r in items if r.overfill)),
                str(sum(1 for r in items if r.underfill)),
            ]
        )
    emit_table(
        lines,
        [
            "Variant",
            "Phase",
            "N",
            "Notional sum",
            "Slip mean bps",
            "Slip med bps",
            "Slip p95 bps",
            "Drag USD",
            "Sync med ms",
            "Sync p95 ms",
            "Over",
            "Under",
        ],
        rows,
    )

    lines.append("## Leg Fill By Variant / Phase / Symbol")
    lines.append("")
    rows = []
    for (variant, phase, symbol), items in group_by(leg_records, lambda r: (r.variant, r.phase, r.leg_symbol)).items():
        slip = stats(r.slippage_bps for r in items)
        fee = stats(r.fee_bps for r in items)
        lat = stats(r.latency_ms for r in items)
        rows.append(
            [
                variant.upper(),
                phase,
                symbol,
                str(len(items)),
                fmt(slip["median"]),
                fmt(slip["p95"]),
                fmt(fee["median"]),
                fmt_int(lat["median"]),
                fmt_int(lat["p95"]),
                str(sum(1 for r in items if is_reissue_like(r))),
            ]
        )
    emit_table(
        lines,
        [
            "Variant",
            "Phase",
            "Leg",
            "N",
            "Slip med bps",
            "Slip p95 bps",
            "Fee med bps",
            "Latency med ms",
            "Latency p95 ms",
            "Reissue-like",
        ],
        rows,
    )

    if not records:
        lines.append("No ledger records matched the requested inputs/window.")
        lines.append("")
    return "\n".join(lines)


def expand_inputs(patterns: list[str]) -> list[Path]:
    paths: list[Path] = []
    for pattern in patterns:
        matches = glob.glob(str(Path(pattern).expanduser()))
        if matches:
            paths.extend(Path(match) for match in matches)
        else:
            paths.append(Path(pattern).expanduser())
    return sorted(set(paths))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledger", action="append", required=True, help="Execution ledger JSONL path or glob")
    parser.add_argument("--since", help="UTC lower bound, ISO-8601")
    parser.add_argument("--until", help="UTC upper bound, ISO-8601")
    parser.add_argument("--report-out", type=Path)
    args = parser.parse_args()

    inputs = expand_inputs(args.ledger)
    missing = [path for path in inputs if not path.exists()]
    if missing:
        for path in missing:
            print(f"missing input: {path}", file=sys.stderr)
        return 1

    since = parse_ts(args.since) if args.since else None
    until = parse_ts(args.until) if args.until else None
    records = load_records(inputs, since, until)
    report = render_report(records, inputs, since, until)
    if args.report_out:
        args.report_out.expanduser().write_text(report + "\n", encoding="utf-8")
    else:
        print(report)
    return 0


if __name__ == "__main__":
    sys.exit(main())
