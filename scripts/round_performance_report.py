#!/usr/bin/env python3
"""Build a repeatable live-round performance snapshot from local artifacts.

This report complements ``live_trade_attribution.py``.  DEX accounting exports
remain the per-trade source of truth; this script automates the other evidence
used in a round readout:

* flat-to-flat collateral delta and external equity-curve MDD;
* bot PnL JSONL as explicitly secondary attribution;
* pairwise A/B/C comparison, including opportunities available to only one arm;
* execution-ledger slippage, notional, reissue, overfill, and underfill metrics.

The script never fetches remote data and never changes live state.  See
``docs/round-attribution-readout.md`` for the read-only collection commands.
"""

from __future__ import annotations

import argparse
import json
import math
import re
import statistics
import sys
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

import execution_ledger_report as ledger


@dataclass(frozen=True)
class EquitySample:
    ts_ms: int
    equity: float


@dataclass(frozen=True)
class BotPnlRow:
    ts: float
    entry_ts: float
    pnl: float
    funding: float
    direction: str
    close_reason: str
    hold_secs: float


@dataclass
class VariantSummary:
    variant: str
    start_collateral: float
    end_collateral: float | None
    end_ts: str | None
    collateral_delta: float | None
    collateral_return_bps: float | None
    mdd_usd: float | None
    mdd_bps: float | None
    mdd_peak_ts: str | None
    mdd_trough_ts: str | None
    pre_daily_dd_days: int
    pre_daily_dd_plus_2sigma_bps: float | None
    exceeds_pre_daily_dd_plus_2sigma: bool | None
    isolated_spikes_removed: int
    capital_event_like_steps: int
    bot_trade_count: int
    bot_pnl: float
    bot_return_bps: float
    bot_funding: float
    close_reasons: dict[str, int]


def parse_utc(raw: str) -> datetime:
    text = raw.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def iso_ms(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000.0, timezone.utc).isoformat()


def as_float(raw: object, default: float = 0.0) -> float:
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return default
    return value if math.isfinite(value) else default


def load_jsonl(path: Path) -> Iterable[dict]:
    with path.open(encoding="utf-8", errors="replace") as handle:
        for line_no, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError as exc:
                print(f"warning: {path}:{line_no}: invalid json: {exc}", file=sys.stderr)
                continue
            if isinstance(value, dict):
                yield value


def find_equity_path(data_dir: Path, variant: str, agent: str) -> Path | None:
    names = [
        f"{variant}.equity_history.jsonl",
        f"{agent}.equity_history.jsonl",
    ]
    for name in names:
        matches = sorted(data_dir.rglob(name))
        if matches:
            return matches[-1]
    return None


def load_equity(path: Path) -> list[EquitySample]:
    samples: list[EquitySample] = []
    for row in load_jsonl(path):
        ts = as_float(row.get("ts"), -1.0)
        equity = as_float(row.get("equity"), float("nan"))
        if ts < 0 or not math.isfinite(equity):
            continue
        ts_ms = int(ts if ts >= 1_000_000_000_000 else ts * 1000)
        samples.append(EquitySample(ts_ms=ts_ms, equity=equity))
    return sorted(samples, key=lambda item: item.ts_ms)


def remove_isolated_spikes(
    samples: list[EquitySample],
    reference: float,
    threshold_bps: float,
    neighbor_tolerance_bps: float,
) -> tuple[list[EquitySample], list[EquitySample]]:
    """Remove one-sample spikes whose immediate neighbors agree.

    A real deposit/withdrawal persists into the following samples.  This guard
    only removes an isolated middle point with two mutually consistent
    neighbors, and reports every removal in the generated packet.
    """
    if len(samples) < 3 or reference <= 0:
        return list(samples), []
    removed: list[EquitySample] = []
    keep = [True] * len(samples)
    for i in range(1, len(samples) - 1):
        prev, cur, nxt = samples[i - 1], samples[i], samples[i + 1]
        neighbor_gap = abs(prev.equity - nxt.equity) / reference * 10_000.0
        cur_gap = min(abs(cur.equity - prev.equity), abs(cur.equity - nxt.equity))
        cur_gap_bps = cur_gap / reference * 10_000.0
        if neighbor_gap <= neighbor_tolerance_bps and cur_gap_bps >= threshold_bps:
            keep[i] = False
            removed.append(cur)
    return [sample for sample, include in zip(samples, keep) if include], removed


def max_drawdown(
    samples: list[EquitySample], start_equity: float, start_ts_ms: int
) -> tuple[float, float, int, int]:
    peak = start_equity
    peak_ts = start_ts_ms
    worst_usd = 0.0
    worst_bps = 0.0
    worst_peak_ts = start_ts_ms
    trough_ts = start_ts_ms
    for sample in samples:
        if sample.equity > peak:
            peak = sample.equity
            peak_ts = sample.ts_ms
        if peak <= 0:
            continue
        dd_usd = peak - sample.equity
        dd_bps = dd_usd / peak * 10_000.0
        if dd_bps > worst_bps:
            worst_usd = dd_usd
            worst_bps = dd_bps
            worst_peak_ts = peak_ts
            trough_ts = sample.ts_ms
    return worst_usd, worst_bps, worst_peak_ts, trough_ts


def daily_drawdowns(samples: list[EquitySample]) -> dict[date, float]:
    grouped: dict[date, list[EquitySample]] = defaultdict(list)
    for sample in samples:
        day = datetime.fromtimestamp(sample.ts_ms / 1000.0, timezone.utc).date()
        grouped[day].append(sample)
    result: dict[date, float] = {}
    for day, rows in grouped.items():
        peak = rows[0].equity
        worst = 0.0
        for row in rows:
            peak = max(peak, row.equity)
            if peak > 0:
                worst = max(worst, (peak - row.equity) / peak * 10_000.0)
        result[day] = worst
    return result


def capital_event_like_steps(samples: list[EquitySample], reference: float) -> int:
    if reference <= 0:
        return 0
    threshold = reference * 0.50
    return sum(
        abs(current.equity - previous.equity) >= threshold
        for previous, current in zip(samples, samples[1:])
    )


def load_bot_pnl(
    data_dir: Path,
    service: str,
    variant: str,
    since_ts: float,
    until_ts: float,
) -> list[BotPnlRow]:
    pattern = f"pnl-{service}-{variant}-*.jsonl"
    paths = sorted(data_dir.rglob(pattern))
    rows: list[BotPnlRow] = []
    seen: set[tuple[object, ...]] = set()
    for path in paths:
        for raw in load_jsonl(path):
            ts = as_float(raw.get("ts"), -1.0)
            if ts >= 1_000_000_000_000:
                ts /= 1000.0
            if not since_ts <= ts < until_ts:
                continue
            pnl = as_float(raw.get("pnl"))
            hold_secs = as_float(raw.get("hold_secs"))
            key = (
                ts,
                pnl,
                raw.get("close_reason"),
                hold_secs,
                raw.get("direction"),
                raw.get("entry_price_a"),
                raw.get("entry_price_b"),
            )
            if key in seen:
                continue
            seen.add(key)
            rows.append(
                BotPnlRow(
                    ts=ts,
                    entry_ts=ts - hold_secs,
                    pnl=pnl,
                    funding=as_float(raw.get("funding_carry_usd")),
                    direction=str(raw.get("direction", "")),
                    close_reason=str(raw.get("close_reason", "unknown") or "unknown"),
                    hold_secs=hold_secs,
                )
            )
    return sorted(rows, key=lambda row: (row.entry_ts, row.ts))


def parse_flat_metrics(path: Path | None, variants: Iterable[str]) -> dict[str, bool | None]:
    result = {variant: None for variant in variants}
    if path is None:
        return result
    pattern = re.compile(
        r'^pairtrade_has_position\{(?P<labels>[^}]*)\}\s+(?P<value>[-+0-9.eE]+)\s*$'
    )
    variant_re = re.compile(r'(?:^|,)variant="([^"]+)"(?:,|$)')
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        match = pattern.match(line)
        if not match:
            continue
        variant_match = variant_re.search(match.group("labels"))
        if not variant_match:
            continue
        variant = variant_match.group(1).lower()
        if variant in result:
            result[variant] = as_float(match.group("value"), 1.0) == 0.0
    return result


def pair_bot_rows(
    left: list[BotPnlRow], right: list[BotPnlRow], tolerance_secs: float
) -> tuple[list[tuple[BotPnlRow, BotPnlRow]], list[BotPnlRow], list[BotPnlRow]]:
    used: set[int] = set()
    pairs: list[tuple[BotPnlRow, BotPnlRow]] = []
    left_only: list[BotPnlRow] = []
    for left_row in left:
        candidates = [
            (abs(left_row.entry_ts - right_row.entry_ts), index, right_row)
            for index, right_row in enumerate(right)
            if index not in used
            and left_row.direction == right_row.direction
            and abs(left_row.entry_ts - right_row.entry_ts) <= tolerance_secs
        ]
        if not candidates:
            left_only.append(left_row)
            continue
        _, index, right_row = min(candidates, key=lambda item: item[0])
        used.add(index)
        pairs.append((left_row, right_row))
    right_only = [row for index, row in enumerate(right) if index not in used]
    return pairs, left_only, right_only


def stat(values: Iterable[float | None], kind: str) -> float | None:
    cleaned = [value for value in values if value is not None]
    if not cleaned:
        return None
    if kind == "sum":
        return sum(cleaned)
    if kind == "mean":
        return statistics.mean(cleaned)
    if kind == "p95":
        return ledger.percentile(cleaned, 0.95)
    raise ValueError(kind)


def execution_summary(
    data_dir: Path, service: str, since: datetime, until: datetime
) -> tuple[dict[str, dict[str, dict[str, float | int | None]]], list[Path]]:
    paths = sorted(data_dir.rglob(f"execution-{service}_*.jsonl"))
    records = ledger.load_records(paths, since, until) if paths else []
    summaries: dict[str, dict[str, dict[str, float | int | None]]] = defaultdict(dict)
    variants = sorted({record.variant for record in records if record.variant != "n/a"})
    for variant in variants:
        for phase in ("entry", "exit"):
            pair_rows = [
                record
                for record in records
                if record.variant == variant
                and record.phase == phase
                and record.event == "pair_fill_summary"
            ]
            leg_rows = [
                record
                for record in records
                if record.variant == variant
                and record.phase == phase
                and record.event == "leg_fill"
            ]
            summaries[variant][phase] = {
                "n": len(pair_rows),
                "notional_usd": stat((row.notional_usd for row in pair_rows), "sum"),
                "slippage_mean_bps": stat((row.slippage_bps for row in pair_rows), "mean"),
                "slippage_p95_bps": stat((row.slippage_bps for row in pair_rows), "p95"),
                "slippage_usd": stat((row.slippage_usd for row in pair_rows), "sum"),
                "overfill_summaries": sum(row.overfill for row in pair_rows),
                "underfill_summaries": sum(row.underfill for row in pair_rows),
                "leg_fills": len(leg_rows),
                "reissue_like_leg_fills": sum(ledger.is_reissue_like(row) for row in leg_rows),
            }
    return {key: dict(value) for key, value in summaries.items()}, paths


def build_report(
    manifest: dict,
    data_dir: Path,
    until: datetime,
    metrics_path: Path | None,
) -> tuple[str, dict]:
    since = parse_utc(manifest["since"])
    if until <= since:
        raise ValueError("--until must be after manifest since")
    service = str(manifest["service"])
    variants_cfg = manifest["variants"]
    variants = sorted(variants_cfg)
    flat = parse_flat_metrics(metrics_path, variants)
    filter_cfg = manifest.get("equity_spike_filter", {})
    remove_spikes = bool(filter_cfg.get("enabled", False))
    spike_threshold = as_float(filter_cfg.get("threshold_bps"), 1000.0)
    neighbor_tolerance = as_float(filter_cfg.get("neighbor_tolerance_bps"), 5.0)
    lookback_days = int(manifest.get("daily_dd_lookback_days", 30))
    pre_window_guard_hours = as_float(manifest.get("pre_window_guard_hours"), 24.0)

    summaries: dict[str, VariantSummary] = {}
    pnl_rows: dict[str, list[BotPnlRow]] = {}
    equity_paths: dict[str, str | None] = {}
    for variant in variants:
        cfg = variants_cfg[variant]
        start_collateral = as_float(
            cfg.get("start_collateral_usd", cfg.get("equity_reference_usd"))
        )
        reference = as_float(cfg.get("equity_reference_usd"), start_collateral)
        agent = str(cfg.get("agent", f"{service}-{variant}"))
        path = find_equity_path(data_dir, variant, agent)
        equity_paths[variant] = str(path) if path else None
        removed: list[EquitySample] = []
        window: list[EquitySample] = []
        pre_window: list[EquitySample] = []
        if path:
            all_samples = load_equity(path)
            if remove_spikes:
                all_samples, removed = remove_isolated_spikes(
                    all_samples, reference, spike_threshold, neighbor_tolerance
                )
            since_ms = int(since.timestamp() * 1000)
            until_ms = int(until.timestamp() * 1000)
            lookback_ms = int(timedelta(days=lookback_days).total_seconds() * 1000)
            pre_guard_ms = int(timedelta(hours=pre_window_guard_hours).total_seconds() * 1000)
            window = [sample for sample in all_samples if since_ms <= sample.ts_ms < until_ms]
            pre_window = [
                sample
                for sample in all_samples
                if since_ms - lookback_ms <= sample.ts_ms < since_ms - pre_guard_ms
            ]

        if window:
            end = window[-1]
            delta = end.equity - start_collateral
            return_bps = delta / start_collateral * 10_000.0 if start_collateral else None
            mdd_usd, mdd_bps, peak_ts, trough_ts = max_drawdown(
                window, start_collateral, int(since.timestamp() * 1000)
            )
            pre_dds = list(daily_drawdowns(pre_window).values())
            threshold = None
            if pre_dds:
                threshold = statistics.mean(pre_dds) + 2.0 * statistics.pstdev(pre_dds)
            cap_steps = capital_event_like_steps(window, reference)
            end_equity: float | None = end.equity
            end_ts: str | None = iso_ms(end.ts_ms)
        else:
            end_equity = None
            end_ts = None
            delta = None
            return_bps = None
            mdd_usd = None
            mdd_bps = None
            peak_ts = None
            trough_ts = None
            pre_dds = []
            threshold = None
            cap_steps = 0

        rows = load_bot_pnl(
            data_dir, service, variant, since.timestamp(), until.timestamp()
        )
        pnl_rows[variant] = rows
        bot_pnl = sum(row.pnl for row in rows)
        summaries[variant] = VariantSummary(
            variant=variant,
            start_collateral=start_collateral,
            end_collateral=end_equity,
            end_ts=end_ts,
            collateral_delta=delta,
            collateral_return_bps=return_bps,
            mdd_usd=mdd_usd,
            mdd_bps=mdd_bps,
            mdd_peak_ts=iso_ms(peak_ts) if peak_ts is not None else None,
            mdd_trough_ts=iso_ms(trough_ts) if trough_ts is not None else None,
            pre_daily_dd_days=len(pre_dds),
            pre_daily_dd_plus_2sigma_bps=threshold,
            exceeds_pre_daily_dd_plus_2sigma=(mdd_bps > threshold)
            if mdd_bps is not None and threshold is not None
            else None,
            isolated_spikes_removed=len(
                [
                    sample
                    for sample in removed
                    if int(since.timestamp() * 1000) <= sample.ts_ms < int(until.timestamp() * 1000)
                ]
            ),
            capital_event_like_steps=cap_steps,
            bot_trade_count=len(rows),
            bot_pnl=bot_pnl,
            bot_return_bps=bot_pnl / start_collateral * 10_000.0
            if start_collateral
            else 0.0,
            bot_funding=sum(row.funding for row in rows),
            close_reasons=dict(sorted(Counter(row.close_reason for row in rows).items())),
        )

    execution, execution_paths = execution_summary(data_dir, service, since, until)
    tolerance = as_float(manifest.get("pair_match_tolerance_secs"), 20.0)
    comparisons: list[dict] = []
    for comparison in manifest.get("comparisons", []):
        left = str(comparison["left"]).lower()
        right = str(comparison["right"]).lower()
        left_ref = summaries[left].start_collateral
        right_ref = summaries[right].start_collateral
        pairs, left_only, right_only = pair_bot_rows(
            pnl_rows[left], pnl_rows[right], tolerance
        )
        paired_gap = sum(
            lrow.pnl / left_ref * 10_000.0 - rrow.pnl / right_ref * 10_000.0
            for lrow, rrow in pairs
        )
        left_only_bps = sum(row.pnl for row in left_only) / left_ref * 10_000.0
        right_only_bps = sum(row.pnl for row in right_only) / right_ref * 10_000.0
        material_divergences = sum(
            lrow.close_reason != rrow.close_reason or abs(lrow.ts - rrow.ts) >= 300.0
            for lrow, rrow in pairs
        )
        notional_ratios: dict[str, float | None] = {}
        for phase in ("entry", "exit"):
            left_notional = execution.get(left, {}).get(phase, {}).get("notional_usd")
            right_notional = execution.get(right, {}).get(phase, {}).get("notional_usd")
            notional_ratios[phase] = (
                float(left_notional) / float(right_notional)
                if left_notional is not None and right_notional not in (None, 0)
                else None
            )
        left_return = summaries[left].collateral_return_bps
        right_return = summaries[right].collateral_return_bps
        comparisons.append(
            {
                "label": str(comparison.get("label", f"{left.upper()} vs {right.upper()}")),
                "left": left,
                "right": right,
                "collateral_gap_bps": left_return - right_return
                if left_return is not None and right_return is not None
                else None,
                "bot_gap_bps_secondary": summaries[left].bot_return_bps
                - summaries[right].bot_return_bps,
                "paired_trades": len(pairs),
                "left_only_trades": len(left_only),
                "right_only_trades": len(right_only),
                "paired_gap_bps_secondary": paired_gap,
                "left_only_bps_secondary": left_only_bps,
                "right_only_bps_secondary": right_only_bps,
                "opportunity_gap_bps_secondary": left_only_bps - right_only_bps,
                "material_close_divergences": material_divergences,
                "execution_notional_ratio": notional_ratios,
            }
        )

    packet = {
        "round": manifest.get("round", "unknown"),
        "service": service,
        "since": since.isoformat(),
        "until": until.isoformat(),
        "metrics": str(metrics_path) if metrics_path else None,
        "flat": flat,
        "equity_paths": equity_paths,
        "execution_ledger_files": [str(path) for path in execution_paths],
        "variants": {variant: asdict(summary) for variant, summary in summaries.items()},
        "comparisons": comparisons,
        "execution": execution,
    }
    return render_markdown(manifest, packet), packet


def fmt(value: float | None, places: int = 2, signed: bool = False) -> str:
    if value is None:
        return "n/a"
    sign = "+" if signed else ""
    return f"{value:{sign}.{places}f}"


def money(value: float | None, signed: bool = False) -> str:
    return "$" + fmt(value, 3, signed=signed)


def render_table(lines: list[str], headers: list[str], rows: list[list[str]]) -> None:
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("|" + "|".join("---" for _ in headers) + "|")
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")
    lines.append("")


def render_markdown(manifest: dict, packet: dict) -> str:
    lines = [f"# {str(packet['round']).title()} Performance Snapshot", ""]
    lines.append(f"Window: `{packet['since']}` to `{packet['until']}`")
    lines.append("")
    lines.append(
        "Accounting note: flat-to-flat collateral delta is a venue-equity cross-check. "
        "DEX accounting exports processed by `live_trade_attribution.py` remain the per-trade source of truth. "
        "Bot PnL below is secondary attribution only."
    )
    lines.append("")

    lines.append("## Variant Snapshot")
    lines.append("")
    rows = []
    for variant, summary in packet["variants"].items():
        flat_value = packet["flat"].get(variant)
        flat_text = "yes" if flat_value is True else "no" if flat_value is False else "unknown"
        rows.append(
            [
                variant.upper(),
                str(summary["bot_trade_count"]),
                flat_text,
                money(summary["end_collateral"]),
                f"{money(summary['collateral_delta'], signed=True)} / {fmt(summary['collateral_return_bps'], signed=True)} bps",
                f"{fmt(summary['mdd_bps'])} bps",
                f"{fmt(summary['pre_daily_dd_plus_2sigma_bps'])} bps",
                "yes" if summary["exceeds_pre_daily_dd_plus_2sigma"] else "no"
                if summary["exceeds_pre_daily_dd_plus_2sigma"] is not None
                else "n/a",
            ]
        )
    render_table(
        lines,
        ["Arm", "Bot closes", "Flat", "End collateral", "Collateral Δ", "External MDD", "Pre-window daily DD +2σ", "Trigger"],
        rows,
    )

    total_start = sum(summary["start_collateral"] for summary in packet["variants"].values())
    deltas = [summary["collateral_delta"] for summary in packet["variants"].values()]
    if deltas and all(delta is not None for delta in deltas):
        total_delta = sum(float(delta) for delta in deltas)
        lines.append(
            f"Book collateral delta: **{money(total_delta, signed=True)} / "
            f"{fmt(total_delta / total_start * 10_000.0, signed=True)} bps**."
        )
        lines.append("")

    lines.append("## Pairwise Comparisons")
    lines.append("")
    render_table(
        lines,
        ["Comparison", "Collateral gap", "Bot gap (secondary)", "Paired", "Left-only / right-only", "Opportunity gap (secondary)", "Material close divergences", "Entry / exit notional ratio"],
        [
            [
                item["label"],
                f"{fmt(item['collateral_gap_bps'], signed=True)} bps",
                f"{fmt(item['bot_gap_bps_secondary'], signed=True)} bps",
                str(item["paired_trades"]),
                f"{item['left_only_trades']} / {item['right_only_trades']}",
                f"{fmt(item['opportunity_gap_bps_secondary'], signed=True)} bps",
                str(item["material_close_divergences"]),
                f"{fmt(item['execution_notional_ratio']['entry'])}x / {fmt(item['execution_notional_ratio']['exit'])}x",
            ]
            for item in packet["comparisons"]
        ],
    )
    for item in packet["comparisons"]:
        lines.append(
            f"- {item['label']}: paired bot-side gap "
            f"{fmt(item['paired_gap_bps_secondary'], signed=True)} bps; "
            f"left-only {fmt(item['left_only_bps_secondary'], signed=True)} bps; "
            f"right-only {fmt(item['right_only_bps_secondary'], signed=True)} bps."
        )
    lines.append("")

    lines.append("## Execution Ledger")
    lines.append("")
    execution_rows: list[list[str]] = []
    for variant, phases in packet["execution"].items():
        for phase, values in phases.items():
            execution_rows.append(
                [
                    variant.upper(),
                    phase,
                    str(values["n"]),
                    money(values["notional_usd"]),
                    fmt(values["slippage_mean_bps"]),
                    fmt(values["slippage_p95_bps"]),
                    str(values["reissue_like_leg_fills"]),
                    str(values["overfill_summaries"]),
                    str(values["underfill_summaries"]),
                ]
            )
    if execution_rows:
        render_table(
            lines,
            ["Arm", "Phase", "N", "Notional", "Slip mean bps", "Slip p95 bps", "Reissue-like legs", "Over", "Under"],
            execution_rows,
        )
    else:
        lines.append("No execution-ledger files covered this window.")
        lines.append("")

    lines.append("## Bot PnL (Secondary Only)")
    lines.append("")
    render_table(
        lines,
        ["Arm", "N", "Bot PnL", "Bot return", "Funding field", "Close reasons"],
        [
            [
                variant.upper(),
                str(summary["bot_trade_count"]),
                money(summary["bot_pnl"], signed=True),
                f"{fmt(summary['bot_return_bps'], signed=True)} bps",
                money(summary["bot_funding"], signed=True),
                ", ".join(f"{key}={value}" for key, value in summary["close_reasons"].items()),
            ]
            for variant, summary in packet["variants"].items()
        ],
    )

    lines.append("## Data Guards")
    lines.append("")
    known_bias = manifest.get("known_bot_pnl_bias_issue")
    if known_bias:
        lines.append(f"- Bot PnL is non-authoritative while {known_bias} is not live-verified on this process.")
    for variant, summary in packet["variants"].items():
        if summary["isolated_spikes_removed"]:
            lines.append(
                f"- {variant.upper()}: removed {summary['isolated_spikes_removed']} isolated equity spike(s) "
                "whose immediate neighbors agreed; inspect the JSON packet before accepting the MDD."
            )
        if summary["capital_event_like_steps"]:
            lines.append(
                f"- {variant.upper()}: {summary['capital_event_like_steps']} persistent capital-event-like step(s) remain; "
                "collateral delta is not a clean PnL score until transfers are reconciled."
            )
        if packet["flat"].get(variant) is not True:
            lines.append(
                f"- {variant.upper()}: flatness was not proven from metrics; collateral delta may include unrealized PnL."
            )
    lines.append("- Circuit-breaker/halt history must be joined from journal or a durable risk-event archive; current gauges only show current state.")
    lines.append("- Do not restart, deploy, transfer capital, or mutate risk state as part of this readout.")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--data-dir", required=True, type=Path)
    parser.add_argument("--until", required=True, help="Exclusive UTC cutoff")
    parser.add_argument("--metrics", type=Path, help="Optional Prometheus snapshot proving flatness")
    parser.add_argument("--report-out", type=Path)
    parser.add_argument("--json-out", type=Path)
    args = parser.parse_args()

    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    report, packet = build_report(
        manifest=manifest,
        data_dir=args.data_dir,
        until=parse_utc(args.until),
        metrics_path=args.metrics,
    )
    if args.report_out:
        args.report_out.write_text(report + "\n", encoding="utf-8")
    else:
        print(report)
    if args.json_out:
        args.json_out.write_text(json.dumps(packet, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
