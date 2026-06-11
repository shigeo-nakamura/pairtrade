#!/usr/bin/env python3
"""Pair/universe pre-screen for bot-strategy#513.

This is a coarse screen, not a byte-exact trading simulation. It reads
multi-symbol JSONL snapshots and ranks symbol pairs by residual mean-reversion
amplitude after simple executable-cost estimates.

Supported input shapes:
  * pairtrade / pair_data_collector: {"timestamp": ms, "prices": {...}}
  * Variational shadow logger: {"ts_utc": "...Z", "listings": {...}}

Examples:
  scripts/pair_universe_screen.py --input-glob '/tmp/bt/*.jsonl'
  scripts/pair_universe_screen.py --source variational \
      --input-glob '/tmp/variational_stats_2026-06-*.jsonl'
"""

import argparse
import csv
import glob
import json
import math
import statistics
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from itertools import combinations
from typing import Dict, Iterable, List, Optional, Tuple


@dataclass
class SymbolSample:
    price: float
    spread_bps: Optional[float] = None
    funding_per_hour: Optional[float] = None
    top_depth_usd: Optional[float] = None
    volume_24h: Optional[float] = None


@dataclass
class Snapshot:
    ts_ms: int
    symbols: Dict[str, SymbolSample]


@dataclass
class SymbolStats:
    symbol: str
    samples: int = 0
    first_ts_ms: Optional[int] = None
    last_ts_ms: Optional[int] = None
    spread_values: List[float] = field(default_factory=list)
    depth_values: List[float] = field(default_factory=list)
    volume_values: List[float] = field(default_factory=list)

    def observe(self, ts_ms: int, sample: SymbolSample) -> None:
        self.samples += 1
        self.first_ts_ms = ts_ms if self.first_ts_ms is None else min(self.first_ts_ms, ts_ms)
        self.last_ts_ms = ts_ms if self.last_ts_ms is None else max(self.last_ts_ms, ts_ms)
        if sample.spread_bps is not None:
            self.spread_values.append(sample.spread_bps)
        if sample.top_depth_usd is not None:
            self.depth_values.append(sample.top_depth_usd)
        if sample.volume_24h is not None:
            self.volume_values.append(sample.volume_24h)


@dataclass
class PairResult:
    pair: str
    n: int
    days: float
    beta: float
    corr_ret: Optional[float]
    spread_std_bps: float
    half_life_h: Optional[float]
    events: int
    gross_median_bps: Optional[float]
    gross_p25_bps: Optional[float]
    gross_win_rate: Optional[float]
    avg_cost_bps: float
    net_median_bps: Optional[float]
    sample_interval_s: Optional[float]
    liquidity_usd: Optional[float]
    avg_spread_a_bps: Optional[float]
    avg_spread_b_bps: Optional[float]
    avg_funding_diff_bps_per_h: Optional[float]


def parse_float(value) -> Optional[float]:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed):
        return None
    return parsed


def parse_ts_utc(value: str) -> Optional[int]:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return int(dt.timestamp() * 1000)


def percentile(values: List[float], pct: float) -> Optional[float]:
    if not values:
        return None
    xs = sorted(values)
    if len(xs) == 1:
        return xs[0]
    rank = (len(xs) - 1) * pct
    lo = math.floor(rank)
    hi = math.ceil(rank)
    if lo == hi:
        return xs[lo]
    frac = rank - lo
    return xs[lo] * (1.0 - frac) + xs[hi] * frac


def mean(values: Iterable[float]) -> Optional[float]:
    xs = list(values)
    return sum(xs) / len(xs) if xs else None


def safe_median(values: List[float]) -> Optional[float]:
    return statistics.median(values) if values else None


def fmt_num(value: Optional[float], digits: int = 2) -> str:
    if value is None or not math.isfinite(value):
        return "-"
    return f"{value:.{digits}f}"


def fmt_int(value: Optional[float]) -> str:
    if value is None or not math.isfinite(value):
        return "-"
    return f"{value:.0f}"


def load_snapshots(paths: List[str], source: str) -> List[Snapshot]:
    snapshots: List[Snapshot] = []
    for path in paths:
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                snap = parse_record(record, source)
                if snap is not None and len(snap.symbols) >= 2:
                    snapshots.append(snap)
    snapshots.sort(key=lambda s: s.ts_ms)
    return snapshots


def parse_record(record: dict, source: str) -> Optional[Snapshot]:
    if source == "auto":
        if isinstance(record.get("listings"), dict):
            source = "variational"
        elif isinstance(record.get("prices"), dict):
            source = "pairtrade"
        else:
            return None
    if source == "variational":
        return parse_variational_record(record)
    if source == "pairtrade":
        return parse_pairtrade_record(record)
    raise ValueError(f"unknown source: {source}")


def parse_pairtrade_record(record: dict) -> Optional[Snapshot]:
    ts_ms = record.get("timestamp")
    if ts_ms is None:
        ts_ms = parse_ts_utc(record.get("ts_utc", ""))
    try:
        ts_ms = int(ts_ms)
    except (TypeError, ValueError):
        return None

    symbols: Dict[str, SymbolSample] = {}
    for symbol, raw in (record.get("prices") or {}).items():
        if not isinstance(raw, dict):
            continue
        price = parse_float(raw.get("price") or raw.get("mid") or raw.get("mark_price"))
        bid = parse_float(raw.get("bid_price"))
        ask = parse_float(raw.get("ask_price"))
        if bid and ask and bid > 0 and ask > 0:
            mid = (bid + ask) * 0.5
            price = mid
            spread_bps = (ask - bid) / mid * 10_000.0
        else:
            spread_bps = parse_float(raw.get("spread_bps"))
        if price is None or price <= 0:
            continue

        bid_size = parse_float(raw.get("bid_size"))
        ask_size = parse_float(raw.get("ask_size"))
        top_depth_usd = None
        if bid_size is not None and ask_size is not None:
            top_depth_usd = max(0.0, min(bid_size, ask_size) * price)
        funding_per_hour = parse_float(raw.get("funding_rate"))
        symbols[str(symbol)] = SymbolSample(
            price=price,
            spread_bps=spread_bps,
            funding_per_hour=funding_per_hour,
            top_depth_usd=top_depth_usd,
        )
    return Snapshot(ts_ms=ts_ms, symbols=symbols)


def parse_variational_record(record: dict) -> Optional[Snapshot]:
    ts_ms = parse_ts_utc(record.get("ts_utc", ""))
    if ts_ms is None:
        return None
    symbols: Dict[str, SymbolSample] = {}
    for symbol, raw in (record.get("listings") or {}).items():
        if not isinstance(raw, dict):
            continue
        quotes = raw.get("quotes") or {}
        base = quotes.get("base") or {}
        bid = parse_float(base.get("bid"))
        ask = parse_float(base.get("ask"))
        mark = parse_float(raw.get("mark_price"))
        if bid and ask and bid > 0 and ask > 0:
            price = (bid + ask) * 0.5
            spread_bps = (ask - bid) / price * 10_000.0
        else:
            price = mark
            spread_bps = parse_float(raw.get("base_spread_bps"))
        if price is None or price <= 0:
            continue

        funding_raw = parse_float(raw.get("funding_rate"))
        interval_s = parse_float(raw.get("funding_interval_s")) or 28_800.0
        funding_per_hour = None
        if funding_raw is not None and interval_s > 0:
            # Variational shadow data exposes percent per funding interval.
            funding_per_hour = (funding_raw / 100.0) / (interval_s / 3600.0)

        symbols[str(symbol)] = SymbolSample(
            price=price,
            spread_bps=spread_bps,
            funding_per_hour=funding_per_hour,
            top_depth_usd=variational_depth_usd(quotes),
            volume_24h=parse_float(raw.get("volume_24h")),
        )
    return Snapshot(ts_ms=ts_ms, symbols=symbols)


def variational_depth_usd(quotes: dict) -> Optional[float]:
    if not isinstance(quotes, dict):
        return None
    depth = None
    for key, usd in (("size_1m", 1_000_000.0), ("size_100k", 100_000.0), ("size_1k", 1_000.0)):
        q = quotes.get(key) or {}
        bid = parse_float(q.get("bid"))
        ask = parse_float(q.get("ask"))
        if bid is not None and ask is not None and bid > 0 and ask > 0:
            depth = usd
            break
    return depth


def collect_symbol_stats(snapshots: List[Snapshot]) -> Dict[str, SymbolStats]:
    stats: Dict[str, SymbolStats] = {}
    for snap in snapshots:
        for symbol, sample in snap.symbols.items():
            stats.setdefault(symbol, SymbolStats(symbol=symbol)).observe(snap.ts_ms, sample)
    return stats


def aligned_pair_series(
    snapshots: List[Snapshot],
    a: str,
    b: str,
) -> Tuple[List[int], List[float], List[float], List[SymbolSample], List[SymbolSample]]:
    ts: List[int] = []
    pa: List[float] = []
    pb: List[float] = []
    sa: List[SymbolSample] = []
    sb: List[SymbolSample] = []
    for snap in snapshots:
        aa = snap.symbols.get(a)
        bb = snap.symbols.get(b)
        if aa is None or bb is None:
            continue
        if aa.price <= 0 or bb.price <= 0:
            continue
        ts.append(snap.ts_ms)
        pa.append(aa.price)
        pb.append(bb.price)
        sa.append(aa)
        sb.append(bb)
    return ts, pa, pb, sa, sb


def ols_beta(x: List[float], y: List[float]) -> Optional[float]:
    if len(x) != len(y) or len(x) < 3:
        return None
    mx = sum(x) / len(x)
    my = sum(y) / len(y)
    var_x = sum((v - mx) ** 2 for v in x)
    if var_x <= 0:
        return None
    cov = sum((xx - mx) * (yy - my) for xx, yy in zip(x, y))
    beta = cov / var_x
    if not math.isfinite(beta):
        return None
    return beta


def pearson(x: List[float], y: List[float]) -> Optional[float]:
    if len(x) != len(y) or len(x) < 3:
        return None
    mx = sum(x) / len(x)
    my = sum(y) / len(y)
    dx = [v - mx for v in x]
    dy = [v - my for v in y]
    sx = math.sqrt(sum(v * v for v in dx))
    sy = math.sqrt(sum(v * v for v in dy))
    if sx <= 0 or sy <= 0:
        return None
    return sum(a * b for a, b in zip(dx, dy)) / (sx * sy)


def residual_series(pa: List[float], pb: List[float]) -> Tuple[Optional[float], List[float]]:
    la = [math.log(p) for p in pa]
    lb = [math.log(p) for p in pb]
    beta = ols_beta(lb, la)
    if beta is None:
        return None, []
    alpha = (sum(la) / len(la)) - beta * (sum(lb) / len(lb))
    residuals = [a - alpha - beta * b for a, b in zip(la, lb)]
    return beta, residuals


def half_life_hours(residuals: List[float], sample_interval_s: Optional[float]) -> Optional[float]:
    if len(residuals) < 10 or not sample_interval_s:
        return None
    x = residuals[:-1]
    y = residuals[1:]
    phi = ols_beta(x, y)
    if phi is None or phi <= 0.0 or phi >= 1.0:
        return None
    periods = -math.log(2.0) / math.log(phi)
    return periods * sample_interval_s / 3600.0


def median_interval_seconds(ts_ms: List[int]) -> Optional[float]:
    if len(ts_ms) < 2:
        return None
    diffs = [(b - a) / 1000.0 for a, b in zip(ts_ms, ts_ms[1:]) if b > a]
    return safe_median(diffs)


def return_corr(pa: List[float], pb: List[float]) -> Optional[float]:
    if len(pa) < 4:
        return None
    ra = [math.log(b / a) for a, b in zip(pa, pa[1:]) if a > 0 and b > 0]
    rb = [math.log(b / a) for a, b in zip(pb, pb[1:]) if a > 0 and b > 0]
    n = min(len(ra), len(rb))
    return pearson(ra[:n], rb[:n]) if n >= 3 else None


def estimate_excursions(
    residuals: List[float],
    ts_ms: List[int],
    entry_z: float,
    exit_z: float,
    max_hold_hours: float,
) -> List[float]:
    if len(residuals) < 20:
        return []
    mu = sum(residuals) / len(residuals)
    std = statistics.stdev(residuals)
    if std <= 0:
        return []
    z = [(r - mu) / std for r in residuals]
    max_hold_ms = int(max_hold_hours * 3600 * 1000)
    events: List[float] = []
    i = 0
    while i < len(z):
        zi = z[i]
        if abs(zi) < entry_z:
            i += 1
            continue
        direction = -1.0 if zi > 0 else 1.0
        entry_resid = residuals[i]
        deadline = ts_ms[i] + max_hold_ms
        j = i + 1
        exit_idx = None
        while j < len(z) and ts_ms[j] <= deadline:
            if abs(z[j]) <= exit_z:
                exit_idx = j
                break
            j += 1
        if exit_idx is None:
            exit_idx = min(j, len(z) - 1)
        exit_resid = residuals[exit_idx]
        gross_bps = direction * (exit_resid - entry_resid) * 10_000.0
        events.append(gross_bps)
        i = max(exit_idx + 1, i + 1)
    return events


def pair_cost_bps(
    sa: List[SymbolSample],
    sb: List[SymbolSample],
    fee_bps: float,
    hold_hours: Optional[float],
) -> Tuple[float, Optional[float], Optional[float], Optional[float], Optional[float]]:
    spreads_a = [x.spread_bps for x in sa if x.spread_bps is not None]
    spreads_b = [x.spread_bps for x in sb if x.spread_bps is not None]
    avg_spread_a = mean(spreads_a)
    avg_spread_b = mean(spreads_b)
    spread_cost = (avg_spread_a or 0.0) + (avg_spread_b or 0.0)

    fund_diffs = []
    for aa, bb in zip(sa, sb):
        if aa.funding_per_hour is not None and bb.funding_per_hour is not None:
            fund_diffs.append(abs(aa.funding_per_hour - bb.funding_per_hour) * 10_000.0)
    avg_funding_diff_bps_per_h = mean(fund_diffs)
    funding_cost = (avg_funding_diff_bps_per_h or 0.0) * max(0.0, hold_hours or 0.0)

    depth_values = []
    for aa, bb in zip(sa, sb):
        if aa.top_depth_usd is not None and bb.top_depth_usd is not None:
            depth_values.append(min(aa.top_depth_usd, bb.top_depth_usd))
    liquidity_usd = percentile(depth_values, 0.25)

    total = spread_cost + 4.0 * fee_bps + funding_cost
    return total, avg_spread_a, avg_spread_b, avg_funding_diff_bps_per_h, liquidity_usd


def analyze_pair(
    snapshots: List[Snapshot],
    a: str,
    b: str,
    args: argparse.Namespace,
) -> Optional[PairResult]:
    ts, pa, pb, sa, sb = aligned_pair_series(snapshots, a, b)
    if len(ts) < args.min_samples:
        return None
    beta, residuals = residual_series(pa, pb)
    if beta is None or len(residuals) < args.min_samples:
        return None
    sample_interval_s = median_interval_seconds(ts)
    hl_h = half_life_hours(residuals, sample_interval_s)
    events = estimate_excursions(residuals, ts, args.entry_z, args.exit_z, args.max_hold_hours)
    corr = return_corr(pa, pb)
    spread_std_bps = statistics.stdev(residuals) * 10_000.0 if len(residuals) > 1 else 0.0
    days = (ts[-1] - ts[0]) / 86_400_000.0 if len(ts) > 1 else 0.0
    hold_hours = hl_h if hl_h is not None else args.max_hold_hours
    cost, avg_sa, avg_sb, fund_diff, liquidity = pair_cost_bps(sa, sb, args.fee_bps, hold_hours)

    gross_median = safe_median(events)
    gross_p25 = percentile(events, 0.25)
    gross_win = mean(1.0 if e > 0 else 0.0 for e in events) if events else None
    net_median = gross_median - cost if gross_median is not None else None

    return PairResult(
        pair=f"{a}/{b}",
        n=len(ts),
        days=days,
        beta=beta,
        corr_ret=corr,
        spread_std_bps=spread_std_bps,
        half_life_h=hl_h,
        events=len(events),
        gross_median_bps=gross_median,
        gross_p25_bps=gross_p25,
        gross_win_rate=gross_win,
        avg_cost_bps=cost,
        net_median_bps=net_median,
        sample_interval_s=sample_interval_s,
        liquidity_usd=liquidity,
        avg_spread_a_bps=avg_sa,
        avg_spread_b_bps=avg_sb,
        avg_funding_diff_bps_per_h=fund_diff,
    )


def analyze(snapshots: List[Snapshot], args: argparse.Namespace) -> Tuple[Dict[str, SymbolStats], List[PairResult]]:
    stats = collect_symbol_stats(snapshots)
    symbols = sorted(s for s, st in stats.items() if st.samples >= args.min_symbol_samples)
    results: List[PairResult] = []
    for a, b in combinations(symbols, 2):
        result = analyze_pair(snapshots, a, b, args)
        if result is not None:
            results.append(result)
    results.sort(
        key=lambda r: (
            r.net_median_bps if r.net_median_bps is not None else -1e9,
            r.events,
            r.n,
        ),
        reverse=True,
    )
    return stats, results


def render_markdown(stats: Dict[str, SymbolStats], results: List[PairResult], args: argparse.Namespace) -> str:
    lines = []
    lines.append("# Pair/universe pre-screen")
    lines.append("")
    lines.append(
        "Caveat: this is a coarse residual screen from synchronized market snapshots, "
        "not byte-exact trade PnL. `ideal net med bps` subtracts rough cost "
        "from idealized residual reversion amplitude; promote candidates only after replay-quality follow-up."
    )
    lines.append("")
    lines.append(
        f"Inputs: files={args.input_count}, snapshots={args.snapshot_count}, "
        f"entry_z={args.entry_z:g}, exit_z={args.exit_z:g}, max_hold_h={args.max_hold_hours:g}, "
        f"fee_bps_per_fill={args.fee_bps:g}"
    )
    lines.append("")
    lines.append("## Data Availability")
    lines.append("")
    lines.append("| symbol | samples | days | avg spread bps | p25 depth USD | avg volume 24h |")
    lines.append("|---|---:|---:|---:|---:|---:|")
    for symbol in sorted(stats):
        st = stats[symbol]
        days = 0.0
        if st.first_ts_ms is not None and st.last_ts_ms is not None:
            days = (st.last_ts_ms - st.first_ts_ms) / 86_400_000.0
        lines.append(
            f"| {symbol} | {st.samples} | {days:.2f} | "
            f"{fmt_num(mean(st.spread_values), 2)} | "
            f"{fmt_int(percentile(st.depth_values, 0.25))} | "
            f"{fmt_int(mean(st.volume_values))} |"
        )
    lines.append("")
    lines.append("## Ranked Pairs")
    lines.append("")
    lines.append("| rank | pair | ideal net med bps | gross med bps | cost bps | events | win% | p25 gross | half-life h | corr ret | std bps | p25 depth USD | samples | days |")
    lines.append("|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    for idx, r in enumerate(results[: args.top], 1):
        win_pct = r.gross_win_rate * 100.0 if r.gross_win_rate is not None else None
        lines.append(
            f"| {idx} | {r.pair} | {fmt_num(r.net_median_bps, 2)} | "
            f"{fmt_num(r.gross_median_bps, 2)} | {fmt_num(r.avg_cost_bps, 2)} | "
            f"{r.events} | {fmt_num(win_pct, 1)} | {fmt_num(r.gross_p25_bps, 2)} | "
            f"{fmt_num(r.half_life_h, 2)} | {fmt_num(r.corr_ret, 3)} | "
            f"{fmt_num(r.spread_std_bps, 2)} | {fmt_int(r.liquidity_usd)} | "
            f"{r.n} | {r.days:.2f} |"
        )
    lines.append("")
    if not results:
        lines.append("No pair passed the minimum sample/event filters.")
    else:
        viable = [
            r for r in results
            if r.net_median_bps is not None
            and r.net_median_bps > 0
            and r.events >= args.min_events
        ]
        if viable:
            top = ", ".join(r.pair for r in viable[:3])
            lines.append(f"Top candidates for replay-quality follow-up: {top}.")
        else:
            lines.append("No pair clears the coarse net-median/event threshold; treat this window as reject or collect broader data.")
    return "\n".join(lines)


def write_csv(path: str, results: List[PairResult]) -> None:
    fields = [name for name in PairResult.__dataclass_fields__]
    with open(path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for r in results:
            writer.writerow({field: getattr(r, field) for field in fields})


def expand_globs(patterns: List[str]) -> List[str]:
    paths: List[str] = []
    for pattern in patterns:
        matches = sorted(glob.glob(pattern))
        if matches:
            paths.extend(matches)
        else:
            paths.append(pattern)
    seen = set()
    deduped = []
    for path in paths:
        if path not in seen:
            seen.add(path)
            deduped.append(path)
    return deduped


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--input-glob", action="append", required=True, help="JSONL file or glob. Repeatable.")
    p.add_argument("--source", choices=("auto", "pairtrade", "variational"), default="auto")
    p.add_argument("--fee-bps", type=float, default=0.0, help="Per-fill fee in bps. Cost uses 4 fills per round trip.")
    p.add_argument("--entry-z", type=float, default=2.0)
    p.add_argument("--exit-z", type=float, default=0.2)
    p.add_argument("--max-hold-hours", type=float, default=3.0)
    p.add_argument("--min-samples", type=int, default=300)
    p.add_argument("--min-symbol-samples", type=int, default=300)
    p.add_argument("--min-events", type=int, default=3)
    p.add_argument("--top", type=int, default=20)
    p.add_argument("--csv-out", help="Optional CSV path for the ranked pairs.")
    p.add_argument("--markdown-out", help="Optional Markdown report path.")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    paths = expand_globs(args.input_glob)
    if not paths:
        print("ERROR: no input files", file=sys.stderr)
        return 2
    snapshots = load_snapshots(paths, args.source)
    args.input_count = len(paths)
    args.snapshot_count = len(snapshots)
    if not snapshots:
        print("ERROR: no usable snapshots", file=sys.stderr)
        return 2
    stats, results = analyze(snapshots, args)
    if args.csv_out:
        write_csv(args.csv_out, results)
    report = render_markdown(stats, results, args)
    if args.markdown_out:
        with open(args.markdown_out, "w", encoding="utf-8") as fh:
            fh.write(report)
            fh.write("\n")
    print(report)
    return 0


if __name__ == "__main__":
    sys.exit(main())
