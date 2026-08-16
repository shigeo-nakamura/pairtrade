#!/usr/bin/env python3
"""Report cadence, completeness, and book-quality stats from observer dumps."""

from __future__ import annotations

import argparse
import gzip
import json
import math
import statistics
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path


def open_text(path: Path):
    if path.suffix == ".gz":
        return gzip.open(path, "rt", encoding="utf-8", errors="replace")
    return path.open(encoding="utf-8", errors="replace")


def percentile(values: list[float], fraction: float) -> float:
    if not values:
        return float("nan")
    ordered = sorted(values)
    index = (len(ordered) - 1) * fraction
    lower = int(index)
    upper = min(lower + 1, len(ordered) - 1)
    weight = index - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def utc_stamp(timestamp_ms: int | None) -> str:
    if timestamp_ms is None:
        return "n/a"
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc).isoformat()


def timestamp_in_datetime_range(timestamp_ms: int) -> bool:
    try:
        datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
    except (OverflowError, OSError, ValueError):
        return False
    return True


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("dumps", nargs="+", type=Path)
    parser.add_argument("--symbols", nargs="+", default=["BTC", "ETH", "SOL"])
    args = parser.parse_args()

    frames = 0
    invalid_json = 0
    non_monotonic = 0
    missing_symbol = defaultdict(int)
    missing_symbol_examples: dict[str, list[int]] = defaultdict(list)
    invalid_book = defaultdict(int)
    invalid_book_examples: dict[str, list[int]] = defaultdict(list)
    crossed = defaultdict(int)
    nonpositive_size = defaultdict(int)
    spreads_bps: dict[str, list[float]] = defaultdict(list)
    max_spread_event: dict[str, tuple[float, int] | None] = {
        symbol: None for symbol in args.symbols
    }
    gaps: list[float] = []
    gap_events: list[tuple[float, int, int]] = []
    first_ts = None
    last_ts = None

    run_price: dict[str, str] = {}
    run_start: dict[str, int] = {}
    run_last: dict[str, int] = {}
    longest: dict[str, tuple[int, int, int, str] | None] = {
        symbol: None for symbol in args.symbols
    }

    def finish_run(symbol: str) -> None:
        if symbol not in run_start:
            return
        candidate = (
            run_last[symbol] - run_start[symbol],
            run_start[symbol],
            run_last[symbol],
            run_price[symbol],
        )
        current = longest[symbol]
        if current is None or candidate[0] > current[0]:
            longest[symbol] = candidate

    def reset_run(symbol: str) -> None:
        finish_run(symbol)
        run_price.pop(symbol, None)
        run_start.pop(symbol, None)
        run_last.pop(symbol, None)

    previous_ts = None
    for path in args.dumps:
        with open_text(path) as src:
            for line in src:
                try:
                    row = json.loads(line)
                    timestamp = int(row["timestamp"])
                    if not timestamp_in_datetime_range(timestamp):
                        raise ValueError("timestamp is outside the datetime range")
                    prices = row["prices"]
                    if not isinstance(prices, dict):
                        raise TypeError("prices must be an object")
                except (
                    KeyError,
                    TypeError,
                    ValueError,
                    OverflowError,
                    json.JSONDecodeError,
                ):
                    invalid_json += 1
                    for symbol in list(run_start):
                        reset_run(symbol)
                    continue

                frames += 1
                first_ts = timestamp if first_ts is None else min(first_ts, timestamp)
                last_ts = timestamp if last_ts is None else max(last_ts, timestamp)
                if previous_ts is not None:
                    delta = (timestamp - previous_ts) / 1000
                    if delta > 0:
                        gaps.append(delta)
                        if delta > 30:
                            gap_events.append((delta, previous_ts, timestamp))
                    else:
                        non_monotonic += 1
                previous_ts = timestamp

                for symbol in args.symbols:
                    book = prices.get(symbol)
                    if not isinstance(book, dict):
                        missing_symbol[symbol] += 1
                        if len(missing_symbol_examples[symbol]) < 5:
                            missing_symbol_examples[symbol].append(timestamp)
                        reset_run(symbol)
                        continue
                    try:
                        price = str(book["price"])
                        price_value = float(price)
                        bid = float(book["bid_price"])
                        ask = float(book["ask_price"])
                        bid_size = float(book["bid_size"])
                        ask_size = float(book["ask_size"])
                        if not all(
                            math.isfinite(value)
                            for value in (price_value, bid, ask, bid_size, ask_size)
                        ):
                            raise ValueError("book values must be finite")
                    except (KeyError, TypeError, ValueError):
                        invalid_book[symbol] += 1
                        if len(invalid_book_examples[symbol]) < 5:
                            invalid_book_examples[symbol].append(timestamp)
                        reset_run(symbol)
                        continue

                    if bid > ask:
                        crossed[symbol] += 1
                    if bid_size <= 0 or ask_size <= 0:
                        nonpositive_size[symbol] += 1
                    mid = (bid + ask) / 2
                    if mid > 0:
                        spread = (ask - bid) / mid * 10_000
                        spreads_bps[symbol].append(spread)
                        current_max = max_spread_event[symbol]
                        if current_max is None or spread > current_max[0]:
                            max_spread_event[symbol] = (spread, timestamp)

                    if run_price.get(symbol) == price:
                        run_last[symbol] = timestamp
                    else:
                        finish_run(symbol)
                        run_price[symbol] = price
                        run_start[symbol] = timestamp
                        run_last[symbol] = timestamp

    for symbol in args.symbols:
        finish_run(symbol)

    print(
        f"Coverage: {utc_stamp(first_ts)} -> {utc_stamp(last_ts)}; "
        f"frames={frames}, invalid_json={invalid_json}, non_monotonic={non_monotonic}"
    )
    if gaps:
        print(
            "Tick gaps: "
            f"median={statistics.median(gaps):.3f}s, "
            f"p90={percentile(gaps, 0.90):.3f}s, "
            f"p99={percentile(gaps, 0.99):.3f}s, "
            f"max={max(gaps):.3f}s, "
            f">30s={sum(gap > 30 for gap in gaps)}, "
            f">60s={sum(gap > 60 for gap in gaps)}"
        )
    else:
        print("Tick gaps: n/a")
    for gap, start, end in sorted(gap_events, reverse=True):
        print(
            f"  gap {gap:.3f}s: {utc_stamp(start)} -> {utc_stamp(end)}"
        )

    print()
    print("| Symbol | Missing symbol | Invalid book | Crossed | Nonpositive size | Spread p50/p99/max (bps) | Longest same-price run |")
    print("|---|---:|---:|---:|---:|---:|---:|")
    for symbol in args.symbols:
        spreads = spreads_bps[symbol]
        spread_summary = (
            f"{percentile(spreads, 0.50):.3f}/"
            f"{percentile(spreads, 0.99):.3f}/"
            f"{max(spreads):.3f}"
            if spreads
            else "n/a"
        )
        run = longest[symbol]
        run_summary = (
            f"{run[0] / 60_000:.2f}m "
            f"({utc_stamp(run[1])} -> {utc_stamp(run[2])}, price={run[3]})"
            if run
            else "n/a"
        )
        print(
            f"| {symbol} | {missing_symbol[symbol]} | {invalid_book[symbol]} | "
            f"{crossed[symbol]} | "
            f"{nonpositive_size[symbol]} | {spread_summary} | {run_summary} |"
        )
        if missing_symbol_examples[symbol]:
            examples = ", ".join(
                utc_stamp(value) for value in missing_symbol_examples[symbol]
            )
            print(f"  - {symbol} missing-symbol examples: {examples}")
        if invalid_book_examples[symbol]:
            examples = ", ".join(
                utc_stamp(value) for value in invalid_book_examples[symbol]
            )
            print(f"  - {symbol} invalid-book examples: {examples}")
        if max_spread_event[symbol]:
            spread, timestamp = max_spread_event[symbol]
            print(f"  - {symbol} max spread {spread:.3f} bps at {utc_stamp(timestamp)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
