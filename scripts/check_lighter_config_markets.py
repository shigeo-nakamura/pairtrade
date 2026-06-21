#!/usr/bin/env python3
"""Fail fast when a pairtrade Lighter config references unknown markets.

This intentionally uses only the Python standard library: production hosts
already have python3 for AWS tooling, but should not need PyYAML just to start
an observe-only collector.
"""

from __future__ import annotations

import argparse
import difflib
import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Iterable


DEFAULT_TIMEOUT_SECS = 15.0
ORDER_BOOK_DETAILS_PATH = "/api/v1/orderBookDetails"


def normalize_symbol(symbol: str) -> str:
    upper = symbol.strip().upper()
    if "/" in upper:
        return upper

    normalized = upper
    for suffix in ("-PERP", "_PERP", ".PERP", "-USD", "_USD", "-USDC", "_USDC"):
        normalized = normalized.replace(suffix, "")
    return normalized


def clean_scalar(value: str) -> str:
    value = value.split("#", 1)[0].strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        value = value[1:-1]
    return value.strip()


def parse_inline_list(value: str) -> list[str]:
    value = clean_scalar(value)
    if not (value.startswith("[") and value.endswith("]")):
        return []
    inner = value[1:-1].strip()
    if not inner:
        return []
    return [clean_scalar(part) for part in inner.split(",") if clean_scalar(part)]


def load_config_subset(path: Path) -> tuple[str, list[str]]:
    rest_endpoint: str | None = None
    pairs: list[str] = []
    in_universe_pairs = False

    for raw in path.read_text(encoding="utf-8").splitlines():
        stripped = raw.strip()
        if not stripped or stripped.startswith("#"):
            continue

        if in_universe_pairs:
            if stripped.startswith("- "):
                pair = clean_scalar(stripped[2:])
                if pair:
                    pairs.append(pair)
                continue
            if not raw.startswith((" ", "\t")):
                in_universe_pairs = False

        if stripped.startswith("rest_endpoint:"):
            rest_endpoint = clean_scalar(stripped.split(":", 1)[1])
            continue

        if stripped.startswith("universe_pairs:"):
            in_universe_pairs = True
            inline = parse_inline_list(stripped.split(":", 1)[1])
            pairs.extend(inline)

    if not rest_endpoint:
        raise ValueError(f"{path}: missing rest_endpoint")
    if not pairs:
        raise ValueError(f"{path}: missing universe_pairs")
    return rest_endpoint, pairs


def legs_from_pairs(pairs: Iterable[str]) -> list[str]:
    legs: set[str] = set()
    invalid: list[str] = []
    for pair in pairs:
        if "/" not in pair:
            invalid.append(pair)
            continue
        base, quote = pair.split("/", 1)
        base = normalize_symbol(base)
        quote = normalize_symbol(quote)
        if not base or not quote:
            invalid.append(pair)
            continue
        legs.add(base)
        legs.add(quote)

    if invalid:
        raise ValueError(f"invalid universe_pairs entries: {', '.join(invalid)}")
    return sorted(legs)


def load_order_book_details(
    rest_endpoint: str,
    timeout_secs: float,
    markets_json: Path | None,
) -> dict:
    if markets_json is not None:
        return json.loads(markets_json.read_text(encoding="utf-8"))

    url = rest_endpoint.rstrip("/") + ORDER_BOOK_DETAILS_PATH
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "pairtrade-lighter-market-preflight/1.0"},
    )
    with urllib.request.urlopen(req, timeout=timeout_secs) as resp:
        return json.loads(resp.read().decode("utf-8"))


def available_symbols(payload: dict) -> set[str]:
    details = payload.get("order_book_details")
    if not isinstance(details, list):
        raise ValueError("orderBookDetails response missing order_book_details list")

    symbols: set[str] = set()
    for item in details:
        if not isinstance(item, dict):
            continue
        symbol = normalize_symbol(str(item.get("symbol", "")))
        if symbol:
            symbols.add(symbol)
    return symbols


def format_missing(missing: list[str], available: set[str]) -> str:
    lines = [", ".join(missing)]
    available_list = sorted(available)
    for symbol in missing:
        matches = difflib.get_close_matches(symbol, available_list, n=3, cutoff=0.5)
        if matches:
            lines.append(f"  {symbol}: closest={', '.join(matches)}")
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "config",
        nargs="?",
        default=os.environ.get("PAIRTRADE_CONFIG_PATH"),
        help="pairtrade YAML config path (defaults to PAIRTRADE_CONFIG_PATH)",
    )
    parser.add_argument(
        "--timeout-secs",
        type=float,
        default=float(os.environ.get("LIGHTER_MARKET_PREFLIGHT_TIMEOUT_SECS", DEFAULT_TIMEOUT_SECS)),
        help="HTTP timeout for orderBookDetails",
    )
    parser.add_argument(
        "--markets-json",
        type=Path,
        help="read a saved orderBookDetails payload instead of calling Lighter",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not args.config:
        print("ERROR: config path not supplied and PAIRTRADE_CONFIG_PATH is empty", file=sys.stderr)
        return 2

    config_path = Path(args.config)
    try:
        rest_endpoint, pairs = load_config_subset(config_path)
        required = legs_from_pairs(pairs)
        payload = load_order_book_details(rest_endpoint, args.timeout_secs, args.markets_json)
        available = available_symbols(payload)
    except (OSError, ValueError, json.JSONDecodeError, urllib.error.URLError) as exc:
        print(f"ERROR: Lighter market preflight failed: {exc}", file=sys.stderr)
        return 1

    missing = [symbol for symbol in required if symbol not in available]
    if missing:
        print(
            "ERROR: config references Lighter symbols absent from orderBookDetails: "
            + format_missing(missing, available),
            file=sys.stderr,
        )
        return 1

    print(f"Lighter market preflight OK: {len(required)} symbols resolved")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
