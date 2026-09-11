#!/usr/bin/env python3
"""Print the XSMOM candidate pool and check a book config covers it.

The shadow watcher (`xsmom_shadow_695.py`, bot-strategy#695) re-screens its
universe point-in-time at every rebalance: Lighter markets that are active,
not hidden, not reduce-only and not session-gated, intersected with the
Binance USDT perpetuals whose underlying is a COIN, then filtered on that
day's liquidity (24h quote volume, spread, depth). The liquidity filters
move every rebalance; the pool they select from only moves on a listing or
delisting. So the deployed `universe.symbols` must cover the *pool*, not
the last few books: covering the books alone missed the 2026-09-11 decision
(ARB and OP crossed the volume bar that morning and the runtime skipped the
key, bot-strategy#941).

    xsmom_universe_pool.py                       # pool as a YAML list
    xsmom_universe_pool.py --check CONFIG        # exit 1 naming pool symbols
                                                 # CONFIG does not list

Reads only public endpoints; no credentials. The same active/hidden/
reduce-only/trading-hours predicate as the watcher, kept in one place so
the two cannot drift.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from xsmom_signal_producer import universe_from_config  # noqa: E402

LIGHTER = "https://mainnet.zklighter.elliot.ai/api/v1"
FAPI = "https://fapi.binance.com"
UA = {"User-Agent": "xsmom-universe-pool"}


def http_json(url: str):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())


def lighter_tradable(order_book_details: list) -> set:
    """Symbols the watcher's screen can see: same predicate as
    `xsmom_shadow_695.lighter_markets`."""
    out = set()
    for m in order_book_details:
        cfg = m.get("market_config") or {}
        if (m.get("status") != "active" or cfg.get("hidden")
                or cfg.get("force_reduce_only") or cfg.get("trading_hours")):
            continue
        out.add(m["symbol"])
    return out


def binance_coin_bases(exchange_info: dict) -> set:
    """Base assets of Binance USDT perpetuals with a COIN underlying: same
    predicate as `xsmom_shadow_695.binance_coin_bases`."""
    return {s["baseAsset"] for s in exchange_info["symbols"]
            if s.get("quoteAsset") == "USDT" and s.get("status") == "TRADING"
            and s.get("contractType") == "PERPETUAL"
            and s.get("underlyingType") == "COIN"}


def pool(order_book_details: list, exchange_info: dict) -> list:
    return sorted(lighter_tradable(order_book_details)
                  & binance_coin_bases(exchange_info))


def fetch_pool() -> list:
    obd = http_json(f"{LIGHTER}/orderBookDetails")["order_book_details"]
    info = http_json(f"{FAPI}/fapi/v1/exchangeInfo")
    return pool(obd, info)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--check", metavar="CONFIG", default=None,
                    help="book config whose universe.symbols must cover the pool")
    a = ap.parse_args(argv)
    syms = fetch_pool()
    if a.check is None:
        for s in syms:
            print(f"    - {s}")
        return 0
    have = universe_from_config(a.check)
    missing = [s for s in syms if s not in have]
    extra = sorted(have - set(syms))
    if extra:
        # Not an error: a delisted or reduce-only market stays in the
        # config so a leg the book still holds keeps its price feed.
        print(f"note: {len(extra)} configured symbol(s) outside the current "
              f"pool: {', '.join(extra)}", file=sys.stderr)
    if missing:
        print(f"{a.check}: {len(missing)} pool symbol(s) not in universe.symbols: "
              f"{', '.join(missing)}", file=sys.stderr)
        return 1
    print(f"{a.check}: universe.symbols covers all {len(syms)} pool symbols")
    return 0


if __name__ == "__main__":
    sys.exit(main())
