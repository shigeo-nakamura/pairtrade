#!/usr/bin/env python3
"""
Variational DEX stats shadow logger.

Polls Variational Omni's public market-data endpoint and appends a filtered
JSONL snapshot per poll. Used to build an offline dataset for comparing
Variational quote quality / funding / OI against Lighter ahead of Variational's
trading API GA, so we can decide whether the connector work is worth doing as
soon as the trading API becomes available.

Usage:
    python3 variational_stats_collector.py

Environment:
    VARIATIONAL_INTERVAL_SECS  Poll interval seconds (default: 10)
    VARIATIONAL_DATA_DIR       Output directory (default: /opt/variational-shadow)
    VARIATIONAL_API_URL        Base URL (default: https://omni-client-api.prod.ap-northeast-1.variational.io)
    VARIATIONAL_SYMBOLS        Comma-separated tickers to keep (default: BTC,ETH,SOL)

Output: variational_stats_YYYY-MM-DD.jsonl (one line per poll, UTC date)

Rate limits (per Variational docs):
    Per IP:   10 req / 10 s
    Global:   1000 req / min
    Quote cache may be up to 600 s on the server side.

See: https://github.com/shigeo-nakamura/bot-strategy/issues/365
"""
import json
import logging
import os
import signal
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

INTERVAL_SECS = int(os.getenv("VARIATIONAL_INTERVAL_SECS", "10"))
DATA_DIR = os.getenv("VARIATIONAL_DATA_DIR", "/opt/variational-shadow")
API_URL = os.getenv(
    "VARIATIONAL_API_URL",
    "https://omni-client-api.prod.ap-northeast-1.variational.io",
).rstrip("/")
STATS_PATH = "/metadata/stats"
SYMBOLS = [s.strip() for s in os.getenv("VARIATIONAL_SYMBOLS", "BTC,ETH,SOL").split(",") if s.strip()]

HTTP_TIMEOUT_SECS = 8
USER_AGENT = "variational-shadow-collector/1.0"

shutdown = False


def handle_signal(signum, _frame):
    global shutdown
    logger.info("received signal %s, shutting down", signum)
    shutdown = True


def fetch_stats():
    req = urllib.request.Request(
        API_URL + STATS_PATH,
        headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT_SECS) as resp:
        return json.load(resp)


def filter_snapshot(payload, symbols):
    listings_idx = {l.get("ticker"): l for l in payload.get("listings", [])}
    kept = {}
    for tk in symbols:
        l = listings_idx.get(tk)
        if l is None:
            continue
        kept[tk] = {
            "mark_price": l.get("mark_price"),
            "volume_24h": l.get("volume_24h"),
            "open_interest": l.get("open_interest"),
            "funding_rate": l.get("funding_rate"),
            "funding_interval_s": l.get("funding_interval_s"),
            "base_spread_bps": l.get("base_spread_bps"),
            "quotes": l.get("quotes"),
        }
    return {
        "summary": {
            "total_volume_24h": payload.get("total_volume_24h"),
            "cumulative_volume": payload.get("cumulative_volume"),
            "tvl": payload.get("tvl"),
            "open_interest": payload.get("open_interest"),
            "num_markets": payload.get("num_markets"),
            "loss_refund": payload.get("loss_refund"),
        },
        "listings": kept,
        "missing": [tk for tk in symbols if tk not in kept],
    }


def output_path_for(now_utc):
    return os.path.join(DATA_DIR, f"variational_stats_{now_utc.strftime('%Y-%m-%d')}.jsonl")


def main():
    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)
    os.makedirs(DATA_DIR, exist_ok=True)
    logger.info(
        "variational shadow collector start: url=%s interval=%ss symbols=%s out=%s",
        API_URL + STATS_PATH, INTERVAL_SECS, SYMBOLS, DATA_DIR,
    )

    fail_streak = 0
    while not shutdown:
        loop_start = time.monotonic()
        now_utc = datetime.now(timezone.utc)
        record = {"ts_utc": now_utc.isoformat().replace("+00:00", "Z")}
        try:
            payload = fetch_stats()
            record.update(filter_snapshot(payload, SYMBOLS))
            fail_streak = 0
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, ValueError) as e:
            fail_streak += 1
            record["error"] = f"{type(e).__name__}: {e}"
            if fail_streak == 1 or fail_streak % 10 == 0:
                logger.warning("fetch failed (streak=%d): %s", fail_streak, record["error"])

        try:
            with open(output_path_for(now_utc), "a", encoding="utf-8") as f:
                f.write(json.dumps(record, separators=(",", ":")) + "\n")
        except OSError as e:
            logger.error("write failed: %s", e)

        elapsed = time.monotonic() - loop_start
        sleep_for = max(0.0, INTERVAL_SECS - elapsed)
        slept = 0.0
        while slept < sleep_for and not shutdown:
            step = min(0.5, sleep_for - slept)
            time.sleep(step)
            slept += step

    logger.info("variational shadow collector exit")


if __name__ == "__main__":
    sys.exit(main())
