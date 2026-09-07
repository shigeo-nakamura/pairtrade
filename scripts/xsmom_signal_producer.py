#!/usr/bin/env python3
"""XSMOM signal producer for the book runtime (bot-strategy#937 / #695).

Turns the shadow watcher's committed rebalance row (`~/bot/logs/xsmom_shadow/
ledger.jsonl`, written by `xsmom_shadow_695.py`, frozen cell
L28_H5_q20_riskadj) into a schema-v1 signal file for `book-runtime`. No
signal logic lives here: the universe screen, ranking, and quantile
membership are exactly the shadow book's, so the DRY_RUN book on the host
tracks the pre-registered shadow track 1:1.

    weight[sym] = side * notional / gross

One decision per rebalance date (`decision_key = YYYY-MM-DD`, the runtime's
`interval_days` grid anchored 2026-07-03 / every 5 days). On a date with no
rebalance row nothing is written (exit 0) -- the runtime skips the key at
the end of its window. A rebalance row whose date is off the grid, or whose
weights violate the runtime caps, is refused here (exit 2) so a bad book
never reaches the host silently.

Run hourly from cron right after the watcher (`25 * * * *`); re-running on
the same date rewrites an identical payload (hash unchanged, only
`generated_at` moves), which the runtime treats as the same decision.

    xsmom_signal_producer.py --out ~/bot/logs/xsmom_shadow/signal.json \
        --s3-uri s3://debot-dashboard/debot/book/xsmom-695/signal.json
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import date, datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import book_signal_file as bsf  # noqa: E402

DEFAULT_LEDGER = os.path.expanduser("~/bot/logs/xsmom_shadow/ledger.jsonl")
PRODUCER_ID = "xsmom_695_L28_H5_q20_riskadj"
ANCHOR = date(2026, 7, 3)
EVERY_DAYS = 5
GROSS_USD = 1000.0
MAX_SYMBOL_WEIGHT = 0.15
NET_TOLERANCE = 0.05


def on_grid(d: date, anchor: date = ANCHOR, every: int = EVERY_DAYS) -> bool:
    delta = (d - anchor).days
    return delta >= 0 and delta % every == 0


def load_rebalance_rows(path: str) -> list[dict]:
    rows = []
    with open(path) as f:
        for i, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                r = json.loads(line)
            except json.JSONDecodeError as e:
                raise SystemExit(f"{path}:{i}: bad JSON: {e}")
            if r.get("type") == "rebalance":
                rows.append(r)
    return rows


def weights_from_book(book: dict, gross: float) -> dict:
    out = {}
    for sym, leg in book.items():
        side = int(leg["side"])
        notional = float(leg["notional"])
        if side not in (1, -1):
            raise SystemExit(f"{sym}: side must be +1/-1, got {side}")
        if notional < 0:
            raise SystemExit(f"{sym}: negative notional {notional}")
        if notional == 0:
            continue
        out[sym] = side * notional / gross
    return out


def check_caps(weights: dict, max_symbol_weight: float, net_tolerance: float) -> None:
    gross = sum(abs(w) for w in weights.values())
    net = sum(weights.values())
    for sym, w in weights.items():
        if abs(w) > max_symbol_weight + 1e-12:
            raise SystemExit(f"{sym}: |weight| {w:.6f} > max_symbol_weight {max_symbol_weight}")
    if gross > 1.0 + 1e-9:
        raise SystemExit(f"sum |w| = {gross:.6f} > 1")
    if abs(net) > net_tolerance + 1e-12:
        raise SystemExit(f"|net| = {abs(net):.6f} > net_tolerance {net_tolerance}")


def build(row: dict, gross: float, generated_at: datetime, producer_id: str = PRODUCER_ID) -> dict:
    d = date.fromisoformat(row["date"])
    weights = weights_from_book(row.get("book", {}), gross)
    check_caps(weights, MAX_SYMBOL_WEIGHT, NET_TOLERANCE)
    as_of = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
    meta = {
        "source": "xsmom_shadow_695 ledger rebalance row",
        "ledger_ts": row.get("ts"),
        "n_eligible": row.get("n_eligible"),
        "k": row.get("k"),
        "n_positions": len(weights),
        "gross_usd": round(sum(abs(w) for w in weights.values()) * gross, 2),
        "net_usd": round(sum(weights.values()) * gross, 2),
        "gross_reference_usd": gross,
    }
    return bsf.build_signal(producer_id, generated_at, as_of, d.isoformat(), weights, meta)


def upload(path: str, s3_uri: str) -> None:
    subprocess.run(
        ["aws", "s3", "cp", "--only-show-errors", "--content-type", "application/json",
         "--cache-control", "max-age=60", path, s3_uri],
        check=True,
    )


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--ledger", default=DEFAULT_LEDGER)
    ap.add_argument("--out", required=True, help="local signal.json path (written atomically)")
    ap.add_argument("--s3-uri", default=None, help="optional s3://bucket/key to upload the file to")
    ap.add_argument("--date", default=None, help="decision date YYYY-MM-DD (default: today UTC)")
    ap.add_argument("--gross", type=float, default=GROSS_USD)
    ap.add_argument("--producer-id", default=PRODUCER_ID)
    ap.add_argument("--allow-off-grid", action="store_true",
                    help="emit even if the date is not on the anchor+5d grid (the runtime will reject it)")
    a = ap.parse_args()

    today = date.fromisoformat(a.date) if a.date else datetime.now(timezone.utc).date()
    rows = load_rebalance_rows(a.ledger)
    row = next((r for r in reversed(rows) if r.get("date") == today.isoformat()), None)
    if row is None:
        last = rows[-1]["date"] if rows else None
        print(f"no rebalance row for {today} (last: {last}); nothing written")
        return 0
    if not on_grid(today) and not a.allow_off_grid:
        print(f"refusing: {today} is not on the {ANCHOR}+{EVERY_DAYS}d grid", file=sys.stderr)
        return 2
    sig = build(row, a.gross, datetime.now(timezone.utc), a.producer_id)
    bsf.write_signal(a.out, sig)
    print(f"wrote {a.out} key={sig['decision_key']} n={len(sig['weights'])} "
          f"gross=${sig['meta']['gross_usd']} net=${sig['meta']['net_usd']} sha={sig['payload_sha256'][:12]}")
    if a.s3_uri:
        upload(a.out, a.s3_uri)
        print(f"uploaded to {a.s3_uri}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
