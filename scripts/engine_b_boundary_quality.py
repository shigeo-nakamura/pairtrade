#!/usr/bin/env python3
"""Offline boundary preflight for Engine B; never a complete G0-2 verdict."""

import argparse
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import tempfile
from urllib.parse import urlsplit

ALIASES = ("lighter", "lighter_mainnet_context")
MAINNET = "mainnet.zklighter.elliot.ai"
HOUR = 3_600_000_000
SECOND = 1_000_000


def digest(path):
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def provenance(db):
    mappings = {}
    for row in db.execute("SELECT config_json FROM collector_manifest"):
        config = json.loads(row[0])
        for venue in config["venues"]:
            name = venue["name"]
            if name not in ALIASES:
                continue
            rest, ws = urlsplit(venue["rest_url"]), urlsplit(venue["ws_url"])
            if (rest.scheme, rest.hostname, ws.scheme, ws.hostname) != ("https", MAINNET, "wss", MAINNET):
                raise ValueError(f"{name}: unverified mainnet endpoints")
            for market in venue["markets"]:
                key = (name, market["symbol"])
                market_id = market["market_id"]
                if key in mappings and mappings[key] != market_id:
                    raise ValueError(f"{name}/{market['symbol']}: conflicting market IDs")
                mappings[key] = market_id
    if not mappings:
        raise ValueError("no verified mainnet collector manifest")
    return mappings


class Dataset:
    """Only closed copies: hashes cover the DBs actually read, never a live WAL."""
    def __init__(self, root):
        self.root = root.resolve()
        self.opened = {}
        self.inventory = {}

    def open(self, hour_us):
        hour = datetime.fromtimestamp(hour_us / SECOND, timezone.utc).strftime("%Y%m%d_%H")
        name = f"engine_b_phase0_{hour}.sqlite3"
        if name in self.opened:
            return self.opened[name]
        path = self.root / name
        if not path.exists():
            self.inventory[name] = {"missing": True}
            return None
        if any(Path(str(path) + suffix).exists() for suffix in ("-wal", "-shm")):
            raise ValueError(f"{name}: use a closed offline copy without WAL/SHM")
        sha = digest(path)
        db = sqlite3.connect(path.as_uri() + "?mode=ro&immutable=1", uri=True)
        db.row_factory = sqlite3.Row
        try:
            mapping = provenance(db)
        except Exception:
            db.close()
            raise
        self.inventory[name] = {"sha256": sha, "bytes": path.stat().st_size}
        self.opened[name] = (name, db, mapping)
        return self.opened[name]

    def verify_and_close(self):
        try:
            for name in self.opened:
                path = self.root / name
                if (digest(path) != self.inventory[name]["sha256"]
                        or any(Path(str(path) + suffix).exists() for suffix in ("-wal", "-shm"))):
                    raise ValueError(f"{name}: input changed during analysis")
        finally:
            for _, db, _ in self.opened.values():
                db.close()


def book_metrics(rows):
    sides = {"bid": [], "ask": []}
    for row in rows:
        side = row["side"]
        price, size = Decimal(row["price"]), Decimal(row["size"])
        if side not in sides or not price.is_finite() or not size.is_finite() or price <= 0 or size <= 0:
            raise ValueError("nonpositive/nonfinite price or size, or invalid side")
        sides[side].append((row["level"], price, size))
    for side, levels in sides.items():
        levels.sort()
        if not levels or [x[0] for x in levels] != list(range(len(levels))):
            raise ValueError("missing side or noncontiguous levels")
        prices = [x[1] for x in levels]
        if len(set(prices)) != len(prices) or prices != sorted(prices, reverse=side == "bid"):
            raise ValueError("duplicate or unsorted prices")
    bid, ask = sides["bid"][0][1], sides["ask"][0][1]
    if bid >= ask:
        raise ValueError("locked/crossed book")
    mid = (bid + ask) / 2
    return {"mid": str(mid), "spread_bps": str((ask - bid) / mid * 10000),
            "bid_levels": len(sides["bid"]), "ask_levels": len(sides["ask"]),
            "bid_top5_usd": str(sum((p * q for _, p, q in sides["bid"][:5]), Decimal(0))),
            "ask_top5_usd": str(sum((p * q for _, p, q in sides["ask"][:5]), Decimal(0)))}


def boundary(dataset, at_us, symbols, max_age_us, window_us):
    start, end = at_us - window_us, at_us + window_us
    sources = [dataset.open(t) for t in range(start // HOUR * HOUR, end // HOUR * HOUR + 1, HOUR)]
    missing = sum(source is None for source in sources)
    result = {"at_us": at_us, "missing_window_partitions": missing, "symbols": {}}
    for symbol in symbols:
        reasons, candidates, gaps = [], [], []
        for source in sources:
            if source is None:
                continue
            name, db, mapping = source
            tables = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            for venue in ALIASES:
                event = db.execute(
                    "SELECT * FROM book_event WHERE venue=? AND symbol=? AND ts_recv_us BETWEEN ? AND ? "
                    "AND is_complete_snapshot=1 ORDER BY ts_recv_us DESC, local_sequence DESC, book_event_id DESC LIMIT 1",
                    (venue, symbol, at_us - max_age_us, at_us),
                ).fetchone()
                if event is not None:
                    if mapping.get((venue, symbol)) != event["market_id"]:
                        reasons.append("unverified_event_market")
                    else:
                        candidates.append((event["ts_recv_us"], name, db, event))
                for table in ("data_gap", "sealed_gap_interval"):
                    if table not in tables:
                        reasons.append(f"missing_{table}_evidence")
                        continue
                    for gap in db.execute(
                        f"SELECT * FROM {table} WHERE venue=? AND (symbol=? OR symbol IS NULL) "
                        "AND channel IN ('connection','order_book') AND ts_start_us <= ? "
                        "AND (ts_end_us IS NULL OR ts_end_us >= ?)", (venue, symbol, end, start),
                    ):
                        market_id = gap["market_id"]
                        if market_id is not None and market_id != mapping.get((venue, symbol)):
                            continue
                        gaps.append({"source": name, "table": table, "venue": venue,
                                     "start_us": gap["ts_start_us"], "end_us": gap["ts_end_us"],
                                     "reason": gap["reason"]})
        row = {"reasons": reasons, "known_window_gaps": gaps, "selected": None}
        if missing:
            reasons.append("missing_window_partition")
        if gaps:
            reasons.append("known_gap_in_window")
        if not candidates:
            reasons.append("missing_or_stale_complete_snapshot")
        else:
            candidates.sort(key=lambda x: (x[0], x[1]))
            stamp, name, db, event = candidates[-1]
            tied = [c for c in candidates if c[0] == stamp]
            if len(tied) > 1:
                reasons.append("ambiguous_latest_snapshot")
            selected = {"source": name, "book_event_id": event["book_event_id"],
                        "venue": event["venue"], "market_id": event["market_id"],
                        "ts_recv_us": stamp, "age_us": at_us - stamp,
                        "exchange_sequence": event["exchange_sequence"],
                        "server_offset_us": stamp - event["ts_srv_us"] if event["ts_srv_us"] is not None else None}
            if event["exchange_sequence"] is None:
                reasons.append("missing_sequence")
            try:
                levels = db.execute("SELECT side,level,price,size FROM book_level WHERE book_event_id=? ORDER BY side,level", (event["book_event_id"],))
                selected.update(book_metrics(levels))
            except (ValueError, InvalidOperation, TypeError) as exc:
                reasons.append(f"invalid_book: {exc}")
            row["selected"] = selected
        row["reasons"] = sorted(set(reasons))
        row["boundary_preflight_pass"] = not row["reasons"]
        result["symbols"][symbol] = row
    result["boundary_preflight_pass"] = all(r["boundary_preflight_pass"] for r in result["symbols"].values())
    return result


def analyze(root, calendar_path, start, end, symbols, max_age_seconds=30, window_seconds=900):
    if not symbols or len(set(symbols)) != len(symbols):
        raise ValueError("provide distinct required symbols")
    if not 0 < max_age_seconds <= window_seconds:
        raise ValueError("require 0 < max age <= window")
    first, last = date.fromisoformat(start), date.fromisoformat(end)
    if first > last:
        raise ValueError("start must not exceed end")
    calendar_bytes = calendar_path.read_bytes()
    calendar = json.loads(calendar_bytes)
    dataset = Dataset(root)
    days = []
    try:
        day = first
        while day <= last:
            session = calendar["sessions"][day.isoformat()]
            row = {"date": day.isoformat(), "g0_2": "not_evaluated"}
            if not (session["krx_is_open"] and session["us_is_open"]):
                row["status"] = "market_closed"
            else:
                times = [session["krx_open_utc_us"], session["krx_close_utc_us"], session["us_open_utc_us"]]
                if any(type(t) is not int for t in times) or not times[0] < times[1] < times[2]:
                    raise ValueError(f"{day}: invalid boundary timestamps")
                row["boundaries"] = {label: boundary(dataset, t, symbols, max_age_seconds * SECOND, window_seconds * SECOND)
                                     for label, t in zip(("t0", "t1", "t2"), times)}
                row["status"] = "boundary_preflight_pass" if all(b["boundary_preflight_pass"] for b in row["boundaries"].values()) else "boundary_preflight_fail"
            days.append(row)
            day += timedelta(days=1)
    finally:
        dataset.verify_and_close()
    parameters = {"symbols": symbols, "start": start, "end": end,
                  "max_age_seconds": max_age_seconds, "window_seconds": window_seconds}
    evidence = {"inputs": dataset.inventory, "calendar_sha256": hashlib.sha256(calendar_bytes).hexdigest(),
                "code_sha256": digest(Path(__file__)), "parameters": parameters}
    return {"schema_version": 1, "scope": "boundary_preflight_only", "g0_2": "not_evaluated",
            "calendar_version": calendar["calendar_version"], **evidence,
            "analysis_hash": hashlib.sha256(json.dumps(evidence, sort_keys=True).encode()).hexdigest(), "days": days}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--calendar", type=Path, required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--symbols", nargs="+", required=True, help="Explicit required inputs; no implicit primary freeze")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        report = analyze(args.data_dir, args.calendar, args.start, args.end, args.symbols)
    except (OSError, ValueError, KeyError, TypeError, sqlite3.Error) as exc:
        parser.exit(2, f"analysis failed: {exc}\n")
    if args.output.resolve() == args.calendar.resolve() or args.output.suffix == ".sqlite3":
        parser.exit(2, "output must not overwrite an input database or calendar\n")
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", dir=args.output.parent, delete=False) as stream:
            temporary = Path(stream.name)
            stream.write(json.dumps(report, sort_keys=True, indent=2) + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, args.output)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
