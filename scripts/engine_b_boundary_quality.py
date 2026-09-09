#!/usr/bin/env python3
"""Offline boundary preflight for Engine B; never a complete G0-2 verdict."""

import argparse
from collections import OrderedDict
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
# The producer's own session-entry rule, copied whole rather than
# approximated: `TradingCalendar._valid_timestamp_us` in
# scripts/engine_b_phase0.py. `type(v) is int` because bool subclasses int,
# a lower bound because a pre-epoch microsecond is never a session
# boundary, and an upper bound because the collector stores these in a
# SQLite signed integer column. Reading a calendar the collector itself
# would refuse must be an input error here too, not a boundary_preflight_fail
# derived from searching 1969 (Codex, PR #311).
SQLITE_INT_MAX = 2**63 - 1


def valid_timestamp_us(value) -> bool:
    return type(value) is int and 0 <= value <= SQLITE_INT_MAX


def validate_session(day, session):
    """The collector's session-entry rules, applied whole.

    Mirrors `TradingCalendar.load` (scripts/engine_b_phase0.py) field for
    field: both flags must be booleans; a valid, ordered open/close pair is
    required on the KRX side exactly when KRX is open, and a valid US open
    exactly when the US is open. Deliberately the producer's whole rule
    rather than the subset a given run happens to read -- a calendar the
    collector would refuse to load must never produce a report, whichever
    field is wrong and whether or not this day's branch would have looked
    at it. A one-sided session (KRX open on a US holiday) short-circuits to
    `market_closed`, so validating inside that branch is how malformed
    timestamps got through (Codex, PR #311).
    """
    for flag in ("krx_is_open", "us_is_open"):
        if type(session[flag]) is not bool:
            raise ValueError(f"{day}: krx_is_open/us_is_open must be booleans")
    if session["krx_is_open"]:
        krx_open, krx_close = session["krx_open_utc_us"], session["krx_close_utc_us"]
        if not valid_timestamp_us(krx_open) or not valid_timestamp_us(krx_close):
            raise ValueError(f"{day}: invalid krx_open_utc_us/krx_close_utc_us")
        if krx_open >= krx_close:
            raise ValueError(f"{day}: krx_open_utc_us must be before krx_close_utc_us")
    if session["us_is_open"] and not valid_timestamp_us(session["us_open_utc_us"]):
        raise ValueError(f"{day}: invalid us_open_utc_us")


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


def reject_unless_closed_copy(name, path):
    """A partition must be a regular file with no WAL/SHM beside it.

    Symlinks are refused outright rather than resolved. `digest()` and SQLite
    both follow a link, but a `-wal`/`-shm` check beside the *link* looks in
    the wrong directory: a link to a live collector database then opens the
    target with `immutable=1`, ignores its real WAL, hashes identically
    before and after (writes are confined to that WAL), and yields a
    plausible passing report missing recent events or gaps. "Closed offline
    copy" is the contract; a link is not one, whatever it points at
    (Codex, PR #311). Applied on every open and again at the end, so a file
    swapped for a link mid-run is caught too.
    """
    if path.is_symlink():
        raise ValueError(f"{name}: must be a regular file, not a symlink")
    if path.exists() and not path.is_file():
        raise ValueError(f"{name}: must be a regular file")
    if any(Path(str(path) + suffix).exists() for suffix in ("-wal", "-shm")):
        raise ValueError(f"{name}: use a closed offline copy without WAL/SHM")


class Dataset:
    """Only closed copies: hashes cover the DBs actually read, never a live WAL."""

    # A full 2026-2027 calendar range touches ~1,900 hourly partitions, and
    # holding one connection open for each exhausts a typical 1,024
    # RLIMIT_NOFILE -- sqlite3.connect then fails and the CLI exits 2 on
    # exactly the multi-day range this tool exists to support. The integrity
    # guarantee never depended on the handle staying open: it comes from the
    # recorded SHA-256, which is now checked on every reopen as well as at
    # the end. So connections are a bounded LRU while the hashes and
    # manifests are retained for every partition read (Codex, PR #311).
    MAX_OPEN = 64

    def __init__(self, root, max_open=None):
        self.root = root.resolve()
        self.max_open = self.MAX_OPEN if max_open is None else max_open
        # name -> connection, least-recently-used first.
        self.opened = OrderedDict()
        self.mappings = {}
        self.inventory = {}
        self.absent = set()

    @staticmethod
    def name_for(hour_us):
        hour = datetime.fromtimestamp(hour_us / SECOND, timezone.utc).strftime("%Y%m%d_%H")
        return f"engine_b_phase0_{hour}.sqlite3"

    def open_window(self, hours_us):
        """Open every partition one boundary needs, all live at once.

        Eviction runs only after the whole window is open and protects
        exactly this set, so a bounded cache can never close a handle the
        caller is still reading from.
        """
        names = [self.name_for(hour_us) for hour_us in hours_us]
        if len(set(names)) > self.max_open:
            raise ValueError(
                f"window spans {len(set(names))} partitions, more than the {self.max_open} "
                "connections this analysis may hold open at once")
        sources = [self._open(name) for name in names]
        self._evict(set(names))
        return sources

    def open(self, hour_us):
        source = self._open(self.name_for(hour_us))
        self._evict(set())
        return source

    def _open(self, name):
        if name in self.absent:
            return None
        if name in self.opened:
            self.opened.move_to_end(name)
            return (name, self.opened[name], self.mappings[name])
        path = self.root / name
        reject_unless_closed_copy(name, path)
        if not path.exists():
            self.inventory[name] = {"missing": True}
            self.absent.add(name)
            return None
        sha = digest(path)
        known = self.inventory.get(name, {}).get("sha256")
        if known is not None and known != sha:
            # A reopen is a second chance to catch what verify_and_close
            # checks at the end, and it catches it earlier: everything read
            # after this point would otherwise come from a different file
            # than the one the report's hash names.
            raise ValueError(f"{name}: input changed during analysis")
        db = sqlite3.connect(path.as_uri() + "?mode=ro&immutable=1", uri=True)
        db.row_factory = sqlite3.Row
        try:
            mapping = provenance(db)
        except Exception:
            db.close()
            raise
        self.inventory[name] = {"sha256": sha, "bytes": path.stat().st_size}
        self.mappings[name] = mapping
        self.opened[name] = db
        return (name, db, mapping)

    def _evict(self, protected):
        for name in [n for n in self.opened if n not in protected]:
            if len(self.opened) <= self.max_open:
                break
            self.opened.pop(name).close()

    def verify_and_close(self):
        try:
            # Every partition that was read, not merely the ones still
            # cached: an evicted file must not be able to change unnoticed.
            for name, entry in self.inventory.items():
                if entry.get("missing"):
                    continue
                path = self.root / name
                reject_unless_closed_copy(name, path)
                if digest(path) != entry["sha256"]:
                    raise ValueError(f"{name}: input changed during analysis")
        finally:
            for db in self.opened.values():
                db.close()
            self.opened.clear()


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
    sources = dataset.open_window(range(start // HOUR * HOUR, end // HOUR * HOUR + 1, HOUR))
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
    # Taken before anything is read, and checked again at the end. A
    # checkout or deployment that replaces this file mid-run leaves Python
    # executing the already-loaded code while an end-of-run digest would
    # name the new file, so the report and its analysis_hash would claim
    # provenance for code that did not produce them -- the same failure the
    # database inputs are already protected from (Codex, PR #311).
    code_sha256 = digest(Path(__file__))
    first, last = date.fromisoformat(start), date.fromisoformat(end)
    if first > last:
        raise ValueError("start must not exceed end")
    calendar_bytes = calendar_path.read_bytes()
    calendar = json.loads(calendar_bytes)
    # The whole calendar, before the range is applied: TradingCalendar.load
    # validates every entry in `sessions` and refuses the file as a unit, so
    # one malformed session outside --start/--end is still a calendar the
    # collector would not load -- and the invariant is that such a calendar
    # never produces a report, not that the days this run reads are clean
    # (Codex, PR #311).
    for session_day, session in calendar["sessions"].items():
        validate_session(session_day, session)
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
                # What the producer does not check: the two markets'
                # boundaries must be usable as t0 < t1 < t2.
                if not times[0] < times[1] < times[2]:
                    raise ValueError(f"{day}: boundary timestamps out of order")
                # ... and they must belong to this session date. Three
                # ordered timestamps from another day pass every field
                # check, and the prices found at them would be recorded
                # under the requested date and contaminate date-level
                # analysis. Every boundary in the checked-in calendar falls
                # on its own UTC date (KRX opens at 00:00 UTC), so this is
                # an invariant of the data, not a new constraint on it
                # (Codex, PR #311).
                if any(datetime.fromtimestamp(t / SECOND, timezone.utc).date() != day for t in times):
                    raise ValueError(f"{day}: boundary timestamps do not fall on the session date")
                row["boundaries"] = {label: boundary(dataset, t, symbols, max_age_seconds * SECOND, window_seconds * SECOND)
                                     for label, t in zip(("t0", "t1", "t2"), times)}
                row["status"] = "boundary_preflight_pass" if all(b["boundary_preflight_pass"] for b in row["boundaries"].values()) else "boundary_preflight_fail"
            days.append(row)
            day += timedelta(days=1)
    finally:
        dataset.verify_and_close()
    if digest(Path(__file__)) != code_sha256:
        raise ValueError(f"{Path(__file__).name}: analysis code changed during analysis")
    parameters = {"symbols": symbols, "start": start, "end": end,
                  "max_age_seconds": max_age_seconds, "window_seconds": window_seconds}
    evidence = {"inputs": dataset.inventory, "calendar_sha256": hashlib.sha256(calendar_bytes).hexdigest(),
                "code_sha256": code_sha256, "parameters": parameters}
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
