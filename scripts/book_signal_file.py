#!/usr/bin/env python3
"""Reference writer for the book runtime signal file (schema v1).

`docs/book-runtime.md` §3 is the contract; `src/book/signal.rs` is the
validator. Producers (XSMOM, Engine B) import `write_signal` so the payload
hash is computed exactly the way the runtime recomputes it:

    sha256(json.dumps({"as_of", "decision_key", "producer_id", "weights"},
                      sort_keys=True, separators=(",", ":")))

Timestamps are rendered as `YYYY-MM-DDTHH:MM:SSZ` (UTC, second precision).
Weights are plain floats; `repr(float)` formatting on both sides agrees for
the magnitudes used here (the Rust side normalises `1` → `1.0`).

Usage as a CLI (mostly for replay fixtures):

    book_signal_file.py --out signals/2026-07-03.json --producer p \
        --decision-key 2026-07-03 --as-of 2026-07-03T00:00:00Z \
        --generated-at 2026-07-03T00:20:00Z BTC=0.5 DOT=-0.5
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import tempfile
from datetime import datetime, timezone

SCHEMA_VERSION = 1
TS_FMT = "%Y-%m-%dT%H:%M:%SZ"


def fmt_ts(ts: datetime) -> str:
    if ts.tzinfo is None:
        raise ValueError("timestamps must be timezone-aware (UTC)")
    return ts.astimezone(timezone.utc).strftime(TS_FMT)


def parse_ts(s: str) -> datetime:
    return datetime.strptime(s, TS_FMT).replace(tzinfo=timezone.utc)


def canonical_payload(producer_id: str, as_of: str, decision_key: str, weights: dict) -> str:
    payload = {
        "as_of": as_of,
        "decision_key": decision_key,
        "producer_id": producer_id,
        "weights": {k: float(v) for k, v in weights.items()},
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def payload_sha256(producer_id: str, as_of: str, decision_key: str, weights: dict) -> str:
    return hashlib.sha256(
        canonical_payload(producer_id, as_of, decision_key, weights).encode("utf-8")
    ).hexdigest()


def build_signal(
    producer_id: str,
    generated_at: datetime,
    as_of: datetime,
    decision_key: str,
    weights: dict,
    meta: dict | None = None,
) -> dict:
    for sym, w in weights.items():
        if not isinstance(sym, str) or not sym:
            raise ValueError(f"bad symbol {sym!r}")
        w = float(w)
        if w != w or w in (float("inf"), float("-inf")):
            raise ValueError(f"non-finite weight for {sym}")
    as_of_s = fmt_ts(as_of)
    gen_s = fmt_ts(generated_at)
    if as_of > generated_at:
        raise ValueError("as_of must not be after generated_at (look-ahead)")
    return {
        "schema_version": SCHEMA_VERSION,
        "producer_id": producer_id,
        "generated_at": gen_s,
        "as_of": as_of_s,
        "decision_key": decision_key,
        "weights": {k: float(v) for k, v in sorted(weights.items())},
        "meta": meta or {},
        "payload_sha256": payload_sha256(producer_id, as_of_s, decision_key, weights),
    }


def write_signal(path: str, signal: dict) -> None:
    """Atomic write (tmp + rename in the same directory) so the runtime
    never reads a half-written file."""
    d = os.path.dirname(path) or "."
    os.makedirs(d, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".signal.", suffix=".tmp", dir=d)
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(signal, f, sort_keys=True, indent=1)
            f.write("\n")
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", required=True)
    ap.add_argument("--producer", required=True)
    ap.add_argument("--decision-key", required=True)
    ap.add_argument("--as-of", required=True, help="YYYY-MM-DDTHH:MM:SSZ")
    ap.add_argument("--generated-at", default=None, help="YYYY-MM-DDTHH:MM:SSZ (default: now)")
    ap.add_argument("weights", nargs="*", help="SYMBOL=weight")
    a = ap.parse_args()
    weights = {}
    for w in a.weights:
        sym, _, val = w.partition("=")
        weights[sym] = float(val)
    generated_at = parse_ts(a.generated_at) if a.generated_at else datetime.now(timezone.utc)
    sig = build_signal(a.producer, generated_at, parse_ts(a.as_of), a.decision_key, weights)
    write_signal(a.out, sig)
    print(sig["payload_sha256"])


if __name__ == "__main__":
    main()
