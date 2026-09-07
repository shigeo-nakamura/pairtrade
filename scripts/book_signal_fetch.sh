#!/bin/bash
# Fetch the producer's signal file from S3 into the book runtime's state
# directory (bot-strategy#937). Runs from book-signal-fetch-<instance>.timer
# on the host as the book-runtime identity. Atomic: the download lands in a
# temp file next to the target and is renamed only if it parses as JSON
# and its payload changed, so the runtime never reads a half-written or
# byte-identical re-upload as a new file.
#
#   book_signal_fetch.sh s3://bucket/debot/book/xsmom-695/signal.json /var/lib/book-runtime/xsmom-695/signal.json
set -euo pipefail

SRC=${1:?s3 uri}
DST=${2:?destination path}
DIR=$(dirname "$DST")
TMP="$DIR/.signal.fetch.$$"
trap 'rm -f "$TMP"' EXIT

if ! aws s3 cp --only-show-errors "$SRC" "$TMP"; then
  echo "book_signal_fetch: download failed: $SRC" >&2
  exit 1
fi
# Full schema-v1 check plus the payload hash (same canonical form as
# scripts/book_signal_file.py / src/book/signal.rs) before the download can
# displace the last valid local file; a file the runtime would reject must
# never replace one it accepted.
if ! SUMMARY=$(python3 - "$TMP" <<'PY'
import hashlib, json, math, sys
from datetime import datetime, timezone
def _reject_constant(token):
    raise SystemExit(f"non-standard JSON constant {token} (the runtime's parser rejects it)")
def _no_duplicate_keys(pairs):
    seen = set()
    for k, _ in pairs:
        if k in seen:
            # serde rejects a repeated struct field; Python would silently
            # keep the last one and promote a file the runtime cannot parse.
            raise SystemExit(f"duplicate JSON key {k!r}")
        seen.add(k)
    return dict(pairs)
d = json.load(
    open(sys.argv[1]),
    parse_constant=_reject_constant,
    object_pairs_hook=_no_duplicate_keys,
)
for k in ("schema_version", "producer_id", "generated_at", "as_of", "decision_key", "weights", "payload_sha256"):
    if k not in d:
        raise SystemExit(f"missing field {k}")
if isinstance(d["schema_version"], bool) or not isinstance(d["schema_version"], int) or d["schema_version"] != 1:
    raise SystemExit(f"schema_version must be the integer 1, got {d['schema_version']!r}")
for k in ("producer_id", "decision_key", "payload_sha256"):
    if not isinstance(d[k], str) or not d[k].strip():
        raise SystemExit(f"{k} must be a non-empty string")
ts = {}
for k in ("generated_at", "as_of"):
    if not isinstance(d[k], str):
        raise SystemExit(f"{k} must be a string timestamp")
    try:
        ts[k] = datetime.strptime(d[k], "%Y-%m-%dT%H:%M:%SZ")
    except ValueError as e:
        raise SystemExit(f"{k} is not a YYYY-MM-DDTHH:MM:SSZ timestamp: {e}")
if ts["as_of"] > ts["generated_at"]:
    raise SystemExit("as_of is after generated_at (look-ahead); the runtime would reject this file")
# The runtime refuses a file generated more than 60s in the future (a
# producer clock error); promoting it here would displace a usable signal
# and lose the window. Same bound, applied before the mv.
ahead = (ts["generated_at"] - datetime.now(timezone.utc).replace(tzinfo=None)).total_seconds()
if ahead > 60:
    raise SystemExit(f"generated_at is {ahead:.0f}s in the future; the runtime would reject this file")
if not isinstance(d["weights"], dict):
    raise SystemExit("weights must be a symbol -> number mapping")
for sym, v in d["weights"].items():
    if not isinstance(sym, str) or not sym.strip():
        raise SystemExit(f"bad symbol key {sym!r}")
    if isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(float(v)):
        raise SystemExit(f"weight for {sym} must be a finite number, got {v!r}")
payload = {"as_of": d["as_of"], "decision_key": d["decision_key"], "producer_id": d["producer_id"],
           "weights": {k: float(v) for k, v in d["weights"].items()}}
sha = hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
if sha != str(d["payload_sha256"]).lower():
    raise SystemExit(f"payload_sha256 mismatch: file {d['payload_sha256']} computed {sha}")
print(f"{d['decision_key']} {sha[:12]} n={len(d['weights'])}")
PY
); then
  echo "book_signal_fetch: $SRC failed schema/hash validation; keeping the current file" >&2
  exit 1
fi
if [ -f "$DST" ] && cmp -s "$TMP" "$DST"; then
  exit 0
fi
chmod 0640 "$TMP"
mv -f "$TMP" "$DST"
echo "book_signal_fetch: updated $DST ($SUMMARY)"
