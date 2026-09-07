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
import hashlib, json, sys
d = json.load(open(sys.argv[1]))
for k in ("schema_version", "producer_id", "generated_at", "as_of", "decision_key", "weights", "payload_sha256"):
    if k not in d:
        raise SystemExit(f"missing field {k}")
if d["schema_version"] != 1:
    raise SystemExit(f"schema_version {d['schema_version']} != 1")
if not isinstance(d["weights"], dict) or not all(isinstance(v, (int, float)) and not isinstance(v, bool) for v in d["weights"].values()):
    raise SystemExit("weights must be a symbol -> number mapping")
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
