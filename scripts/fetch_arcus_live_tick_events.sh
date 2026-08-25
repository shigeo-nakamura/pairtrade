#!/usr/bin/env bash
# Fetch immutable daily Arcus event segments and verify them before replay.
set -euo pipefail
umask 077

if [ "$#" -ne 3 ]; then
  echo "usage: $0 START_DATE END_DATE OUT_JSONL" >&2
  exit 64
fi
START_DATE=$1
END_DATE=$2
OUT_JSONL=$3
VERIFY_SCRIPT="${VERIFY_SCRIPT:-$(cd "$(dirname "$0")" && pwd)/arcus_live_tick_event_stream.py}"
S3_BUCKET="${S3_BUCKET:-debot-dashboard}"
S3_PREFIX="${S3_PREFIX:-arcus-archive/live-tick-events/debot-arcus}"

DATES=$(python3 - "$START_DATE" "$END_DATE" <<'PY'
import sys
from datetime import datetime, timedelta
try:
    current = datetime.strptime(sys.argv[1], "%Y-%m-%d")
    end = datetime.strptime(sys.argv[2], "%Y-%m-%d")
except ValueError as error:
    raise SystemExit(f"ERROR: invalid date: {error}")
if end < current:
    raise SystemExit("ERROR: END_DATE must not precede START_DATE")
while current <= end:
    print(current.strftime("%Y-%m-%d"))
    current += timedelta(days=1)
PY
)

mkdir -p "$(dirname "$OUT_JSONL")"
if [ -L "$OUT_JSONL" ] || [ -L "$OUT_JSONL.manifests.jsonl" ]; then
  echo "ERROR: refusing to replace symlink output" >&2
  exit 1
fi
WORK=$(mktemp -d)
OUT_STAGE="$OUT_JSONL.$$.new"
MANIFEST_STAGE="$OUT_JSONL.manifests.jsonl.$$.new"
trap 'rm -rf "$WORK"; rm -f "$OUT_STAGE" "$MANIFEST_STAGE"' EXIT
mkdir -p "$WORK/segments" "$WORK/manifests"

for day in $DATES; do
  prefix="$S3_PREFIX/${day:0:4}/${day:5:2}"
  aws s3api get-object \
    --bucket "$S3_BUCKET" \
    --key "$prefix/$day.events.jsonl.gz" \
    "$WORK/$day.events.jsonl.gz" >/dev/null
  aws s3api get-object \
    --bucket "$S3_BUCKET" \
    --key "$prefix/$day.manifest.json" \
    "$WORK/manifests/$day.json" >/dev/null

  python3 - "$WORK/$day.events.jsonl.gz" "$WORK/manifests/$day.json" "$day" <<'PY'
import hashlib
import json
import sys
archive, manifest_path, expected_day = sys.argv[1:]
with open(manifest_path, encoding="utf-8") as source:
    manifest = json.load(source)
with open(archive, "rb") as source:
    digest = "sha256:" + hashlib.sha256(source.read()).hexdigest()
if manifest.get("date_utc") != expected_day:
    raise SystemExit("ERROR: manifest date mismatch")
if manifest.get("gzip_sha256") != digest:
    raise SystemExit("ERROR: compressed archive hash mismatch")
if manifest.get("storage", {}).get("private") is not True:
    raise SystemExit("ERROR: manifest does not assert private storage")
PY
  gzip -dc "$WORK/$day.events.jsonl.gz" > "$WORK/segments/$day.jsonl"
  python3 "$VERIFY_SCRIPT" \
    "$WORK/segments/$day.jsonl" \
    --manifest-out "$WORK/$day.verified.json" >/dev/null
  python3 - "$WORK/segments/$day.jsonl" \
    "$WORK/manifests/$day.json" "$WORK/$day.verified.json" <<'PY'
import hashlib
import json
import sys
segment, archived_path, verified_path = sys.argv[1:]
with open(archived_path, encoding="utf-8") as source:
    archived = json.load(source)
with open(verified_path, encoding="utf-8") as source:
    verified = json.load(source)
with open(segment, "rb") as source:
    raw_sha = "sha256:" + hashlib.sha256(source.read()).hexdigest()
for key in (
    "records", "first_sequence", "last_sequence", "first_chain_sha256",
    "last_chain_sha256", "first_previous_chain_sha256", "stream_sha256",
):
    if archived.get(key) != verified.get(key):
        raise SystemExit(f"ERROR: archived manifest differs on {key}")
if archived.get("raw_sha256") != raw_sha or verified.get("stream_sha256") != raw_sha:
    raise SystemExit("ERROR: raw stream hash mismatch")
PY
done

python3 - "$WORK/segments" "$WORK/combined.jsonl" <<'PY'
import os
import sys
source_dir, output = sys.argv[1:]
with open(output, "wb") as destination:
    for name in sorted(os.listdir(source_dir)):
        with open(os.path.join(source_dir, name), "rb") as source:
            destination.write(source.read())
PY

mapfile -t SEGMENTS < <(find "$WORK/segments" -maxdepth 1 -type f -name '*.jsonl' | sort)
python3 "$VERIFY_SCRIPT" "${SEGMENTS[@]}" >/dev/null
python3 - "$WORK/manifests" "$WORK/manifests.jsonl" <<'PY'
import json
import os
import sys
source_dir, output = sys.argv[1:]
with open(output, "w", encoding="utf-8") as destination:
    for name in sorted(os.listdir(source_dir)):
        with open(os.path.join(source_dir, name), encoding="utf-8") as source:
            destination.write(json.dumps(json.load(source), sort_keys=True) + "\n")
PY
install -m 0600 "$WORK/combined.jsonl" "$OUT_STAGE"
install -m 0600 "$WORK/manifests.jsonl" "$MANIFEST_STAGE"
mv -f "$OUT_STAGE" "$OUT_JSONL"
mv -f "$MANIFEST_STAGE" "$OUT_JSONL.manifests.jsonl"
echo "[fetch_arcus_events] verified $START_DATE..$END_DATE -> $OUT_JSONL"
