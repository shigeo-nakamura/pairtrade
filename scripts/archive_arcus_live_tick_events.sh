#!/usr/bin/env bash
# Verify and immutably archive all closed UTC-day Arcus live-tick segments.
# An explicit YYYY-MM-DD limits the operation to one day for repair/testing.
set -euo pipefail

if [ "$#" -gt 1 ]; then
  echo "usage: $0 [YYYY-MM-DD]" >&2
  exit 64
fi

STATE_DIR="${STATE_DIR:-/var/lib/debot-arcus/spot-execute-once}"
STREAM_DIR="${STREAM_DIR:-$STATE_DIR/live-tick-events}"
if [ "$#" -eq 0 ]; then
  if [ ! -d "$STREAM_DIR" ]; then
    echo "[archive_arcus_events] stream not initialized; no closed segments to archive"
    exit 0
  fi
  LAST_CLOSED=$(date -u -d 'yesterday' +%Y-%m-%d)
  CLOSED_SEGMENTS=0
  while IFS= read -r segment; do
    day=$(basename "$segment" .jsonl)
    if [[ "$day" > "$LAST_CLOSED" ]]; then
      continue
    fi
    # Persistent timers coalesce an arbitrary outage into one activation.
    # Re-run every closed segment idempotently so a multi-day outage cannot
    # leave older days stranded only on the instance.
    "$0" "$day"
    CLOSED_SEGMENTS=$((CLOSED_SEGMENTS + 1))
  done < <(find "$STREAM_DIR" -maxdepth 1 -type f -name '????-??-??.jsonl' | sort)
  echo "[archive_arcus_events] verified closed segments=$CLOSED_SEGMENTS through=$LAST_CLOSED"
  exit 0
fi

DATE=$1
if ! [[ "$DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] ||
   [ "$(date -u -d "$DATE" +%Y-%m-%d 2>/dev/null || true)" != "$DATE" ]; then
  echo "ERROR: DATE must be a real YYYY-MM-DD date, got: $DATE" >&2
  exit 64
fi
if [[ "$DATE" > "$(date -u -d 'yesterday' +%Y-%m-%d)" ]]; then
  echo "ERROR: refusing to archive an open or future UTC day: $DATE" >&2
  exit 64
fi

VERIFY_SCRIPT="${VERIFY_SCRIPT:-/usr/local/libexec/debot/arcus_live_tick_event_stream.py}"
S3_BUCKET="${S3_BUCKET:-debot-dashboard}"
S3_PREFIX="${S3_PREFIX:-arcus-archive/live-tick-events/debot-arcus}"
SEGMENT="$STREAM_DIR/$DATE.jsonl"

if [ ! -f "$SEGMENT" ] || [ -L "$SEGMENT" ]; then
  if [ ! -d "$STREAM_DIR" ]; then
    echo "[archive_arcus_events] stream not initialized; skipping pre-deployment date=$DATE"
    exit 0
  fi
  FIRST_SEGMENT=$(find "$STREAM_DIR" -maxdepth 1 -type f -name '????-??-??.jsonl' \
    -printf '%f\n' | sort | head -1)
  if [ -n "$FIRST_SEGMENT" ] && [[ "$DATE.jsonl" < "$FIRST_SEGMENT" ]]; then
    echo "[archive_arcus_events] skipping date before stream start: date=$DATE first=$FIRST_SEGMENT"
    exit 0
  fi
  echo "ERROR: missing or non-regular Arcus event segment: $SEGMENT" >&2
  exit 1
fi
if [ "$(stat -c %a "$SEGMENT")" != 600 ]; then
  echo "ERROR: Arcus event segment must have mode 0600: $SEGMENT" >&2
  exit 1
fi

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
MANIFEST="$WORK/$DATE.manifest.json"
ARCHIVE="$WORK/$DATE.events.jsonl.gz"

python3 "$VERIFY_SCRIPT" "$SEGMENT" --manifest-out "$MANIFEST" >/dev/null
gzip -n -9 -c "$SEGMENT" > "$ARCHIVE"
RAW_SHA256=$(sha256sum "$SEGMENT" | cut -d ' ' -f1)
GZIP_SHA256=$(sha256sum "$ARCHIVE" | cut -d ' ' -f1)
RAW_BYTES=$(stat -c %s "$SEGMENT")
GZIP_BYTES=$(stat -c %s "$ARCHIVE")
CLOSED_AT=$(date -u -d "$DATE + 1 day" +%Y-%m-%dT00:00:00Z)
DATA_KEY="$S3_PREFIX/${DATE:0:4}/${DATE:5:2}/$DATE.events.jsonl.gz"
MANIFEST_KEY="$S3_PREFIX/${DATE:0:4}/${DATE:5:2}/$DATE.manifest.json"

python3 - "$MANIFEST" "$DATE" "$CLOSED_AT" "$DATA_KEY" "$S3_BUCKET" \
  "$RAW_SHA256" "$GZIP_SHA256" "$RAW_BYTES" "$GZIP_BYTES" <<'PY'
import json
import sys

path, date, closed_at, data_key, bucket, raw_sha, gzip_sha, raw_bytes, gzip_bytes = sys.argv[1:]
with open(path, encoding="utf-8") as source:
    manifest = json.load(source)
manifest.update({
    "archive_schema_version": 1,
    "date_utc": date,
    "closed_at": closed_at,
    "data_key": data_key,
    "compression": "gzip-n",
    "raw_sha256": "sha256:" + raw_sha,
    "gzip_sha256": "sha256:" + gzip_sha,
    "raw_bytes": int(raw_bytes),
    "gzip_bytes": int(gzip_bytes),
    "storage": {
        "bucket": bucket,
        "private": True,
        "versioning": "enabled",
        "current_version_retention": "indefinite",
        "noncurrent_version_retention_days": 90,
    },
})
with open(path, "w", encoding="utf-8") as output:
    json.dump(manifest, output, indent=2, sort_keys=True)
    output.write("\n")
PY

put_immutable() {
  local key=$1
  local source=$2
  local content_type=$3
  local existing="$WORK/existing-$(basename "$source")"
  if aws s3api head-object --bucket "$S3_BUCKET" --key "$key" >/dev/null 2>&1; then
    aws s3api get-object --bucket "$S3_BUCKET" --key "$key" "$existing" >/dev/null
    if ! cmp -s "$source" "$existing"; then
      echo "ERROR: immutable Arcus archive key already has different content: $key" >&2
      exit 1
    fi
    echo "[archive_arcus_events] already archived identically: s3://$S3_BUCKET/$key"
    return
  fi
  if ! aws s3api put-object \
      --bucket "$S3_BUCKET" \
      --key "$key" \
      --body "$source" \
      --content-type "$content_type" \
      --if-none-match '*' >/dev/null; then
    aws s3api get-object --bucket "$S3_BUCKET" --key "$key" "$existing" >/dev/null
    if ! cmp -s "$source" "$existing"; then
      echo "ERROR: immutable Arcus archive put raced with different content: $key" >&2
      exit 1
    fi
  fi
}

# Publish data first. A retry can safely finish the manifest after a crash;
# neither immutable key can be overwritten with different bytes.
put_immutable "$DATA_KEY" "$ARCHIVE" application/gzip
put_immutable "$MANIFEST_KEY" "$MANIFEST" application/json

echo "[archive_arcus_events] archived and verified date=$DATE"
echo "[archive_arcus_events] data=s3://$S3_BUCKET/$DATA_KEY sha256:$GZIP_SHA256"
echo "[archive_arcus_events] manifest=s3://$S3_BUCKET/$MANIFEST_KEY"
