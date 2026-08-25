#!/usr/bin/env bash
# Verify and immutably archive all closed UTC-day Arcus live-tick segments.
# An explicit YYYY-MM-DD limits the operation to one day for repair/testing.
set -euo pipefail
umask 077

if [ "$#" -gt 1 ]; then
  echo "usage: $0 [YYYY-MM-DD]" >&2
  exit 64
fi

STATE_DIR="${STATE_DIR:-/var/lib/debot-arcus/spot-execute-once}"
STREAM_DIR="${STREAM_DIR:-$STATE_DIR/live-tick-events}"
START_MARKER="$STREAM_DIR/.archive-start-date"
LAST_CLOSED="${ARCUS_ARCHIVE_LAST_CLOSED_UTC:-$(date -u -d 'yesterday' +%Y-%m-%d)}"
if ! [[ "$LAST_CLOSED" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] ||
   [ "$(date -u -d "$LAST_CLOSED" +%Y-%m-%d 2>/dev/null || true)" != "$LAST_CLOSED" ]; then
  echo "ERROR: ARCUS_ARCHIVE_LAST_CLOSED_UTC must be a real YYYY-MM-DD date" >&2
  exit 64
fi

resolve_stream_start() {
  local first_segment marker_bytes marker_tmp start_date
  if [ -e "$START_MARKER" ] || [ -L "$START_MARKER" ]; then
    if [ ! -f "$START_MARKER" ] || [ -L "$START_MARKER" ]; then
      echo "ERROR: Arcus archive start marker must be a regular file: $START_MARKER" >&2
      return 1
    fi
    if [ "$(stat -c %a "$START_MARKER")" != 600 ]; then
      echo "ERROR: Arcus archive start marker must have mode 0600: $START_MARKER" >&2
      return 1
    fi
    marker_bytes=$(wc -c < "$START_MARKER")
    start_date=$(sed -n '1p' "$START_MARKER")
    if [ "$marker_bytes" -ne 11 ] ||
       ! [[ "$start_date" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] ||
       [ "$(date -u -d "$start_date" +%Y-%m-%d 2>/dev/null || true)" != "$start_date" ]; then
      echo "ERROR: invalid Arcus archive start marker: $START_MARKER" >&2
      return 1
    fi
    printf '%s\n' "$start_date"
    return
  fi

  first_segment=$(find "$STREAM_DIR" -maxdepth 1 -type f -name '????-??-??.jsonl' \
    -printf '%f\n' | sort | sed -n '1p')
  if [ -z "$first_segment" ]; then
    return
  fi
  start_date=${first_segment%.jsonl}
  if [ "$(date -u -d "$start_date" +%Y-%m-%d 2>/dev/null || true)" != "$start_date" ]; then
    echo "ERROR: invalid first Arcus event segment date: $first_segment" >&2
    return 1
  fi
  marker_tmp=$(mktemp "$STREAM_DIR/.archive-start-date.XXXXXX")
  printf '%s\n' "$start_date" > "$marker_tmp"
  chmod 0600 "$marker_tmp"
  sync -f "$marker_tmp"
  mv "$marker_tmp" "$START_MARKER"
  sync -f "$STREAM_DIR"
  echo "[archive_arcus_events] initialized durable start date=$start_date" >&2
  printf '%s\n' "$start_date"
}

if [ "$#" -eq 0 ]; then
  if [ -L "$STREAM_DIR" ]; then
    echo "ERROR: refusing symlink Arcus event stream directory: $STREAM_DIR" >&2
    exit 1
  fi
  if [ ! -d "$STREAM_DIR" ]; then
    echo "[archive_arcus_events] stream not initialized; no closed segments to archive"
    exit 0
  fi
  START_DATE=$(resolve_stream_start)
  if [ -z "$START_DATE" ] || [[ "$START_DATE" > "$LAST_CLOSED" ]]; then
    echo "[archive_arcus_events] no closed segments through=$LAST_CLOSED"
    exit 0
  fi
  day=$START_DATE
  CLOSED_SEGMENTS=0
  while [[ "$day" < "$LAST_CLOSED" || "$day" == "$LAST_CLOSED" ]]; do
    segment="$STREAM_DIR/$day.jsonl"
    if [ ! -f "$segment" ] || [ -L "$segment" ]; then
      echo "ERROR: missing or non-regular closed Arcus event segment: $segment" >&2
      exit 1
    fi
    # Persistent timers coalesce an arbitrary outage into one activation.
    # Re-run every calendar day idempotently and fail closed on a missing day;
    # otherwise an outage can silently leave an un-fetchable archive gap.
    "$0" "$day"
    CLOSED_SEGMENTS=$((CLOSED_SEGMENTS + 1))
    day=$(date -u -d "$day + 1 day" +%Y-%m-%d)
  done
  echo "[archive_arcus_events] verified closed segments=$CLOSED_SEGMENTS through=$LAST_CLOSED"
  exit 0
fi

DATE=$1
if ! [[ "$DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] ||
   [ "$(date -u -d "$DATE" +%Y-%m-%d 2>/dev/null || true)" != "$DATE" ]; then
  echo "ERROR: DATE must be a real YYYY-MM-DD date, got: $DATE" >&2
  exit 64
fi
if [[ "$DATE" > "$LAST_CLOSED" ]]; then
  echo "ERROR: refusing to archive an open or future UTC day: $DATE" >&2
  exit 64
fi

VERIFY_SCRIPT="${VERIFY_SCRIPT:-/usr/local/libexec/debot/arcus_live_tick_event_stream.py}"
S3_BUCKET="${S3_BUCKET:-debot-dashboard}"
S3_PREFIX="${S3_PREFIX:-arcus-archive/live-tick-events/debot-arcus}"
SEGMENT="$STREAM_DIR/$DATE.jsonl"

if [ -L "$STREAM_DIR" ]; then
  echo "ERROR: refusing symlink Arcus event stream directory: $STREAM_DIR" >&2
  exit 1
fi
if [ ! -f "$SEGMENT" ] || [ -L "$SEGMENT" ]; then
  if [ ! -d "$STREAM_DIR" ]; then
    echo "[archive_arcus_events] stream not initialized; skipping pre-deployment date=$DATE"
    exit 0
  fi
  START_DATE=$(resolve_stream_start)
  if [ -n "$START_DATE" ] && [[ "$DATE" < "$START_DATE" ]]; then
    echo "[archive_arcus_events] skipping date before stream start: date=$DATE first=$START_DATE"
    exit 0
  fi
  echo "ERROR: missing or non-regular Arcus event segment: $SEGMENT" >&2
  exit 1
fi
if [ "$(stat -c %a "$SEGMENT")" != 600 ]; then
  echo "ERROR: Arcus event segment must have mode 0600: $SEGMENT" >&2
  exit 1
fi
START_DATE=$(resolve_stream_start)
if [ -z "$START_DATE" ] || [[ "$DATE" < "$START_DATE" ]]; then
  echo "ERROR: Arcus archive date precedes or lacks its durable stream start" >&2
  exit 1
fi

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
MANIFEST="$WORK/$DATE.manifest.json"
ARCHIVE="$WORK/$DATE.events.jsonl.gz"

python3 "$VERIFY_SCRIPT" "$SEGMENT" --manifest-out "$MANIFEST" >/dev/null
if [ "$DATE" = "$START_DATE" ]; then
  python3 - "$MANIFEST" <<'PY'
import json
import sys
with open(sys.argv[1], encoding="utf-8") as source:
    manifest = json.load(source)
if manifest.get("first_previous_chain_sha256") is not None:
    raise SystemExit("ERROR: first archived segment does not begin at the stream chain anchor")
PY
else
  PREVIOUS_DATE=$(date -u -d "$DATE - 1 day" +%Y-%m-%d)
  PREVIOUS_SEGMENT="$STREAM_DIR/$PREVIOUS_DATE.jsonl"
  if [ ! -f "$PREVIOUS_SEGMENT" ] || [ -L "$PREVIOUS_SEGMENT" ] ||
     [ "$(stat -c %a "$PREVIOUS_SEGMENT" 2>/dev/null || true)" != 600 ]; then
    echo "ERROR: missing or unsafe predecessor Arcus event segment: $PREVIOUS_SEGMENT" >&2
    exit 1
  fi
  python3 "$VERIFY_SCRIPT" "$PREVIOUS_SEGMENT" "$SEGMENT" >/dev/null
  PREVIOUS_MANIFEST_KEY="$S3_PREFIX/${PREVIOUS_DATE:0:4}/${PREVIOUS_DATE:5:2}/$PREVIOUS_DATE.manifest.json"
  PREVIOUS_MANIFEST="$WORK/$PREVIOUS_DATE.manifest.json"
  if ! aws s3api get-object --bucket "$S3_BUCKET" --key "$PREVIOUS_MANIFEST_KEY" \
      "$PREVIOUS_MANIFEST" >/dev/null; then
    echo "ERROR: predecessor manifest must be archived before date=$DATE" >&2
    exit 1
  fi
  python3 - "$PREVIOUS_MANIFEST" "$MANIFEST" "$PREVIOUS_DATE" <<'PY'
import json
import sys
previous_path, current_path, expected_previous_date = sys.argv[1:]
with open(previous_path, encoding="utf-8") as source:
    previous = json.load(source)
with open(current_path, encoding="utf-8") as source:
    current = json.load(source)
if previous.get("date_utc") != expected_previous_date:
    raise SystemExit("ERROR: predecessor manifest date mismatch")
if previous.get("last_chain_sha256") != current.get("first_previous_chain_sha256"):
    raise SystemExit("ERROR: current segment does not continue the archived predecessor chain")
PY
fi
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
