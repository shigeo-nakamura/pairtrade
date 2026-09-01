#!/bin/bash
# Archive completed hourly SQLite partitions, then remove only the verified local copy.
set -euo pipefail

DATA_DIR=${ENGINE_B_PHASE0_DATA_DIR:-/var/lib/engine-b-phase0/data}
S3_BUCKET=${ENGINE_B_PHASE0_S3_BUCKET:-debot-dashboard}
S3_PREFIX=${ENGINE_B_PHASE0_S3_PREFIX:-debot/engine-b/phase0/raw}
PYTHON_BIN=${ENGINE_B_PHASE0_PYTHON:-/opt/engine-b-phase0/venv/bin/python}
CURRENT_PARTITION=$(date -u +%Y%m%d_%H)
HOST_ID=$(hostname -s)

if [ ! -d "$DATA_DIR" ]; then
  echo "No Engine B data directory yet: $DATA_DIR"
  exit 0
fi

shopt -s nullglob
for db in "$DATA_DIR"/engine_b_phase0_*.sqlite3; do
  base=$(basename "$db")
  partition=${base#engine_b_phase0_}
  partition=${partition%.sqlite3}
  if [[ "$partition" == "$CURRENT_PARTITION" || "$partition" > "$CURRENT_PARTITION" ]]; then
    continue
  fi
  if [ -e "$db-wal" ] || [ -e "$db-shm" ]; then
    echo "Skipping partition with live SQLite sidecar: $db" >&2
    continue
  fi

  "$PYTHON_BIN" - "$db" <<'PY'
import sqlite3
import sys

connection = sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True)
try:
    result = connection.execute("PRAGMA integrity_check").fetchone()[0]
finally:
    connection.close()
if result != "ok":
    raise SystemExit(f"integrity_check failed for {sys.argv[1]}: {result}")
PY

  year=${partition:0:4}
  month=${partition:4:2}
  key="$S3_PREFIX/$HOST_ID/$year/$month/$base.gz"
  checksum_key="$key.sha256"
  gzip -c "$db" | aws s3 cp - "s3://$S3_BUCKET/$key" \
    --sse AES256 --content-type application/gzip --no-progress
  sha256sum "$db" | aws s3 cp - "s3://$S3_BUCKET/$checksum_key" \
    --sse AES256 --content-type text/plain --no-progress

  remote_size=$(aws s3api head-object --bucket "$S3_BUCKET" --key "$key" \
    --query ContentLength --output text)
  remote_sse=$(aws s3api head-object --bucket "$S3_BUCKET" --key "$key" \
    --query ServerSideEncryption --output text)
  checksum_size=$(aws s3api head-object --bucket "$S3_BUCKET" --key "$checksum_key" \
    --query ContentLength --output text)
  if [[ ! "$remote_size" =~ ^[1-9][0-9]*$ ]] || [ "$remote_sse" != "AES256" ] \
      || [[ ! "$checksum_size" =~ ^[1-9][0-9]*$ ]]; then
    echo "Archive verification failed for $db" >&2
    exit 1
  fi

  rm -f -- "$db"
  echo "Archived and removed closed partition: $db -> s3://$S3_BUCKET/$key"
done
