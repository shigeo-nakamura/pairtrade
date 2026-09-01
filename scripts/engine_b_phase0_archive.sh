#!/bin/bash
# Archive completed hourly SQLite partitions, then remove only the verified local copy.
set -euo pipefail

DATA_DIR=${ENGINE_B_PHASE0_DATA_DIR:-/var/lib/engine-b-phase0/data}
S3_BUCKET=${ENGINE_B_PHASE0_S3_BUCKET:-debot-dashboard}
S3_PREFIX=${ENGINE_B_PHASE0_S3_PREFIX:-debot/engine-b/phase0/raw}
PYTHON_BIN=${ENGINE_B_PHASE0_PYTHON:-/opt/engine-b-phase0/venv/bin/python}
DELETE_VERIFIED_LOCAL=${ENGINE_B_PHASE0_DELETE_VERIFIED_LOCAL:-false}
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
  set +e
  "$PYTHON_BIN" - "$db" <<'PY'
import sqlite3
import sys

connection = sqlite3.connect(sys.argv[1], timeout=0)
try:
    checkpoint = connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
    if checkpoint and checkpoint[0]:
        raise SystemExit(75)
    tables = {
        row[0]
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        )
    }
    if "ohlcv_1m" in tables:
        connection.execute("UPDATE ohlcv_1m SET is_complete = 1 WHERE is_complete = 0")
        connection.commit()
    mode = connection.execute("PRAGMA journal_mode=DELETE").fetchone()[0]
    if mode.lower() != "delete":
        raise SystemExit(f"could not leave WAL mode for {sys.argv[1]}: {mode}")
    result = connection.execute("PRAGMA integrity_check").fetchone()[0]
except sqlite3.OperationalError as exc:
    if "locked" in str(exc).lower() or "busy" in str(exc).lower():
        raise SystemExit(75) from exc
    raise
finally:
    connection.close()
if result != "ok":
    raise SystemExit(f"integrity_check failed for {sys.argv[1]}: {result}")
PY
  checkpoint_status=$?
  set -e
  if [ "$checkpoint_status" -eq 75 ]; then
    echo "Skipping partition still owned by a live SQLite writer: $db" >&2
    continue
  fi
  if [ "$checkpoint_status" -ne 0 ]; then
    exit "$checkpoint_status"
  fi
  if [ -e "$db-wal" ] || [ -e "$db-shm" ]; then
    echo "Skipping partition with unrecovered SQLite sidecar: $db" >&2
    continue
  fi

  source_fingerprint=$(stat -c '%s:%Y:%y' "$db")
  year=${partition:0:4}
  month=${partition:4:2}
  key="$S3_PREFIX/$HOST_ID/$year/$month/$base.gz"
  checksum_key="$key.sha256"
  archive_tmp=$(mktemp "$DATA_DIR/.${base}.archive.XXXXXX.gz")
  checksum_tmp=$(mktemp "$DATA_DIR/.${base}.checksum.XXXXXX")
  remote_tmp=$(mktemp "$DATA_DIR/.${base}.remote.XXXXXX.gz")
  remote_checksum_tmp=$(mktemp "$DATA_DIR/.${base}.remote-checksum.XXXXXX")
  remote_db_tmp=$(mktemp "$DATA_DIR/.${base}.remote-db.XXXXXX")
  trap 'rm -f -- "$archive_tmp" "$checksum_tmp" "$remote_tmp" "$remote_checksum_tmp" "$remote_db_tmp"' EXIT

  gzip -c "$db" > "$archive_tmp"
  gzip -t "$archive_tmp"
  expected_db_sha=$(sha256sum "$db" | cut -d' ' -f1)
  printf '%s  %s\n' "$expected_db_sha" "$base" > "$checksum_tmp"
  aws s3 cp "$archive_tmp" "s3://$S3_BUCKET/$key" \
    --sse AES256 --content-type application/gzip --no-progress
  aws s3 cp "$checksum_tmp" "s3://$S3_BUCKET/$checksum_key" \
    --sse AES256 --content-type text/plain --no-progress

  remote_size=$(aws s3api head-object --bucket "$S3_BUCKET" --key "$key" \
    --query ContentLength --output text)
  remote_sse=$(aws s3api head-object --bucket "$S3_BUCKET" --key "$key" \
    --query ServerSideEncryption --output text)
  checksum_size=$(aws s3api head-object --bucket "$S3_BUCKET" --key "$checksum_key" \
    --query ContentLength --output text)
  checksum_sse=$(aws s3api head-object --bucket "$S3_BUCKET" --key "$checksum_key" \
    --query ServerSideEncryption --output text)
  local_size=$(wc -c < "$archive_tmp")
  aws s3 cp "s3://$S3_BUCKET/$key" "$remote_tmp" --no-progress
  aws s3 cp "s3://$S3_BUCKET/$checksum_key" "$remote_checksum_tmp" --no-progress
  gzip -t "$remote_tmp"
  gzip -dc "$remote_tmp" > "$remote_db_tmp"
  "$PYTHON_BIN" - "$remote_db_tmp" <<'PY'
import sqlite3
import sys

connection = sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True)
try:
    result = connection.execute("PRAGMA integrity_check").fetchone()[0]
finally:
    connection.close()
if result != "ok":
    raise SystemExit(f"remote integrity_check failed for {sys.argv[1]}: {result}")
PY
  remote_db_sha=$(sha256sum "$remote_db_tmp" | cut -d' ' -f1)
  remote_checksum_sha=$(cut -d' ' -f1 "$remote_checksum_tmp")

  if [ "$remote_size" != "$local_size" ] || [ "$remote_sse" != "AES256" ] \
      || [[ ! "$checksum_size" =~ ^[1-9][0-9]*$ ]] || [ "$checksum_sse" != "AES256" ] \
      || ! cmp -s "$archive_tmp" "$remote_tmp" \
      || [ "$remote_db_sha" != "$expected_db_sha" ] \
      || [ "$remote_checksum_sha" != "$expected_db_sha" ]; then
    echo "Archive verification failed for $db" >&2
    exit 1
  fi

  current_fingerprint=$(stat -c '%s:%Y:%y' "$db")
  source_stable=true
  if [ "$current_fingerprint" != "$source_fingerprint" ] \
      || [ -e "$db-wal" ] || [ -e "$db-shm" ]; then
    source_stable=false
  fi
  if [ "$DELETE_VERIFIED_LOCAL" = "true" ] && [ "$source_stable" = "true" ]; then
    rm -f -- "$db"
    echo "Archived, verified, and removed closed partition: $db -> s3://$S3_BUCKET/$key"
  elif [ "$source_stable" != "true" ]; then
    echo "Archived snapshot verified, but source changed during upload; local partition retained: $db" >&2
  else
    echo "Archived and verified closed partition; local deletion disabled: $db -> s3://$S3_BUCKET/$key"
  fi
  rm -f -- "$archive_tmp" "$checksum_tmp" "$remote_tmp" "$remote_checksum_tmp" "$remote_db_tmp"
  trap - EXIT
done
