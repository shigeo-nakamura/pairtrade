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
STATE_DIR=${ENGINE_B_PHASE0_STATE_DIR:-$(dirname "$DATA_DIR")}
LOCK_DIR="$STATE_DIR/locks"
SEALED_DIR="$STATE_DIR/sealed"

if [ ! -d "$DATA_DIR" ]; then
  echo "No Engine B data directory yet: $DATA_DIR"
  exit 0
fi
install -d -m 0750 "$LOCK_DIR" "$SEALED_DIR"

shopt -s nullglob
for db in "$DATA_DIR"/engine_b_phase0_*.sqlite3; do
  base=$(basename "$db")
  partition=${base#engine_b_phase0_}
  partition=${partition%.sqlite3}
  if [[ "$partition" == "$CURRENT_PARTITION" || "$partition" > "$CURRENT_PARTITION" ]]; then
    continue
  fi
  seal_path="$SEALED_DIR/$partition.json"
  if [ -e "$seal_path" ]; then
    echo "Skipping already sealed partition: $db" >&2
    continue
  fi
  lock_file="$LOCK_DIR/$partition.lock"
  exec {lock_fd}> "$lock_file"
  chmod 0640 "$lock_file"
  flock "$lock_fd"
  if [ -e "$seal_path" ]; then
    flock -u "$lock_fd"
    exec {lock_fd}>&-
    echo "Skipping partition sealed while waiting for lock: $db" >&2
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
    flock -u "$lock_fd"
    exec {lock_fd}>&-
    continue
  fi
  if [ "$checkpoint_status" -ne 0 ]; then
    exit "$checkpoint_status"
  fi
  if [ -e "$db-wal" ] || [ -e "$db-shm" ]; then
    echo "Skipping partition with unrecovered SQLite sidecar: $db" >&2
    flock -u "$lock_fd"
    exec {lock_fd}>&-
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
  trap 'rm -f -- "$archive_tmp" "$checksum_tmp" "$remote_tmp" "$remote_checksum_tmp" "$remote_db_tmp" "${seal_tmp:-}" "${trade_index_tmp:-}"' EXIT

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

  seal_created=false
  if [ "$DELETE_VERIFIED_LOCAL" = "true" ]; then
    trade_index_path="$SEALED_DIR/$partition.trade_ids.sqlite3"
    trade_index_tmp=$(mktemp "$SEALED_DIR/.${partition}.trade-ids.XXXXXX.sqlite3")
    "$PYTHON_BIN" - "$db" "$trade_index_tmp" "$partition" "$expected_db_sha" <<'PY'
import sqlite3
import sys

source_path, index_path, partition, canonical_sha256 = sys.argv[1:]
source = sqlite3.connect(f"file:{source_path}?mode=ro", uri=True)
index = sqlite3.connect(index_path)
try:
    index.execute(
        """CREATE TABLE sealed_metadata(
             partition TEXT PRIMARY KEY,
             canonical_db_sha256 TEXT NOT NULL
           )"""
    )
    index.execute(
        """CREATE TABLE archived_trade_identity(
             venue TEXT NOT NULL,
             market_id INTEGER NOT NULL,
             exchange_trade_id TEXT NOT NULL,
             PRIMARY KEY(venue, market_id, exchange_trade_id)
           ) WITHOUT ROWID"""
    )
    tables = {
        row[0]
        for row in source.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        )
    }
    if "trade" in tables:
        index.executemany(
            """INSERT OR IGNORE INTO archived_trade_identity
               VALUES (?, ?, ?)""",
            source.execute(
                "SELECT venue, market_id, exchange_trade_id FROM trade"
            ),
        )
    index.execute(
        "INSERT INTO sealed_metadata VALUES (?, ?)",
        (partition, canonical_sha256),
    )
    index.commit()
    if index.execute("PRAGMA integrity_check").fetchone() != ("ok",):
        raise SystemExit(f"trade identity index integrity_check failed: {index_path}")
finally:
    index.close()
    source.close()
PY
    chmod 0640 "$trade_index_tmp"
    trade_identity_count=$(
      "$PYTHON_BIN" - "$trade_index_tmp" <<'PY'
import sqlite3
import sys

connection = sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True)
try:
    print(connection.execute("SELECT COUNT(*) FROM archived_trade_identity").fetchone()[0])
finally:
    connection.close()
PY
    )
    mv "$trade_index_tmp" "$trade_index_path"
    seal_tmp=$(mktemp "$SEALED_DIR/.${partition}.XXXXXX")
    sealed_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
    printf '{"partition":"%s","s3_key":"s3://%s/%s","sha256":"%s","trade_index":"%s","trade_identity_count":%s,"sealed_at":"%s"}\n' \
      "$partition" "$S3_BUCKET" "$key" "$expected_db_sha" \
      "$(basename "$trade_index_path")" "$trade_identity_count" "$sealed_at" > "$seal_tmp"
    chmod 0640 "$seal_tmp"
    mv "$seal_tmp" "$seal_path"
    seal_created=true
  fi

  current_fingerprint=$(stat -c '%s:%Y:%y' "$db")
  source_stable=true
  if [ "$current_fingerprint" != "$source_fingerprint" ] \
      || [ -e "$db-wal" ] || [ -e "$db-shm" ]; then
    source_stable=false
    if [ "$seal_created" = "true" ]; then
      rm -f -- "$seal_path"
      rm -f -- "$trade_index_path"
      seal_created=false
    fi
  fi
  if [ "$DELETE_VERIFIED_LOCAL" = "true" ] && [ "$source_stable" = "true" ]; then
    if ! rm -f -- "$db"; then
      rm -f -- "$seal_path"
      rm -f -- "$trade_index_path"
      exit 1
    fi
    echo "Archived, sealed, verified, and removed closed partition: $db -> s3://$S3_BUCKET/$key"
  elif [ "$source_stable" != "true" ]; then
    echo "Archived snapshot verified, but source changed during upload; local partition retained: $db" >&2
  else
    echo "Archived and verified closed partition; local deletion disabled: $db -> s3://$S3_BUCKET/$key"
  fi
  rm -f -- "$archive_tmp" "$checksum_tmp" "$remote_tmp" "$remote_checksum_tmp" "$remote_db_tmp" "${seal_tmp:-}" "${trade_index_tmp:-}"
  trap - EXIT
  flock -u "$lock_fd"
  exec {lock_fd}>&-
done
