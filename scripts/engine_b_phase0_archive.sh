#!/bin/bash
# Archive completed hourly SQLite partitions, then remove only the verified local copy.
set -euo pipefail

DATA_DIR=${ENGINE_B_PHASE0_DATA_DIR:-/var/lib/engine-b-phase0/data}
S3_BUCKET=${ENGINE_B_PHASE0_S3_BUCKET:-debot-dashboard}
S3_PREFIX=${ENGINE_B_PHASE0_S3_PREFIX:-debot/engine-b/phase0/raw}
PYTHON_BIN=${ENGINE_B_PHASE0_PYTHON:-/opt/engine-b-phase0/venv/bin/python}
OBSERVER_SCRIPT=${ENGINE_B_PHASE0_OBSERVER_SCRIPT:-$(dirname "$0")/engine_b_phase0.py}
DELETE_VERIFIED_LOCAL=${ENGINE_B_PHASE0_DELETE_VERIFIED_LOCAL:-false}
CURRENT_PARTITION=$(date -u +%Y%m%d_%H)
HOST_ID=$(hostname -s)
STATE_DIR=${ENGINE_B_PHASE0_STATE_DIR:-$(dirname "$DATA_DIR")}
LOCK_DIR="$STATE_DIR/locks"
SEALED_DIR="$STATE_DIR/sealed"
GAP_CONTINUATION_DIR="$STATE_DIR/gap-continuations"
SESSION_CONTINUATION_DIR="$STATE_DIR/session-continuations"

fsync_files_and_directory() {
  "$PYTHON_BIN" - "$@" <<'PY'
import os
import sys

directory = sys.argv[1]
for path in sys.argv[2:]:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
descriptor = os.open(directory, os.O_RDONLY)
try:
    os.fsync(descriptor)
finally:
    os.close(descriptor)
PY
}

fsync_directory() {
  fsync_files_and_directory "$1"
}

persist_seal_sidecars() (
  set -euo pipefail
  local source_db=$1
  local trade_index=$2
  local seal=$3
  local canonical_uri=$4
  local canonical_bucket_and_key=${canonical_uri#s3://}
  local canonical_bucket=${canonical_bucket_and_key%%/*}
  local canonical_key=${canonical_bucket_and_key#*/}
  local trade_index_key="${canonical_key}.trade_ids.sqlite3"
  local seal_key="${canonical_key}.seal.json"
  local remote_trade_index
  local remote_seal
  if [[ "$canonical_uri" != s3://*/* ]] \
      || [ -z "$canonical_bucket" ] \
      || [ -z "$canonical_key" ] \
      || [ "$canonical_bucket_and_key" = "$canonical_key" ]; then
    echo "Invalid canonical archive URI in seal: $canonical_uri" >&2
    exit 1
  fi
  remote_trade_index=$(mktemp "$DATA_DIR/.seal-index.remote.XXXXXX.sqlite3")
  remote_seal=$(mktemp "$DATA_DIR/.seal.remote.XXXXXX.json")
  trap 'rm -f -- "$remote_trade_index" "$remote_seal"' EXIT

  if [ "$source_db" != "-" ]; then
    "$PYTHON_BIN" "$OBSERVER_SCRIPT" --verify-sealed-partition \
      "$source_db" "$trade_index" "$seal" "$(basename "$seal" .json)"
  else
    "$PYTHON_BIN" - "$trade_index" "$seal" <<'PY'
import json
import sqlite3
import sys

seal = json.load(open(sys.argv[2]))
index = sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True)
try:
    assert index.execute("PRAGMA integrity_check").fetchone() == ("ok",)
    assert index.execute(
        "SELECT partition, canonical_db_sha256 FROM sealed_metadata"
    ).fetchone() == (seal["partition"], seal["sha256"])
finally:
    index.close()
PY
  fi
  aws s3 cp "$trade_index" "s3://$canonical_bucket/$trade_index_key" \
    --sse AES256 --content-type application/vnd.sqlite3 --no-progress
  aws s3 cp "$seal" "s3://$canonical_bucket/$seal_key" \
    --sse AES256 --content-type application/json --no-progress
  aws s3 cp "s3://$canonical_bucket/$trade_index_key" "$remote_trade_index" --no-progress
  aws s3 cp "s3://$canonical_bucket/$seal_key" "$remote_seal" --no-progress

  local index_sse
  local seal_sse
  local index_size
  local seal_size
  index_sse=$(aws s3api head-object --bucket "$canonical_bucket" --key "$trade_index_key" \
    --query ServerSideEncryption --output text)
  seal_sse=$(aws s3api head-object --bucket "$canonical_bucket" --key "$seal_key" \
    --query ServerSideEncryption --output text)
  index_size=$(aws s3api head-object --bucket "$canonical_bucket" --key "$trade_index_key" \
    --query ContentLength --output text)
  seal_size=$(aws s3api head-object --bucket "$canonical_bucket" --key "$seal_key" \
    --query ContentLength --output text)
  if [ "$index_sse" != "AES256" ] || [ "$seal_sse" != "AES256" ] \
      || [[ ! "$index_size" =~ ^[1-9][0-9]*$ ]] \
      || [[ ! "$seal_size" =~ ^[1-9][0-9]*$ ]] \
      || ! cmp -s "$trade_index" "$remote_trade_index" \
      || ! cmp -s "$seal" "$remote_seal"; then
    echo "Remote seal sidecar verification failed for $source_db" >&2
    exit 1
  fi
)

republish_reconciled_sidecars() (
  set -euo pipefail
  local source_db=$1
  local partitions
  partitions=$("$PYTHON_BIN" - "$source_db" <<'PY'
import sqlite3
import sys

connection = sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True)
try:
    tables = {row[0] for row in connection.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table'"
    )}
    if "late_trade" in tables:
        for row in connection.execute(
            "SELECT DISTINCT sealed_partition FROM late_trade ORDER BY sealed_partition"
        ):
            print(row[0])
finally:
    connection.close()
PY
  )
  while IFS= read -r sealed_partition; do
    [ -n "$sealed_partition" ] || continue
    local seal="$SEALED_DIR/$sealed_partition.json"
    local index="$SEALED_DIR/$sealed_partition.trade_ids.sqlite3"
    local canonical_uri
    canonical_uri=$("$PYTHON_BIN" - "$seal" <<'PY'
import json
import sys
print(json.load(open(sys.argv[1]))["s3_key"])
PY
    )
    persist_seal_sidecars - "$index" "$seal" "$canonical_uri"
  done <<< "$partitions"
)

if [ ! -d "$DATA_DIR" ]; then
  echo "No Engine B data directory yet: $DATA_DIR"
  exit 0
fi
install -d -m 0750 "$LOCK_DIR" "$SEALED_DIR" "$GAP_CONTINUATION_DIR" "$SESSION_CONTINUATION_DIR"

shopt -s nullglob
for db in "$DATA_DIR"/engine_b_phase0_*.sqlite3; do
  base=$(basename "$db")
  partition=${base#engine_b_phase0_}
  partition=${partition%.sqlite3}
  if [[ "$partition" == "$CURRENT_PARTITION" || "$partition" > "$CURRENT_PARTITION" ]]; then
    continue
  fi
  seal_path="$SEALED_DIR/$partition.json"
  year=${partition:0:4}
  month=${partition:4:2}
  key="$S3_PREFIX/$HOST_ID/$year/$month/$base.gz"
  lock_file="$LOCK_DIR/$partition.lock"
  exec {lock_fd}> "$lock_file"
  chmod 0640 "$lock_file"
  flock "$lock_fd"
  if [ -e "$seal_path" ]; then
    if [ "$DELETE_VERIFIED_LOCAL" != "true" ]; then
      flock -u "$lock_fd"
      exec {lock_fd}>&-
      echo "Retaining sealed partition because local deletion is disabled: $db" >&2
      continue
    fi
    if [ -e "$db-wal" ] || [ -e "$db-shm" ]; then
      echo "Refusing sealed-partition recovery with SQLite sidecars: $db" >&2
      exit 1
    fi
    trade_index_path="$SEALED_DIR/$partition.trade_ids.sqlite3"
    "$PYTHON_BIN" "$OBSERVER_SCRIPT" --verify-sealed-partition \
      "$db" "$trade_index_path" "$seal_path" "$partition"
    "$PYTHON_BIN" "$OBSERVER_SCRIPT" --reconcile-late-trade-identities \
      "$db" "$SEALED_DIR"
    republish_reconciled_sidecars "$db"
    canonical_uri=$("$PYTHON_BIN" - "$seal_path" <<'PY'
import json
import sys
print(json.load(open(sys.argv[1]))["s3_key"])
PY
    )
    persist_seal_sidecars "$db" "$trade_index_path" "$seal_path" "$canonical_uri"
    fsync_files_and_directory "$SEALED_DIR" "$trade_index_path" "$seal_path"
    rm -f -- "$db"
    fsync_directory "$DATA_DIR"
    flock -u "$lock_fd"
    exec {lock_fd}>&-
    echo "Recovered and removed verified sealed partition left by an interrupted archive: $db"
    continue
  fi
  set +e
  "$PYTHON_BIN" - "$db" "$partition" "$GAP_CONTINUATION_DIR" "$SESSION_CONTINUATION_DIR" <<'PY'
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import sys

connection = sqlite3.connect(sys.argv[1], timeout=0)
try:
    checkpoint = connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
    if checkpoint and checkpoint[0]:
        raise SystemExit(75)
    mode = connection.execute("PRAGMA journal_mode=DELETE").fetchone()[0]
    if mode.lower() != "delete":
        raise SystemExit(f"could not leave WAL mode for {sys.argv[1]}: {mode}")
    tables = {
        row[0]
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        )
    }
    partition_end = (
        datetime.strptime(sys.argv[2], "%Y%m%d_%H").replace(tzinfo=timezone.utc)
        + timedelta(hours=1)
    )
    partition_end_us = int(partition_end.timestamp() * 1_000_000)
    if "ohlcv_1m" in tables:
        connection.execute("UPDATE ohlcv_1m SET is_complete = 1 WHERE is_complete = 0")
    if "ws_connection" in tables:
        marker_dir = Path(sys.argv[4])
        rows = connection.execute(
            """SELECT connection_session_id, venue, api_schema_version
               FROM ws_connection WHERE ended_ts_recv_us IS NULL"""
        ).fetchall()
        for session_id, venue, api_schema_version in rows:
            continuation_id = f"partition:{sys.argv[2]}:{session_id}"
            marker = {
                "continuation_id": continuation_id,
                "start_us": partition_end_us,
                "source_partition": sys.argv[2],
                "connection": {
                    "id": session_id,
                    "venue": venue,
                    "started_us": partition_end_us,
                    "api_schema_version": api_schema_version,
                },
            }
            marker_name = hashlib.sha256(continuation_id.encode()).hexdigest()
            marker_path = marker_dir / f"{marker_name}.json"
            encoded = json.dumps(marker, sort_keys=True, separators=(",", ":")) + "\n"
            if marker_path.exists():
                if marker_path.read_text() != encoded:
                    raise RuntimeError(
                        f"session continuation marker mismatch: {marker_path}"
                    )
            else:
                temporary = marker_path.with_name(
                    f".{marker_path.name}.{os.getpid()}.tmp"
                )
                try:
                    with temporary.open("w") as output:
                        output.write(encoded)
                        output.flush()
                        os.fsync(output.fileno())
                    os.chmod(temporary, 0o640)
                    os.replace(temporary, marker_path)
                    directory_fd = os.open(marker_dir, os.O_RDONLY)
                    try:
                        os.fsync(directory_fd)
                    finally:
                        os.close(directory_fd)
                finally:
                    temporary.unlink(missing_ok=True)
        connection.execute(
            """UPDATE ws_connection
               SET ended_ts_recv_us = MAX(started_ts_recv_us, ?),
                   end_reason = 'partition_rotation'
               WHERE ended_ts_recv_us IS NULL""",
            (partition_end_us,),
        )
    if "data_gap" in tables:
        marker_dir = Path(sys.argv[3])
        connection.execute(
            """DELETE FROM data_gap
               WHERE ts_end_us IS NULL AND channel = 'connection'
                 AND gap_id NOT IN (
                   SELECT MIN(gap_id) FROM data_gap
                   WHERE ts_end_us IS NULL AND channel = 'connection'
                   GROUP BY venue, market_id, channel
                 )"""
        )
        rows = connection.execute(
            """SELECT gap_id, venue, market_id, symbol, channel,
                      expected_sequence, observed_sequence, reason
               FROM data_gap WHERE ts_end_us IS NULL"""
        ).fetchall()
        for gap_id, venue, market_id, symbol, channel, expected, observed, reason in rows:
            marker = {
                "continuation_id": f"partition:{sys.argv[2]}:{gap_id}",
                "start_us": partition_end_us,
                "venue": venue,
                "market_id": market_id,
                "symbol": symbol,
                "channel": channel,
                "expected_sequence": expected,
                "observed_sequence": observed,
                "reason": reason,
                "source_partition": sys.argv[2],
                "source_gap_id": gap_id,
            }
            marker_path = marker_dir / f"{sys.argv[2]}-{gap_id}.json"
            encoded = json.dumps(marker, sort_keys=True, separators=(",", ":")) + "\n"
            if marker_path.exists():
                if marker_path.read_text() != encoded:
                    raise RuntimeError(f"gap continuation marker mismatch: {marker_path}")
            else:
                temporary = marker_path.with_suffix(".tmp")
                with temporary.open("w") as output:
                    output.write(encoded)
                    output.flush()
                    os.fsync(output.fileno())
                os.chmod(temporary, 0o640)
                os.replace(temporary, marker_path)
                directory_fd = os.open(marker_dir, os.O_RDONLY)
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
        connection.execute(
            """UPDATE data_gap
               SET ts_end_us = MAX(ts_start_us, ?)
               WHERE ts_end_us IS NULL""",
            (partition_end_us,),
        )
    if "ohlcv_1m" in tables or "ws_connection" in tables or "data_gap" in tables:
        connection.commit()
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

  # A committed late_trade row is the durable journal for its separately
  # maintained sealed-hour identity. Never archive or delete that journal
  # until every referenced sidecar has accepted the identity.
  "$PYTHON_BIN" "$OBSERVER_SCRIPT" --reconcile-late-trade-identities \
    "$db" "$SEALED_DIR"
  republish_reconciled_sidecars "$db"

  source_fingerprint=$(stat -c '%s:%Y:%y' "$db")
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
    "$PYTHON_BIN" "$OBSERVER_SCRIPT" --build-sealed-trade-index \
      "$db" "$trade_index_tmp" "$partition" "$expected_db_sha"
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
    printf '{"partition":"%s","s3_key":"s3://%s/%s","sha256":"%s","trade_index":"%s","trade_index_s3_key":"s3://%s/%s.trade_ids.sqlite3","seal_s3_key":"s3://%s/%s.seal.json","trade_identity_count":%s,"sealed_at":"%s"}\n' \
      "$partition" "$S3_BUCKET" "$key" "$expected_db_sha" \
      "$(basename "$trade_index_path")" "$S3_BUCKET" "$key" \
      "$S3_BUCKET" "$key" "$trade_identity_count" "$sealed_at" > "$seal_tmp"
    chmod 0640 "$seal_tmp"
    mv "$seal_tmp" "$seal_path"
    fsync_files_and_directory "$SEALED_DIR" "$trade_index_path" "$seal_path"
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
      fsync_directory "$SEALED_DIR"
      seal_created=false
    fi
  fi
  if [ "$DELETE_VERIFIED_LOCAL" = "true" ] && [ "$source_stable" = "true" ]; then
    persist_seal_sidecars \
      "$db" "$trade_index_path" "$seal_path" "s3://$S3_BUCKET/$key"
    if ! rm -f -- "$db"; then
      echo "Remote seal is committed but local deletion failed; retaining local seal for recovery: $db" >&2
      exit 1
    fi
    fsync_directory "$DATA_DIR"
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
