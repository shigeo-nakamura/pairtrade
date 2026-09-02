#!/bin/bash
set -euo pipefail

ROOT=$(mktemp -d /tmp/test-engine-b-archive.XXXXXX)
trap 'rm -rf -- "$ROOT"' EXIT
DATA_DIR="$ROOT/data"
FAKE_BIN="$ROOT/bin"
FAKE_S3="$ROOT/s3"
mkdir -p "$DATA_DIR" "$FAKE_BIN" "$FAKE_S3"

current=$(date -u +%Y%m%d_%H)
current_db="$DATA_DIR/engine_b_phase0_${current}.sqlite3"
old_db="$DATA_DIR/engine_b_phase0_20000101_00.sqlite3"
python3 - "$current_db" <<'PY'
import sqlite3
import sys

connection = sqlite3.connect(sys.argv[1])
connection.execute("CREATE TABLE sample(value TEXT NOT NULL)")
connection.execute("INSERT INTO sample VALUES ('current')")
connection.commit()
connection.close()
PY
python3 - "$old_db" <<'PY'
import os
import sqlite3
import sys

connection = sqlite3.connect(sys.argv[1])
connection.execute("PRAGMA journal_mode=WAL")
connection.execute("CREATE TABLE sample(value TEXT NOT NULL)")
connection.execute("CREATE TABLE ohlcv_1m(is_complete INTEGER NOT NULL)")
connection.execute(
    """CREATE TABLE ws_connection(
         connection_session_id TEXT PRIMARY KEY,
         venue TEXT NOT NULL,
         channel TEXT NOT NULL,
         started_ts_recv_us INTEGER NOT NULL,
         ended_ts_recv_us INTEGER,
         api_schema_version TEXT NOT NULL,
         end_reason TEXT
       )"""
)
connection.execute(
    """CREATE TABLE data_gap(
         gap_id INTEGER PRIMARY KEY AUTOINCREMENT,
         venue TEXT NOT NULL,
         market_id INTEGER,
         symbol TEXT,
         channel TEXT NOT NULL,
         ts_start_us INTEGER NOT NULL,
         ts_end_us INTEGER,
         expected_sequence TEXT,
         observed_sequence TEXT,
         reason TEXT NOT NULL
       )"""
)
connection.execute(
    """CREATE TABLE trade(
         trade_row_id INTEGER PRIMARY KEY AUTOINCREMENT,
         connection_session_id TEXT NOT NULL,
         venue TEXT NOT NULL,
         market_id INTEGER NOT NULL,
         exchange_trade_id TEXT,
         exchange_sequence TEXT,
         local_sequence INTEGER NOT NULL,
         ts_recv_us INTEGER NOT NULL,
         ts_srv_us INTEGER,
         raw_public_json TEXT NOT NULL
       )"""
)
connection.execute("INSERT INTO sample VALUES ('recovered-from-wal')")
connection.execute("INSERT INTO ohlcv_1m VALUES (0)")
connection.execute(
    "INSERT INTO ws_connection VALUES (?, ?, ?, ?, NULL, ?, NULL)",
    (
        "legacy-connection",
        "robinhood",
        "multiplexed_public",
        946_684_800_000_000,
        "2026-01",
    ),
)
connection.execute(
    """INSERT INTO data_gap(
         venue, market_id, symbol, channel, ts_start_us, reason
       ) VALUES ('robinhood', 37, 'SKHY', 'connection', ?, 'test_outage')""",
    (946_684_800_000_000,),
)
connection.execute(
    """INSERT INTO trade(
         connection_session_id, venue, market_id, exchange_trade_id,
         exchange_sequence, local_sequence, ts_recv_us, ts_srv_us,
         raw_public_json
       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
    (
        "legacy-connection",
        "robinhood",
        37,
        "canonical-trade",
        "9001",
        1,
        1_774_884_082_400_000,
        1_774_884_082_309_000,
        '{"price":"101","size":"2"}',
    ),
)
connection.execute(
    """INSERT INTO trade(
         connection_session_id, venue, market_id, exchange_trade_id,
         exchange_sequence, local_sequence, ts_recv_us, ts_srv_us,
         raw_public_json
       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
    (
        "legacy-connection",
        "robinhood",
        37,
        None,
        "9001",
        2,
        1_774_884_082_400_000,
        1_774_884_082_309_000,
        '{"price":"101","size":"2"}',
    ),
)
connection.execute(
    """INSERT INTO trade(
         connection_session_id, venue, market_id, exchange_trade_id,
         exchange_sequence, local_sequence, ts_recv_us, ts_srv_us,
         raw_public_json
       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
    (
        "legacy-connection",
        "robinhood",
        37,
        "synthetic:obsolete-hash",
        "9001",
        3,
        1_774_884_082_400_000,
        1_774_884_082_309_000,
        '{"price":"101","size":"2"}',
    ),
)
connection.execute(
    """INSERT INTO trade(
         connection_session_id, venue, market_id, exchange_trade_id,
         exchange_sequence, local_sequence, ts_recv_us, ts_srv_us,
         raw_public_json
       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
    (
        "legacy-connection",
        "robinhood",
        37,
        "synthetic:v3:preserve-me",
        "9001",
        4,
        1_774_884_082_400_000,
        1_774_884_082_309_000,
        '{"price":"101","size":"2"}',
    ),
)
connection.commit()
os._exit(0)
PY
test -e "$old_db-wal"
test -e "$old_db-shm"

cat > "$FAKE_BIN/aws" <<'FAKE'
#!/bin/bash
set -euo pipefail
if [ "$1 $2" = "s3 cp" ]; then
  source=$3
  destination=$4
  if [[ "$destination" == s3://test-bucket/* ]]; then
    key=${destination#s3://test-bucket/}
    output="$FAKE_S3/$key"
    mkdir -p "$(dirname "$output")"
    if [ "$source" = "-" ]; then
      cp /dev/stdin "$output"
    else
      cp "$source" "$output"
    fi
    if [ "${FAKE_CORRUPT_UPLOAD:-0}" = "1" ] && [[ "$key" == *.sqlite3.gz ]]; then
      printf 'truncated' > "$output"
    fi
  elif [[ "$source" == s3://test-bucket/* ]]; then
    key=${source#s3://test-bucket/}
    if [ "$destination" = "-" ]; then
      cat "$FAKE_S3/$key"
    else
      cp "$FAKE_S3/$key" "$destination"
    fi
  else
    exit 2
  fi
elif [ "$1 $2" = "s3api head-object" ]; then
  shift 2
  key=""
  query=""
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --key) key=$2; shift 2 ;;
      --query) query=$2; shift 2 ;;
      *) shift ;;
    esac
  done
  if [ "$query" = "ContentLength" ]; then
    wc -c < "$FAKE_S3/$key"
  elif [ "$query" = "ServerSideEncryption" ]; then
    echo AES256
  else
    exit 2
  fi
else
  exit 2
fi
FAKE
chmod +x "$FAKE_BIN/aws"

FAKE_S3="$FAKE_S3" PATH="$FAKE_BIN:$PATH" \
ENGINE_B_PHASE0_DATA_DIR="$DATA_DIR" \
ENGINE_B_PHASE0_S3_BUCKET=test-bucket \
ENGINE_B_PHASE0_S3_PREFIX=test-prefix \
ENGINE_B_PHASE0_PYTHON=python3 \
ENGINE_B_PHASE0_DELETE_VERIFIED_LOCAL=true \
bash "$(dirname "$0")/engine_b_phase0_archive.sh"

test -f "$current_db"
test ! -e "$old_db"
test ! -e "$old_db-wal"
test ! -e "$old_db-shm"
archive=$(find "$FAKE_S3" -type f -name 'engine_b_phase0_20000101_00.sqlite3.gz')
checksum=$(find "$FAKE_S3" -type f -name 'engine_b_phase0_20000101_00.sqlite3.gz.sha256')
test -n "$archive"
test -n "$checksum"
gzip -t "$archive"
restored_db="$ROOT/restored.sqlite3"
gzip -dc "$archive" > "$restored_db"
python3 - "$restored_db" <<'PY'
import sqlite3
import sys
from datetime import datetime, timedelta, timezone

connection = sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True)
try:
    assert connection.execute("SELECT value FROM sample").fetchone() == ("recovered-from-wal",)
    assert connection.execute("SELECT is_complete FROM ohlcv_1m").fetchone() == (1,)
    partition_end = (
        datetime.strptime("20000101_00", "%Y%m%d_%H").replace(tzinfo=timezone.utc)
        + timedelta(hours=1)
    )
    assert connection.execute(
        "SELECT ended_ts_recv_us, end_reason FROM ws_connection"
    ).fetchone() == (
        int(partition_end.timestamp() * 1_000_000),
        "partition_rotation",
    )
    assert connection.execute("SELECT ts_end_us FROM data_gap").fetchone() == (
        int(partition_end.timestamp() * 1_000_000),
    )
    assert connection.execute("PRAGMA integrity_check").fetchone() == ("ok",)
finally:
    connection.close()
PY
expected_sha=$(sha256sum "$restored_db" | cut -d' ' -f1)
stored_sha=$(cut -d' ' -f1 "$checksum")
test "$stored_sha" = "$expected_sha"
grep -Eq '^[0-9a-f]{64}  engine_b_phase0_20000101_00.sqlite3$' "$checksum"
seal="$ROOT/sealed/20000101_00.json"
trade_index="$ROOT/sealed/20000101_00.trade_ids.sqlite3"
test -f "$seal"
test -f "$trade_index"
remote_trade_index=$(find "$FAKE_S3" -type f -name '*.trade_ids.sqlite3')
remote_seal=$(find "$FAKE_S3" -type f -name '*.seal.json')
test -n "$remote_trade_index"
test -n "$remote_seal"
cmp -s "$trade_index" "$remote_trade_index"
cmp -s "$seal" "$remote_seal"
continuation_marker="$ROOT/gap-continuations/20000101_00-1.json"
test -f "$continuation_marker"
python3 - "$continuation_marker" <<'PY'
import json
import sys
marker = json.load(open(sys.argv[1]))
assert marker["continuation_id"] == "partition:20000101_00:1"
assert marker["start_us"] == 946_688_400_000_000
assert marker["source_partition"] == "20000101_00"
assert marker["source_gap_id"] == 1
PY
session_marker=$(find "$ROOT/session-continuations" -type f -name '*.json')
test -n "$session_marker"
python3 - "$session_marker" <<'PY'
import json
import sys
marker = json.load(open(sys.argv[1]))
assert marker["continuation_id"] == "partition:20000101_00:legacy-connection"
assert marker["start_us"] == 946_688_400_000_000
assert marker["source_partition"] == "20000101_00"
assert marker["connection"] == {
    "id": "legacy-connection",
    "venue": "robinhood",
    "started_us": 946_688_400_000_000,
    "api_schema_version": "2026-01",
}
PY
python3 - "$seal" "$trade_index" "$expected_sha" <<'PY'
import json
import hashlib
import sqlite3
import sys

seal = json.load(open(sys.argv[1]))
assert seal["partition"] == "20000101_00"
assert seal["trade_index"] == "20000101_00.trade_ids.sqlite3"
assert seal["trade_identity_count"] == 7
assert seal["sha256"] == sys.argv[3]
assert seal["trade_index_s3_key"].endswith(".trade_ids.sqlite3")
assert seal["seal_s3_key"].endswith(".seal.json")
connection = sqlite3.connect(f"file:{sys.argv[2]}?mode=ro", uri=True)
try:
    assert connection.execute("SELECT partition FROM sealed_metadata").fetchone() == (
        "20000101_00",
    )
    identities = connection.execute(
        "SELECT venue, market_id, exchange_trade_id FROM archived_trade_identity"
        " ORDER BY exchange_trade_id"
    ).fetchall()
    assert ("robinhood", 37, "canonical-trade") in identities
    assert ("robinhood", 37, "synthetic:v3:preserve-me") in identities
    for position in (1, 2):
        identity = {
            "venue": "robinhood",
            "market_id": 37,
            "event_ts_us": 1_774_884_082_309_000,
            "stable_occurrence": position,
            "raw_public_json": '{"price":"101","size":"2"}',
        }
        encoded_identity = json.dumps(
            identity,
            sort_keys=True,
            separators=(",", ":"),
        )
        expected_synthetic = (
            "synthetic:v3:" + hashlib.sha256(encoded_identity.encode()).hexdigest()
        )
        assert ("robinhood", 37, expected_synthetic) in identities
        identity["message_scope"] = "update/trade:9001"
        expected_update_synthetic = (
            "synthetic:v3:"
            + hashlib.sha256(
                json.dumps(
                    identity, sort_keys=True, separators=(",", ":")
                ).encode()
            ).hexdigest()
        )
        assert ("robinhood", 37, expected_update_synthetic) in identities
    assert not any(row[2] == "synthetic:obsolete-hash" for row in identities)
    assert connection.execute(
        "SELECT COUNT(*) FROM archived_gap_continuation"
    ).fetchone() == (0,)
    assert connection.execute(
        "SELECT connection_session_id FROM archived_connection_session"
    ).fetchone() == ("legacy-connection",)
    assert connection.execute(
        "SELECT COUNT(*) FROM archived_trade_replay_alias"
    ).fetchone()[0] >= 1
    assert connection.execute("SELECT COUNT(*) FROM late_trade_identity").fetchone() == (0,)
    assert connection.execute("PRAGMA integrity_check").fetchone() == ("ok",)
finally:
    connection.close()
PY
late_db="$DATA_DIR/engine_b_phase0_20000101_02.sqlite3"
python3 - "$late_db" <<'PY'
import sqlite3
import sys

connection = sqlite3.connect(sys.argv[1])
connection.execute(
    """CREATE TABLE late_trade(
         sealed_partition TEXT NOT NULL,
         venue TEXT NOT NULL,
         market_id INTEGER NOT NULL,
         exchange_trade_id TEXT NOT NULL,
         replay_alias TEXT
       )"""
)
connection.execute(
    """INSERT INTO late_trade VALUES(
         '20000101_00', 'robinhood', 37, 'late-republish',
         'synthetic-replay:v1:late-republish'
       )"""
)
connection.commit()
connection.close()
PY
FAKE_S3="$FAKE_S3" PATH="$FAKE_BIN:$PATH" \
ENGINE_B_PHASE0_DATA_DIR="$DATA_DIR" \
ENGINE_B_PHASE0_S3_BUCKET=test-bucket \
ENGINE_B_PHASE0_S3_PREFIX=test-prefix \
ENGINE_B_PHASE0_PYTHON=python3 \
ENGINE_B_PHASE0_DELETE_VERIFIED_LOCAL=true \
bash "$(dirname "$0")/engine_b_phase0_archive.sh"
test ! -e "$late_db"
cmp -s "$trade_index" "$remote_trade_index"
python3 - "$remote_trade_index" <<'PY'
import sqlite3
import sys
connection = sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True)
try:
    assert connection.execute(
        """SELECT 1 FROM late_trade_identity
           WHERE exchange_trade_id = 'late-republish'"""
    ).fetchone() == (1,)
    assert connection.execute(
        """SELECT exchange_trade_id FROM archived_trade_replay_alias
           WHERE replay_alias = 'synthetic-replay:v1:late-republish'"""
    ).fetchone() == ("late-republish",)
finally:
    connection.close()
PY
canonical_sha=$(sha256sum "$archive" | cut -d' ' -f1)
trade_index_sha=$(sha256sum "$trade_index" | cut -d' ' -f1)
cp "$restored_db" "$old_db"
FAKE_S3="$FAKE_S3" PATH="$FAKE_BIN:$PATH" \
ENGINE_B_PHASE0_DATA_DIR="$DATA_DIR" \
ENGINE_B_PHASE0_S3_BUCKET=test-bucket \
ENGINE_B_PHASE0_S3_PREFIX=test-prefix \
ENGINE_B_PHASE0_PYTHON=python3 \
ENGINE_B_PHASE0_DELETE_VERIFIED_LOCAL=true \
bash "$(dirname "$0")/engine_b_phase0_archive.sh"
test ! -e "$old_db"
test "$(sha256sum "$archive" | cut -d' ' -f1)" = "$canonical_sha"
test "$(sha256sum "$trade_index" | cut -d' ' -f1)" = "$trade_index_sha"

python3 - "$old_db" <<'PY'
import sqlite3
import sys

connection = sqlite3.connect(sys.argv[1])
connection.execute("CREATE TABLE fragment(value TEXT NOT NULL)")
connection.execute("INSERT INTO fragment VALUES ('late-only')")
connection.execute(
    """CREATE TABLE late_trade(
         sealed_partition TEXT NOT NULL,
         venue TEXT NOT NULL,
         market_id INTEGER NOT NULL,
         exchange_trade_id TEXT NOT NULL
       )"""
)
connection.execute(
    "INSERT INTO late_trade VALUES ('20000101_00', 'robinhood', 37, 'must-not-reconcile')"
)
connection.commit()
connection.close()
PY
if FAKE_S3="$FAKE_S3" PATH="$FAKE_BIN:$PATH" \
    ENGINE_B_PHASE0_DATA_DIR="$DATA_DIR" \
    ENGINE_B_PHASE0_S3_BUCKET=test-bucket \
    ENGINE_B_PHASE0_S3_PREFIX=test-prefix \
    ENGINE_B_PHASE0_PYTHON=python3 \
    ENGINE_B_PHASE0_DELETE_VERIFIED_LOCAL=true \
    bash "$(dirname "$0")/engine_b_phase0_archive.sh"; then
  echo "Mismatched sealed fragment was removed or re-archived" >&2
  exit 1
fi
test -f "$old_db"
test "$(sha256sum "$archive" | cut -d' ' -f1)" = "$canonical_sha"
test "$(sha256sum "$trade_index" | cut -d' ' -f1)" = "$trade_index_sha"
rm -f -- "$old_db"

corrupt_db="$DATA_DIR/engine_b_phase0_20000101_01.sqlite3"
python3 - "$corrupt_db" <<'PY'
import sqlite3
import sys

connection = sqlite3.connect(sys.argv[1])
connection.execute("CREATE TABLE sample(value TEXT NOT NULL)")
connection.execute("INSERT INTO sample VALUES ('must-remain-local')")
connection.commit()
connection.close()
PY
if FAKE_CORRUPT_UPLOAD=1 FAKE_S3="$FAKE_S3" PATH="$FAKE_BIN:$PATH" \
    ENGINE_B_PHASE0_DATA_DIR="$DATA_DIR" \
    ENGINE_B_PHASE0_S3_BUCKET=test-bucket \
    ENGINE_B_PHASE0_S3_PREFIX=test-prefix \
    ENGINE_B_PHASE0_PYTHON=python3 \
    ENGINE_B_PHASE0_DELETE_VERIFIED_LOCAL=true \
    bash "$(dirname "$0")/engine_b_phase0_archive.sh"; then
  echo "Corrupt remote archive was accepted" >&2
  exit 1
fi
test -f "$corrupt_db"

echo "Engine B archive test passed"
