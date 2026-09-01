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
    "CREATE TABLE trade(venue TEXT NOT NULL, market_id INTEGER NOT NULL, exchange_trade_id TEXT NOT NULL)"
)
connection.execute("INSERT INTO sample VALUES ('recovered-from-wal')")
connection.execute("INSERT INTO ohlcv_1m VALUES (0)")
connection.execute("INSERT INTO trade VALUES ('robinhood', 37, 'canonical-trade')")
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

connection = sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True)
try:
    assert connection.execute("SELECT value FROM sample").fetchone() == ("recovered-from-wal",)
    assert connection.execute("SELECT is_complete FROM ohlcv_1m").fetchone() == (1,)
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
python3 - "$seal" "$trade_index" "$expected_sha" <<'PY'
import json
import sqlite3
import sys

seal = json.load(open(sys.argv[1]))
assert seal["partition"] == "20000101_00"
assert seal["trade_index"] == "20000101_00.trade_ids.sqlite3"
assert seal["trade_identity_count"] == 1
assert seal["sha256"] == sys.argv[3]
connection = sqlite3.connect(f"file:{sys.argv[2]}?mode=ro", uri=True)
try:
    assert connection.execute("SELECT partition FROM sealed_metadata").fetchone() == (
        "20000101_00",
    )
    assert connection.execute(
        "SELECT venue, market_id, exchange_trade_id FROM archived_trade_identity"
    ).fetchone() == ("robinhood", 37, "canonical-trade")
    assert connection.execute("PRAGMA integrity_check").fetchone() == ("ok",)
finally:
    connection.close()
PY
canonical_sha=$(sha256sum "$archive" | cut -d' ' -f1)
trade_index_sha=$(sha256sum "$trade_index" | cut -d' ' -f1)
python3 - "$old_db" <<'PY'
import sqlite3
import sys

connection = sqlite3.connect(sys.argv[1])
connection.execute("CREATE TABLE fragment(value TEXT NOT NULL)")
connection.execute("INSERT INTO fragment VALUES ('late-only')")
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
test -f "$old_db"
test "$(sha256sum "$archive" | cut -d' ' -f1)" = "$canonical_sha"
test "$(sha256sum "$trade_index" | cut -d' ' -f1)" = "$trade_index_sha"

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
