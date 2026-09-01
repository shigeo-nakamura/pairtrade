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
python3 - "$current_db" "$old_db" <<'PY'
import sqlite3
import sys

for path in sys.argv[1:]:
    connection = sqlite3.connect(path)
    connection.execute("CREATE TABLE sample(value TEXT NOT NULL)")
    connection.execute("INSERT INTO sample VALUES ('public-data-only')")
    connection.commit()
    connection.close()
PY

cat > "$FAKE_BIN/aws" <<'FAKE'
#!/bin/bash
set -euo pipefail
if [ "$1 $2" = "s3 cp" ]; then
  destination=$4
  key=${destination#s3://test-bucket/}
  output="$FAKE_S3/$key"
  mkdir -p "$(dirname "$output")"
  cp /dev/stdin "$output"
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
bash "$(dirname "$0")/engine_b_phase0_archive.sh"

test -f "$current_db"
test ! -e "$old_db"
archive=$(find "$FAKE_S3" -type f -name 'engine_b_phase0_20000101_00.sqlite3.gz')
checksum=$(find "$FAKE_S3" -type f -name 'engine_b_phase0_20000101_00.sqlite3.gz.sha256')
test -n "$archive"
test -n "$checksum"
gzip -t "$archive"
grep -Eq '^[0-9a-f]{64}  .*/engine_b_phase0_20000101_00.sqlite3$' "$checksum"

echo "Engine B archive test passed"
