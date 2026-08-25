#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT=$(cd "$(dirname "$0")/.." && pwd)
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
STREAM_DIR="$WORK/state/live-tick-events"
FAKE_S3="$WORK/s3"
mkdir -p "$STREAM_DIR" "$FAKE_S3" "$WORK/bin"

python3 - "$REPO_ROOT/scripts" "$STREAM_DIR" <<'PY'
import importlib.util
import json
import pathlib
import sys

scripts, stream_dir = map(pathlib.Path, sys.argv[1:])
spec = importlib.util.spec_from_file_location(
    "stream", scripts / "arcus_live_tick_event_stream.py")
stream = importlib.util.module_from_spec(spec)
assert spec.loader
spec.loader.exec_module(stream)

previous = None
for sequence, observed_at in (
    (70, "2026-08-23T23:47:00.123456789Z"),
    (71, "2026-08-24T00:02:00Z"),
):
    event = {
        "sequence": sequence,
        "observed_at": observed_at,
        "relative_log_price": 0.5,
        "z_score": 1.0,
        "decision": {"observe": {"hold": {"code": "no_signal"}}},
    }
    event_json = json.dumps(event, separators=(",", ":"))
    event_sha = stream.sha256_prefixed(event_json.encode())
    chain_sha = stream.chain_sha256(previous, event_sha)
    record = {
        "schema_version": 1,
        "previous_chain_sha256": previous,
        "event_sha256": event_sha,
        "chain_sha256": chain_sha,
        "event_json": event_json,
    }
    day = observed_at[:10]
    with open(stream_dir / f"{day}.jsonl", "a", encoding="utf-8") as output:
        output.write(json.dumps(record, separators=(",", ":")) + "\n")
    previous = chain_sha
for path in stream_dir.iterdir():
    path.chmod(0o600)
PY

cat > "$WORK/bin/aws" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
test "$1" = s3api
action=$2
shift 2
bucket=
key=
body=
destination=
while [ "$#" -gt 0 ]; do
  case "$1" in
    --bucket) bucket=$2; shift 2 ;;
    --key) key=$2; shift 2 ;;
    --body) body=$2; shift 2 ;;
    --content-type) shift 2 ;;
    --if-none-match) shift 2 ;;
    --*) echo "unexpected fake aws option: $1" >&2; exit 98 ;;
    *) destination=$1; shift ;;
  esac
done
test -n "$bucket"
test -n "$key"
object="$FAKE_S3/$bucket/$key"
case "$action" in
  head-object)
    test -f "$object"
    printf '{}\n'
    ;;
  get-object)
    test -n "$destination"
    test -f "$object"
    cp "$object" "$destination"
    printf '{}\n'
    ;;
  put-object)
    test -n "$body"
    if [ -e "$object" ]; then
      exit 1
    fi
    mkdir -p "$(dirname "$object")"
    cp "$body" "$object"
    printf '{}\n'
    ;;
  *) echo "unexpected fake aws action: $action" >&2; exit 98 ;;
esac
EOF
chmod +x "$WORK/bin/aws"

run_archive() {
  PATH="$WORK/bin:$PATH" \
  FAKE_S3="$FAKE_S3" \
  STREAM_DIR="$STREAM_DIR" \
  VERIFY_SCRIPT="$REPO_ROOT/scripts/arcus_live_tick_event_stream.py" \
  S3_BUCKET=test-private \
  S3_PREFIX=arcus-archive/live-tick-events/test-host \
    bash "$REPO_ROOT/scripts/archive_arcus_live_tick_events.sh" "$1"
}

# A fresh deployment has no segment for the prior day; that expected boundary
# is a no-op. Once a first segment exists, a later missing day is an error.
STREAM_DIR="$WORK/not-initialized" run_archive 2026-08-22
STREAM_DIR="$STREAM_DIR" run_archive 2026-08-22
mkdir -p "$WORK/gapped"
cp "$STREAM_DIR/2026-08-23.jsonl" "$WORK/gapped/"
if STREAM_DIR="$WORK/gapped" run_archive 2026-08-24; then
  echo "expected a post-initialization missing day to fail" >&2
  exit 1
fi

run_archive 2026-08-23
run_archive 2026-08-24

DATA_23="$FAKE_S3/test-private/arcus-archive/live-tick-events/test-host/2026/08/2026-08-23.events.jsonl.gz"
MANIFEST_23="$FAKE_S3/test-private/arcus-archive/live-tick-events/test-host/2026/08/2026-08-23.manifest.json"
test -f "$DATA_23"
test -f "$MANIFEST_23"
cp "$MANIFEST_23" "$WORK/first-manifest.json"
run_archive 2026-08-23
cmp "$WORK/first-manifest.json" "$MANIFEST_23"

PATH="$WORK/bin:$PATH" \
FAKE_S3="$FAKE_S3" \
VERIFY_SCRIPT="$REPO_ROOT/scripts/arcus_live_tick_event_stream.py" \
S3_BUCKET=test-private \
S3_PREFIX=arcus-archive/live-tick-events/test-host \
  bash "$REPO_ROOT/scripts/fetch_arcus_live_tick_events.sh" \
    2026-08-23 2026-08-24 "$WORK/fetched.jsonl"

python3 "$REPO_ROOT/scripts/arcus_live_tick_event_stream.py" \
  "$WORK/fetched.jsonl" --manifest-out "$WORK/fetched.manifest.json" >/dev/null
test "$(wc -l < "$WORK/fetched.jsonl")" -eq 2
test "$(wc -l < "$WORK/fetched.jsonl.manifests.jsonl")" -eq 2
python3 - "$MANIFEST_23" <<'PY'
import json
import sys
with open(sys.argv[1], encoding="utf-8") as source:
    manifest = json.load(source)
assert manifest["storage"]["private"] is True
assert manifest["storage"]["current_version_retention"] == "indefinite"
assert manifest["closed_at"] == "2026-08-24T00:00:00Z"
PY

printf 'tamper' >> "$DATA_23"
if PATH="$WORK/bin:$PATH" \
   FAKE_S3="$FAKE_S3" \
   VERIFY_SCRIPT="$REPO_ROOT/scripts/arcus_live_tick_event_stream.py" \
   S3_BUCKET=test-private \
   S3_PREFIX=arcus-archive/live-tick-events/test-host \
     bash "$REPO_ROOT/scripts/fetch_arcus_live_tick_events.sh" \
       2026-08-23 2026-08-24 "$WORK/tampered.jsonl"; then
  echo "expected compressed archive integrity failure" >&2
  exit 1
fi
test ! -e "$WORK/tampered.jsonl"
test ! -e "$WORK/tampered.jsonl.manifests.jsonl"

echo "Arcus live-tick event archive tests passed"
