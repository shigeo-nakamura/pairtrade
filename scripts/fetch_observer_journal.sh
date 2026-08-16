#!/bin/bash
# Fetch archived observer journal lines for an exact UTC window.
#
# Usage:
#   scripts/fetch_observer_journal.sh HOST_TAG SERVICE SINCE UNTIL OUT_FILE
#
# Daily manifests are written next to OUT_FILE as OUT_FILE.manifests.jsonl
# so retention gaps remain visible to the analyst.
set -euo pipefail

HOST_TAG="${1:?usage: $0 HOST_TAG SERVICE SINCE UNTIL OUT_FILE}"
SERVICE="${2:?usage: $0 HOST_TAG SERVICE SINCE UNTIL OUT_FILE}"
SINCE="${3:?usage: $0 HOST_TAG SERVICE SINCE UNTIL OUT_FILE}"
UNTIL="${4:?usage: $0 HOST_TAG SERVICE SINCE UNTIL OUT_FILE}"
OUT_FILE="${5:?usage: $0 HOST_TAG SERVICE SINCE UNTIL OUT_FILE}"

case "$HOST_TAG" in
    frankfurt|tokyo) ;;
    *) echo "ERROR: HOST_TAG must be frankfurt or tokyo, got: $HOST_TAG" >&2; exit 64 ;;
esac

S3_BUCKET="${S3_BUCKET:-debot-dashboard}"
S3_PREFIX="${S3_PREFIX:-debot/observer-journal}"

read -r SINCE_TS UNTIL_TS START_DATE END_DATE <<EOF
$(python3 - "$SINCE" "$UNTIL" <<'PYEOF'
import sys
from datetime import datetime, timedelta, timezone

def parse(value):
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(value.strip(), fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    raise SystemExit(f"ERROR: cannot parse {value!r} as UTC")

since = parse(sys.argv[1])
until = parse(sys.argv[2])
if until <= since:
    raise SystemExit("ERROR: UNTIL must be after SINCE")
print(
    int(since.timestamp()),
    int(until.timestamp()),
    since.strftime("%Y-%m-%d"),
    (until - timedelta(microseconds=1)).strftime("%Y-%m-%d"),
)
PYEOF
)
EOF

mkdir -p "$(dirname "$OUT_FILE")"
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
mkdir -p "$WORK/journal" "$WORK/manifest"

DATES=$(python3 - "$START_DATE" "$END_DATE" <<'PYEOF'
import sys
from datetime import datetime, timedelta

current = datetime.strptime(sys.argv[1], "%Y-%m-%d")
end = datetime.strptime(sys.argv[2], "%Y-%m-%d")
while current <= end:
    print(current.strftime("%Y-%m-%d"))
    current += timedelta(days=1)
PYEOF
)

JOURNAL_PREFIX="s3://${S3_BUCKET}/${S3_PREFIX}/${HOST_TAG}/${SERVICE}/journal"
MANIFEST_PREFIX="s3://${S3_BUCKET}/${S3_PREFIX}/${HOST_TAG}/${SERVICE}/manifest"
MISSING_DAYS=0
MISSING_MANIFESTS=0

echo "[fetch_observer_journal] host=$HOST_TAG service=$SERVICE"
echo "[fetch_observer_journal]   window: $SINCE -> $UNTIL"

for day in $DATES; do
    if ! aws s3 cp --no-progress "$JOURNAL_PREFIX/$day.log.gz" "$WORK/journal/$day.log.gz" 2>/dev/null; then
        echo "[fetch_observer_journal]   WARN: missing journal day $day"
        MISSING_DAYS=$((MISSING_DAYS + 1))
    fi
    if ! aws s3 cp --no-progress "$MANIFEST_PREFIX/$day.json" "$WORK/manifest/$day.json" 2>/dev/null; then
        echo "[fetch_observer_journal]   WARN: missing manifest day $day"
        MISSING_MANIFESTS=$((MISSING_MANIFESTS + 1))
    fi
done

python3 - "$WORK/journal" "$OUT_FILE" "$SINCE_TS" "$UNTIL_TS" <<'PYEOF'
import gzip
import os
import re
import sys
from datetime import datetime

source_dir, output, since_s, until_s = sys.argv[1:]
since = int(since_s)
until = int(until_s)
outer_ts = re.compile(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?[+-]\d{4})\s")
written = 0

with open(output, "w", encoding="utf-8") as dst:
    for name in sorted(os.listdir(source_dir)):
        with gzip.open(os.path.join(source_dir, name), "rt", encoding="utf-8", errors="replace") as src:
            for line in src:
                match = outer_ts.match(line)
                if not match:
                    continue
                stamp = int(datetime.strptime(match.group(1), "%Y-%m-%dT%H:%M:%S.%f%z").timestamp())
                if since <= stamp < until:
                    dst.write(line)
                    written += 1
print(f"[fetch_observer_journal]   selected lines: {written} -> {output}")
PYEOF

python3 - "$WORK/manifest" "$OUT_FILE.manifests.jsonl" <<'PYEOF'
import json
import os
import sys

source_dir, output = sys.argv[1:]
items = []
for name in sorted(os.listdir(source_dir)):
    with open(os.path.join(source_dir, name), encoding="utf-8") as src:
        items.append(json.load(src))
with open(output, "w", encoding="utf-8") as dst:
    for item in items:
        dst.write(json.dumps(item, sort_keys=True) + "\n")
print(f"[fetch_observer_journal]   manifests: {len(items)} -> {output}")
PYEOF

if [ "$MISSING_DAYS" -gt 0 ]; then
    echo "[fetch_observer_journal]   WARN: $MISSING_DAYS requested day(s) missing"
fi
if [ "$MISSING_MANIFESTS" -gt 0 ]; then
    echo "[fetch_observer_journal]   WARN: $MISSING_MANIFESTS requested day manifest(s) missing"
fi
