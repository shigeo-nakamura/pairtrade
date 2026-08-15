#!/bin/bash
# Archive the high-signal journal lines needed for observer readouts.
#
# The market-data dump is sufficient for cadence/gap analysis, but signal
# eligibility and runtime errors only exist in journald. On Frankfurt those
# logs can rotate in roughly a day, so persist one UTC day before retention
# removes it.
#
# S3 layout:
#   s3://debot-dashboard/debot/observer-journal/<host>/<service>/journal/<date>.log.gz
#   s3://debot-dashboard/debot/observer-journal/<host>/<service>/manifest/<date>.json
#
# Usage:
#   SERVICE=debot-pair-hyperliquid-observe scripts/archive_observer_journal.sh [DATE]
#
# DATE defaults to yesterday in UTC. Set ALLOW_PARTIAL_DAY=true only for a
# manual backfill of a day that has already been partly rotated.
set -euo pipefail

DATE="${1:-$(date -u -d 'yesterday' +%Y-%m-%d)}"

if ! [[ "$DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    echo "ERROR: DATE must be YYYY-MM-DD, got: $DATE" >&2
    exit 64
fi

SERVICE="${SERVICE:?set SERVICE to the systemd unit name}"
S3_BUCKET="${S3_BUCKET:-debot-dashboard}"
S3_PREFIX="${S3_PREFIX:-debot/observer-journal}"
ALLOW_PARTIAL_DAY="${ALLOW_PARTIAL_DAY:-false}"

if [ -z "${HOST_TAG:-}" ]; then
    REGION=$(curl -fsS --max-time 2 \
        http://169.254.169.254/latest/dynamic/instance-identity/document 2>/dev/null \
        | python3 -c 'import json,sys; print(json.load(sys.stdin)["region"])' \
        2>/dev/null || true)
    case "$REGION" in
        eu-central-1)   HOST_TAG=frankfurt ;;
        ap-northeast-1) HOST_TAG=tokyo ;;
        *) echo "ERROR: cannot derive HOST_TAG from region '$REGION'; set HOST_TAG explicitly" >&2; exit 1 ;;
    esac
fi

case "$ALLOW_PARTIAL_DAY" in
    true|false) ;;
    *) echo "ERROR: ALLOW_PARTIAL_DAY must be true or false" >&2; exit 64 ;;
esac

SINCE_UTC="${DATE} 00:00:00 UTC"
UNTIL_UTC="$(date -u -d "${DATE} +1 day" +%Y-%m-%d) 00:00:00 UTC"

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

echo "[archive_observer_journal] host=$HOST_TAG service=$SERVICE date=$DATE"
echo "[archive_observer_journal]   window: $SINCE_UTC -> $UNTIL_UTC"

# Store the raw day only in the temporary directory. The S3 artifact below
# keeps a deliberately small, non-secret subset: duty-cycle, paper-trade,
# warning, and error lines. Filtering by the outer journal timestamp also
# removes an event exactly at the inclusive UNTIL boundary.
sudo journalctl -u "$SERVICE" \
    --since "$SINCE_UTC" --until "$UNTIL_UTC" \
    --utc --no-pager -o short-iso-precise > "$WORK/raw.log"

python3 - "$WORK/raw.log" "$WORK/journal.log" "$WORK/manifest.json" \
    "$DATE" "$HOST_TAG" "$SERVICE" <<'PYEOF'
import json
import re
import sys
from datetime import datetime, timedelta, timezone

raw_path, log_path, manifest_path, date_s, host_tag, service = sys.argv[1:]
day = datetime.strptime(date_s, "%Y-%m-%d").replace(tzinfo=timezone.utc)
day_end = day + timedelta(days=1)
outer_ts = re.compile(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?[+-]\d{4})\s")
keep = re.compile(
    r"\[METRICS\]|\[ENTRY\]|\[(?:EXIT|CLOSE)\]|\[(?:WARN|ERROR)\]|"
    r"allMids missing symbol|disconnect|reconnect",
    re.IGNORECASE,
)

counts = {
    "selected_lines": 0,
    "metrics_lines": 0,
    "entry_lines": 0,
    "exit_lines": 0,
    "warning_lines": 0,
    "error_lines": 0,
}
first = None
last = None

with open(raw_path, encoding="utf-8", errors="replace") as src, open(
    log_path, "w", encoding="utf-8"
) as dst:
    for line in src:
        match = outer_ts.match(line)
        if not match:
            continue
        stamp = datetime.strptime(match.group(1), "%Y-%m-%dT%H:%M:%S.%f%z")
        if not day <= stamp < day_end or not keep.search(line):
            continue
        dst.write(line)
        counts["selected_lines"] += 1
        counts["metrics_lines"] += "[METRICS]" in line
        counts["entry_lines"] += "[ENTRY]" in line
        counts["exit_lines"] += "[EXIT]" in line or "[CLOSE]" in line
        counts["warning_lines"] += "[WARN]" in line
        counts["error_lines"] += "[ERROR]" in line
        first = stamp if first is None else min(first, stamp)
        last = stamp if last is None else max(last, stamp)

# METRICS normally arrive every five minutes. A selected event in the first
# and last ten minutes is therefore a simple, auditable completeness guard.
complete = bool(
    first
    and last
    and first <= day + timedelta(minutes=10)
    and last >= day_end - timedelta(minutes=10)
)
manifest = {
    "date_utc": date_s,
    "host_tag": host_tag,
    "service": service,
    "first_selected_event_utc": first.isoformat() if first else None,
    "last_selected_event_utc": last.isoformat() if last else None,
    "complete_day": complete,
    **counts,
}
with open(manifest_path, "w", encoding="utf-8") as out:
    json.dump(manifest, out, indent=2, sort_keys=True)
    out.write("\n")

print(json.dumps(manifest, sort_keys=True))
PYEOF

gzip -9 -c "$WORK/journal.log" > "$WORK/journal.log.gz"

JOURNAL_KEY="s3://${S3_BUCKET}/${S3_PREFIX}/${HOST_TAG}/${SERVICE}/journal/${DATE}.log.gz"
MANIFEST_KEY="s3://${S3_BUCKET}/${S3_PREFIX}/${HOST_TAG}/${SERVICE}/manifest/${DATE}.json"

aws s3 cp --no-progress "$WORK/journal.log.gz" "$JOURNAL_KEY"
aws s3 cp --no-progress "$WORK/manifest.json" "$MANIFEST_KEY"

echo "[archive_observer_journal] uploaded:"
echo "[archive_observer_journal]   $JOURNAL_KEY"
echo "[archive_observer_journal]   $MANIFEST_KEY"

COMPLETE_DAY=$(python3 -c 'import json,sys; print(str(json.load(open(sys.argv[1]))["complete_day"]).lower())' "$WORK/manifest.json")
if [ "$COMPLETE_DAY" != true ] && [ "$ALLOW_PARTIAL_DAY" != true ]; then
    echo "ERROR: selected journal does not cover the full UTC day; artifacts were uploaded with complete_day=false" >&2
    exit 1
fi
