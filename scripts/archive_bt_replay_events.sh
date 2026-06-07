#!/bin/bash
# Archive yesterday's [EVAL] BTC/ETH + systemd-restart timestamps to S3.
#
# Runs on a Lighter pairtrade host (Frankfurt or Tokyo Lighter). Designed
# to be invoked daily from a systemd timer. Uses the same parsing logic
# as extract_bt_replay_events.sh so files written here are byte-identical
# to a same-day call of that script over the same window.
#
# S3 layout (matches the design in bot-strategy#255):
#   s3://debot-dashboard/debot/bt-archive/<host-tag>/<service>/eval_ts/<YYYY-MM-DD>.txt
#   s3://debot-dashboard/debot/bt-archive/<host-tag>/<service>/restart_ts/<YYYY-MM-DD>.txt
#
# Usage:
#   scripts/archive_bt_replay_events.sh [DATE]
#
# DATE is the UTC day to archive in YYYY-MM-DD form. Defaults to yesterday.
#
# Environment overrides (mostly for testing / replay):
#   S3_BUCKET   - default debot-dashboard
#   S3_PREFIX   - default debot/bt-archive
#   HOST_TAG    - default auto-detected from /etc/aws-region (eu-central-1
#                 -> frankfurt, ap-northeast-1 -> tokyo)
#   SERVICE     - default debot-pair-btceth
set -euo pipefail

DATE="${1:-$(date -u -d 'yesterday' +%Y-%m-%d)}"

if ! [[ "$DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    echo "ERROR: DATE must be YYYY-MM-DD, got: $DATE" >&2
    exit 64
fi

S3_BUCKET="${S3_BUCKET:-debot-dashboard}"
S3_PREFIX="${S3_PREFIX:-debot/bt-archive}"
SERVICE="${SERVICE:-debot-pair-btceth}"

if [ -z "${HOST_TAG:-}" ]; then
    REGION=$(curl -fs --max-time 2 \
        http://169.254.169.254/latest/dynamic/instance-identity/document 2>/dev/null \
        | python3 -c 'import json,sys; print(json.load(sys.stdin)["region"])' \
        2>/dev/null || true)
    case "$REGION" in
        eu-central-1)   HOST_TAG=frankfurt ;;
        ap-northeast-1) HOST_TAG=tokyo ;;
        *) echo "ERROR: cannot derive HOST_TAG from region '$REGION'; set HOST_TAG explicitly" >&2; exit 1 ;;
    esac
fi

# journalctl --since/--until are inclusive, so use the next day at 00:00
# as the upper bound to capture every event in DATE.
SINCE_UTC="${DATE} 00:00:00 UTC"
UNTIL_UTC="$(date -u -d "${DATE} +1 day" +%Y-%m-%d) 00:00:00 UTC"

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

echo "[archive_bt_replay] host=$HOST_TAG service=$SERVICE date=$DATE"
echo "[archive_bt_replay]   window: $SINCE_UTC → $UNTIL_UTC"

sudo journalctl -u "$SERVICE" \
    --since "$SINCE_UTC" --until "$UNTIL_UTC" \
    --utc --no-pager \
  | grep -E '\[EVAL\] BTC/ETH' > "$WORK/eval_raw.txt" || true

sudo journalctl -u "$SERVICE" \
    --since "$SINCE_UTC" --until "$UNTIL_UTC" \
    --utc --no-pager \
  | grep -E "systemd\\[1\\]: Started ${SERVICE//./\\.}" > "$WORK/restart_raw.txt" || true

# Eval timestamps: parse the inline `2026-04-15T07:02:05+0100` stamp the
# bot itself emits. Same logic as extract_bt_replay_events.sh.
python3 - "$WORK/eval_raw.txt" "$WORK/eval_ts.txt" <<'PYEOF'
import re, sys
from datetime import datetime, timezone, timedelta
raw, out = sys.argv[1], sys.argv[2]
cet = timezone(timedelta(hours=1))
seen = set()
with open(raw) as f:
    for line in f:
        m = re.search(r'(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})\+0100', line)
        if not m:
            continue
        dt_cet = datetime(*(int(g) for g in m.groups()), tzinfo=cet)
        seen.add(int(dt_cet.timestamp()))
with open(out, 'w') as f:
    for t in sorted(seen):
        f.write(f"{t}\n")
print(f"[archive_bt_replay]   eval lines: {len(seen)}")
PYEOF

# Restart timestamps: parse the journalctl outer stamp. systemd 252's
# `--utc` only shifts timezone, not format — the default is still
# `Jun 06 21:11:58 ip-...` (yearless `%b %d %H:%M:%S`). We inject the
# year from DATE since the cron always archives a single calendar day.
python3 - "$WORK/restart_raw.txt" "$WORK/restart_ts.txt" "$DATE" <<'PYEOF'
import sys
from datetime import datetime, timezone
raw, out, date_str = sys.argv[1], sys.argv[2], sys.argv[3]
year = int(date_str.split('-')[0])
ts = []
with open(raw) as f:
    for line in f:
        parts = line.strip().split()
        if len(parts) < 3:
            continue
        try:
            dt = datetime.strptime(
                f"{year} {parts[0]} {parts[1]} {parts[2]}",
                "%Y %b %d %H:%M:%S",
            ).replace(tzinfo=timezone.utc)
            ts.append(int(dt.timestamp()))
        except Exception:
            pass
ts = sorted(set(ts))
with open(out, 'w') as f:
    for t in ts:
        f.write(f"{t}\n")
print(f"[archive_bt_replay]   restart lines: {len(ts)}")
PYEOF

EVAL_KEY="s3://${S3_BUCKET}/${S3_PREFIX}/${HOST_TAG}/${SERVICE}/eval_ts/${DATE}.txt"
RESTART_KEY="s3://${S3_BUCKET}/${S3_PREFIX}/${HOST_TAG}/${SERVICE}/restart_ts/${DATE}.txt"

aws s3 cp --no-progress "$WORK/eval_ts.txt"    "$EVAL_KEY"
aws s3 cp --no-progress "$WORK/restart_ts.txt" "$RESTART_KEY"

echo "[archive_bt_replay] uploaded:"
echo "[archive_bt_replay]   $EVAL_KEY"
echo "[archive_bt_replay]   $RESTART_KEY"
