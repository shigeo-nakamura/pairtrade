#!/bin/bash
# Fetch archived [EVAL] + restart timestamps from S3 for a BT replay window.
#
# This is the cron-archive complement to extract_bt_replay_events.sh:
# instead of pulling from the host's journalctl (which only retains ~7
# days), it pulls the daily files written by archive-bt-replay.timer
# (bot-strategy#255) and concatenates them, filtered to the requested
# window. For any window inside the cron-archived range it produces
# byte-identical output to a same-day call of extract_bt_replay_events.sh.
#
# Usage:
#   scripts/fetch_bt_replay_events.sh HOST_TAG SERVICE SINCE UNTIL OUT_DIR
#
# Example:
#   scripts/fetch_bt_replay_events.sh frankfurt debot-pair-btceth \
#     '2026-03-15 00:00:00' '2026-03-20 00:00:00' /tmp/bt_events/
#
# HOST_TAG: frankfurt | tokyo
# SINCE/UNTIL: UTC, in YYYY-MM-DD or 'YYYY-MM-DD HH:MM:SS' form
# Writes:
#   OUT_DIR/eval_ts.txt
#   OUT_DIR/restart_ts.txt
#
# Pass to the BT binary via:
#   BT_EVAL_TIMESTAMPS_FILE=OUT_DIR/eval_ts.txt \
#   BT_RESTART_TIMESTAMPS_FILE=OUT_DIR/restart_ts.txt \
#   scripts/bt_live_data.sh ...
set -euo pipefail

HOST_TAG="${1:?usage: $0 HOST_TAG SERVICE SINCE UNTIL OUT_DIR}"
SERVICE="${2:?usage: $0 HOST_TAG SERVICE SINCE UNTIL OUT_DIR}"
SINCE="${3:?usage: $0 HOST_TAG SERVICE SINCE UNTIL OUT_DIR}"
UNTIL="${4:?usage: $0 HOST_TAG SERVICE SINCE UNTIL OUT_DIR}"
OUT_DIR="${5:?usage: $0 HOST_TAG SERVICE SINCE UNTIL OUT_DIR}"

case "$HOST_TAG" in
  frankfurt|tokyo) ;;
  *) echo "ERROR: HOST_TAG must be 'frankfurt' or 'tokyo', got: $HOST_TAG" >&2; exit 64 ;;
esac

S3_BUCKET="${S3_BUCKET:-debot-dashboard}"
S3_PREFIX="${S3_PREFIX:-debot/bt-archive}"

mkdir -p "$OUT_DIR"

# Convert SINCE/UNTIL to unix seconds (UTC) for client-side filtering, and
# also enumerate the calendar dates whose files we need to download.
read -r SINCE_TS UNTIL_TS START_DATE END_DATE <<EOF
$(python3 - "$SINCE" "$UNTIL" <<'PYEOF'
import sys
from datetime import datetime, timezone, timedelta

def parse(s):
    s = s.strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    raise SystemExit(f"ERROR: cannot parse '{s}' as UTC date or datetime")

since = parse(sys.argv[1])
until = parse(sys.argv[2])
if until <= since:
    raise SystemExit("ERROR: UNTIL must be after SINCE")
print(int(since.timestamp()), int(until.timestamp()),
      since.strftime("%Y-%m-%d"),
      # End date is inclusive of any day touched by the window. UNTIL at
      # exactly 00:00:00 of day D means we don't actually need day D's
      # file (the window ends at the start of D), so step back one second.
      (until - timedelta(seconds=1)).strftime("%Y-%m-%d"))
PYEOF
)
EOF

echo "[fetch_bt_replay] host=$HOST_TAG service=$SERVICE"
echo "[fetch_bt_replay]   window: $SINCE → $UNTIL"
echo "[fetch_bt_replay]   dates:  $START_DATE..$END_DATE"

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

# Enumerate every calendar date between START_DATE and END_DATE inclusive.
DATES=$(python3 - "$START_DATE" "$END_DATE" <<'PYEOF'
import sys
from datetime import datetime, timedelta
start = datetime.strptime(sys.argv[1], "%Y-%m-%d")
end   = datetime.strptime(sys.argv[2], "%Y-%m-%d")
d = start
while d <= end:
    print(d.strftime("%Y-%m-%d"))
    d += timedelta(days=1)
PYEOF
)

EVAL_PREFIX="s3://${S3_BUCKET}/${S3_PREFIX}/${HOST_TAG}/${SERVICE}/eval_ts"
RESTART_PREFIX="s3://${S3_BUCKET}/${S3_PREFIX}/${HOST_TAG}/${SERVICE}/restart_ts"

mkdir -p "$WORK/eval" "$WORK/restart"
MISSING_DAYS=0
for d in $DATES; do
    if aws s3 cp --no-progress "$EVAL_PREFIX/$d.txt" "$WORK/eval/$d.txt" 2>/dev/null; then
        :
    else
        echo "[fetch_bt_replay]   WARN: missing $EVAL_PREFIX/$d.txt"
        MISSING_DAYS=$((MISSING_DAYS + 1))
    fi
    if aws s3 cp --no-progress "$RESTART_PREFIX/$d.txt" "$WORK/restart/$d.txt" 2>/dev/null; then
        :
    else
        # restart files are often empty / absent on calm days — only log
        # if eval is also missing, to avoid spurious noise.
        :
    fi
done

# Concatenate, filter to [SINCE_TS, UNTIL_TS), uniq + sort.
python3 - "$WORK/eval" "$OUT_DIR/eval_ts.txt" "$SINCE_TS" "$UNTIL_TS" <<'PYEOF'
import os, sys
src_dir, out, lo, hi = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
seen = set()
if os.path.isdir(src_dir):
    for fn in sorted(os.listdir(src_dir)):
        with open(os.path.join(src_dir, fn)) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                t = int(line)
                if lo <= t < hi:
                    seen.add(t)
with open(out, 'w') as f:
    for t in sorted(seen):
        f.write(f"{t}\n")
print(f"[fetch_bt_replay]   eval timestamps: {len(seen)} -> {out}")
PYEOF

python3 - "$WORK/restart" "$OUT_DIR/restart_ts.txt" "$SINCE_TS" "$UNTIL_TS" <<'PYEOF'
import os, sys
src_dir, out, lo, hi = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
seen = set()
if os.path.isdir(src_dir):
    for fn in sorted(os.listdir(src_dir)):
        with open(os.path.join(src_dir, fn)) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                t = int(line)
                if lo <= t < hi:
                    seen.add(t)
with open(out, 'w') as f:
    for t in sorted(seen):
        f.write(f"{t}\n")
print(f"[fetch_bt_replay]   restart timestamps: {len(seen)} -> {out}")
PYEOF

if [ "$MISSING_DAYS" -gt 0 ]; then
    echo "[fetch_bt_replay]   WARN: $MISSING_DAYS day(s) missing eval_ts file in S3"
    echo "[fetch_bt_replay]   (cron started 2026-04-30; pre-cron days will be absent)"
fi

echo
echo "[fetch_bt_replay] Done. For a BT run:"
echo "  BT_EVAL_TIMESTAMPS_FILE=$OUT_DIR/eval_ts.txt \\"
echo "  BT_RESTART_TIMESTAMPS_FILE=$OUT_DIR/restart_ts.txt \\"
echo "  BT_WARM_START_SNAPSHOT=/tmp/bt/pairtrade_history_BTC_ETH.json \\"
echo "  scripts/bt_live_data.sh /tmp/bt"
