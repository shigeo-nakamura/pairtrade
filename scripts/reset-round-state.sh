#!/bin/bash
# Reset round-bound persistent state in /opt/debot/risk_state.json before
# starting a new evaluation round, or between rollout phases like #278's
# Step 2 small-equity verification → Step 4 full live restart.
#
# Background: bot-strategy#320 made trade_stats persist across restart, so
# `total_trades` / `total_wins` / `peak_pnl` / `max_dd` survive a `systemctl
# restart`. Several other persisted fields also need clearing at a round
# boundary; see #354 for the rationale and per-field analysis.
#
# Fields zeroed:
#   - total_trades, total_wins, total_pnl, peak_pnl, max_dd  (#320)
#   - consecutive_losses, circuit_breaker_until_ts            (risk gating)
#   - last_stop_loss_per_pair                                 (#316 cool-down anchors)
#   - equity_samples                                          (#185 Phase 3-1, leverage-scale dependent)
#   - session_halted, session_halt_reason, session_halt_ts    (#185 Phase 3-1)
#
# Fields NOT touched (correctly auto-rolling at UTC midnight, do not need
# round-boundary handling):
#   - session_start_equity, session_start_ts, realized_pnl_today
#
# The dashboard's `status.equity_history.jsonl` is also truncated, though
# this is now redundant after debot-dashboard#351 (filters by
# service_started_at). Truncating keeps the chart's y-axis baseline tight
# for the first day of the new round.
#
# Usage:
#   scripts/reset-round-state.sh                          # interactive, runs the reset
#   scripts/reset-round-state.sh --dry-run                # show before/after, no writes
#   scripts/reset-round-state.sh --no-history-truncate    # skip equity_history truncate
#   scripts/reset-round-state.sh --state /path/risk.json  # custom risk_state.json path
#   scripts/reset-round-state.sh --status-dir /path       # custom status dir
#
# Defaults assume the standard Frankfurt / Tokyo Lighter pairtrade layout:
#   --state      /opt/debot/risk_state.json
#   --status-dir /home/ec2-user/debot_status
#
# Safety:
#   - Refuses to run if `debot-pair-btceth.service` is `active` (avoid
#     racing the bot's own writer; the bot persists after every closed
#     trade, so a concurrent edit can be silently overwritten).
#   - Always writes a backup `risk_state.json.bak.<UTC-timestamp>` before
#     mutating. No-op if the input file is missing.

set -euo pipefail

STATE_PATH="/opt/debot/risk_state.json"
STATUS_DIR="/home/ec2-user/debot_status"
SERVICE="debot-pair-btceth"
DRY_RUN=0
TRUNCATE_HISTORY=1

usage() {
    grep '^# ' "$0" | sed 's/^# \?//'
    exit 64
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry-run) DRY_RUN=1; shift ;;
        --no-history-truncate) TRUNCATE_HISTORY=0; shift ;;
        --state) STATE_PATH="$2"; shift 2 ;;
        --status-dir) STATUS_DIR="$2"; shift 2 ;;
        --service) SERVICE="$2"; shift 2 ;;
        -h|--help) usage ;;
        *) echo "ERROR: unknown arg: $1" >&2; usage ;;
    esac
done

if ! command -v jq >/dev/null 2>&1; then
    echo "ERROR: jq is required but not on PATH" >&2
    exit 65
fi

if [[ ! -f "$STATE_PATH" ]]; then
    echo "ERROR: $STATE_PATH does not exist" >&2
    exit 66
fi

# Refuse if the bot is running. The bot persists after every closed trade,
# so a concurrent jq write is racy and the bot's next persist will clobber
# our reset.
if systemctl is-active --quiet "$SERVICE" 2>/dev/null; then
    echo "ERROR: $SERVICE is active; stop it before resetting (sudo systemctl stop $SERVICE)" >&2
    exit 67
fi

TS=$(date -u +%Y%m%dT%H%M%SZ)
BACKUP="${STATE_PATH}.bak.${TS}"

# The transformation. Per-instance fields only — top-level (_v, instances)
# stays as-is.
RESET='.instances |= with_entries(.value += {
    total_trades: 0,
    total_wins: 0,
    total_pnl: 0,
    peak_pnl: 0,
    max_dd: 0,
    consecutive_losses: 0,
    circuit_breaker_until_ts: null,
    last_stop_loss_per_pair: {},
    equity_samples: [],
    session_halted: false,
    session_halt_reason: null,
    session_halt_ts: null
})'

echo "[reset-round-state] state file: $STATE_PATH"
echo "[reset-round-state] dry-run:    $([[ $DRY_RUN -eq 1 ]] && echo yes || echo no)"
echo

echo "=== BEFORE (per-instance, abbreviated) ==="
jq '.instances | with_entries(.value |= {
    total_trades, total_wins, total_pnl, peak_pnl, max_dd,
    consecutive_losses, circuit_breaker_until_ts,
    last_stop_loss_per_pair: (.last_stop_loss_per_pair // {} | length),
    equity_samples_n: (.equity_samples // [] | length),
    session_halted
})' "$STATE_PATH"

if [[ $DRY_RUN -eq 1 ]]; then
    echo
    echo "=== AFTER (preview, dry-run) ==="
    jq "$RESET | .instances | with_entries(.value |= {
        total_trades, total_wins, total_pnl, peak_pnl, max_dd,
        consecutive_losses, circuit_breaker_until_ts,
        last_stop_loss_per_pair: (.last_stop_loss_per_pair // {} | length),
        equity_samples_n: (.equity_samples // [] | length),
        session_halted
    })" "$STATE_PATH"
    echo
    echo "[reset-round-state] dry-run complete; no files modified."
    exit 0
fi

# Backup first, then write atomically via tmp.
cp "$STATE_PATH" "$BACKUP"
echo "[reset-round-state] backup: $BACKUP"

TMP=$(mktemp "${STATE_PATH}.tmp.XXXXXX")
trap 'rm -f "$TMP"' EXIT
jq "$RESET" "$STATE_PATH" > "$TMP"
chmod --reference="$STATE_PATH" "$TMP"
chown --reference="$STATE_PATH" "$TMP"
mv "$TMP" "$STATE_PATH"
trap - EXIT

echo
echo "=== AFTER (per-instance, abbreviated) ==="
jq '.instances | with_entries(.value |= {
    total_trades, total_wins, total_pnl, peak_pnl, max_dd,
    consecutive_losses, circuit_breaker_until_ts,
    last_stop_loss_per_pair: (.last_stop_loss_per_pair // {} | length),
    equity_samples_n: (.equity_samples // [] | length),
    session_halted
})' "$STATE_PATH"

if [[ $TRUNCATE_HISTORY -eq 1 ]]; then
    echo
    echo "=== Truncating equity_history.jsonl per variant ==="
    if [[ -d "$STATUS_DIR" ]]; then
        for d in "$STATUS_DIR"/${SERVICE}-*; do
            [[ -d "$d" ]] || continue
            f="$d/status.equity_history.jsonl"
            if [[ -f "$f" ]]; then
                truncate -s 0 "$f"
                echo "[reset-round-state] truncated: $f"
            fi
        done
    else
        echo "[reset-round-state] WARN: $STATUS_DIR not present, skipping equity_history truncate"
    fi
fi

echo
echo "[reset-round-state] done. Restart the service when ready: sudo systemctl start $SERVICE"
