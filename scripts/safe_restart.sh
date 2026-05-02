#!/usr/bin/env bash
# safe_restart.sh — flat-first restart helper for debot trading services
# (bot-strategy#269 Phase 4).
#
# The bot's startup-time `[Startup] Force closing` path adverse-prices any
# position carried over a restart. Manually running `sudo systemctl restart`
# while a position is open is the most common preventable loss source on
# this fleet (4/29 10:36 / 4/30 09:58 documented in #269). This helper
# checks the bot's recent journal for "open position" markers and refuses
# to restart unless the strategy is currently flat.
#
# Usage:
#   scripts/safe_restart.sh SERVICE [SSH_HOST] [--restart|--force]
#
# Default action: print state and exit (dry-run / safety-first). Re-run
# with --restart once you've read the state and want to proceed. Use
# --force to bypass the state check entirely; this exists for emergencies
# (e.g., bot stuck mid-error). The whole point of this helper is to keep
# you from `--force`ing routinely.
#
# Examples:
#   # Tokyo Extended — just check
#   scripts/safe_restart.sh debot-pair-btceth-extended debot-tokyo
#   # Frankfurt Lighter — restart after confirming flat
#   scripts/safe_restart.sh debot-pair-btceth debot --restart
#   # Tokyo Arb — emergency restart, accept any kill loss
#   scripts/safe_restart.sh debot-xvenue-arb debot-tokyo-arb --force
#
# SSH_HOST defaults to `debot` (Frankfurt) — match the inventory in
# bot/CLAUDE.md (debot / debot-tokyo / debot-tokyo-arb).
set -euo pipefail

ACTION="check"
SERVICE=""
SSH_HOST=""
for arg in "$@"; do
  case "$arg" in
    --restart) ACTION="restart" ;;
    --force)   ACTION="force"   ;;
    --help|-h) sed -n '2,30p' "$0"; exit 0 ;;
    -*)        echo "ERROR: unknown flag '$arg'" >&2; exit 64 ;;
    *)
      if   [ -z "$SERVICE"  ]; then SERVICE="$arg"
      elif [ -z "$SSH_HOST" ]; then SSH_HOST="$arg"
      else echo "ERROR: unexpected positional arg '$arg'" >&2; exit 64
      fi ;;
  esac
done
[ -n "$SERVICE" ] || {
  echo "usage: $0 SERVICE [SSH_HOST] [--restart|--force]" >&2
  exit 64
}
SSH_HOST="${SSH_HOST:-debot}"

echo "[safe_restart] service=$SERVICE  ssh=$SSH_HOST  action=$ACTION"

if [ "$ACTION" = "force" ]; then
  echo "[safe_restart] --force: skipping state check, restarting now."
  ssh "$SSH_HOST" "sudo systemctl restart $SERVICE"
  sleep 3
  ssh "$SSH_HOST" "sudo systemctl is-active $SERVICE"
  exit 0
fi

JOURNAL=$(ssh "$SSH_HOST" "sudo journalctl -u $SERVICE --since '5 minutes ago' --no-pager 2>&1" || true)
if [ -z "$JOURNAL" ]; then
  echo "[safe_restart] WARN: empty journal in last 5 min — service may be offline or just booted."
  echo "[safe_restart]       Check 'sudo systemctl status $SERVICE' on $SSH_HOST manually."
  exit 2
fi

case "$SERVICE" in
  *xvenue-arb*)
    # xvenue-arb logs [STATUS] every minute with enter_l, enter_s, exit
    # counters. Position is flat iff total opens == exit.
    LATEST_STATUS=$(echo "$JOURNAL" | grep -E '\[STATUS\] ticks=' | tail -1)
    if [ -z "$LATEST_STATUS" ]; then
      echo "[safe_restart] WARN: no [STATUS] tick in last 5 min — xvenue-arb may be offline."
      exit 2
    fi
    ENTER_L=$(echo "$LATEST_STATUS" | grep -oE 'enter_l=[0-9]+' | cut -d= -f2)
    ENTER_S=$(echo "$LATEST_STATUS" | grep -oE 'enter_s=[0-9]+' | cut -d= -f2)
    EXIT_N=$(echo  "$LATEST_STATUS" | grep -oE 'exit=[0-9]+'    | cut -d= -f2)
    : "${ENTER_L:=?}"; : "${ENTER_S:=?}"; : "${EXIT_N:=?}"
    OPEN_COUNT=$(( ENTER_L + ENTER_S - EXIT_N ))

    echo
    echo "=== xvenue-arb latest [STATUS] (1-min cadence) ==="
    echo "$LATEST_STATUS"
    echo "  enter_l=$ENTER_L  enter_s=$ENTER_S  exit=$EXIT_N  ⇒ open=$OPEN_COUNT"
    echo

    if [ "$OPEN_COUNT" -gt 0 ]; then
      echo "[safe_restart] state=OPEN — $OPEN_COUNT entry/entries not yet exited."
      echo "[safe_restart] DO NOT restart now. Wait for exit or use --force."
      exit 1
    fi
    echo "[safe_restart] state=FLAT — entries == exits."
    ;;

  *)
    # pairtrade family: `[POSITION] open positions detected (...) blocking
    # new entries` is logged every eval tick while open, so its absence in
    # the last 5 min is the flat signal. `[ZCHECK]` (~every 30s) is the
    # liveness signal — `[METRICS]` is too sparse (5-min cadence).
    LOG=$(echo "$JOURNAL" \
      | grep -E '\[(POSITION|ZCHECK|METRICS|ENTRY|CLOSE|FORCE_CLOSE|Startup)\]' \
      || true)

    if [ -z "$LOG" ]; then
      echo "[safe_restart] WARN: no eval-tick events in last 5 min — service may be offline or just booted."
      exit 2
    fi

    OPEN_HITS=$(echo "$LOG" | grep -cE '\[POSITION\] open positions detected' || true)
    ZCHECK_HITS=$(echo "$LOG" | grep -cE '\[ZCHECK\]' || true)

    echo
    echo "=== Last 5 min summary ==="
    echo "  [POSITION] open positions detected lines: $OPEN_HITS"
    echo "  [ZCHECK] eval-tick lines (~30s cadence):  $ZCHECK_HITS"
    echo
    echo "=== Tail of recent trade events (last 8 lines) ==="
    echo "$LOG" | tail -8
    echo

    if [ "$OPEN_HITS" -gt 0 ]; then
      echo "[safe_restart] state=OPEN — recent ticks show an active position."
      echo "[safe_restart] DO NOT restart now. The [Startup] force-close path will adverse-price it."
      echo "[safe_restart] Wait for the strategy to close (next MR signal or force_close_secs timeout)"
      echo "[safe_restart] and re-run this script. If you absolutely must restart now, use --force."
      exit 1
    fi

    # Healthy live cadence is ~30s/tick, so 5 min should have ≥5 ZCHECK lines.
    # Demand at least 3 to tolerate a temporary WS hiccup; lower than that
    # means the eval loop is stalled and the "no open" signal is unreliable.
    if [ "$ZCHECK_HITS" -lt 3 ]; then
      echo "[safe_restart] WARN: only $ZCHECK_HITS [ZCHECK] tick(s) in 5min — eval loop may be stalled."
      echo "[safe_restart]       Inspect logs before relying on the 'no open positions' signal."
      exit 2
    fi

    echo "[safe_restart] state=FLAT — no open-position markers in the last 5 min."
    ;;
esac

if [ "$ACTION" = "restart" ]; then
  echo "[safe_restart] running: ssh $SSH_HOST sudo systemctl restart $SERVICE"
  ssh "$SSH_HOST" "sudo systemctl restart $SERVICE"
  sleep 3
  echo
  echo "=== Post-restart status ==="
  ssh "$SSH_HOST" "sudo systemctl is-active $SERVICE && sudo journalctl -u $SERVICE --since '1 minute ago' --no-pager | tail -15"
else
  echo "[safe_restart] OK to restart. Re-run with --restart to actually do it."
fi
