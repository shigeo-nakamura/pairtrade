#!/bin/bash
# Clear a sticky session-DD halt on a pairtrade bot without using the
# host-wide /opt/debot/RISK_ACK file. Edits the target bot's
# risk_state.json directly, restarts the service, and waits for the
# bot to come back up. bot-strategy#489 (follow-up to the 2026-06-07
# canary restart documented in bot-strategy#485).
#
# Usage:
#   clear_session_halt.sh <bot-name> [instance] [--dry-run] [--yes]
#
# Examples:
#   clear_session_halt.sh debot-pair-canary
#   clear_session_halt.sh debot-pair-btceth c
#   clear_session_halt.sh debot-pair-btceth-extended --dry-run
#
# Known bots:
#   debot-pair-btceth           — Frankfurt main (A/B/C); needs instance arg
#   debot-pair-canary           — Frankfurt canary (single instance 'a')
#   debot-pair-btceth-extended  — Tokyo Extended (single instance 'a')
#
# When run without --yes the script prints what it would change and
# prompts for confirmation. --dry-run skips the prompt and does nothing.
#
# After a successful restart the script tails journalctl for ~60 s and
# emits ✓/✗ marks for the three expected boot-time log lines:
#   [RISK_STATE] restored
#   [SESSION_DD] equity initialized
#   [METRICS]

set -eu

usage() {
    sed -n '2,30p' "$0"
    exit 1
}

POSITIONAL=()
DRY_RUN=0
YES=0
for arg in "$@"; do
    case "$arg" in
        --dry-run) DRY_RUN=1 ;;
        --yes|-y) YES=1 ;;
        --help|-h) usage ;;
        --*)
            echo "ERROR: unknown flag '$arg'" >&2
            exit 1
            ;;
        *) POSITIONAL+=("$arg") ;;
    esac
done

if [ "${#POSITIONAL[@]}" -lt 1 ] || [ "${#POSITIONAL[@]}" -gt 2 ]; then
    usage
fi

BOT="${POSITIONAL[0]}"
INSTANCE_FILTER="${POSITIONAL[1]:-}"

case "$BOT" in
    debot-pair-btceth)
        SSH_HOST=debot
        STATE_DIR=/opt/debot
        SERVICE=debot-pair-btceth.service
        ;;
    debot-pair-canary)
        SSH_HOST=debot
        STATE_DIR=/opt/debot-canary
        SERVICE=debot-pair-canary.service
        ;;
    debot-pair-btceth-extended)
        SSH_HOST=debot-tokyo
        STATE_DIR=/opt/debot-extended
        SERVICE=debot-pair-btceth-extended.service
        ;;
    *)
        echo "ERROR: unknown bot '$BOT'" >&2
        echo "Supported: debot-pair-btceth | debot-pair-canary | debot-pair-btceth-extended" >&2
        exit 1
        ;;
esac

RISK_STATE="$STATE_DIR/risk_state.json"
TS=$(date -u +%Y%m%d_%H%M%S)
BACKUP="$RISK_STATE.bak.$TS"

echo "==> Target"
echo "  bot      : $BOT"
echo "  ssh host : $SSH_HOST"
echo "  state    : $RISK_STATE"
echo "  service  : $SERVICE"
if [ -n "$INSTANCE_FILTER" ]; then
    echo "  instance : $INSTANCE_FILTER (only this one will be cleared)"
else
    echo "  instance : <all halted instances>"
fi

echo "==> Pre-check (current halt state)"
JSON=$(ssh "$SSH_HOST" "sudo cat '$RISK_STATE'")

PY_PRECHECK=$(cat <<'PY'
import json, sys
filt = sys.argv[1] if len(sys.argv) > 1 else ""
d = json.load(sys.stdin)
out = []
for name, inst in d.get("instances", {}).items():
    if filt and name != filt:
        continue
    halted = inst.get("session_halted")
    reason = inst.get("session_halt_reason")
    ts = inst.get("session_halt_ts")
    out.append((name, halted, reason, ts))
if not out:
    print("NO_MATCH filter=" + repr(filt))
    sys.exit(0)
for name, halted, reason, ts in out:
    print("  " + name + ": halted=" + str(halted) + " reason=" + str(reason) + " halt_ts=" + str(ts))
halted_names = [n for n, h, _, _ in out if h]
if halted_names:
    val = ",".join(halted_names)
else:
    val = "NONE"
print("WOULD_CLEAR=" + val)
PY
)
PRECHECK=$(echo "$JSON" | python3 -c "$PY_PRECHECK" "$INSTANCE_FILTER")
echo "$PRECHECK"

WOULD_CLEAR=$(echo "$PRECHECK" | awk -F= '/^WOULD_CLEAR=/ {print $2}')
if [ "$WOULD_CLEAR" = "NONE" ] || [ -z "$WOULD_CLEAR" ]; then
    echo "==> Nothing to do (no halted instances match)"
    exit 0
fi

if [ "$DRY_RUN" = 1 ]; then
    echo "==> --dry-run, no changes made"
    exit 0
fi

if [ "$YES" != 1 ]; then
    read -r -p "Proceed (backup + clear + restart)? [y/N] " ans
    if [ "$ans" != "y" ] && [ "$ans" != "Y" ]; then
        echo "Aborted"
        exit 1
    fi
fi

echo "==> Backing up to $BACKUP"
ssh "$SSH_HOST" "sudo cp '$RISK_STATE' '$BACKUP'"

echo "==> Clearing session_halted for: $WOULD_CLEAR"
ssh "$SSH_HOST" "sudo python3 - '$INSTANCE_FILTER' <<'PY'
import json, sys
filt = sys.argv[1]
with open('$RISK_STATE') as f:
    d = json.load(f)
for name, inst in d.get('instances', {}).items():
    if filt and name != filt:
        continue
    if inst.get('session_halted'):
        inst['session_halted'] = False
        inst['session_halt_reason'] = None
        inst['session_halt_ts'] = None
        print(f'  cleared {name}', file=sys.stderr)
with open('$RISK_STATE', 'w') as f:
    json.dump(d, f, indent=2)
PY"

echo "==> Restarting $SERVICE"
ssh "$SSH_HOST" "sudo systemctl restart '$SERVICE'"

echo "==> Waiting 60s for boot…"
sleep 60

echo "==> Post-check (journalctl markers since restart)"
LOG_SINCE=$(date -u -d '90 seconds ago' '+%Y-%m-%d %H:%M:%S')
LOG=$(ssh "$SSH_HOST" "sudo journalctl -u '$SERVICE' --since '$LOG_SINCE UTC' --no-pager")

check() {
    local label="$1" pat="$2"
    if echo "$LOG" | grep -qE "$pat"; then
        echo "  ✓ $label"
    else
        echo "  ✗ $label   (pattern: $pat)"
    fi
}
check "[RISK_STATE] restored"          '\[RISK_STATE\] [a-z]+ restored'
check "[SESSION_DD] equity initialized" '\[SESSION_DD\] [a-z]+ equity initialized'
check "[METRICS] first tick"            '\[METRICS\]'

# Surface any ERROR lines as a final sanity check.
ERRORS=$(echo "$LOG" | grep -E '\[ERROR\]' || true)
if [ -n "$ERRORS" ]; then
    echo "==> WARNING: ERROR lines observed during boot:"
    echo "$ERRORS" | head -5
fi

echo "==> Done. Backup at $BACKUP on $SSH_HOST"
