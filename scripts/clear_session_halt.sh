#!/bin/bash
# Clear a sticky session-DD halt on a pairtrade bot without using the
# host-wide /opt/debot/RISK_ACK file. Stops the target service, edits
# risk_state.json offline, restarts, and waits for the bot to come back
# up. bot-strategy#489 (follow-up to the 2026-06-07 canary restart
# documented in bot-strategy#485).
#
# Usage:
#   clear_session_halt.sh <bot-name> [instance] [flags]
#
# Examples:
#   clear_session_halt.sh debot-pair-canary
#   clear_session_halt.sh debot-pair-btceth c --allow-main-restart
#   clear_session_halt.sh debot-pair-btceth --all-variants --allow-main-restart
#   clear_session_halt.sh debot-pair-btceth-extended --dry-run
#
# Known bots:
#   debot-pair-btceth           — Frankfurt main (A/B/C; needs instance OR --all-variants)
#   debot-pair-canary           — Frankfurt canary (single instance 'a')
#   debot-pair-btceth-extended  — Tokyo Extended (single instance 'a')
#
# Flags:
#   --dry-run                    preview only, no changes
#   --yes / -y                   skip the interactive "proceed?" prompt
#   --all-variants               required for main bot when no instance arg supplied
#                                (acknowledges every halted variant is in scope)
#   --allow-main-restart         required for debot-pair-btceth (Frankfurt main).
#                                Restarting main triggers [Startup] force-close on any
#                                open exchange position at ~50 bps slippage — the flag
#                                forces the operator to confirm they accept that risk
#   --force-with-open-position   required to proceed when status.json reports a position
#                                on ANY variant the service owns (not just the cleared
#                                ones — restart force-closes service-wide) OR when status
#                                is UNKNOWN (file missing / unreadable / unparseable).
#                                UNKNOWN is treated as equivalent to OPEN because it's
#                                the worst case for this restart path.
#   --reanchor-peak              also collapse the rolling-peak window (equity_samples)
#                                to a single sample at the current equity, so DD resets
#                                to 0 and the cleared halt does not re-breach at the
#                                boundary. Use after a capital top-up, or whenever a
#                                sticky 30-day peak is pinning the variant. bot-strategy#575.
#   --collateral=<USD>           override the equity value used by --reanchor-peak with the
#                                real post-deposit collateral (the deposit-sync case). When
#                                omitted, the latest equity_samples value is reused.
#
# After a successful restart the script tails journalctl for ~60 s and
# emits ✓/✗ marks for the three expected boot-time log lines:
#   [RISK_STATE] restored
#   [SESSION_DD] equity initialized
#   [METRICS]

set -eu

usage() {
    sed -n '2,48p' "$0"
    exit 1
}

POSITIONAL=()
DRY_RUN=0
YES=0
ALL_VARIANTS=0
ALLOW_MAIN_RESTART=0
FORCE_WITH_OPEN_POSITION=0
REANCHOR_PEAK=0
COLLATERAL=""
for arg in "$@"; do
    case "$arg" in
        --dry-run) DRY_RUN=1 ;;
        --yes|-y) YES=1 ;;
        --all-variants) ALL_VARIANTS=1 ;;
        --allow-main-restart) ALLOW_MAIN_RESTART=1 ;;
        --force-with-open-position) FORCE_WITH_OPEN_POSITION=1 ;;
        --reanchor-peak) REANCHOR_PEAK=1 ;;
        --collateral=*) COLLATERAL="${arg#*=}" ;;
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

# --collateral only makes sense with --reanchor-peak (it overrides the equity
# value the rebaseline uses). Validate it is a positive number.
if [ -n "$COLLATERAL" ]; then
    if [ "$REANCHOR_PEAK" != 1 ]; then
        echo "ERROR: --collateral=<USD> requires --reanchor-peak" >&2
        exit 1
    fi
    if ! echo "$COLLATERAL" | grep -qE '^[0-9]+(\.[0-9]+)?$'; then
        echo "ERROR: --collateral must be a positive number (got '$COLLATERAL')" >&2
        exit 1
    fi
fi

# Bot lookup: ssh_host / state_dir / service / is_main / status_dir_pattern
# status_dir_pattern uses {variant} placeholder which is interpolated below.
case "$BOT" in
    debot-pair-btceth)
        SSH_HOST=debot
        STATE_DIR=/opt/debot
        SERVICE=debot-pair-btceth.service
        IS_MAIN=1
        STATUS_DIR_BASE=/home/ec2-user/debot_status/debot-pair-btceth-{variant}
        SERVICE_VARIANTS=(a b c)
        ;;
    debot-pair-canary)
        SSH_HOST=debot
        STATE_DIR=/opt/debot-canary
        SERVICE=debot-pair-canary.service
        IS_MAIN=0
        STATUS_DIR_BASE=/home/ec2-user/debot_status/debot-pair-canary
        SERVICE_VARIANTS=(a)
        ;;
    debot-pair-btceth-extended)
        SSH_HOST=debot-tokyo
        STATE_DIR=/opt/debot-extended
        SERVICE=debot-pair-btceth-extended.service
        IS_MAIN=0
        STATUS_DIR_BASE=/home/ec2-user/debot_status/debot-pair-btceth-ext
        SERVICE_VARIANTS=(a)
        ;;
    *)
        echo "ERROR: unknown bot '$BOT'" >&2
        echo "Supported: debot-pair-btceth | debot-pair-canary | debot-pair-btceth-extended" >&2
        exit 1
        ;;
esac

# Safety gate 1: main bot without --all-variants must specify instance.
if [ "$IS_MAIN" = 1 ] && [ -z "$INSTANCE_FILTER" ] && [ "$ALL_VARIANTS" != 1 ]; then
    echo "ERROR: $BOT runs multiple variants. Either pass an explicit instance" >&2
    echo "       (e.g. \`$0 debot-pair-btceth c --allow-main-restart\`) or pass" >&2
    echo "       --all-variants to acknowledge you intend to clear every halted variant." >&2
    exit 1
fi

# Safety gate 2: main bot restart needs the dedicated flag.
if [ "$IS_MAIN" = 1 ] && [ "$ALLOW_MAIN_RESTART" != 1 ]; then
    echo "ERROR: $BOT is the Frankfurt main bot. Restarting it triggers [Startup]" >&2
    echo "       force-close on any open exchange position at ~50 bps slippage" >&2
    echo "       (see feedback_pairtrade_restart_force_closes). Re-run with" >&2
    echo "       --allow-main-restart to confirm you accept that risk." >&2
    exit 1
fi

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
elif [ "$ALL_VARIANTS" = 1 ]; then
    echo "  instance : <all halted variants> (--all-variants)"
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

# Position pre-check — restart triggers [Startup] force-close on EVERY
# variant on the service, not just the ones we're clearing. So we read
# status.json for every variant the service owns (a/b/c for main, just
# 'a' for canary / Extended). status.json layout: per-variant dir for
# main (debot-pair-btceth-{a,b,c}); single dir for canary / Extended.
#
# UNKNOWN (file missing / unreadable / unparseable) is the worst-case
# state for this restart path — we have no idea whether a position is
# open or not, but restart will still force-close whatever is there. So
# UNKNOWN is treated as equivalent to OPEN for the gating decision and
# requires --force-with-open-position to override. PR #74 review item 5.
echo "==> Position pre-check (service-wide — restart force-closes every open position)"
BLOCKED_POSITIONS=""
for inst in "${SERVICE_VARIANTS[@]}"; do
    if [ "$IS_MAIN" = 1 ]; then
        STATUS_DIR="${STATUS_DIR_BASE/\{variant\}/$inst}"
    else
        STATUS_DIR="$STATUS_DIR_BASE"
    fi
    STATUS_JSON="$STATUS_DIR/status.json"
    POS=$(ssh "$SSH_HOST" "sudo cat '$STATUS_JSON' 2>/dev/null" \
        | python3 -c '
import json, sys
raw = sys.stdin.read()
if not raw.strip():
    print("UNKNOWN empty-or-missing")
    sys.exit(0)
try:
    d = json.loads(raw)
except Exception as exc:
    print("UNKNOWN parse-error " + type(exc).__name__)
    sys.exit(0)
has = d.get("has_position")
cnt = d.get("position_count", 0)
positions = d.get("positions", [])
if has is None:
    print("UNKNOWN no-has_position-field")
elif has:
    print("OPEN n=" + str(cnt) + " " + str(positions)[:200])
else:
    print("CLEAN")
' || echo "UNKNOWN ssh-or-pipe-error")
    echo "  $inst: $POS"
    if [[ "$POS" == OPEN* ]] || [[ "$POS" == UNKNOWN* ]]; then
        BLOCKED_POSITIONS="$BLOCKED_POSITIONS $inst($POS)"
    fi
done

if [ -n "$BLOCKED_POSITIONS" ] && [ "$FORCE_WITH_OPEN_POSITION" != 1 ]; then
    echo "ERROR: cannot confirm clean position state on:$BLOCKED_POSITIONS" >&2
    echo "       Restart will [Startup] force-close every open position on the" >&2
    echo "       service at ~50 bps slippage. UNKNOWN status means the bot may" >&2
    echo "       still hold a position we can't see, which is the most dangerous" >&2
    echo "       state for this restart path. Re-run with --force-with-open-position" >&2
    echo "       to accept the cost, or close positions / fix status.json first." >&2
    exit 1
fi

if [ "$DRY_RUN" = 1 ]; then
    echo "==> --dry-run, no changes made"
    exit 0
fi

if [ "$YES" != 1 ]; then
    read -r -p "Proceed (stop + backup + clear + start)? [y/N] " ans
    if [ "$ans" != "y" ] && [ "$ans" != "Y" ]; then
        echo "Aborted"
        exit 1
    fi
fi

# Stop → edit → start eliminates the race where the running bot's own
# persist_risk_state() tick could overwrite the cleared fields before /
# after the restart command. PR #74 review item 2.
echo "==> Stopping $SERVICE"
ssh "$SSH_HOST" "sudo systemctl stop '$SERVICE'"

echo "==> Backing up to $BACKUP"
ssh "$SSH_HOST" "sudo cp '$RISK_STATE' '$BACKUP'"

echo "==> Clearing session_halted for: $WOULD_CLEAR (reanchor_peak=$REANCHOR_PEAK collateral=${COLLATERAL:-<auto>})"
NOW_TS=$(date -u +%s)
ssh "$SSH_HOST" "sudo python3 - '$INSTANCE_FILTER' '$REANCHOR_PEAK' '$COLLATERAL' '$NOW_TS' <<'PY'
import json, sys
filt = sys.argv[1]
reanchor = sys.argv[2] == '1'
collateral = float(sys.argv[3]) if sys.argv[3] else None
now_ts = int(sys.argv[4])
with open('$RISK_STATE') as f:
    d = json.load(f)
for name, inst in d.get('instances', {}).items():
    if filt and name != filt:
        continue
    if inst.get('session_halted'):
        inst['session_halted'] = False
        inst['session_halt_reason'] = None
        inst['session_halt_ts'] = None
        msg = f'  cleared {name}'
        if reanchor:
            # bot-strategy#575: collapse the rolling-peak window to a single
            # sample at the (possibly topped-up) collateral so DD resets to 0
            # and the cleared halt does not re-breach at the boundary.
            samples = inst.get('equity_samples') or []
            base = collateral
            if base is None:
                base = samples[-1]['equity'] if samples else None
            if base is not None:
                inst['equity_samples'] = [{'ts': now_ts, 'equity': base}]
                inst['capital_baseline_equity'] = base
                inst['capital_baseline_accounted_pnl'] = (
                    float(inst.get('total_pnl', 0.0))
                    + float(inst.get('total_funding_carry', 0.0))
                )
                inst['capital_position_seen_since_baseline'] = False
                msg += f' (peak reanchored to {base:.2f}, DD->0)'
            else:
                msg += ' (reanchor skipped: no equity reference available)'
        print(msg, file=sys.stderr)
with open('$RISK_STATE', 'w') as f:
    json.dump(d, f, indent=2)
PY"

echo "==> Starting $SERVICE"
ssh "$SSH_HOST" "sudo systemctl start '$SERVICE'"

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
