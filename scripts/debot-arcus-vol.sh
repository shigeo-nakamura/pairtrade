#!/bin/bash
# Launch wrapper for the Arcus Perps presence / volume runtime
# (arcus_vol_runtime, bot-strategy#1093), run by debot-arcus-vol.service on
# the Tokyo debot-main host. Operator-started: CI deploys the binary and this
# file but never starts the unit.
#
# What it does before exec'ing the runtime:
#   1. reads /etc/debot-arcus-vol/live.env   (Arcus API credentials, mode 600)
#      and   /etc/debot-arcus-vol/config.env (market / sizing / stops; optional,
#      every value has a default below) as plain KEY=VALUE lines -- not sourced.
#      The signing key is ARCUS_API_PRIVATE_KEY, KMS-encrypted under the host's
#      ENCRYPTED_DATA_KEY (live.env or /opt/debot/scripts/debot_secrets_common.env),
#      produced by /opt/debot/scripts/arcus_vol_encrypt_key.sh. A plain key
#      (ARCUS_PLAIN_API_PRIVATE_KEY) starts only with ALLOW_PLAIN_KEY=1 in config.env;
#   2. refuses to start while a KILL_SWITCH or sticky HALT sentinel sits in the
#      state dir (exit 0, so Restart=on-failure does not loop -- the operator
#      removes the file after reading the log);
#   3. checks on the venue (public REST, IPv4) that the market is flat and has
#      no open orders on this subaccount;
#   4. sizes the quotes from the account's FREE collateral at start:
#         clip = min(CLIP_MAX_USD, free * leverage / 2 * 0.9, rounded down to $100)
#         inventory cap = 2 clips, margin = cap / leverage
#      (free collateral, not equity: other positions on this cross account use
#      margin that is not ours);
#   5. execs the binary so systemd supervises the runtime directly: SIGTERM ->
#      the runtime's own cancel-all + fill harvest + dead-man's-switch disarm
#      (TimeoutStopSec in the unit leaves room for that).
#   Optional quote gate (bot-strategy#1120): GATE_MODE=off|shadow|enforce (default
#   off), GATE_MODEL=<model json>, GATE_FALLBACK, GATE_LOG_FEATURES,
#   GATE_ENFORCE_CONFIRM in config.env pass through as ARCUS_VOL_GATE_*; shadow
#   computes and logs only, enforce needs the runtime's confirm token.
#
# Operator quick reference (STATE_DIR, default /var/lib/debot-arcus-vol/state):
#   touch KILL_SWITCH   runtime pulls quotes, flattens, halts; remove before
#                       the next start
#   HALT                written by the runtime on a sticky halt (cumulative
#                       stop etc.); read the journal, then remove it by hand
#   status.json         live status (plan, quotes, inventory, pnl)
#   journalctl -u debot-arcus-vol -f | grep -E '\[ARCUS_VOL\]|WARN|ERROR'

set -euo pipefail

ETC_DIR="${DEBOT_ARCUS_VOL_ETC_DIR:-/etc/debot-arcus-vol}"
SECRETS_COMMON="${DEBOT_ARCUS_VOL_SECRETS_COMMON:-/opt/debot/scripts/debot_secrets_common.env}"
BIN="${DEBOT_ARCUS_VOL_BIN:-/opt/debot-arcus-vol/bin/arcus_vol_runtime}"
API_BASE="${DEBOT_ARCUS_VOL_API_BASE:-https://api.arcus.xyz}"
LIVE_ENV="$ETC_DIR/live.env"
CONFIG_ENV="$ETC_DIR/config.env"

log() { echo "[debot-arcus-vol] $*"; }
die() { echo "[debot-arcus-vol] REFUSED: $*" >&2; exit 1; }
# A deliberate stop (sentinel present) is not a failure: exit 0 so systemd's
# Restart=on-failure leaves the unit stopped instead of retrying every minute.
hold() { echo "[debot-arcus-vol] NOT STARTING: $*" >&2; exit 0; }
# Both env files are read as plain KEY=VALUE lines and exported -- never
# sourced, so a stray shell metacharacter in a value is a refusal, not a
# command. Blank lines and `#` comments are allowed.
# load_kv FILE MODE KEYS...: MODE `allow` = every key must be one of KEYS (an
# unknown key refuses the start: config.env must not be able to redirect the
# venue or move the account the preflight checked); MODE `only` = keys outside
# KEYS are ignored (the shared debot_secrets_common.env contributes nothing but
# the data key).
load_kv() {
    local file="$1" mode="$2" line key value n=0
    shift 2
    local allowed=" $* "
    while IFS= read -r line || [ -n "$line" ]; do
        n=$((n + 1))
        line="${line%$'\r'}"
        [[ "$line" =~ ^[[:space:]]*(#|$) ]] && continue
        [[ "$line" =~ ^(export[[:space:]]+)?([A-Z][A-Z0-9_]*)=(.*)$ ]] || die "$file line $n: expected KEY=VALUE"
        key="${BASH_REMATCH[2]}"; value="${BASH_REMATCH[3]}"
        if [[ "$allowed" != *" $key "* ]]; then
            [ "$mode" = only ] && continue
            die "$file line $n: key $key is not accepted here (allowed: ${allowed% })"
        fi
        # debot_secrets_common.env writes `export KEY="value"`: strip one pair of quotes.
        if [[ "$value" =~ ^\"(.*)\"$ ]]; then value="${BASH_REMATCH[1]}"; fi
        [[ "$value" =~ ^[A-Za-z0-9_./:+=,-]*$ ]] || die "$file line $n: unexpected characters in the value of $key"
        export "$key=$value"
    done < "$file"
}

# ---- credentials -----------------------------------------------------------
[ -f "$LIVE_ENV" ] || die "missing $LIVE_ENV (create it from /opt/debot/scripts/debot-arcus-vol.env.example, mode 600)"
[ "$(stat -c %a "$LIVE_ENV")" = "600" ] || die "$LIVE_ENV must be mode 600 (is $(stat -c %a "$LIVE_ENV"))"
load_kv "$LIVE_ENV" allow ARCUS_ADDRESS ARCUS_API_KEY ARCUS_ACCOUNT_INDEX ARCUS_PLAIN_API_PRIVATE_KEY ARCUS_API_PRIVATE_KEY ENCRYPTED_DATA_KEY
[ -n "${ARCUS_ADDRESS:-}" ] && [ -n "${ARCUS_API_KEY:-}" ] || die "credentials incomplete in $LIVE_ENV (ARCUS_ADDRESS / ARCUS_API_KEY)"

# ---- configuration (every value has a default) ----------------------------
[ -f "$CONFIG_ENV" ] && load_kv "$CONFIG_ENV" allow MARKET CLIP_MAX_USD QUOTE_OFFSET_BPS REPEG_BAND_BPS SESSION_OFFSET_BPS SESSION_BAND_BPS POSITION_STOP_BPS DAILY_STOP_USD CUM_STOP_USD LEVERAGE STATE_DIR LOCK_DIR ALLOW_PLAIN_KEY RUST_LOG GATE_MODE GATE_MODEL GATE_FALLBACK GATE_LOG_FEATURES GATE_ENFORCE_CONFIRM
MARKET="${MARKET:-SPY-USD}"
CLIP_MAX_USD="${CLIP_MAX_USD:-2500}"
QUOTE_OFFSET_BPS="${QUOTE_OFFSET_BPS:-5}"
REPEG_BAND_BPS="${REPEG_BAND_BPS:-2}"
DAILY_STOP_USD="${DAILY_STOP_USD:-5}"
CUM_STOP_USD="${CUM_STOP_USD:-21}"
LEVERAGE="${LEVERAGE:-5}"
STATE_DIR="${STATE_DIR:-/var/lib/debot-arcus-vol/state}"
LOCK_DIR="${LOCK_DIR:-/var/lib/debot-arcus-vol/locks}"
ALLOW_PLAIN_KEY="${ALLOW_PLAIN_KEY:-0}"

# ---- signing key: encrypted (production) or plain (testing, opt-in) --------
# pairtrade's Arcus config loader prefers a PLAIN key whenever one is set, so
# the launcher never exports both: the encrypted path drops the plain variable.
if [ -n "${ARCUS_API_PRIVATE_KEY:-}" ]; then
    if [ -z "${ENCRYPTED_DATA_KEY:-}" ]; then
        [ -f "$SECRETS_COMMON" ] || die "ARCUS_API_PRIVATE_KEY is set but ENCRYPTED_DATA_KEY is neither in $LIVE_ENV nor available from $SECRETS_COMMON"
        load_kv "$SECRETS_COMMON" only ENCRYPTED_DATA_KEY
        [ -n "${ENCRYPTED_DATA_KEY:-}" ] || die "ENCRYPTED_DATA_KEY not found in $SECRETS_COMMON"
    fi
    [[ "$ENCRYPTED_DATA_KEY" =~ ^[A-Za-z0-9+/=]+$ ]] || die "ENCRYPTED_DATA_KEY does not look like base64"
    unset ARCUS_PLAIN_API_PRIVATE_KEY
    KEY_MODE="encrypted (ARCUS_API_PRIVATE_KEY under the host data key)"
elif [ -n "${ARCUS_PLAIN_API_PRIVATE_KEY:-}" ]; then
    [ "$ALLOW_PLAIN_KEY" = "1" ] || die "live.env carries a PLAIN signing key; encrypt it with: sudo /opt/debot/scripts/arcus_vol_encrypt_key.sh (or set ALLOW_PLAIN_KEY=1 in $CONFIG_ENV for testing only)"
    unset ENCRYPTED_DATA_KEY ARCUS_API_PRIVATE_KEY
    KEY_MODE="PLAIN (ALLOW_PLAIN_KEY=1, testing only)"
else
    die "no signing key in $LIVE_ENV (ARCUS_API_PRIVATE_KEY, or ARCUS_PLAIN_API_PRIVATE_KEY with ALLOW_PLAIN_KEY=1)"
fi

case "$MARKET" in *-USD) ;; *) die "MARKET must look like SPY-USD (got '$MARKET')" ;; esac
for v in CLIP_MAX_USD QUOTE_OFFSET_BPS REPEG_BAND_BPS DAILY_STOP_USD CUM_STOP_USD LEVERAGE; do
    [[ "${!v}" =~ ^[0-9]+(\.[0-9]+)?$ ]] || die "$v must be a number (got '${!v}')"
done
# Session offset (optional, both or neither): the distance/band while the
# market's venue session is open (US equity/ETF perps: 04:00-20:00 New York).
SESSION_OFFSET_BPS="${SESSION_OFFSET_BPS:-}"
SESSION_BAND_BPS="${SESSION_BAND_BPS:-}"
if [ -n "$SESSION_OFFSET_BPS$SESSION_BAND_BPS" ]; then
    [ -n "$SESSION_OFFSET_BPS" ] && [ -n "$SESSION_BAND_BPS" ] || die "set both SESSION_OFFSET_BPS and SESSION_BAND_BPS, or neither"
    for v in SESSION_OFFSET_BPS SESSION_BAND_BPS; do
        [[ "${!v}" =~ ^[0-9]+(\.[0-9]+)?$ ]] || die "$v must be a number (got '${!v}')"
    done
fi
# Per-position stop (optional): flatten once the open position is this many bp
# against its average entry; the runtime validates 1..=500.
POSITION_STOP_BPS="${POSITION_STOP_BPS:-}"
if [ -n "$POSITION_STOP_BPS" ]; then
    [[ "$POSITION_STOP_BPS" =~ ^[0-9]+(\.[0-9]+)?$ ]] || die "POSITION_STOP_BPS must be a number (got '$POSITION_STOP_BPS')"
fi
[[ "$LEVERAGE" =~ ^[0-9]+$ ]] && [ "$LEVERAGE" -ge 1 ] || die "LEVERAGE must be a whole number >= 1"
# Quote gate (bot-strategy#1120). Format only: the runtime validates the model
# file (venue / market / feature names / sha) and the enforce token itself.
GATE_MODE="${GATE_MODE:-off}"
GATE_MODEL="${GATE_MODEL:-}"
GATE_FALLBACK="${GATE_FALLBACK:-}"
GATE_LOG_FEATURES="${GATE_LOG_FEATURES:-}"
GATE_ENFORCE_CONFIRM="${GATE_ENFORCE_CONFIRM:-}"
case "$GATE_MODE" in off|shadow|enforce) ;; *) die "GATE_MODE must be off|shadow|enforce (got '$GATE_MODE')" ;; esac
if [ -n "$GATE_MODEL" ]; then
    [ -r "$GATE_MODEL" ] || die "GATE_MODEL $GATE_MODEL is not readable"
fi
if [ -n "$GATE_FALLBACK" ]; then
    [[ "$GATE_FALLBACK" =~ ^(baseline|pull|baseline_then_pull:[0-9]+)$ ]] || die "GATE_FALLBACK must be baseline|pull|baseline_then_pull:<secs> (got '$GATE_FALLBACK')"
fi
if [ -n "$GATE_LOG_FEATURES" ]; then
    case "$GATE_LOG_FEATURES" in all|none) ;; *) die "GATE_LOG_FEATURES must be all|none (got '$GATE_LOG_FEATURES')" ;; esac
fi
if [ "$GATE_MODE" = enforce ]; then
    [ -n "$GATE_ENFORCE_CONFIRM" ] && [ -n "$GATE_MODEL" ] || die "GATE_MODE=enforce needs GATE_ENFORCE_CONFIRM and GATE_MODEL (shadow readout first, bot-strategy#1120)"
fi

[ -x "$BIN" ] || die "binary missing or not executable: $BIN"
if ldd "$BIN" 2>/dev/null | grep -q 'not found'; then die "unresolved shared libraries: $(ldd "$BIN" | grep 'not found' | tr '\n' ' ')"; fi

mkdir -p "$STATE_DIR" "$LOCK_DIR"
[ -e "$STATE_DIR/KILL_SWITCH" ] && hold "KILL_SWITCH present in $STATE_DIR; remove it to start"
[ -e "$STATE_DIR/HALT" ] && hold "sticky HALT present in $STATE_DIR; read the journal, then remove it by hand"

# ---- venue: flat (or dust) and no open orders in this market ---------------
# Checked after EVERY file is loaded: the runtime starts on the account the
# preflight looked at, and that account is subaccount 0 (hard requirement).
# A position the venue itself would not accept as an order (below its
# minOrderSize or minOrderNotional at the mark) is DUST: nothing can flatten
# it, the runtime adopts and carries it (bot-strategy#1093), so it must not
# keep the service from starting. Anything above the venue minimum refuses.
ACCOUNT_INDEX="${ARCUS_ACCOUNT_INDEX:-}"
[ "$ACCOUNT_INDEX" = "0" ] || die "this launcher is for subaccount 0 (ARCUS_ACCOUNT_INDEX='$ACCOUNT_INDEX')"
addr="$(echo "$ARCUS_ADDRESS" | tr 'A-Z' 'a-z')"
fetch() { curl -4 -sf --max-time 20 "$API_BASE/v1/$1?address=$addr&accountIndex=$ACCOUNT_INDEX${2:+&$2}"; }
pos="$(fetch positions "market=$MARKET")" || die "position read failed ($API_BASE)"
ord="$(fetch openOrders "market=$MARKET")" || die "open-orders read failed ($API_BASE)"
# /v1/markets is only needed to judge a residual position: a flat account
# must start even while that endpoint is down (Codex round 2 on pairtrade#382).
has_pos="$(python3 - "$pos" "$MARKET" <<'PY'
import json, sys
p = json.loads(sys.argv[1]).get("positions") or {}
rows = p.values() if isinstance(p, dict) else p
print(1 if any(v.get("marketDisplayName") == sys.argv[2] and float(v.get("size") or 0) != 0 for v in rows) else 0)
PY
)" || die "position payload unreadable"
mkts="{}"
if [ "$has_pos" = "1" ]; then
    mkts="$(curl -4 -sf --max-time 20 "$API_BASE/v1/markets")" || die "markets read failed ($API_BASE); cannot judge a residual position, not starting blind"
fi
python3 - "$pos" "$ord" "$MARKET" "$mkts" <<'PY' || exit 1
import json, sys
p = json.loads(sys.argv[1]).get("positions") or {}
o = json.loads(sys.argv[2]).get("orders") or []
mk = sys.argv[3]
rows = p.values() if isinstance(p, dict) else p
open_pos = [v for v in rows if v.get("marketDisplayName") == mk and float(v.get("size") or 0) != 0]
if open_pos:
    ms = json.loads(sys.argv[4])
    ms = ms.get("markets") if isinstance(ms, dict) else ms
    info = next((m for m in ms or [] if m.get("marketDisplayName") == mk), None)
    if info is None:
        sys.exit("[debot-arcus-vol] REFUSED: %s position open and %s not found in /v1/markets; cannot judge it" % (mk, mk))
    mark = float(info.get("markPrice") or info.get("oraclePrice") or 0)
    min_size = float(info.get("minOrderSize") or 0)
    min_notional = float(info.get("minOrderNotional") or 0)
    if mark <= 0 or (min_size <= 0 and min_notional <= 0):
        sys.exit("[debot-arcus-vol] REFUSED: %s position open and /v1/markets gives no mark/minimums to judge it" % mk)
    size = sum(abs(float(v.get("size") or 0)) for v in open_pos)
    notional = size * mark
    if size < min_size or notional < min_notional:
        print("[debot-arcus-vol] venue check: %s dust position %s (~$%.2f) carried, below venue minimum (size %s / notional %s)"
              % (mk, open_pos[0].get("size"), notional, min_size, min_notional))
    else:
        sys.exit("[debot-arcus-vol] REFUSED: %s position open on this subaccount (%s, ~$%.2f, above the venue minimum): %s"
                 % (mk, open_pos[0].get("size"), notional, open_pos))
open_ord = [x for x in o if x.get("marketDisplayName") in (None, mk)]
if open_ord:
    sys.exit("[debot-arcus-vol] REFUSED: %d open %s order(s) on this subaccount; cancel them first" % (len(open_ord), mk))
if not open_pos:
    print("[debot-arcus-vol] venue check: %s flat, no open %s orders" % (mk, mk))
PY

# ---- sizing from free collateral ------------------------------------------
acct="$(fetch account)" || die "account read failed ($API_BASE)"
read -r MARGIN CLIP CAP FREE <<< "$(python3 - "$acct" "$CLIP_MAX_USD" "$LEVERAGE" <<'PY'
import json, sys
d = json.loads(sys.argv[1])
clip_max = int(float(sys.argv[2]))
lev = int(sys.argv[3])
free = min(float(d["equity"]), float(d["freeCollateral"]))
by_margin = int(free * lev / 2 * 0.9 / 100) * 100   # per side, rounded down to $100
clip = min(clip_max, by_margin)
cap = 2 * clip
margin = cap // lev
print(margin, clip, cap, int(free))
PY
)"
[ "${CLIP:-0}" -ge 500 ] || die "free collateral too small for presence quoting (free \$$FREE, clip \$$CLIP)"

log "signing key: $KEY_MODE"
log "sizing: market $MARKET, clip \$$CLIP per side (max \$$CLIP_MAX_USD), inventory cap \$$CAP, margin \$$MARGIN at ${LEVERAGE}x (free collateral \$$FREE); presence offset ${QUOTE_OFFSET_BPS} bp +/- ${REPEG_BAND_BPS} bp${SESSION_OFFSET_BPS:+ (in session ${SESSION_OFFSET_BPS} bp +/- ${SESSION_BAND_BPS} bp)}; daily stop \$$DAILY_STOP_USD, cumulative stop \$$CUM_STOP_USD; state $STATE_DIR"
log "quote gate: $GATE_MODE${GATE_MODEL:+ (model $GATE_MODEL)}${GATE_FALLBACK:+, fallback $GATE_FALLBACK}"

# Everything from here on is the runtime's own process (systemd sees its pid).
export ARCUS_VOL_DRY_RUN=false
export ARCUS_VOL_LIVE_CONFIRM=1093-G2
export ARCUS_VOL_ALLOW_ACCOUNT0=I-understand-shared-account
export ARCUS_VOL_STATE_DIR="$STATE_DIR"
export ARCUS_VOL_LOCK_DIR="$LOCK_DIR"
export ARCUS_VOL_MARKET="$MARKET"
export ARCUS_VOL_CLIP_USD="$CLIP"
export ARCUS_VOL_SKEW_USD="$CLIP"
export ARCUS_VOL_HARD_CAP_USD="$CAP"
export ARCUS_VOL_MARGIN_USD="$MARGIN"
export ARCUS_VOL_LEVERAGE="$LEVERAGE"
export ARCUS_VOL_QUOTE_OFFSET_BPS="$QUOTE_OFFSET_BPS"
export ARCUS_VOL_REPEG_BAND_BPS="$REPEG_BAND_BPS"
if [ -n "$SESSION_OFFSET_BPS" ]; then
    export ARCUS_VOL_SESSION_OFFSET_BPS="$SESSION_OFFSET_BPS"
    export ARCUS_VOL_SESSION_BAND_BPS="$SESSION_BAND_BPS"
fi
if [ -n "$POSITION_STOP_BPS" ]; then
    export ARCUS_VOL_POSITION_STOP_BPS="$POSITION_STOP_BPS"
fi
export ARCUS_VOL_GATE_MODE="$GATE_MODE"
[ -n "$GATE_MODEL" ] && export ARCUS_VOL_GATE_MODEL="$GATE_MODEL"
[ -n "$GATE_FALLBACK" ] && export ARCUS_VOL_GATE_FALLBACK="$GATE_FALLBACK"
[ -n "$GATE_LOG_FEATURES" ] && export ARCUS_VOL_GATE_LOG_FEATURES="$GATE_LOG_FEATURES"
[ -n "$GATE_ENFORCE_CONFIRM" ] && export ARCUS_VOL_GATE_ENFORCE_CONFIRM="$GATE_ENFORCE_CONFIRM"
export ARCUS_VOL_DAILY_STOP_USD="$DAILY_STOP_USD"
export ARCUS_VOL_CUM_STOP_USD="$CUM_STOP_USD"
export ARCUS_WS_PRIVATE=1
export RUST_LOG="${RUST_LOG:-info}"
exec "$BIN"
