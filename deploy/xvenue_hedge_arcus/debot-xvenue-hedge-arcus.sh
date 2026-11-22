#!/bin/bash
# Launch wrapper for the Arcus-long / Lighter-Core-short hedge-holder instances
# (bot-strategy#1123 → #1228 → #1293). One file, one instance per argument:
#
#   argument  unit                           market  Arcus wallet (sub 0)  Lighter short leg
#   (none)    debot-xvenue-hedge-arcus       AAPL    0x812B…DF05           Robinhood Lighter 3209 (instance rh; #1293, moved off Core 574527 2026-11-22)
#   nvda      debot-xvenue-hedge-arcus-nvda  NVDA    0xA2C7…66d6           Core 281474976624889   (instance core; #1293 second hold, retired 11-21)
#
# Each instance has its own env file (/opt/debot/scripts/<unit>.env), /opt state
# dir and S3 status object. The table pins which wallet, Lighter venue + account and
# market every instance may touch, so a mis-edited env can never point one at
# another bot's account or put the same market on both wallets (#1093 rule).
# The symbol and size still come from the env (HEDGE_SYMBOLS / HEDGE_TARGET_NOTIONAL_USD).
#
# Copy of scripts/debot-xvenue-hedge-holder.sh (pairtrade 091fbea) with its own
# env files and state dirs. The first instance (debot-xvenue-hedge-holder,
# /opt/debot-hedge-holder) is never touched by this file.
#
# Operator quick reference (under <state dir>/hedge/):
#   echo 'AAPL 40000' > ARM  build both legs at $40,000/leg (AAPL instance)
#   echo 'NVDA 20000' > ARM  build both legs at $20,000/leg (nvda instance)
#   touch DISARM             close both legs now (short leg first)
#   touch KILL_SWITCH        no growth on either leg (reductions still run)
#   touch RISK_ACK           clear a halt

set -eu

INSTANCE="${1:-}"
fail() { echo "[debot-xvenue-hedge-arcus${INSTANCE:+-$INSTANCE}] refusing to start: $*" >&2; exit 64; }
case "$INSTANCE" in
    '')
        UNIT=debot-xvenue-hedge-arcus
        SYMBOL=AAPL
        WALLET=0x812b6a6da8e0df1fbca7939ae32089cf85c5df05
        SHORT_INSTANCE=rh
        SHORT_ACCOUNT=3209
        SHORT_HOST=api.rh.lighter.xyz
        ;;
    nvda)
        UNIT=debot-xvenue-hedge-arcus-nvda
        SYMBOL=NVDA
        WALLET=0xa2c78e14dfd5586444ce4fe28fc4e36308a066d6
        SHORT_INSTANCE=core
        SHORT_ACCOUNT=281474976624889
        SHORT_HOST=mainnet.zklighter.elliot.ai
        ;;
    *) fail "unknown instance '$INSTANCE' (expected no argument or 'nvda')";;
esac

ENV_DIR="${DEBOT_ENV_DIR:-/opt/debot/scripts}"
# Fixed on purpose: an inherited HEDGE_BASE_DIR must never point an instance at
# another one's state. DEBOT_HEDGE_ARCUS_ROOT is a test prefix (empty on hosts).
STATE_DIR="${DEBOT_HEDGE_ARCUS_ROOT:-}/opt/${UNIT/debot-xvenue-/debot-}"
STATUS_URI="s3://debot-dashboard/debot/status/robinhood-lighter/${UNIT#debot-}.json"

source "$ENV_DIR/debot_secrets_common.env"
source "$ENV_DIR/debot.env"
# Venue fallbacks: config.rs reads NAME_<INSTANCE> and falls back to NAME.
# Drop every unsuffixed venue variable so a blank suffixed value can never
# resolve to another bot's account.
unset LIGHTER_ACCOUNT_INDEX LIGHTER_API_KEY_INDEX LIGHTER_PUBLIC_API_KEY \
      LIGHTER_PRIVATE_API_KEY LIGHTER_PLAIN_PUBLIC_API_KEY LIGHTER_PLAIN_PRIVATE_API_KEY \
      LIGHTER_EVM_WALLET_PRIVATE_KEY LIGHTER_WALLET_ADDRESS REST_ENDPOINT WEB_SOCKET_ENDPOINT \
      ARCUS_ADDRESS ARCUS_ACCOUNT_INDEX ARCUS_API_KEY ARCUS_API_PRIVATE_KEY \
      ARCUS_PLAIN_API_PRIVATE_KEY ARCUS_REST_ENDPOINT ARCUS_WEBSOCKET_ENDPOINT \
      HEDGE_BASE_DIR HEDGE_POINTS_HISTORY_PATH
source "$ENV_DIR/$UNIT.env"

# Belt and braces against a mis-edited env (the install scripts check these too).
# The status object is derived from the unit name, so it can be neither the
# first holder's nor the other instance's.
[ "${HEDGE_STATUS_S3_URI:-}" = "$STATUS_URI" ] || fail "HEDGE_STATUS_S3_URI must be $STATUS_URI"
[ "${HEDGE_LONG_VENUE:-}" = arcus ] || fail "HEDGE_LONG_VENUE must be arcus"
[ "${HEDGE_LONG_INSTANCE:-}" = arcus ] || fail "HEDGE_LONG_INSTANCE must be arcus"
[ "${HEDGE_SHORT_VENUE:-}" = lighter ] || fail "HEDGE_SHORT_VENUE must be lighter"
[ "${HEDGE_SHORT_INSTANCE:-}" = "$SHORT_INSTANCE" ] || fail "HEDGE_SHORT_INSTANCE must be $SHORT_INSTANCE"
[ "${ARCUS_ACCOUNT_INDEX_ARCUS:-}" = 0 ] || fail "ARCUS_ACCOUNT_INDEX_ARCUS must be 0 (the dedicated wallet's subaccount 0)"
[ "$(printf '%s' "${ARCUS_ADDRESS_ARCUS:-}" | tr 'A-F' 'a-f')" = "$WALLET" ] \
    || fail "ARCUS_ADDRESS_ARCUS must be this instance's wallet $WALLET"
# The short leg's account and venue host come from the table, read through the instance suffix.
SFX=$(printf '%s' "$SHORT_INSTANCE" | tr 'a-z' 'A-Z')
ACCT_VAR="LIGHTER_ACCOUNT_INDEX_$SFX"; REST_VAR="REST_ENDPOINT_$SFX"; WS_VAR="WEB_SOCKET_ENDPOINT_$SFX"
[ "${!ACCT_VAR:-}" = "$SHORT_ACCOUNT" ] \
    || fail "$ACCT_VAR must be $SHORT_ACCOUNT (this instance's Lighter account)"
[ "${!REST_VAR:-}" = "https://$SHORT_HOST" ] || fail "$REST_VAR must be https://$SHORT_HOST"
[ "${!WS_VAR:-}" = "wss://$SHORT_HOST/stream" ] || fail "$WS_VAR must be wss://$SHORT_HOST/stream"
# Exactly one book, and only this instance's market: `SYMBOL:LONG_MMR/SHORT_MMR`.
case "${HEDGE_SYMBOLS:-}" in
    "$SYMBOL:"*) case "$HEDGE_SYMBOLS" in *,*) fail "HEDGE_SYMBOLS must list only $SYMBOL";; esac;;
    *) fail "HEDGE_SYMBOLS must be '$SYMBOL:<mmr>' (this instance holds $SYMBOL only)";;
esac
[ "${HEDGE_ROLL_ENABLED:-false}" = false ] || fail "the roll stays off (hold, not volume)"
[ -z "${HEDGE_BASE_DIR:-}" ] || fail "HEDGE_BASE_DIR must not be set in the env"
[ -z "${HEDGE_POINTS_HISTORY_PATH:-}" ] || fail "HEDGE_POINTS_HISTORY_PATH must not be set in the env"

mkdir -p "$STATE_DIR/hedge"
export HEDGE_BASE_DIR="$STATE_DIR"
export HEDGE_DRY_RUN="${HEDGE_DRY_RUN:-true}"

# libsigner.so for the Lighter Go bindings, from this instance's own lib/ dir.
if [ -f "$STATE_DIR/lib/libsigner.so" ]; then
    export LIGHTER_GO_PATH="$STATE_DIR/lib"
    export LD_LIBRARY_PATH="${LIGHTER_GO_PATH}:${LD_LIBRARY_PATH:-}"
fi

exec "$STATE_DIR/bin/xvenue_hedge_holder"
