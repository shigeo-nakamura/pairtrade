#!/bin/bash
# Launch wrapper for the SECOND cross-venue hedge holder instance
# (bot-strategy#1123, reused by #1228): Arcus Perps long (dedicated wallet 0x812B, subaccount 0)
# + Lighter Core short (dedicated Core sub-account 281474976574527), operator-armed.
# The symbol and size come from the env (HEDGE_SYMBOLS / HEDGE_TARGET_NOTIONAL_USD):
# #1123 ran QQQ $7.5k (10-04..10-07); #1228 runs AAPL $20k (10-28..11-04).
#
# Copy of scripts/debot-xvenue-hedge-holder.sh (pairtrade 091fbea) with its own
# env file and state dir. The first instance (debot-xvenue-hedge-holder,
# /opt/debot-hedge-holder) is never touched by this file.
#
# Operator quick reference (all under /opt/debot-hedge-arcus/hedge/):
#   echo 'AAPL 20000' > ARM  build both legs at $20,000/leg (#1228)
#   touch DISARM             close both legs now (short leg first)
#   touch KILL_SWITCH        no growth on either leg (reductions still run)
#   touch RISK_ACK           clear a halt

set -eu

ENV_DIR="${DEBOT_ENV_DIR:-/opt/debot/scripts}"
# Fixed on purpose: an inherited HEDGE_BASE_DIR must never point this
# instance at the first one's state.
STATE_DIR=/opt/debot-hedge-arcus
FIRST_STATUS_URI="s3://debot-dashboard/debot/status/robinhood-lighter/xvenue-hedge-holder.json"

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
source "$ENV_DIR/debot-xvenue-hedge-arcus.env"

# Belt and braces against a mis-edited env (the install script checks these too).
fail() { echo "[debot-xvenue-hedge-arcus] refusing to start: $*" >&2; exit 64; }
[ "${HEDGE_STATUS_S3_URI:-}" != "$FIRST_STATUS_URI" ] || fail "HEDGE_STATUS_S3_URI is the first instance's object"
[ "${HEDGE_LONG_VENUE:-}" = arcus ] || fail "HEDGE_LONG_VENUE must be arcus"
[ "${ARCUS_ACCOUNT_INDEX_ARCUS:-}" = 0 ] || fail "ARCUS_ACCOUNT_INDEX_ARCUS must be 0 (the dedicated wallet's subaccount 0)"
case "$(printf '%s' "${ARCUS_ADDRESS_ARCUS:-}" | tr 'A-F' 'a-f')" in
    ''|0xa2c78e14dfd5586444ce4fe28fc4e36308a066d6) fail "ARCUS_ADDRESS_ARCUS is empty or the #1093 wallet (debot-arcus-vol)";;
esac
[ "${LIGHTER_ACCOUNT_INDEX_CORE:-}" = 281474976574527 ] || fail "LIGHTER_ACCOUNT_INDEX_CORE must be 281474976574527 (the #1123 Core sub-account)"
[ "${HEDGE_ROLL_ENABLED:-false}" = false ] || fail "the roll stays off for #1123"
[ -z "${HEDGE_BASE_DIR:-}" ] || fail "HEDGE_BASE_DIR must not be set in the env"

mkdir -p "$STATE_DIR/hedge"
export HEDGE_BASE_DIR="$STATE_DIR"
export HEDGE_DRY_RUN="${HEDGE_DRY_RUN:-true}"

# libsigner.so for the Lighter Go bindings, from this instance's own lib/ dir.
if [ -f "$STATE_DIR/lib/libsigner.so" ]; then
    export LIGHTER_GO_PATH="$STATE_DIR/lib"
    export LD_LIBRARY_PATH="${LIGHTER_GO_PATH}:${LD_LIBRARY_PATH:-}"
fi

exec "$STATE_DIR/bin/xvenue_hedge_holder"
