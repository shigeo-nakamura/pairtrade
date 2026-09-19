#!/bin/bash
# Launch wrapper for the cross-venue hedge holder (bot-strategy#1046):
# Lighter on Robinhood Chain BTC long + Lighter Core BTC short, equal size,
# held for the weekly points drop.
#
# Runs on the Tokyo Robinhood host (debot-robinhood-lighter). Binary + state
# live under /opt/debot-hedge-holder/ (same isolation pattern as
# debot-bull-holder -> /opt/debot-bull-holder). State: ARM / DISARM /
# KILL_SWITCH / RISK_ACK sentinels, state.json, status.json, events.jsonl
# under hedge/.
#
# Operator quick reference (all under $STATE_DIR/hedge/):
#   touch ARM             build both legs at HEDGE_TARGET_NOTIONAL_USD/leg
#   echo 25000 > ARM      ... at $25,000/leg instead (<= HEDGE_MAX_NOTIONAL_USD)
#   touch DISARM          close both legs now
#   touch KILL_SWITCH     no growth on either leg (reductions still run)
#   touch RISK_ACK        clear a halt (net-exposure / leverage / liq_guard)

set -eu

ENV_DIR="${DEBOT_ENV_DIR:-/opt/debot/scripts}"
STATE_DIR="${HEDGE_BASE_DIR:-/opt/debot-hedge-holder}"

# Shared KMS data key + RUST_LOG defaults (same files as the Robinhood
# pairtrade units on this host). Both legs' API keys must be encrypted
# under THIS host's ENCRYPTED_DATA_KEY.
source "$ENV_DIR/debot_secrets_common.env"
source "$ENV_DIR/debot.env"
# Hedge-specific credentials (suffixed _RH / _CORE) + parameters.
source "$ENV_DIR/debot-xvenue-hedge-holder.env"

mkdir -p "$STATE_DIR/hedge"
export HEDGE_BASE_DIR="$STATE_DIR"
export HEDGE_DRY_RUN="${HEDGE_DRY_RUN:-true}"

# libsigner.so for the Lighter Go bindings, from this bot's own lib/ dir.
if [ -f "$STATE_DIR/lib/libsigner.so" ]; then
    export LIGHTER_GO_PATH="$STATE_DIR/lib"
    export LD_LIBRARY_PATH="${LIGHTER_GO_PATH}:${LD_LIBRARY_PATH:-}"
fi

exec "$STATE_DIR/bin/xvenue_hedge_holder"
