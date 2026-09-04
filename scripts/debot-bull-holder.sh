#!/bin/bash
# Launch wrapper for the bull-mode holder (bot-strategy#893/#894/#895).
#
# Two-venue, two-leg holder: Hyperliquid spot 1x + Lighter perp 0.5x per
# symbol (BTC/ETH), operator-armed, single 30% daily-close exit. DRY_RUN is
# forced on by the binary until the #895 rollout gates lift it.
#
# Binary + state live entirely under /opt/debot-bull-holder/ — separate from
# the pairtrade A/B/C tree at /opt/debot/ (same isolation pattern as
# debot-pair-canary -> /opt/debot-canary and the Hyperliquid observer ->
# /opt/debot-hl). State: ARM / DISARM / KILL_SWITCH / RISK_ACK sentinels,
# state.json, status.json, pnl_log.jsonl under bull_holder/.
#
# Operator quick reference (all under $STATE_DIR/bull_holder/):
#   touch ARM          open the book (one-shot, full size)
#   touch DISARM       close every leg now (manual exit; the only other exit
#                      is the automatic 30% daily-close rule)
#   touch KILL_SWITCH  block new ARMs / stop re-placement (exits still run)
#   touch RISK_ACK     clear a reconcile/data halt

set -eu

ENV_DIR="${DEBOT_ENV_DIR:-/opt/debot/scripts}"
STATE_DIR="${BULL_HOLDER_BASE_DIR:-/opt/debot-bull-holder}"

# Shared KMS data key + RUST_LOG defaults (same files as the pairtrade units).
source "$ENV_DIR/debot_secrets_common.env"
source "$ENV_DIR/debot.env"
# Holder-specific credentials + parameters (see debot-bull-holder.env.example).
source "$ENV_DIR/debot-bull-holder.env"

mkdir -p "$STATE_DIR/bull_holder"
export BULL_HOLDER_BASE_DIR="$STATE_DIR"
export BULL_HOLDER_DRY_RUN="${BULL_HOLDER_DRY_RUN:-true}"

# libsigner.so for Lighter Go bindings (same convention as
# debot-pair-canary.sh / debot-pair-btceth.sh, but from this bot's own
# isolated lib/ dir rather than the shared /opt/debot/lib/).
if [ -f "$STATE_DIR/lib/libsigner.so" ]; then
    export LIGHTER_GO_PATH="$STATE_DIR/lib"
    export LD_LIBRARY_PATH="${LIGHTER_GO_PATH}:${LD_LIBRARY_PATH:-}"
fi

exec "$STATE_DIR/bin/bull-holder"
