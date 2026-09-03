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
# /opt/debot-hl). State: ARM / KILL_SWITCH / RISK_ACK sentinels, state.json,
# status.json, pnl_log.jsonl under bull_holder/.

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

exec "$STATE_DIR/bin/bull-holder"
