#!/bin/bash
# Production wrapper for the SOL/ETH observe-only data collector
# (bot-strategy#519 Phase 1).
#
# - observe_only + dry_run: never places orders, real or simulated. Its sole
#   deliverable is /opt/debot-sol/market_data_soleth_*.jsonl for the #519
#   second-pair backtest.
# - Reuses the canary's Lighter sub-account credentials
#   (debot-pair-canary.env). Safe because observe mode signs no transactions
#   (no nonce contention with the canary process); it only adds WS/REST read
#   load, which the lighter-ratelimit sidecar already arbitrates.
# - Always uses the latest /opt/debot/bin/debot deployed by CI, but is NOT in
#   the ci.yml auto-restart allowlist — restart manually after deploys if a
#   dump-format change matters (none expected).
# - State lives under /opt/debot-sol/ so pairtrade_history_SOL_ETH.json,
#   risk_state.json etc. never collide with /opt/debot/ or /opt/debot-canary/.

set -eu

ENV_DIR="${DEBOT_ENV_DIR:-/opt/debot/scripts}"
STATE_DIR="${DEBOT_SOL_STATE_DIR:-/opt/debot-sol}"

# Common KMS-encrypted shared key + RUST_LOG defaults
source "$ENV_DIR/debot_secrets_common.env"
source "$ENV_DIR/debot.env"

# Canary sub-account credentials (see header for why sharing is safe here).
source "$ENV_DIR/debot-pair-canary.env"

export DEBOT_STATUS_DIR="${DEBOT_STATUS_DIR:-/home/ec2-user/debot_status}"
export DEBOT_STATUS_ID=debot-pair-soleth-observe
export PAIRTRADE_CONFIG_PATH=/opt/debot/configs/pairtrade/debot-pair-soleth-observe.yaml

# Perps-only, same as main (bot-strategy#128)
export LIGHTER_SKIP_SPOT_MARKETS=1

# Own ack sentinel (bot-strategy#488 pattern); unused in observe mode but
# keeps the path disjoint from main/canary.
export RISK_ACK_PATH="$STATE_DIR/RISK_ACK"

# Deterministic startup spacing (bot-strategy#163): the systemd unit's
# ExecStartPre sleep provides inter-process spacing; disable in-process
# random jitter.
export LIGHTER_STARTUP_JITTER_SECS=0

# libsigner.so for Lighter Go bindings (same path as main).
if [ -f /opt/debot/lib/libsigner.so ]; then
    export LIGHTER_GO_PATH=/opt/debot/lib
    export LD_LIBRARY_PATH="${LIGHTER_GO_PATH}:${LD_LIBRARY_PATH:-}"
fi

mkdir -p "$DEBOT_STATUS_DIR" "$STATE_DIR/history_archive"

# Working dir is the observer state tree so pairtrade_history_SOL_ETH.json,
# risk_state.json, equity_history.jsonl all live next to each other.
cd "$STATE_DIR"

exec /opt/debot/bin/debot
