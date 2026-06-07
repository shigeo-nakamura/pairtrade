#!/bin/bash
# Production wrapper for the canary pairtrade bot (bot-strategy#376).
#
# - Single-variant clone of main A with entry_z_score_base=1.0 (vs 1.5)
# - $50 equity on Lighter sub-account 281474976624819 (shared parent wallet
#   with main A/B/C — the 4th sub-account under
#   0x812B6A6da8E0dF1fBCA7939ae32089Cf85c5DF05)
# - Always uses the *latest* /opt/debot/bin/debot deployed by CI. Restarted
#   automatically by ci.yml's deploy step (explicit allowlist exception to the
#   bot-strategy#269 manual-restart rule)
# - State directories live under /opt/debot-canary/ so the canary's
#   pairtrade_history / history_archive / risk_state do not collide with the
#   main process's files at /opt/debot/.

set -eu

ENV_DIR="${DEBOT_ENV_DIR:-/opt/debot/scripts}"
STATE_DIR="${DEBOT_CANARY_STATE_DIR:-/opt/debot-canary}"

# Common KMS-encrypted shared key + RUST_LOG defaults
source "$ENV_DIR/debot_secrets_common.env"
source "$ENV_DIR/debot.env"

# Canary's per-account credentials. Schema mirrors debot-pair-btceth.env:
# LIGHTER_PUBLIC_API_KEY (KMS), LIGHTER_PRIVATE_API_KEY (KMS),
# LIGHTER_API_KEY_INDEX, LIGHTER_WALLET_ADDRESS,
# LIGHTER_EVM_WALLET_PRIVATE_KEY (KMS), LIGHTER_ACCOUNT_INDEX.
# WALLET_ADDRESS + EVM_WALLET_PRIVATE_KEY are shared with the main process
# (same parent EOA, different sub-account index).
source "$ENV_DIR/debot-pair-canary.env"

# Canary is a single-variant bot; no per-variant suffix needed (the bot's
# single-process A/B/C path looks for LIGHTER_*_{A,B,C} but a single
# strategy entry uses the unsuffixed vars sourced above).

# Independent state tree to avoid colliding with /opt/debot/.
export DEBOT_STATUS_DIR="${DEBOT_STATUS_DIR:-/home/ec2-user/debot_status}"
export DEBOT_STATUS_ID=debot-pair-canary
export PAIRTRADE_CONFIG_PATH=/opt/debot/configs/pairtrade/debot-pair-canary.yaml

# Wholesale skip the spot-markets fetch (perps-only, same as main; bot-strategy#128)
export LIGHTER_SKIP_SPOT_MARKETS=1

# Separate the manual-ack sentinel from the main bot so dropping ack on one
# bot can't accidentally release the other. bot-strategy#488 (the original
# /opt/debot/RISK_ACK is shared host-wide and was a footgun during the
# 2026-06-07 canary restart while variant C had to stay halted).
export RISK_ACK_PATH="$STATE_DIR/RISK_ACK"

# Deterministic startup spacing (bot-strategy#163). Even though canary only
# has one account on this EOA's 4th sub-account, the main process is already
# running, so /apikeys may have been hit in the recent past. ExecStartPre in
# the systemd unit takes care of the inter-process delay; this disables the
# in-process random jitter.
export LIGHTER_STARTUP_JITTER_SECS=0

# libsigner.so for Lighter Go bindings (same path as main).
if [ -f /opt/debot/lib/libsigner.so ]; then
    export LIGHTER_GO_PATH=/opt/debot/lib
    export LD_LIBRARY_PATH="${LIGHTER_GO_PATH}:${LD_LIBRARY_PATH:-}"
fi

mkdir -p "$DEBOT_STATUS_DIR" "$STATE_DIR/history_archive"

# Working dir is the canary state tree so pairtrade_history_BTC_ETH.json,
# risk_state.json, equity_history.jsonl all live next to each other and
# never touch the main process's copies.
cd "$STATE_DIR"

exec /opt/debot/bin/debot
