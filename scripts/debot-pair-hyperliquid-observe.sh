#!/bin/bash
# Production wrapper for the Hyperliquid read-only pairtrade observer
# (bot-strategy#724; deploys the #709 first slice as a persistent shadow
# collector on Frankfurt).
#
# - observe_only + dry_run: the hyperliquid-sdk connector is read-only —
#   every trading/account/signing method returns DexError::Permanent — so no
#   credentials or secrets env files are sourced here (public /info data
#   only, no Lighter WS/REST budget consumed).
# - Runs the dedicated hyperliquid feature build at /opt/debot-hl/bin/debot
#   deployed by deploy-hyperliquid-observer.yml, NOT /opt/debot/bin/debot:
#   lighter-sdk and hyperliquid-sdk are mutually exclusive feature builds.
# - State lives under /opt/debot-hl/ so its dump, history snapshot and
#   risk_state.json never collide with /opt/debot/, /opt/debot-canary/,
#   /opt/debot-sol/ or /opt/debot-shadow*/.

set -eu

STATE_DIR="${DEBOT_HL_STATE_DIR:-/opt/debot-hl}"

export RUST_LOG="${RUST_LOG:-info}"

export DEBOT_STATUS_DIR="${DEBOT_STATUS_DIR:-/home/ec2-user/debot_status}"
export DEBOT_STATUS_ID=debot-pair-hyperliquid-observe
export PAIRTRADE_CONFIG_PATH=/opt/debot/configs/pairtrade/debot-pair-hyperliquid-observe.yaml

# The connector factory reads the endpoints from the environment (see the
# header note in debot-pair-hyperliquid-observe.yaml).
export REST_ENDPOINT=https://api.hyperliquid.xyz
export WEB_SOCKET_ENDPOINT=wss://api.hyperliquid.xyz/ws

# Persistent-path overrides (pairtrade PR #167 runbook): the shared YAML
# ships /tmp smoke-test paths; resolved absolute paths keep the shadow
# collector's state isolated and reboot-safe. risk_state.json is derived as
# a sibling of the history file.
export DATA_DUMP_FILE="$STATE_DIR/market_data_hyperliquid_pairs.jsonl"
export HISTORY_ARCHIVE_DIR="$STATE_DIR/history_archive"
export PAIRTRADE_HISTORY_FILE="$STATE_DIR/pairtrade_history_hyperliquid.json"

# Own ack sentinel (bot-strategy#488 pattern); unused in observe mode but
# keeps the path disjoint from the other units.
export RISK_ACK_PATH="$STATE_DIR/RISK_ACK"

mkdir -p "$DEBOT_STATUS_DIR" "$STATE_DIR/history_archive"

# Working dir is the observer state tree so anything the engine writes
# relative to cwd lives next to the dump and history snapshot.
cd "$STATE_DIR"

exec /opt/debot-hl/bin/debot
