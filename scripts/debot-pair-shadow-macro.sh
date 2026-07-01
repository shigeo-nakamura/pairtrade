#!/bin/bash
# Production wrapper for the NON-CRYPTO multi-pair observe-only data collector
# (bot-strategy#513 pair/universe screen; sibling of debot-pair-shadow #560).
#
# - observe_only + dry_run: never places orders, real or simulated. Its sole
#   deliverable is /opt/debot-shadow-macro/market_data_pairmacro_*.jsonl plus
#   the live engine's [METRICS] eligibility stream for the #513 non-crypto
#   universe (metals XAU/XAG + XPT/XPD, energy WTI/BRENTOIL, index SPY/QQQ, FX
#   EURUSD/GBPUSD, equity NVDA/AMD — real ~5s cadence + real L1 quotes).
# - Reuses the canary's Lighter sub-account credentials
#   (debot-pair-canary.env). Safe because observe mode signs no transactions
#   (no nonce contention). REST fallback is disabled, so it only consumes
#   Lighter WS market data.
# - Always uses the latest /opt/debot/bin/debot deployed by CI, but is NOT in
#   the ci.yml auto-restart allowlist — restart manually after deploys if a
#   dump-format change matters (none expected).
# - State lives under /opt/debot-shadow-macro/ so its history snapshots,
#   risk_state, etc. never collide with /opt/debot/, /opt/debot-canary/,
#   /opt/debot-sol/, or /opt/debot-shadow/ (the crypto sibling).

set -eu

ENV_DIR="${DEBOT_ENV_DIR:-/opt/debot/scripts}"
STATE_DIR="${DEBOT_SHADOW_MACRO_STATE_DIR:-/opt/debot-shadow-macro}"

# Common KMS-encrypted shared key + RUST_LOG defaults
source "$ENV_DIR/debot_secrets_common.env"
source "$ENV_DIR/debot.env"

# Canary sub-account credentials (see header for why sharing is safe here).
source "$ENV_DIR/debot-pair-canary.env"

export DEBOT_STATUS_DIR="${DEBOT_STATUS_DIR:-/home/ec2-user/debot_status}"
export DEBOT_STATUS_ID=debot-pair-shadow-macro
export PAIRTRADE_CONFIG_PATH=/opt/debot/configs/pairtrade/debot-pair-shadow-macro.yaml

# Perps-only, same as main (bot-strategy#128). Disable Lighter REST
# market-data fallback so this observe-only collector cannot consume the live
# bot's REST budget when WS is stale.
export LIGHTER_SKIP_SPOT_MARKETS=1
export LIGHTER_DISABLE_REST_FALLBACK=1

# Optional REST preflight for manual config changes. Disabled by default because
# this observer must not spend Lighter REST budget during normal startup.
if [ "${LIGHTER_ALLOW_SHADOW_STARTUP_REST:-0}" = "1" ]; then
    python3 "$ENV_DIR/check_lighter_config_markets.py" "$PAIRTRADE_CONFIG_PATH"
fi

# Own ack sentinel (bot-strategy#488 pattern); unused in observe mode but
# keeps the path disjoint from main/canary/sol/shadow.
export RISK_ACK_PATH="$STATE_DIR/RISK_ACK"

# Deterministic startup spacing (bot-strategy#163): the systemd unit's
# ExecStartPre sleep provides inter-process spacing; disable in-process jitter.
export LIGHTER_STARTUP_JITTER_SECS=0

# libsigner.so for Lighter Go bindings (same path as main).
if [ -f /opt/debot/lib/libsigner.so ]; then
    export LIGHTER_GO_PATH=/opt/debot/lib
    export LD_LIBRARY_PATH="${LIGHTER_GO_PATH}:${LD_LIBRARY_PATH:-}"
fi

mkdir -p "$DEBOT_STATUS_DIR" "$STATE_DIR/history_archive"

# Working dir is the observer state tree so its history snapshots,
# risk_state.json, equity_history.jsonl all live next to each other.
cd "$STATE_DIR"

exec /opt/debot/bin/debot
