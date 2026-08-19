#!/bin/bash
# Robinhood Chain Lighter launcher — two-arm split (bot-strategy#798,
# 2026-08-18). freq reuses the original sub-account (unsuffixed env vars,
# falls back naturally via pairtrade::config::lighter_env). b gets its own
# sub-account, credentials re-exported with the _B suffix (lighter_env ->
# id.to_uppercase().replace('-','_')) so they don't collide with freq's.
set -eu

ENV_DIR="${DEBOT_ENV_DIR:-/opt/debot/scripts}"
STATE_DIR="${DEBOT_ROBINHOOD_STATE_DIR:-/opt/debot-robinhood-lighter}"

set -a
source "$ENV_DIR/debot_secrets_common.env"
source "$ENV_DIR/debot.env"
source "$ENV_DIR/debot-pair-robinhood-lighter.env"
set +a

# b-arm sub-account credentials: read KEY=VALUE lines directly (no `source`,
# so they never land in unsuffixed vars and clobber freq's) and re-export
# with the _B suffix.
while IFS='=' read -r _key _val; do
  case "$_key" in
    LIGHTER_PUBLIC_API_KEY|LIGHTER_PRIVATE_API_KEY|LIGHTER_API_KEY_INDEX|LIGHTER_ACCOUNT_INDEX|LIGHTER_PLAIN_PUBLIC_API_KEY|LIGHTER_PLAIN_PRIVATE_API_KEY|LIGHTER_EVM_WALLET_PRIVATE_KEY)
      export "${_key}_B=$_val"
      ;;
  esac
done < <(grep -v '^[[:space:]]*#' "$ENV_DIR/debot-pair-robinhood-lighter-b.env" | grep '=')
unset _key _val

# Set after debot.env so generic values cannot route this bot to another venue.
export REST_ENDPOINT=https://api.rh.lighter.xyz
export WEB_SOCKET_ENDPOINT=wss://api.rh.lighter.xyz/stream
export DRY_RUN="${ROBINHOOD_LIGHTER_DRY_RUN:-true}"
export PAIRTRADE_CONFIG_PATH=/opt/debot/configs/pairtrade/debot-pair-robinhood-lighter.yaml
export DEBOT_STATUS_DIR="${DEBOT_STATUS_DIR:-/home/ec2-user/debot_status}"
export DEBOT_STATUS_ID=debot-pair-robinhood-lighter
export LIGHTER_SKIP_SPOT_MARKETS=1
export LIGHTER_STARTUP_JITTER_SECS=0
export RISK_ACK_PATH="$STATE_DIR/RISK_ACK"
export RESTART_PENDING_PATH="$STATE_DIR/RESTART_PENDING"

if [ -f /opt/debot/lib/libsigner.so ]; then
    export LIGHTER_GO_PATH=/opt/debot/lib
    export LD_LIBRARY_PATH="${LIGHTER_GO_PATH}:${LD_LIBRARY_PATH:-}"
fi

mkdir -p "$DEBOT_STATUS_DIR" "$STATE_DIR/history_archive_robinhood_lighter"
cd "$STATE_DIR"
exec /opt/debot/bin/debot
