#!/bin/bash
# Robinhood Chain Lighter launcher — two-arm split (bot-strategy#798/#868).
# freq reuses the original sub-account: it relies on
# pairtrade::config::lighter_env's fallback to the *unsuffixed* vars, so
# unlike the Frankfurt A/B/C launcher (scripts/debot-pair-btceth.sh) this
# script cannot blanket `unset` them afterward. b gets its own sub-account
# (freq2's 50x clone arm was terminated 2026-09-01, bot-strategy#868);
# credentials are loaded via the same source-in-a-subshell technique
# as debot-pair-btceth.sh's `vars_for_variant` (handles comments/whitespace
# correctly, unlike a hand-rolled `IFS='=' read` line parser) and
# re-exported with the per-variant suffix (lighter_env -> id.to_uppercase()).
#
# Because freq's unsuffixed fallback must stay in place, a missing/
# unreadable/misconfigured variant env can't be caught by unsetting the
# unsuffixed vars the way btceth does — lighter_env() would silently
# resolve a variant's credentials to freq's own (real-money) account instead of
# erroring, doubling notional exposure on one account with no crash or log
# line (PR #214 review). The explicit check below is the fail-loud guard
# btceth gets for free from its unset.
set -eu

ENV_DIR="${DEBOT_ENV_DIR:-/opt/debot/scripts}"
STATE_DIR="${DEBOT_ROBINHOOD_STATE_DIR:-/opt/debot-robinhood-lighter}"
RUNTIME_DIR="${DEBOT_ROBINHOOD_RUNTIME_DIR:-/run/debot-pair-robinhood-lighter/runtime}"

set -a
source "$ENV_DIR/debot_secrets_common.env"
source "$ENV_DIR/debot.env"
source "$ENV_DIR/debot-pair-robinhood-lighter.env"
set +a

# Additional sub-account credentials, mirroring debot-pair-btceth.sh's
# vars_for_variant: source each file in a subshell (never leaks into this
# shell's unsuffixed vars, and correctly handles quoting/inline comments/
# leading whitespace the way a hand-rolled parser doesn't) and echo out
# only the known Lighter credential vars, suffixed for lighter_env().
vars_for_variant() {
  (
    suffix="$2"
    # Do not let the subshell inherit freq's unsuffixed credentials. The
    # variant file must provide its own keys or the fail-loud check below
    # must reject startup.
    unset LIGHTER_PUBLIC_API_KEY LIGHTER_PRIVATE_API_KEY \
          LIGHTER_API_KEY_INDEX LIGHTER_WALLET_ADDRESS \
          LIGHTER_EVM_WALLET_PRIVATE_KEY LIGHTER_ACCOUNT_INDEX
    # shellcheck disable=SC1090
    source "$1" >/dev/null 2>&1
    for var in LIGHTER_PUBLIC_API_KEY LIGHTER_PRIVATE_API_KEY \
               LIGHTER_API_KEY_INDEX LIGHTER_WALLET_ADDRESS \
               LIGHTER_EVM_WALLET_PRIVATE_KEY LIGHTER_ACCOUNT_INDEX; do
      if [ -n "${!var:-}" ]; then
        printf '%s_%s=%s\n' "$var" "$suffix" "${!var}"
      fi
    done
  )
}
while IFS='=' read -r _key _val; do
  export "$_key=$_val"
done < <(vars_for_variant "$ENV_DIR/debot-pair-robinhood-lighter-b.env" B)
unset _key _val

# Fail loudly instead of letting lighter_env() silently fall back to
# freq's unsuffixed credentials: a missing file, empty file, or a source
# error inside vars_for_variant (none of which trip `set -e` through the process
# substitution above) must not be allowed to start the process.
require_variant_credentials() {
  suffix="$1"
  env_file="$2"
  public_var="LIGHTER_PUBLIC_API_KEY_${suffix}"
  private_var="LIGHTER_PRIVATE_API_KEY_${suffix}"
  if [ -z "${!public_var:-}" ] || [ -z "${!private_var:-}" ]; then
    echo "FATAL: ${public_var} / ${private_var} not set after loading ${env_file};" \
         "refusing to start because this variant would silently authenticate against" \
         "freq's own account. Check the file exists and is readable." >&2
    exit 1
  fi
}
require_variant_credentials B "$ENV_DIR/debot-pair-robinhood-lighter-b.env"

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

if [ ! -x "$RUNTIME_DIR/bin/debot" ]; then
    echo "FATAL: pinned Robinhood runtime is missing executable: $RUNTIME_DIR/bin/debot" >&2
    exit 1
fi

if [ -f "$RUNTIME_DIR/lib/libsigner.so" ]; then
    export LIGHTER_GO_PATH="$RUNTIME_DIR/lib"
    export LD_LIBRARY_PATH="${LIGHTER_GO_PATH}:${LD_LIBRARY_PATH:-}"
fi

mkdir -p "$DEBOT_STATUS_DIR" "$STATE_DIR/history_archive_robinhood_lighter"
cd "$STATE_DIR"
exec "$RUNTIME_DIR/bin/debot"
