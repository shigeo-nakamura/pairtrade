#!/bin/bash
# Install/update the Engine B live-experiment binary (bot-strategy#866).
# Does not start/restart the service and never touches the host-only
# secrets file (/etc/engine-b-live/live-secrets.env) beyond ensuring its
# parent directory exists with the right ownership -- that file is created
# once, by hand, per docs/engine-b-live-operations.md's credential
# provisioning runbook, and this installer must never overwrite it.
set -euo pipefail

INSTALL_DIR=${ENGINE_B_LIVE_INSTALL_DIR:-/opt/engine-b-live}
SECRETS_DIR=${ENGINE_B_LIVE_SECRETS_DIR:-/etc/engine-b-live}
BINARY_SOURCE=${ENGINE_B_LIVE_BINARY_SOURCE:-/opt/debot/bin/engine_b_live}
CALENDAR_SOURCE=${ENGINE_B_LIVE_CALENDAR_SOURCE:-/opt/debot/configs/engine-b/trading_calendar.json}
CONFIG_SOURCE=${ENGINE_B_LIVE_CONFIG_SOURCE:-/opt/debot/configs/engine-b/live.json}
UNIT_SOURCE_DIR=${ENGINE_B_LIVE_UNIT_SOURCE_DIR:-/opt/debot/deploy}
SERVICE_USER=engine-b-live
SERVICE_GROUP=engine-b-live
STATE_DIR=/var/lib/engine-b-live

for source in "$BINARY_SOURCE" "$CALENDAR_SOURCE" "$CONFIG_SOURCE" "$UNIT_SOURCE_DIR/engine-b-live.service"; do
  if [ ! -f "$source" ]; then
    echo "Engine B live runtime source is missing: $source" >&2
    exit 1
  fi
done

if ! getent group "$SERVICE_GROUP" >/dev/null; then
  groupadd --system "$SERVICE_GROUP"
fi
if ! id -u "$SERVICE_USER" >/dev/null 2>&1; then
  useradd --system --gid "$SERVICE_GROUP" --home-dir /nonexistent \
    --shell /sbin/nologin --no-create-home "$SERVICE_USER"
fi

install -d -o root -g "$SERVICE_GROUP" -m 0750 "$INSTALL_DIR" "$INSTALL_DIR/bin"
install -o root -g "$SERVICE_GROUP" -m 0550 "$BINARY_SOURCE" "$INSTALL_DIR/bin/engine_b_live"
install -o root -g "$SERVICE_GROUP" -m 0440 "$CALENDAR_SOURCE" "$INSTALL_DIR/trading_calendar.json"

# Non-secret tuning config: configs/engine-b/live.json (git-committed) ->
# KEY=VALUE env file. This is the only place JSON-to-env conversion
# happens; the secrets file is hand-assembled separately and never passes
# through this script (see docs/engine-b-live-operations.md).
python3 -c "
import json
with open('$CONFIG_SOURCE') as f:
    data = json.load(f)
lines = [f'{k}={v}' for k, v in data.items() if not k.startswith('_')]
with open('$INSTALL_DIR/live-config.env.tmp', 'w') as f:
    f.write('\n'.join(lines) + '\n')
"
mv "$INSTALL_DIR/live-config.env.tmp" "$INSTALL_DIR/live-config.env"
chown root:"$SERVICE_GROUP" "$INSTALL_DIR/live-config.env"
chmod 0440 "$INSTALL_DIR/live-config.env"

# Secrets directory only -- never the file itself. A missing secrets file
# means the service will fail to start (get_lighter_config_from_env's
# `.expect(...)` calls), which is the correct fail-closed behavior before
# an operator has completed the provisioning runbook.
install -d -o root -g "$SERVICE_GROUP" -m 0750 "$SECRETS_DIR"

install -d -o "$SERVICE_USER" -g "$SERVICE_GROUP" -m 0750 "$STATE_DIR"

systemd-analyze verify "$UNIT_SOURCE_DIR/engine-b-live.service"
install -o root -g root -m 0644 "$UNIT_SOURCE_DIR/engine-b-live.service" /etc/systemd/system/engine-b-live.service
systemctl daemon-reload

echo "Engine B live runtime and unit installed; service was not started or restarted."
if [ ! -f "$SECRETS_DIR/live-secrets.env" ]; then
  echo "NOTE: $SECRETS_DIR/live-secrets.env does not exist yet -- the service cannot start until an operator completes the credential provisioning runbook in docs/engine-b-live-operations.md."
fi
