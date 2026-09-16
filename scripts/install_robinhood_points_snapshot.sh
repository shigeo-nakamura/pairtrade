#!/bin/bash
# Install the Robinhood Chain Lighter points snapshot timer (bot-strategy#938).
#
# Owner-run once on the Tokyo host (i-0095af4fe0efbc5dd), as root:
#   sudo bash /opt/debot/scripts/install_robinhood_points_snapshot.sh
#
# Copies the oneshot service + hourly timer from /opt/debot/deploy (where
# deploy-configs.yml delivers them), creates the output directory for
# ec2-user, and enables + starts the timer. The frozen pairtrade service
# (`debot-pair-robinhood-lighter`) is not touched: the collector only reads
# the same credential files as ec2-user and writes its own history file.
#
# Re-running is safe: units are re-copied, the timer stays enabled. To
# verify: `systemctl list-timers robinhood-points-snapshot.timer` and, after
# the first run, `journalctl -u robinhood-points-snapshot.service`.
set -euo pipefail

UNIT_SOURCE_DIR=${ROBINHOOD_POINTS_UNIT_SOURCE_DIR:-/opt/debot/deploy}
SYSTEMD_DIR=${ROBINHOOD_POINTS_SYSTEMD_DIR:-/etc/systemd/system}
COLLECTOR=${ROBINHOOD_POINTS_COLLECTOR:-/opt/debot/scripts/robinhood_points_collector.py}
OUT_DIR=${ROBINHOOD_POINTS_OUT_DIR:-/home/ec2-user/debot_status/robinhood-points}
SYSTEMCTL=${ROBINHOOD_POINTS_SYSTEMCTL:-systemctl}
RUN_AS=${ROBINHOOD_POINTS_USER:-ec2-user}

for source in "$COLLECTOR" "$UNIT_SOURCE_DIR/robinhood-points-snapshot.service" \
              "$UNIT_SOURCE_DIR/robinhood-points-snapshot.timer"; do
  if [ ! -f "$source" ]; then
    echo "Robinhood points snapshot source is missing: $source" >&2
    exit 1
  fi
done
if ! python3 -c 'import cryptography' 2>/dev/null; then
  echo "python3 'cryptography' module is required (AES-CBC decrypt of the API key)" >&2
  exit 1
fi

install -d -o "$RUN_AS" -g "$RUN_AS" -m 0750 "$OUT_DIR"
install -m 0644 "$UNIT_SOURCE_DIR/robinhood-points-snapshot.service" \
  "$SYSTEMD_DIR/robinhood-points-snapshot.service"
install -m 0644 "$UNIT_SOURCE_DIR/robinhood-points-snapshot.timer" \
  "$SYSTEMD_DIR/robinhood-points-snapshot.timer"
"$SYSTEMCTL" daemon-reload
"$SYSTEMCTL" enable --now robinhood-points-snapshot.timer
echo "robinhood-points-snapshot.timer installed and enabled; history -> $OUT_DIR/points_history.jsonl"
