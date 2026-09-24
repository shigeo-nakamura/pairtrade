#!/bin/bash
# Install the Lighter Core points snapshot timer on Frankfurt (bot-strategy#938).
#
# Owner-run once on the Frankfurt host (i-0c08fba996bc21879), as root:
#   sudo bash /opt/debot/scripts/install_lighter_core_points_snapshot.sh
#
# Copies the oneshot service + hourly timer from /opt/debot/deploy (where
# deploy-configs.yml delivers them), creates the output directory, and
# enables + starts the timer. `debot-bull-holder` is NOT touched: the
# collector only reads the same credential file and writes its own history.
#
# Why this exists: the Robinhood host's timer measures the Robinhood-chain
# arms, and its single Core arm is the #1046 hedge's short leg, which is
# DRY_RUN and does not trade. Without a Core arm that does trade, the
# Core side of the cost-per-point KPI has no denominator, which is what
# blocks the Core:RH allocation decision in bot-strategy#925.
#
# Re-running is safe: units are re-copied, the timer stays enabled. To
# verify: `systemctl list-timers lighter-core-points-snapshot.timer` and,
# after the first run, `journalctl -u lighter-core-points-snapshot.service`.
set -euo pipefail

UNIT_SOURCE_DIR=${LIGHTER_CORE_POINTS_UNIT_SOURCE_DIR:-/opt/debot/deploy}
SYSTEMD_DIR=${LIGHTER_CORE_POINTS_SYSTEMD_DIR:-/etc/systemd/system}
COLLECTOR=${LIGHTER_CORE_POINTS_COLLECTOR:-/opt/debot/scripts/robinhood_points_collector.py}
OUT_DIR=${LIGHTER_CORE_POINTS_OUT_DIR:-/opt/debot/status/lighter-core}
SYSTEMCTL=${LIGHTER_CORE_POINTS_SYSTEMCTL:-systemctl}
# The arm's credentials and the Go signer the token mint needs. Both are
# named in the unit; checked here so a missing one fails the install
# rather than every hourly run.
ARM_ENV=${LIGHTER_CORE_POINTS_ARM_ENV:-/opt/debot/scripts/debot-bull-holder.env}
LIBSIGNER=${LIGHTER_CORE_POINTS_LIBSIGNER:-/opt/debot-bull-holder/lib/libsigner.so}
# The unit has no User=, so the history directory belongs to root in
# production; overridable so the installer's test can run unprivileged.
OWNER=${LIGHTER_CORE_POINTS_OWNER:-root}

for source in "$COLLECTOR" "$ARM_ENV" "$LIBSIGNER" \
              "$UNIT_SOURCE_DIR/lighter-core-points-snapshot.service" \
              "$UNIT_SOURCE_DIR/lighter-core-points-snapshot.timer"; do
  if [ ! -f "$source" ]; then
    echo "Lighter Core points snapshot source is missing: $source" >&2
    exit 1
  fi
done
if ! python3 -c 'import cryptography' 2>/dev/null; then
  echo "python3 'cryptography' module is required (AES-CBC decrypt of the API key)" >&2
  exit 1
fi

# 0750 root: the history holds only account indices and point tallies, but
# it sits next to the holder's state and there is no reader that needs it
# on the host -- the readout reads the S3 mirror.
install -d -o "$OWNER" -g "$OWNER" -m 0750 "$OUT_DIR"
install -m 0644 "$UNIT_SOURCE_DIR/lighter-core-points-snapshot.service" \
  "$SYSTEMD_DIR/lighter-core-points-snapshot.service"
install -m 0644 "$UNIT_SOURCE_DIR/lighter-core-points-snapshot.timer" \
  "$SYSTEMD_DIR/lighter-core-points-snapshot.timer"
"$SYSTEMCTL" daemon-reload
"$SYSTEMCTL" enable --now lighter-core-points-snapshot.timer
echo "lighter-core-points-snapshot.timer installed and enabled; history -> $OUT_DIR/points_history.jsonl"
