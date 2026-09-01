#!/bin/bash
# Install/update the isolated Phase 0A observer runtime. Does not start/restart it.
set -euo pipefail

INSTALL_DIR=${ENGINE_B_PHASE0_INSTALL_DIR:-/opt/engine-b-phase0}
PYTHON_BIN=${ENGINE_B_PHASE0_PYTHON_BIN:-/usr/bin/python3.11}
REQUIREMENTS=${ENGINE_B_PHASE0_REQUIREMENTS:-/opt/debot/scripts/engine_b_phase0_requirements.txt}
UNIT_SOURCE_DIR=${ENGINE_B_PHASE0_UNIT_SOURCE_DIR:-/opt/debot/deploy}
CODE_COMMIT=${ENGINE_B_PHASE0_CODE_COMMIT:-}

if [ ! -x "$PYTHON_BIN" ]; then
  echo "Python 3.11 is required at $PYTHON_BIN" >&2
  exit 1
fi
if [ ! -f "$REQUIREMENTS" ]; then
  echo "Requirements lock is missing: $REQUIREMENTS" >&2
  exit 1
fi
if [[ ! "$CODE_COMMIT" =~ ^[0-9a-f]{40}$ ]]; then
  echo "ENGINE_B_PHASE0_CODE_COMMIT must be the deployed 40-character Git SHA" >&2
  exit 1
fi
for unit in engine-b-phase0.service engine-b-phase0-archive.service engine-b-phase0-archive.timer; do
  if [ ! -f "$UNIT_SOURCE_DIR/$unit" ]; then
    echo "Engine B systemd unit is missing: $UNIT_SOURCE_DIR/$unit" >&2
    exit 1
  fi
done

version=$($PYTHON_BIN -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
if [ "$version" != "3.11" ]; then
  echo "Expected Python 3.11, got $version from $PYTHON_BIN" >&2
  exit 1
fi

install -d -o ec2-user -g ec2-user -m 0750 "$INSTALL_DIR"
if [ ! -x "$INSTALL_DIR/venv/bin/python" ]; then
  sudo -u ec2-user "$PYTHON_BIN" -m venv "$INSTALL_DIR/venv"
fi
sudo -u ec2-user "$INSTALL_DIR/venv/bin/pip" install \
  --disable-pip-version-check --requirement "$REQUIREMENTS"

requirements_hash=$(sha256sum "$REQUIREMENTS" | cut -d' ' -f1)
installed_version=$($INSTALL_DIR/venv/bin/python -c 'import websockets; print(websockets.__version__)')
if [ "$installed_version" != "15.0.1" ]; then
  echo "Unexpected websockets version after install: $installed_version" >&2
  exit 1
fi
systemd-analyze verify \
  "$UNIT_SOURCE_DIR/engine-b-phase0.service" \
  "$UNIT_SOURCE_DIR/engine-b-phase0-archive.service" \
  "$UNIT_SOURCE_DIR/engine-b-phase0-archive.timer"
for unit in engine-b-phase0.service engine-b-phase0-archive.service engine-b-phase0-archive.timer; do
  install -o root -g root -m 0644 "$UNIT_SOURCE_DIR/$unit" "/etc/systemd/system/$unit"
done
systemctl daemon-reload

release_tmp=$(mktemp "$INSTALL_DIR/.release.env.XXXXXX")
trap 'rm -f -- "$release_tmp"' EXIT
printf 'ENGINE_B_PHASE0_CODE_COMMIT=%s\n' "$CODE_COMMIT" > "$release_tmp"
install -o ec2-user -g ec2-user -m 0640 "$release_tmp" "$INSTALL_DIR/release.env"
rm -f -- "$release_tmp"
trap - EXIT

printf 'requirements_sha256=%s\npython_version=%s\nwebsockets_version=%s\ncode_commit=%s\n' \
  "$requirements_hash" "$version" "$installed_version" "$CODE_COMMIT" \
  > "$INSTALL_DIR/runtime-manifest.txt"
chown ec2-user:ec2-user "$INSTALL_DIR/runtime-manifest.txt"
chmod 0640 "$INSTALL_DIR/runtime-manifest.txt"

echo "Engine B Phase 0 runtime and units installed; services were not started or restarted."
