#!/bin/bash
# Activate the staged Robinhood sidecar only during a coordinated bot start.
set -euo pipefail

if [ "$#" -ne 1 ]; then
    echo "usage: $0 <pairtrade-install-dir>" >&2
    exit 2
fi

INSTALL_DIR=$1
BUNDLE_DIR=${SIDECAR_BUNDLE_DIR:-$INSTALL_DIR/robinhood-sidecar-bundle}
BOOTSTRAP_BIN=${BOOTSTRAP_BIN:-$INSTALL_DIR/scripts/bootstrap-robinhood-sidecar.sh}
FLOCK_BIN=${FLOCK_BIN:-flock}
RUNTIME_LOCK_FILE=${ROBINHOOD_RUNTIME_LOCK_FILE:-/run/lock/debot-robinhood-runtime.lock}

# Runtime publication uses the same lock. Because this helper runs as a
# separate systemd dependency, waiting here cannot deadlock the bot start job;
# the bot has not entered ExecStartPre/ExecStart yet.
install -d -m 0755 "$(dirname "$RUNTIME_LOCK_FILE")"
exec 9>"$RUNTIME_LOCK_FILE"
"$FLOCK_BIN" -x 9

# Migration guard: config-only deploys can install this hook before the first
# runtime deploy has staged a local bundle. Preserve the pre-hook behavior until
# that runtime migration completes instead of blocking a crash restart.
if [ ! -d "$BUNDLE_DIR" ]; then
    echo "WARNING: Robinhood sidecar bundle not staged yet; preserving the existing sidecar" >&2
    exit 0
fi

for required in lighter-ratelimit lighter-ratelimit.sha256 manifest.json lighter-ratelimit.service; do
    if [ ! -f "$BUNDLE_DIR/$required" ]; then
        echo "Robinhood staged sidecar bundle is incomplete: $BUNDLE_DIR/$required" >&2
        exit 2
    fi
done

export SIDECAR_BUNDLE_DIR="$BUNDLE_DIR"
echo "Activating Robinhood sidecar from local staged bundle during coordinated bot start"
exec bash "$BOOTSTRAP_BIN" local-bundle "$INSTALL_DIR"
