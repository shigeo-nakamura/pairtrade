#!/bin/bash
# Pin one complete Robinhood release, then activate its matching sidecar.
set -euo pipefail

if [ "$#" -lt 1 ] || [ "$#" -gt 2 ]; then
    echo "usage: $0 <pairtrade-install-dir> [runtime-pin]" >&2
    exit 2
fi

INSTALL_DIR=$1
CURRENT_LINK=${ROBINHOOD_RUNTIME_CURRENT:-$INSTALL_DIR/robinhood-runtime-current}
PIN_LINK=${ROBINHOOD_RUNTIME_PIN:-${2:-/run/debot-pair-robinhood-lighter/runtime}}
BOOTSTRAP_BIN=${BOOTSTRAP_BIN:-$INSTALL_DIR/scripts/bootstrap-robinhood-sidecar.sh}
MV_BIN=${MV_BIN:-mv}

# A config-only deploy can install this dependency before the first versioned
# runtime deploy. Pin the legacy tree in that migration state so a crash restart
# preserves the old bot+sidecar behavior instead of becoming unavailable.
if [ -e "$CURRENT_LINK" ] || [ -L "$CURRENT_LINK" ]; then
    RELEASE_DIR=$(readlink -f -- "$CURRENT_LINK")
    if [ -z "$RELEASE_DIR" ] || [ ! -d "$RELEASE_DIR" ]; then
        echo "Robinhood current runtime does not resolve to a release: $CURRENT_LINK" >&2
        exit 2
    fi
else
    RELEASE_DIR=$(readlink -f -- "$INSTALL_DIR")
fi

install -d -m 0755 "$(dirname "$PIN_LINK")"
PIN_TMP="$PIN_LINK.tmp.$$"
cleanup() {
    rm -f -- "$PIN_TMP"
}
trap cleanup EXIT
ln -s "$RELEASE_DIR" "$PIN_TMP"
"$MV_BIN" -Tf "$PIN_TMP" "$PIN_LINK"
trap - EXIT

BUNDLE_DIR="$RELEASE_DIR/robinhood-sidecar-bundle"
if [ ! -d "$BUNDLE_DIR" ]; then
    echo "WARNING: Robinhood sidecar bundle not staged yet; pinned legacy runtime and preserved the existing sidecar" >&2
    exit 0
fi

for required in lighter-ratelimit lighter-ratelimit.sha256 manifest.json lighter-ratelimit.service; do
    if [ ! -f "$BUNDLE_DIR/$required" ]; then
        echo "Robinhood pinned sidecar bundle is incomplete: $BUNDLE_DIR/$required" >&2
        exit 2
    fi
done

export SIDECAR_BUNDLE_DIR="$BUNDLE_DIR"
echo "Activating Robinhood sidecar from pinned runtime release: $RELEASE_DIR"
exec bash "$BOOTSTRAP_BIN" local-bundle "$RELEASE_DIR"
