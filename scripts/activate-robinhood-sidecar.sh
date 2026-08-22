#!/bin/bash
# Activate the staged Robinhood sidecar only during a coordinated bot start.
set -euo pipefail

if [ "$#" -ne 1 ]; then
    echo "usage: $0 <pairtrade-install-dir>" >&2
    exit 2
fi

INSTALL_DIR=$1
BUCKET_FILE=${SIDECAR_BUCKET_FILE:-$INSTALL_DIR/robinhood-sidecar-s3-bucket}
BOOTSTRAP_BIN=${BOOTSTRAP_BIN:-$INSTALL_DIR/scripts/bootstrap-robinhood-sidecar.sh}

if [ ! -f "$BUCKET_FILE" ]; then
    echo "Robinhood sidecar bucket file is missing: $BUCKET_FILE" >&2
    exit 2
fi

mapfile -t BUCKET_LINES < "$BUCKET_FILE"
if [ "${#BUCKET_LINES[@]}" -ne 1 ] ||
   [[ ! "${BUCKET_LINES[0]}" =~ ^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$ ]]; then
    echo "Robinhood sidecar bucket file is invalid: $BUCKET_FILE" >&2
    exit 2
fi
S3_BUCKET=${BUCKET_LINES[0]}

echo "Activating Robinhood sidecar during coordinated bot start (bucket=$S3_BUCKET)"
exec bash "$BOOTSTRAP_BIN" "$S3_BUCKET" "$INSTALL_DIR"
