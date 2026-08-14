#!/bin/bash
# Deploy Robinhood Lighter configs and record when a running bot owes a restart.
set -euo pipefail

if [ "$#" -ne 2 ]; then
    echo "usage: $0 <s3-bucket> <install-dir>" >&2
    exit 2
fi

S3_BUCKET=$1
INSTALL_DIR=$2
CONFIG_REL=configs/pairtrade/debot-pair-robinhood-lighter.yaml
CONFIG_PATH="$INSTALL_DIR/$CONFIG_REL"
STATE_DIR=/opt/debot-robinhood-lighter
MARKER="$STATE_DIR/RESTART_PENDING"
SERVICE=debot-pair-robinhood-lighter

old_sha=missing
if [ -f "$CONFIG_PATH" ]; then
    old_sha=$(sha256sum "$CONFIG_PATH" | awk "{print \$1}")
fi

aws s3 sync "s3://$S3_BUCKET/debot/configs" "$INSTALL_DIR/configs"

new_sha=$(sha256sum "$CONFIG_PATH" | awk "{print \$1}")
if [ "$old_sha" != "$new_sha" ] && systemctl is-active --quiet "$SERVICE"; then
    install -d -o ec2-user -g ec2-user -m 0750 "$STATE_DIR"
    {
        echo "deployed_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
        echo "config=$CONFIG_PATH"
        echo "old_sha=$old_sha"
        echo "new_sha=$new_sha"
        echo "action=manual restart required to load the deployed config"
    } > "$MARKER"
    chown ec2-user:ec2-user "$MARKER"
    echo "WARNING: Robinhood config changed while $SERVICE is active; $MARKER created"
fi
