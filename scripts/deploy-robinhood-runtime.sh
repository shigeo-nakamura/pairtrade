#!/bin/bash
# Stage and validate Robinhood pairtrade artifacts before mutating /opt/debot.
set -euo pipefail

if [ "$#" -ne 4 ]; then
    echo "usage: $0 <s3-bucket> <install-dir> <debot-sha256> <libsigner-sha256>" >&2
    exit 2
fi

S3_BUCKET=$1
INSTALL_DIR=$2
EXPECTED_DEBOT_SHA=$3
EXPECTED_LIBSIGNER_SHA=$4
AWS_BIN=${AWS_BIN:-aws}
JQ_BIN=${JQ_BIN:-jq}
BOOTSTRAP_BIN=${BOOTSTRAP_BIN:-$INSTALL_DIR/scripts/bootstrap-robinhood-sidecar.sh}
INSTALL_OWNER=${INSTALL_OWNER:-root}
INSTALL_GROUP=${INSTALL_GROUP:-root}

if [[ ! "$EXPECTED_DEBOT_SHA" =~ ^[0-9a-f]{64}$ ]] ||
   [[ ! "$EXPECTED_LIBSIGNER_SHA" =~ ^[0-9a-f]{64}$ ]]; then
    echo "expected artifact SHA-256 values must each contain exactly 64 lowercase hex characters" >&2
    exit 2
fi

WORK=$(mktemp -d)
cleanup() {
    rm -rf -- "$WORK"
}
trap cleanup EXIT

STAGE="$WORK/pairtrade"
mkdir -p "$STAGE/bin" "$STAGE/lib"

"$AWS_BIN" s3 cp "s3://$S3_BUCKET/debot/debot" "$STAGE/bin/debot"
"$AWS_BIN" s3 cp "s3://$S3_BUCKET/debot/libsigner.so" "$STAGE/lib/libsigner.so"
"$AWS_BIN" s3 cp "s3://$S3_BUCKET/debot/checksums.sha256" "$STAGE/checksums.sha256"
"$AWS_BIN" s3 cp "s3://$S3_BUCKET/debot/manifest.json" "$STAGE/manifest.json"

echo "$EXPECTED_DEBOT_SHA  $STAGE/bin/debot" | sha256sum -c -
echo "$EXPECTED_LIBSIGNER_SHA  $STAGE/lib/libsigner.so" | sha256sum -c -
(cd "$STAGE" && sha256sum -c checksums.sha256)

"$JQ_BIN" -e \
    --arg debot_sha "$EXPECTED_DEBOT_SHA" \
    --arg libsigner_sha "$EXPECTED_LIBSIGNER_SHA" '
      .schema_version == 1 and
      .architecture == "aarch64" and
      .artifacts.debot.path == "bin/debot" and
      .artifacts.debot.sha256 == $debot_sha and
      .artifacts.debot.mode == "0755" and
      .artifacts.libsigner.path == "lib/libsigner.so" and
      .artifacts.libsigner.sha256 == $libsigner_sha and
      .artifacts.libsigner.mode == "0644" and
      (.dex_connector_sha | type == "string" and test("^[0-9a-f]{40}$"))
    ' "$STAGE/manifest.json" >/dev/null

# Validate/activate the sidecar against the staged manifest. A provenance
# failure therefore occurs before any installed pairtrade runtime artifact is
# changed (bot-strategy#836).
bash "$BOOTSTRAP_BIN" "$S3_BUCKET" "$STAGE"

install -d -o "$INSTALL_OWNER" -g "$INSTALL_GROUP" -m 0750 \
    "$INSTALL_DIR/bin" "$INSTALL_DIR/lib"
install -o "$INSTALL_OWNER" -g "$INSTALL_GROUP" -m 0755 \
    "$STAGE/bin/debot" "$INSTALL_DIR/bin/debot"
install -o "$INSTALL_OWNER" -g "$INSTALL_GROUP" -m 0644 \
    "$STAGE/lib/libsigner.so" "$INSTALL_DIR/lib/libsigner.so"
install -o "$INSTALL_OWNER" -g "$INSTALL_GROUP" -m 0644 \
    "$STAGE/checksums.sha256" "$INSTALL_DIR/checksums.sha256"
install -o "$INSTALL_OWNER" -g "$INSTALL_GROUP" -m 0644 \
    "$STAGE/manifest.json" "$INSTALL_DIR/manifest.json"

(cd "$INSTALL_DIR" && sha256sum -c checksums.sha256)
test "$(stat -c %U:%G:%a "$INSTALL_DIR/bin/debot")" = "$INSTALL_OWNER:$INSTALL_GROUP:755"
test "$(stat -c %U:%G:%a "$INSTALL_DIR/lib/libsigner.so")" = "$INSTALL_OWNER:$INSTALL_GROUP:644"

echo "Robinhood pairtrade runtime installed after sidecar preflight (debot=$EXPECTED_DEBOT_SHA libsigner=$EXPECTED_LIBSIGNER_SHA)"
