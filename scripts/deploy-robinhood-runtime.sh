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
RUNTIME_DIR_OWNER=${RUNTIME_DIR_OWNER:-ec2-user}
RUNTIME_DIR_GROUP=${RUNTIME_DIR_GROUP:-ec2-user}
INSTALL_BIN=${INSTALL_BIN:-install}

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

# Validate the matching sidecar without installing or restarting it. The old
# sidecar remains active until systemd coordinates activation with the next bot
# start (#836).
SIDECAR_STAGE_DIR="$STAGE/robinhood-sidecar-bundle" \
    bash "$BOOTSTRAP_BIN" --validate-only "$S3_BUCKET" "$STAGE"

RUNTIME_FILES=(
    bin/debot
    lib/libsigner.so
    checksums.sha256
    manifest.json
    robinhood-sidecar-bundle/lighter-ratelimit
    robinhood-sidecar-bundle/lighter-ratelimit.sha256
    robinhood-sidecar-bundle/manifest.json
    robinhood-sidecar-bundle/lighter-ratelimit.service
)
BACKUP="$WORK/runtime-backup"
PRESENT_FILE="$WORK/runtime-present"
mkdir -p "$BACKUP"
: > "$PRESENT_FILE"
BUNDLE_DIR_PREEXISTED=false
if [ -d "$INSTALL_DIR/robinhood-sidecar-bundle" ]; then
    BUNDLE_DIR_PREEXISTED=true
fi
for relative in "${RUNTIME_FILES[@]}"; do
    target="$INSTALL_DIR/$relative"
    if [ -e "$target" ]; then
        mkdir -p "$BACKUP/$(dirname "$relative")"
        cp -a -- "$target" "$BACKUP/$relative"
        printf '%s\n' "$relative" >> "$PRESENT_FILE"
    fi
done

rollback_runtime() {
    for relative in "${RUNTIME_FILES[@]}"; do
        target="$INSTALL_DIR/$relative"
        rm -f -- "$target"
        if grep -Fxq "$relative" "$PRESENT_FILE"; then
            mkdir -p "$INSTALL_DIR/$(dirname "$relative")"
            cp -a -- "$BACKUP/$relative" "$target"
        fi
    done
    if [ "$BUNDLE_DIR_PREEXISTED" = false ]; then
        rmdir -- "$INSTALL_DIR/robinhood-sidecar-bundle" 2>/dev/null || true
    fi
}

commit_runtime() {
    "$INSTALL_BIN" -d -o "$RUNTIME_DIR_OWNER" -g "$RUNTIME_DIR_GROUP" -m 0750 \
        "$INSTALL_DIR/bin" "$INSTALL_DIR/lib" || return 1
    "$INSTALL_BIN" -o "$INSTALL_OWNER" -g "$INSTALL_GROUP" -m 0755 \
        "$STAGE/bin/debot" "$INSTALL_DIR/bin/debot" || return 1
    "$INSTALL_BIN" -o "$INSTALL_OWNER" -g "$INSTALL_GROUP" -m 0644 \
        "$STAGE/lib/libsigner.so" "$INSTALL_DIR/lib/libsigner.so" || return 1
    "$INSTALL_BIN" -o "$INSTALL_OWNER" -g "$INSTALL_GROUP" -m 0644 \
        "$STAGE/checksums.sha256" "$INSTALL_DIR/checksums.sha256" || return 1
    "$INSTALL_BIN" -o "$INSTALL_OWNER" -g "$INSTALL_GROUP" -m 0644 \
        "$STAGE/manifest.json" "$INSTALL_DIR/manifest.json" || return 1
    "$INSTALL_BIN" -d -o "$INSTALL_OWNER" -g "$INSTALL_GROUP" -m 0755 \
        "$INSTALL_DIR/robinhood-sidecar-bundle" || return 1
    "$INSTALL_BIN" -o "$INSTALL_OWNER" -g "$INSTALL_GROUP" -m 0755 \
        "$STAGE/robinhood-sidecar-bundle/lighter-ratelimit" \
        "$INSTALL_DIR/robinhood-sidecar-bundle/lighter-ratelimit" || return 1
    "$INSTALL_BIN" -o "$INSTALL_OWNER" -g "$INSTALL_GROUP" -m 0644 \
        "$STAGE/robinhood-sidecar-bundle/lighter-ratelimit.sha256" \
        "$STAGE/robinhood-sidecar-bundle/manifest.json" \
        "$STAGE/robinhood-sidecar-bundle/lighter-ratelimit.service" \
        "$INSTALL_DIR/robinhood-sidecar-bundle/" || return 1

    (cd "$INSTALL_DIR" && sha256sum -c checksums.sha256) || return 1
    test "$(stat -c %U:%G:%a "$INSTALL_DIR/bin/debot")" = \
        "$INSTALL_OWNER:$INSTALL_GROUP:755" || return 1
    test "$(stat -c %U:%G:%a "$INSTALL_DIR/lib/libsigner.so")" = \
        "$INSTALL_OWNER:$INSTALL_GROUP:644" || return 1
    test "$(stat -c %U:%G:%a "$INSTALL_DIR/bin")" = \
        "$RUNTIME_DIR_OWNER:$RUNTIME_DIR_GROUP:750" || return 1
    test "$(stat -c %U:%G:%a "$INSTALL_DIR/lib")" = \
        "$RUNTIME_DIR_OWNER:$RUNTIME_DIR_GROUP:750" || return 1
    (cd "$INSTALL_DIR/robinhood-sidecar-bundle" && \
        sha256sum -c lighter-ratelimit.sha256) || return 1
    test "$(stat -c %U:%G:%a "$INSTALL_DIR/robinhood-sidecar-bundle/lighter-ratelimit")" = \
        "$INSTALL_OWNER:$INSTALL_GROUP:755" || return 1
}

if ! commit_runtime; then
    echo "Robinhood runtime install failed; restoring the previous runtime before sidecar activation" >&2
    rollback_runtime
    exit 1
fi

echo "Robinhood pairtrade runtime and verified local sidecar bundle installed; activation is deferred to the coordinated bot start (debot=$EXPECTED_DEBOT_SHA libsigner=$EXPECTED_LIBSIGNER_SHA)"
