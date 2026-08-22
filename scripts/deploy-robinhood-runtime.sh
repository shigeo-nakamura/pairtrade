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
RUNTIME_DIR_OWNER=${RUNTIME_DIR_OWNER:-root}
RUNTIME_DIR_GROUP=${RUNTIME_DIR_GROUP:-root}
INSTALL_BIN=${INSTALL_BIN:-install}
MV_BIN=${MV_BIN:-mv}
RELEASES_DIR=${ROBINHOOD_RELEASES_DIR:-$INSTALL_DIR/robinhood-releases}
CURRENT_LINK=${ROBINHOOD_RUNTIME_CURRENT:-$INSTALL_DIR/robinhood-runtime-current}

if [[ ! "$EXPECTED_DEBOT_SHA" =~ ^[0-9a-f]{64}$ ]] ||
   [[ ! "$EXPECTED_LIBSIGNER_SHA" =~ ^[0-9a-f]{64}$ ]]; then
    echo "expected artifact SHA-256 values must each contain exactly 64 lowercase hex characters" >&2
    exit 2
fi

WORK=$(mktemp -d)
PUBLISH_DIR=
NEXT_LINK=
cleanup() {
    if [ -n "$PUBLISH_DIR" ] && [ -d "$PUBLISH_DIR" ]; then
        rm -rf -- "$PUBLISH_DIR"
    fi
    if [ -n "$NEXT_LINK" ] && [ -L "$NEXT_LINK" ]; then
        rm -f -- "$NEXT_LINK"
    fi
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

RUNTIME_MANIFEST_SHA=$(sha256sum "$STAGE/manifest.json" | awk '{print $1}')
BUNDLE_FINGERPRINT=$(
    cd "$STAGE/robinhood-sidecar-bundle"
    sha256sum lighter-ratelimit lighter-ratelimit.sha256 manifest.json \
        lighter-ratelimit.service | sha256sum | awk '{print $1}'
)
RELEASE_ID=$(
    printf '%s\n%s\n%s\n' "$EXPECTED_DEBOT_SHA" \
        "$RUNTIME_MANIFEST_SHA" "$BUNDLE_FINGERPRINT" |
        sha256sum | awk '{print $1}'
)
RELEASE_DIR="$RELEASES_DIR/$RELEASE_ID"

verify_release() {
    local release_dir=$1
    cmp "$STAGE/bin/debot" "$release_dir/bin/debot"
    cmp "$STAGE/lib/libsigner.so" "$release_dir/lib/libsigner.so"
    cmp "$STAGE/checksums.sha256" "$release_dir/checksums.sha256"
    cmp "$STAGE/manifest.json" "$release_dir/manifest.json"
    cmp "$STAGE/robinhood-sidecar-bundle/lighter-ratelimit" \
        "$release_dir/robinhood-sidecar-bundle/lighter-ratelimit"
    cmp "$STAGE/robinhood-sidecar-bundle/lighter-ratelimit.sha256" \
        "$release_dir/robinhood-sidecar-bundle/lighter-ratelimit.sha256"
    cmp "$STAGE/robinhood-sidecar-bundle/manifest.json" \
        "$release_dir/robinhood-sidecar-bundle/manifest.json"
    cmp "$STAGE/robinhood-sidecar-bundle/lighter-ratelimit.service" \
        "$release_dir/robinhood-sidecar-bundle/lighter-ratelimit.service"
    (cd "$release_dir" && sha256sum -c checksums.sha256)
    (cd "$release_dir/robinhood-sidecar-bundle" && \
        sha256sum -c lighter-ratelimit.sha256)
    test "$(stat -c %U:%G:%a "$release_dir")" = \
        "$INSTALL_OWNER:$INSTALL_GROUP:755"
    test "$(stat -c %U:%G:%a "$release_dir/bin")" = \
        "$RUNTIME_DIR_OWNER:$RUNTIME_DIR_GROUP:755"
    test "$(stat -c %U:%G:%a "$release_dir/lib")" = \
        "$RUNTIME_DIR_OWNER:$RUNTIME_DIR_GROUP:755"
    test "$(stat -c %U:%G:%a "$release_dir/bin/debot")" = \
        "$INSTALL_OWNER:$INSTALL_GROUP:755"
    test "$(stat -c %U:%G:%a "$release_dir/lib/libsigner.so")" = \
        "$INSTALL_OWNER:$INSTALL_GROUP:644"
    test "$(stat -c %U:%G:%a \
        "$release_dir/robinhood-sidecar-bundle/lighter-ratelimit")" = \
        "$INSTALL_OWNER:$INSTALL_GROUP:755"
}

publish_release() {
    "$INSTALL_BIN" -d -o "$INSTALL_OWNER" -g "$INSTALL_GROUP" -m 0755 \
        "$RELEASES_DIR" || return 1

    if [ -d "$RELEASE_DIR" ]; then
        verify_release "$RELEASE_DIR"
        return
    fi

    PUBLISH_DIR=$(mktemp -d "$RELEASES_DIR/.release.XXXXXX") || return 1
    "$INSTALL_BIN" -d -o "$INSTALL_OWNER" -g "$INSTALL_GROUP" -m 0755 \
        "$PUBLISH_DIR" || return 1
    "$INSTALL_BIN" -d -o "$RUNTIME_DIR_OWNER" -g "$RUNTIME_DIR_GROUP" -m 0755 \
        "$PUBLISH_DIR/bin" "$PUBLISH_DIR/lib" || return 1
    "$INSTALL_BIN" -d -o "$INSTALL_OWNER" -g "$INSTALL_GROUP" -m 0755 \
        "$PUBLISH_DIR/robinhood-sidecar-bundle" || return 1
    "$INSTALL_BIN" -o "$INSTALL_OWNER" -g "$INSTALL_GROUP" -m 0755 \
        "$STAGE/bin/debot" "$PUBLISH_DIR/bin/debot" || return 1
    "$INSTALL_BIN" -o "$INSTALL_OWNER" -g "$INSTALL_GROUP" -m 0644 \
        "$STAGE/lib/libsigner.so" "$PUBLISH_DIR/lib/libsigner.so" || return 1
    "$INSTALL_BIN" -o "$INSTALL_OWNER" -g "$INSTALL_GROUP" -m 0644 \
        "$STAGE/checksums.sha256" "$STAGE/manifest.json" "$PUBLISH_DIR/" || return 1
    "$INSTALL_BIN" -o "$INSTALL_OWNER" -g "$INSTALL_GROUP" -m 0755 \
        "$STAGE/robinhood-sidecar-bundle/lighter-ratelimit" \
        "$PUBLISH_DIR/robinhood-sidecar-bundle/lighter-ratelimit" || return 1
    "$INSTALL_BIN" -o "$INSTALL_OWNER" -g "$INSTALL_GROUP" -m 0644 \
        "$STAGE/robinhood-sidecar-bundle/lighter-ratelimit.sha256" \
        "$STAGE/robinhood-sidecar-bundle/manifest.json" \
        "$STAGE/robinhood-sidecar-bundle/lighter-ratelimit.service" \
        "$PUBLISH_DIR/robinhood-sidecar-bundle/" || return 1

    verify_release "$PUBLISH_DIR" || return 1
    if ! "$MV_BIN" -T "$PUBLISH_DIR" "$RELEASE_DIR"; then
        [ -d "$RELEASE_DIR" ] && verify_release "$RELEASE_DIR" || return 1
        return
    fi
    PUBLISH_DIR=
}

if ! publish_release; then
    echo "Robinhood release publication failed; current runtime pointer was not changed" >&2
    exit 1
fi

# Publish the complete immutable runtime+bundle with one same-filesystem rename.
NEXT_LINK="$INSTALL_DIR/.robinhood-runtime-current.$$"
ln -s "robinhood-releases/$RELEASE_ID" "$NEXT_LINK"
if ! "$MV_BIN" -Tf "$NEXT_LINK" "$CURRENT_LINK"; then
    echo "Robinhood runtime pointer switch failed; previous release remains current" >&2
    exit 1
fi
NEXT_LINK=

test "$(readlink -f "$CURRENT_LINK")" = "$(readlink -f "$RELEASE_DIR")"
verify_release "$RELEASE_DIR"
echo "Robinhood runtime release published atomically; activation is deferred to the systemd dependency (release=$RELEASE_ID debot=$EXPECTED_DEBOT_SHA libsigner=$EXPECTED_LIBSIGNER_SHA)"
