#!/usr/bin/env bash
# Install the Lighter rate-limit sidecar before provisioning the Robinhood bot.
set -euo pipefail

SERVICE=lighter-ratelimit.service
BOT_SERVICE=debot-pair-robinhood-lighter.service
AWS_BIN=${AWS_BIN:-aws}
FILE_BIN=${FILE_BIN:-file}
JQ_BIN=${JQ_BIN:-jq}
LDD_BIN=${LDD_BIN:-ldd}
SYSTEMCTL=${SYSTEMCTL:-systemctl}
SYSTEMD_ANALYZE=${SYSTEMD_ANALYZE:-systemd-analyze}
SYSTEMD_DIR=${SYSTEMD_DIR:-/etc/systemd/system}
SIDECAR_ROOT=${SIDECAR_ROOT:-/opt/lighter-ratelimit}
SOCKET_PATH=${SOCKET_PATH:-/run/lighter-ratelimit/lighter-ratelimit.sock}
INSTALL_OWNER=${INSTALL_OWNER:-root}
INSTALL_GROUP=${INSTALL_GROUP:-root}

if [ "$#" -ne 2 ]; then
  echo "usage: $0 S3_BUCKET PAIRTRADE_INSTALL_DIR" >&2
  exit 2
fi
S3_BUCKET=$1
PAIRTRADE_INSTALL_DIR=$2
PAIRTRADE_MANIFEST="$PAIRTRADE_INSTALL_DIR/manifest.json"

if { [ "$SYSTEMD_DIR" = /etc/systemd/system ] || [ "$SIDECAR_ROOT" = /opt/lighter-ratelimit ]; } &&
   [ "$(id -u)" -ne 0 ]; then
  echo "production sidecar bootstrap must run as root" >&2
  exit 2
fi
if [ ! -f "$PAIRTRADE_MANIFEST" ]; then
  echo "pairtrade artifact manifest is missing: $PAIRTRADE_MANIFEST" >&2
  exit 2
fi

EXPECTED_SOURCE_SHA=$("$JQ_BIN" -er '
  .dex_connector_sha
  | select(type == "string" and test("^[0-9a-f]{40}$"))
' "$PAIRTRADE_MANIFEST")

WORK=$(mktemp -d)
cleanup() {
  rm -rf -- "$WORK"
}
trap cleanup EXIT

BINARY="$WORK/lighter-ratelimit"
CHECKSUM="$WORK/lighter-ratelimit.sha256"
MANIFEST="$WORK/manifest.json"
UNIT="$WORK/$SERVICE"

"$AWS_BIN" s3 cp "s3://$S3_BUCKET/lighter-ratelimit/lighter-ratelimit" "$BINARY"
"$AWS_BIN" s3 cp "s3://$S3_BUCKET/lighter-ratelimit/lighter-ratelimit.sha256" "$CHECKSUM"
"$AWS_BIN" s3 cp "s3://$S3_BUCKET/lighter-ratelimit/manifest.json" "$MANIFEST"
"$AWS_BIN" s3 cp "s3://$S3_BUCKET/deploy/lighter-ratelimit.service" "$UNIT"

"$JQ_BIN" -e --arg source_sha "$EXPECTED_SOURCE_SHA" '
  .schema_version == 1 and
  .artifact == "lighter-ratelimit" and
  .architecture == "aarch64" and
  .source_sha == $source_sha and
  (.deployment_source_sha | type == "string" and test("^[0-9a-f]{40}$")) and
  (.binary_sha256 | type == "string" and test("^[0-9a-f]{64}$")) and
  .mode == "0755" and
  (.unit_sha256 | type == "string" and test("^[0-9a-f]{64}$")) and
  .unit_mode == "0644"
' "$MANIFEST" >/dev/null

BINARY_SHA256=$("$JQ_BIN" -r .binary_sha256 "$MANIFEST")
UNIT_SHA256=$("$JQ_BIN" -r .unit_sha256 "$MANIFEST")
CHECKSUM_NAME=$(awk 'NR == 1 { sub(/^\*/, "", $2); print $2 } END { if (NR != 1) exit 1 }' "$CHECKSUM")
if [ "$CHECKSUM_NAME" != lighter-ratelimit ]; then
  echo "unexpected sidecar checksum entry: $CHECKSUM_NAME" >&2
  exit 1
fi
echo "$BINARY_SHA256  $BINARY" | sha256sum -c -
echo "$UNIT_SHA256  $UNIT" | sha256sum -c -

FILE_OUTPUT=$("$FILE_BIN" "$BINARY")
if [[ "$FILE_OUTPUT" != *aarch64* ]]; then
  echo "sidecar artifact is not aarch64: $FILE_OUTPUT" >&2
  exit 1
fi
LDD_OUTPUT=$("$LDD_BIN" "$BINARY" 2>&1)
if [[ "$LDD_OUTPUT" == *"not found"* ]]; then
  echo "sidecar artifact has unresolved dynamic libraries" >&2
  echo "$LDD_OUTPUT" >&2
  exit 1
fi
UNIT_BEFORE=$(awk -F= '$1 == "Before" { print substr($0, index($0, "=") + 1) }' "$UNIT")
if [[ " $UNIT_BEFORE " != *" $BOT_SERVICE "* ]]; then
  echo "$SERVICE does not order itself before $BOT_SERVICE" >&2
  exit 1
fi

TARGET_BINARY="$SIDECAR_ROOT/bin/lighter-ratelimit"
TARGET_MANIFEST="$SIDECAR_ROOT/manifest.json"
TARGET_UNIT="$SYSTEMD_DIR/$SERVICE"
install -d -o "$INSTALL_OWNER" -g "$INSTALL_GROUP" -m 0755 "$SIDECAR_ROOT/bin" "$SYSTEMD_DIR"

binary_changed=true
unit_changed=true
if [ -f "$TARGET_BINARY" ] && cmp -s "$BINARY" "$TARGET_BINARY"; then
  binary_changed=false
fi
if [ -f "$TARGET_UNIT" ] && cmp -s "$UNIT" "$TARGET_UNIT"; then
  unit_changed=false
fi

install -o "$INSTALL_OWNER" -g "$INSTALL_GROUP" -m 0755 "$BINARY" "$TARGET_BINARY"
install -o "$INSTALL_OWNER" -g "$INSTALL_GROUP" -m 0644 "$MANIFEST" "$TARGET_MANIFEST"
install -o "$INSTALL_OWNER" -g "$INSTALL_GROUP" -m 0644 "$UNIT" "$TARGET_UNIT"
"$SYSTEMD_ANALYZE" verify "$TARGET_UNIT"
test "$(stat -c %U:%G:%a "$TARGET_BINARY")" = "$INSTALL_OWNER:$INSTALL_GROUP:755"
test "$(stat -c %U:%G:%a "$TARGET_MANIFEST")" = "$INSTALL_OWNER:$INSTALL_GROUP:644"
test "$(stat -c %U:%G:%a "$TARGET_UNIT")" = "$INSTALL_OWNER:$INSTALL_GROUP:644"
echo "$BINARY_SHA256  $TARGET_BINARY" | sha256sum -c -
echo "$UNIT_SHA256  $TARGET_UNIT" | sha256sum -c -

if [ "$unit_changed" = true ]; then
  "$SYSTEMCTL" daemon-reload
fi
"$SYSTEMCTL" enable "$SERVICE"
if "$SYSTEMCTL" is-active --quiet "$SERVICE"; then
  if [ "$binary_changed" = true ] || [ "$unit_changed" = true ]; then
    "$SYSTEMCTL" restart "$SERVICE"
  fi
else
  "$SYSTEMCTL" start "$SERVICE"
fi

for _ in {1..10}; do
  if "$SYSTEMCTL" is-active --quiet "$SERVICE" && [ -S "$SOCKET_PATH" ]; then
    break
  fi
  sleep 1
done
"$SYSTEMCTL" is-active --quiet "$SERVICE"
"$SYSTEMCTL" is-enabled --quiet "$SERVICE"
test -S "$SOCKET_PATH"
LOADED_BEFORE=$("$SYSTEMCTL" show --property=Before --value "$SERVICE")
if [[ " $LOADED_BEFORE " != *" $BOT_SERVICE "* ]]; then
  echo "loaded $SERVICE ordering omits $BOT_SERVICE" >&2
  exit 1
fi

echo "Robinhood sidecar ready (binary_changed=$binary_changed, unit_changed=$unit_changed, source=$EXPECTED_SOURCE_SHA)"
