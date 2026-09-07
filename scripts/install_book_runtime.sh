#!/bin/bash
# Install/update one book runtime instance (bot-strategy#937) on a host.
# Never starts or restarts the service and never touches the host-only
# secrets file (/etc/book-runtime/<instance>-secrets.env) beyond creating
# its parent directory -- same discipline as install_engine_b_live.sh.
#
#   sudo env BOOK_INSTANCE=xsmom-695 BOOK_BINARY_SOURCE=/opt/debot/bin/book_runtime \
#        BOOK_LIBSIGNER_SOURCE=/opt/debot/deploy/engine-b-live-libsigner.so \
#        bash install_book_runtime.sh
set -euo pipefail

INSTANCE=${BOOK_INSTANCE:?BOOK_INSTANCE (e.g. xsmom-695) is required}
INSTALL_DIR=${BOOK_INSTALL_DIR:-/opt/book-runtime}
SECRETS_DIR=${BOOK_SECRETS_DIR:-/etc/book-runtime}
STATE_ROOT=${BOOK_STATE_ROOT:-/var/lib/book-runtime}
BINARY_SOURCE=${BOOK_BINARY_SOURCE:-/opt/debot/bin/book_runtime}
LIBSIGNER_SOURCE=${BOOK_LIBSIGNER_SOURCE:-/opt/debot/lib/libsigner.so}
CONFIG_SOURCE=${BOOK_CONFIG_SOURCE:-/opt/debot/configs/book/${INSTANCE}.yaml}
FETCH_SCRIPT_SOURCE=${BOOK_FETCH_SCRIPT_SOURCE:-/opt/debot/scripts/book_signal_fetch.sh}
UNIT_SOURCE_DIR=${BOOK_UNIT_SOURCE_DIR:-/opt/debot/deploy}
SERVICE_USER=book-runtime
SERVICE_GROUP=book-runtime

for source in "$BINARY_SOURCE" "$LIBSIGNER_SOURCE" "$CONFIG_SOURCE" "$FETCH_SCRIPT_SOURCE" \
              "$UNIT_SOURCE_DIR/book-runtime-${INSTANCE}.service" \
              "$UNIT_SOURCE_DIR/book-signal-fetch-${INSTANCE}.service" \
              "$UNIT_SOURCE_DIR/book-signal-fetch-${INSTANCE}.timer"; do
  if [ ! -f "$source" ]; then
    echo "book runtime source is missing: $source" >&2
    exit 1
  fi
done

if ! getent group "$SERVICE_GROUP" >/dev/null; then
  groupadd --system "$SERVICE_GROUP"
fi
if ! id -u "$SERVICE_USER" >/dev/null 2>&1; then
  useradd --system --gid "$SERVICE_GROUP" --home-dir /nonexistent \
    --shell /sbin/nologin --no-create-home "$SERVICE_USER"
fi

install -d -o root -g "$SERVICE_GROUP" -m 0750 "$INSTALL_DIR" "$INSTALL_DIR/bin" "$INSTALL_DIR/lib"
install -o root -g "$SERVICE_GROUP" -m 0550 "$BINARY_SOURCE" "$INSTALL_DIR/bin/book_runtime"
install -o root -g "$SERVICE_GROUP" -m 0440 "$LIBSIGNER_SOURCE" "$INSTALL_DIR/lib/libsigner.so"
install -o root -g "$SERVICE_GROUP" -m 0550 "$FETCH_SCRIPT_SOURCE" "$INSTALL_DIR/bin/book_signal_fetch.sh"

# The config must parse and fingerprint before it replaces the installed
# copy: validate a staged file first, then swap atomically, so a bad config
# never displaces the last-known-good one the next restart would load.
STAGED="$INSTALL_DIR/.${INSTANCE}.yaml.staged"
trap 'rm -f "$STAGED"' EXIT
install -o root -g "$SERVICE_GROUP" -m 0440 "$CONFIG_SOURCE" "$STAGED"
if ! LD_LIBRARY_PATH="$INSTALL_DIR/lib" "$INSTALL_DIR/bin/book_runtime" --config "$STAGED" --validate >/dev/null; then
  echo "book runtime config $CONFIG_SOURCE failed validation; installed config left untouched" >&2
  exit 1
fi
mv -f "$STAGED" "$INSTALL_DIR/${INSTANCE}.yaml"

install -d -o root -g "$SERVICE_GROUP" -m 0750 "$SECRETS_DIR"
install -d -o "$SERVICE_USER" -g "$SERVICE_GROUP" -m 0750 "$STATE_ROOT" "$STATE_ROOT/${INSTANCE}"

for unit in "book-runtime-${INSTANCE}.service" "book-signal-fetch-${INSTANCE}.service" "book-signal-fetch-${INSTANCE}.timer"; do
  install -o root -g root -m 0644 "$UNIT_SOURCE_DIR/$unit" "/etc/systemd/system/$unit"
done
systemctl daemon-reload

echo "book runtime ${INSTANCE} installed; nothing was started or restarted."
if [ ! -f "$SECRETS_DIR/${INSTANCE}-secrets.env" ]; then
  echo "NOTE: $SECRETS_DIR/${INSTANCE}-secrets.env does not exist yet -- the service cannot start until the operator provisions it (docs/book-runtime-operations.md)."
fi
