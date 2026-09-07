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
LOCK_FILE=${BOOK_INSTALL_LOCK:-/var/lock/book-runtime-install.lock}

# ci.yml (binary) and deploy-configs.yml (config/units) both run this
# script on the same host, and a push touching src/** and configs/** fires
# both workflows at once. A shared GitHub concurrency group is the wrong
# tool -- queuing a third job there cancels the pending one, which would
# silently drop a binary deploy -- so the mutual exclusion lives here: each
# run holds an exclusive lock for the whole stage-validate-promote
# sequence, and both contenders install a complete, self-consistent bundle
# from whatever is currently staged under /opt/debot.
install -d -m 0755 "$(dirname "$LOCK_FILE")"
exec 9>"$LOCK_FILE"
if ! flock -w 600 9; then
  echo "another book runtime install is holding $LOCK_FILE; giving up after 600s" >&2
  exit 1
fi

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

# Stage the whole runtime bundle (binary, signer library, config, fetch
# script) and validate the config WITH the staged binary before anything
# installed is touched, so a deploy whose config does not parse -- or whose
# binary rejects it -- leaves the previous bundle intact and consistent.
STAGE=$(mktemp -d "$INSTALL_DIR/.stage.XXXXXX")
trap 'rm -rf "$STAGE"' EXIT
install -d -m 0750 "$STAGE/bin" "$STAGE/lib"
install -o root -g "$SERVICE_GROUP" -m 0550 "$BINARY_SOURCE" "$STAGE/bin/book_runtime"
install -o root -g "$SERVICE_GROUP" -m 0440 "$LIBSIGNER_SOURCE" "$STAGE/lib/libsigner.so"
install -o root -g "$SERVICE_GROUP" -m 0440 "$CONFIG_SOURCE" "$STAGE/${INSTANCE}.yaml"
install -o root -g "$SERVICE_GROUP" -m 0550 "$FETCH_SCRIPT_SOURCE" "$STAGE/bin/book_signal_fetch.sh"
if ! LD_LIBRARY_PATH="$STAGE/lib" "$STAGE/bin/book_runtime" --config "$STAGE/${INSTANCE}.yaml" --validate >/dev/null; then
  echo "book runtime bundle failed validation ($CONFIG_SOURCE with $BINARY_SOURCE); installed bundle left untouched" >&2
  exit 1
fi
mv -f "$STAGE/bin/book_runtime" "$INSTALL_DIR/bin/book_runtime"
mv -f "$STAGE/lib/libsigner.so" "$INSTALL_DIR/lib/libsigner.so"
mv -f "$STAGE/bin/book_signal_fetch.sh" "$INSTALL_DIR/bin/book_signal_fetch.sh"
mv -f "$STAGE/${INSTANCE}.yaml" "$INSTALL_DIR/${INSTANCE}.yaml"

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
