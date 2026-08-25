#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 2 ]; then
  echo "usage: $0 SOURCE_DIR RELEASE_ID" >&2
  exit 64
fi
SOURCE_DIR=$1
RELEASE_ID=$2
SERVICE=archive-arcus-live-tick-events.service
TIMER=archive-arcus-live-tick-events.timer
ARCHIVER=archive_arcus_live_tick_events.sh
VERIFIER=arcus_live_tick_event_stream.py
INSTALLER=install_arcus_live_tick_event_archive.sh
SYSTEMD_DIR=${SYSTEMD_DIR:-/etc/systemd/system}
LIBEXEC_DIR=${LIBEXEC_DIR:-/usr/local/libexec/debot}
PROVENANCE_DIR=${PROVENANCE_DIR:-/usr/local/share/arcus-live-tick-event-archive}
SYSTEMCTL=${SYSTEMCTL:-systemctl}
SYSTEMD_ANALYZE=${SYSTEMD_ANALYZE:-systemd-analyze}
INSTALL_OWNER=${INSTALL_OWNER:-root}
INSTALL_GROUP=${INSTALL_GROUP:-root}

if [ "$INSTALL_OWNER" = root ] && [ "$(id -u)" -ne 0 ]; then
  echo "production archive installation must run as root" >&2
  exit 2
fi
if ! [[ "$RELEASE_ID" =~ ^[A-Za-z0-9._-]+$ ]]; then
  echo "invalid release id" >&2
  exit 2
fi

required=("$SERVICE" "$TIMER" "$ARCHIVER" "$VERIFIER" "$INSTALLER" manifest.json release.sha256)
for file in "${required[@]}"; do
  if [ ! -f "$SOURCE_DIR/$file" ] || [ -L "$SOURCE_DIR/$file" ]; then
    echo "missing or non-regular release artifact: $file" >&2
    exit 2
  fi
done
mapfile -t checksum_names < <(awk '{print $2}' "$SOURCE_DIR/release.sha256" | sed 's/^\*//')
expected_names=("$SERVICE" "$TIMER" "$ARCHIVER" "$VERIFIER" "$INSTALLER" manifest.json)
if [ "${checksum_names[*]}" != "${expected_names[*]}" ]; then
  echo "release checksum manifest has unexpected entries or order" >&2
  exit 2
fi
(cd "$SOURCE_DIR" && sha256sum -c release.sha256)
"$SYSTEMD_ANALYZE" verify "$SOURCE_DIR/$SERVICE" "$SOURCE_DIR/$TIMER"

enabled_before=$("$SYSTEMCTL" is-enabled "$TIMER" 2>&1 || true)
active_before=$("$SYSTEMCTL" is-active "$TIMER" 2>&1 || true)
case "$enabled_before" in enabled|disabled|not-found) ;; *)
  echo "unexpected timer enabled state: $enabled_before" >&2; exit 1 ;;
esac
case "$active_before" in active|inactive) ;; *)
  echo "unexpected timer active state: $active_before" >&2; exit 1 ;;
esac
if [ "$enabled_before" = not-found ] && [ "$active_before" != inactive ]; then
  echo "not-found archive timer unexpectedly active" >&2
  exit 1
fi

install -d -o "$INSTALL_OWNER" -g "$INSTALL_GROUP" -m 0755 \
  "$SYSTEMD_DIR" "$LIBEXEC_DIR" "$PROVENANCE_DIR/releases"
release_dir="$PROVENANCE_DIR/releases/$RELEASE_ID"
release_stage="$PROVENANCE_DIR/releases/.$RELEASE_ID.new"
if [ -e "$release_dir" ] || [ -e "$release_stage" ]; then
  echo "release already exists: $RELEASE_ID" >&2
  exit 1
fi
install -d -o "$INSTALL_OWNER" -g "$INSTALL_GROUP" -m 0755 "$release_stage"
for file in "${required[@]}"; do
  mode=0644
  case "$file" in
    "$ARCHIVER"|"$VERIFIER"|"$INSTALLER") mode=0755 ;;
  esac
  install -o "$INSTALL_OWNER" -g "$INSTALL_GROUP" -m "$mode" \
    "$SOURCE_DIR/$file" "$release_stage/$file"
done
mv "$release_stage" "$release_dir"

targets=(
  "$SYSTEMD_DIR/$SERVICE"
  "$SYSTEMD_DIR/$TIMER"
  "$LIBEXEC_DIR/$ARCHIVER"
  "$LIBEXEC_DIR/$VERIFIER"
)
sources=(
  "$SOURCE_DIR/$SERVICE"
  "$SOURCE_DIR/$TIMER"
  "$SOURCE_DIR/$ARCHIVER"
  "$SOURCE_DIR/$VERIFIER"
)
modes=(0644 0644 0755 0755)
BACKUP=$(mktemp -d)
rollback_needed=true

rollback() {
  set +e
  "$SYSTEMCTL" disable --now "$TIMER" >/dev/null 2>&1
  for index in "${!targets[@]}"; do
    target=${targets[$index]}
    rm -f "$target.$$.new"
    backup="$BACKUP/$index"
    if [ -e "$backup" ]; then
      install -o "$INSTALL_OWNER" -g "$INSTALL_GROUP" -m "${modes[$index]}" "$backup" "$target"
    else
      rm -f "$target"
    fi
  done
  "$SYSTEMCTL" daemon-reload >/dev/null 2>&1
  if [ "$enabled_before" = enabled ]; then
    "$SYSTEMCTL" enable "$TIMER" >/dev/null 2>&1
  fi
  if [ "$active_before" = active ]; then
    "$SYSTEMCTL" start "$TIMER" >/dev/null 2>&1
  fi
  rm -rf "$release_dir"
}
on_exit() {
  rc=$?
  trap - EXIT
  if [ "$rc" -ne 0 ] && [ "$rollback_needed" = true ]; then
    rollback
  fi
  rm -rf "$BACKUP"
  exit "$rc"
}
trap on_exit EXIT

for index in "${!targets[@]}"; do
  target=${targets[$index]}
  if [ -L "$target" ]; then
    echo "refusing to replace symlink target: $target" >&2
    exit 1
  fi
  if [ -e "$target" ]; then
    cp -a "$target" "$BACKUP/$index"
  fi
done
for index in "${!targets[@]}"; do
  target=${targets[$index]}
  stage="$target.$$.new"
  install -o "$INSTALL_OWNER" -g "$INSTALL_GROUP" -m "${modes[$index]}" \
    "${sources[$index]}" "$stage"
  mv -f "$stage" "$target"
done

"$SYSTEMCTL" daemon-reload
"$SYSTEMD_ANALYZE" verify "$SYSTEMD_DIR/$SERVICE" "$SYSTEMD_DIR/$TIMER"
"$SYSTEMCTL" enable --now "$TIMER"
test "$("$SYSTEMCTL" is-enabled "$TIMER")" = enabled
test "$("$SYSTEMCTL" is-active "$TIMER")" = active
for index in "${!targets[@]}"; do
  cmp -s "${sources[$index]}" "${targets[$index]}"
  test "$(stat -c %U:%G:%a "${targets[$index]}")" = \
    "$INSTALL_OWNER:$INSTALL_GROUP:${modes[$index]#0}"
done

rollback_needed=false
echo "Installed and enabled Arcus live-tick event archive release $RELEASE_ID"
