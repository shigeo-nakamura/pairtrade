#!/usr/bin/env bash
# Install the Arcus live-tick unit definitions without changing timer/service
# enablement or runtime state. The caller is responsible for placing a verified
# release bundle in SOURCE_DIR before invoking this script.
set -euo pipefail

SERVICE=arcus-spot-live-tick.service
TIMER=arcus-spot-live-tick.timer
SYSTEMD_DIR=${SYSTEMD_DIR:-/etc/systemd/system}
PROVENANCE_DIR=${PROVENANCE_DIR:-/usr/local/share/arcus-spot-live-tick}
SYSTEMCTL=${SYSTEMCTL:-systemctl}
SYSTEMD_ANALYZE=${SYSTEMD_ANALYZE:-systemd-analyze}
INSTALL_OWNER=${INSTALL_OWNER:-root}
INSTALL_GROUP=${INSTALL_GROUP:-root}

if [ "$#" -ne 2 ]; then
  echo "usage: $0 SOURCE_DIR RELEASE_ID" >&2
  exit 2
fi

SOURCE_DIR=$1
RELEASE_ID=$2
if [[ ! "$RELEASE_ID" =~ ^[A-Za-z0-9._-]+$ ]]; then
  echo "release id contains unsupported characters" >&2
  exit 2
fi
if [ ! -d "$SOURCE_DIR" ]; then
  echo "release source directory does not exist: $SOURCE_DIR" >&2
  exit 2
fi
if [ "$SYSTEMD_DIR" = /etc/systemd/system ] && [ "$(id -u)" -ne 0 ]; then
  echo "production unit installation must run as root" >&2
  exit 2
fi

required=(
  "$SERVICE"
  "$TIMER"
  install_arcus_spot_live_tick_units.sh
  manifest.json
  release.sha256
)
for file in "${required[@]}"; do
  if [ ! -f "$SOURCE_DIR/$file" ] || [ -L "$SOURCE_DIR/$file" ]; then
    echo "missing or non-regular release artifact: $file" >&2
    exit 2
  fi
done

# release.sha256 is authenticated by the deploy workflow before this script is
# called. Restrict its contents to this fixed bundle before using `sha256sum -c`
# so an unexpected checksum entry cannot escape SOURCE_DIR.
mapfile -t checksum_names < <(awk '{print $2}' "$SOURCE_DIR/release.sha256" | sed 's/^\*//')
expected_names=(
  "$SERVICE"
  "$TIMER"
  install_arcus_spot_live_tick_units.sh
  manifest.json
)
if [ "${#checksum_names[@]}" -ne "${#expected_names[@]}" ]; then
  echo "release checksum manifest has an unexpected entry count" >&2
  exit 2
fi
for index in "${!expected_names[@]}"; do
  if [ "${checksum_names[$index]}" != "${expected_names[$index]}" ]; then
    echo "release checksum manifest has an unexpected entry" >&2
    exit 2
  fi
done
(cd "$SOURCE_DIR" && sha256sum -c release.sha256)

"$SYSTEMD_ANALYZE" verify "$SOURCE_DIR/$SERVICE" "$SOURCE_DIR/$TIMER"

install -d -o "$INSTALL_OWNER" -g "$INSTALL_GROUP" -m 0755 "$SYSTEMD_DIR" "$PROVENANCE_DIR/releases"
release_dir="$PROVENANCE_DIR/releases/$RELEASE_ID"
release_stage="$PROVENANCE_DIR/releases/.$RELEASE_ID.new"
if [ -e "$release_dir" ] || [ -e "$release_stage" ]; then
  echo "release already exists: $RELEASE_ID" >&2
  exit 1
fi
install -d -o "$INSTALL_OWNER" -g "$INSTALL_GROUP" -m 0755 "$release_stage"
for file in "${required[@]}"; do
  mode=0644
  if [ "$file" = install_arcus_spot_live_tick_units.sh ]; then
    mode=0755
  fi
  install -o "$INSTALL_OWNER" -g "$INSTALL_GROUP" -m "$mode" "$SOURCE_DIR/$file" "$release_stage/$file"
done
mv "$release_stage" "$release_dir"

normalize_enabled_state() {
  # Before a first install, systemd reports an absent unit as `not-found`.
  # Installing its definitions without enabling it correctly changes that raw
  # answer to `disabled`; both mean the timer will not start at boot. Compare
  # that semantic state while preserving every other is-enabled distinction.
  case "$1" in
    not-found) printf '%s\n' disabled ;;
    *) printf '%s\n' "$1" ;;
  esac
}

active_before=$("$SYSTEMCTL" is-active "$TIMER" 2>&1 || true)
enabled_before_raw=$("$SYSTEMCTL" is-enabled "$TIMER" 2>&1 || true)
enabled_before=$(normalize_enabled_state "$enabled_before_raw")

service_stage="$SYSTEMD_DIR/.$SERVICE.$$.new"
timer_stage="$SYSTEMD_DIR/.$TIMER.$$.new"
service_backup="$SYSTEMD_DIR/.$SERVICE.$$.backup"
timer_backup="$SYSTEMD_DIR/.$TIMER.$$.backup"
service_existed=false
timer_existed=false
definitions_changed=false
rollback_needed=false

cleanup() {
  rm -f "$service_stage" "$timer_stage" "$service_backup" "$timer_backup"
}

rollback() {
  set +e
  if [ "$service_existed" = true ]; then
    mv -f "$service_backup" "$SYSTEMD_DIR/$SERVICE"
  else
    rm -f "$SYSTEMD_DIR/$SERVICE"
  fi
  if [ "$timer_existed" = true ]; then
    mv -f "$timer_backup" "$SYSTEMD_DIR/$TIMER"
  else
    rm -f "$SYSTEMD_DIR/$TIMER"
  fi
  "$SYSTEMCTL" daemon-reload
  rm -rf "$release_dir"
  cleanup
}

on_exit() {
  rc=$?
  trap - EXIT
  if [ "$rc" -ne 0 ] && [ "$rollback_needed" = true ]; then
    rollback
  else
    cleanup
    if [ "$rc" -ne 0 ]; then
      rm -rf "$release_dir"
    fi
  fi
  exit "$rc"
}
trap on_exit EXIT

install -o "$INSTALL_OWNER" -g "$INSTALL_GROUP" -m 0644 "$SOURCE_DIR/$SERVICE" "$service_stage"
install -o "$INSTALL_OWNER" -g "$INSTALL_GROUP" -m 0644 "$SOURCE_DIR/$TIMER" "$timer_stage"

if [ -f "$SYSTEMD_DIR/$SERVICE" ] && cmp -s "$service_stage" "$SYSTEMD_DIR/$SERVICE" &&
   [ -f "$SYSTEMD_DIR/$TIMER" ] && cmp -s "$timer_stage" "$SYSTEMD_DIR/$TIMER"; then
  echo "Arcus live-tick unit definitions already match release $RELEASE_ID"
else
  definitions_changed=true
  if [ -e "$SYSTEMD_DIR/$SERVICE" ]; then
    cp -a "$SYSTEMD_DIR/$SERVICE" "$service_backup"
    service_existed=true
  fi
  if [ -e "$SYSTEMD_DIR/$TIMER" ]; then
    cp -a "$SYSTEMD_DIR/$TIMER" "$timer_backup"
    timer_existed=true
  fi
  rollback_needed=true
  mv -f "$service_stage" "$SYSTEMD_DIR/$SERVICE"
  mv -f "$timer_stage" "$SYSTEMD_DIR/$TIMER"
  "$SYSTEMCTL" daemon-reload
  "$SYSTEMD_ANALYZE" verify "$SYSTEMD_DIR/$SERVICE" "$SYSTEMD_DIR/$TIMER"
fi

test "$(stat -c %U:%G:%a "$SYSTEMD_DIR/$SERVICE")" = "$INSTALL_OWNER:$INSTALL_GROUP:644"
test "$(stat -c %U:%G:%a "$SYSTEMD_DIR/$TIMER")" = "$INSTALL_OWNER:$INSTALL_GROUP:644"
cmp -s "$SOURCE_DIR/$SERVICE" "$SYSTEMD_DIR/$SERVICE"
cmp -s "$SOURCE_DIR/$TIMER" "$SYSTEMD_DIR/$TIMER"

active_after=$("$SYSTEMCTL" is-active "$TIMER" 2>&1 || true)
enabled_after_raw=$("$SYSTEMCTL" is-enabled "$TIMER" 2>&1 || true)
enabled_after=$(normalize_enabled_state "$enabled_after_raw")
if [ "$active_after" != "$active_before" ] || [ "$enabled_after" != "$enabled_before" ]; then
  echo "timer lifecycle state changed unexpectedly (active: $active_before -> $active_after; enabled: $enabled_before_raw -> $enabled_after_raw)" >&2
  exit 1
fi

rollback_needed=false
echo "Installed Arcus live-tick lifecycle release $RELEASE_ID (definitions_changed=$definitions_changed, active=$active_after, enabled=$enabled_after)"
