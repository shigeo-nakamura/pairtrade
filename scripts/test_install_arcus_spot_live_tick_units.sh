#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT=$(cd "$(dirname "$0")/.." && pwd)
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

SOURCE="$WORK/source"
SYSTEMD="$WORK/systemd"
PROVENANCE="$WORK/provenance"
mkdir -p "$SOURCE" "$SYSTEMD" "$PROVENANCE"
cp "$REPO_ROOT/deploy/arcus-spot-live-tick.service" "$SOURCE/"
cp "$REPO_ROOT/deploy/arcus-spot-live-tick.timer" "$SOURCE/"
cp "$REPO_ROOT/scripts/install_arcus_spot_live_tick_units.sh" "$SOURCE/"
printf '{"schema_version":1}\n' > "$SOURCE/manifest.json"
(cd "$SOURCE" && sha256sum \
  arcus-spot-live-tick.service \
  arcus-spot-live-tick.timer \
  install_arcus_spot_live_tick_units.sh \
  manifest.json > release.sha256)

cat > "$WORK/systemctl" <<'EOF'
#!/usr/bin/env bash
set -eu
printf '%s\n' "$*" >> "$SYSTEMCTL_LOG"
case "$1" in
  is-active) printf '%s\n' inactive ;;
  is-enabled)
    if [ -e "$FAKE_SYSTEMD_DIR/$2" ]; then
      printf '%s\n' disabled
    else
      printf '%s\n' not-found
    fi
    ;;
  daemon-reload)
    if [ "${FAIL_DAEMON_RELOAD:-false}" = true ]; then
      exit 1
    fi
    ;;
  *) exit 99 ;;
esac
EOF
chmod +x "$WORK/systemctl"
: > "$WORK/systemctl.log"

TEST_OWNER=$(id -un)
TEST_GROUP=$(id -gn)
run_installer() {
  SYSTEMD_DIR="$SYSTEMD" \
  PROVENANCE_DIR="$PROVENANCE" \
  SYSTEMCTL="$WORK/systemctl" \
  SYSTEMCTL_LOG="$WORK/systemctl.log" \
  FAKE_SYSTEMD_DIR="$SYSTEMD" \
  SYSTEMD_ANALYZE=/bin/true \
  INSTALL_OWNER="$TEST_OWNER" \
  INSTALL_GROUP="$TEST_GROUP" \
    bash "$REPO_ROOT/scripts/install_arcus_spot_live_tick_units.sh" "$SOURCE" "$1"
}

# Regression: a fresh host reports not-found before the timer definition exists
# and disabled afterward. That semantic disabled state must count as preserved.
run_installer first
cmp "$SOURCE/arcus-spot-live-tick.service" "$SYSTEMD/arcus-spot-live-tick.service"
cmp "$SOURCE/arcus-spot-live-tick.timer" "$SYSTEMD/arcus-spot-live-tick.timer"
test "$(stat -c %a "$SYSTEMD/arcus-spot-live-tick.service")" = 644
test "$(stat -c %a "$SYSTEMD/arcus-spot-live-tick.timer")" = 644
test -f "$PROVENANCE/releases/first/manifest.json"
test "$(grep -c '^daemon-reload$' "$WORK/systemctl.log")" -eq 1

# Reinstalling identical definitions records provenance without reloading or
# changing the timer lifecycle.
run_installer second
test -f "$PROVENANCE/releases/second/manifest.json"
test "$(grep -c '^daemon-reload$' "$WORK/systemctl.log")" -eq 1
if grep -Eq '^(start|restart|stop|enable|disable) ' "$WORK/systemctl.log"; then
  echo "installer attempted to mutate lifecycle state" >&2
  exit 1
fi

# A failed daemon-reload restores both prior definitions and removes the
# incomplete provenance release.
printf '# local prior service\n' > "$SYSTEMD/arcus-spot-live-tick.service"
printf '# local prior timer\n' > "$SYSTEMD/arcus-spot-live-tick.timer"
cp "$SYSTEMD/arcus-spot-live-tick.service" "$WORK/prior.service"
cp "$SYSTEMD/arcus-spot-live-tick.timer" "$WORK/prior.timer"
if SYSTEMD_DIR="$SYSTEMD" \
   PROVENANCE_DIR="$PROVENANCE" \
   SYSTEMCTL="$WORK/systemctl" \
   SYSTEMCTL_LOG="$WORK/systemctl.log" \
   FAKE_SYSTEMD_DIR="$SYSTEMD" \
   SYSTEMD_ANALYZE=/bin/true \
   INSTALL_OWNER="$TEST_OWNER" \
   INSTALL_GROUP="$TEST_GROUP" \
   FAIL_DAEMON_RELOAD=true \
     bash "$REPO_ROOT/scripts/install_arcus_spot_live_tick_units.sh" "$SOURCE" failed; then
  echo "expected daemon-reload failure" >&2
  exit 1
fi
cmp "$WORK/prior.service" "$SYSTEMD/arcus-spot-live-tick.service"
cmp "$WORK/prior.timer" "$SYSTEMD/arcus-spot-live-tick.timer"
test ! -e "$PROVENANCE/releases/failed"

echo "Arcus live-tick lifecycle installer tests passed"
