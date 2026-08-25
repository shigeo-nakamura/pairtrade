#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT=$(cd "$(dirname "$0")/.." && pwd)
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
SOURCE="$WORK/source"
SYSTEMD="$WORK/systemd"
LIBEXEC="$WORK/libexec"
PROVENANCE="$WORK/provenance"
mkdir -p "$SOURCE" "$SYSTEMD" "$LIBEXEC" "$PROVENANCE"
for file in \
  archive-arcus-live-tick-events.service \
  archive-arcus-live-tick-events.timer; do
  cp "$REPO_ROOT/deploy/$file" "$SOURCE/"
done
for file in \
  archive_arcus_live_tick_events.sh \
  arcus_live_tick_event_stream.py \
  install_arcus_live_tick_event_archive.sh; do
  cp "$REPO_ROOT/scripts/$file" "$SOURCE/"
done
printf '{"schema_version":1}\n' > "$SOURCE/manifest.json"
(cd "$SOURCE" && sha256sum \
  archive-arcus-live-tick-events.service \
  archive-arcus-live-tick-events.timer \
  archive_arcus_live_tick_events.sh \
  arcus_live_tick_event_stream.py \
  install_arcus_live_tick_event_archive.sh \
  manifest.json > release.sha256)

cat > "$WORK/systemctl" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >> "$SYSTEMCTL_LOG"
command=$1
shift
case "$command" in
  is-enabled)
    if [ -e "$FAKE_ENABLED" ]; then printf 'enabled\n'
    elif [ -e "$FAKE_SYSTEMD_DIR/$1" ]; then printf 'disabled\n'
    else printf 'not-found\n'; fi
    ;;
  is-active)
    if [ -e "$FAKE_ACTIVE" ]; then printf 'active\n'; else printf 'inactive\n'; fi
    ;;
  daemon-reload)
    if [ "${FAIL_DAEMON_RELOAD:-false}" = true ]; then exit 1; fi
    ;;
  enable)
    if [ "${FAIL_ENABLE:-false}" = true ] && [ "${1:-}" = --now ]; then exit 1; fi
    if [ "${1:-}" = --now ]; then shift; touch "$FAKE_ACTIVE"; fi
    touch "$FAKE_ENABLED"
    ;;
  disable)
    if [ "${1:-}" = --now ]; then shift; rm -f "$FAKE_ACTIVE"; fi
    rm -f "$FAKE_ENABLED"
    ;;
  start) touch "$FAKE_ACTIVE" ;;
  stop) rm -f "$FAKE_ACTIVE" ;;
  *) exit 99 ;;
esac
EOF
chmod +x "$WORK/systemctl"
: > "$WORK/systemctl.log"

TEST_OWNER=$(id -un)
TEST_GROUP=$(id -gn)
run_installer() {
  SYSTEMD_DIR="$SYSTEMD" \
  LIBEXEC_DIR="$LIBEXEC" \
  PROVENANCE_DIR="$PROVENANCE" \
  SYSTEMCTL="$WORK/systemctl" \
  SYSTEMCTL_LOG="$WORK/systemctl.log" \
  FAKE_SYSTEMD_DIR="$SYSTEMD" \
  FAKE_ENABLED="$WORK/enabled" \
  FAKE_ACTIVE="$WORK/active" \
  SYSTEMD_ANALYZE=/bin/true \
  INSTALL_OWNER="$TEST_OWNER" \
  INSTALL_GROUP="$TEST_GROUP" \
    bash "$REPO_ROOT/scripts/install_arcus_live_tick_event_archive.sh" "$SOURCE" "$1"
}

run_installer first
test -e "$WORK/enabled"
test -e "$WORK/active"
test -f "$PROVENANCE/releases/first/manifest.json"
test "$(stat -c %a "$LIBEXEC/archive_arcus_live_tick_events.sh")" = 755
test "$(stat -c %a "$LIBEXEC/arcus_live_tick_event_stream.py")" = 755
cmp "$SOURCE/archive-arcus-live-tick-events.timer" \
  "$SYSTEMD/archive-arcus-live-tick-events.timer"

cp "$SYSTEMD/archive-arcus-live-tick-events.service" "$WORK/prior.service"
cp "$LIBEXEC/archive_arcus_live_tick_events.sh" "$WORK/prior.archiver"
printf '# changed service\n' > "$SOURCE/archive-arcus-live-tick-events.service"
(cd "$SOURCE" && sha256sum \
  archive-arcus-live-tick-events.service \
  archive-arcus-live-tick-events.timer \
  archive_arcus_live_tick_events.sh \
  arcus_live_tick_event_stream.py \
  install_arcus_live_tick_event_archive.sh \
  manifest.json > release.sha256)
if FAIL_ENABLE=true \
   SYSTEMD_DIR="$SYSTEMD" \
   LIBEXEC_DIR="$LIBEXEC" \
   PROVENANCE_DIR="$PROVENANCE" \
   SYSTEMCTL="$WORK/systemctl" \
   SYSTEMCTL_LOG="$WORK/systemctl.log" \
   FAKE_SYSTEMD_DIR="$SYSTEMD" \
   FAKE_ENABLED="$WORK/enabled" \
   FAKE_ACTIVE="$WORK/active" \
   SYSTEMD_ANALYZE=/bin/true \
   INSTALL_OWNER="$TEST_OWNER" \
   INSTALL_GROUP="$TEST_GROUP" \
     bash "$REPO_ROOT/scripts/install_arcus_live_tick_event_archive.sh" "$SOURCE" failed; then
  echo "expected archive installer enable failure" >&2
  exit 1
fi
cmp "$WORK/prior.service" "$SYSTEMD/archive-arcus-live-tick-events.service"
cmp "$WORK/prior.archiver" "$LIBEXEC/archive_arcus_live_tick_events.sh"
test ! -e "$PROVENANCE/releases/failed"
test -e "$WORK/enabled"
test -e "$WORK/active"

echo "Arcus live-tick event archive installer tests passed"
