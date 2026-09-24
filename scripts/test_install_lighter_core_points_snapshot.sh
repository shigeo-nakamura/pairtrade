#!/usr/bin/env bash
# Tests for scripts/install_lighter_core_points_snapshot.sh (bot-strategy#938):
# the installer copies both units, creates the history dir, enables the timer
# through systemctl, never names the holder, and refuses when any source it
# depends on is missing. systemctl is faked; install(1) is real, so the
# owner is the test's own user.
set -euo pipefail

REPO_ROOT=$(cd "$(dirname "$0")/.." && pwd)
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
SOURCE="$WORK/deploy"; SYSTEMD="$WORK/systemd"; OUT="$WORK/status/lighter-core"
mkdir -p "$SOURCE" "$SYSTEMD" "$WORK/scripts"
cp "$REPO_ROOT/deploy/lighter-core-points-snapshot.service" \
   "$REPO_ROOT/deploy/lighter-core-points-snapshot.timer" "$SOURCE/"
cp "$REPO_ROOT/scripts/robinhood_points_collector.py" "$WORK/scripts/"
ARM_ENV="$WORK/scripts/debot-bull-holder.env"
printf 'export LIGHTER_ACCOUNT_INDEX="1"\n' > "$ARM_ENV"; chmod 0600 "$ARM_ENV"
LIBSIGNER="$WORK/scripts/libsigner.so"; : > "$LIBSIGNER"
cat > "$WORK/systemctl" <<'FAKE'
#!/usr/bin/env bash
printf '%s\n' "$*" >> "$SYSTEMCTL_LOG"
FAKE
chmod +x "$WORK/systemctl"
export SYSTEMCTL_LOG="$WORK/systemctl.log"
me=$(id -un)

run_installer() {
  LIGHTER_CORE_POINTS_UNIT_SOURCE_DIR="$SOURCE" LIGHTER_CORE_POINTS_SYSTEMD_DIR="$SYSTEMD" \
  LIGHTER_CORE_POINTS_COLLECTOR="$WORK/scripts/robinhood_points_collector.py" \
  LIGHTER_CORE_POINTS_OUT_DIR="$OUT" LIGHTER_CORE_POINTS_SYSTEMCTL="$WORK/systemctl" \
  LIGHTER_CORE_POINTS_ARM_ENV="$ARM_ENV" LIGHTER_CORE_POINTS_LIBSIGNER="$LIBSIGNER" \
  LIGHTER_CORE_POINTS_OWNER="$me" bash "$REPO_ROOT/scripts/install_lighter_core_points_snapshot.sh"
}

# 1. happy path
: > "$SYSTEMCTL_LOG"
run_installer >/dev/null
[ -f "$SYSTEMD/lighter-core-points-snapshot.service" ] || { echo "service not installed"; exit 1; }
[ -f "$SYSTEMD/lighter-core-points-snapshot.timer" ] || { echo "timer not installed"; exit 1; }
[ "$(stat -c %a "$SYSTEMD/lighter-core-points-snapshot.timer")" = 644 ] || { echo "timer mode"; exit 1; }
[ -d "$OUT" ] || { echo "out dir not created"; exit 1; }
[ "$(stat -c %a "$OUT")" = 750 ] || { echo "out dir mode"; exit 1; }
grep -qx 'daemon-reload' "$SYSTEMCTL_LOG" || { echo "no daemon-reload"; exit 1; }
grep -qx 'enable --now lighter-core-points-snapshot.timer' "$SYSTEMCTL_LOG" || { echo "timer not enabled"; exit 1; }
# The live holder is never named to systemctl, and its env is left as found.
if grep -q 'debot-bull-holder' "$SYSTEMCTL_LOG"; then echo "touched the holder"; exit 1; fi
[ "$(stat -c %a "$ARM_ENV")" = 600 ] || { echo "arm env permissions changed"; exit 1; }

# 2. re-run is idempotent
: > "$SYSTEMCTL_LOG"
run_installer >/dev/null
grep -qx 'enable --now lighter-core-points-snapshot.timer' "$SYSTEMCTL_LOG" || { echo "re-run"; exit 1; }

# 3. a missing unit refuses before touching systemd
mv "$SOURCE/lighter-core-points-snapshot.timer" "$WORK/timer.bak"
: > "$SYSTEMCTL_LOG"
if run_installer >/dev/null 2>"$WORK/err"; then echo "should refuse without the timer"; exit 1; fi
grep -q 'lighter-core-points-snapshot.timer' "$WORK/err" || { echo "error should name the unit"; exit 1; }
[ ! -s "$SYSTEMCTL_LOG" ] || { echo "systemctl called despite missing source"; exit 1; }
mv "$WORK/timer.bak" "$SOURCE/lighter-core-points-snapshot.timer"

# 4. the arm's own inputs are refusals, not warnings: this host has a single
#    arm, so a missing env or signer would leave the timer collecting nothing.
for missing in "$ARM_ENV" "$LIBSIGNER"; do
  mv "$missing" "$missing.bak"
  : > "$SYSTEMCTL_LOG"
  if run_installer >/dev/null 2>"$WORK/err"; then echo "should refuse without $missing"; exit 1; fi
  grep -q "$(basename "$missing")" "$WORK/err" || { echo "error should name $missing"; exit 1; }
  [ ! -s "$SYSTEMCTL_LOG" ] || { echo "systemctl called without $missing"; exit 1; }
  mv "$missing.bak" "$missing"
done

echo "install_lighter_core_points_snapshot: ok"
