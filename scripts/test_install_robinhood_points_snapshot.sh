#!/usr/bin/env bash
# Tests for scripts/install_robinhood_points_snapshot.sh (bot-strategy#938):
# the installer copies both units, creates the output dir for the run-as
# user, enables the timer through systemctl, and refuses to run when a
# source is missing. systemctl is faked; install(1) is real, so the
# run-as user is the test's own.
set -euo pipefail

REPO_ROOT=$(cd "$(dirname "$0")/.." && pwd)
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
SOURCE="$WORK/deploy"; SYSTEMD="$WORK/systemd"; OUT="$WORK/out/robinhood-points"
mkdir -p "$SOURCE" "$SYSTEMD" "$WORK/scripts"
cp "$REPO_ROOT/deploy/robinhood-points-snapshot.service" "$REPO_ROOT/deploy/robinhood-points-snapshot.timer" "$SOURCE/"
cp "$REPO_ROOT/scripts/robinhood_points_collector.py" "$WORK/scripts/"
HEDGE_ENV="$WORK/scripts/debot-xvenue-hedge-holder.env"
printf 'export LIGHTER_ACCOUNT_INDEX_CORE="1"\n' > "$HEDGE_ENV"; chmod 0600 "$HEDGE_ENV"
cat > "$WORK/systemctl" <<'FAKE'
#!/usr/bin/env bash
printf '%s\n' "$*" >> "$SYSTEMCTL_LOG"
FAKE
chmod +x "$WORK/systemctl"
export SYSTEMCTL_LOG="$WORK/systemctl.log"
me=$(id -un)

run_installer() {
  ROBINHOOD_POINTS_UNIT_SOURCE_DIR="$SOURCE" ROBINHOOD_POINTS_SYSTEMD_DIR="$SYSTEMD" \
  ROBINHOOD_POINTS_COLLECTOR="$WORK/scripts/robinhood_points_collector.py" \
  ROBINHOOD_POINTS_OUT_DIR="$OUT" ROBINHOOD_POINTS_SYSTEMCTL="$WORK/systemctl" \
  ROBINHOOD_POINTS_HEDGE_ENV="$HEDGE_ENV" \
  ROBINHOOD_POINTS_USER="$me" bash "$REPO_ROOT/scripts/install_robinhood_points_snapshot.sh"
}

# 1. happy path
: > "$SYSTEMCTL_LOG"
run_installer >/dev/null
[ -f "$SYSTEMD/robinhood-points-snapshot.service" ] || { echo "service not installed"; exit 1; }
[ -f "$SYSTEMD/robinhood-points-snapshot.timer" ] || { echo "timer not installed"; exit 1; }
[ "$(stat -c %a "$SYSTEMD/robinhood-points-snapshot.timer")" = 644 ] || { echo "timer mode"; exit 1; }
[ -d "$OUT" ] || { echo "out dir not created"; exit 1; }
[ "$(stat -c %a "$OUT")" = 750 ] || { echo "out dir mode"; exit 1; }
grep -qx 'daemon-reload' "$SYSTEMCTL_LOG" || { echo "no daemon-reload"; exit 1; }
grep -qx 'enable --now robinhood-points-snapshot.timer' "$SYSTEMCTL_LOG" || { echo "timer not enabled"; exit 1; }
[ "$(stat -c %a "$HEDGE_ENV")" = 640 ] || { echo "hedge env not group-readable"; exit 1; }
[ "$(stat -c %G "$HEDGE_ENV")" = "$(id -gn)" ] || { echo "hedge env group"; exit 1; }
# The trading service is never named to systemctl.
if grep -q 'debot-pair-robinhood-lighter' "$SYSTEMCTL_LOG"; then echo "touched the bot"; exit 1; fi

# 2. re-run is idempotent; a missing hedge env is a warning, not a refusal
: > "$SYSTEMCTL_LOG"
rm "$HEDGE_ENV"
run_installer >/dev/null 2>"$WORK/warn"
grep -q 'core-canary arm will fail' "$WORK/warn" || { echo "missing hedge env should warn"; exit 1; }
grep -qx 'enable --now robinhood-points-snapshot.timer' "$SYSTEMCTL_LOG" || { echo "re-run"; exit 1; }

# 3. a missing unit refuses before touching systemd
rm "$SOURCE/robinhood-points-snapshot.timer"
: > "$SYSTEMCTL_LOG"
if run_installer >/dev/null 2>"$WORK/err"; then echo "should refuse without the timer"; exit 1; fi
grep -q 'robinhood-points-snapshot.timer' "$WORK/err" || { echo "error should name the unit"; exit 1; }
[ ! -s "$SYSTEMCTL_LOG" ] || { echo "systemctl called despite missing source"; exit 1; }

echo "install_robinhood_points_snapshot: ok"
