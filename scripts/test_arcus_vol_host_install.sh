#!/bin/bash
# Tests for deploy/arcus_vol/install_arcus_vol_host.sh (bot-strategy#1093).
# Runs entirely in a temp dir with a fake systemctl; never touches the real host.
set -u
HERE="$(cd "$(dirname "$0")/.." && pwd)"
INST="$HERE/deploy/arcus_vol/install_arcus_vol_host.sh"
fail=0; ok() { echo "ok   $*"; }; bad() { echo "FAIL $*"; fail=1; }
T=$(mktemp -d); trap 'rm -rf "$T"' EXIT
mkdir -p "$T/sys" "$T/scripts" "$T/root" "$T/bin"
# fake systemctl: enablement in $T/en/<unit>, active timers in $T/act/<unit>, log of calls
mkdir -p "$T/en" "$T/act"
cat > "$T/bin/systemctl" <<SH
#!/bin/bash
echo "\$*" >> "$T/calls"
case "\$1" in
  is-enabled) cat "$T/en/\$2" 2>/dev/null || { echo disabled; exit 1; } ;;
  is-active) cat "$T/act/\$2" 2>/dev/null || { echo inactive; exit 3; } ;;
  enable) [ "\$2" = --now ] && { echo enabled > "$T/en/\$3"; echo active > "$T/act/\$3"; } || echo enabled > "$T/en/\$2" ;;
  disable) echo disabled > "$T/en/\$2" ;;
  daemon-reload) ;;
  *) echo "unexpected systemctl \$*" >> "$T/bad" ;;
esac
SH
chmod +x "$T/bin/systemctl"
export ARCUS_VOL_SYSTEMD_DIR="$T/sys" ARCUS_VOL_SCRIPTS_DIR="$T/scripts" ARCUS_VOL_ROOT="$T/root" ARCUS_VOL_SYSTEMCTL="$T/bin/systemctl" ARCUS_VOL_TEST_NO_CHOWN=1
# qqq is retired on the host: config.env.retired-* present, no config.env
mkdir -p "$T/root/etc/debot-arcus-vol-qqq"; touch "$T/root/etc/debot-arcus-vol-qqq/config.env.retired-20261010T093729Z"

# 1. verify on an empty host: drift, exit 1, nothing written, no state-changing systemctl call
out=$(bash "$INST" --verify 2>&1); rc=$?
[ $rc = 1 ] && ok "verify on empty host exits 1" || bad "verify rc=$rc"
echo "$out" | grep -q "DRIFT missing $T/sys/debot-arcus-vol-gld.service" && ok "verify reports a missing unit" || bad "missing unit not reported"
[ -z "$(ls "$T/sys")" ] && [ -z "$(ls "$T/scripts")" ] && ok "verify writes nothing" || bad "verify wrote files"
grep -qE "^(enable|disable|start|stop|restart|daemon-reload)" "$T/calls" 2>/dev/null && bad "verify changed systemd state" || ok "verify only queries systemd"

# 2. apply: installs everything, sets enablement, enables timers, never start/stop/restart
: > "$T/calls"
out=$(bash "$INST" --apply 2>&1); rc=$?
[ $rc = 0 ] && ok "apply exits 0" || bad "apply rc=$rc"
[ -x "$T/scripts/arcus-vol-pause.sh" ] && [ -x "$T/scripts/arcus-vol-reopen.sh" ] && ok "scripts installed 0755" || bad "scripts"
[ -f "$T/sys/debot-arcus-vol-aapl.service" ] && [ -f "$T/sys/arcus-vol-open-pause.timer" ] && [ -f "$T/sys/debot-arcus-vol.service" ] && ok "units + timers installed" || bad "units"
cmp -s "$T/root/etc/debot-arcus-vol-aapl/config.env" "$HERE/deploy/arcus_vol/config/aapl.config" && ok "missing config.env installed" || bad "config"
[ ! -e "$T/root/etc/debot-arcus-vol-qqq/config.env" ] && ok "retired qqq gets no config.env" || bad "qqq config written"
[ "$("$T/bin/systemctl" is-enabled debot-arcus-vol-nvda)" = disabled ] && [ "$(cat "$T/en/debot-arcus-vol-slv")" = enabled ] && ok "enablement per instances.tsv" || bad "enablement"
[ "$(cat "$T/act/arcus-vol-open-pause.timer")" = active ] && ok "timers enabled --now" || bad "timers"
grep -qE "^(start|stop|restart|reload|try-restart|kill) " "$T/calls" && bad "apply started/stopped a unit" || ok "apply never starts/stops/restarts"
grep -E "^enable --now" "$T/calls" | grep -qv "\.timer" && bad "enable --now on a non-timer" || ok "--now only for timers"
[ -f "$T/bad" ] && bad "unexpected systemctl: $(cat "$T/bad")"
echo "$out" | grep -q "sudo systemctl restart debot-arcus-vol-aapl" && ok "prints restart commands for changed instances" || bad "no restart hint"

# 3. verify after apply: clean
out=$(bash "$INST" --verify 2>&1); rc=$?
[ $rc = 0 ] && echo "$out" | grep -q "^OK: host matches repo" && ok "verify after apply is clean" || { bad "verify after apply rc=$rc"; echo "$out"; }

# 4. a host-tuned config.env is kept by --apply, overwritten (with backup) only by --apply-config
echo "CLIP_MAX_USD=1" >> "$T/root/etc/debot-arcus-vol-gld/config.env"
out=$(bash "$INST" --verify 2>&1); [ $? = 1 ] && echo "$out" | grep -q "DRIFT content $T/root/etc/debot-arcus-vol-gld/config.env" && ok "config drift reported" || bad "config drift"
bash "$INST" --apply >/dev/null 2>&1; grep -q "CLIP_MAX_USD=1" "$T/root/etc/debot-arcus-vol-gld/config.env" && ok "--apply keeps a differing config.env" || bad "--apply overwrote config"
bash "$INST" --apply --apply-config >/dev/null 2>&1
cmp -s "$T/root/etc/debot-arcus-vol-gld/config.env" "$HERE/deploy/arcus_vol/config/gld.config" && ls "$T/root/etc/debot-arcus-vol-gld/"config.env.bak-* >/dev/null 2>&1 && ok "--apply-config overwrites with backup" || bad "--apply-config"

# 5. live.env is never touched
echo "SECRET=x" > "$T/root/etc/debot-arcus-vol-gld/live.env"; bash "$INST" --apply --apply-config >/dev/null 2>&1
[ "$(cat "$T/root/etc/debot-arcus-vol-gld/live.env")" = "SECRET=x" ] && ok "live.env untouched" || bad "live.env changed"
grep -q "live.env" "$INST" && ! grep -vE '^\s*#' "$INST" | grep -q "live.env" && ok "installer never references live.env outside comments" || bad "live.env referenced in code"

# 6. repo units are syntactically sane
bash -n "$INST" && for s in "$HERE"/deploy/arcus_vol/scripts/*.sh; do bash -n "$s" || bad "bash -n $s"; done && ok "bash -n"
exit $fail
