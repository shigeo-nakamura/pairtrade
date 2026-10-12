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
q=0; [ "\$2" = --quiet ] && { q=1; set -- "\$1" "\$3"; }
case "\$1" in
  is-enabled) cat "$T/en/\$2" 2>/dev/null || { echo disabled; exit 1; } ;;
  is-active) s=\$(cat "$T/act/\$2" 2>/dev/null || echo inactive); [ \$q = 1 ] || echo "\$s"; [ "\$s" = active ] ;;
  enable) [ "\$2" = --now ] && { echo enabled > "$T/en/\$3"; echo active > "$T/act/\$3"; } || echo enabled > "$T/en/\$2" ;;
  disable) echo disabled > "$T/en/\$2" ;;
  daemon-reload) ;;
  stop) echo inactive > "$T/act/\$2" ;;
  start) echo active > "$T/act/\$2" ;;
  list-units) for f in "$T"/act/debot-arcus-vol*; do [ "\$(cat "\$f")" = active ] && echo "\$(basename "\$f").service loaded active running x"; done ;;
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

echo active > "$T/act/debot-arcus-vol-aapl"; echo active > "$T/act/debot-arcus-vol-nvda"
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
echo "$out" | grep -q "sudo systemctl try-restart debot-arcus-vol-aapl" && ok "try-restart hint for an enabled+active changed instance" || bad "no try-restart hint for aapl"
echo "$out" | grep -qE "systemctl (try-)?restart debot-arcus-vol-(nvda|qqq)\b" && bad "restart hint for a disabled/retired instance" || ok "no hint for disabled nvda / retired qqq"
echo "$out" | grep -qE "systemctl (try-)?restart debot-arcus-vol-hood\b" && bad "restart hint for an inactive instance" || ok "no hint for an enabled but stopped instance"
echo "$out" | grep -qE "systemctl restart " && bad "plain restart suggested" || ok "only try-restart is suggested"

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

# 7. install failures propagate: unwritable scripts dir → exit 4, no "apply done"
rm -f "$T/scripts/arcus-vol-pause.sh"; chmod 500 "$T/scripts"
out=$(bash "$INST" --apply 2>&1); rc=$?; chmod 700 "$T/scripts"
[ $rc = 4 ] && echo "$out" | grep -q "^FAIL install $T/scripts/arcus-vol-pause.sh" && ! echo "$out" | grep -q "apply done" && ok "install failure → exit 4, FAIL line, no 'apply done'" || { bad "failure not propagated rc=$rc"; echo "$out" | tail -3; }

# 8. pause / reopen scripts (fake systemctl, fake root)
PS="$HERE/deploy/arcus_vol/scripts/arcus-vol-pause.sh"; RS="$HERE/deploy/arcus_vol/scripts/arcus-vol-reopen.sh"
export ARCUS_VOL_PAUSE_WAIT_S=2 ARCUS_VOL_PAUSE_STEP_S=1 ARCUS_VOL_RESUME_SETTLE_S=0 ARCUS_VOL_PAUSE_SCRIPT="$PS"
st() { mkdir -p "$T/root/var/lib/debot-arcus-vol-$1/state"; echo "{\"inventory\":{\"qty\":\"$2\"}}" > "$T/root/var/lib/debot-arcus-vol-$1/state/status.json"; }
for n in gld aapl hood slv; do echo active > "$T/act/debot-arcus-vol-$n"; done
rm -f "$T/act/debot-arcus-vol-nvda" "$T/act/debot-arcus-vol"
st gld 0.0000000; st aapl 7.32; st hood 0; rm -f "$T/root/var/lib/debot-arcus-vol-hood/state/status.json"; echo '{bad json' > "$T/root/var/lib/debot-arcus-vol-hood/state/status.json"; st slv 0
: > "$T/calls"
bash "$PS" pause aapl >/dev/null 2>&1; rc=$?
[ $rc = 3 ] && [ "$(cat "$T/act/debot-arcus-vol-aapl")" = active ] && [ -f "$T/root/var/lib/debot-arcus-vol-aapl/state/KILL_SWITCH" ] && ok "never-flat: not stopped, KILL_SWITCH kept, rc 3" || bad "never-flat pause rc=$rc act=$(cat "$T/act/debot-arcus-vol-aapl")"
grep -q "^stop debot-arcus-vol-aapl" "$T/calls" && bad "never-flat instance was stopped" || ok "never-flat: no stop call"
bash "$PS" pause hood >/dev/null 2>&1; rc=$?
[ $rc = 3 ] && [ "$(cat "$T/act/debot-arcus-vol-hood")" = active ] && ok "unreadable status.json = not flat, not stopped" || bad "unreadable status rc=$rc"
bash "$PS" pause gld >/dev/null 2>&1; rc=$?
[ $rc = 0 ] && [ "$(cat "$T/act/debot-arcus-vol-gld")" = inactive ] && [ ! -f "$T/root/var/lib/debot-arcus-vol-gld/state/KILL_SWITCH" ] && ok "flat: stopped, KILL_SWITCH removed" || bad "flat pause rc=$rc"
# reopen: gld already stopped; aapl/hood not flat, slv flat
rm -f "$T/root/var/lib/debot-arcus-vol-aapl/state/KILL_SWITCH" "$T/root/var/lib/debot-arcus-vol-hood/state/KILL_SWITCH"
bash "$RS" pause > "$T/rs.out" 2>&1; rc=$?
rec="$T/root/var/lib/arcus-vol-reopen/paused"
[ $rc != 0 ] && grep -qx aapl "$rec" && grep -qx hood "$rec" && grep -qx slv "$rec" && ! grep -qx gld "$rec" && ok "reopen pause: failures recorded, rc≠0, stopped instance not recorded" || bad "reopen pause rc=$rc rec=$(tr '\n' ' ' < "$rec" 2>/dev/null)"
[ "$(cat "$T/act/debot-arcus-vol-slv")" = inactive ] && [ "$(cat "$T/act/debot-arcus-vol-aapl")" = active ] && ok "reopen pause: flat one stopped, not-flat one left running" || bad "reopen pause states"
bash "$RS" resume >/dev/null 2>&1; rc=$?
[ $rc = 0 ] && [ ! -f "$T/root/var/lib/debot-arcus-vol-aapl/state/KILL_SWITCH" ] && [ ! -f "$T/root/var/lib/debot-arcus-vol-hood/state/KILL_SWITCH" ] && [ "$(cat "$T/act/debot-arcus-vol-slv")" = active ] && [ ! -f "$rec" ] && ok "reopen resume: KILL_SWITCH cleared, stopped one started, record removed" || bad "reopen resume rc=$rc"
[ "$(cat "$T/act/debot-arcus-vol-gld")" = inactive ] && ok "reopen resume leaves an instance it did not pause alone" || bad "resume started gld"
exit $fail
