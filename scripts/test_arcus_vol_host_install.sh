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
  is-enabled) s=\$(cat "$T/en/\$2" 2>/dev/null || echo disabled); [ \$q = 1 ] || echo "\$s"; [ "\$s" = enabled ] ;;
  is-active) s=\$(cat "$T/act/\$2" 2>/dev/null || echo inactive); [ \$q = 1 ] || echo "\$s"; [ "\$s" = active ] ;;
  enable) [ "\$2" = --now ] && { echo enabled > "$T/en/\$3"; echo active > "$T/act/\$3"; } || echo enabled > "$T/en/\$2" ;;
  disable) echo disabled > "$T/en/\$2" ;;
  daemon-reload) ;;
  stop) [ -e "$T/stopfail" ] && exit 1; echo inactive > "$T/act/\$2" ;;
  start) [ -e "$T/startfail" ] && exit 1; [ -e "$T/startdies" ] && { echo inactive > "$T/act/\$2"; exit 0; }; echo active > "$T/act/\$2" ;;
  list-units) [ -e "$T/listfail" ] && exit 1; for f in "$T"/act/debot-arcus-vol*; do [ "\$(cat "\$f")" = active ] && echo "\$(basename "\$f").service loaded active running x"; done; exit 0 ;;
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
FUT=$(( $(date +%s) * 1000 + 3600000 )); OLD=$(( $(date +%s) * 1000 - 60000 ))
# st <name> <ts_ms> <halt|null> <qty> [resting]  — status.json as the runtime writes it
st() { local q='{"bid":null,"ask":null}'; [ "${5:-}" = resting ] && q='{"bid":{"px":"1"},"ask":null}'
  local h=null; [ "$3" != null ] && h="\"$3\""
  mkdir -p "$T/root/var/lib/debot-arcus-vol-$1/state"
  echo "{\"ts_ms\":$2,\"halt\":$h,\"quotes\":$q,\"inventory\":{\"qty\":\"$4\"}}" > "$T/root/var/lib/debot-arcus-vol-$1/state/status.json"; }
ks() { [ -f "$T/root/var/lib/debot-arcus-vol-$1/state/KILL_SWITCH" ]; }
act() { cat "$T/act/debot-arcus-vol-$1"; }
up() { for n in "$@"; do echo active > "$T/act/debot-arcus-vol-$n"; done; }
rm -f "$T/act/debot-arcus-vol-nvda" "$T/act/debot-arcus-vol"
pz() { : > "$T/calls"; bash "$PS" pause "$1" >/dev/null 2>&1; }

up gld; st gld "$FUT" kill_switch 0;              pz gld; rc=$?
[ $rc = 0 ] && [ "$(act gld)" = inactive ] && ! ks gld && ok "acked (post-touch, halt, no quotes, flat): stopped, KILL_SWITCH removed" || bad "acked pause rc=$rc"
up gld; st gld "$OLD" kill_switch 0;              pz gld; rc=$?
[ $rc = 3 ] && [ "$(act gld)" = active ] && ks gld && ! grep -q "^stop" "$T/calls" && ok "pre-touch flat status is not an ack: not stopped, KILL_SWITCH kept, rc 3" || bad "stale ack rc=$rc"
up gld; st gld "$FUT" kill_switch 0 resting;      pz gld; rc=$?
[ $rc = 3 ] && [ "$(act gld)" = active ] && ok "quote still resting: not stopped" || bad "resting rc=$rc"
up gld; st gld "$FUT" null 0;                     pz gld; rc=$?
[ $rc = 3 ] && [ "$(act gld)" = active ] && ok "no halt in status: not stopped" || bad "no-halt rc=$rc"
up aapl; st aapl "$FUT" kill_switch 7.32;         pz aapl; rc=$?
[ $rc = 3 ] && [ "$(act aapl)" = active ] && ks aapl && ok "never flat: not stopped, KILL_SWITCH kept, rc 3" || bad "never-flat rc=$rc"
up hood; st hood "$FUT" kill_switch 0; echo '{bad' > "$T/root/var/lib/debot-arcus-vol-hood/state/status.json"; pz hood; rc=$?
[ $rc = 3 ] && [ "$(act hood)" = active ] && ok "unreadable status.json: not stopped" || bad "unreadable rc=$rc"
up gld; st gld "$FUT" kill_switch 0; touch "$T/stopfail"; pz gld; rc=$?; rm -f "$T/stopfail"
[ $rc = 4 ] && ks gld && ok "stop fails: KILL_SWITCH kept, rc 4" || bad "stop-fail rc=$rc"
rm -f "$T/root/var/lib/debot-arcus-vol-gld/state/KILL_SWITCH"; echo inactive > "$T/act/debot-arcus-vol-gld"
touch "$T/startfail"; bash "$PS" resume gld >/dev/null 2>&1; rc=$?; rm -f "$T/startfail"
[ $rc = 5 ] && ok "start fails: resume rc 5" || bad "start-fail rc=$rc"
touch "$T/startdies"; bash "$PS" resume gld >/dev/null 2>&1; rc=$?; rm -f "$T/startdies"
[ $rc = 5 ] && ok "start ok but unit dies: resume rc 5" || bad "start-dies rc=$rc"
bash "$PS" resume gld >/dev/null 2>&1; rc=$?
[ $rc = 0 ] && [ "$(act gld)" = active ] && ok "resume starts the unit" || bad "resume rc=$rc"

# reopen: gld acked, aapl never flat, slv acked, hood already stopped
rec="$T/root/var/lib/arcus-vol-reopen/paused"; rm -f "$rec"
for f in "$T"/root/var/lib/debot-arcus-vol-*/state/KILL_SWITCH; do rm -f "$f"; done
up gld aapl slv; echo inactive > "$T/act/debot-arcus-vol-hood"
st gld "$FUT" kill_switch 0; st aapl "$FUT" kill_switch 7.32; st slv "$FUT" kill_switch 0
bash "$RS" pause >/dev/null 2>&1; rc=$?
[ $rc != 0 ] && grep -qx aapl "$rec" && grep -qx gld "$rec" && grep -qx slv "$rec" && ! grep -qx hood "$rec" && ok "reopen pause: failure recorded, rc≠0, stopped instance not recorded" || bad "reopen pause rc=$rc rec=$(tr '\n' ' ' < "$rec" 2>/dev/null)"
[ "$(act slv)" = inactive ] && [ "$(act aapl)" = active ] && ks aapl && ok "reopen pause: acked one stopped, not-flat one left running with KILL_SWITCH" || bad "reopen pause states"
touch "$T/startfail"; bash "$RS" resume >/dev/null 2>&1; rc=$?; rm -f "$T/startfail"
[ $rc != 0 ] && grep -qx slv "$rec" && grep -qx gld "$rec" && ! ks aapl && ok "reopen resume: failed starts kept for retry, rc≠0 (running one cleared)" || bad "resume-fail rc=$rc rec=$(tr '\n' ' ' < "$rec" 2>/dev/null)"
bash "$RS" resume >/dev/null 2>&1; rc=$?
[ $rc = 0 ] && [ ! -e "$rec" ] && [ "$(act slv)" = active ] && [ "$(act gld)" = active ] && ok "reopen resume retry: all started, record removed" || bad "resume retry rc=$rc"
[ "$(act hood)" = inactive ] && ok "reopen never starts an instance it did not pause" || bad "hood started"
touch "$T/listfail"; up gld; bash "$RS" pause >/dev/null 2>&1; rc=$?; rm -f "$T/listfail"
[ $rc = 1 ] && [ "$(act gld)" = active ] && [ ! -s "$rec" ] && ok "list-units fails: pause exits 1, nothing claimed" || bad "list-fail rc=$rc"

# 9. weekend path (own record per tag) and record-before-pause
for f in "$T"/root/var/lib/debot-arcus-vol-*/state/KILL_SWITCH; do rm -f "$f"; done
RD="$T/root/var/lib/arcus-vol-reopen"; rm -f "$RD"/*
# operator stopped HOOD before Saturday: weekend pause skips it, resume must not start it
echo inactive > "$T/act/debot-arcus-vol-hood"; : > "$T/calls"
bash "$RS" pause --tag weekend-hood hood >/dev/null 2>&1; rc=$?
[ $rc = 0 ] && [ ! -s "$RD/weekend-hood" ] && ok "weekend pause: already-stopped bot skipped, not recorded" || bad "weekend skip rc=$rc"
bash "$RS" resume --tag weekend-hood >/dev/null 2>&1; rc=$?
[ $rc = 0 ] && [ "$(act hood)" = inactive ] && ! grep -q "^start debot-arcus-vol-hood" "$T/calls" && ok "weekend resume does not start an operator-stopped bot" || bad "weekend resume started hood rc=$rc"
# normal weekend: running + acked → stopped, recorded under its tag only; resume starts it
up hood gld; st hood "$FUT" kill_switch 0
bash "$RS" pause --tag weekend-hood hood >/dev/null 2>&1; rc=$?
[ $rc = 0 ] && [ "$(act hood)" = inactive ] && grep -qx hood "$RD/weekend-hood" && [ "$(act gld)" = active ] && [ ! -e "$RD/paused" ] && ok "weekend pause: named bot stopped + recorded under its tag, others untouched" || bad "weekend pause rc=$rc"
bash "$RS" resume >/dev/null 2>&1
[ "$(act hood)" = inactive ] && ok "default-tag resume ignores the weekend record" || bad "default resume started hood"
bash "$RS" resume --tag weekend-hood >/dev/null 2>&1; rc=$?
[ $rc = 0 ] && [ "$(act hood)" = active ] && [ ! -e "$RD/weekend-hood" ] && ok "weekend resume starts what it stopped, record removed" || bad "weekend resume rc=$rc"
# operator disabled HOOD after the weekend pause → resume must leave it stopped
st hood "$FUT" kill_switch 0
bash "$RS" pause --tag weekend-hood hood >/dev/null 2>&1
echo disabled > "$T/en/debot-arcus-vol-hood"; : > "$T/calls"
bash "$RS" resume --tag weekend-hood >/dev/null 2>&1; rc=$?
[ $rc = 0 ] && [ "$(act hood)" = inactive ] && ! grep -q "^start debot-arcus-vol-hood" "$T/calls" && ok "resume leaves a bot disabled after the pause stopped" || bad "resume started a disabled bot rc=$rc"
echo enabled > "$T/en/debot-arcus-vol-hood"; up hood; rm -f "$RD"/weekend-hood
# record cannot be written → nothing paused, rc≠0
up gld slv; st gld "$FUT" kill_switch 0; st slv "$FUT" kill_switch 0; rm -f "$RD"/*; chmod 500 "$RD"; : > "$T/calls"
bash "$RS" pause >/dev/null 2>&1; rc=$?; chmod 700 "$RD"
[ $rc = 1 ] && [ "$(act gld)" = active ] && [ "$(act slv)" = active ] && ! grep -q "^stop" "$T/calls" && ! ks gld && ok "record write fails: nothing paused, no KILL_SWITCH, rc 1" || bad "record-fail rc=$rc"
bash "$RS" pause --tag ../x hood >/dev/null 2>&1; [ $? = 2 ] && ok "bad --tag rejected" || bad "bad tag accepted"
# units: every pause/resume oneshot can outlive the 180 s ack wait
for u in "$HERE"/deploy/arcus_vol/units/arcus-vol-*.service; do
  grep -qx "TimeoutStartSec=600" "$u" || bad "no TimeoutStartSec=600 in $(basename "$u")"
done; ok "TimeoutStartSec=600 on every pause/resume service"
grep -l "arcus-vol-pause.sh" "$HERE"/deploy/arcus_vol/units/*.service >/dev/null 2>&1 && bad "a unit calls arcus-vol-pause.sh directly (bypasses the record)" || ok "all units go through the recorded path"

# 10. run EVERY timer-driven unit's ExecStart (as systemd would) against the stubs:
#     nothing running, no records → each must exit 0 and touch no unit
for f in "$T"/act/debot-arcus-vol*; do echo inactive > "$f"; done
rm -f "$T"/root/var/lib/arcus-vol-reopen/*; : > "$T/calls"; nexec=0
for u in "$HERE"/deploy/arcus_vol/units/arcus-vol-*.service; do
  line=$(grep -E '^ExecStart=' "$u" | head -1 | cut -d= -f2-)
  [ -n "$line" ] || { bad "no ExecStart in $(basename "$u")"; continue; }
  cmd=${line//\/opt\/debot\/scripts\//$HERE\/deploy\/arcus_vol\/scripts\/}
  out=$(bash -c "$cmd" 2>&1); rc=$?; nexec=$((nexec + 1))
  [ $rc = 0 ] || bad "ExecStart of $(basename "$u") exits $rc: $out"
done
[ $nexec = 8 ] && ok "ran all 8 unit ExecStart lines" || bad "ran $nexec unit ExecStart lines, expected 8"
grep -qE "^(start|stop|restart) " "$T/calls" && bad "an idle ExecStart started/stopped a unit" || ok "idle ExecStart lines touch no unit"
# and a recorded weekend round trip through the units' own command lines
up hood; st hood "$FUT" kill_switch 0
for a in pause resume; do
  line=$(grep -E '^ExecStart=' "$HERE/deploy/arcus_vol/units/arcus-vol-$a-hood.service" | cut -d= -f2-)
  bash -c "${line//\/opt\/debot\/scripts\//$HERE/deploy/arcus_vol/scripts/}" >/dev/null 2>&1 || bad "weekend $a unit command failed"
  [ $a = pause ] && { [ "$(act hood)" = inactive ] && ok "weekend pause unit stops HOOD" || bad "weekend pause unit"; }
done
[ "$(act hood)" = active ] && [ ! -e "$T/root/var/lib/arcus-vol-reopen/weekend-hood" ] && ok "weekend resume unit restarts what its pause stopped" || bad "weekend resume unit"
exit $fail
