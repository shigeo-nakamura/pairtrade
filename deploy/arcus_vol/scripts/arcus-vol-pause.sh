#!/bin/bash
# Pause / resume one arcus-vol instance without leaving inventory open (bot-strategy#1093).
#   arcus-vol-pause.sh pause  <name>   KILL_SWITCH → wait flat (runtime flattens) → stop → remove KILL_SWITCH
#   arcus-vol-pause.sh resume <name>   remove KILL_SWITCH → start
# <name> = spy (unit debot-arcus-vol) or the suffix of debot-arcus-vol-<name>.
#
# If the instance is not flat within the wait (or its status.json cannot be
# read), the service is NOT stopped: stopping would leave the position
# unmonitored for the whole blackout / weekend. It keeps running with
# KILL_SWITCH in place (runtime keeps flattening, quotes stay pulled), the
# failure is logged loudly, and the script exits 3. `resume` clears it.
set -uo pipefail
ACT=${1:-}; NAME=${2:-}
[ -n "$ACT" ] && [ -n "$NAME" ] || { echo "usage: $0 pause|resume <name>"; exit 2; }
ROOT=${ARCUS_VOL_ROOT:-}                       # test hook: prefix for /etc and /var/lib
SYSTEMCTL=${ARCUS_VOL_SYSTEMCTL:-systemctl}    # test hook
WAIT_S=${ARCUS_VOL_PAUSE_WAIT_S:-180}; STEP_S=${ARCUS_VOL_PAUSE_STEP_S:-2}
if [ "$NAME" = spy ]; then U=debot-arcus-vol; E=$ROOT/etc/debot-arcus-vol/config.env
else U=debot-arcus-vol-$NAME; E=$ROOT/etc/debot-arcus-vol-$NAME/config.env; fi
[ -f "$E" ] || { echo "no $E"; exit 1; }
C=$(grep -E '^STATE_DIR=' "$E" | cut -d= -f2)
[ -n "$C" ] || C=/var/lib/$U/state
C=$ROOT$C
# absolute inventory qty, or empty when status.json is missing / unreadable (= NOT flat)
inv() { python3 -c "import json,sys
try: print(abs(float(json.load(open(sys.argv[1]))['inventory']['qty'])))
except Exception: pass" "$C/status.json" 2>/dev/null; }
flat() { local q; q=$(inv); [ -n "$q" ] && python3 -c "import sys;sys.exit(0 if float(sys.argv[1])<1e-9 else 1)" "$q"; }
case "$ACT" in
  pause)
    $SYSTEMCTL is-active --quiet "$U" || { echo "$U already stopped"; exit 0; }
    [ -d "$C" ] || { echo "ERROR $U: no state dir $C; not stopping"; exit 3; }
    touch "$C/KILL_SWITCH"
    waited=0
    until flat; do
      [ "$waited" -ge "$WAIT_S" ] && break
      sleep "$STEP_S"; waited=$((waited + STEP_S))
    done
    if ! flat; then
      q=$(inv); echo "ERROR $U: not flat after ${WAIT_S}s (inventory=${q:-unreadable}); NOT stopping — left running with KILL_SWITCH (quotes pulled, runtime keeps flattening). Clear with: $0 resume $NAME" >&2
      logger -t arcus-vol-pause -p user.err "$U not flat after ${WAIT_S}s (inventory=${q:-unreadable}); left running with KILL_SWITCH" 2>/dev/null || true
      exit 3
    fi
    echo "$U inventory before stop: $(inv)"
    $SYSTEMCTL stop "$U"
    rm -f "$C/KILL_SWITCH"
    echo "$U paused ($($SYSTEMCTL is-active "$U"))" ;;
  resume)
    rm -f "$C/KILL_SWITCH"
    $SYSTEMCTL start "$U"; sleep "${ARCUS_VOL_RESUME_SETTLE_S:-20}"
    echo "$U resumed ($($SYSTEMCTL is-active "$U"))" ;;
  *) echo "usage: $0 pause|resume <name>"; exit 2 ;;
esac
