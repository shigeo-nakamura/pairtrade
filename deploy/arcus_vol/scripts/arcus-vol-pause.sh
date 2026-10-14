#!/bin/bash
# Pause / resume one arcus-vol instance IN PROCESS (bot-strategy#1093).
#   arcus-vol-pause.sh pause  <name>
#   arcus-vol-pause.sh resume <name>
#   arcus-vol-pause.sh held   <name>   exit 0 iff a KILL_SWITCH hold is in place
# <name> = spy (unit debot-arcus-vol) or the suffix of debot-arcus-vol-<name>.
#
# The process is never stopped or started: a blackout / weekend pause holds the
# running runtime in its KILL_SWITCH halt (quotes cancelled, inventory
# flattened, no new quotes) and resume removes the sentinel. The runtime keeps
# refreshing its per-market dead man's switch the whole time, so a pause costs
# no venue auto-fire (Arcus caps those at 10 per UTC day per subaccount; a stop
# that could not disarm the switch cost one — the 2026-10-14 incident).
#
# Invariants
#   pause
#   1. An instance that is not running is left alone (exit 0).
#   2. KILL_SWITCH is touched, then the script waits for the runtime to
#      ACKNOWLEDGE it: a status.json written AFTER the touch that shows a halt,
#      no resting quote on either side, and a flat inventory (venue dust counts
#      as flat). A pre-touch "flat" status never counts.
#   3. Acknowledged: exit 0, process left running, KILL_SWITCH kept (the hold).
#      Not acknowledged within the wait (or status.json unreadable): KILL_SWITCH
#      kept too (quotes stay pulled, the runtime keeps flattening), logged
#      loudly, exit 3. Either way nothing is stopped.
#   resume
#   4. A disabled/masked unit is an operator hold: KILL_SWITCH and unit left
#      alone (exit 0). A failed is-enabled lookup is an error (exit 6).
#   5. KILL_SWITCH is removed. An instance that is no longer running (it
#      crashed or the host rebooted during the hold: the launcher refuses to
#      start while KILL_SWITCH exists) is started — it was running when the
#      pause recorded it; an operator who wants it down disables it (4).
#      The instance must then be active and show a status.json written after
#      the removal whose halt is not kill_switch (it may still be halted by a
#      daily/cum stop) within the settle time, else exit 5.
set -uo pipefail
ACT=${1:-}; NAME=${2:-}
[ -n "$ACT" ] && [ -n "$NAME" ] || { echo "usage: $0 pause|resume <name>"; exit 2; }
ROOT=${ARCUS_VOL_ROOT:-}                       # test hook: prefix for /etc and /var/lib
SYSTEMCTL=${ARCUS_VOL_SYSTEMCTL:-systemctl}    # test hook
WAIT_S=${ARCUS_VOL_PAUSE_WAIT_S:-180}; STEP_S=${ARCUS_VOL_PAUSE_STEP_S:-2}
SETTLE_S=${ARCUS_VOL_RESUME_SETTLE_S:-30}
if [ "$NAME" = spy ]; then U=debot-arcus-vol; E=$ROOT/etc/debot-arcus-vol/config.env
else U=debot-arcus-vol-$NAME; E=$ROOT/etc/debot-arcus-vol-$NAME/config.env; fi
[ -f "$E" ] || { echo "ERROR no $E" >&2; exit 1; }
C=$(grep -E '^STATE_DIR=' "$E" | cut -d= -f2)
[ -n "$C" ] || C=/var/lib/$U/state
C=$ROOT$C
loud() { echo "ERROR $U: $*" >&2; logger -t arcus-vol-pause -p user.err "$U: $*" 2>/dev/null || true; }
# exit 0 iff status.json was written after $1 (epoch ms) and shows halt + no quotes + flat.
# "flat" includes venue dust: |qty| below inventory.venue_min_qty cannot be traded
# (the runtime carries it by design), so waiting for exact zero never ends (AAPL 2026-10-13).
acked() { python3 - "$C/status.json" "$1" <<'PY'
import json, sys
def flat(inv):
    q = abs(float(inv["qty"]))
    if q < 1e-9 or inv.get("dust") is True:   # the runtime's own dust decision wins
        return True
    m = inv.get("venue_min_qty")
    return m is not None and 0 < float(m) and q < float(m)
try:
    d = json.load(open(sys.argv[1])); since = int(sys.argv[2])
    q = d.get("quotes") or {}
    ok = (int(d["ts_ms"]) > since and d.get("halt") is not None
          and q.get("bid") is None and q.get("ask") is None
          and flat(d["inventory"]))
except Exception:
    ok = False
sys.exit(0 if ok else 1)
PY
}
# exit 0 iff status.json was written after $1 (epoch ms) and its halt is not kill_switch
released() { python3 - "$C/status.json" "$1" <<'PY'
import json, sys
try:
    d = json.load(open(sys.argv[1])); since = int(sys.argv[2])
    ok = int(d["ts_ms"]) > since and d.get("halt") != "kill_switch"
except Exception:
    ok = False
sys.exit(0 if ok else 1)
PY
}
inv() { python3 -c "import json,sys
try: print(json.load(open(sys.argv[1]))['inventory']['qty'])
except Exception: print('unreadable')" "$C/status.json" 2>/dev/null; }
case "$ACT" in
  pause)
    $SYSTEMCTL is-active --quiet "$U" || { echo "$U not running; left alone"; exit 0; }
    [ -d "$C" ] || { loud "no state dir $C; cannot pause"; exit 3; }
    since=$(date +%s%3N)
    touch "$C/KILL_SWITCH" || { loud "cannot create KILL_SWITCH; NOT paused"; exit 3; }
    # durable before the hold is reported: a power loss must not drop the
    # sentinel while the (synced) pause record survives
    sync "$C/KILL_SWITCH" "$C" || { loud "cannot fsync KILL_SWITCH / $C; hold NOT confirmed"; exit 3; }
    waited=0
    until acked "$since"; do
      if [ "$waited" -ge "$WAIT_S" ]; then
        loud "no KILL_SWITCH acknowledgement (halt + no quotes + flat) within ${WAIT_S}s (inventory=$(inv)); KILL_SWITCH kept, runtime keeps flattening. Clear with: $0 resume $NAME"
        exit 3
      fi
      sleep "$STEP_S"; waited=$((waited + STEP_S))
    done
    echo "$U paused in process (KILL_SWITCH held; flat, quotes pulled)" ;;
  resume)
    # An operator who disabled the unit after the pause wants it to stay down
    # (e.g. a risk hold over the weekend): leave it — and any KILL_SWITCH — alone.
    # Only an explicit disabled/masked state is a hold; a failed lookup is an
    # error (nonzero) so the caller keeps the record and retries.
    en=$($SYSTEMCTL is-enabled "$U" 2>/dev/null)
    case "$en" in
      enabled|enabled-runtime|static|indirect|alias|generated|transient) ;;
      disabled|masked|masked-runtime) echo "$U is $en: not resuming (operator hold)"; exit 0 ;;
      *) loud "is-enabled lookup failed (${en:-no output}); not resuming"; exit 6 ;;
    esac
    since=$(date +%s%3N)
    rm -f "$C/KILL_SWITCH" || { loud "cannot remove KILL_SWITCH"; exit 5; }
    if ! $SYSTEMCTL is-active --quiet "$U"; then
      # crashed / rebooted during the hold: the launcher refused to start while
      # KILL_SWITCH existed; it was running when the pause recorded it
      echo "$U not running at resume (crash/reboot during the hold): starting it"
      if ! $SYSTEMCTL start "$U" || ! $SYSTEMCTL is-active --quiet "$U"; then
        loud "start failed or unit not active after start"; exit 5
      fi
    fi
    waited=0
    until released "$since"; do
      if [ "$waited" -ge "$SETTLE_S" ]; then
        loud "runtime did not leave the kill_switch halt within ${SETTLE_S}s after KILL_SWITCH removal"
        exit 5
      fi
      sleep "$STEP_S"; waited=$((waited + STEP_S))
    done
    echo "$U resumed in process (KILL_SWITCH removed, kill halt released)" ;;
  held)
    [ -e "$C/KILL_SWITCH" ] ;;
  *) echo "usage: $0 pause|resume|held <name>"; exit 2 ;;
esac
