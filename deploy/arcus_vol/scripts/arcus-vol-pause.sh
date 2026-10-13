#!/bin/bash
# Pause / resume one arcus-vol instance without leaving inventory open (bot-strategy#1093).
#   arcus-vol-pause.sh pause  <name>
#   arcus-vol-pause.sh resume <name>
# <name> = spy (unit debot-arcus-vol) or the suffix of debot-arcus-vol-<name>.
#
# Invariants
#   pause
#   1. An instance that is not running is left alone (exit 0).
#   2. KILL_SWITCH is touched, then the script waits for the runtime to
#      ACKNOWLEDGE it: a status.json written AFTER the touch that shows a halt,
#      no resting quote on either side, and zero inventory. A pre-touch "flat"
#      status never counts (quotes could still be resting and fill during the
#      shutdown, which does not flatten).
#   3. No acknowledgement within the wait (or status.json unreadable): the
#      service is NOT stopped — stopping would leave a position unmonitored for
#      the whole blackout / weekend. It keeps running with KILL_SWITCH (quotes
#      pulled, runtime keeps flattening); logged loudly; exit 3.
#   4. KILL_SWITCH is removed only after `systemctl stop` succeeded AND the unit
#      is no longer active. A failed / timed-out stop keeps it; exit 4.
#   resume
#   5. KILL_SWITCH is removed, the unit started; success only if the start
#      succeeds and the unit is active after the settle time, else exit 5.
#      (Starting an instance left running by invariant 3 is a no-op; removing
#      KILL_SWITCH lets that runtime quote again.)
set -uo pipefail
ACT=${1:-}; NAME=${2:-}
[ -n "$ACT" ] && [ -n "$NAME" ] || { echo "usage: $0 pause|resume <name>"; exit 2; }
ROOT=${ARCUS_VOL_ROOT:-}                       # test hook: prefix for /etc and /var/lib
SYSTEMCTL=${ARCUS_VOL_SYSTEMCTL:-systemctl}    # test hook
WAIT_S=${ARCUS_VOL_PAUSE_WAIT_S:-180}; STEP_S=${ARCUS_VOL_PAUSE_STEP_S:-2}
SETTLE_S=${ARCUS_VOL_RESUME_SETTLE_S:-20}
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
inv() { python3 -c "import json,sys
try: print(json.load(open(sys.argv[1]))['inventory']['qty'])
except Exception: print('unreadable')" "$C/status.json" 2>/dev/null; }
case "$ACT" in
  pause)
    $SYSTEMCTL is-active --quiet "$U" || { echo "$U already stopped"; exit 0; }
    [ -d "$C" ] || { loud "no state dir $C; not stopping"; exit 3; }
    since=$(date +%s%3N)
    touch "$C/KILL_SWITCH" || { loud "cannot create KILL_SWITCH; not stopping"; exit 3; }
    waited=0
    until acked "$since"; do
      if [ "$waited" -ge "$WAIT_S" ]; then
        loud "no KILL_SWITCH acknowledgement (halt + no quotes + flat) within ${WAIT_S}s (inventory=$(inv)); NOT stopping — left running with KILL_SWITCH. Clear with: $0 resume $NAME"
        exit 3
      fi
      sleep "$STEP_S"; waited=$((waited + STEP_S))
    done
    echo "$U acknowledged KILL_SWITCH (flat, quotes pulled)"
    if ! $SYSTEMCTL stop "$U" || $SYSTEMCTL is-active --quiet "$U"; then
      loud "stop failed or unit still active; KILL_SWITCH kept"
      exit 4
    fi
    rm -f "$C/KILL_SWITCH"
    echo "$U paused (inactive)" ;;
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
    rm -f "$C/KILL_SWITCH"
    if ! $SYSTEMCTL start "$U"; then loud "start failed"; exit 5; fi
    sleep "$SETTLE_S"
    if ! $SYSTEMCTL is-active --quiet "$U"; then loud "not active ${SETTLE_S}s after start"; exit 5; fi
    echo "$U resumed (active)" ;;
  *) echo "usage: $0 pause|resume <name>"; exit 2 ;;
esac
