#!/bin/bash
# Blackout pause/resume for every running arcus-vol instance (bot-strategy#1093):
# Sunday reopens (18:00 / 20:00 New York) and the weekday cash open (09:30 New York),
# when quotes resting on a stale price get picked off.
#   pause:  pause every ACTIVE instance (in parallel), remember which
#   resume: resume only the instances this script paused
# A failed pause (instance not flat → left running with KILL_SWITCH) is still
# recorded, so resume clears its KILL_SWITCH; the overall exit is then nonzero.
set -uo pipefail
ROOT=${ARCUS_VOL_ROOT:-}; SYSTEMCTL=${ARCUS_VOL_SYSTEMCTL:-systemctl}
PAUSE=${ARCUS_VOL_PAUSE_SCRIPT:-/opt/debot/scripts/arcus-vol-pause.sh}
REC=$ROOT/var/lib/arcus-vol-reopen/paused; mkdir -p "$(dirname "$REC")"
name_of() { [ "$1" = debot-arcus-vol ] && echo spy || echo "${1#debot-arcus-vol-}"; }
rc=0
case "${1:-}" in
  pause)
    : > "$REC.new"; pids=(); names=()
    for u in $($SYSTEMCTL list-units --type=service --state=active --no-legend 'debot-arcus-vol*' | awk '{print $1}' | sed 's/\.service$//'); do
      n=$(name_of "$u"); echo "$n" >> "$REC.new"
      "$PAUSE" pause "$n" & pids+=($!); names+=("$n")
    done
    for i in "${!pids[@]}"; do
      wait "${pids[$i]}" || { r=$?; rc=1; echo "ERROR pause ${names[$i]} failed (exit $r); recorded for resume" >&2; }
    done
    cat "$REC.new" >> "$REC"; rm -f "$REC.new"
    echo "paused: $(sort -u "$REC" | tr '\n' ' ')" ;;
  resume)
    [ -f "$REC" ] || { echo "nothing to resume"; exit 0; }
    pids=(); names=()
    for n in $(sort -u "$REC"); do "$PAUSE" resume "$n" & pids+=($!); names+=("$n"); done
    for i in "${!pids[@]}"; do
      wait "${pids[$i]}" || { rc=1; echo "ERROR resume ${names[$i]} failed" >&2; }
    done
    rm -f "$REC" ;;
  *) echo "usage: $0 pause|resume"; exit 2 ;;
esac
exit $rc
