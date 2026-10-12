#!/bin/bash
# Blackout pause/resume for every running arcus-vol instance (bot-strategy#1093):
# Sunday reopens (18:00 / 20:00 New York) and the weekday cash open (09:30 New York),
# when quotes resting on a stale price get picked off.
#   pause:  pause every ACTIVE instance (in parallel) and record each one
#   resume: resume the recorded instances; keep failures recorded for a retry
# Invariants
#   1. If the active instances cannot be listed, the pause FAILS (exit 1) — it
#      never reports success while every bot keeps quoting.
#   2. Every instance a pause was attempted on is recorded (also failures: one
#      left running with KILL_SWITCH must get it cleared by resume). Any failed
#      pause makes the exit nonzero.
#   3. Resume removes a name from the record only when its resume succeeded;
#      failures stay recorded (the next resume run retries them); exit nonzero.
set -uo pipefail
ROOT=${ARCUS_VOL_ROOT:-}; SYSTEMCTL=${ARCUS_VOL_SYSTEMCTL:-systemctl}
PAUSE=${ARCUS_VOL_PAUSE_SCRIPT:-/opt/debot/scripts/arcus-vol-pause.sh}
REC=$ROOT/var/lib/arcus-vol-reopen/paused
mkdir -p "$(dirname "$REC")" || { echo "ERROR cannot create $(dirname "$REC")" >&2; exit 1; }
name_of() { [ "$1" = debot-arcus-vol ] && echo spy || echo "${1#debot-arcus-vol-}"; }
rc=0
case "${1:-}" in
  pause)
    if ! units=$($SYSTEMCTL list-units --type=service --state=active --no-legend --plain 'debot-arcus-vol*'); then
      echo "ERROR cannot list active arcus-vol units; blackout NOT applied" >&2
      logger -t arcus-vol-reopen -p user.err "cannot list active units; blackout not applied" 2>/dev/null || true
      exit 1
    fi
    pids=(); names=()
    for u in $(printf '%s\n' "$units" | awk 'NF {print $1}' | sed 's/\.service$//'); do
      n=$(name_of "$u"); echo "$n" >> "$REC"
      "$PAUSE" pause "$n" & pids+=($!); names+=("$n")
    done
    for i in "${!pids[@]}"; do
      wait "${pids[$i]}" || { r=$?; rc=1; echo "ERROR pause ${names[$i]} failed (exit $r); recorded for resume" >&2; }
    done
    echo "paused: $(sort -u "$REC" 2>/dev/null | tr '\n' ' ')" ;;
  resume)
    [ -s "$REC" ] || { echo "nothing to resume"; rm -f "$REC"; exit 0; }
    pids=(); names=()
    for n in $(sort -u "$REC"); do "$PAUSE" resume "$n" & pids+=($!); names+=("$n"); done
    : > "$REC.left"
    for i in "${!pids[@]}"; do
      if ! wait "${pids[$i]}"; then rc=1; echo "${names[$i]}" >> "$REC.left"; echo "ERROR resume ${names[$i]} failed; kept for retry" >&2; fi
    done
    if [ -s "$REC.left" ]; then mv "$REC.left" "$REC"; else rm -f "$REC" "$REC.left"; fi ;;
  *) echo "usage: $0 pause|resume"; exit 2 ;;
esac
exit $rc
