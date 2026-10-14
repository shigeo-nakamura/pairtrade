#!/bin/bash
# Pause / resume arcus-vol instances for a scheduled window (bot-strategy#1093):
# weekday cash-open / evening and Sunday-reopen blackouts (all running instances)
# and the SLV weekend pause (one named instance, own record).
#   arcus-vol-reopen.sh pause  [--tag TAG] [NAME...]   NAMEs default to every ACTIVE instance
#   arcus-vol-reopen.sh resume [--tag TAG]             resume what that TAG's pause recorded
# TAG selects the record file /var/lib/arcus-vol-reopen/TAG (default: paused).
# Pausing is in process (arcus-vol-pause.sh: KILL_SWITCH hold, no stop/start),
# so the dead man's switch stays armed and a window costs no venue auto-fire.
# Invariants
#   1. Only ACTIVE instances are paused (a named one that is already stopped,
#      e.g. by the operator before the weekend, is skipped and NOT recorded).
#      An instance that is already HELD (KILL_SWITCH present: another window's
#      hold, e.g. SLV's weekend hold during the Sunday reopen blackout, or the
#      operator's) is skipped and not newly recorded, so a window never
#      releases a hold it did not create. (A hold left by a failed earlier
#      pause of this TAG is still in this TAG's record and still released.)
#      The claim itself is atomic (arcus-vol-pause.sh exit 7 = someone else's
#      KILL_SWITCH appeared after this check): such a name is dropped from
#      the record again unless an earlier pause of this TAG recorded it.
#   2. If the active instances cannot be listed, the pause FAILS (exit 1).
#   3. The record (every instance about to be paused) is written and synced
#      BEFORE any pause starts; if that fails nothing is paused (exit 1).
#   4. Every attempted instance stays recorded, also a failed pause (one left
#      running with KILL_SWITCH needs resume to clear it); any failure → nonzero.
#   5. Resume removes a name only when its resume succeeded; failures stay
#      recorded for the next run; exit nonzero.
set -uo pipefail
ROOT=${ARCUS_VOL_ROOT:-}; SYSTEMCTL=${ARCUS_VOL_SYSTEMCTL:-systemctl}
PAUSE=${ARCUS_VOL_PAUSE_SCRIPT:-/opt/debot/scripts/arcus-vol-pause.sh}
ACT=${1:-}; shift || true
TAG=paused
if [ "${1:-}" = --tag ]; then TAG=${2:-}; shift 2 || true; fi
case "$TAG" in ''|*/*|.*) echo "ERROR bad --tag '$TAG'" >&2; exit 2 ;; esac
DIR=$ROOT/var/lib/arcus-vol-reopen; REC=$DIR/$TAG
name_of() { [ "$1" = debot-arcus-vol ] && echo spy || echo "${1#debot-arcus-vol-}"; }
err() { echo "ERROR $*" >&2; logger -t arcus-vol-reopen -p user.err "$*" 2>/dev/null || true; }
rc=0
case "$ACT" in
  pause)
    if ! units=$($SYSTEMCTL list-units --type=service --state=active --no-legend --plain 'debot-arcus-vol*'); then
      err "cannot list active arcus-vol units; $TAG pause NOT applied"; exit 1
    fi
    active=$(printf '%s\n' "$units" | awk 'NF {print $1}' | sed 's/\.service$//' | while read -r u; do name_of "$u"; done)
    if [ $# -gt 0 ]; then
      todo=(); for n in "$@"; do
        if printf '%s\n' "$active" | grep -qx "$n"; then todo+=("$n"); else echo "$n not running; skipped, not recorded"; fi
      done
    else
      mapfile -t todo < <(printf '%s\n' "$active" | awk 'NF')
    fi
    keep=(); for n in "${todo[@]}"; do
      if "$PAUSE" held "$n" >/dev/null 2>&1; then
        echo "$n already held (KILL_SWITCH present); skipped, not recorded"
      else keep+=("$n"); fi
    done; todo=("${keep[@]}")
    [ ${#todo[@]} -gt 0 ] || { echo "$TAG: nothing running to pause"; exit 0; }
    pre=(); [ -f "$REC" ] && mapfile -t pre < "$REC"
    # record first, durably; no record → no pause
    if ! { mkdir -p "$DIR" \
           && { [ ! -f "$REC" ] || cat "$REC"; printf '%s\n' "${todo[@]}"; } > "$REC.new" \
           && sync "$REC.new" && mv -f "$REC.new" "$REC" && sync "$DIR"; }; then
      rm -f "$REC.new" 2>/dev/null; err "cannot write record $REC; $TAG pause NOT applied (nothing paused)"; exit 1
    fi
    pids=(); for n in "${todo[@]}"; do "$PAUSE" pause "$n" & pids+=($!); done
    notours=()
    for i in "${!pids[@]}"; do
      wait "${pids[$i]}"; r=$?
      if [ "$r" = 7 ]; then
        # a hold appeared between the check and the claim: not ours, never release it
        notours+=("${todo[$i]}"); echo "${todo[$i]} held by someone else; dropped from $TAG record"
      elif [ "$r" != 0 ]; then
        rc=1; err "pause ${todo[$i]} failed (exit $r); recorded for resume"
      fi
    done
    if [ ${#notours[@]} -gt 0 ]; then
      # drop only names this run added; one already recorded by an earlier
      # pause of this TAG is still this TAG's hold
      drop=(); for n in "${notours[@]}"; do printf '%s\n' "${pre[@]:-}" | grep -qx "$n" || drop+=("$n"); done
      if [ ${#drop[@]} -gt 0 ]; then
        if ! { grep -vxF -f <(printf '%s\n' "${drop[@]}") "$REC" > "$REC.new" || true; } \
             || ! { sync "$REC.new" && mv -f "$REC.new" "$REC" && sync "$DIR"; }; then
          rc=1; err "cannot rewrite record $REC after an unclaimed hold"
        fi
        [ -s "$REC" ] || rm -f "$REC"
      fi
    fi
    echo "$TAG paused: $( [ -f "$REC" ] && sort -u "$REC" | tr '\n' ' ')" ;;
  resume)
    [ $# -eq 0 ] || { echo "usage: $0 resume [--tag TAG]" >&2; exit 2; }
    [ -s "$REC" ] || { echo "$TAG: nothing to resume"; rm -f "$REC"; exit 0; }
    mapfile -t names < <(sort -u "$REC")
    pids=(); for n in "${names[@]}"; do "$PAUSE" resume "$n" & pids+=($!); done
    left=()
    for i in "${!pids[@]}"; do
      wait "${pids[$i]}" || { rc=1; left+=("${names[$i]}"); err "resume ${names[$i]} failed; kept for retry"; }
    done
    if [ ${#left[@]} -gt 0 ]; then printf '%s\n' "${left[@]}" > "$REC.new" && mv -f "$REC.new" "$REC"
    else rm -f "$REC"; fi ;;
  *) echo "usage: $0 pause [--tag TAG] [NAME...] | resume [--tag TAG]"; exit 2 ;;
esac
exit $rc
