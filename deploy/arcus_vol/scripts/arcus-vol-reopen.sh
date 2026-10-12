#!/bin/bash
# Reopen blackout (bot-strategy#1093, owner 10-12): pull every running arcus-vol
# instance for ~10 min around the Sunday venue reopens (18:00 and 20:00 New York),
# when quotes resting on the frozen weekend price get picked off.
#   pause:  pause every ACTIVE instance (in parallel), remember which
#   resume: resume only the instances this script paused
set -uo pipefail
REC=/var/lib/arcus-vol-reopen/paused; mkdir -p "$(dirname $REC)"
name_of() { [ "$1" = debot-arcus-vol ] && echo spy || echo "${1#debot-arcus-vol-}"; }
case "${1:-}" in
  pause)
    : > "$REC.new"
    for u in $(systemctl list-units --type=service --state=active --no-legend 'debot-arcus-vol*' | awk '{print $1}' | sed 's/\.service$//'); do
      n=$(name_of "$u"); echo "$n" >> "$REC.new"
      /opt/debot/scripts/arcus-vol-pause.sh pause "$n" &
    done
    wait; cat "$REC.new" >> "$REC"; rm -f "$REC.new"
    echo "paused: $(sort -u $REC | tr '\n' ' ')" ;;
  resume)
    [ -f "$REC" ] || { echo "nothing to resume"; exit 0; }
    for n in $(sort -u "$REC"); do /opt/debot/scripts/arcus-vol-pause.sh resume "$n" & done
    wait; rm -f "$REC" ;;
  *) echo "usage: $0 pause|resume"; exit 2 ;;
esac
