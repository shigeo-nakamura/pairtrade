#!/bin/bash
# Pause / resume one arcus-vol instance without leaving inventory open (bot-strategy#1093).
#   arcus-vol-pause.sh pause  <name>   KILL_SWITCH → wait flat (runtime flattens) → stop → remove KILL_SWITCH
#   arcus-vol-pause.sh resume <name>   start
# <name> = spy (unit debot-arcus-vol) or the suffix of debot-arcus-vol-<name>.
set -uo pipefail
ACT=$1; NAME=$2
if [ "$NAME" = spy ]; then U=debot-arcus-vol; E=/etc/debot-arcus-vol/config.env
else U=debot-arcus-vol-$NAME; E=/etc/debot-arcus-vol-$NAME/config.env; fi
[ -f "$E" ] || { echo "no $E"; exit 1; }
C=$(grep -E '^STATE_DIR=' $E | cut -d= -f2)
[ -n "$C" ] || C=/var/lib/$U/state
[ -f "$C/status.json" ] || { echo "no $C/status.json"; exit 1; }
inv() { python3 -c "import json;print(abs(float(json.load(open('$C/status.json'))['inventory']['qty'])))"; }
case "$ACT" in
  pause)
    systemctl is-active --quiet $U || { echo "$U already stopped"; exit 0; }
    touch "$C/KILL_SWITCH"
    for i in $(seq 1 90); do python3 -c "import sys;sys.exit(0 if float('$(inv)')<1e-9 else 1)" && break; sleep 2; done
    echo "$U inventory before stop: $(inv)"
    systemctl stop $U
    rm -f "$C/KILL_SWITCH"
    echo "$U paused ($(systemctl is-active $U))" ;;
  resume)
    rm -f "$C/KILL_SWITCH"
    systemctl start $U; sleep 20
    echo "$U resumed ($(systemctl is-active $U))" ;;
  *) echo "usage: $0 pause|resume <name>"; exit 2 ;;
esac
