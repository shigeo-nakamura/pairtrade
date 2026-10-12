#!/bin/bash
# Reproduce / check the Arcus volume-bot host state on Tokyo debot-main
# (bot-strategy#1093). Covers everything the deploy-arcus-vol.yml workflow does
# NOT install: the per-market instance units, their config.env files, the
# pause/reopen scripts and the blackout / weekend-pause timers.
#
#   sudo ./install_arcus_vol_host.sh            # = --verify: report drift, change nothing
#   sudo ./install_arcus_vol_host.sh --verify
#   sudo ./install_arcus_vol_host.sh --apply    # install files, daemon-reload, set enablement, enable timers
#   sudo ./install_arcus_vol_host.sh --apply --apply-config   # also overwrite differing config.env (backup kept)
#
# It NEVER starts, stops or restarts a trading unit. After --apply it prints the
# restart commands for instances whose unit or config.env changed; the operator
# runs them (owner approval required for any bot restart on this host).
# live.env (credentials) is never read, written or compared.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
MODE=verify; APPLY_CONFIG=0
for a in "$@"; do case "$a" in
  --verify) MODE=verify ;; --apply) MODE=apply ;; --apply-config) APPLY_CONFIG=1 ;;
  -h|--help) sed -n '2,17p' "$0"; exit 0 ;; *) echo "unknown arg $a"; exit 2 ;; esac; done
[ "$APPLY_CONFIG" = 1 ] && [ "$MODE" != apply ] && { echo "--apply-config needs --apply"; exit 2; }
SYS=${ARCUS_VOL_SYSTEMD_DIR:-/etc/systemd/system}
SCRIPTS=${ARCUS_VOL_SCRIPTS_DIR:-/opt/debot/scripts}
ROOT=${ARCUS_VOL_ROOT:-}            # test hook: prefix for /etc and /var/lib paths
SYSTEMCTL=${ARCUS_VOL_SYSTEMCTL:-systemctl}
OWN=(-o root -g root); [ "${ARCUS_VOL_TEST_NO_CHOWN:-}" = 1 ] && OWN=()   # test hook only
drift=0; changed=(); ts=$(date -u +%Y%m%dT%H%M%SZ)
say() { echo "$*"; }
d() { drift=1; say "DRIFT $*"; }

# file <repo-src> <host-dst> <mode>
file() {
  local src=$1 dst=$2 mode=$3
  if [ -f "$dst" ] && cmp -s "$src" "$dst"; then
    local m; m=$(stat -c %a "$dst"); [ "$m" = "$mode" ] || { d "mode $dst $m != $mode"; [ $MODE = apply ] && chmod "$mode" "$dst"; }
    return 0
  fi
  if [ -f "$dst" ]; then d "content $dst"; else d "missing $dst"; fi
  if [ $MODE = apply ]; then install "${OWN[@]}" -m "$mode" "$src" "$dst" && say "  installed $dst"; return 1; fi
  return 1
}

# 1. scripts
for s in arcus-vol-pause.sh arcus-vol-reopen.sh; do file "$HERE/scripts/$s" "$SCRIPTS/$s" 755; done

# 2. timer/oneshot units
reload=0
for u in "$HERE"/units/arcus-vol-*; do file "$u" "$SYS/$(basename "$u")" 644 || reload=1; done

# 3. instances
while read -r name unit etc state enab wallet note; do
  case "$name" in ''|\#*) continue ;; esac
  # unit file: spy's comes from deploy/debot-arcus-vol.service (installed by the workflow)
  if [ "$name" = spy ]; then usrc="$HERE/../debot-arcus-vol.service"; else usrc="$HERE/units/$unit.service"; fi
  file "$usrc" "$SYS/$unit.service" 644 || { reload=1; changed+=("$unit"); }
  # config.env
  # repo copy is .config, not .env: *.env is gitignored repo-wide (secrets guard)
  cfg="$HERE/config/$name.config"; dst="$ROOT$etc/config.env"
  if [ -f "$cfg.retired" ]; then
    [ -e "$dst" ] && d "retired $name still has $dst"
    ls "$ROOT$etc"/config.env.retired-* >/dev/null 2>&1 || d "retired $name: no $etc/config.env.retired-* on host"
  else
    if [ -f "$dst" ] && cmp -s "$cfg" "$dst"; then :
    else
      if [ -f "$dst" ]; then d "content $dst"; else d "missing $dst"; fi
      if [ $MODE = apply ] && { [ ! -f "$dst" ] || [ $APPLY_CONFIG = 1 ]; }; then
        mkdir -p -m 700 "$ROOT$etc"
        [ -f "$dst" ] && cp -p "$dst" "$dst.bak-$ts"
        install "${OWN[@]}" -m 644 "$cfg" "$dst" && say "  installed $dst"; changed+=("$unit")
      elif [ $MODE = apply ]; then say "  kept host $dst (use --apply-config to overwrite)"; fi
    fi
  fi
  [ $MODE = apply ] && mkdir -p "$ROOT$state/state" "$ROOT$state/locks"
  # enablement only — never start/stop
  cur=$($SYSTEMCTL is-enabled "$unit" 2>/dev/null || true)
  if [ "$cur" != "$enab" ]; then
    d "enablement $unit $cur != $enab"
    [ $MODE = apply ] && $SYSTEMCTL "${enab%d}" "$unit" >/dev/null 2>&1   # enable / disable (no --now)
  fi
done < "$HERE/instances.tsv"

# 4. timers enabled + active
if [ $MODE = apply ] && [ $reload = 1 ]; then $SYSTEMCTL daemon-reload; fi
for t in "$HERE"/units/arcus-vol-*.timer; do
  t=$(basename "$t")
  e=$($SYSTEMCTL is-enabled "$t" 2>/dev/null || true); a=$($SYSTEMCTL is-active "$t" 2>/dev/null || true)
  if [ "$e" != enabled ] || [ "$a" != active ]; then
    d "timer $t enabled=$e active=$a"
    [ $MODE = apply ] && $SYSTEMCTL enable --now "$t" >/dev/null 2>&1
  fi
done

if [ $MODE = apply ] && [ ${#changed[@]} -gt 0 ]; then
  say ""; say "Changed instance unit/config — restart is the OPERATOR's call (owner approval; check flat first):"
  for u in $(printf '%s\n' "${changed[@]}" | sort -u); do say "  sudo systemctl restart $u"; done
fi
if [ $drift = 0 ]; then say "OK: host matches repo (deploy/arcus_vol)"; exit 0; fi
[ $MODE = verify ] && { say "drift found (verify only, nothing changed)"; exit 1; }
say "apply done (drift listed above was being corrected)"; exit 0
