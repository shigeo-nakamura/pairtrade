#!/bin/bash
# Install / check the NVDA hedge-holder instance `debot-xvenue-hedge-arcus-nvda`
# on Tokyo debot-main (bot-strategy#1293, second Arcus points hold). Runs ON THE
# HOST as root (the owner-run workstation script ships this directory and the
# Lighter key fragment over SSM/S3 and calls it).
#
#   sudo ./install_hedge_arcus_nvda.sh --verify                 # report drift, change nothing (exit 1 on drift)
#   sudo ./install_hedge_arcus_nvda.sh --apply --fragment FILE  # install, then start in DRY_RUN and post-check
#
# What --apply does (all idempotent except the env, which is written once):
#   /opt/debot-hedge-arcus-nvda/{bin,lib,hedge}   binary + libsigner COPIED from the AAPL
#                                                instance /opt/debot-hedge-arcus (same build)
#   /opt/debot/scripts/debot-xvenue-hedge-arcus.sh   shared launcher (this directory's copy; the
#                                                AAPL instance picks it up at its next restart,
#                                                its running process is untouched)
#   /etc/systemd/system/debot-xvenue-hedge-arcus-nvda.service
#   /opt/debot/scripts/debot-xvenue-hedge-arcus-nvda.env  (root 0600) = env.example with the key
#       lines filled: Arcus wallet 0xA2C7 lines copied from /etc/debot-arcus-vol/live.env (the
#       retired SPY volume bot's credentials, already ciphertext under the host data key) and the
#       Lighter Core lines from FILE, the owner's fragment (ciphertext only, exactly these keys:
#       LIGHTER_ACCOUNT_INDEX_CORE LIGHTER_API_KEY_INDEX_CORE LIGHTER_PUBLIC_API_KEY_CORE
#       LIGHTER_PRIVATE_API_KEY_CORE). Nothing is decrypted and no key value is ever printed.
#   then daemon-reload, enable, start (HEDGE_DRY_RUN=true) and post-check journal + status.json.
#   On a failed post-check the new unit is stopped again (DRY_RUN: nothing was traded).
# It never starts, stops or restarts any OTHER unit, and refuses while a volume bot on the
# same wallet (debot-arcus-vol / -gld) is enabled or active: the hold wallet is hold-only.
# A re-run with an existing env only refreshes files: a running unit is never restarted, and
# a stopped unit whose env is already LIVE (HEDGE_DRY_RUN != true or a confirm token) is never
# started here — a live instance is restarted by the owner only (a persisted armed book resumes).
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
MODE=""; FRAGMENT=""
while [ $# -gt 0 ]; do case "$1" in
  --verify|--apply) MODE=${1#--}; shift ;;
  --fragment) FRAGMENT=${2:-}; shift 2 ;;
  -h|--help) sed -n '2,30p' "$0"; exit 0 ;;
  *) echo "unknown arg $1" >&2; exit 2 ;; esac; done
[ -n "$MODE" ] || { echo "usage: $0 --verify | --apply --fragment FILE" >&2; exit 2; }
[ "$MODE" = apply ] && [ -z "$FRAGMENT" ] && { echo "--apply needs --fragment FILE" >&2; exit 2; }

# Instance table (must match the launcher).
UNIT=debot-xvenue-hedge-arcus-nvda
SRC_UNIT=debot-xvenue-hedge-arcus
SYMBOL=NVDA
WALLET=0xa2c78e14dfd5586444ce4fe28fc4e36308a066d6
CORE_ACCOUNT=281474976624889
VOL_UNITS="debot-arcus-vol debot-arcus-vol-gld"   # volume bots on the same wallet: must stay off

ROOT=${HEDGE_ARCUS_ROOT:-}                        # test hook: prefix for /opt and /etc
SYSTEMCTL=${HEDGE_ARCUS_SYSTEMCTL:-systemctl}
JOURNALCTL=${HEDGE_ARCUS_JOURNALCTL:-journalctl}
WAIT=${HEDGE_ARCUS_WAIT_SECS:-200}                # 105 s unit stagger + start-up
OWN=(-o root -g root); [ "${HEDGE_ARCUS_TEST_NO_CHOWN:-}" = 1 ] && OWN=()
SCRIPTS=$ROOT/opt/debot/scripts
SRC_BASE=$ROOT/opt/debot-hedge-arcus
BASE=$ROOT/opt/debot-hedge-arcus-nvda
SYS=$ROOT/etc/systemd/system
LAUNCHER=$SCRIPTS/debot-xvenue-hedge-arcus.sh
UNIT_FILE=$SYS/$UNIT.service
ENVF=$SCRIPTS/$UNIT.env
SECRETS=$SCRIPTS/debot_secrets_common.env
VOL_LIVE=$ROOT/etc/debot-arcus-vol/live.env
EXAMPLE=$HERE/$UNIT.env.example

failed=0; drift=0
say() { echo "$*"; }
failf() { failed=1; say "FAIL $*" >&2; }
d() { drift=1; say "DRIFT $*"; }
lc() { printf '%s' "$1" | tr 'A-F' 'a-f'; }
# Read one KEY from a KEY=VALUE / export KEY="VALUE" file without sourcing it.
kv_get() { local file=$1 key=$2 line value
  while IFS= read -r line || [ -n "$line" ]; do
    line=${line%$'\r'}
    [[ "$line" =~ ^[[:space:]]*(export[[:space:]]+)?${key}=(.*)$ ]] || continue
    value=${BASH_REMATCH[2]}; [[ "$value" =~ ^\"(.*)\"$ ]] && value=${BASH_REMATCH[1]}
    printf '%s' "$value"; return 0
  done < "$file"; return 0; }
# base64 of 16-byte IV + 48-byte AES-CBC body: what encrypt.py / arcus_vol_encrypt_key.sh
# produce for a 32-byte Arcus seed or a 40-byte Lighter key.
is_cipher64() { local n; [[ "$1" =~ ^[A-Za-z0-9+/]{86}==$ ]] || return 1
  n=$(printf '%s' "$1" | base64 -d 2>/dev/null | wc -c) || return 1; [ "$n" -eq 64 ]; }

# ---------------------------------------------------------------- preflight (both modes)
for f in "$HERE/debot-xvenue-hedge-arcus.sh" "$HERE/$UNIT.service" "$EXAMPLE"; do [ -f "$f" ] || failf "kit file missing: $f"; done
bash -n "$HERE/debot-xvenue-hedge-arcus.sh" 2>/dev/null || failf "launcher does not parse"
for f in "$SRC_BASE/bin/xvenue_hedge_holder" "$SRC_BASE/lib/libsigner.so"; do
  [ -f "$f" ] && say "source $(basename "$f") sha256 $(sha256sum "$f" | cut -c1-12)" || failf "AAPL instance file missing: $f (the binary is copied from it)"; done
[ -f "$SCRIPTS/debot.env" ] || failf "missing $SCRIPTS/debot.env"
if [ -f "$SECRETS" ]; then common_edk=$(kv_get "$SECRETS" ENCRYPTED_DATA_KEY); [ -n "$common_edk" ] || failf "$SECRETS has no ENCRYPTED_DATA_KEY"
else common_edk=""; failf "missing $SECRETS"; fi
for u in $VOL_UNITS; do
  "$SYSTEMCTL" is-active --quiet "$u" 2>/dev/null && failf "$u is ACTIVE on the hold wallet — the wallet is hold-only (bot-strategy#1093)"
  [ "$("$SYSTEMCTL" is-enabled "$u" 2>/dev/null)" = enabled ] && failf "$u is enabled — disable it first (hold wallet is hold-only)"
done
# Arcus credentials of wallet 0xA2C7: the retired volume bot's live.env (never printed).
if [ -f "$VOL_LIVE" ]; then
  [ "$(stat -c %a "$VOL_LIVE")" = 600 ] || failf "$VOL_LIVE is mode $(stat -c %a "$VOL_LIVE"), not 600"
  [ "$(lc "$(kv_get "$VOL_LIVE" ARCUS_ADDRESS)")" = "$WALLET" ] || failf "$VOL_LIVE ARCUS_ADDRESS is not wallet $WALLET"
  vi=$(kv_get "$VOL_LIVE" ARCUS_ACCOUNT_INDEX); [ -z "$vi" ] || [ "$vi" = 0 ] || failf "$VOL_LIVE ARCUS_ACCOUNT_INDEX is $vi, not 0"
  a_pub=$(kv_get "$VOL_LIVE" ARCUS_API_KEY); a_pub=${a_pub#0x}
  [[ "$a_pub" =~ ^[0-9a-fA-F]{64}$ ]] || failf "$VOL_LIVE ARCUS_API_KEY is not 32-byte hex"
  a_priv=$(kv_get "$VOL_LIVE" ARCUS_API_PRIVATE_KEY)
  is_cipher64 "$a_priv" || failf "$VOL_LIVE ARCUS_API_PRIVATE_KEY is not ciphertext-shaped (run arcus_vol_encrypt_key.sh first)"
  [ -z "$(kv_get "$VOL_LIVE" ARCUS_PLAIN_API_PRIVATE_KEY)" ] || failf "$VOL_LIVE still has a plaintext key line"
  v_edk=$(kv_get "$VOL_LIVE" ENCRYPTED_DATA_KEY)
  if [ -z "$v_edk" ]; then say "live.env data key: none (uses the common one)"
  elif [ "$v_edk" = "$common_edk" ]; then say "live.env data key: same as debot_secrets_common.env"
  else failf "live.env ENCRYPTED_DATA_KEY differs from debot_secrets_common.env — its ciphertext would not decrypt in this instance"; fi
else failf "missing $VOL_LIVE (wallet 0xA2C7 credentials)"; a_pub=""; a_priv=""; fi
# The Core sub-account must be this instance's alone.
n=0
for ef in "$SCRIPTS"/*.env "$ROOT"/etc/*/*.env; do
  [ -f "$ef" ] && [ "$ef" != "$ENVF" ] || continue
  for name in $(grep -oE '^(export +)?LIGHTER_ACCOUNT_INDEX[A-Z0-9_]*=' "$ef" 2>/dev/null | sed -E 's/^export +//; s/=$//'); do
    n=$((n + 1)); [ "$(kv_get "$ef" "$name")" != "$CORE_ACCOUNT" ] || failf "Core account $CORE_ACCOUNT is already used by $ef ($name)"; done
done
say "Core account $CORE_ACCOUNT not used by another env: checked ($n entries)"
avail=$(df -Pm "$ROOT/opt" 2>/dev/null | awk 'NR==2{print $4}'); [ "${avail:-0}" -ge 200 ] || failf "less than 200 MB free on /opt"
# Owner's Lighter fragment (apply).
if [ -n "$FRAGMENT" ]; then
  if [ -f "$FRAGMENT" ]; then
    [ "$(stat -c %a "$FRAGMENT")" = 600 ] || failf "fragment must be mode 600"
    names=$(grep -vE '^[[:space:]]*(#|$)' "$FRAGMENT" | sed -E 's/^export +//; s/=.*//' | sort | tr '\n' ' ')
    [ "$names" = "LIGHTER_ACCOUNT_INDEX_CORE LIGHTER_API_KEY_INDEX_CORE LIGHTER_PRIVATE_API_KEY_CORE LIGHTER_PUBLIC_API_KEY_CORE " ] \
      || failf "fragment must hold exactly LIGHTER_{ACCOUNT_INDEX,API_KEY_INDEX,PUBLIC_API_KEY,PRIVATE_API_KEY}_CORE (has: $names)"
    [ "$(kv_get "$FRAGMENT" LIGHTER_ACCOUNT_INDEX_CORE)" = "$CORE_ACCOUNT" ] || failf "fragment LIGHTER_ACCOUNT_INDEX_CORE is not $CORE_ACCOUNT"
    l_idx=$(kv_get "$FRAGMENT" LIGHTER_API_KEY_INDEX_CORE); [[ "$l_idx" =~ ^[0-9]{1,3}$ ]] && [ "$l_idx" -le 255 ] || failf "fragment LIGHTER_API_KEY_INDEX_CORE is not 0..255"
    l_pub=$(kv_get "$FRAGMENT" LIGHTER_PUBLIC_API_KEY_CORE); is_cipher64 "$l_pub" || failf "fragment LIGHTER_PUBLIC_API_KEY_CORE is not ciphertext-shaped"
    l_priv=$(kv_get "$FRAGMENT" LIGHTER_PRIVATE_API_KEY_CORE); is_cipher64 "$l_priv" || failf "fragment LIGHTER_PRIVATE_API_KEY_CORE is not ciphertext-shaped"
  else failf "fragment $FRAGMENT missing"; fi
fi
[ $failed = 0 ] || { say "PREFLIGHT FAILED — nothing changed"; exit 1; }

# ---------------------------------------------------------------- verify
cmpfile() { local src=$1 dst=$2 mode=$3   # -> 0 identical (mode fixed in apply), 1 differs/missing
  if [ -f "$dst" ] && cmp -s "$src" "$dst"; then
    [ "$(stat -c %a "$dst")" = "$mode" ] || { d "mode $dst $(stat -c %a "$dst") != $mode"; [ $MODE = apply ] && chmod "$mode" "$dst"; }
    return 0; fi
  if [ -f "$dst" ]; then d "content $dst"; else d "missing $dst"; fi; return 1; }
env_check() {   # the installed env: pinned accounts, no placeholder, no plaintext (values never printed)
  local f=$1 bad=0
  [ "$(stat -c %a "$f")" = 600 ] || { d "mode $f != 600"; bad=1; }
  grep -qE '^export [A-Z0-9_]+="[^"]*<' "$f" && { d "$f still has a <placeholder> value"; bad=1; }
  grep -qE '^export [A-Z_]*PLAIN' "$f" && { d "$f has a plaintext key variable"; bad=1; }
  [ "$(lc "$(kv_get "$f" ARCUS_ADDRESS_ARCUS)")" = "$WALLET" ] || { d "$f ARCUS_ADDRESS_ARCUS is not $WALLET"; bad=1; }
  [ "$(kv_get "$f" LIGHTER_ACCOUNT_INDEX_CORE)" = "$CORE_ACCOUNT" ] || { d "$f LIGHTER_ACCOUNT_INDEX_CORE is not $CORE_ACCOUNT"; bad=1; }
  case "$(kv_get "$f" HEDGE_SYMBOLS)" in "$SYMBOL:"*) ;; *) d "$f HEDGE_SYMBOLS is not $SYMBOL"; bad=1 ;; esac
  for k in ARCUS_API_PRIVATE_KEY_ARCUS LIGHTER_PUBLIC_API_KEY_CORE LIGHTER_PRIVATE_API_KEY_CORE; do
    is_cipher64 "$(kv_get "$f" "$k")" || { d "$f $k is not ciphertext-shaped"; bad=1; }; done
  return $bad; }
need_launcher=0; need_unit=0
cmpfile "$HERE/debot-xvenue-hedge-arcus.sh" "$LAUNCHER" 755 || need_launcher=1
cmpfile "$HERE/$UNIT.service" "$UNIT_FILE" 644 || need_unit=1
for f in bin/xvenue_hedge_holder lib/libsigner.so; do
  if [ -f "$BASE/$f" ]; then cmp -s "$SRC_BASE/$f" "$BASE/$f" || d "$BASE/$f differs from $SRC_BASE/$f"
  else d "missing $BASE/$f"; fi; done
[ -d "$BASE/hedge" ] || d "missing $BASE/hedge"
if [ -f "$ENVF" ]; then env_check "$ENVF" || true; else d "missing $ENVF"; fi
[ "$("$SYSTEMCTL" is-enabled "$UNIT" 2>/dev/null)" = enabled ] || d "$UNIT not enabled"
if [ $MODE = verify ]; then
  [ $drift = 0 ] && { say "VERIFY OK: $UNIT host state matches the kit"; exit 0; } || { say "VERIFY: drift above"; exit 1; }
fi

# ---------------------------------------------------------------- apply
set -e
active_before=$("$SYSTEMCTL" is-active "$UNIT" 2>/dev/null || true)
src_pid=$("$SYSTEMCTL" show -p MainPID --value "$SRC_UNIT" 2>/dev/null || echo 0)
mkdir -p "$BASE/bin" "$BASE/lib" "$BASE/hedge"; chmod 755 "$BASE" "$BASE/bin" "$BASE/lib" "$BASE/hedge"
for f in bin/xvenue_hedge_holder:0755 lib/libsigner.so:0644; do p=${f%:*}; m=${f#*:}
  if [ -f "$BASE/$p" ] && ! cmp -s "$SRC_BASE/$p" "$BASE/$p"; then
    [ "$active_before" = active ] && { say "NOTE: $BASE/$p differs but $UNIT is active — left as is (restart is owner-run)"; continue; }
  fi
  cmp -s "$SRC_BASE/$p" "$BASE/$p" 2>/dev/null || { install "${OWN[@]}" -m "$m" "$SRC_BASE/$p" "$BASE/$p"; say "  installed $BASE/$p (sha256 $(sha256sum "$BASE/$p" | cut -c1-12))"; }
done
[ ! -f "$SRC_BASE/PAIRTRADE_SHA" ] || cp -p "$SRC_BASE/PAIRTRADE_SHA" "$BASE/PAIRTRADE_SHA"
[ $need_launcher = 0 ] || { install "${OWN[@]}" -m 0755 "$HERE/debot-xvenue-hedge-arcus.sh" "$LAUNCHER"; say "  installed $LAUNCHER (shared; the AAPL instance uses it at its next restart)"; }
[ $need_unit = 0 ] || { install "${OWN[@]}" -m 0644 "$HERE/$UNIT.service" "$UNIT_FILE"; say "  installed $UNIT_FILE"; }
env_live=0
if [ -f "$ENVF" ]; then
  say "  $ENVF exists — kept (keys are written once; delete it to rebuild from live.env + fragment)"
  # A kept env may already be the owner's LIVE flip: never auto-start that (a persisted armed
  # book would resume and trade); the owner restarts a live instance.
  { [ "$(kv_get "$ENVF" HEDGE_DRY_RUN)" = true ] && ! grep -qE '^export HEDGE_(ARCUS_)?LIVE_CONFIRM=' "$ENVF"; } || env_live=1
else
  tmp=$(mktemp "$SCRIPTS/.$UNIT.env.XXXXXX"); chmod 600 "$tmp"
  while IFS= read -r line || [ -n "$line" ]; do
    case "$line" in
      'export ARCUS_API_KEY_ARCUS="<'*) line="export ARCUS_API_KEY_ARCUS=\"$a_pub\"" ;;
      'export ARCUS_API_PRIVATE_KEY_ARCUS="<'*) line="export ARCUS_API_PRIVATE_KEY_ARCUS=\"$a_priv\"" ;;
      'export LIGHTER_API_KEY_INDEX_CORE="<'*) line="export LIGHTER_API_KEY_INDEX_CORE=\"$l_idx\"" ;;
      'export LIGHTER_PUBLIC_API_KEY_CORE="<'*) line="export LIGHTER_PUBLIC_API_KEY_CORE=\"$l_pub\"" ;;
      'export LIGHTER_PRIVATE_API_KEY_CORE="<'*) line="export LIGHTER_PRIVATE_API_KEY_CORE=\"$l_priv\"" ;;
    esac
    printf '%s\n' "$line" >> "$tmp"
  done < "$EXAMPLE"
  [ "$(kv_get "$tmp" HEDGE_DRY_RUN)" = true ] || { rm -f "$tmp"; failf "env.example is not DRY_RUN"; exit 1; }
  grep -qE '^export HEDGE_(ARCUS_)?LIVE_CONFIRM=' "$tmp" && { rm -f "$tmp"; failf "env.example has a live confirm token"; exit 1; }
  env_check "$tmp" || { rm -f "$tmp"; failf "built env failed its checks — nothing installed"; exit 1; }
  [ "${OWN[*]:-}" = "" ] || chown root:root "$tmp"
  mv -f "$tmp" "$ENVF"; say "  wrote $ENVF (0600; Arcus lines from live.env, Lighter lines from the fragment)"
fi
"$SYSTEMCTL" daemon-reload
"$SYSTEMCTL" enable "$UNIT" >/dev/null 2>&1 || true
if [ "$active_before" = active ]; then
  say "APPLY OK: files updated; $UNIT was already active and was not restarted (owner-run)"; exit 0
fi
if [ $env_live = 1 ]; then
  say "APPLY OK: files updated; $ENVF is a LIVE configuration (HEDGE_DRY_RUN != true or a confirm token set) and $UNIT is stopped — NOT started: a live instance is restarted by the owner only"; exit 0
fi
T0=$(date +%s)
"$SYSTEMCTL" start --no-block "$UNIT"
say "started $UNIT (DRY_RUN) at $(date -u +%H:%M:%SZ); waiting ${WAIT}s (105 s stagger + start-up)"
sleep "$WAIT"
set +e
pc=0; pfail() { pc=1; say "POSTCHECK FAIL $*"; }
"$SYSTEMCTL" is-active --quiet "$UNIT" && say "$UNIT: active" || pfail "$UNIT is not active"
J=$("$JOURNALCTL" -u "$UNIT" --since "@$T0" --no-pager 2>/dev/null)
printf '%s\n' "$J" | grep -E '\[(CONFIG|STARTUP)\]|ERROR|refusing' | tail -12 | cut -c1-300
printf '%s\n' "$J" | grep -qE "\[CONFIG\] bot=xvenue_hedge_holder dry_run=true symbols=$SYMBOL:5(\.0+)?/3(\.0+)? long=arcus:arcus short=lighter:core " \
  || pfail "no [CONFIG] line with dry_run=true symbols=$SYMBOL:5/3 long=arcus:arcus short=lighter:core"
printf '%s\n' "$J" | grep -qE '\[STARTUP\] mode=Off ' || pfail "no [STARTUP] mode=Off line"
printf '%s\n' "$J" | grep -qE '\[ERROR\]|refusing to start|init (long|short)-leg' && pfail "errors in the journal"
python3 - "$BASE/hedge" "$SYMBOL" <<'PY' || pfail "status/events check"
import json, os, sys
d, sym = sys.argv[1], sys.argv[2]
s = json.load(open(os.path.join(d, "status.json")))
h = s["hedge_holder"]; legs = h.get("legs") or {}; b = (h.get("books") or {}).get(sym) or {}
print("status", s.get("updated_at"), "dry_run", s.get("dry_run"), "mode", h.get("mode"), "halted", h.get("halted"),
      "long", (legs.get("long") or {}).get("exchange"), "short", (legs.get("short") or {}).get("exchange"), sym, b.get("mode"))
bad = []
if s.get("dry_run") is not True: bad.append("dry_run is not true")
if h.get("mode") != "Off" or (b and b.get("mode") != "Off"): bad.append("mode is not Off")
if h.get("halted"): bad.append("halted")
if (legs.get("long") or {}).get("exchange") != "arcus" or (legs.get("short") or {}).get("exchange") != "lighter": bad.append("leg exchanges")
ev = os.path.join(d, "events.jsonl")
if os.path.exists(ev):
    kinds = [json.loads(l).get("event") for l in open(ev) if l.strip()]
    if any(k in ("fill", "arm", "halt", "uncertain") for k in kinds): bad.append("order/arm/halt events: %s" % kinds)
if bad: print("  ", bad); sys.exit(1)
PY
[ "$("$SYSTEMCTL" show -p MainPID --value "$SRC_UNIT" 2>/dev/null || echo 0)" = "$src_pid" ] && say "$SRC_UNIT untouched (pid $src_pid)" || pfail "$SRC_UNIT PID CHANGED"
if [ $pc != 0 ]; then
  "$SYSTEMCTL" stop "$UNIT"; say "POSTCHECK FAILED: $UNIT stopped again (DRY_RUN, mode Off — nothing was traded). Files left in place."; exit 1
fi
say "INSTALL OK: $UNIT running in DRY_RUN, mode Off, no orders. Next: the owner-run ARM script (live flip + 'NVDA 20000')."
