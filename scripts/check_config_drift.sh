#!/bin/bash
# check_config_drift.sh — detect a pairtrade config that was deployed but never
# loaded by the running process (deploy ≠ restart), or whose running effective
# params don't match an expected round config. bot-strategy#580.
#
# Background: pairtrade CI deploys configs without restarting the service (#269,
# avoid mid-trade restarts). On 2026-06-15 (#491) the live A/B/C ran the wrong
# `force_close_time_secs` for ~8 days because a freshly-deployed Round-6 YAML was
# never loaded — and nothing anywhere signalled the mismatch. This script is the
# reusable signal: run it from a cron/healthcheck and/or as a blocking preflight
# in the round-eval harness.
#
# Three independent checks (any one firing = drift, exit 2):
#   1. mtime > start  — on-disk config file is newer than the running process's
#      boot time. Needs only systemd + stat; would have fired immediately on
#      06-15. This is the canonical "deployed, not yet loaded" signal.
#   2. file sha drift — sha256-12 of the on-disk file ≠ the sha the running
#      process recorded at boot (pairtrade_config_file_info gauge). Catches an
#      in-place edit even if mtime is ambiguous. Requires the metrics endpoint.
#   3. round assertion — running effective params / fingerprint ≠ the expected
#      round config (EXPECT_FP / EXPECT_FORCE_CLOSE_<VARIANT>). Requires metrics.
#
# Usage:
#   scripts/check_config_drift.sh [--service NAME] [--config PATH]
#                                 [--metrics URL] [--expect-fp FP]
#                                 [--expect-fc VARIANT=SECS ...]
#                                 [--round-json PATH] [--quiet]
#
# --round-json loads the expected per-variant force_close / exit_z / frozen_beta
# from a committed round file (configs/pairtrade/round.json) and asserts the
# running effective gauges match it — the blocking preflight for the round-eval
# harness (#3 in bot-strategy#580). Requires python3.
#
# Env equivalents: SERVICE, CONFIG, METRICS_URL, EXPECT_FP, EXPECT_FC_<VARIANT>.
#
# Exit: 0 = no drift, 2 = drift detected, 1 = usage/runtime error.
set -uo pipefail

SERVICE="${SERVICE:-debot-pair-btceth}"
CONFIG="${CONFIG:-/opt/debot/configs/pairtrade/debot-pair-btceth.yaml}"
METRICS_URL="${METRICS_URL:-http://127.0.0.1:9464/metrics}"
EXPECT_FP="${EXPECT_FP:-}"
QUIET=0
ROUND_JSON="${ROUND_JSON:-}"
declare -A EXPECT_FC=()
declare -A EXPECT_EXITZ=()
declare -A EXPECT_FROZEN=()

# Seed EXPECT_FC from EXPECT_FC_<VARIANT> env vars (e.g. EXPECT_FC_A=10800).
for kv in $(env | grep -E '^EXPECT_FC_[A-Za-z0-9]+=' || true); do
  k="${kv%%=*}"; v="${kv#*=}"
  EXPECT_FC["$(echo "${k#EXPECT_FC_}" | tr '[:upper:]' '[:lower:]')"]="$v"
done

while [ $# -gt 0 ]; do
  case "$1" in
    --service) SERVICE="$2"; shift 2 ;;
    --config) CONFIG="$2"; shift 2 ;;
    --metrics) METRICS_URL="$2"; shift 2 ;;
    --expect-fp) EXPECT_FP="$2"; shift 2 ;;
    --expect-fc) var="${2%%=*}"; EXPECT_FC["$(echo "$var" | tr '[:upper:]' '[:lower:]')"]="${2#*=}"; shift 2 ;;
    --round-json) ROUND_JSON="$2"; shift 2 ;;
    --quiet) QUIET=1; shift ;;
    -h|--help) sed -n '2,40p' "$0"; exit 0 ;;
    *) echo "unknown arg: $1" >&2; exit 1 ;;
  esac
done

say() { [ "$QUIET" -eq 1 ] || echo "$@"; }
DRIFT=0
note_drift() { DRIFT=1; echo "‼️  DRIFT: $*" >&2; }

# Load expected per-variant params from a committed round file (#3).
if [ -n "$ROUND_JSON" ]; then
  if [ ! -f "$ROUND_JSON" ]; then echo "round file not found: $ROUND_JSON" >&2; exit 1; fi
  if ! command -v python3 >/dev/null; then echo "--round-json needs python3" >&2; exit 1; fi
  while IFS=$'\t' read -r v fc ez fz; do
    EXPECT_FC["$v"]="$fc"; EXPECT_EXITZ["$v"]="$ez"; EXPECT_FROZEN["$v"]="$fz"
  done < <(python3 - "$ROUND_JSON" <<'PY'
import json, sys
d = json.load(open(sys.argv[1]))
for v, p in d.get("variants", {}).items():
    fz = 1 if p.get("use_frozen_beta_exit_z") else 0
    print(f'{v.lower()}\t{p.get("force_close_secs","")}\t{p.get("exit_z","")}\t{fz}')
PY
)
fi

# --- Check 1: file mtime vs process start (no metrics needed) -----------------
if [ ! -f "$CONFIG" ]; then
  echo "config file not found: $CONFIG" >&2; exit 1
fi
file_mtime=$(stat -c %Y "$CONFIG")
start_raw=$(systemctl show -p ExecMainStartTimestamp --value "$SERVICE" 2>/dev/null)
if [ -z "$start_raw" ]; then
  echo "could not read ExecMainStartTimestamp for $SERVICE (not running? not on host?)" >&2
  exit 1
fi
start_epoch=$(date -d "$start_raw" +%s 2>/dev/null || echo 0)
say "service      : $SERVICE"
say "config       : $CONFIG"
say "proc started : $start_raw ($start_epoch)"
say "config mtime : $(date -d "@$file_mtime" '+%a %Y-%m-%d %H:%M:%S %Z') ($file_mtime)"
if [ "$start_epoch" -gt 0 ] && [ "$file_mtime" -gt "$start_epoch" ]; then
  note_drift "config file mtime is newer than the running process start — deployed but NOT loaded. Restart $SERVICE to apply, then re-check the [CONFIG] fingerprint."
fi

# --- Metrics-backed checks (2 + 3) -------------------------------------------
metrics=""
if [ -n "$METRICS_URL" ]; then
  metrics=$(curl -s --max-time 6 "$METRICS_URL" 2>/dev/null || true)
fi
if [ -z "$metrics" ]; then
  say "metrics      : unavailable ($METRICS_URL) — skipping sha + round-assertion checks"
else
  disk_sha=$(sha256sum "$CONFIG" | cut -c1-12)
  running_sha=$(echo "$metrics" | grep '^pairtrade_config_file_info{' | head -1 \
    | sed -n 's/.*sha="\([^"]*\)".*/\1/p')
  say "disk sha     : $disk_sha"
  say "running sha  : ${running_sha:-<absent>}"
  # Check 2: on-disk file sha vs the sha the process fingerprinted at boot.
  if [ -n "$running_sha" ] && [ "$running_sha" != "$disk_sha" ]; then
    note_drift "on-disk config sha ($disk_sha) ≠ running process boot sha ($running_sha) — file changed since boot, not yet loaded."
  fi

  # Helper: read a single per-variant gauge value (last field) for a variant.
  gauge_for() { echo "$metrics" | grep "^$1{variant=\"$2\"" | head -1 | awk '{print $NF}'; }

  # Check 3: round assertion on effective per-variant params (force_close is the
  # field that drifted in #491; exit_z / frozen_beta asserted too when known).
  while IFS= read -r line; do
    variant=$(echo "$line" | sed -n 's/.*variant="\([^"]*\)".*/\1/p')
    fc_int=$(echo "$line" | awk '{print $NF}'); fc_int=${fc_int%.*}
    want="${EXPECT_FC[$variant]:-}"
    ez=$(gauge_for pairtrade_effective_exit_z "$variant")
    fz=$(gauge_for pairtrade_effective_frozen_beta_exit_z "$variant"); fz=${fz%.*}
    say "variant $variant   : effective force_close=${fc_int}s exit_z=${ez:-?} frozen_beta=${fz:-?}"
    if [ -n "$want" ] && [ "$fc_int" != "$want" ]; then
      note_drift "variant $variant effective force_close=${fc_int}s ≠ expected ${want}s (round config)."
    fi
    want_ez="${EXPECT_EXITZ[$variant]:-}"
    if [ -n "$want_ez" ] && [ -n "$ez" ] && [ "$(awk -v a="$ez" -v b="$want_ez" 'BEGIN{print (a==b)?1:0}')" != "1" ]; then
      note_drift "variant $variant effective exit_z=${ez} ≠ expected ${want_ez} (round config)."
    fi
    want_fz="${EXPECT_FROZEN[$variant]:-}"
    if [ -n "$want_fz" ] && [ -n "$fz" ] && [ "$fz" != "$want_fz" ]; then
      note_drift "variant $variant frozen_beta_exit_z=${fz} ≠ expected ${want_fz} (round config)."
    fi
  done < <(echo "$metrics" | grep '^pairtrade_effective_force_close_secs{')

  if [ -n "$EXPECT_FP" ]; then
    matched=0
    while IFS= read -r line; do
      fp=$(echo "$line" | sed -n 's/.*[,{]fp="\([^"]*\)".*/\1/p')
      variant=$(echo "$line" | sed -n 's/.*variant="\([^"]*\)".*/\1/p')
      if [ "$fp" = "$EXPECT_FP" ]; then matched=1; else
        note_drift "variant $variant fingerprint $fp ≠ expected $EXPECT_FP."
      fi
    done < <(echo "$metrics" | grep '^pairtrade_config_fingerprint{')
    [ "$matched" -eq 1 ] && say "fingerprint  : at least one variant matches expected $EXPECT_FP"
  fi
fi

if [ "$DRIFT" -eq 1 ]; then
  echo "RESULT: config drift detected for $SERVICE" >&2
  exit 2
fi
say "RESULT: no config drift for $SERVICE"
exit 0
