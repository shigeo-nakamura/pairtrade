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
#      06-15. This is the canonical "deployed, not yet loaded" signal — but it
#      is CORROBORATED, not standalone: a newer mtime is only escalated to drift
#      when content identity cannot be confirmed (metrics down / boot-sha gauge
#      absent → fail closed) or the on-disk sha differs from the boot sha. When
#      the boot sha matches the on-disk sha the file was merely re-deployed with
#      byte-identical content (CI re-runs Deploy Configs on unrelated master
#      pushes and re-syncs every YAML, bumping mtime without a content change —
#      bot-strategy#608), which is benign and would otherwise nag daily until a
#      pointless restart. The real #491 failure (different content not loaded)
#      still trips checks 2 & 3, so this loses no coverage.
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
# --round-json loads the expected round config from a committed file
# (configs/pairtrade/round.json) and asserts the running effective gauges match
# EVERY field it declares (per-variant force_close / exit_z / stop_loss_z /
# frozen_beta / equity_reference_usd / max_leverage / sizing_beta_floor /
# exit_on_sizing_beta_floor, process-wide ineligible-close and eligibility-grace
# settings, plus a top-level max_leverage fallback for
# variants that don't declare their own — the blocking preflight for the
# round-eval harness (#3 in bot-strategy#580). A
# field absent from round.json is skipped; a declared field with no matching
# gauge is reported as drift. When --round-json is used, its variant set is
# exact: an unexpected running variant is also drift. Requires python3.
#
# max_leverage is per-variant since bot-strategy#814 (pairtrade StrategyConfig
# now resolves it per strategy, not from a single process-wide scalar): a
# variant's `variants.<id>.max_leverage` in round.json wins, otherwise the
# top-level `max_leverage` applies to every variant — same resolution order
# as the running process (YAML strategies[].max_leverage / MAX_LEVERAGE_<ID>
# env, else the top-level YAML value).
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
declare -A EXPECT_SLZ=()
declare -A EXPECT_FROZEN=()
declare -A EXPECT_EQUITY=()
declare -A EXPECT_MAXLEV_VARIANT=()
declare -A EXPECT_SIZING_FLOOR=()
declare -A EXPECT_EXIT_ON_FLOOR=()
EXPECT_MAXLEV=""
EXPECT_INELIG_CAP=""
EXPECT_INELIG_SPREAD=""
EXPECT_INELIG_STALE=""
EXPECT_ELIG_GRACE=""
EXPECT_ELIG_BETA_EXIT=""
EXPECT_EXACT_VARIANTS=0

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
  EXPECT_EXACT_VARIANTS=1
  if [ ! -f "$ROUND_JSON" ]; then echo "round file not found: $ROUND_JSON" >&2; exit 1; fi
  if ! command -v python3 >/dev/null; then echo "--round-json needs python3" >&2; exit 1; fi
  # Assert EVERY field committed in round.json (#580 review): a round that
  # changes only one of these must not silently pass the preflight. An empty
  # cell ('') means "not declared for this variant" → that field is skipped.
  #
  # Field separator is '|', not a tab: bash's `read` treats a run of IFS
  # *whitespace* characters (which includes tab, regardless of what IFS is
  # currently set to) as a single delimiter, so an empty cell adjacent to a
  # tab silently swallowed it and shifted every field after it left by one
  # (bot-strategy#814 review — caught because it broke this PR's new
  # `max_leverage` cell whenever `use_frozen_beta_exit_z` was undeclared, the
  # common case; the pre-existing `equity_reference_usd` cell had the same
  # bug). '|' is not IFS whitespace, so adjacent delimiters correctly yield
  # an empty field instead of collapsing.
  while IFS='|' read -r kind v fc ez slz fz eq mlev sbf eobf; do
    if [ "$kind" = "maxlev" ]; then EXPECT_MAXLEV="$v"; continue; fi
    if [ "$kind" = "ineligcap" ]; then EXPECT_INELIG_CAP="$v"; continue; fi
    if [ "$kind" = "ineligspread" ]; then EXPECT_INELIG_SPREAD="$v"; continue; fi
    if [ "$kind" = "ineligstale" ]; then EXPECT_INELIG_STALE="$v"; continue; fi
    if [ "$kind" = "eliggrace" ]; then EXPECT_ELIG_GRACE="$v"; continue; fi
    if [ "$kind" = "eligbeta" ]; then EXPECT_ELIG_BETA_EXIT="$v"; continue; fi
    EXPECT_FC["$v"]="$fc"; EXPECT_EXITZ["$v"]="$ez"; EXPECT_SLZ["$v"]="$slz"
    EXPECT_FROZEN["$v"]="$fz"; EXPECT_EQUITY["$v"]="$eq"
    [ -n "$mlev" ] && EXPECT_MAXLEV_VARIANT["$v"]="$mlev"
    [ -n "$sbf" ] && EXPECT_SIZING_FLOOR["$v"]="$sbf"
    [ -n "$eobf" ] && EXPECT_EXIT_ON_FLOOR["$v"]="$eobf"
  done < <(python3 - "$ROUND_JSON" <<'PY'
import json, sys
d = json.load(open(sys.argv[1]))
def cell(p, k):
    v = p.get(k)
    return "" if v is None else v
if d.get("max_leverage") is not None:
    print(f'maxlev|{d["max_leverage"]}|||||')
if d.get("ineligible_close_defer_cap_secs") is not None:
    print(f'ineligcap|{d["ineligible_close_defer_cap_secs"]}|||||')
if d.get("ineligible_close_defer_spread_bps") is not None:
    print(f'ineligspread|{d["ineligible_close_defer_spread_bps"]}|||||')
if d.get("ineligible_close_defer_stale_secs") is not None:
    print(f'ineligstale|{d["ineligible_close_defer_stale_secs"]}|||||')
if d.get("eligibility_margin_grace_secs") is not None:
    print(f'eliggrace|{d["eligibility_margin_grace_secs"]}|||||')
if d.get("eligibility_beta_gap_exit") is not None:
    print(f'eligbeta|{d["eligibility_beta_gap_exit"]}|||||')
for v, p in d.get("variants", {}).items():
    fz = "" if p.get("use_frozen_beta_exit_z") is None else (1 if p["use_frozen_beta_exit_z"] else 0)
    eobf = "" if p.get("exit_on_sizing_beta_floor") is None else (1 if p["exit_on_sizing_beta_floor"] else 0)
    print(f'var|{v.lower()}|{cell(p,"force_close_secs")}|{cell(p,"exit_z")}|'
          f'{cell(p,"stop_loss_z")}|{fz}|{cell(p,"equity_reference_usd")}|'
          f'{cell(p,"max_leverage")}|{cell(p,"sizing_beta_floor")}|{eobf}')
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
# Defer the verdict: a newer mtime is only "deployed but not loaded" when the
# deployed CONTENT differs from what the process booted with. Resolved AFTER the
# metrics block, once the on-disk vs boot sha is known (see "resolve check 1").
mtime_newer=0
if [ "$start_epoch" -gt 0 ] && [ "$file_mtime" -gt "$start_epoch" ]; then
  mtime_newer=1
fi

# --- Metrics-backed checks (2 + 3) -------------------------------------------
# Did the caller request an assertion that can ONLY be verified via /metrics?
# If so, an unreachable endpoint must FAIL CLOSED (drift) — otherwise a bad/old
# process, an old binary without the new gauges, or a disabled exporter would
# silently pass this blocking preflight without checking anything.
WANT_METRICS_ASSERT=""
if [ -n "$ROUND_JSON" ] || [ -n "$EXPECT_FP" ] || [ "${#EXPECT_FC[@]}" -gt 0 ]; then
  WANT_METRICS_ASSERT=1
fi

# Default empty so the "resolve check 1" block below is safe under `set -u`
# when metrics are unavailable (the else-branch never assigns these).
disk_sha=""
running_sha=""
metrics=""
if [ -n "$METRICS_URL" ]; then
  metrics=$(curl -s --max-time 6 "$METRICS_URL" 2>/dev/null || true)
fi
if [ -z "$metrics" ]; then
  if [ -n "$WANT_METRICS_ASSERT" ]; then
    note_drift "metrics unavailable ($METRICS_URL) but a config assertion (--round-json / --expect-fp / --expect-fc) was requested — cannot verify the running config against the round. Failing closed (old binary without the new gauges, disabled exporter, or down process all land here)."
  else
    say "metrics      : unavailable ($METRICS_URL) — skipping sha + round-assertion checks (none requested)"
  fi
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
  # Numeric-equality assertion: drift unless `want` is empty (not declared) or
  # the gauge is absent. Compares numerically so "4" == "4.0" etc.
  assert_num() { # $1=variant $2=field-name $3=want $4=got
    [ -z "$3" ] && return 0
    if [ -z "$4" ]; then note_drift "variant $1 $2 gauge absent — cannot verify against expected $3 (round config)."; return; fi
    [ "$(awk -v a="$4" -v b="$3" 'BEGIN{print (a==b)?1:0}')" != "1" ] \
      && note_drift "variant $1 effective $2=$4 ≠ expected $3 (round config)."
  }

  # Check 3: round assertion. Iterate over the UNION of variants declared in
  # round.json and variants observed in /metrics — driving off observed alone
  # would silently skip an entire EXPECTED variant that the running process
  # dropped/renamed (a config-not-loaded symptom). A declared variant with no
  # gauges is drift; every committed field is asserted for the rest.
  declared_variants=$(printf '%s\n' "${!EXPECT_FC[@]}" "${!EXPECT_EXITZ[@]}" \
    "${!EXPECT_SLZ[@]}" "${!EXPECT_FROZEN[@]}" "${!EXPECT_EQUITY[@]}" \
    "${!EXPECT_MAXLEV_VARIANT[@]}" "${!EXPECT_SIZING_FLOOR[@]}" \
    "${!EXPECT_EXIT_ON_FLOOR[@]}" | sort -u | grep -v '^$' || true)
  observed_variants=$(echo "$metrics" | grep '^pairtrade_effective_force_close_secs{' \
    | sed -n 's/.*variant="\([^"]*\)".*/\1/p' | sort -u)
  all_variants=$(printf '%s\n%s\n' "$declared_variants" "$observed_variants" | sort -u | grep -v '^$' || true)
  is_declared() { printf '%s\n' "$declared_variants" | grep -qx "$1"; }
  for variant in $all_variants; do
    fc_raw=$(gauge_for pairtrade_effective_force_close_secs "$variant")
    if [ "$EXPECT_EXACT_VARIANTS" -eq 1 ] && ! is_declared "$variant"; then
      note_drift "unexpected running variant $variant is absent from round.json — stale or wrong config loaded."
      continue
    fi
    if [ -z "$fc_raw" ]; then
      if is_declared "$variant"; then
        note_drift "expected variant $variant (round.json) is ABSENT from running metrics — config not loaded, or the variant was dropped/renamed."
      fi
      continue
    fi
    fc_int=${fc_raw%.*}
    ez=$(gauge_for pairtrade_effective_exit_z "$variant")
    slz=$(gauge_for pairtrade_effective_stop_loss_z "$variant")
    fz=$(gauge_for pairtrade_effective_frozen_beta_exit_z "$variant"); fz=${fz%.*}
    eq=$(gauge_for pairtrade_equity_reference_usd "$variant")
    mlev=$(gauge_for pairtrade_max_leverage_config "$variant")
    inelig_cap=$(gauge_for pairtrade_effective_ineligible_close_defer_cap_secs "$variant")
    inelig_cap=${inelig_cap%.*}
    inelig_spread=$(gauge_for pairtrade_effective_ineligible_close_defer_spread_bps "$variant")
    inelig_stale=$(gauge_for pairtrade_effective_ineligible_close_defer_stale_secs "$variant")
    inelig_stale=${inelig_stale%.*}
    elig_grace=$(gauge_for pairtrade_effective_eligibility_margin_grace_secs "$variant")
    elig_grace=${elig_grace%.*}
    elig_beta_exit=$(gauge_for pairtrade_effective_eligibility_beta_gap_exit "$variant")
    sizing_floor=$(gauge_for pairtrade_effective_sizing_beta_floor "$variant")
    exit_on_floor=$(gauge_for pairtrade_effective_exit_on_sizing_beta_floor "$variant"); exit_on_floor=${exit_on_floor%.*}
    say "variant $variant   : force_close=${fc_int}s exit_z=${ez:-?} stop_loss_z=${slz:-?} frozen_beta=${fz:-?} equity=${eq:-?} max_leverage=${mlev:-?} inelig_defer_cap=${inelig_cap:-?} inelig_defer_spread=${inelig_spread:-?} inelig_defer_stale=${inelig_stale:-?} elig_grace=${elig_grace:-?} elig_beta_exit=${elig_beta_exit:-?} sizing_beta_floor=${sizing_floor:-?} exit_on_sizing_beta_floor=${exit_on_floor:-?}"
    assert_num "$variant" force_close "${EXPECT_FC[$variant]:-}" "$fc_int"
    assert_num "$variant" exit_z "${EXPECT_EXITZ[$variant]:-}" "$ez"
    assert_num "$variant" stop_loss_z "${EXPECT_SLZ[$variant]:-}" "$slz"
    assert_num "$variant" equity_reference_usd "${EXPECT_EQUITY[$variant]:-}" "$eq"
    assert_num "$variant" sizing_beta_floor "${EXPECT_SIZING_FLOOR[$variant]:-}" "$sizing_floor"
    assert_num "$variant" exit_on_sizing_beta_floor "${EXPECT_EXIT_ON_FLOOR[$variant]:-}" "$exit_on_floor"
    # Per-variant max_leverage (bot-strategy#814) wins when round.json declares
    # one for this variant; otherwise fall back to the top-level max_leverage,
    # matching the running process's own YAML/env override -> top-level order.
    want_mlev="${EXPECT_MAXLEV_VARIANT[$variant]:-$EXPECT_MAXLEV}"
    assert_num "$variant" max_leverage "$want_mlev" "$mlev"
    assert_num "$variant" ineligible_close_defer_cap_secs "$EXPECT_INELIG_CAP" "$inelig_cap"
    assert_num "$variant" ineligible_close_defer_spread_bps "$EXPECT_INELIG_SPREAD" "$inelig_spread"
    assert_num "$variant" ineligible_close_defer_stale_secs "$EXPECT_INELIG_STALE" "$inelig_stale"
    assert_num "$variant" eligibility_margin_grace_secs "$EXPECT_ELIG_GRACE" "$elig_grace"
    assert_num "$variant" eligibility_beta_gap_exit "$EXPECT_ELIG_BETA_EXIT" "$elig_beta_exit"
    want_fz="${EXPECT_FROZEN[$variant]:-}"
    if [ -n "$want_fz" ] && [ -n "$fz" ] && [ "$fz" != "$want_fz" ]; then
      note_drift "variant $variant frozen_beta_exit_z=${fz} ≠ expected ${want_fz} (round config)."
    fi
  done

  if [ -n "$EXPECT_FP" ]; then
    matched=0
    seen=0
    while IFS= read -r line; do
      seen=$((seen + 1))
      fp=$(echo "$line" | sed -n 's/.*[,{]fp="\([^"]*\)".*/\1/p')
      variant=$(echo "$line" | sed -n 's/.*variant="\([^"]*\)".*/\1/p')
      if [ "$fp" = "$EXPECT_FP" ]; then matched=1; else
        note_drift "variant $variant fingerprint $fp ≠ expected $EXPECT_FP."
      fi
    done < <(echo "$metrics" | grep '^pairtrade_config_fingerprint{')
    # Fail closed if the series is absent entirely (reachable endpoint but old
    # binary / wrong process / 404 / exporter missing the gauge): an --expect-fp
    # preflight that observes zero fingerprints has verified nothing.
    if [ "$seen" -eq 0 ]; then
      note_drift "EXPECT_FP set but no pairtrade_config_fingerprint series in /metrics — old binary, wrong process, or exporter missing the gauge; cannot verify the running config."
    elif [ "$matched" -eq 1 ]; then
      say "fingerprint  : at least one variant matches expected $EXPECT_FP"
    fi
  fi
fi

# --- Resolve check 1 (mtime), now that the on-disk vs boot sha is known -------
# A config file newer than the running process only means "deployed but not
# loaded" if the deployed CONTENT actually differs from what the process booted
# with. CI re-runs Deploy Configs on unrelated master pushes, re-syncing every
# YAML and bumping mtime even when the bytes are identical (bot-strategy#608) —
# a benign re-deploy, not drift, and clearing it would need a pointless restart
# that force-closes live positions. So escalate a newer mtime to drift ONLY when
# content identity cannot be confirmed: boot sha matches on-disk sha → benign;
# otherwise (metrics down / boot-sha gauge absent → fail closed, or sha differs)
# → drift. The real #491 failure (different content not loaded) makes the boot
# sha differ, so check 2 also fires and this still escalates — no lost coverage.
if [ "$mtime_newer" -eq 1 ]; then
  if [ -n "$running_sha" ] && [ "$running_sha" = "$disk_sha" ]; then
    say "mtime note   : config file is newer than process start, but on-disk sha == boot sha ($disk_sha) — benign re-deploy of byte-identical content, not drift (bot-strategy#608)."
  else
    note_drift "config file mtime is newer than the running process start and content identity could not be confirmed (boot sha ${running_sha:-<absent>} vs on-disk ${disk_sha:-<unknown>}) — deployed but NOT loaded. Restart $SERVICE to apply, then re-check the [CONFIG] fingerprint."
  fi
fi

if [ "$DRIFT" -eq 1 ]; then
  echo "RESULT: config drift detected for $SERVICE" >&2
  exit 2
fi
say "RESULT: no config drift for $SERVICE"
exit 0
