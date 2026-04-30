#!/bin/bash
# bot-strategy#70 Phase A: fee-resilient parameter grid for pairtrade BTC/ETH.
# Sweeps 5 × 5 × 2 × 2 = 100 cells across:
#   ENTRY_Z_SCORE_BASE   : 1.5 2.0 2.5 3.0 3.5
#   FORCE_CLOSE_TIME_SECS: 3600 5400 7200 10800 14400
#   EXIT_Z_SCORE         : 0.2 0.3
#   STOP_LOSS_Z_SCORE    : 4.0 6.0
#
# Uses the pre-converted BT bin file from bt_live_data.sh (single shared input,
# env-overridden params per cell). Runs -j N cells in parallel.
#
# Prereqs:
#   1. Run scripts/bt_live_data.sh once to populate /tmp/bt_live_data/live.bin
#   2. configs/pairtrade/debot-pair-btceth-grid.yaml present (single-strategy,
#      MTF on, 4 grid dims env-overridable)
#   3. BT_WARM_START_SNAPSHOT path set (v2 snapshot)
#
# Outputs:
#   /tmp/phase_a_grid/cell_NNN.log       — per-cell BT log
#   /tmp/phase_a_grid/results.csv        — one row per cell with metrics
set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

BINARY="$REPO_ROOT/target/release/debot"
CONFIG="$REPO_ROOT/configs/pairtrade/debot-pair-btceth-grid.yaml"
BT_BIN="${BT_BIN:-/tmp/bt_live_data/live.bin}"
SNAPSHOT="${BT_WARM_START_SNAPSHOT:-/tmp/bt_phase_a/pairtrade_history_BTC_ETH.json}"
OUT_DIR="${OUT_DIR:-/tmp/phase_a_grid}"
PARALLEL_JOBS="${PARALLEL_JOBS:-4}"
ANALYZER="$SCRIPT_DIR/log_analyzer.py"

for req in "$BINARY" "$CONFIG" "$BT_BIN" "$SNAPSHOT"; do
  [ -e "$req" ] || { echo "ERROR: missing $req" >&2; exit 1; }
done

mkdir -p "$OUT_DIR"
RESULTS_CSV="$OUT_DIR/results.csv"
echo "cell,entry_z,fc_secs,exit_z,sl_z,pnl_0bp,pnl_5bp,trades,win_rate_5bp,sharpe_5bp,maxdd_5bp,calmar_5bp,fc_rate" > "$RESULTS_CSV"

ENTRY_Z_VALUES="1.5 2.0 2.5 3.0 3.5"
FC_VALUES="3600 5400 7200 10800 14400"
EXIT_Z_VALUES="0.2 0.3"
SL_Z_VALUES="4.0 6.0"

cell_idx=0
CELLS_FILE="$OUT_DIR/cells.txt"
: > "$CELLS_FILE"
for ez in $ENTRY_Z_VALUES; do
  for fc in $FC_VALUES; do
    for xz in $EXIT_Z_VALUES; do
      for sl in $SL_Z_VALUES; do
        cell_idx=$((cell_idx+1))
        printf "%03d %s %s %s %s\n" "$cell_idx" "$ez" "$fc" "$xz" "$sl" >> "$CELLS_FILE"
      done
    done
  done
done
TOTAL_CELLS=$cell_idx
echo "=== Phase A grid: $TOTAL_CELLS cells, $PARALLEL_JOBS-way parallel ==="
echo "Data: $BT_BIN  Snapshot: $SNAPSHOT"
echo "Output: $OUT_DIR/"
echo ""

run_cell() {
  local tag="$1" ez="$2" fc="$3" xz="$4" sl="$5"
  local log="$OUT_DIR/cell_${tag}.log"
  PAIRTRADE_CONFIG_PATH="$CONFIG" \
    BACKTEST_MODE=true BACKTEST_FILE="$BT_BIN" DRY_RUN=true ENABLE_DATA_DUMP=false \
    RUST_LOG="warn,debot::pairtrade=info" UNIVERSE_PAIRS="BTC/ETH" \
    BT_WARM_START_SNAPSHOT="$SNAPSHOT" \
    ENTRY_Z_SCORE_BASE="$ez" EXIT_Z_SCORE="$xz" \
    STOP_LOSS_Z_SCORE="$sl" FORCE_CLOSE_TIME_SECS="$fc" \
    "$BINARY" > "$log" 2>&1 || true

  python3 -c "
import sys
sys.path.insert(0, '$SCRIPT_DIR')
from log_analyzer import calculate_pnl, compute_max_drawdown, compute_sharpe
log = '$log'
p0, tp0, tr0, hs0 = calculate_pnl(log, None, None, 0.0, 0.0)
p5, tp5, tr5, hs5 = calculate_pnl(log, None, None, 5.0, 0.0)
n = len(tp5)
w5 = sum(1 for p in tp5 if p > 0)
dd5 = compute_max_drawdown(tp5) if tp5 else 0.0
sh5 = compute_sharpe(tp5) if tp5 else 0.0
cm5 = (float(p5)/dd5) if dd5 > 0 else 0.0
# FC rate: grep force_close from log's exit lines (rough, BT log format)
import subprocess
fc_ct = int(subprocess.check_output(
    ['bash', '-c', f\"grep -cE 'reason.*force_close|force_close_time' {log} || echo 0\"],
    stderr=subprocess.DEVNULL).decode().strip().split()[0])
fc_rate = (fc_ct / n) if n > 0 else 0.0
print(f'${tag},${ez},${fc},${xz},${sl},{float(p0):.2f},{float(p5):.2f},{n},{(w5/n*100 if n>0 else 0):.1f},{sh5:.3f},{dd5:.2f},{cm5:.3f},{fc_rate:.3f}')
" >> "$RESULTS_CSV"

  echo "[done] cell_${tag}  ez=$ez fc=$fc xz=$xz sl=$sl"
}
export -f run_cell
export OUT_DIR CONFIG BT_BIN SNAPSHOT BINARY SCRIPT_DIR RESULTS_CSV

START=$(date +%s)
xargs -P "$PARALLEL_JOBS" -n 5 -a "$CELLS_FILE" bash -c 'run_cell "$@"' _
ELAPSED=$(($(date +%s) - START))
echo ""
echo "=== Grid complete: ${TOTAL_CELLS} cells in ${ELAPSED}s ==="
echo "Results: $RESULTS_CSV"
