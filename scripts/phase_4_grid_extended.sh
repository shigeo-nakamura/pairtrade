#!/bin/bash
# bot-strategy#123 Phase 4: parameter grid for Extended (Tokyo) pairtrade.
# Mirrors scripts/phase_a_grid.sh layout (5 × 5 × 2 × 2 = 100 cells) but
# targets Extended's fee=2.5bp microstructure with the dedicated
# debot-pair-btceth-extended-grid.yaml.
#
# Sweeps:
#   ENTRY_Z_SCORE_BASE   : 1.5 2.0 2.5 3.0 3.5
#   FORCE_CLOSE_TIME_SECS: 3600 5400 7200 10800 14400
#   EXIT_Z_SCORE         : 0.2 0.3
#   STOP_LOSS_Z_SCORE    : 4.0 6.0
#
# Prereqs:
#   1. /tmp/bt_extended_phase4/live.bin produced via convert-data
#      (cargo run --release --bin convert-data -- combined.jsonl live.bin 0)
#   2. configs/pairtrade/debot-pair-btceth-extended-grid.yaml present
#   3. BT_WARM_START_SNAPSHOT path set (Extended snapshot)
#
# Outputs:
#   /tmp/phase_4_grid_extended/cell_NNN.log
#   /tmp/phase_4_grid_extended/results.csv
#     cols: cell,entry_z,fc_secs,exit_z,sl_z,pnl_0bp,pnl_25bp,trades,
#           win_rate_25bp,sharpe_25bp,maxdd_25bp,calmar_25bp,fc_rate
set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

BINARY="$REPO_ROOT/target/release/debot"
CONFIG="$REPO_ROOT/configs/pairtrade/debot-pair-btceth-extended-grid.yaml"
BT_BIN="${BT_BIN:-/tmp/bt_extended_phase4/live.bin}"
SNAPSHOT="${BT_WARM_START_SNAPSHOT:-/tmp/bt_extended/pairtrade_history_BTC_ETH.json}"
OUT_DIR="${OUT_DIR:-/tmp/phase_4_grid_extended}"
PARALLEL_JOBS="${PARALLEL_JOBS:-4}"
ANALYZER="$SCRIPT_DIR/log_analyzer.py"

for req in "$BINARY" "$CONFIG" "$BT_BIN" "$SNAPSHOT"; do
  [ -e "$req" ] || { echo "ERROR: missing $req" >&2; exit 1; }
done

mkdir -p "$OUT_DIR"
RESULTS_CSV="$OUT_DIR/results.csv"
echo "cell,entry_z,fc_secs,exit_z,sl_z,pnl_0bp,pnl_25bp,trades,win_rate_25bp,sharpe_25bp,maxdd_25bp,calmar_25bp,fc_rate" > "$RESULTS_CSV"

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
echo "=== Phase 4 (Extended) grid: $TOTAL_CELLS cells, $PARALLEL_JOBS-way parallel ==="
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
    FEE_BPS=2.5 \
    ENTRY_Z_SCORE_BASE="$ez" EXIT_Z_SCORE="$xz" \
    STOP_LOSS_Z_SCORE="$sl" FORCE_CLOSE_TIME_SECS="$fc" \
    "$BINARY" > "$log" 2>&1 || true

  python3 -c "
import sys
sys.path.insert(0, '$SCRIPT_DIR')
from log_analyzer import calculate_pnl, compute_max_drawdown, compute_sharpe
log = '$log'
p0, tp0, tr0, hs0 = calculate_pnl(log, None, None, 0.0, 0.0)
p25, tp25, tr25, hs25 = calculate_pnl(log, None, None, 2.5, 0.0)
n = len(tp25)
w = sum(1 for p in tp25 if p > 0)
dd = compute_max_drawdown(tp25) if tp25 else 0.0
sh = compute_sharpe(tp25) if tp25 else 0.0
cm = (float(p25)/dd) if dd > 0 else 0.0
import subprocess
fc_ct = int(subprocess.check_output(
    ['bash', '-c', f\"grep -cE 'reason.*force_close|force_close_time' {log} || echo 0\"],
    stderr=subprocess.DEVNULL).decode().strip().split()[0])
fc_rate = (fc_ct / n) if n > 0 else 0.0
print(f'${tag},${ez},${fc},${xz},${sl},{float(p0):.2f},{float(p25):.2f},{n},{(w/n*100 if n>0 else 0):.1f},{sh:.3f},{dd:.2f},{cm:.3f},{fc_rate:.3f}')
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
