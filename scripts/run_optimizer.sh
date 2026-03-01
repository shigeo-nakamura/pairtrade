#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." &> /dev/null && pwd)"
LOG_FILE="${REPO_ROOT}/optimizer.log"

export OPTIMIZER_CONFIG_PATH="${REPO_ROOT}/configs/pairtrade/debot00.yaml"
export DEBOT_ENV="${SCRIPT_DIR}/debot00.env"
export DATA_DUMP_FILE="${REPO_ROOT}/market_data_365d.jsonl"
export OPTIMIZER_MODE="optimize"
export OPTIMIZER_MAX_COMBOS="768"
export DEX_NAME="lighter"
export OPTIMIZER_LOG_PATH="${LOG_FILE}"

# optimizer.py manages its own log file via OPTIMIZER_LOG_PATH;
# redirect nohup output to /dev/null to avoid deleted-inode conflict.
echo "Starting optimizer in background. Log: ${LOG_FILE}"
nohup python3 -u "${SCRIPT_DIR}/optimizer.py" > /dev/null 2>&1 &
echo "PID: $!"
