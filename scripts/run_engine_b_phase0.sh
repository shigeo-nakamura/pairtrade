#!/bin/bash
# Minimal no-secret launcher for the isolated Phase 0 observer runtime.
set -eu

PYTHON_BIN=${ENGINE_B_PHASE0_PYTHON:-/opt/engine-b-phase0/venv/bin/python}
SCRIPT=${ENGINE_B_PHASE0_SCRIPT:-/opt/debot/scripts/engine_b_phase0.py}

if [ ! -x "$PYTHON_BIN" ]; then
  echo "Engine B Phase 0 Python runtime is missing: $PYTHON_BIN" >&2
  exit 1
fi
if [ ! -r "$SCRIPT" ]; then
  echo "Engine B Phase 0 observer is missing: $SCRIPT" >&2
  exit 1
fi

exec "$PYTHON_BIN" "$SCRIPT" "$@"
