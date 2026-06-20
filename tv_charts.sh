#!/usr/bin/env bash
# =============================================================
# Purpose:      Start the tv_charts app. Creates the venv + installs deps on
#               first run, then launches the single uvicorn process.
# Runs:         ./tv_charts.sh   (from anywhere; it cd's to its own folder)
# Outputs:      Serves http://0.0.0.0:8800  (UI at /, MCP at /mcp)
# =============================================================
set -euo pipefail

# Always operate from the project directory (the folder this script lives in).
cd "$(dirname "$(readlink -f "$0")")"

# Create the virtualenv on first run.
if [ ! -d ".venv" ]; then
  echo "[tv_charts] creating virtualenv..."
  python3 -m venv .venv
  ./.venv/bin/python -m pip install --quiet --upgrade pip
  ./.venv/bin/python -m pip install --quiet -r requirements.txt
fi

echo "[tv_charts] starting on http://0.0.0.0:8800  (UI: /  |  MCP: /mcp)"
exec ./.venv/bin/python main_tv_charts.py
