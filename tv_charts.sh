#!/usr/bin/env bash
# =============================================================
# Purpose:      Start the tv_charts app and (on a desktop) open the two things
#               you use with it: the chart page in a browser, and a terminal
#               running Claude Code already linked to the MCP server.
# Runs:         ./tv_charts.sh            (start + open browser + Claude terminal)
#               ./tv_charts.sh --no-open  (just start the server; no windows)
#               TVC_NO_OPEN=1 ./tv_charts.sh   (same as --no-open)
# Outputs:      Serves http://localhost:8800  (UI at /, MCP at /mcp)
# Notes:        GUI windows are skipped automatically with no display (e.g. SSH).
# =============================================================
set -euo pipefail

# Always operate from the project directory (the folder this script lives in).
DIR="$(dirname "$(readlink -f "$0")")"
cd "$DIR"

# --- options -----------------------------------------------------------
OPEN=1
[ "${TVC_NO_OPEN:-}" = "1" ] && OPEN=0
for arg in "$@"; do
  case "$arg" in
    --no-open) OPEN=0 ;;
    -h|--help) grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
  esac
done

# Port comes from the Python config so the two never drift.
PORT="$(grep -oP '^PORT\s*=\s*\K[0-9]+' config_tv_charts.py 2>/dev/null || echo 8800)"
URL="http://localhost:${PORT}"

# --- first-run bootstrap ----------------------------------------------
if [ ! -d ".venv" ]; then
  echo "[tv_charts] creating virtualenv..."
  python3 -m venv .venv
  ./.venv/bin/python -m pip install --quiet --upgrade pip
  ./.venv/bin/python -m pip install --quiet -r requirements.txt
fi

# --- helpers -----------------------------------------------------------
have_display() { [ -n "${DISPLAY:-}" ] || [ -n "${WAYLAND_DISPLAY:-}" ]; }

server_ready() { curl -fsS -o /dev/null "$URL/" 2>/dev/null; }

open_browser() {
  echo "[tv_charts] opening chart page: $URL/"
  xdg-open "$URL/" >/dev/null 2>&1 &
}

register_mcp() {
  command -v claude >/dev/null 2>&1 || return 0
  if ! claude mcp get tv_charts >/dev/null 2>&1; then
    if claude mcp add --transport http tv_charts "$URL/mcp" >/dev/null 2>&1; then
      echo "[tv_charts] registered MCP server 'tv_charts' -> $URL/mcp"
    fi
  fi
}

open_claude_terminal() {
  if ! command -v claude >/dev/null 2>&1; then
    echo "[tv_charts] claude not found on PATH; skipping Claude terminal."
    echo "            Link it manually: claude mcp add --transport http tv_charts $URL/mcp"
    return 0
  fi
  register_mcp
  echo "[tv_charts] opening Claude terminal (linked to $URL/mcp)"
  # Keep the terminal open after claude exits (exec bash) so you can re-run it.
  local run="claude; exec bash"
  if command -v gnome-terminal >/dev/null 2>&1; then
    gnome-terminal --working-directory="$DIR" -- bash -lc "$run" >/dev/null 2>&1 &
  elif command -v kitty >/dev/null 2>&1; then
    kitty --directory "$DIR" bash -lc "$run" >/dev/null 2>&1 &
  elif command -v x-terminal-emulator >/dev/null 2>&1; then
    ( cd "$DIR" && x-terminal-emulator -e bash -lc "$run" ) >/dev/null 2>&1 &
  else
    echo "[tv_charts] no known terminal emulator; run 'claude' yourself in $DIR"
  fi
}

open_windows() {
  if [ "$OPEN" != 1 ]; then return 0; fi
  if ! have_display; then
    echo "[tv_charts] no display detected; not opening windows. UI: $URL/  MCP: $URL/mcp"
    return 0
  fi
  open_browser
  open_claude_terminal
}

# --- already running? just open the windows and exit -------------------
if server_ready; then
  echo "[tv_charts] already running on :$PORT — opening windows only"
  open_windows
  exit 0
fi

# --- start the server, wait for it, then open windows ------------------
echo "[tv_charts] starting on $URL  (UI: /  |  MCP: /mcp)"
./.venv/bin/python main_tv_charts.py &
SERVER_PID=$!

# Stop the server when this script exits / is Ctrl-C'd.
cleanup() { kill "$SERVER_PID" 2>/dev/null || true; }
trap cleanup EXIT INT TERM

# Wait up to ~15s for the server to come up (it fetches history on boot).
for _ in $(seq 1 30); do
  if server_ready; then break; fi
  # bail early if the server process died during startup
  kill -0 "$SERVER_PID" 2>/dev/null || { echo "[tv_charts] server exited during startup"; wait "$SERVER_PID"; exit 1; }
  sleep 0.5
done

open_windows

# Keep the server in the foreground (Ctrl-C here stops everything).
wait "$SERVER_PID"
