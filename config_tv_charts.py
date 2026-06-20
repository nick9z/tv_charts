# =============================================================
# Purpose:      Central configuration for tv_charts (the single source of
#               truth for hosts, market, timeframes, layout defaults, etc.).
# Runs:         Imported once at process start by main_tv_charts.py and
#               functions_tv_charts.py. No side effects.
# Inputs:       None (static module-level constants).
# Outputs:      Module-level constants consumed across the app.
# Dependencies: stdlib only.
# Risks:        Bybit interval codes are source-specific (see TF_TO_INTERVAL):
#               Bybit v5 uses bare D/W/M (monthly is "M", NOT "1M" -- "1M"
#               is the Binance convention and returns an empty list on Bybit).
#               Verified empirically against api.bybit.com on 2026-06-20.
# =============================================================

# ---- Network / process -------------------------------------------------
HOST = "0.0.0.0"          # bind on all interfaces for LAN access
PORT = 8800

# ---- Market (Bybit v5, USDT perpetuals) --------------------------------
CATEGORY = "linear"                                  # USDT perpetual contracts
BYBIT_REST = "https://api.bybit.com"
BYBIT_WS = "wss://stream.bybit.com/v5/public/linear"

# Tradable assets (internal api symbols). Display label adds a ".P" suffix
# (TradingView perp notation) -- see functions_tv_charts.display_label().
ASSETS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "AVAXUSDT"]
DEFAULT_ASSET = "BTCUSDT"

# ---- Timeframes --------------------------------------------------------
# Selector order is ascending (TradingView style).
TIMEFRAMES = ["15m", "1H", "4H", "D", "W", "M"]

# UI label -> Bybit v5 kline interval code.
# NOTE: Bybit uses bare letters for D/W/M. Monthly is "M" (verified live);
# "1M" would be Binance's code and returns no data on Bybit.
TF_TO_INTERVAL = {"15m": "15", "1H": "60", "4H": "240", "D": "D", "W": "W", "M": "M"}
# Reverse map (Bybit interval -> UI label) for decoding WS kline topics.
INTERVAL_TO_TF = {v: k for k, v in TF_TO_INTERVAL.items()}

# ---- Layout defaults ---------------------------------------------------
DEFAULT_LAYOUT = 4                                   # 2x2 grid
# Default timeframe assignment per layout (order = TL, TR, BL, BR).
DEFAULT_TF = {1: ["D"], 2: ["D", "4H"], 4: ["D", "4H", "1H", "15m"]}

# ---- Data / indicators -------------------------------------------------
HISTORY_BARS = 1000                                  # bars per tf on (re)load
VP_BINS = 50                                          # volume-profile buckets
VP_VALUE_AREA = 0.70                                  # 70% value area band
# Default lookback for VWAP anchor / Volume Profile range when the AI or user
# does not specify one: start this many BARS before the latest loaded bar, on
# whatever timeframe the chart is on (so the window is always the last N bars).
# Clamps to the first available bar if fewer than this are loaded.
DEFAULT_LOOKBACK_BARS = 14

# ---- Storage -----------------------------------------------------------
DB_PATH = "tv_charts.db"                              # wiped on every launch

# ---- Frontend pin ------------------------------------------------------
# Lightweight Charts v4.2.x (v5 changed the series-creation API).
LWC_CDN = "https://cdn.jsdelivr.net/npm/lightweight-charts@4.2.0/dist/lightweight-charts.standalone.production.js"

# ---- Overlay colors (dark theme) ---------------------------------------
EMA_COLORS = ["#f0b90b", "#ff8a3d", "#d16bff", "#36d1a6"]
SMA_COLORS = ["#4d9fff", "#9aa7ff", "#ff6b9d", "#7be0ff"]
VWAP_COLOR = "#e0e0e0"
VP_BAR_COLOR = "#5b6b8c"
VP_POC_COLOR = "#f0b90b"
VP_VALUE_COLOR = "#888888"
