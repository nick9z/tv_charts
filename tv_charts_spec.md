# tv_charts — Build Specification (v1)

**Multi-timeframe crypto perpetual chart terminal with AI/MCP control.**

This document is the build brief for Claude Code. Read it fully, then implement v1.
Where it says *verify against the installed version*, do that before coding that part —
do not assume an API shape from memory.

---

## 0. Purpose & context

A desktop web app that shows **one** crypto perpetual at a time across **multiple timeframes**,
plus an **MCP server** so an AI client can drive the charts (add indicators, draw lines) and
**query conditions on demand** (e.g. "has the 4H closed above the 50 EMA?").

- **Runs on:** Beelink, Ubuntu 24.04, always-on. Built and launched via Claude Code on that box.
- **Accessed from:** a desktop browser on the LAN. No mobile, no HTTPS/Tailscale in v1.
- **Project directory:** `~/ai_projects/tv_charts/`
- **One process** serves everything: the web page, the browser bridge, and the MCP endpoint.

---

## 1. Tech stack

- **Python 3.11+**, functional style (see §11 for conventions).
- **FastAPI + uvicorn** — single ASGI process.
- **MCP**: the official Python `mcp` SDK (FastMCP), **streamable HTTP** transport, mounted into the FastAPI app.
- **SQLite** via stdlib `sqlite3` (ephemeral, wiped each launch).
- **httpx** (async) — Bybit REST history.
- **websockets** — Bybit public WS live stream (client). The browser bridge uses FastAPI's native WebSocket.
- **numpy** — indicator math.
- **Lightweight Charts v4.x** (pin, see §8) — vanilla JS frontend, no framework.

`requirements.txt`:
```
fastapi
uvicorn[standard]
httpx
websockets
mcp
numpy
```
Pin versions as you see fit. **Verify the `mcp` package's current streamable-HTTP mounting + lifespan API** before wiring §9.1.

---

## 2. Architecture

```
AI client  --MCP (streamable HTTP /mcp)-->  tv_charts (one uvicorn process)  --WS /ws-->  Browser (chart grid)
                                                   |
                                                   |  REST history + WS live
                                                   v
                                            Bybit v5 (category=linear)
```

- The **server is the brain**: it holds the candle data (SQLite), computes all indicators, evaluates all
  conditions, and owns the **scene state**. The browser is a renderer that receives commands over `/ws`.
- The **AI never touches `/ws`** — it calls **MCP tools** at `/mcp`. Tool implementations mutate the scene,
  broadcast draw/state messages to browser clients, and read the DB for condition checks.
- **On startup:** wipe + create the DB, REST-fetch history for the default asset across the default
  timeframes, subscribe to the Bybit WS for live closed-candle updates.

---

## 3. Data layer

### 3.1 Symbols & categories (IMPORTANT)

All market data uses **`category=linear`** (USDT perpetuals). This is the contract the user trades.

- **Display label** uses TradingView perp notation with a `.P` suffix, for cross-platform consistency:
  internal `BTCUSDT` is shown to the user as **`BTCUSDT.P`**.
- **API symbol** is the label with `.P` stripped: `display.removesuffix(".P")` → `BTCUSDT`.
- On Bybit v5, `category=linear` + `symbol=BTCUSDT` resolves to the **BTCUSDT perpetual**
  (`contractType: "LinearPerpetual"`). The same string under `category=spot` would be the spot pair —
  we never use spot. Always send `category=linear`.

**Asset list (config, extensible):** `BTCUSDT, ETHUSDT, SOLUSDT, AVAXUSDT`. Default `BTCUSDT` (shown `BTCUSDT.P`).
One asset across all charts at a time; switching wipes + refetches.

### 3.2 Timeframe ↔ Bybit interval map

| UI label | Bybit interval |
|----------|----------------|
| 15m      | `15`           |
| 1H       | `60`           |
| 4H       | `240`          |
| D        | `D`            |
| W        | `W`            |
| M        | `M`            |

- **Selector order (ascending, TradingView style): `15m, 1H, 4H, D, W, M`.**
- Note: Bybit monthly is **`M`** (not `1M`). Weekly `W`, daily `D`.

### 3.3 REST history fetch

```
GET https://api.bybit.com/v5/market/kline?category=linear&symbol={api_symbol}&interval={iv}&limit=1000
```
- Response: `result.list` = array of `[startTime(ms), open, high, low, close, volume, turnover]`,
  **newest-first → reverse to oldest-first.**
- Parse OHLCV strings to floats. Candle `time` for the chart = `startTime // 1000` (UNIX seconds, UTC, int).
- Fetch **1000 bars per active timeframe** on init, on asset change, and when a new timeframe is first selected.

### 3.4 Live updates (Bybit public WS)

- URL: `wss://stream.bybit.com/v5/public/linear`
- Subscribe per active `(symbol, interval)`: `{"op":"subscribe","args":["kline.{iv}.{api_symbol}", ...]}`
  (e.g. `kline.240.BTCUSDT`).
- Each kline message carries a **`confirm`** flag:
  - `confirm=true` → the candle has **closed** → upsert it as a closed bar into the DB; push a
    `candle_update` (closed=true) to the browser. **Only closed bars count for conditions.**
  - `confirm=false` → the still-forming candle → push a `candle_update` (closed=false) so the chart's
    last candle animates; do **not** treat it as closed for conditions.
- **Keepalive:** send `{"op":"ping"}` roughly every 20s; handle reconnects and **re-subscribe** when the
  asset or any timeframe changes.

### 3.5 Fresh DB per launch

On startup, delete the SQLite file (or DROP+CREATE the table) so each session starts clean. The DB is a
per-session cache, never persisted between runs.

### 3.6 SQLite schema

```sql
CREATE TABLE candles (
  symbol    TEXT    NOT NULL,
  timeframe TEXT    NOT NULL,   -- UI label: 15m,1H,4H,D,W,M
  ts        INTEGER NOT NULL,   -- candle open time, UNIX seconds UTC
  open      REAL    NOT NULL,
  high      REAL    NOT NULL,
  low       REAL    NOT NULL,
  close     REAL    NOT NULL,
  volume    REAL    NOT NULL,
  PRIMARY KEY (symbol, timeframe, ts)
);
```
Upsert with `INSERT ... ON CONFLICT(symbol,timeframe,ts) DO UPDATE` so the closing candle replaces the forming one.

---

## 4. Indicator engine (server-side, numpy)

All indicators are computed from **closed** candles for a `(symbol, timeframe)` and returned as
draw-ready series (`[{time, value}]`) plus latest values for conditions. Indicators are **computed on the
server and sent to the browser to draw** — never recomputed in the browser, so screen and AI agree exactly.

Multiple EMAs/SMAs may coexist on one chart (distinct periods → distinct ids/colors). VWAP and Volume
Profile are one-per-chart but re-anchorable/re-rangeable.

### 4.1 SMA(period)
Rolling mean of `close` over `period`. `null` before index `period-1`.

### 4.2 EMA(period)
`k = 2/(period+1)`. Seed: `EMA[period-1] = SMA(first period closes)`. Then
`EMA[i] = close[i]*k + EMA[i-1]*(1-k)` for `i >= period`. `null` before `period-1`.

### 4.3 VWAP — anchored
Typical price `TP = (high+low+close)/3`. From an **anchor** bar onward:
`VWAP[i] = cumsum(TP*volume) / cumsum(volume)` (cumulative from the anchor).
- Param `anchor_time` (UNIX seconds or ISO); **default = first loaded bar** of the window.
- The AI can re-anchor (e.g. to a swing low) by calling `add_vwap` again with a new anchor.

### 4.4 Volume Profile — POC / VAH / VAL
Over a range `[start_ts, end_ts]` (default = full loaded window):
1. Bin price into `bins` buckets (default 50) between `min(low)` and `max(high)` of the range.
2. For each bar, **distribute its volume evenly across the buckets its `[low, high]` spans** (a bar covering
   N buckets adds `volume/N` to each).
3. **POC** = price (bucket center) of the max-volume bucket.
4. **Value area (default 70%)**: start the band at the POC bucket; repeatedly look at the buckets immediately
   above and below the current band, add whichever single side has the greater volume, until the band's
   cumulative volume ≥ `0.70 * total`. **VAH** = top edge, **VAL** = bottom edge of that band.
- Returns per-bucket volumes (for the overlay) plus `{poc, vah, val}`.
- AI can set `start_time`, `end_time`, `bins`.

### 4.5 Volume pane
Raw per-bar `volume` as a histogram in a separate bottom pane, colored by candle direction
(up = green, down = red).

### 4.6 Overlay colors (dark theme, distinct)
EMA: `#f0b90b`, `#ff8a3d`, `#d16bff`, `#36d1a6` (assign in add-order). SMA: `#4d9fff`, `#9aa7ff`, `#ff6b9d`,
`#7be0ff`. VWAP: `#e0e0e0` (with anchor marker). VP: bars `#5b6b8c` @ ~50% opacity, POC `#f0b90b`,
VAH/VAL `#888`.

---

## 5. Condition engine (pull, server-side)

Single entry point reads the **current DB (closed candles only)** and returns a JSON-serializable dict.

`evaluate(timeframe, test, **params) -> dict` with a consistent shape:
`{ "result": bool, "test": str, "timeframe": str, ...details, "bar_time": int, "confirmed": true }`

**v1 tests:**

| test | params | returns (besides `result`) |
|------|--------|----------------------------|
| `close_above_ema` / `close_below_ema` | `period` | `close`, `ema`, `gap_pct` |
| `close_above_sma` / `close_below_sma` | `period` | `close`, `sma`, `gap_pct` |
| `close_above_vwap` / `close_below_vwap` | — (uses chart's current VWAP anchor) | `close`, `vwap`, `gap_pct` |
| `ema_cross` | `fast`, `slow` | `direction` (`bullish`/`bearish`/`none`), `fast`, `slow` |
| `sma_cross` | `fast`, `slow` | `direction`, `fast`, `slow` |
| `price_above` / `price_below` | `value` | `last_close`, `value` |
| `close_crossed_line` | `line_price`, `direction` (`above`/`below`) | `close`, `prev_close`, `line_price`, `crossed` |
| `candle_bullish` / `candle_bearish` | — | `open`, `close` |
| `above_poc` / `below_poc` | — (uses chart's current VP range) | `close`, `poc` |

- `*_cross` are evaluated on the **last closed bar** (prev bar on one side, current on the other).
- `gap_pct` = `(close - indicator) / indicator * 100`.
- **Multi-timeframe alignment is NOT a tool** — the AI calls per-timeframe checks and synthesizes the
  answer ("above the 50 EMA on D, 4H and 1H → aligned bullish").

---

## 6. Scene / state model (in-memory, server-owned)

- **Global:** `asset` (`display` + `api_symbol`), `layout` ∈ {1, 2, 4}.
- **Slots:** a list of `layout` slots. Each slot: `{ slot_id (1..N), timeframe, indicators: {id: {...}}, drawings: [...] }`.
- **Default timeframes per layout** (order = top→bottom / TL,TR,BL,BR):
  - 1 → `[D]`
  - 2 → `[D, 4H]`
  - 4 → `[D, 4H, 1H, 15m]`
  - **DEFAULT_LAYOUT = 4.**
- **Chart addressing by the AI: by timeframe** (assumed unique within a layout). If the timeframe isn't
  currently displayed, the tool returns a clear error. (`slot_id` is also accepted as a fallback.)
- **On asset change:** wipe DB for the new symbol, refetch, **clear price-specific drawings**, recompute
  indicators, broadcast a fresh scene.
- **On layout change:** add/remove slots; assign default timeframes to new slots.
- **On a slot's timeframe change:** fetch that tf if not cached, recompute that slot's indicators, rebroadcast.

---

## 7. Browser ↔ server protocol (`/ws`, JSON)

The browser connects to `/ws`. The AI does **not** use this channel.

**Server → browser:**
- `{type:"scene", asset, layout, slots:[{slot_id, timeframe, indicators, drawings}]}` — on connect and on any structural change.
- `{type:"candles", timeframe, data:[{time,open,high,low,close,volume}]}` — full set on load/refresh.
- `{type:"candle_update", timeframe, bar:{...}, closed:bool}` — live last-candle.
- `{type:"indicator", timeframe, id, kind:"ema"|"sma"|"vwap"|"vp"|"volume", series:[...], meta:{poc,vah,val}?}` — draw/update overlay (meta only for `vp`).
- `{type:"remove_indicator", timeframe, id}`
- `{type:"drawing", timeframe, id, kind:"hline"|"trendline", ...params}`
- `{type:"clear_drawings", timeframe}`
- `{type:"ack", cmd_id, ok:bool, error?:str}`

**Browser → server:**
- `{type:"hello"}` on connect (server replies with full `scene` + `candles` per slot).
- `{type:"manual", action, ...}` — when the user uses on-page controls (change asset/layout/timeframe,
  toggle an indicator). The server performs the change and **broadcasts back**, so manual and AI stay in
  sync and the server remains authoritative.

---

## 8. Frontend (Lightweight Charts)

### 8.1 Library
Lightweight Charts **v4.2.x** standalone build via CDN (pin — v5 changed the series API):
```
https://cdn.jsdelivr.net/npm/lightweight-charts@4.2.0/dist/lightweight-charts.standalone.production.js
```
Use `chart.addCandlestickSeries`, `chart.addLineSeries`, `chart.addHistogramSeries`,
`series.createPriceLine`, `chart.removeSeries`, `chart.remove`, `chart.timeScale().fitContent()`, and
`autoSize: true` on `createChart`.

### 8.2 Layout & controls
- **Top bar:** asset selector (dropdown of `.P` labels), layout selector (`1 / 2 / 4`), connection-status
  dot, manual refresh.
- **Per-chart panel header:** timeframe picker (single-select `15m 1H 4H D W M`, ascending), last price +
  % change, and indicator toggles (`EMA · SMA · VWAP · VP · Vol`) for manual use.
- **Layout CSS (grid):** `1` = single full panel; `2` = two panels stacked vertically (top/bottom);
  `4` = 2×2 grid. Each panel fills its cell; `autoSize` handles width/height.
- **Theme:** dark trading-terminal. Suggested tokens — bg `#0a0e14`, panel `#11161f`, border `#232b38`,
  text `#d4dae3`, muted `#6b7685`, accent `#2dd4bf`, candle up `#26a69a`, candle down `#ef5350`.
  Monospace numerics for prices.

### 8.3 Rendering rules
- Candlestick series + (optional) volume histogram in a separate pane.
- EMA/SMA/VWAP as overlay line series; **Volume Profile as a custom right-aligned horizontal-bar overlay**
  (Lightweight Charts has no native VP — draw it as a canvas/HTML overlay aligned to the price scale, with
  POC/VAH/VAL as price lines).
- Horizontal lines via `createPriceLine`; trendlines as a 2-point line series.
- Time axis: UNIX seconds UTC; `timeVisible: true` for intraday.

### 8.4 Browser command handlers (`window.chartAPI` for debug/manual)
Implement handlers that apply the `/ws` messages: `applyScene`, `setCandles`, `updateCandle`,
`drawIndicator`, `removeIndicator`, `drawHline`, `drawTrendline`, `clearDrawings`. Expose them on
`window.chartAPI` for console debugging.

### 8.5 Optional
A small collapsible **event log** panel showing recent AI commands (nice for seeing what the AI did).

---

## 9. MCP server (the AI contract)

### 9.1 Transport
**Streamable HTTP at `/mcp`**, mounted inside the FastAPI app so any MCP-capable client/model on the LAN
can connect to `http://{beelink}:8800/mcp`. Ensure the MCP session manager's lifespan is included in the
app lifespan. **Verify the exact FastMCP mounting pattern against the installed `mcp` version.**

### 9.2 Tools
Each tool is a plain decorated function with typed params and a docstring. Tools call internal functions
in `functions_tv_charts.py` that mutate the scene, broadcast to `/ws`, and/or read the DB. All return
`{ok: bool, ...payload, error?: str}` (except `check_condition`, which returns the §5 dict).

**Scene control**
- `get_scene()` → full scene snapshot.
- `set_asset(symbol)` → accepts `"BTCUSDT"` or `"BTCUSDT.P"`; wipes + refetches + rebroadcasts.
- `set_layout(n)` → `n ∈ {1,2,4}`.
- `set_slot_timeframe(slot_id, timeframe)` → set one slot's timeframe.
- `list_assets()`, `list_timeframes()`.

**Indicators**
- `add_ema(timeframe, period)`
- `add_sma(timeframe, period)`
- `add_vwap(timeframe, anchor_time=None)` → default anchor = window start.
- `add_volume_profile(timeframe, start_time=None, end_time=None, bins=50)` → returns `{poc, vah, val}`.
- `toggle_volume_pane(timeframe, on)`
- `remove_indicator(timeframe, indicator_id)`
- `list_indicators(timeframe)`

**Drawings**
- `draw_hline(timeframe, price, label=None, color=None)` → `id`
- `draw_trendline(timeframe, t1, price1, t2, price2, label=None, color=None)` → `id`
  (`t1`/`t2` are UNIX seconds or ISO)
- `clear_drawings(timeframe)`

**Data / conditions**
- `get_candles(timeframe, count=200)` → recent OHLCV (for the AI to reason on raw bars).
- `get_indicator_values(timeframe, indicator_id, count=200)`
- `check_condition(timeframe, test, **params)` → §5 result.

---

## 10. Config

Keep a single config block (top of `main_tv_charts.py` or `config_tv_charts.py`):

```
HOST = "0.0.0.0"
PORT = 8800
CATEGORY = "linear"
BYBIT_REST = "https://api.bybit.com"
BYBIT_WS   = "wss://stream.bybit.com/v5/public/linear"
ASSETS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "AVAXUSDT"]
DEFAULT_ASSET = "BTCUSDT"
TIMEFRAMES = ["15m", "1H", "4H", "D", "W", "M"]          # selector order (ascending)
TF_TO_INTERVAL = {"15m":"15","1H":"60","4H":"240","D":"D","W":"W","M":"M"}
DEFAULT_LAYOUT = 4
DEFAULT_TF = {1:["D"], 2:["D","4H"], 4:["D","4H","1H","15m"]}
HISTORY_BARS = 1000
VP_BINS = 50
VP_VALUE_AREA = 0.70
DB_PATH = "tv_charts.db"                                    # wiped on launch
```

---

## 11. File structure & coding conventions

```
~/ai_projects/tv_charts/
  main_tv_charts.py        # entry point: config, FastAPI app, startup (DB+fetch+WS), route wiring, uvicorn run
  functions_tv_charts.py   # all logic: bybit REST+WS, DB, indicators, conditions, scene mutators, ws broadcast, MCP tool impls
  config_tv_charts.py      # CONFIG (optional; may live in main)
  static/
    index.html           # frontend shell
    app.js               # chart logic + /ws client + window.chartAPI
    style.css            # dark theme
  requirements.txt
  README.md              # run instructions
```

**Conventions (follow these):**
- **Python only, functional style** — no classes except where a framework requires them. Legibility over cleverness.
- Entry point `main_<projectname>.py`; logic in `functions_<projectname>.py`.
- **Header doc block at the top of every `.py` file:**
```
# =============================================================
# Purpose:      <what this file does>
# Runs:         <how / when it runs>
# Inputs:       <inputs>
# Outputs:      <outputs>
# Dependencies: <deps>
# Risks:        <risks / edge cases>
# =============================================================
```
- Thorough inline docs on every function (purpose, params, returns).

---

## 12. Run instructions (put in README.md)

```
cd ~/ai_projects/tv_charts
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python main_tv_charts.py            # serves on http://0.0.0.0:8800
```
- Charts: open `http://{beelink-ip}:8800/` in a desktop browser.
- AI client: connect MCP (streamable HTTP) to `http://{beelink-ip}:8800/mcp`.

---

## 13. Acceptance criteria (v1 "done")

1. Launch wipes the DB, fetches 1000 bars/tf for the default asset, renders the 2×2 default layout.
2. Switching asset (from the `.P` list) reloads all charts; layout `1/2/4` and per-chart timeframe both work.
3. Live candles update via the Bybit WS; closed candles persist and drive conditions.
4. EMA, SMA, anchored VWAP, Volume Profile (POC/VAH/VAL), and the volume pane all render correctly.
5. An MCP client can run `get_scene`, `set_asset`, `set_layout`, `set_slot_timeframe`, `add_*`/`remove_indicator`,
   `draw_hline`/`draw_trendline`/`clear_drawings`, and `check_condition`.
6. **Smoke test:** AI adds EMA 50 to the 4H, draws an hline, then `check_condition(timeframe="4H",
   test="close_above_ema", period=50)` returns a correct `true/false` matching what's on screen.

---

## 14. Out of scope for v1 — v2 roadmap

- **Condition watching (push):** register conditions that the server monitors on a timer and fires when they
  trigger. MCP can't push to the AI unsolicited, so route alerts to the existing **Hermes Mailbox PWA**
  (mailbox post → web-push to phone → AI reads the mailbox next session). Design the §5 evaluator now so it
  can be reused on a timer later.
- More indicators (Bollinger Bands) and drawings (zones, candle markers, text labels).
- Mobile / Tailscale (HTTPS+WSS).
- **Trading bot (authenticated Bybit linear):** place orders with attached TP/SL via `/v5/order/create`
  (`takeProfit`/`stopLoss`, market or limit) and edit TP/SL on open positions via the position trading-stop
  endpoint. Brings leverage, position mode (one-way/hedge), and TP/SL modes. **Same `linear` perp market as
  v1 — no migration.** Requirements when built: **testnet first**, API key+secret in env/secrets (never in
  code), HMAC-signed requests, and a hard kill-switch + position/loss caps before touching the live account.

---

## 15. Risks & notes for the builder

- **MCP SDK HTTP transport changes across versions** → verify the current FastMCP `streamable_http` mounting
  + lifespan pattern against the installed `mcp` package before building §9.1.
- **Lightweight Charts v4 vs v5** differ (series creation API) → pin v4.2.x.
- **Bybit REST kline is newest-first** → reverse to oldest-first before storing/plotting.
- **Bybit interval codes:** monthly is `M` (not `1M`), weekly `W`, daily `D`.
- **Bybit WS** needs `{"op":"ping"}` keepalive (~20s) and re-subscribe on asset/timeframe change; handle reconnects.
- **Volume Profile** has no native Lightweight Charts support → custom overlay aligned to the price scale;
  recompute + redraw on data append and on range change.
- **Rate limits:** init does ≤ (timeframes) REST calls (one per tf) — fine. Use the WS for liveness; never
  REST-poll per candle.
- **One asset at a time**; addressing charts by timeframe assumes timeframes are unique within a layout.
- The DB is **ephemeral** — wiped every launch by design.
