# tv_charts

**Multi-timeframe crypto perpetual chart terminal with AI/MCP control.**

One always-on Python process serves a chart web UI, a live browser bridge, **and**
an MCP endpoint so an AI client can drive the charts (add indicators, draw lines)
and **answer conditions on demand** — e.g. *"has the 4H closed above the 50 EMA?"*

Market data is **Bybit v5** USDT perpetuals (`category=linear`). The server is the
brain: it holds the candle cache, computes every indicator and condition, and owns
the on-screen scene. The browser is only a renderer, so the screen and the AI always
agree exactly.

---

# Part 1 — User Manual

## 1.1 What it is

`tv_charts` shows **one** crypto perpetual at a time (BTC, ETH, SOL, or AVAX) across
**several timeframes side by side** (e.g. Daily + 4H + 1H + 15m in a 2×2 grid). You
can add indicators and draw lines either by clicking on the page, or by asking an AI
assistant connected over MCP. Both paths go through the same server, so they never
drift out of sync.

## 1.2 Starting it

On the Beelink (Ubuntu, always-on) the easiest way is the launcher script:

```bash
~/ai_projects/tv_charts/tv_charts.sh
```

On a desktop this **starts the server, opens the chart page in your browser, and opens
a terminal running Claude Code already linked to the MCP server** (it registers/uses the
`tv_charts` MCP server; the first `claude` launch asks you to approve the project's
`.mcp.json` once, then auto-connects). Move the windows wherever you like. Press
**Ctrl-C** in the launcher to stop everything.

- `tv_charts.sh --no-open` (or `TVC_NO_OPEN=1 tv_charts.sh`) just starts the server.
- With no display (e.g. over SSH) the window-opening is skipped automatically.
- If the server is already running, it just opens the windows.

Or start the server by hand:

```bash
cd ~/ai_projects/tv_charts
source .venv/bin/activate          # venv already created during the build
python main_tv_charts.py           # serves on http://0.0.0.0:8800
```

Leave it running. Every launch **wipes and rebuilds** the local candle cache
(`tv_charts.db`) — that file is a throwaway per-session cache, never your source of
truth.

## 1.3 Opening the charts

From any desktop browser on the same LAN:

```
http://<beelink-ip>:8800/
```

There is no mobile layout, login, or HTTPS in v1 — it is a trusted-LAN tool.
The small dot in the top-right corner is the connection indicator: **green = live**,
**red = disconnected** (it auto-reconnects).

## 1.4 The interface

**Top bar (global):**
- **Asset** — pick the perpetual (shown in TradingView notation, e.g. `BTCUSDT.P`).
  Switching reloads every chart for the new asset.
- **Layout** — `1`, `2`, or `4` panels (single / stacked / 2×2 grid).
- **Refresh** — reloads all charts for the current asset.
- **Log** — toggles a side panel showing recent AI/manual actions.

**Each chart panel:**
- **Timeframe picker** (`15m 1H 4H D W M`) — single-select; click to change that
  panel's timeframe.
- **Last price + % change** since the start of the loaded window.
- **Indicator toggles** — `EMA · SMA · VWAP · VP · Vol` for manual use.

## 1.5 Adding indicators by hand

Click an indicator toggle on a panel:
- **EMA / SMA** — click to toggle on/off (no dialog). The first time you switch a kind
  on it uses a default period (**EMA 9 / SMA 21**); after that, toggling off remembers the
  period(s) currently shown — including any the AI set — and toggling back on restores them.
  The remembered periods are saved in the browser's **localStorage** (key
  `tvcharts.indMemory`), so they survive reloads and browser restarts. Multiple periods can
  coexist (each gets its own color); set custom periods via the AI.
- **VWAP** — anchored VWAP; by default anchored **14 bars back** from the latest bar on
  whatever timeframe the chart is on (`DEFAULT_LOOKBACK_BARS`). Toggle off to remove.
- **VP** — Volume Profile over the **last 14 bars** by default, drawing the value-area
  bars on the right plus **POC / VAH / VAL** price lines.
- **Vol** — a volume histogram in a strip along the bottom, green/red by candle.

## 1.6 Driving it with an AI (the main event)

Point any MCP-capable assistant at:

```
http://<beelink-ip>:8800/mcp        (streamable HTTP transport)
```

Then just talk to it in plain language. The assistant translates your intent into the
MCP tools below. Examples:

- *"Put the 20 and 50 EMA on the 4H and the 200 on the Daily."*
- *"Anchor a VWAP to the swing low on the 1H."*  (it passes the bar time as the anchor)
- *"Add a volume profile on the Daily and mark the POC."*
- *"Draw a horizontal line at 65,000 on the 4H."*
- *"Switch to ETH and give me a 4-chart layout."*

## 1.7 Asking conditions (the killer feature)

Ask yes/no market questions; the AI answers from the **last closed bar** (live, forming
candles never count). Examples and the underlying test:

| You ask | Test used |
|---------|-----------|
| "Has the 4H closed above the 50 EMA?" | `close_above_ema` (period 50) |
| "Is price above the Daily 200 SMA?" | `close_above_sma` (period 200) |
| "Did the 1H cross the 9/21 EMA?" | `ema_cross` (fast 9, slow 21) |
| "Is the Daily above its VWAP?" | `close_above_vwap` |
| "Are we above the volume-profile POC?" | `above_poc` |
| "Did the 15m close back above 64,000?" | `close_crossed_line` |
| "Is the last 4H candle bullish?" | `candle_bullish` |

**Multi-timeframe alignment** is not a single tool — the AI checks each timeframe and
synthesises: *"Above the 50 EMA on D, 4H and 1H → aligned bullish."*

Every answer comes back with the numbers behind it (close, indicator value, % gap), so
you can sanity-check it against the chart.

## 1.8 Manual and AI stay in sync

Anything the AI does appears on the page instantly, and anything you do on the page is
sent to the server and reflected back — there is a single authoritative state. Open the
**Log** panel to watch the AI's actions land in real time.

## 1.9 Troubleshooting

- **Red connection dot / blank charts** — the server isn't running or the LAN IP/port is
  wrong. Confirm `python main_tv_charts.py` is up and you used port `8800`.
- **"timeframe X is not currently displayed"** from the AI — that timeframe isn't in the
  current layout. Ask it to set the panel to that timeframe first.
- **No live updates** — check the server console for `[bybit-ws]` reconnect messages; the
  feed auto-reconnects, but a dead LAN/Internet link will stall live candles.
- **Charts look stale after a restart** — expected: the DB is wiped each launch and
  refetched fresh.

---

# Part 2 — Build Documentation

## 2.1 Architecture

```
AI client  --MCP (streamable HTTP /mcp)-->  tv_charts (one uvicorn process)  --WS /ws-->  Browser (chart grid)
                                                   |
                                                   |  REST history + WS live
                                                   v
                                            Bybit v5 (category=linear)
```

- **Server is authoritative.** It owns the SQLite candle cache, computes all indicators
  and conditions, and holds the in-memory scene. Browsers receive draw commands over `/ws`.
- **The AI never touches `/ws`.** It calls MCP tools at `/mcp`; tool implementations mutate
  the scene, broadcast to browser clients, and read the DB for condition checks.
- **On startup:** wipe + create the DB, REST-fetch history for the default asset across the
  default timeframes, then subscribe to the Bybit public WS for live candles.

## 2.2 Tech stack (verified installed versions)

| Component | Version | Notes |
|-----------|---------|-------|
| Python | 3.12.3 | spec floor 3.11+ |
| FastAPI / uvicorn | 0.115.6 / 0.34.0 | single ASGI process |
| mcp (FastMCP) | **1.28.0** | streamable HTTP; older 1.2.x had no streamable transport |
| httpx | 0.28.1 | async Bybit REST |
| websockets | 14.1 | Bybit public WS client |
| numpy | 2.2.1 | indicator math |
| Lightweight Charts | **4.2.0** (CDN) | pinned; v5 changed the series API |

## 2.3 File structure

```
~/ai_projects/tv_charts/
  main_tv_charts.py        # FastAPI app, MCP mount, lifespan, routes, uvicorn run
  functions_tv_charts.py   # all logic: Bybit REST+WS, DB, indicators, conditions,
                           #            scene mutators, /ws broadcast, MCP tool impls
  config_tv_charts.py      # single config block (hosts, assets, timeframes, defaults)
  static/
    index.html             # frontend shell
    app.js                 # chart logic + /ws client + window.chartAPI
    style.css              # dark theme
  requirements.txt
  README.md                # this file (user manual + build docs)
  tv_charts_spec.md        # the original build brief (in the parent dir)
```

Conventions: Python-only, functional style (no classes except where a framework
requires them); every `.py` carries a Purpose/Runs/Inputs/Outputs/Dependencies/Risks
header; every function is documented.

## 2.4 Data layer

- **Symbols:** internal api symbol `BTCUSDT`; displayed as `BTCUSDT.P`. Always queried
  under `category=linear` (the USDT perpetual).
- **Timeframe → Bybit interval:** `15m→15, 1H→60, 4H→240, D→D, W→W, M→M`.
  > **Important:** Bybit v5 uses **bare** interval letters — monthly is **`M`**, not `1M`.
  > `1M` is *Binance's* convention and returns an **empty list** on Bybit (verified live
  > against `api.bybit.com` on 2026-06-20). If the data source is ever swapped to Binance,
  > monthly flips to `1M`. See `config_tv_charts.TF_TO_INTERVAL`.
- **REST history:** `GET /v5/market/kline?category=linear&symbol=…&interval=…&limit=1000`.
  Bybit returns **newest-first**, so we reverse to oldest-first; candle `time = startTime//1000`
  (UNIX seconds UTC).
- **Live WS:** `wss://stream.bybit.com/v5/public/linear`, topic `kline.{interval}.{symbol}`.
  `confirm=true` → closed bar (upserted to DB, drives conditions); `confirm=false` → forming
  bar (animates the last candle only). App-level `{"op":"ping"}` every ~20s; reconnect +
  re-subscribe on asset/timeframe change.
- **SQLite:** one `candles` table keyed `(symbol, timeframe, ts)`, upsert-on-conflict so a
  closing candle replaces its forming version. The DB file is deleted every launch.

## 2.5 Indicator engine (numpy, server-side)

Computed from **closed** candles and sent to the browser as draw-ready series, so screen
and AI agree. SMA, EMA (SMA-seeded), anchored VWAP (cumulative typical-price×volume from an
anchor bar), Volume Profile (per-bar volume spread across the buckets its [low,high] spans →
POC + 70% value-area VAH/VAL), and a direction-colored volume histogram. Multiple EMAs/SMAs
may coexist; VWAP and VP are one-per-chart but re-anchorable / re-rangeable.

## 2.6 Condition engine (pull, server-side)

`evaluate(timeframe, test, **params)` reads the current closed candles and returns
`{result, test, timeframe, …details, bar_time, confirmed:true}`. Tests:
`close_above/below_ema`, `close_above/below_sma`, `close_above/below_vwap`,
`ema_cross`, `sma_cross`, `price_above/below`, `close_crossed_line`,
`candle_bullish/bearish`, `above/below_poc`. Crosses use the last two closed bars.
The evaluator is deliberately reusable so v2 can run it on a timer for push alerts.

## 2.7 Scene model & `/ws` protocol

In-memory scene: `{asset, layout∈{1,2,4}, slots:[{slot_id, timeframe, indicators, drawings}]}`.
Charts are addressed **by timeframe** (unique within a layout; `slot_id` is a fallback).
Server→browser messages: `scene`, `candles`, `candle_update`, `indicator`,
`remove_indicator`, `drawing`, `clear_drawings`, `ack`. Browser→server: `hello` (triggers a
full scene + per-slot data push) and `manual` (on-page user actions, applied server-side then
broadcast back).

## 2.8 MCP server

Mounted at **`/mcp`** (streamable HTTP). **Verified pattern for mcp 1.28.0:**

- `FastMCP("tv_charts", stateless_http=True, json_response=True,
  transport_security=TransportSecuritySettings(enable_dns_rebinding_protection=False))`
- Mount `mcp.streamable_http_app()` at `/` **after** the app's own routes — its inner route
  is exactly `/mcp` (so no trailing-slash redirect) and it acts as the catch-all.
- Drive `mcp.session_manager.run()` from the FastAPI **lifespan** (mounted sub-app lifespans
  don't fire on their own).
- DNS-rebinding protection is **off** so the Beelink's LAN-IP `Host` header is accepted.

> Re-verify this pattern if `mcp` is upgraded — the streamable-HTTP mounting + lifespan API
> has changed across versions.

**Tools** (19). Names that would shadow internal helpers carry a `_tool` suffix on the wire:
`get_scene`, `set_asset_tool`, `set_layout_tool`, `set_slot_timeframe_tool`, `list_assets`,
`list_timeframes`, `add_ema_tool`, `add_sma_tool`, `add_vwap_tool`,
`add_volume_profile_tool`, `toggle_volume_pane_tool`, `remove_indicator_tool`,
`list_indicators_tool`, `draw_hline_tool`, `draw_trendline_tool`, `clear_drawings_tool`,
`get_candles_tool`, `get_indicator_values_tool`, `check_condition`. All return
`{ok, …}` except `check_condition`, which returns the condition dict above.

## 2.9 Config reference

All knobs live at the top of `config_tv_charts.py`: `HOST/PORT`, `CATEGORY`, Bybit REST/WS
URLs, `ASSETS` + `DEFAULT_ASSET`, `TIMEFRAMES` + `TF_TO_INTERVAL`, `DEFAULT_LAYOUT` +
`DEFAULT_TF`, `HISTORY_BARS`, `VP_BINS`, `VP_VALUE_AREA`, `DB_PATH`, the LWC CDN url, and the
overlay color palettes.

## 2.10 Verification performed at build time

- Bybit monthly interval `M` vs `1M` tested live (`M` works, `1M` empty).
- `mcp` upgraded 1.2.0 → 1.28.0 after confirming 1.2.0 lacked streamable HTTP; the mount +
  lifespan pattern was proven with an in-process `initialize` round-trip.
- Full acceptance smoke test against the running server: scene/asset/layout/timeframe tools,
  EMA/SMA/VWAP/VP/volume, drawings, and `check_condition(close_above_ema, 50)` whose result
  was **cross-checked against the raw close vs EMA** and matched.
- `/ws` hello delivered scene + 4×1000 candles; live `candle_update`s confirmed flowing from
  the Bybit WS; a manual `add_ema` round-tripped back to the client.

## 2.11 Out of scope (v1) / v2 roadmap

Condition **watching** (server-side timers → push alerts via the Hermes Mailbox PWA), more
indicators (Bollinger) and drawings (zones, markers, text), mobile / Tailscale (HTTPS+WSS),
and an authenticated Bybit **trading bot** (same `linear` market; testnet-first, secrets in
env, HMAC-signed orders, hard kill-switch + position/loss caps). See `tv_charts_spec.md`.
