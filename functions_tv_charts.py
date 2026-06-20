# =============================================================
# Purpose:      All tv_charts logic: Bybit REST history + public WS live feed,
#               SQLite candle cache, numpy indicator engine, pull-based
#               condition engine, the in-memory server-owned scene model,
#               browser (/ws) broadcasting, and the MCP tool implementations.
# Runs:         Imported by main_tv_charts.py. Stateful module: holds the DB
#               connection, the scene dict, and the set of browser WS clients
#               in module-level globals (single-process, single event loop).
# Inputs:       Bybit v5 market data; commands from MCP tools and from manual
#               browser actions over /ws.
# Outputs:      Mutations to the scene + DB; JSON messages broadcast to
#               browser clients; dict results returned to MCP callers.
# Dependencies: httpx (REST), websockets (Bybit WS), numpy (math), stdlib
#               sqlite3 / asyncio / json, config_tv_charts, mcp (FastMCP).
# Risks:        Bybit REST kline is newest-first (reversed here). Monthly is
#               "M" not "1M". WS needs an app-level {"op":"ping"} keepalive
#               and re-subscribe on asset/timeframe change. The DB is wiped
#               every launch by design. Volume Profile has no native chart
#               support -> sent as series+levels for a custom overlay.
# =============================================================

from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import time
from typing import Any, Optional

import httpx
import numpy as np
import websockets

import config_tv_charts as C

# =============================================================
# Module-level state (single process, single asyncio loop)
# =============================================================

_db: Optional[sqlite3.Connection] = None          # SQLite candle cache
_http: Optional[httpx.AsyncClient] = None         # shared async HTTP client
_clients: set = set()                             # connected browser WebSockets
_draw_counter = 0                                 # monotonic id source for drawings
_loaded: set = set()                              # (api_symbol, tf) already fetched
_ws_stop: Optional[asyncio.Event] = None          # signals the Bybit WS loop to stop

# The scene is the single source of truth the AI and browser both observe.
_scene: dict = {
    "asset": {"display": "", "api_symbol": ""},
    "layout": C.DEFAULT_LAYOUT,
    "slots": [],   # list of {slot_id, timeframe, indicators:{id:cfg}, drawings:[cfg]}
}


# =============================================================
# Small helpers
# =============================================================

def display_label(api_symbol: str) -> str:
    """Internal api symbol -> user-facing label (BTCUSDT -> BTCUSDT.P)."""
    return f"{api_symbol}.P"


def to_api_symbol(symbol: str) -> str:
    """Accept either 'BTCUSDT' or 'BTCUSDT.P' and return the api symbol."""
    return symbol.upper().removesuffix(".P")


def get_scene() -> dict:
    """Return the live scene dict (the authoritative server state)."""
    return _scene


def slot_by_timeframe(timeframe: str) -> Optional[dict]:
    """Find the slot currently showing `timeframe` (unique within a layout)."""
    for s in _scene["slots"]:
        if s["timeframe"] == timeframe:
            return s
    return None


def slot_by_id(slot_id: int) -> Optional[dict]:
    """Find a slot by its 1..N id."""
    for s in _scene["slots"]:
        if s["slot_id"] == slot_id:
            return s
    return None


def _resolve_slot(timeframe: Optional[str] = None,
                  slot_id: Optional[int] = None) -> tuple[Optional[dict], Optional[str]]:
    """Resolve a slot by timeframe (primary) or slot_id (fallback).

    Returns (slot, error_message). Exactly one of slot/error is non-None.
    """
    if timeframe is not None:
        s = slot_by_timeframe(timeframe)
        if s is None:
            shown = [x["timeframe"] for x in _scene["slots"]]
            return None, f"timeframe {timeframe!r} is not currently displayed (showing {shown})"
        return s, None
    if slot_id is not None:
        s = slot_by_id(slot_id)
        if s is None:
            return None, f"slot_id {slot_id} does not exist"
        return s, None
    return None, "must provide a timeframe (or slot_id)"


# =============================================================
# SQLite layer (fresh per launch -- see config DB_PATH)
# =============================================================

def init_db() -> None:
    """Wipe any existing DB file and (re)create the candles table.

    The DB is a per-session cache only; it is intentionally deleted each launch.
    """
    global _db
    if _db is not None:
        try:
            _db.close()
        except Exception:
            pass
        _db = None
    if os.path.exists(C.DB_PATH):
        os.remove(C.DB_PATH)
    _db = sqlite3.connect(C.DB_PATH, check_same_thread=False)
    _db.execute(
        """
        CREATE TABLE candles (
          symbol    TEXT    NOT NULL,
          timeframe TEXT    NOT NULL,
          ts        INTEGER NOT NULL,
          open      REAL    NOT NULL,
          high      REAL    NOT NULL,
          low       REAL    NOT NULL,
          close     REAL    NOT NULL,
          volume    REAL    NOT NULL,
          PRIMARY KEY (symbol, timeframe, ts)
        )
        """
    )
    _db.commit()


def upsert_candle(symbol: str, timeframe: str, bar: dict) -> None:
    """Insert/replace one candle. `bar` keys: time, open, high, low, close, volume.

    ON CONFLICT replaces a previously-forming bar with its closed version.
    """
    _db.execute(
        """
        INSERT INTO candles(symbol,timeframe,ts,open,high,low,close,volume)
        VALUES(?,?,?,?,?,?,?,?)
        ON CONFLICT(symbol,timeframe,ts) DO UPDATE SET
          open=excluded.open, high=excluded.high, low=excluded.low,
          close=excluded.close, volume=excluded.volume
        """,
        (symbol, timeframe, bar["time"], bar["open"], bar["high"],
         bar["low"], bar["close"], bar["volume"]),
    )
    _db.commit()


def upsert_candles(symbol: str, timeframe: str, bars: list[dict]) -> None:
    """Bulk insert/replace candles (used after a REST history fetch)."""
    _db.executemany(
        """
        INSERT INTO candles(symbol,timeframe,ts,open,high,low,close,volume)
        VALUES(?,?,?,?,?,?,?,?)
        ON CONFLICT(symbol,timeframe,ts) DO UPDATE SET
          open=excluded.open, high=excluded.high, low=excluded.low,
          close=excluded.close, volume=excluded.volume
        """,
        [(symbol, timeframe, b["time"], b["open"], b["high"], b["low"],
          b["close"], b["volume"]) for b in bars],
    )
    _db.commit()


def read_candles(symbol: str, timeframe: str, count: Optional[int] = None) -> list[dict]:
    """Return closed candles oldest-first as dicts. `count` limits to the most recent N."""
    rows = _db.execute(
        "SELECT ts,open,high,low,close,volume FROM candles "
        "WHERE symbol=? AND timeframe=? ORDER BY ts ASC",
        (symbol, timeframe),
    ).fetchall()
    if count is not None and len(rows) > count:
        rows = rows[-count:]
    return [{"time": r[0], "open": r[1], "high": r[2], "low": r[3],
             "close": r[4], "volume": r[5]} for r in rows]


def read_ohlcv(symbol: str, timeframe: str) -> dict:
    """Return closed candles as parallel numpy arrays (oldest-first).

    Keys: time(int64), open, high, low, close, volume (float64). Empty arrays
    if nothing is cached yet.
    """
    rows = _db.execute(
        "SELECT ts,open,high,low,close,volume FROM candles "
        "WHERE symbol=? AND timeframe=? ORDER BY ts ASC",
        (symbol, timeframe),
    ).fetchall()
    if not rows:
        z = np.array([], dtype=float)
        return {"time": np.array([], dtype=np.int64), "open": z, "high": z,
                "low": z, "close": z, "volume": z}
    arr = np.array(rows, dtype=float)
    return {
        "time": arr[:, 0].astype(np.int64),
        "open": arr[:, 1], "high": arr[:, 2], "low": arr[:, 3],
        "close": arr[:, 4], "volume": arr[:, 5],
    }


# =============================================================
# Bybit v5 REST history
# =============================================================

async def fetch_history(api_symbol: str, timeframe: str) -> list[dict]:
    """Fetch up to HISTORY_BARS klines for (api_symbol, timeframe) from Bybit.

    Bybit returns newest-first; we reverse to oldest-first and parse the OHLCV
    strings to floats. Candle `time` = startTime(ms)//1000 (UNIX seconds UTC).
    Returns a list of candle dicts (oldest-first).
    """
    interval = C.TF_TO_INTERVAL[timeframe]
    params = {
        "category": C.CATEGORY,
        "symbol": api_symbol,
        "interval": interval,
        "limit": C.HISTORY_BARS,
    }
    r = await _http.get(f"{C.BYBIT_REST}/v5/market/kline", params=params)
    r.raise_for_status()
    payload = r.json()
    if payload.get("retCode") != 0:
        raise RuntimeError(f"Bybit error: {payload.get('retMsg')}")
    rows = payload["result"]["list"]          # newest-first
    rows = list(reversed(rows))               # -> oldest-first
    out = []
    for it in rows:
        out.append({
            "time": int(it[0]) // 1000,
            "open": float(it[1]), "high": float(it[2]), "low": float(it[3]),
            "close": float(it[4]), "volume": float(it[5]),
        })
    return out


async def ensure_loaded(api_symbol: str, timeframe: str, force: bool = False) -> None:
    """Fetch+store history for (api_symbol, timeframe) unless already cached."""
    key = (api_symbol, timeframe)
    if not force and key in _loaded:
        return
    bars = await fetch_history(api_symbol, timeframe)
    upsert_candles(api_symbol, timeframe, bars)
    _loaded.add(key)


# =============================================================
# Indicator engine (numpy, computed from CLOSED candles)
# =============================================================

def sma(close: np.ndarray, period: int) -> np.ndarray:
    """Simple moving average of `close` over `period`. NaN before index period-1."""
    n = len(close)
    out = np.full(n, np.nan)
    if n >= period:
        csum = np.cumsum(np.insert(close, 0, 0.0))
        out[period - 1:] = (csum[period:] - csum[:-period]) / period
    return out


def ema(close: np.ndarray, period: int) -> np.ndarray:
    """Exponential moving average. Seeded with SMA(first `period`). NaN before period-1."""
    n = len(close)
    out = np.full(n, np.nan)
    if n < period:
        return out
    k = 2.0 / (period + 1.0)
    out[period - 1] = float(np.mean(close[:period]))
    for i in range(period, n):
        out[i] = close[i] * k + out[i - 1] * (1.0 - k)
    return out


def vwap_anchored(o: dict, anchor_time: Optional[int]) -> tuple[np.ndarray, int]:
    """Anchored VWAP. Typical price = (h+l+c)/3, cumulative from the anchor bar.

    Returns (vwap array aligned to all bars with NaN before the anchor, anchor_ts).
    Default anchor = first loaded bar.
    """
    t = o["time"]
    n = len(t)
    out = np.full(n, np.nan)
    if n == 0:
        return out, 0
    if anchor_time is None:
        start = 0
    else:
        # first bar at or after the requested anchor time
        idx = np.searchsorted(t, int(anchor_time), side="left")
        start = int(min(max(idx, 0), n - 1))
    tp = (o["high"] + o["low"] + o["close"]) / 3.0
    pv = tp * o["volume"]
    cum_pv = np.cumsum(pv[start:])
    cum_v = np.cumsum(o["volume"][start:])
    safe = np.where(cum_v == 0, np.nan, cum_v)
    out[start:] = cum_pv / safe
    return out, int(t[start])


def volume_profile(o: dict, start_time: Optional[int], end_time: Optional[int],
                   bins: int) -> dict:
    """Volume Profile over [start_time, end_time] (default = full window).

    Each bar spreads its volume evenly across the price buckets its [low,high]
    spans. Returns per-bucket volumes plus POC / VAH / VAL.
    """
    t = o["time"]
    n = len(t)
    if n == 0:
        return {"buckets": [], "poc": None, "vah": None, "val": None,
                "start_time": start_time, "end_time": end_time, "bins": bins}
    lo_i = 0 if start_time is None else int(np.searchsorted(t, int(start_time), "left"))
    hi_i = n if end_time is None else int(np.searchsorted(t, int(end_time), "right"))
    lo_i = max(0, min(lo_i, n - 1))
    hi_i = max(lo_i + 1, min(hi_i, n))

    low = o["low"][lo_i:hi_i]
    high = o["high"][lo_i:hi_i]
    vol = o["volume"][lo_i:hi_i]
    pmin = float(low.min())
    pmax = float(high.max())
    if pmax <= pmin:                       # degenerate flat range
        pmax = pmin + 1e-9
    edges = np.linspace(pmin, pmax, bins + 1)
    centers = (edges[:-1] + edges[1:]) / 2.0
    width = (pmax - pmin) / bins
    buckets = np.zeros(bins)

    for l, h, v in zip(low, high, vol):
        b0 = int((l - pmin) / width)
        b1 = int((h - pmin) / width)
        b0 = max(0, min(b0, bins - 1))
        b1 = max(0, min(b1, bins - 1))
        span = b1 - b0 + 1
        buckets[b0:b1 + 1] += v / span

    poc_idx = int(np.argmax(buckets))
    total = float(buckets.sum())
    # Grow the value-area band out from the POC, always taking the heavier side.
    lo_b = hi_b = poc_idx
    acc = float(buckets[poc_idx])
    target = C.VP_VALUE_AREA * total
    while acc < target and (lo_b > 0 or hi_b < bins - 1):
        below = buckets[lo_b - 1] if lo_b > 0 else -1.0
        above = buckets[hi_b + 1] if hi_b < bins - 1 else -1.0
        if above >= below:
            hi_b += 1
            acc += float(buckets[hi_b])
        else:
            lo_b -= 1
            acc += float(buckets[lo_b])

    return {
        "buckets": [{"price": float(centers[i]), "volume": float(buckets[i])}
                    for i in range(bins)],
        "poc": float(centers[poc_idx]),
        "vah": float(edges[hi_b + 1]),
        "val": float(edges[lo_b]),
        "start_time": int(t[lo_i]),
        "end_time": int(t[hi_i - 1]),
        "bins": bins,
    }


def _series_from(time_arr: np.ndarray, values: np.ndarray) -> list[dict]:
    """Build a draw-ready [{time,value}] series, dropping NaN points."""
    out = []
    for ts, v in zip(time_arr, values):
        if not np.isnan(v):
            out.append({"time": int(ts), "value": float(v)})
    return out


def volume_series(o: dict, up: str = "#26a69a", down: str = "#ef5350") -> list[dict]:
    """Per-bar volume histogram colored by candle direction."""
    out = []
    for i in range(len(o["time"])):
        color = up if o["close"][i] >= o["open"][i] else down
        out.append({"time": int(o["time"][i]), "value": float(o["volume"][i]),
                    "color": color})
    return out


# =============================================================
# Indicator computation -> draw payloads (from a slot's stored config)
# =============================================================

def compute_indicator_payload(symbol: str, timeframe: str, cfg: dict) -> dict:
    """Compute a draw-ready /ws 'indicator' message body for one indicator cfg.

    Mutates VP cfg in place with the latest poc/vah/val so conditions and the
    scene stay consistent. Returns {id, kind, series, meta?, color?}.
    """
    o = read_ohlcv(symbol, timeframe)
    kind = cfg["kind"]
    if kind == "ema":
        series = _series_from(o["time"], ema(o["close"], cfg["period"]))
        return {"id": cfg["id"], "kind": "ema", "color": cfg["color"], "series": series}
    if kind == "sma":
        series = _series_from(o["time"], sma(o["close"], cfg["period"]))
        return {"id": cfg["id"], "kind": "sma", "color": cfg["color"], "series": series}
    if kind == "vwap":
        v, anchor = vwap_anchored(o, cfg.get("anchor_time"))
        cfg["anchor_time"] = anchor
        series = _series_from(o["time"], v)
        return {"id": cfg["id"], "kind": "vwap", "color": cfg["color"],
                "series": series, "meta": {"anchor_time": anchor}}
    if kind == "vp":
        vp = volume_profile(o, cfg.get("start_time"), cfg.get("end_time"),
                            cfg.get("bins", C.VP_BINS))
        cfg["poc"], cfg["vah"], cfg["val"] = vp["poc"], vp["vah"], vp["val"]
        cfg["start_time"], cfg["end_time"] = vp["start_time"], vp["end_time"]
        return {"id": cfg["id"], "kind": "vp",
                "series": vp["buckets"],
                "meta": {"poc": vp["poc"], "vah": vp["vah"], "val": vp["val"]},
                "colors": {"bar": C.VP_BAR_COLOR, "poc": C.VP_POC_COLOR,
                           "value": C.VP_VALUE_COLOR}}
    if kind == "volume":
        return {"id": cfg["id"], "kind": "volume", "series": volume_series(o)}
    raise ValueError(f"unknown indicator kind {kind!r}")


# =============================================================
# Condition engine (pull; reads CLOSED candles only)
# =============================================================

def _gap_pct(close: float, indicator: float) -> float:
    return (close - indicator) / indicator * 100.0 if indicator else 0.0


def evaluate(timeframe: str, test: str, **params) -> dict:
    """Evaluate a condition on the last CLOSED bar of `timeframe`.

    Returns {result, test, timeframe, ...details, bar_time, confirmed:true}.
    On any structural problem returns {result:False, error:...}.
    """
    slot = slot_by_timeframe(timeframe)
    symbol = _scene["asset"]["api_symbol"]
    o = read_ohlcv(symbol, timeframe)
    base = {"result": False, "test": test, "timeframe": timeframe,
            "confirmed": True}
    if len(o["time"]) == 0:
        return {**base, "error": "no candles loaded for this timeframe"}

    close = o["close"]
    last = len(close) - 1
    bar_time = int(o["time"][last])
    c = float(close[last])
    base["bar_time"] = bar_time

    try:
        if test in ("close_above_ema", "close_below_ema"):
            period = int(params["period"])
            val = ema(close, period)[last]
            if np.isnan(val):
                return {**base, "error": f"not enough bars for EMA {period}"}
            res = c > val if test.endswith("above_ema") else c < val
            return {**base, "result": bool(res), "close": c, "ema": float(val),
                    "gap_pct": _gap_pct(c, float(val))}

        if test in ("close_above_sma", "close_below_sma"):
            period = int(params["period"])
            val = sma(close, period)[last]
            if np.isnan(val):
                return {**base, "error": f"not enough bars for SMA {period}"}
            res = c > val if test.endswith("above_sma") else c < val
            return {**base, "result": bool(res), "close": c, "sma": float(val),
                    "gap_pct": _gap_pct(c, float(val))}

        if test in ("close_above_vwap", "close_below_vwap"):
            cfg = _find_indicator(slot, "vwap")
            if cfg is None:
                return {**base, "error": "no VWAP on this chart (add_vwap first)"}
            v, _ = vwap_anchored(o, cfg.get("anchor_time"))
            val = v[last]
            if np.isnan(val):
                return {**base, "error": "VWAP undefined at last bar"}
            res = c > val if test.endswith("above_vwap") else c < val
            return {**base, "result": bool(res), "close": c, "vwap": float(val),
                    "gap_pct": _gap_pct(c, float(val))}

        if test in ("ema_cross", "sma_cross"):
            fast_p, slow_p = int(params["fast"]), int(params["slow"])
            fn = ema if test == "ema_cross" else sma
            f = fn(close, fast_p)
            s = fn(close, slow_p)
            if last < 1 or np.isnan(f[last]) or np.isnan(s[last]) \
                    or np.isnan(f[last - 1]) or np.isnan(s[last - 1]):
                return {**base, "error": "not enough bars for the cross"}
            prev = f[last - 1] - s[last - 1]
            now = f[last] - s[last]
            if prev <= 0 < now:
                direction = "bullish"
            elif prev >= 0 > now:
                direction = "bearish"
            else:
                direction = "none"
            return {**base, "result": direction != "none", "direction": direction,
                    "fast": float(f[last]), "slow": float(s[last])}

        if test in ("price_above", "price_below"):
            value = float(params["value"])
            res = c > value if test == "price_above" else c < value
            return {**base, "result": bool(res), "last_close": c, "value": value}

        if test == "close_crossed_line":
            line = float(params["line_price"])
            direction = params.get("direction", "above")
            if last < 1:
                return {**base, "error": "need at least 2 bars"}
            prev_c = float(close[last - 1])
            if direction == "above":
                crossed = prev_c <= line < c
            else:
                crossed = prev_c >= line > c
            return {**base, "result": bool(crossed), "close": c, "prev_close": prev_c,
                    "line_price": line, "crossed": bool(crossed)}

        if test in ("candle_bullish", "candle_bearish"):
            op = float(o["open"][last])
            res = c > op if test == "candle_bullish" else c < op
            return {**base, "result": bool(res), "open": op, "close": c}

        if test in ("above_poc", "below_poc"):
            cfg = _find_indicator(slot, "vp")
            if cfg is None:
                return {**base, "error": "no Volume Profile on this chart (add_volume_profile first)"}
            vp = volume_profile(o, cfg.get("start_time"), cfg.get("end_time"),
                                cfg.get("bins", C.VP_BINS))
            poc = vp["poc"]
            res = c > poc if test == "above_poc" else c < poc
            return {**base, "result": bool(res), "close": c, "poc": float(poc)}

        return {**base, "error": f"unknown test {test!r}"}
    except KeyError as e:
        return {**base, "error": f"missing required param {e}"}
    except Exception as e:                       # pragma: no cover - defensive
        return {**base, "error": f"{type(e).__name__}: {e}"}


def _find_indicator(slot: Optional[dict], kind: str) -> Optional[dict]:
    """Return the first indicator config of `kind` in a slot, or None."""
    if slot is None:
        return None
    for cfg in slot["indicators"].values():
        if cfg["kind"] == kind:
            return cfg
    return None


# =============================================================
# Browser broadcast (/ws)
# =============================================================

def register_client(ws) -> None:
    _clients.add(ws)


def unregister_client(ws) -> None:
    _clients.discard(ws)


async def _send(ws, msg: dict) -> bool:
    """Send one JSON message to one client; return False if the socket is dead."""
    try:
        await ws.send_text(json.dumps(msg))
        return True
    except Exception:
        return False


async def broadcast(msg: dict) -> None:
    """Send a JSON message to all connected browser clients (pruning dead ones)."""
    dead = []
    for ws in list(_clients):
        if not await _send(ws, msg):
            dead.append(ws)
    for ws in dead:
        _clients.discard(ws)


def scene_message() -> dict:
    """Build the {type:'scene', ...} structural snapshot for the browser."""
    return {
        "type": "scene",
        "asset": _scene["asset"],
        "layout": _scene["layout"],
        "slots": [
            {"slot_id": s["slot_id"], "timeframe": s["timeframe"],
             "indicators": s["indicators"], "drawings": s["drawings"]}
            for s in _scene["slots"]
        ],
    }


async def push_slot_data(slot: dict, ws=None) -> None:
    """Send a slot's candles + every indicator series + every drawing.

    If `ws` is given, send only to that client; otherwise broadcast to all.
    """
    symbol = _scene["asset"]["api_symbol"]
    tf = slot["timeframe"]
    target = (lambda m: _send(ws, m)) if ws is not None else broadcast

    await target({"type": "candles", "timeframe": tf,
                  "data": read_candles(symbol, tf)})
    for cfg in slot["indicators"].values():
        payload = compute_indicator_payload(symbol, tf, cfg)
        await target({"type": "indicator", "timeframe": tf, **payload})
    for d in slot["drawings"]:
        await target({"type": "drawing", "timeframe": tf, **d})


async def push_full_scene(ws=None) -> None:
    """Send the scene snapshot followed by every slot's data."""
    target = (lambda m: _send(ws, m)) if ws is not None else broadcast
    await target(scene_message())
    for slot in _scene["slots"]:
        await push_slot_data(slot, ws=ws)


async def rebroadcast_indicator(slot: dict, cfg: dict) -> None:
    """Recompute one indicator and broadcast its updated series."""
    payload = compute_indicator_payload(_scene["asset"]["api_symbol"],
                                         slot["timeframe"], cfg)
    await broadcast({"type": "indicator", "timeframe": slot["timeframe"], **payload})


# =============================================================
# Scene construction & mutators (shared by MCP tools and manual /ws actions)
# =============================================================

def _new_slot(slot_id: int, timeframe: str) -> dict:
    return {"slot_id": slot_id, "timeframe": timeframe, "indicators": {}, "drawings": []}


def _build_slots(layout: int) -> list[dict]:
    tfs = C.DEFAULT_TF[layout]
    return [_new_slot(i + 1, tfs[i]) for i in range(layout)]


async def bootstrap_scene() -> None:
    """Initialise the scene for the default asset+layout and load history."""
    api = C.DEFAULT_ASSET
    _scene["asset"] = {"display": display_label(api), "api_symbol": api}
    _scene["layout"] = C.DEFAULT_LAYOUT
    _scene["slots"] = _build_slots(C.DEFAULT_LAYOUT)
    for slot in _scene["slots"]:
        await ensure_loaded(api, slot["timeframe"])


async def set_asset(symbol: str) -> dict:
    """Switch the global asset. Wipes price-specific drawings, refetches, rebroadcasts."""
    api = to_api_symbol(symbol)
    if api not in C.ASSETS:
        return {"ok": False, "error": f"unknown asset {symbol!r}; choose from {C.ASSETS}"}
    _scene["asset"] = {"display": display_label(api), "api_symbol": api}
    for slot in _scene["slots"]:
        slot["drawings"] = []                       # price-specific -> clear
        await ensure_loaded(api, slot["timeframe"])
    await push_full_scene()
    return {"ok": True, "asset": _scene["asset"]}


async def set_layout(n: int) -> dict:
    """Change layout to 1, 2, or 4 slots (assigning default timeframes)."""
    if n not in (1, 2, 4):
        return {"ok": False, "error": "layout must be 1, 2, or 4"}
    _scene["layout"] = n
    _scene["slots"] = _build_slots(n)
    api = _scene["asset"]["api_symbol"]
    for slot in _scene["slots"]:
        await ensure_loaded(api, slot["timeframe"])
    await push_full_scene()
    return {"ok": True, "layout": n}


async def set_slot_timeframe(slot_id: int, timeframe: str) -> dict:
    """Set one slot's timeframe (fetch if needed, recompute, rebroadcast)."""
    if timeframe not in C.TIMEFRAMES:
        return {"ok": False, "error": f"unknown timeframe {timeframe!r}; choose from {C.TIMEFRAMES}"}
    slot = slot_by_id(slot_id)
    if slot is None:
        return {"ok": False, "error": f"slot_id {slot_id} does not exist"}
    if any(s["timeframe"] == timeframe and s is not slot for s in _scene["slots"]):
        return {"ok": False, "error": f"timeframe {timeframe} already shown in another slot"}
    slot["timeframe"] = timeframe
    slot["indicators"] = {}                          # indicators are tf-specific
    slot["drawings"] = []
    await ensure_loaded(_scene["asset"]["api_symbol"], timeframe)
    await push_full_scene()
    return {"ok": True, "slot_id": slot_id, "timeframe": timeframe}


def _next_color(slot: dict, kind: str, palette: list[str]) -> str:
    used = sum(1 for c in slot["indicators"].values() if c["kind"] == kind)
    return palette[used % len(palette)]


async def add_ema(timeframe: str, period: int) -> dict:
    """Add an EMA(period) overlay to the chart on `timeframe`."""
    slot, err = _resolve_slot(timeframe=timeframe)
    if err:
        return {"ok": False, "error": err}
    iid = f"ema-{period}"
    cfg = {"id": iid, "kind": "ema", "period": int(period),
           "color": _next_color(slot, "ema", C.EMA_COLORS)}
    slot["indicators"][iid] = cfg
    await rebroadcast_indicator(slot, cfg)
    return {"ok": True, "id": iid}


async def add_sma(timeframe: str, period: int) -> dict:
    """Add an SMA(period) overlay to the chart on `timeframe`."""
    slot, err = _resolve_slot(timeframe=timeframe)
    if err:
        return {"ok": False, "error": err}
    iid = f"sma-{period}"
    cfg = {"id": iid, "kind": "sma", "period": int(period),
           "color": _next_color(slot, "sma", C.SMA_COLORS)}
    slot["indicators"][iid] = cfg
    await rebroadcast_indicator(slot, cfg)
    return {"ok": True, "id": iid}


def _default_lookback_ts(symbol: str, timeframe: str) -> int:
    """Default VWAP-anchor / VP-start time when none is given.

    Looks back a timeframe-aware number of days (DEFAULT_LOOKBACK_DAYS map)
    before the latest loaded bar for (symbol, timeframe), falling back to
    wall-clock now minus the lookback if no bars are cached yet. UNIX seconds.
    """
    days = C.DEFAULT_LOOKBACK_DAYS.get(timeframe, C.DEFAULT_LOOKBACK_DAYS_FALLBACK)
    lookback = days * 86400
    o = read_ohlcv(symbol, timeframe)
    if len(o["time"]):
        return int(o["time"][-1]) - lookback
    return int(time.time()) - lookback


async def add_vwap(timeframe: str, anchor_time: Optional[int] = None) -> dict:
    """Add (or re-anchor) an anchored VWAP.

    Default anchor (anchor_time=None) = DEFAULT_LOOKBACK_DAYS (14) before the
    latest loaded bar. The VWAP anchor is clamped to the first available bar.
    """
    slot, err = _resolve_slot(timeframe=timeframe)
    if err:
        return {"ok": False, "error": err}
    iid = "vwap"
    if anchor_time is None:
        anchor_time = _default_lookback_ts(_scene["asset"]["api_symbol"], timeframe)
    cfg = {"id": iid, "kind": "vwap",
           "anchor_time": int(anchor_time),
           "color": C.VWAP_COLOR}
    slot["indicators"][iid] = cfg
    await rebroadcast_indicator(slot, cfg)
    return {"ok": True, "id": iid, "anchor_time": cfg["anchor_time"]}


async def add_volume_profile(timeframe: str, start_time: Optional[int] = None,
                             end_time: Optional[int] = None,
                             bins: int = C.VP_BINS) -> dict:
    """Add (or re-range) a Volume Profile. Returns {poc, vah, val}.

    Default range start (start_time=None) = DEFAULT_LOOKBACK_DAYS (14) before
    the latest loaded bar; end_time=None means the latest bar.
    """
    slot, err = _resolve_slot(timeframe=timeframe)
    if err:
        return {"ok": False, "error": err}
    iid = "vp"
    if start_time is None:
        start_time = _default_lookback_ts(_scene["asset"]["api_symbol"], timeframe)
    cfg = {"id": iid, "kind": "vp",
           "start_time": int(start_time),
           "end_time": int(end_time) if end_time is not None else None,
           "bins": int(bins)}
    slot["indicators"][iid] = cfg
    await rebroadcast_indicator(slot, cfg)               # fills poc/vah/val
    return {"ok": True, "id": iid, "poc": cfg.get("poc"),
            "vah": cfg.get("vah"), "val": cfg.get("val")}


async def toggle_volume_pane(timeframe: str, on: bool) -> dict:
    """Show/hide the bottom volume histogram pane on `timeframe`."""
    slot, err = _resolve_slot(timeframe=timeframe)
    if err:
        return {"ok": False, "error": err}
    iid = "volume"
    if on:
        cfg = {"id": iid, "kind": "volume"}
        slot["indicators"][iid] = cfg
        await rebroadcast_indicator(slot, cfg)
    else:
        slot["indicators"].pop(iid, None)
        await broadcast({"type": "remove_indicator", "timeframe": timeframe, "id": iid})
    return {"ok": True, "on": bool(on)}


async def remove_indicator(timeframe: str, indicator_id: str) -> dict:
    """Remove an indicator by id from the chart on `timeframe`."""
    slot, err = _resolve_slot(timeframe=timeframe)
    if err:
        return {"ok": False, "error": err}
    if indicator_id not in slot["indicators"]:
        return {"ok": False, "error": f"no indicator {indicator_id!r} on {timeframe}"}
    slot["indicators"].pop(indicator_id)
    await broadcast({"type": "remove_indicator", "timeframe": timeframe, "id": indicator_id})
    return {"ok": True, "id": indicator_id}


def list_indicators(timeframe: str) -> dict:
    """List indicator configs currently on `timeframe`."""
    slot, err = _resolve_slot(timeframe=timeframe)
    if err:
        return {"ok": False, "error": err}
    return {"ok": True, "indicators": list(slot["indicators"].values())}


# ---- drawings ----------------------------------------------------------

def _next_draw_id(prefix: str) -> str:
    global _draw_counter
    _draw_counter += 1
    return f"{prefix}-{_draw_counter}"


async def draw_hline(timeframe: str, price: float, label: Optional[str] = None,
                     color: Optional[str] = None) -> dict:
    """Draw a horizontal price line on `timeframe`."""
    slot, err = _resolve_slot(timeframe=timeframe)
    if err:
        return {"ok": False, "error": err}
    d = {"id": _next_draw_id("hline"), "kind": "hline", "price": float(price),
         "label": label, "color": color or "#facc15"}
    slot["drawings"].append(d)
    await broadcast({"type": "drawing", "timeframe": timeframe, **d})
    return {"ok": True, "id": d["id"]}


async def draw_trendline(timeframe: str, t1: int, price1: float, t2: int,
                         price2: float, label: Optional[str] = None,
                         color: Optional[str] = None) -> dict:
    """Draw a 2-point trendline on `timeframe` (t1/t2 = UNIX seconds)."""
    slot, err = _resolve_slot(timeframe=timeframe)
    if err:
        return {"ok": False, "error": err}
    d = {"id": _next_draw_id("trend"), "kind": "trendline",
         "t1": int(t1), "price1": float(price1), "t2": int(t2), "price2": float(price2),
         "label": label, "color": color or "#22d3ee"}
    slot["drawings"].append(d)
    await broadcast({"type": "drawing", "timeframe": timeframe, **d})
    return {"ok": True, "id": d["id"]}


async def clear_drawings(timeframe: str) -> dict:
    """Remove all drawings from `timeframe`."""
    slot, err = _resolve_slot(timeframe=timeframe)
    if err:
        return {"ok": False, "error": err}
    slot["drawings"] = []
    await broadcast({"type": "clear_drawings", "timeframe": timeframe})
    return {"ok": True}


# ---- data reads for the AI --------------------------------------------

def get_candles(timeframe: str, count: int = 200) -> dict:
    """Return the most recent `count` OHLCV bars for `timeframe`."""
    slot, err = _resolve_slot(timeframe=timeframe)
    if err:
        return {"ok": False, "error": err}
    data = read_candles(_scene["asset"]["api_symbol"], timeframe, count=count)
    return {"ok": True, "timeframe": timeframe, "candles": data}


def get_indicator_values(timeframe: str, indicator_id: str, count: int = 200) -> dict:
    """Return the most recent `count` values of one indicator on `timeframe`."""
    slot, err = _resolve_slot(timeframe=timeframe)
    if err:
        return {"ok": False, "error": err}
    cfg = slot["indicators"].get(indicator_id)
    if cfg is None:
        return {"ok": False, "error": f"no indicator {indicator_id!r} on {timeframe}"}
    payload = compute_indicator_payload(_scene["asset"]["api_symbol"], timeframe, cfg)
    series = payload.get("series", [])
    if count is not None and len(series) > count:
        series = series[-count:]
    out = {"ok": True, "timeframe": timeframe, "id": indicator_id,
           "kind": cfg["kind"], "values": series}
    if "meta" in payload:
        out["meta"] = payload["meta"]
    return out


# =============================================================
# Manual browser actions (mirror MCP mutators so the server stays authoritative)
# =============================================================

async def handle_manual(action: str, params: dict) -> None:
    """Apply an on-page user action received over /ws (then broadcasts happen)."""
    if action == "set_asset":
        await set_asset(params["symbol"])
    elif action == "set_layout":
        await set_layout(int(params["n"]))
    elif action == "set_slot_timeframe":
        await set_slot_timeframe(int(params["slot_id"]), params["timeframe"])
    elif action == "add_ema":
        await add_ema(params["timeframe"], int(params["period"]))
    elif action == "add_sma":
        await add_sma(params["timeframe"], int(params["period"]))
    elif action == "add_vwap":
        await add_vwap(params["timeframe"], params.get("anchor_time"))
    elif action == "add_volume_profile":
        await add_volume_profile(params["timeframe"], params.get("start_time"),
                                 params.get("end_time"), int(params.get("bins", C.VP_BINS)))
    elif action == "toggle_volume_pane":
        await toggle_volume_pane(params["timeframe"], bool(params["on"]))
    elif action == "remove_indicator":
        await remove_indicator(params["timeframe"], params["indicator_id"])
    elif action == "clear_drawings":
        await clear_drawings(params["timeframe"])


# =============================================================
# Bybit public WS live feed
# =============================================================

def _desired_topics() -> set[str]:
    """Build the set of kline topics for the current asset's active timeframes."""
    api = _scene["asset"]["api_symbol"]
    if not api:
        return set()
    out = set()
    for slot in _scene["slots"]:
        iv = C.TF_TO_INTERVAL[slot["timeframe"]]
        out.add(f"kline.{iv}.{api}")
    return out


async def _handle_ws_kline(topic: str, items: list[dict]) -> None:
    """Process one kline WS payload: upsert closed bars, push live updates."""
    # topic = kline.{interval}.{symbol}
    parts = topic.split(".")
    if len(parts) != 3:
        return
    interval, api_symbol = parts[1], parts[2]
    tf = C.INTERVAL_TO_TF.get(interval)
    if tf is None or api_symbol != _scene["asset"]["api_symbol"]:
        return
    for it in items:
        bar = {
            "time": int(it["start"]) // 1000,
            "open": float(it["open"]), "high": float(it["high"]),
            "low": float(it["low"]), "close": float(it["close"]),
            "volume": float(it["volume"]),
        }
        closed = bool(it.get("confirm"))
        if closed:
            upsert_candle(api_symbol, tf, bar)
            # a newly closed bar changes indicators -> recompute & rebroadcast
            slot = slot_by_timeframe(tf)
            if slot is not None:
                for cfg in slot["indicators"].values():
                    await rebroadcast_indicator(slot, cfg)
        await broadcast({"type": "candle_update", "timeframe": tf,
                         "bar": bar, "closed": closed})


async def bybit_ws_loop() -> None:
    """Maintain the Bybit public WS: subscribe to active topics, keepalive, reconnect.

    Reconciles its subscription set every few seconds; if the desired topics
    change (asset/timeframe switch) it reconnects with the new set. Sends an
    app-level {"op":"ping"} ~every 20s (library auto-ping disabled).
    """
    assert _ws_stop is not None
    backoff = 1.0
    while not _ws_stop.is_set():
        topics = _desired_topics()
        if not topics:
            await asyncio.sleep(1.0)
            continue
        try:
            async with websockets.connect(C.BYBIT_WS, ping_interval=None,
                                           open_timeout=10) as ws:
                await ws.send(json.dumps({"op": "subscribe", "args": sorted(topics)}))
                backoff = 1.0
                last_ping = time.time()
                while not _ws_stop.is_set():
                    if _desired_topics() != topics:
                        break                              # reconnect w/ new topics
                    if time.time() - last_ping > 20:
                        await ws.send(json.dumps({"op": "ping"}))
                        last_ping = time.time()
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=5)
                    except asyncio.TimeoutError:
                        continue
                    msg = json.loads(raw)
                    topic = msg.get("topic", "")
                    if topic.startswith("kline.") and "data" in msg:
                        await _handle_ws_kline(topic, msg["data"])
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"[bybit-ws] {type(e).__name__}: {e}; reconnecting in {backoff:.0f}s")
            try:
                await asyncio.wait_for(_ws_stop.wait(), timeout=backoff)
            except asyncio.TimeoutError:
                pass
            backoff = min(backoff * 2, 30.0)


# =============================================================
# Lifecycle (called from main lifespan)
# =============================================================

async def startup() -> None:
    """Open HTTP client, wipe+create DB, build scene, fetch history."""
    global _http, _ws_stop
    _http = httpx.AsyncClient(timeout=20.0)
    _ws_stop = asyncio.Event()
    init_db()
    await bootstrap_scene()


async def shutdown() -> None:
    """Close the HTTP client and the DB."""
    if _ws_stop is not None:
        _ws_stop.set()
    if _http is not None:
        await _http.aclose()
    if _db is not None:
        _db.close()


# =============================================================
# MCP tool registration (the AI contract; see spec section 9)
# =============================================================

def register_mcp_tools(mcp) -> None:
    """Register all MCP tools on the given FastMCP instance.

    Each tool delegates to the mutators above so AI and manual actions share
    one authoritative code path. All return {ok, ...} except check_condition.
    """

    # ---- scene control ----
    @mcp.tool()
    async def get_scene() -> dict:
        """Return the full scene: asset, layout, and every slot's timeframe,
        indicators, and drawings."""
        return {"ok": True, "scene": get_scene_state()}

    @mcp.tool()
    async def set_asset_tool(symbol: str) -> dict:
        """Switch the global asset for all charts. Accepts 'BTCUSDT' or
        'BTCUSDT.P'. Wipes price-specific drawings and refetches history."""
        return await set_asset(symbol)

    @mcp.tool()
    async def set_layout_tool(n: int) -> dict:
        """Set the chart grid layout. n must be 1, 2, or 4."""
        return await set_layout(n)

    @mcp.tool()
    async def set_slot_timeframe_tool(slot_id: int, timeframe: str) -> dict:
        """Set one slot's timeframe. timeframe in 15m,1H,4H,D,W,M."""
        return await set_slot_timeframe(slot_id, timeframe)

    @mcp.tool()
    async def list_assets() -> dict:
        """List the selectable assets (display labels)."""
        return {"ok": True, "assets": [display_label(a) for a in C.ASSETS]}

    @mcp.tool()
    async def list_timeframes() -> dict:
        """List the available timeframes in selector order (ascending)."""
        return {"ok": True, "timeframes": list(C.TIMEFRAMES)}

    # ---- indicators ----
    @mcp.tool()
    async def add_ema_tool(timeframe: str, period: int) -> dict:
        """Add an EMA(period) line to the chart on `timeframe`."""
        return await add_ema(timeframe, period)

    @mcp.tool()
    async def add_sma_tool(timeframe: str, period: int) -> dict:
        """Add an SMA(period) line to the chart on `timeframe`."""
        return await add_sma(timeframe, period)

    @mcp.tool()
    async def add_vwap_tool(timeframe: str, anchor_time: Optional[int] = None) -> dict:
        """Add or re-anchor an anchored VWAP on `timeframe`. anchor_time is
        UNIX seconds; default anchor = 14 days before the latest bar."""
        return await add_vwap(timeframe, anchor_time)

    @mcp.tool()
    async def add_volume_profile_tool(timeframe: str, start_time: Optional[int] = None,
                                      end_time: Optional[int] = None,
                                      bins: int = C.VP_BINS) -> dict:
        """Add or re-range a Volume Profile on `timeframe`. Returns poc/vah/val.
        start_time/end_time are UNIX seconds (default range = last 14 days)."""
        return await add_volume_profile(timeframe, start_time, end_time, bins)

    @mcp.tool()
    async def toggle_volume_pane_tool(timeframe: str, on: bool) -> dict:
        """Show (on=true) or hide the bottom volume histogram on `timeframe`."""
        return await toggle_volume_pane(timeframe, on)

    @mcp.tool()
    async def remove_indicator_tool(timeframe: str, indicator_id: str) -> dict:
        """Remove an indicator by id from `timeframe`."""
        return await remove_indicator(timeframe, indicator_id)

    @mcp.tool()
    async def list_indicators_tool(timeframe: str) -> dict:
        """List indicators currently on `timeframe`."""
        return list_indicators(timeframe)

    # ---- drawings ----
    @mcp.tool()
    async def draw_hline_tool(timeframe: str, price: float, label: Optional[str] = None,
                              color: Optional[str] = None) -> dict:
        """Draw a horizontal line at `price` on `timeframe`."""
        return await draw_hline(timeframe, price, label, color)

    @mcp.tool()
    async def draw_trendline_tool(timeframe: str, t1: int, price1: float, t2: int,
                                  price2: float, label: Optional[str] = None,
                                  color: Optional[str] = None) -> dict:
        """Draw a 2-point trendline on `timeframe`. t1/t2 are UNIX seconds."""
        return await draw_trendline(timeframe, t1, price1, t2, price2, label, color)

    @mcp.tool()
    async def clear_drawings_tool(timeframe: str) -> dict:
        """Remove all drawings from `timeframe`."""
        return await clear_drawings(timeframe)

    # ---- data / conditions ----
    @mcp.tool()
    async def get_candles_tool(timeframe: str, count: int = 200) -> dict:
        """Return recent OHLCV bars for `timeframe` (default 200)."""
        return get_candles(timeframe, count)

    @mcp.tool()
    async def get_indicator_values_tool(timeframe: str, indicator_id: str,
                                        count: int = 200) -> dict:
        """Return recent computed values for one indicator on `timeframe`."""
        return get_indicator_values(timeframe, indicator_id, count)

    @mcp.tool()
    async def check_condition(timeframe: str, test: str, period: Optional[int] = None,
                              fast: Optional[int] = None, slow: Optional[int] = None,
                              value: Optional[float] = None,
                              line_price: Optional[float] = None,
                              direction: Optional[str] = None) -> dict:
        """Evaluate a condition on the last CLOSED bar of `timeframe`.

        Supported `test` values and their params:
          close_above_ema / close_below_ema  -> period
          close_above_sma / close_below_sma  -> period
          close_above_vwap / close_below_vwap-> (uses chart's VWAP anchor)
          ema_cross / sma_cross              -> fast, slow
          price_above / price_below          -> value
          close_crossed_line                 -> line_price, direction(above/below)
          candle_bullish / candle_bearish    -> (none)
          above_poc / below_poc              -> (uses chart's VP range)

        Returns {result, test, timeframe, ...details, bar_time, confirmed:true}.
        """
        params: dict = {}
        if period is not None:
            params["period"] = period
        if fast is not None:
            params["fast"] = fast
        if slow is not None:
            params["slow"] = slow
        if value is not None:
            params["value"] = value
        if line_price is not None:
            params["line_price"] = line_price
        if direction is not None:
            params["direction"] = direction
        return evaluate(timeframe, test, **params)


def get_scene_state() -> dict:
    """Return a JSON-friendly snapshot of the scene (for the get_scene tool)."""
    return {
        "asset": _scene["asset"],
        "layout": _scene["layout"],
        "slots": [
            {"slot_id": s["slot_id"], "timeframe": s["timeframe"],
             "indicators": list(s["indicators"].values()),
             "drawings": list(s["drawings"])}
            for s in _scene["slots"]
        ],
    }
