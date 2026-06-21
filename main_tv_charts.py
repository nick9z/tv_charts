# =============================================================
# Purpose:      Entry point for tv_charts. Builds the FastMCP server + FastAPI
#               app, wires the lifespan (DB wipe+fetch, Bybit WS task, MCP
#               session manager), serves the chart page + /ws bridge, mounts
#               the MCP streamable-HTTP endpoint at /mcp, and runs uvicorn.
# Runs:         `python main_tv_charts.py` -> serves on http://0.0.0.0:8800.
# Inputs:       config_tv_charts; Bybit market data; MCP + browser clients.
# Outputs:      One uvicorn process: web UI (/), browser bridge (/ws), MCP (/mcp).
# Dependencies: fastapi, uvicorn, mcp (FastMCP, streamable HTTP), functions_*.
# Risks:        MCP streamable-HTTP mounting/lifespan differs across mcp
#               versions -- verified against the installed mcp 1.28.0:
#               mount the streamable_http_app at "/" (its inner route is /mcp,
#               so no trailing-slash redirect) AFTER our own routes, and drive
#               session_manager.run() from this lifespan. DNS-rebinding
#               protection is disabled for trusted-LAN IP access.
# =============================================================

from contextlib import asynccontextmanager
import asyncio
import os

import uvicorn
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings

import config_tv_charts as C
import functions_tv_charts as F

STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")

# ---- MCP server --------------------------------------------------------
# stateless_http + json_response keeps ad-hoc LAN clients simple; DNS-rebinding
# protection is off so the Beelink's LAN IP Host header is accepted.
mcp = FastMCP(
    "tv_charts",
    stateless_http=True,
    json_response=True,
    transport_security=TransportSecuritySettings(enable_dns_rebinding_protection=False),
)
F.register_mcp_tools(mcp)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """App lifespan: init data layer, start the Bybit WS task, and run the MCP
    session manager (required for the streamable-HTTP transport)."""
    await F.startup()
    ws_task = asyncio.create_task(F.bybit_ws_loop())
    refresh_task = asyncio.create_task(F.refresh_loop())
    async with mcp.session_manager.run():
        try:
            yield
        finally:
            for t in (ws_task, refresh_task):
                t.cancel()
                try:
                    await t
                except asyncio.CancelledError:
                    pass
            await F.shutdown()


app = FastAPI(title="tv_charts", lifespan=lifespan)


# ---- Web UI + static ---------------------------------------------------
@app.get("/", response_class=HTMLResponse)
async def index():
    """Serve the chart terminal page."""
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))


@app.get("/config.js")
async def config_js():
    """Expose a tiny subset of config to the frontend (CDN url, tokens)."""
    body = (
        "window.TVC_CONFIG = {\n"
        f"  lwcCdn: {C.LWC_CDN!r},\n"
        f"  assets: {[F.display_label(a) for a in C.ASSETS]!r},\n"
        f"  timeframes: {list(C.TIMEFRAMES)!r},\n"
        f"  layouts: [1, 2, 4]\n"
        "};\n"
    )
    return HTMLResponse(content=body, media_type="application/javascript")


@app.post("/snapshot")
async def snapshot(request: Request):
    """Receive a base64 PNG from the browser and save it to img/.

    Body: {asset_display, timeframe, slot_id, image}. Returns {ok, path}.
    """
    try:
        body = await request.json()
        path = F.save_snapshot(body.get("asset_display", "chart"), body["image"])
        return JSONResponse({"ok": True, "path": path})
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"{type(e).__name__}: {e}"},
                            status_code=400)


app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


# ---- Browser bridge (/ws) ----------------------------------------------
@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    """Browser bridge. On 'hello' the server sends the full scene + per-slot
    data; 'manual' messages apply on-page user actions authoritatively."""
    await ws.accept()
    F.register_client(ws)
    try:
        while True:
            msg = await ws.receive_json()
            mtype = msg.get("type")
            if mtype == "hello":
                await F.push_full_scene(ws=ws)
            elif mtype == "manual":
                action = msg.get("action", "")
                params = {k: v for k, v in msg.items() if k not in ("type", "action")}
                await F.handle_manual(action, params)
    except WebSocketDisconnect:
        pass
    finally:
        F.unregister_client(ws)


# ---- MCP streamable HTTP at /mcp ---------------------------------------
# Mounted at "/" LAST: the inner app routes exactly /mcp (no redirect) and acts
# as the catch-all after our own routes above.
app.mount("/", mcp.streamable_http_app())


if __name__ == "__main__":
    uvicorn.run(app, host=C.HOST, port=C.PORT)
