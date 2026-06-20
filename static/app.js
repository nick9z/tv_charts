/* =============================================================
   tv_charts frontend logic.
   - Loads Lightweight Charts v4.2.x from the CDN, then boots.
   - Opens /ws, sends {type:"hello"}, and renders server-pushed messages.
   - The browser is a pure renderer: every structural/indicator/drawing change
     comes from the server, so the screen and the AI always agree.
   - window.chartAPI exposes the message handlers for console debugging.
   ============================================================= */
(function () {
  "use strict";

  const CFG = window.TVC_CONFIG || {};
  const UP = "#26a69a", DOWN = "#ef5350";

  // timeframe -> panel object. Charts are addressed by timeframe (unique).
  const panels = new Map();
  let layout = 4;
  let asset = { display: "", api_symbol: "" };
  let ws = null;

  // ---- dynamic load of Lightweight Charts, then boot ----
  function loadLWC() {
    return new Promise((resolve, reject) => {
      const s = document.createElement("script");
      s.src = CFG.lwcCdn;
      s.onload = resolve;
      s.onerror = () => reject(new Error("failed to load Lightweight Charts"));
      document.head.appendChild(s);
    });
  }

  // =========================================================
  // Top bar
  // =========================================================
  function initTopbar() {
    const aSel = document.getElementById("assetSelect");
    (CFG.assets || []).forEach((a) => {
      const o = document.createElement("option");
      o.value = a; o.textContent = a; aSel.appendChild(o);
    });
    aSel.addEventListener("change", () =>
      sendManual("set_asset", { symbol: aSel.value }));

    const lSel = document.getElementById("layoutSelect");
    (CFG.layouts || [1, 2, 4]).forEach((n) => {
      const o = document.createElement("option");
      o.value = String(n); o.textContent = String(n); lSel.appendChild(o);
    });
    lSel.addEventListener("change", () =>
      sendManual("set_layout", { n: Number(lSel.value) }));

    document.getElementById("refreshBtn").addEventListener("click", () =>
      sendManual("set_asset", { symbol: asset.display }));

    const log = document.getElementById("eventLog");
    document.getElementById("logToggle").addEventListener("click", () =>
      log.classList.toggle("hidden"));
    document.getElementById("logClear").addEventListener("click", () => {
      document.getElementById("eventLogList").innerHTML = "";
    });
  }

  function logEvent(text) {
    const ul = document.getElementById("eventLogList");
    if (!ul) return;
    const li = document.createElement("li");
    const t = new Date().toLocaleTimeString();
    li.innerHTML = `<span class="t">${t}</span>${text}`;
    ul.insertBefore(li, ul.firstChild);
    while (ul.childNodes.length > 200) ul.removeChild(ul.lastChild);
  }

  // =========================================================
  // Panel / chart construction
  // =========================================================
  function clearGrid() {
    panels.forEach((p) => { try { p.chart.remove(); } catch (e) {} });
    panels.clear();
    document.getElementById("grid").innerHTML = "";
  }

  function buildGrid(slots) {
    const grid = document.getElementById("grid");
    grid.className = "grid layout-" + layout;
    clearGrid();
    slots.forEach((slot) => createPanel(slot));
  }

  function createPanel(slot) {
    const grid = document.getElementById("grid");
    const root = document.createElement("section");
    root.className = "panel";

    // header: tf picker + price + indicator toggles
    const head = document.createElement("div");
    head.className = "panel-head";

    const tfPick = document.createElement("div");
    tfPick.className = "tf-picker";
    (CFG.timeframes || []).forEach((tf) => {
      const b = document.createElement("button");
      b.className = "tf-btn" + (tf === slot.timeframe ? " active" : "");
      b.textContent = tf;
      b.addEventListener("click", () =>
        sendManual("set_slot_timeframe", { slot_id: slot.slot_id, timeframe: tf }));
      tfPick.appendChild(b);
    });
    head.appendChild(tfPick);

    const price = document.createElement("div");
    price.className = "panel-price mono";
    price.innerHTML = `<span class="last">—</span> <span class="chg"></span>`;
    head.appendChild(price);

    const toggles = document.createElement("div");
    toggles.className = "ind-toggles";
    head.appendChild(toggles);

    root.appendChild(head);

    const host = document.createElement("div");
    host.className = "chart-host";
    root.appendChild(host);
    grid.appendChild(root);

    const chart = window.LightweightCharts.createChart(host, {
      autoSize: true,
      layout: { background: { color: "#11161f" }, textColor: "#d4dae3",
                fontFamily: "SF Mono, Menlo, monospace" },
      grid: { vertLines: { color: "#1a212c" }, horzLines: { color: "#1a212c" } },
      rightPriceScale: { borderColor: "#232b38" },
      timeScale: { borderColor: "#232b38", timeVisible: true, secondsVisible: false },
      crosshair: { mode: 0 },
    });
    const candleSeries = chart.addCandlestickSeries({
      upColor: UP, downColor: DOWN, borderUpColor: UP, borderDownColor: DOWN,
      wickUpColor: UP, wickDownColor: DOWN,
    });

    // VP overlay canvas (custom; Lightweight Charts has no native VP).
    const vpCanvas = document.createElement("canvas");
    vpCanvas.className = "vp-canvas";
    host.appendChild(vpCanvas);

    const panel = {
      slotId: slot.slot_id,
      timeframe: slot.timeframe,
      root, head, host, price, toggles, chart, candleSeries, vpCanvas,
      lineSeries: new Map(),     // indicator id -> line series
      volumeSeries: null,
      drawObjs: new Map(),       // drawing id -> {type, ref}
      vpLines: [],               // poc/vah/val price lines
      vpData: null,              // last vp buckets+meta for redraw
      firstClose: null,
      lastClose: null,
      indicatorIds: new Set(),
    };
    panels.set(slot.timeframe, panel);

    renderToggles(panel, slot.indicators || {});
    chart.timeScale().fitContent();

    // redraw VP on pan/zoom/resize
    chart.timeScale().subscribeVisibleTimeRangeChange(() => drawVP(panel));
    const ro = new ResizeObserver(() => { sizeVP(panel); drawVP(panel); });
    ro.observe(host);
    return panel;
  }

  function renderToggles(panel, indicators) {
    const have = (kind) =>
      Object.values(indicators).some((c) => c.kind === kind);
    const defs = [
      { key: "EMA", kind: "ema" },
      { key: "SMA", kind: "sma" },
      { key: "VWAP", kind: "vwap" },
      { key: "VP", kind: "vp" },
      { key: "Vol", kind: "volume" },
    ];
    panel.toggles.innerHTML = "";
    defs.forEach((d) => {
      const b = document.createElement("button");
      b.className = "ind-btn" + (have(d.kind) ? " on" : "");
      b.textContent = d.key;
      b.addEventListener("click", () => toggleIndicator(panel, d.kind));
      panel.toggles.appendChild(b);
    });
  }

  function toggleIndicator(panel, kind) {
    const tf = panel.timeframe;
    if (kind === "ema" || kind === "sma") {
      const def = kind === "ema" ? "50" : "200";
      const p = window.prompt(`${kind.toUpperCase()} period`, def);
      if (!p) return;
      const id = `${kind}-${parseInt(p, 10)}`;
      if (panel.indicatorIds.has(id)) {
        sendManual("remove_indicator", { timeframe: tf, indicator_id: id });
      } else {
        sendManual("add_" + kind, { timeframe: tf, period: parseInt(p, 10) });
      }
    } else if (kind === "vwap") {
      if (panel.indicatorIds.has("vwap"))
        sendManual("remove_indicator", { timeframe: tf, indicator_id: "vwap" });
      else sendManual("add_vwap", { timeframe: tf });
    } else if (kind === "vp") {
      if (panel.indicatorIds.has("vp"))
        sendManual("remove_indicator", { timeframe: tf, indicator_id: "vp" });
      else sendManual("add_volume_profile", { timeframe: tf });
    } else if (kind === "volume") {
      sendManual("toggle_volume_pane", { timeframe: tf, on: !panel.volumeSeries });
    }
  }

  // =========================================================
  // Message handlers (also exposed on window.chartAPI)
  // =========================================================
  function applyScene(msg) {
    asset = msg.asset;
    layout = msg.layout;
    document.getElementById("assetSelect").value = asset.display;
    document.getElementById("layoutSelect").value = String(layout);
    buildGrid(msg.slots);
    logEvent(`scene: ${asset.display} layout ${layout}`);
  }

  function setCandles(msg) {
    const p = panels.get(msg.timeframe);
    if (!p) return;
    p.candleSeries.setData(msg.data.map((b) => ({
      time: b.time, open: b.open, high: b.high, low: b.low, close: b.close,
    })));
    if (msg.data.length) {
      p.firstClose = msg.data[0].close;
      updatePrice(p, msg.data[msg.data.length - 1].close);
    }
    p.chart.timeScale().fitContent();
  }

  function updateCandle(msg) {
    const p = panels.get(msg.timeframe);
    if (!p) return;
    const b = msg.bar;
    p.candleSeries.update({
      time: b.time, open: b.open, high: b.high, low: b.low, close: b.close,
    });
    if (p.volumeSeries) {
      p.volumeSeries.update({
        time: b.time, value: b.volume,
        color: b.close >= b.open ? UP : DOWN,
      });
    }
    updatePrice(p, b.close);
  }

  function updatePrice(p, close) {
    p.lastClose = close;
    const el = p.price;
    el.querySelector(".last").textContent = fmt(close);
    if (p.firstClose) {
      const chg = (close - p.firstClose) / p.firstClose * 100;
      const c = el.querySelector(".chg");
      c.textContent = (chg >= 0 ? "+" : "") + chg.toFixed(2) + "%";
      c.className = "chg " + (chg >= 0 ? "up" : "down");
    }
  }

  function drawIndicator(msg) {
    const p = panels.get(msg.timeframe);
    if (!p) return;
    p.indicatorIds.add(msg.id);
    if (msg.kind === "ema" || msg.kind === "sma" || msg.kind === "vwap") {
      let s = p.lineSeries.get(msg.id);
      if (!s) {
        s = p.chart.addLineSeries({
          color: msg.color, lineWidth: 2,
          priceLineVisible: false, lastValueVisible: false,
          crosshairMarkerVisible: false,
        });
        p.lineSeries.set(msg.id, s);
      }
      s.applyOptions({ color: msg.color });
      s.setData(msg.series);
    } else if (msg.kind === "volume") {
      if (!p.volumeSeries) {
        p.volumeSeries = p.chart.addHistogramSeries({
          priceFormat: { type: "volume" },
          priceScaleId: "vol",
          lastValueVisible: false,
        });
        p.chart.priceScale("vol").applyOptions({
          scaleMargins: { top: 0.82, bottom: 0 },
        });
      }
      p.volumeSeries.setData(msg.series);
    } else if (msg.kind === "vp") {
      p.vpData = { buckets: msg.series, meta: msg.meta, colors: msg.colors };
      drawVPLines(p);
      sizeVP(p);
      drawVP(p);
    }
    syncToggleState(p);
    logEvent(`+${msg.kind} ${msg.id} @ ${msg.timeframe}`);
  }

  function removeIndicator(msg) {
    const p = panels.get(msg.timeframe);
    if (!p) return;
    p.indicatorIds.delete(msg.id);
    const s = p.lineSeries.get(msg.id);
    if (s) { p.chart.removeSeries(s); p.lineSeries.delete(msg.id); }
    if (msg.id === "volume" && p.volumeSeries) {
      p.chart.removeSeries(p.volumeSeries); p.volumeSeries = null;
    }
    if (msg.id === "vp") { clearVP(p); p.vpData = null; }
    syncToggleState(p);
    logEvent(`-${msg.id} @ ${msg.timeframe}`);
  }

  function syncToggleState(p) {
    const kinds = new Set();
    p.indicatorIds.forEach((id) => kinds.add(id.split("-")[0]));
    if (p.volumeSeries) kinds.add("volume");
    [...p.toggles.children].forEach((b) => {
      const map = { EMA: "ema", SMA: "sma", VWAP: "vwap", VP: "vp", Vol: "volume" };
      b.classList.toggle("on", kinds.has(map[b.textContent]));
    });
  }

  // ---- drawings ----
  function drawHline(msg) {
    const p = panels.get(msg.timeframe);
    if (!p) return;
    const line = p.candleSeries.createPriceLine({
      price: msg.price, color: msg.color || "#facc15", lineWidth: 1,
      lineStyle: 2, axisLabelVisible: true, title: msg.label || "",
    });
    p.drawObjs.set(msg.id, { type: "hline", ref: line });
    logEvent(`hline ${fmt(msg.price)} @ ${msg.timeframe}`);
  }

  function drawTrendline(msg) {
    const p = panels.get(msg.timeframe);
    if (!p) return;
    const s = p.chart.addLineSeries({
      color: msg.color || "#22d3ee", lineWidth: 2,
      priceLineVisible: false, lastValueVisible: false,
      crosshairMarkerVisible: false,
    });
    const pts = [
      { time: msg.t1, value: msg.price1 },
      { time: msg.t2, value: msg.price2 },
    ].sort((a, b) => a.time - b.time);
    s.setData(pts);
    p.drawObjs.set(msg.id, { type: "trend", ref: s });
    logEvent(`trendline @ ${msg.timeframe}`);
  }

  function clearDrawings(msg) {
    const p = panels.get(msg.timeframe);
    if (!p) return;
    p.drawObjs.forEach((d) => {
      if (d.type === "hline") p.candleSeries.removePriceLine(d.ref);
      else if (d.type === "trend") p.chart.removeSeries(d.ref);
    });
    p.drawObjs.clear();
    logEvent(`clear drawings @ ${msg.timeframe}`);
  }

  // =========================================================
  // Volume Profile overlay (right-aligned horizontal bars + level lines)
  // =========================================================
  function sizeVP(p) {
    const r = p.host.getBoundingClientRect();
    const w = Math.max(80, Math.round(r.width * 0.18));
    p.vpCanvas.width = w * window.devicePixelRatio;
    p.vpCanvas.height = r.height * window.devicePixelRatio;
    p.vpCanvas.style.width = w + "px";
    p.vpCanvas.style.height = r.height + "px";
  }

  function drawVP(p) {
    const ctx = p.vpCanvas.getContext("2d");
    ctx.setTransform(window.devicePixelRatio, 0, 0, window.devicePixelRatio, 0, 0);
    const W = p.vpCanvas.width / window.devicePixelRatio;
    const H = p.vpCanvas.height / window.devicePixelRatio;
    ctx.clearRect(0, 0, W, H);
    if (!p.vpData || !p.vpData.buckets.length) return;
    const maxV = Math.max(...p.vpData.buckets.map((b) => b.volume)) || 1;
    const barColor = (p.vpData.colors && p.vpData.colors.bar) || "#5b6b8c";
    ctx.fillStyle = barColor + "80"; // ~50% opacity
    p.vpData.buckets.forEach((b) => {
      const y = p.candleSeries.priceToCoordinate(b.price);
      if (y === null) return;
      const bw = (b.volume / maxV) * (W - 4);
      const h = Math.max(1, H / p.vpData.buckets.length - 1);
      ctx.fillRect(W - bw, y - h / 2, bw, h);
    });
  }

  function drawVPLines(p) {
    clearVPLevels(p);
    if (!p.vpData || !p.vpData.meta) return;
    const m = p.vpData.meta;
    const cols = p.vpData.colors || {};
    const add = (price, color, title) => {
      if (price == null) return;
      p.vpLines.push(p.candleSeries.createPriceLine({
        price, color, lineWidth: 1, lineStyle: 0,
        axisLabelVisible: true, title,
      }));
    };
    add(m.poc, cols.poc || "#f0b90b", "POC");
    add(m.vah, cols.value || "#888888", "VAH");
    add(m.val, cols.value || "#888888", "VAL");
  }

  function clearVPLevels(p) {
    p.vpLines.forEach((l) => { try { p.candleSeries.removePriceLine(l); } catch (e) {} });
    p.vpLines = [];
  }

  function clearVP(p) {
    clearVPLevels(p);
    const ctx = p.vpCanvas.getContext("2d");
    ctx.clearRect(0, 0, p.vpCanvas.width, p.vpCanvas.height);
  }

  // =========================================================
  // WebSocket plumbing
  // =========================================================
  const dispatch = {
    scene: applyScene,
    candles: setCandles,
    candle_update: updateCandle,
    indicator: drawIndicator,
    remove_indicator: removeIndicator,
    drawing: (m) => (m.kind === "hline" ? drawHline(m) : drawTrendline(m)),
    clear_drawings: clearDrawings,
    ack: (m) => { if (!m.ok) logEvent("ack error: " + m.error); },
  };

  function connect() {
    const proto = location.protocol === "https:" ? "wss" : "ws";
    ws = new WebSocket(`${proto}://${location.host}/ws`);
    ws.onopen = () => {
      setConn(true);
      ws.send(JSON.stringify({ type: "hello" }));
    };
    ws.onclose = () => { setConn(false); setTimeout(connect, 1500); };
    ws.onerror = () => { try { ws.close(); } catch (e) {} };
    ws.onmessage = (ev) => {
      const msg = JSON.parse(ev.data);
      const fn = dispatch[msg.type];
      if (fn) fn(msg);
    };
  }

  function sendManual(action, params) {
    if (ws && ws.readyState === 1)
      ws.send(JSON.stringify(Object.assign({ type: "manual", action }, params)));
  }

  function setConn(live) {
    document.getElementById("connDot").classList.toggle("live", live);
  }

  function fmt(v) {
    if (v == null) return "—";
    const d = v >= 1000 ? 1 : v >= 1 ? 2 : 5;
    return Number(v).toLocaleString(undefined, { minimumFractionDigits: d, maximumFractionDigits: d });
  }

  // expose for console debugging (spec 8.4)
  window.chartAPI = {
    applyScene, setCandles, updateCandle, drawIndicator, removeIndicator,
    drawHline, drawTrendline, clearDrawings, panels,
  };

  // ---- boot ----
  loadLWC()
    .then(() => { initTopbar(); connect(); })
    .catch((e) => { document.body.innerHTML =
      `<p style="color:#ef5350;padding:20px">${e.message}</p>`; });
})();
