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

  // EMA/SMA toggle defaults (used the first time a kind is switched on with no
  // remembered periods).
  const DEFAULT_PERIODS = { ema: 9, sma: 21 };
  // Remembers the period(s) shown when EMA/SMA is toggled off, keyed
  // `${timeframe}:${kind}`, so toggling back on restores them. Persisted to
  // localStorage so the memory survives page reloads / browser restarts.
  const IND_MEMORY_KEY = "tvcharts.indMemory";
  const indMemory = loadIndMemory();

  function loadIndMemory() {
    try {
      const raw = localStorage.getItem(IND_MEMORY_KEY);
      if (raw) return new Map(Object.entries(JSON.parse(raw)));
    } catch (e) { /* storage unavailable / corrupt -> start empty */ }
    return new Map();
  }
  function saveIndMemory() {
    try {
      localStorage.setItem(IND_MEMORY_KEY, JSON.stringify(Object.fromEntries(indMemory)));
    } catch (e) { /* storage unavailable -> in-memory only this session */ }
  }

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

    // Refresh = refetch data only. Preserves indicators, drawings and zoom
    // (a non-destructive reload, not an asset switch / scene rebuild).
    document.getElementById("refreshBtn").addEventListener("click", () =>
      sendManual("refresh", {}));

    const syncBtn = document.getElementById("syncBtn");
    if (syncBtn) {
      syncBtn.classList.toggle("on", syncOn);
      syncBtn.addEventListener("click", () => {
        syncOn = !syncOn;
        saveSync();
        syncBtn.classList.toggle("on", syncOn);
        if (!syncOn) clearAllCrosshairs();
      });
    }

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

    // Slot badge: a chart is addressed by this id (stable by grid position), so
    // you can tell the AI "slot 2" even when two charts show the same timeframe.
    const badge = document.createElement("div");
    badge.className = "slot-badge";
    badge.textContent = slot.slot_id;
    badge.title = `Chart slot ${slot.slot_id}`;
    head.appendChild(badge);

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

    // Camera: save a PNG of this chart to the server's img/ folder.
    const cam = document.createElement("button");
    cam.className = "cam-btn";
    cam.textContent = "📷";
    cam.title = "Save a snapshot of this chart to img/";
    head.appendChild(cam);

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
      drawObjs: new Map(),       // drawing id -> {type, refs:[...]}
      vpLines: [],               // poc/vah/val price lines
      vpData: null,              // last vp buckets+meta for redraw
      firstClose: null,
      lastClose: null,
      indicatorIds: new Set(),
    };
    panels.set(slot.slot_id, panel);

    cam.addEventListener("click", () => capturePanel(panel));

    renderToggles(panel, slot.indicators || {});
    chart.timeScale().fitContent();

    // redraw VP on pan/zoom/resize
    chart.timeScale().subscribeVisibleTimeRangeChange(() => drawVP(panel));
    const ro = new ResizeObserver(() => { sizeVP(panel); drawVP(panel); });
    ro.observe(host);

    // crosshair sync: broadcast this panel's hovered time/price to the others
    chart.subscribeCrosshairMove((param) => onCrosshairMove(panel, param));
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
    const sid = panel.slotId;
    if (kind === "ema" || kind === "sma") {
      // Toggle on/off (no dialog). Periods in use are remembered across toggles,
      // keyed by slot (stable by grid position) so the memory survives reloads.
      const key = `${sid}:${kind}`;
      const current = [...panel.indicatorIds]
        .filter((id) => id.startsWith(kind + "-"))
        .map((id) => parseInt(id.split("-")[1], 10));
      if (current.length) {
        // currently ON -> remember the live period(s) and remove them
        indMemory.set(key, current);
        saveIndMemory();
        current.forEach((p) =>
          sendManual("remove_indicator", { slot_id: sid, indicator_id: `${kind}-${p}` }));
      } else {
        // currently OFF -> restore remembered period(s), or the first-time default
        const remembered = indMemory.get(key);
        const periods = (Array.isArray(remembered) && remembered.length)
          ? remembered : [DEFAULT_PERIODS[kind]];
        periods.forEach((p) =>
          sendManual("add_" + kind, { slot_id: sid, period: p }));
      }
    } else if (kind === "vwap") {
      if (panel.indicatorIds.has("vwap"))
        sendManual("remove_indicator", { slot_id: sid, indicator_id: "vwap" });
      else sendManual("add_vwap", { slot_id: sid });
    } else if (kind === "vp") {
      if (panel.indicatorIds.has("vp"))
        sendManual("remove_indicator", { slot_id: sid, indicator_id: "vp" });
      else sendManual("add_volume_profile", { slot_id: sid });
    } else if (kind === "volume") {
      sendManual("toggle_volume_pane", { slot_id: sid, on: !panel.volumeSeries });
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

  // Candles are shared across every chart on a timeframe (duplicates allowed),
  // so fan a candle message out to all panels showing that timeframe.
  function forEachPanelOnTf(tf, fn) {
    panels.forEach((p) => { if (p.timeframe === tf) fn(p); });
  }

  function setCandles(msg) {
    forEachPanelOnTf(msg.timeframe, (p) => {
      p.candleSeries.setData(msg.data.map((b) => ({
        time: b.time, open: b.open, high: b.high, low: b.low, close: b.close,
      })));
      if (msg.data.length) {
        p.firstClose = msg.data[0].close;
        updatePrice(p, msg.data[msg.data.length - 1].close);
      }
      // Fit only on first load; later refreshes keep the user's zoom/pan.
      if (!p.fitted) { p.chart.timeScale().fitContent(); p.fitted = true; }
    });
  }

  function updateCandle(msg) {
    const b = msg.bar;
    forEachPanelOnTf(msg.timeframe, (p) => {
      p.candleSeries.update({
        time: b.time, open: b.open, high: b.high, low: b.low, close: b.close,
      });
      if (p.volumeSeries) {
        p.volumeSeries.update({
          time: b.time, value: b.volume,
          color: b.close >= b.open ? UP : DOWN,
        });
      }
      // Keep the VP bars aligned as the price axis auto-scales / its width shifts.
      if (p.vpData) drawVP(p);
      updatePrice(p, b.close);
    });
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

  // EMA/SMA price-axis label derived from the indicator id ("ema-50" -> "EMA 50"),
  // so the period (the input value used) is always shown on the chart.
  function maLabel(msg) {
    const m = /^(ema|sma)-(\d+)$/.exec(msg.id || "");
    return m ? `${m[1].toUpperCase()} ${m[2]}` : "";
  }

  function drawIndicator(msg) {
    const p = panels.get(msg.slot_id);
    if (!p) return;
    p.indicatorIds.add(msg.id);
    if (msg.kind === "ema" || msg.kind === "sma" || msg.kind === "vwap") {
      const label = maLabel(msg);
      let s = p.lineSeries.get(msg.id);
      if (!s) {
        s = p.chart.addLineSeries({
          color: msg.color, lineWidth: 2,
          priceLineVisible: false,
          lastValueVisible: !!label,
          title: label,
          crosshairMarkerVisible: false,
        });
        p.lineSeries.set(msg.id, s);
      }
      s.applyOptions({ color: msg.color, title: label, lastValueVisible: !!label });
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
    logEvent(`+${msg.kind} ${msg.id} @ slot ${msg.slot_id} (${msg.timeframe})`);
  }

  function removeIndicator(msg) {
    const p = panels.get(msg.slot_id);
    if (!p) return;
    p.indicatorIds.delete(msg.id);
    const s = p.lineSeries.get(msg.id);
    if (s) { p.chart.removeSeries(s); p.lineSeries.delete(msg.id); }
    if (msg.id === "volume" && p.volumeSeries) {
      p.chart.removeSeries(p.volumeSeries); p.volumeSeries = null;
    }
    if (msg.id === "vp") { clearVP(p); p.vpData = null; }
    syncToggleState(p);
    logEvent(`-${msg.id} @ slot ${msg.slot_id} (${msg.timeframe})`);
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

  // ---- drawings (addressed by slot_id) ----
  // Each drawObjs entry collects whatever chart objects make up one drawing,
  // so hlines, trendlines and multi-line trade setups all remove uniformly.
  function makeDrawEntry() { return { priceLines: [], series: [] }; }

  function addPriceLine(p, entry, opts) {
    entry.priceLines.push(p.candleSeries.createPriceLine(opts));
  }

  function removeDrawObj(p, e) {
    e.priceLines.forEach((l) => { try { p.candleSeries.removePriceLine(l); } catch (x) {} });
    e.series.forEach((s) => { try { p.chart.removeSeries(s); } catch (x) {} });
  }

  function drawHline(msg) {
    const p = panels.get(msg.slot_id);
    if (!p) return;
    const e = makeDrawEntry();
    addPriceLine(p, e, {
      price: msg.price, color: msg.color || "#facc15", lineWidth: 1,
      lineStyle: 2, axisLabelVisible: true, title: msg.label || "",
    });
    p.drawObjs.set(msg.id, e);
    logEvent(`hline ${fmt(msg.price)} @ slot ${msg.slot_id}`);
  }

  function drawTrendline(msg) {
    const p = panels.get(msg.slot_id);
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
    const e = makeDrawEntry();
    e.series.push(s);
    p.drawObjs.set(msg.id, e);
    logEvent(`trendline @ slot ${msg.slot_id}`);
  }

  function drawSetup(msg) {
    const p = panels.get(msg.slot_id);
    if (!p) return;
    const cols = msg.colors || {};
    const tag = msg.label ? msg.label + " " : "";
    const e = makeDrawEntry();
    addPriceLine(p, e, {
      price: msg.entry, color: cols.entry || "#e0e0e0", lineWidth: 2,
      lineStyle: 0, axisLabelVisible: true, title: `${tag}${msg.direction} entry`,
    });
    addPriceLine(p, e, {
      price: msg.stop, color: cols.stop || "#ef5350", lineWidth: 1,
      lineStyle: 2, axisLabelVisible: true, title: "stop",
    });
    (msg.targets || []).forEach((t, i) => {
      addPriceLine(p, e, {
        price: t.price, color: cols.target || "#26a69a", lineWidth: 1,
        lineStyle: 2, axisLabelVisible: true, title: `TP${i + 1} (${t.rr}R)`,
      });
    });
    p.drawObjs.set(msg.id, e);
    logEvent(`setup ${msg.direction} (${(msg.targets || []).length} TP) @ slot ${msg.slot_id}`);
  }

  function dispatchDrawing(msg) {
    // Drawings are re-broadcast on every auto-refresh with their stable id.
    // Drop any existing chart objects for that id first, otherwise each redraw
    // would orphan the old price lines and stack duplicate labels over time.
    const p = panels.get(msg.slot_id);
    if (p) {
      const old = p.drawObjs.get(msg.id);
      if (old) { removeDrawObj(p, old); p.drawObjs.delete(msg.id); }
    }
    if (msg.kind === "hline") drawHline(msg);
    else if (msg.kind === "setup") drawSetup(msg);
    else drawTrendline(msg);
  }

  function removeDrawing(msg) {
    const p = panels.get(msg.slot_id);
    if (!p) return;
    const e = p.drawObjs.get(msg.id);
    if (!e) return;
    removeDrawObj(p, e);
    p.drawObjs.delete(msg.id);
    logEvent(`-drawing ${msg.id} @ slot ${msg.slot_id}`);
  }

  function clearDrawings(msg) {
    const p = panels.get(msg.slot_id);
    if (!p) return;
    p.drawObjs.forEach((e) => removeDrawObj(p, e));
    p.drawObjs.clear();
    logEvent(`clear drawings @ slot ${msg.slot_id}`);
  }

  // =========================================================
  // Volume Profile overlay (right-aligned horizontal bars + level lines)
  // =========================================================
  function sizeVP(p) {
    // Canvas spans the whole chart host; bars are right-aligned to the plot
    // area (left of the price axis) in drawVP so they never cover the labels.
    const r = p.host.getBoundingClientRect();
    const dpr = window.devicePixelRatio || 1;
    p.vpCanvas.width = Math.round(r.width * dpr);
    p.vpCanvas.height = Math.round(r.height * dpr);
    p.vpCanvas.style.width = r.width + "px";
    p.vpCanvas.style.height = r.height + "px";
  }

  function drawVP(p) {
    const dpr = window.devicePixelRatio || 1;
    const ctx = p.vpCanvas.getContext("2d");
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    const W = p.vpCanvas.width / dpr;
    const H = p.vpCanvas.height / dpr;
    ctx.clearRect(0, 0, W, H);
    if (!p.vpData || !p.vpData.buckets.length) return;
    // Right edge of the plotting area = canvas width minus the price-axis width,
    // so bars stop just left of the price labels instead of covering them.
    let axisW = 60;
    try {
      const w = p.chart.priceScale("right").width();
      if (w && w > 0) axisW = w;
    } catch (e) { /* older API -> use fallback width */ }
    const plotRight = Math.max(20, W - axisW - 2);
    const maxBar = Math.min(plotRight - 4, Math.max(40, plotRight * 0.2));
    const maxV = Math.max(...p.vpData.buckets.map((b) => b.volume)) || 1;
    const barColor = (p.vpData.colors && p.vpData.colors.bar) || "#5b6b8c";
    ctx.fillStyle = barColor + "80"; // ~50% opacity
    const h = Math.max(1, H / p.vpData.buckets.length - 1);
    p.vpData.buckets.forEach((b) => {
      const y = p.candleSeries.priceToCoordinate(b.price);
      if (y === null) return;
      const bw = (b.volume / maxV) * maxBar;
      ctx.fillRect(plotRight - bw, y - h / 2, bw, h);
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
  // Snapshot (camera) -> POST /snapshot -> img/
  // =========================================================
  function stampNow() {
    const d = new Date(), p2 = (n) => String(n).padStart(2, "0");
    return `${d.getFullYear()}-${p2(d.getMonth() + 1)}-${p2(d.getDate())} `
         + `${p2(d.getHours())}:${p2(d.getMinutes())}`;
  }

  function capturePanel(p) {
    try {
      const base = p.chart.takeScreenshot();      // LWC v4 returns a <canvas>
      const out = document.createElement("canvas");
      out.width = base.width;
      out.height = base.height;
      const ctx = out.getContext("2d");
      ctx.drawImage(base, 0, 0);
      // overlay the VP bars (a separate canvas) so the snapshot matches the screen
      if (p.vpCanvas && p.vpData) {
        try { ctx.drawImage(p.vpCanvas, 0, 0, out.width, out.height); } catch (e) {}
      }
      // burn a small corner label so the saved file is self-identifying
      const label = `${asset.display}  ${p.timeframe}  ${stampNow()}`;
      ctx.font = "12px monospace";
      const tw = ctx.measureText(label).width + 10;
      ctx.fillStyle = "rgba(10,14,20,0.72)";
      ctx.fillRect(4, 4, tw, 18);
      ctx.fillStyle = "#d4dae3";
      ctx.fillText(label, 9, 17);
      const image = out.toDataURL("image/png");
      fetch("/snapshot", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ asset_display: asset.display, timeframe: p.timeframe,
                               slot_id: p.slotId, image }),
      }).then((r) => r.json())
        .then((j) => logEvent(j.ok ? `📷 saved ${j.path}` : `📷 error: ${j.error}`))
        .catch((e) => logEvent(`📷 error: ${e.message}`));
    } catch (e) {
      logEvent(`📷 capture failed: ${e.message}`);
    }
  }

  // =========================================================
  // Crosshair sync across charts (global toggle, persisted)
  // =========================================================
  const SYNC_KEY = "tvcharts.crosshairSync";
  let syncOn = loadSync();
  let syncing = false;                            // re-entrancy guard

  function loadSync() {
    try { return localStorage.getItem(SYNC_KEY) === "1"; } catch (e) { return false; }
  }
  function saveSync() {
    try { localStorage.setItem(SYNC_KEY, syncOn ? "1" : "0"); } catch (e) {}
  }
  function clearAllCrosshairs() {
    panels.forEach((p) => { try { p.chart.clearCrosshairPosition(); } catch (e) {} });
  }

  function onCrosshairMove(srcPanel, param) {
    if (!syncOn || syncing) return;
    syncing = true;
    try {
      if (!param || !param.point || param.time == null) {
        panels.forEach((p) => {
          if (p !== srcPanel) { try { p.chart.clearCrosshairPosition(); } catch (e) {} }
        });
        return;
      }
      const price = srcPanel.candleSeries.coordinateToPrice(param.point.y);
      if (price == null) return;
      // Same time + price on every other chart (time is absolute UNIX seconds,
      // so this works even across different timeframes).
      panels.forEach((p) => {
        if (p === srcPanel) return;
        // setCrosshairPosition is a CHART method (price, time, series) in
        // Lightweight Charts v4 — not a series method.
        try { p.chart.setCrosshairPosition(price, param.time, p.candleSeries); } catch (e) {}
      });
    } finally {
      syncing = false;
    }
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
    drawing: dispatchDrawing,
    remove_drawing: removeDrawing,
    clear_drawings: clearDrawings,
    snapshot_request: (m) => { const p = panels.get(m.slot_id); if (p) capturePanel(p); },
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
