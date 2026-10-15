"""A single static HTML+JS page, served by DAIS's own FastAPI app at
GET /ui/dashboard: a vertical sidebar listing every pipeline (GET
/pipelines), a run-parameters popup per pipeline that calls the existing
trigger endpoint (POST /pipelines/{name}/run) and polls its status (GET
/pipelines/runs/{run_id}/status) exactly like the Control-M wrapper script
and Dagster's own api_resource.py already do - no new backend endpoint,
this is a client for the existing one. A second sidebar tab reuses the
existing quarantine-summary endpoint to surface exception management
without leaving this page, handing off to the existing per-pipeline
remediation UI (quarantine_ui.py) for the actual row-level fix/resubmit
flow rather than reimplementing it here.

Run history is tracked client-side (sessionStorage), per pipeline, for
runs triggered from this screen - there is no backend endpoint today that
lists every past run for a given spec_name (RunRegistry only supports
lookup by run_id), so a run triggered elsewhere (CLI, Dagster, Control-M)
will not retroactively appear here. That is an accepted v1 limitation,
not an oversight.
"""
from __future__ import annotations

PIPELINE_DASHBOARD_HTML = """<!doctype html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>DAIS pipeline dashboard</title>
<style>
  :root {
    color-scheme: light dark;
    --bg: #ffffff;
    --surface: #ffffff;
    --surface-2: #f1f5f2;
    --sidebar-bg: #f6f8f6;
    --border: #dbe5df;
    --text: #141414;
    --text-dim: #5c6b62;
    --accent: #2f5941;
    --accent-text: #ffffff;
    --accent-secondary: #6fa37d;
    --brand-gold: #c99a2e;
    --ok-bg: #e9f3ec;
    --ok-text: #2f5941;
    --err-bg: #fdecec;
    --err-text: #b3261e;
    --warn-bg: #fdf3e2;
    --warn-text: #8a5a00;
    --font-sans: -apple-system, "Segoe UI", Roboto, sans-serif;
    --font-mono: ui-monospace, "SFMono-Regular", Menlo, Consolas, monospace;
  }
  @media (prefers-color-scheme: dark) {
    :root {
      --bg: #0f1411;
      --surface: #171d19;
      --surface-2: #202a24;
      --sidebar-bg: #111613;
      --border: #33413a;
      --text: #f2f5f3;
      --text-dim: #9ab0a2;
      --accent: #7cc49b;
      --accent-text: #0f1411;
      --accent-secondary: #4f8f6c;
      --brand-gold: #e8be5a;
      --ok-bg: #163326;
      --ok-text: #7cc49b;
      --err-bg: #3a1717;
      --err-text: #ff8a80;
      --warn-bg: #332a12;
      --warn-text: #e8be5a;
    }
  }
  * { box-sizing: border-box; }
  html, body { height: 100%; margin: 0; }
  body {
    font-family: var(--font-sans);
    background: var(--bg);
    color: var(--text);
    line-height: 1.5;
    display: flex;
  }

  /* ---------- sidebar ---------- */
  .sidebar {
    width: 280px; flex: 0 0 280px; height: 100vh; overflow-y: auto;
    background: var(--sidebar-bg); border-right: 1px solid var(--border);
    display: flex; flex-direction: column;
  }
  .sidebar-header { padding: 1.25rem 1.1rem 0.9rem; border-bottom: 1px solid var(--border); }
  .eyebrow { font-size: 0.7rem; letter-spacing: 0.08em; text-transform: uppercase; color: var(--brand-gold); font-weight: 700; }
  .sidebar-header h1 { font-size: 1.15rem; margin: 0.2rem 0 0.8rem; font-weight: 650; }
  .connect-row { display: flex; gap: 0.4rem; align-items: center; flex-wrap: wrap; }
  input[type=password], input[type=text], select {
    padding: 0.45rem 0.6rem; border: 1px solid var(--border); border-radius: 7px;
    font-family: inherit; background: var(--surface-2); color: var(--text); font-size: 0.85rem;
  }
  input[type=password] { min-width: 0; flex: 1; }
  input:focus, select:focus, button:focus-visible { outline: 2px solid var(--accent); outline-offset: 1px; }
  button {
    padding: 0.45rem 0.85rem; border: 1px solid var(--border); border-radius: 7px;
    background: var(--surface-2); color: var(--text); cursor: pointer; font-family: inherit;
    font-size: 0.85rem; font-weight: 500; transition: background 0.12s ease;
  }
  button:hover { background: var(--border); }
  button.primary { background: var(--accent); color: var(--accent-text); border-color: var(--accent); }
  button.primary:hover { filter: brightness(1.08); }
  button:disabled { opacity: 0.55; cursor: not-allowed; }
  .status-dot { display: inline-flex; align-items: center; gap: 0.35rem; font-size: 0.75rem; color: var(--text-dim); margin-top: 0.4rem; }
  .status-dot::before { content: ""; width: 0.45rem; height: 0.45rem; border-radius: 50%; background: var(--text-dim); flex: none; }
  .status-dot.on::before { background: var(--ok-text); }
  .status-dot.err::before { background: var(--err-text); }

  .sidebar-tabs { display: flex; border-bottom: 1px solid var(--border); }
  .sidebar-tab {
    flex: 1; text-align: center; padding: 0.7rem 0.5rem; font-size: 0.82rem; font-weight: 600;
    color: var(--text-dim); cursor: pointer; border-bottom: 2px solid transparent;
  }
  .sidebar-tab.active { color: var(--accent); border-bottom-color: var(--accent); }

  .pipeline-list { flex: 1; overflow-y: auto; padding: 0.5rem; }
  .pipeline-item {
    display: flex; align-items: center; justify-content: space-between; gap: 0.5rem;
    padding: 0.6rem 0.7rem; border-radius: 8px; cursor: pointer; margin-bottom: 0.15rem;
    font-family: var(--font-mono); font-size: 0.85rem; word-break: break-all;
  }
  .pipeline-item:hover { background: var(--surface-2); }
  .pipeline-item.active { background: var(--accent); color: var(--accent-text); }
  .pipeline-item .badge {
    font-family: var(--font-sans); font-size: 0.68rem; font-weight: 700; padding: 0.1rem 0.4rem;
    border-radius: 999px; background: var(--err-bg); color: var(--err-text); flex: none;
  }
  .pipeline-item.active .badge { background: rgba(255,255,255,0.25); color: var(--accent-text); }
  .empty-sidebar { padding: 1.5rem 1rem; color: var(--text-dim); font-size: 0.85rem; text-align: center; }

  /* ---------- main pane ---------- */
  .main { flex: 1; min-width: 0; height: 100vh; overflow-y: auto; padding: 2rem 2.2rem 4rem; }
  .main-placeholder { color: var(--text-dim); text-align: center; padding: 5rem 1rem; }
  .main-placeholder .big { font-size: 2rem; margin-bottom: 0.6rem; }

  .main-header { display: flex; align-items: center; justify-content: space-between; gap: 1rem; margin-bottom: 1.5rem; flex-wrap: wrap; }
  .main-header h2 { font-family: var(--font-mono); font-size: 1.35rem; margin: 0; }

  .card { background: var(--surface); border: 1px solid var(--border); border-radius: 10px; padding: 1.1rem 1.3rem; margin-bottom: 1.25rem; }
  .muted { color: var(--text-dim); font-size: 0.86rem; }

  .run-row {
    display: grid; grid-template-columns: 1fr auto auto; gap: 0.75rem; align-items: center;
    padding: 0.7rem 0; border-bottom: 1px solid var(--border);
  }
  .run-row:last-child { border-bottom: none; }
  .run-id { font-family: var(--font-mono); font-size: 0.82rem; }
  .run-meta { font-size: 0.78rem; color: var(--text-dim); margin-top: 0.15rem; }
  .pill { display: inline-block; font-size: 0.72rem; font-weight: 700; padding: 0.2rem 0.6rem; border-radius: 999px; white-space: nowrap; }
  .pill.running { background: var(--warn-bg); color: var(--warn-text); }
  .pill.succeeded { background: var(--ok-bg); color: var(--ok-text); }
  .pill.failed, .pill.quarantined { background: var(--err-bg); color: var(--err-text); }
  .pill.skipped { background: var(--surface-2); color: var(--text-dim); }
  .run-error { font-size: 0.78rem; color: var(--err-text); margin-top: 0.3rem; word-break: break-word; }

  /* ---------- modal ---------- */
  .modal-backdrop {
    position: fixed; inset: 0; background: rgba(0,0,0,0.45); display: flex;
    align-items: center; justify-content: center; z-index: 50; padding: 1rem;
  }
  .modal { background: var(--surface); border: 1px solid var(--border); border-radius: 12px;
    padding: 1.5rem 1.6rem; width: 100%; max-width: 440px; }
  .modal h3 { margin: 0 0 0.3rem; font-family: var(--font-mono); font-size: 1.1rem; }
  .modal .muted { margin-bottom: 1rem; }
  .field { margin-bottom: 1rem; }
  .field label { display: block; font-size: 0.8rem; font-weight: 600; margin-bottom: 0.3rem; }
  .field input, .field select { width: 100%; }
  .field .hint { font-size: 0.75rem; color: var(--text-dim); margin-top: 0.25rem; }
  .modal-actions { display: flex; justify-content: flex-end; gap: 0.5rem; margin-top: 1.3rem; }
  .modal-error { background: var(--err-bg); color: var(--err-text); border-radius: 8px; padding: 0.6rem 0.8rem; font-size: 0.82rem; margin-bottom: 1rem; }

  /* ---------- exceptions tab ---------- */
  .tiles { display: flex; flex-direction: column; gap: 0.3rem; padding: 0.5rem; }
  .exc-tile {
    display: flex; align-items: center; justify-content: space-between; padding: 0.6rem 0.7rem;
    border-radius: 8px; text-decoration: none; color: inherit; font-family: var(--font-mono); font-size: 0.85rem;
  }
  .exc-tile:hover { background: var(--surface-2); }

  /* ---------- catalog (OpenMetadata) tab ---------- */
  .catalog-panel { padding: 0.9rem 0.9rem 0; display: flex; flex-direction: column; gap: 0.5rem; }
  .catalog-panel .field label { font-size: 0.75rem; }
  .catalog-panel .field input { width: 100%; }
  .catalog-panel .hint { font-size: 0.74rem; color: var(--text-dim); }
  .catalog-panel .actions { display: flex; gap: 0.4rem; }
  .catalog-panel .actions button { flex: 1; font-size: 0.78rem; padding: 0.4rem 0.5rem; }
  .catalog-iframe-wrap { position: absolute; inset: 0; }
  .catalog-iframe-wrap iframe { width: 100%; height: 100%; border: 0; display: block; }
  .main.catalog-mode { padding: 0; position: relative; overflow: hidden; }
</style>
</head>
<body>

<div class="sidebar">
  <div class="sidebar-header">
    <span class="eyebrow">DAIS</span>
    <h1>Pipeline Dashboard</h1>
    <div class="connect-row">
      <input type="password" id="apiKey" placeholder="X-API-Key">
      <button class="primary" onclick="saveKeyAndLoad()">Connect</button>
    </div>
    <div class="status-dot" id="connStatus">not connected</div>
  </div>

  <div class="sidebar-tabs">
    <div class="sidebar-tab active" id="tabPipelines" onclick="switchTab('pipelines')">Pipelines</div>
    <div class="sidebar-tab" id="tabExceptions" onclick="switchTab('exceptions')">Exceptions</div>
    <div class="sidebar-tab" id="tabCatalog" onclick="switchTab('catalog')">Catalog</div>
  </div>

  <div class="pipeline-list" id="pipelineList">
    <div class="empty-sidebar">Connect with your API key to load pipelines.</div>
  </div>
</div>

<div class="main" id="main">
  <div class="main-placeholder">
    <div class="big">&larr;</div>
    <div>Connect, then select a pipeline from the sidebar.</div>
  </div>
</div>

<div id="modalRoot"></div>

<script>
const API_KEY_STORAGE = "dais_api_key";
const RUNS_STORAGE = "dais_dashboard_runs"; // { [spec_name]: [{run_id, started_at}, ...] }
const OM_URL_STORAGE = "dais_openmetadata_url";
const DEFAULT_OM_URL = "http://localhost:8585";
let currentTab = "pipelines";
let currentPipeline = null;
let pollTimers = {};

function apiKey() { return document.getElementById("apiKey").value; }

function saveKeyAndLoad() {
  sessionStorage.setItem(API_KEY_STORAGE, apiKey());
  if (currentTab === "pipelines") loadPipelines(); else loadExceptions();
}

async function apiFetch(path, options) {
  options = options || {};
  const headers = Object.assign({ "X-API-Key": apiKey() }, options.headers || {});
  const res = await fetch(path, Object.assign({}, options, { headers }));
  if (!res.ok) {
    let detail = await res.text();
    try { detail = JSON.parse(detail).detail || detail; } catch (e) {}
    throw new Error(typeof detail === "string" ? detail : JSON.stringify(detail));
  }
  return res.status === 204 ? null : res.json();
}

function setConnStatus(ok, message) {
  const el = document.getElementById("connStatus");
  el.textContent = message || (ok ? "connected" : "not connected");
  el.className = "status-dot" + (ok ? " on" : message ? " err" : "");
}

/* ---------------- run history (sessionStorage) ---------------- */

function loadRunsMap() {
  try { return JSON.parse(sessionStorage.getItem(RUNS_STORAGE)) || {}; }
  catch (e) { return {}; }
}
function saveRunsMap(map) { sessionStorage.setItem(RUNS_STORAGE, JSON.stringify(map)); }
function recordRun(specName, runId) {
  const map = loadRunsMap();
  map[specName] = map[specName] || [];
  map[specName].unshift({ run_id: runId, started_at: new Date().toISOString() });
  saveRunsMap(map);
}
function runsFor(specName) { return loadRunsMap()[specName] || []; }

/* ---------------- sidebar tabs ---------------- */

function switchTab(tab) {
  currentTab = tab;
  document.getElementById("tabPipelines").classList.toggle("active", tab === "pipelines");
  document.getElementById("tabExceptions").classList.toggle("active", tab === "exceptions");
  document.getElementById("tabCatalog").classList.toggle("active", tab === "catalog");
  document.getElementById("main").classList.toggle("catalog-mode", tab === "catalog");
  if (tab === "pipelines") loadPipelines();
  else if (tab === "exceptions") loadExceptions();
  else loadCatalog();
}

/* ---------------- pipelines tab ---------------- */

async function loadPipelines() {
  const listEl = document.getElementById("pipelineList");
  try {
    const names = await apiFetch("/pipelines");
    setConnStatus(true);
    if (names.length === 0) {
      listEl.innerHTML = '<div class="empty-sidebar">No pipeline specs found.</div>';
      return;
    }
    listEl.innerHTML = "";
    names.forEach((name) => {
      const item = document.createElement("div");
      item.className = "pipeline-item" + (name === currentPipeline ? " active" : "");
      item.textContent = name;
      item.onclick = () => selectPipeline(name);
      listEl.appendChild(item);
    });
  } catch (e) {
    setConnStatus(false, "error: " + e.message);
    listEl.innerHTML = '<div class="empty-sidebar">Could not load pipelines - check your API key.</div>';
  }
}

async function selectPipeline(name) {
  currentPipeline = name;
  document.querySelectorAll(".pipeline-item").forEach((el) => {
    el.classList.toggle("active", el.textContent === name);
  });
  renderMainForPipeline(name);
  openRunModal(name);
}

function renderMainForPipeline(name) {
  const main = document.getElementById("main");
  main.innerHTML = `
    <div class="main-header">
      <h2>${name}</h2>
      <button class="primary" onclick="openRunModal('${name}')">Run pipeline</button>
    </div>
    <div class="card">
      <div class="muted" style="margin-bottom:0.6rem;">
        Runs triggered from this dashboard (this browser session only - a run started via
        the CLI, Dagster, or Control-M won't retroactively appear here).
      </div>
      <div id="runList"></div>
    </div>
  `;
  renderRunList(name);
}

function renderRunList(name) {
  const runListEl = document.getElementById("runList");
  if (!runListEl) return; // user navigated away
  const runs = runsFor(name);
  if (runs.length === 0) {
    runListEl.innerHTML = '<div class="muted">No runs yet - click "Run pipeline" to start one.</div>';
    return;
  }
  runListEl.innerHTML = "";
  runs.forEach((r) => {
    const row = document.createElement("div");
    row.className = "run-row";
    row.id = "run-" + r.run_id;
    row.innerHTML = `
      <div>
        <div class="run-id">${r.run_id}</div>
        <div class="run-meta">started ${new Date(r.started_at).toLocaleString()}</div>
        <div class="run-error" style="display:none;"></div>
      </div>
      <div class="run-meta layer-cell"></div>
      <div class="pill running">running</div>
    `;
    runListEl.appendChild(row);
    pollRunStatus(name, r.run_id);
  });
}

async function pollRunStatus(specName, runId) {
  if (pollTimers[runId]) clearTimeout(pollTimers[runId]);
  const row = document.getElementById("run-" + runId);
  if (!row) return; // left this pipeline's view
  try {
    const status = await apiFetch(`/pipelines/runs/${runId}/status`);
    if (!document.getElementById("run-" + runId)) return; // view changed while awaiting
    const pill = row.querySelector(".pill");
    pill.textContent = status.status;
    pill.className = "pill " + status.status;
    row.querySelector(".layer-cell").textContent = status.layer_reached ? "layer: " + status.layer_reached : "";
    const errEl = row.querySelector(".run-error");
    if (status.error) { errEl.textContent = status.error; errEl.style.display = "block"; }

    if (status.status === "running") {
      pollTimers[runId] = setTimeout(() => pollRunStatus(specName, runId), 2000);
    }
  } catch (e) {
    const pill = row.querySelector(".pill");
    pill.textContent = "unknown";
    pill.className = "pill failed";
    row.querySelector(".run-error").textContent = e.message;
    row.querySelector(".run-error").style.display = "block";
  }
}

/* ---------------- run modal ---------------- */

async function openRunModal(name) {
  let spec = null;
  let specError = null;
  try {
    const resp = await apiFetch(`/pipelines/${encodeURIComponent(name)}/spec`);
    spec = resp.data;
  } catch (e) {
    specError = e.message;
  }

  const multiFile = !!(spec && spec.source && spec.source.location && spec.source.location.multi_file);
  // A spec with no gold: block (e.g. fx_rates_ingest, whose gold layer is a
  // separate gold_builds/*.yaml) rejects stop_after="gold" with a 400 (see
  // app.py's trigger_run) - omit that option entirely rather than letting
  // someone pick a combination the server will always refuse.
  const hasGold = !!(spec && spec.gold);
  const root = document.getElementById("modalRoot");
  root.innerHTML = `
    <div class="modal-backdrop" onclick="if (event.target === this) closeRunModal()">
      <div class="modal">
        <h3>Run ${name}</h3>
        <div class="muted">POST /pipelines/${name}/run</div>
        ${specError ? `<div class="modal-error">Could not load spec details: ${specError}</div>` : ""}
        <div id="modalError"></div>
        <div class="field">
          <label for="filePathInput">File path${multiFile ? " (optional)" : ""}</label>
          <input type="text" id="filePathInput" placeholder="${multiFile ? "leave blank to auto-discover matching file(s)" : "./data/.../your_file.csv"}">
          ${multiFile ? '<div class="hint">This spec has source.location.multi_file configured - omit to let the server discover matching file(s).</div>' : ""}
        </div>
        <div class="field">
          <label for="stopAfterInput">Stop after (optional override)</label>
          <select id="stopAfterInput">
            <option value="">(use spec default)</option>
            <option value="raw">raw</option>
            <option value="stage">stage</option>
            ${hasGold ? '<option value="gold">gold</option>' : ""}
          </select>
          ${!hasGold && !specError ? '<div class="hint">This spec has no gold: block - its gold layer, if any, is a separate gold_builds/*.yaml build triggered independently.</div>' : ""}
        </div>
        <div class="modal-actions">
          <button onclick="closeRunModal()">Cancel</button>
          <button class="primary" id="executeBtn" onclick="executeRun('${name}', ${multiFile})">Execute</button>
        </div>
      </div>
    </div>
  `;
}

function closeRunModal() {
  document.getElementById("modalRoot").innerHTML = "";
}

async function executeRun(name, multiFile) {
  const filePath = document.getElementById("filePathInput").value.trim();
  const stopAfter = document.getElementById("stopAfterInput").value;
  const errorEl = document.getElementById("modalError");
  const btn = document.getElementById("executeBtn");
  errorEl.innerHTML = "";

  if (!filePath && !multiFile) {
    errorEl.innerHTML = '<div class="modal-error">file_path is required (this spec has no multi_file discovery configured).</div>';
    return;
  }

  const body = {};
  if (filePath) body.file_path = filePath;
  if (stopAfter) body.stop_after = stopAfter;

  btn.disabled = true;
  btn.textContent = "Running...";
  try {
    const result = await apiFetch(`/pipelines/${encodeURIComponent(name)}/run`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    const runIds = result.run_ids || [result.run_id];
    runIds.forEach((id) => recordRun(name, id));
    closeRunModal();
    if (currentPipeline === name) {
      renderRunList(name);
    }
  } catch (e) {
    errorEl.innerHTML = `<div class="modal-error">${e.message}</div>`;
    btn.disabled = false;
    btn.textContent = "Execute";
  }
}

/* ---------------- exceptions tab ---------------- */

async function loadExceptions() {
  const listEl = document.getElementById("pipelineList");
  const main = document.getElementById("main");
  try {
    const summaries = await apiFetch("/pipelines/quarantine-summary");
    setConnStatus(true);
    if (summaries.length === 0) {
      listEl.innerHTML = '<div class="empty-sidebar">No pending exceptions across any pipeline.</div>';
    } else {
      listEl.innerHTML = '<div class="tiles" id="excTiles"></div>';
      const tilesEl = document.getElementById("excTiles");
      summaries.forEach((s) => {
        const a = document.createElement("a");
        a.className = "exc-tile";
        a.href = `/ui/quarantine/${encodeURIComponent(s.spec_name)}`;
        a.innerHTML = `<span>${s.spec_name}</span><span class="badge">${s.pending_count}</span>`;
        tilesEl.appendChild(a);
      });
    }
    main.innerHTML = `
      <div class="main-header"><h2>Exceptions</h2></div>
      <div class="card muted">
        Select a pipeline on the left to open its quarantine review &amp; resubmit screen.
        Each pipeline's badge shows its current pending-exception count.
      </div>
    `;
  } catch (e) {
    setConnStatus(false, "error: " + e.message);
    listEl.innerHTML = '<div class="empty-sidebar">Could not load exceptions - check your API key.</div>';
  }
}

/* ---------------- catalog tab (embedded OpenMetadata) ---------------- */

function openMetadataUrl() {
  return sessionStorage.getItem(OM_URL_STORAGE) || DEFAULT_OM_URL;
}

function loadCatalog() {
  // No DAIS API call needed for this tab - OpenMetadata is a separate
  // application with its own login, served in an iframe. The sidebar here
  // only controls which URL that iframe points at; connecting the top
  // apiKey field is unrelated to it.
  const url = openMetadataUrl();
  const listEl = document.getElementById("pipelineList");
  listEl.innerHTML = `
    <div class="catalog-panel">
      <div class="field">
        <label for="omUrlInput">OpenMetadata URL</label>
        <input type="text" id="omUrlInput" value="${url}" placeholder="${DEFAULT_OM_URL}">
        <div class="hint">Points the embedded view below at your OpenMetadata instance.</div>
      </div>
      <div class="actions">
        <button class="primary" onclick="saveOmUrlAndReload()">Load</button>
        <button onclick="openCatalogInNewTab()">Open in new tab</button>
      </div>
      <div class="hint">
        Logs in separately inside the embedded view (OpenMetadata has its own
        authentication, independent of the X-API-Key above). If the embed stays
        blank, your OpenMetadata deployment is likely blocking iframe embedding
        (X-Frame-Options/CSP) - use "Open in new tab" instead.
      </div>
    </div>
  `;

  const main = document.getElementById("main");
  main.innerHTML = `
    <div class="catalog-iframe-wrap">
      <iframe id="catalogFrame" src="${url}" title="OpenMetadata"></iframe>
    </div>
  `;
}

function saveOmUrlAndReload() {
  const url = document.getElementById("omUrlInput").value.trim() || DEFAULT_OM_URL;
  sessionStorage.setItem(OM_URL_STORAGE, url);
  const frame = document.getElementById("catalogFrame");
  if (frame) frame.src = url;
}

function openCatalogInNewTab() {
  const url = document.getElementById("omUrlInput").value.trim() || DEFAULT_OM_URL;
  window.open(url, "_blank", "noopener");
}

/* ---------------- init ---------------- */

const savedKey = sessionStorage.getItem(API_KEY_STORAGE);
if (savedKey) {
  document.getElementById("apiKey").value = savedKey;
  loadPipelines();
}
</script>
</body>
</html>
"""
