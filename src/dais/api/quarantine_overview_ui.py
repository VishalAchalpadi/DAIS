"""A single static HTML+JS page, served by DAIS's own FastAPI app at
GET /ui/quarantine, listing every pipeline with currently pending
quarantine records as a clickable tile. Clicking a tile opens that
pipeline's existing remediation UI (quarantine_ui.py); once every pending
row for a pipeline is resolved there, its tile disappears from this page
on the next load/refresh (the tile list is always a fresh read of current
state, never cached client-side).
"""
from __future__ import annotations

QUARANTINE_OVERVIEW_HTML = """<!doctype html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>DAIS exceptions overview</title>
<style>
  :root {
    color-scheme: light dark;
    /* Geode Capital Management brand palette */
    --bg: #ffffff;
    --surface: #ffffff;
    --surface-2: #f1f5f2;
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
    --font-sans: -apple-system, "Segoe UI", Roboto, sans-serif;
    --font-mono: ui-monospace, "SFMono-Regular", Menlo, Consolas, monospace;
  }
  @media (prefers-color-scheme: dark) {
    :root {
      --bg: #0f1411;
      --surface: #171d19;
      --surface-2: #202a24;
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
    }
  }
  * { box-sizing: border-box; }
  body {
    font-family: var(--font-sans);
    background: var(--bg);
    color: var(--text);
    max-width: 1040px;
    margin: 0 auto;
    padding: 2.5rem 1.5rem 4rem;
    line-height: 1.5;
  }
  header { display: flex; flex-direction: column; gap: 0.3rem; margin-bottom: 1.75rem; }
  .eyebrow { font-size: 0.72rem; letter-spacing: 0.08em; text-transform: uppercase; color: var(--brand-gold); font-weight: 700; }
  h1 { font-size: 1.5rem; margin: 0; font-weight: 650; }
  .card { background: var(--surface); border: 1px solid var(--border); border-radius: 10px; padding: 1.25rem 1.4rem; margin-bottom: 1.25rem; }
  .connect-row { display: flex; gap: 0.6rem; align-items: center; flex-wrap: wrap; }
  input[type=password] {
    padding: 0.5rem 0.7rem; border: 1px solid var(--border); border-radius: 7px;
    font-family: inherit; background: var(--surface-2); color: var(--text); min-width: 220px;
  }
  input[type=password]:focus, button:focus-visible { outline: 2px solid var(--accent); outline-offset: 1px; }
  button {
    padding: 0.5rem 1rem; border: 1px solid var(--border); border-radius: 7px;
    background: var(--surface-2); color: var(--text); cursor: pointer; font-family: inherit;
    font-size: 0.88rem; font-weight: 500; transition: background 0.12s ease;
  }
  button:hover { background: var(--border); }
  button.primary { background: var(--accent); color: var(--accent-text); border-color: var(--accent); }
  button.primary:hover { filter: brightness(1.08); }
  .status-dot { display: inline-flex; align-items: center; gap: 0.4rem; font-size: 0.82rem; color: var(--text-dim); }
  .status-dot::before { content: ""; width: 0.5rem; height: 0.5rem; border-radius: 50%; background: var(--text-dim); }
  .status-dot.on::before { background: var(--ok-text); }
  .status-dot.err::before { background: var(--err-text); }
  .muted { color: var(--text-dim); font-size: 0.87rem; }
  .tiles { display: grid; grid-template-columns: repeat(auto-fill, minmax(220px, 1fr)); gap: 1rem; }
  .tile {
    display: block; text-align: left; background: var(--surface); border: 1px solid var(--border);
    border-radius: 12px; padding: 1.1rem 1.2rem; cursor: pointer; text-decoration: none; color: inherit;
    transition: border-color 0.12s ease, transform 0.12s ease;
  }
  .tile:hover { border-color: var(--accent); transform: translateY(-1px); }
  .tile-name { font-family: var(--font-mono); font-weight: 600; font-size: 0.95rem; word-break: break-all; }
  .tile-count {
    display: inline-flex; align-items: center; gap: 0.35rem; margin-top: 0.6rem;
    font-size: 0.78rem; color: var(--err-text); background: var(--err-bg);
    padding: 0.2rem 0.6rem; border-radius: 999px; font-weight: 600;
  }
  .empty-state {
    text-align: center; padding: 3rem 1rem; color: var(--text-dim);
  }
  .empty-state .big { font-size: 2rem; margin-bottom: 0.5rem; }
</style>
</head>
<body>
  <header>
    <span class="eyebrow">DAIS &middot; data quality</span>
    <h1>Exceptions overview</h1>
  </header>

  <div class="card">
    <div class="connect-row">
      <input type="password" id="apiKey" placeholder="X-API-Key">
      <button class="primary" onclick="saveKeyAndLoad()">Connect</button>
      <button onclick="loadSummary()">Refresh</button>
      <span class="status-dot" id="connStatus">not connected</span>
    </div>
  </div>

  <div id="tilesSection" class="card">
    <div id="tiles" class="tiles"></div>
  </div>

<script>
function apiKey() { return document.getElementById("apiKey").value; }

function saveKeyAndLoad() {
  sessionStorage.setItem("dais_api_key", apiKey());
  loadSummary();
}

async function apiFetch(path) {
  const res = await fetch(path, { headers: { "X-API-Key": apiKey() } });
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`${res.status}: ${body}`);
  }
  return res.json();
}

async function loadSummary() {
  const statusEl = document.getElementById("connStatus");
  const tilesEl = document.getElementById("tiles");
  try {
    const summaries = await apiFetch("/pipelines/quarantine-summary");
    statusEl.textContent = "connected";
    statusEl.className = "status-dot on";
    tilesEl.innerHTML = "";

    if (summaries.length === 0) {
      tilesEl.innerHTML = `
        <div class="empty-state" style="grid-column: 1 / -1;">
          <div class="big">&#10003;</div>
          <div>No pending exceptions across any pipeline.</div>
        </div>`;
      return;
    }

    summaries.forEach((s) => {
      const a = document.createElement("a");
      a.className = "tile";
      a.href = `/ui/quarantine/${encodeURIComponent(s.spec_name)}`;
      a.innerHTML = `
        <div class="tile-name">${s.spec_name}</div>
        <div class="tile-count">${s.pending_count} pending</div>
      `;
      tilesEl.appendChild(a);
    });
  } catch (e) {
    statusEl.textContent = "error: " + e.message;
    statusEl.className = "status-dot err";
  }
}

const savedKey = sessionStorage.getItem("dais_api_key");
if (savedKey) {
  document.getElementById("apiKey").value = savedKey;
  loadSummary();
}
</script>
</body>
</html>
"""
