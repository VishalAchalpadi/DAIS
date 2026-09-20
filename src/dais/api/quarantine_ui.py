"""A single static HTML+JS page, served by DAIS's own FastAPI app at
GET /ui/quarantine/{spec_name}, for data ops to review quarantined rows
and resubmit corrections.

Deliberately not a Claude Artifact: an Artifact runs in a sandboxed
browser context whose CSP blocks fetch/XHR to arbitrary hosts (including
a user's own localhost API), so it can't call back into a running DAIS
instance. Serving the page from the same FastAPI app means its fetch()
calls are same-origin - no CORS/CSP problem, and no separate web server
to stand up.
"""
from __future__ import annotations

QUARANTINE_UI_HTML = """<!doctype html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>DAIS quarantine review - __SPEC_NAME__</title>
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
    --danger-text: #b3261e;
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
      --danger-text: #ff8a80;
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
  h1 code { font-family: var(--font-mono); font-weight: 600; background: var(--surface-2); padding: 0.1rem 0.4rem; border-radius: 5px; font-size: 0.85em; }
  h2 { font-size: 1.02rem; font-weight: 600; margin: 0 0 0.35rem; }
  .card { background: var(--surface); border: 1px solid var(--border); border-radius: 10px; padding: 1.25rem 1.4rem; margin-bottom: 1.25rem; }
  .connect-row { display: flex; gap: 0.6rem; align-items: center; flex-wrap: wrap; }
  input[type=password] {
    padding: 0.5rem 0.7rem; border: 1px solid var(--border); border-radius: 7px;
    font-family: inherit; background: var(--surface-2); color: var(--text); min-width: 220px;
  }
  input[type=password]:focus, button:focus-visible, .field-input:focus {
    outline: 2px solid var(--accent); outline-offset: 1px;
  }
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
  .table-wrap { overflow-x: auto; border: 1px solid var(--border); border-radius: 8px; }
  table { width: 100%; border-collapse: collapse; font-size: 0.87rem; }
  thead th {
    text-align: left; padding: 0.6rem 0.85rem; background: var(--surface-2);
    font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.05em; color: var(--text-dim);
    border-bottom: 1px solid var(--border); white-space: nowrap;
  }
  tbody td { padding: 0.55rem 0.85rem; border-bottom: 1px solid var(--border); vertical-align: top; }
  tbody tr:last-child td { border-bottom: none; }
  tbody tr:hover { background: var(--surface-2); }
  .record-link { cursor: pointer; color: var(--accent); font-weight: 500; }
  .record-link:hover { text-decoration: underline; }
  .reason-pill {
    display: inline-block; background: var(--err-bg); color: var(--err-text);
    padding: 0.15rem 0.55rem; border-radius: 999px; font-size: 0.76rem; margin: 0.1rem 0.25rem 0.1rem 0;
  }
  .field-input {
    width: 100%; min-width: 8ch; box-sizing: border-box; padding: 0.35rem 0.5rem;
    font-family: var(--font-mono); font-size: 0.83rem; border: 1px solid var(--border);
    border-radius: 6px; background: var(--surface); color: var(--text);
  }
  .msg { padding: 0.75rem 0.9rem; border-radius: 8px; margin-top: 1rem; font-size: 0.86rem; white-space: pre-wrap; font-family: var(--font-mono); }
  .msg.ok { background: var(--ok-bg); color: var(--ok-text); }
  .msg.err { background: var(--err-bg); color: var(--err-text); }
  .muted { color: var(--text-dim); font-size: 0.87rem; }
  .actions { display: flex; gap: 0.6rem; margin-top: 1rem; }
  section { display: none; }
  section.visible { display: block; }
</style>
</head>
<body>
  <header>
    <span class="eyebrow">DAIS &middot; data quality</span>
    <h1>Quarantine review &mdash; <code>__SPEC_NAME__</code></h1>
    <a href="/ui/quarantine" style="color: var(--text-dim); font-size: 0.85rem;">&larr; All specs</a>
  </header>

  <div class="card">
    <div class="connect-row">
      <input type="password" id="apiKey" placeholder="X-API-Key">
      <button class="primary" onclick="saveKeyAndLoad()">Connect</button>
      <span class="status-dot" id="connStatus">not connected</span>
    </div>
  </div>

  <section id="listSection" class="card">
    <h2>Pending quarantine records</h2>
    <p class="muted" id="emptyState"></p>
    <div class="table-wrap">
      <table>
        <thead><tr><th>File</th><th>Rows</th><th></th></tr></thead>
        <tbody id="recordRows"></tbody>
      </table>
    </div>
  </section>

  <section id="detailSection" class="card">
    <h2>Edit and resubmit: <span id="detailFileName"></span></h2>
    <p class="muted">Correct the values below, then resubmit. Every row is
    re-validated against this pipeline's quality rules and upserted straight
    into stage &mdash; if any row still fails, nothing is written.</p>
    <div class="table-wrap">
      <table>
        <thead id="detailHead"></thead>
        <tbody id="detailRows"></tbody>
      </table>
    </div>
    <div class="actions">
      <button class="primary" onclick="resubmit()">Resubmit corrected rows</button>
      <button onclick="backToList()">Back to list</button>
    </div>
    <div id="resubmitMsg"></div>
  </section>

<script>
const SPEC_NAME = "__SPEC_NAME__";
let currentQuarantineId = null;
let currentColumns = [];

function apiKey() { return document.getElementById("apiKey").value; }

function saveKeyAndLoad() {
  sessionStorage.setItem("dais_api_key", apiKey());
  loadList();
}

async function apiFetch(path, options) {
  const res = await fetch(path, {
    ...options,
    headers: { ...(options && options.headers), "X-API-Key": apiKey(), "Content-Type": "application/json" },
  });
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`${res.status}: ${body}`);
  }
  return res.json();
}

async function loadList() {
  const statusEl = document.getElementById("connStatus");
  try {
    const records = await apiFetch(`/pipelines/${SPEC_NAME}/quarantine`);
    statusEl.textContent = "connected";
    statusEl.className = "status-dot on";
    document.getElementById("listSection").classList.add("visible");
    document.getElementById("detailSection").classList.remove("visible");
    const tbody = document.getElementById("recordRows");
    tbody.innerHTML = "";
    document.getElementById("emptyState").textContent = records.length === 0 ? "No pending quarantine records." : "";
    records.forEach(r => {
      const tr = document.createElement("tr");
      tr.innerHTML = `<td class="record-link" onclick="openRecord('${encodeURIComponent(r.quarantine_id)}', '${r.file_name}')">${r.file_name}</td><td>${r.row_count}</td><td></td>`;
      tbody.appendChild(tr);
    });
  } catch (e) {
    statusEl.textContent = "error: " + e.message;
    statusEl.className = "status-dot err";
  }
}

async function openRecord(quarantineId, fileName) {
  currentQuarantineId = quarantineId;
  const rows = await apiFetch(`/pipelines/${SPEC_NAME}/quarantine/${quarantineId}`);
  document.getElementById("detailFileName").textContent = fileName;
  document.getElementById("listSection").classList.remove("visible");
  document.getElementById("detailSection").classList.add("visible");
  document.getElementById("resubmitMsg").innerHTML = "";

  currentColumns = rows.length ? Object.keys(rows[0].row_data) : [];
  document.getElementById("detailHead").innerHTML =
    "<tr>" + currentColumns.map(c => `<th>${c}</th>`).join("") + "<th>Reasons</th></tr>";

  const tbody = document.getElementById("detailRows");
  tbody.innerHTML = "";
  rows.forEach(row => {
    const tr = document.createElement("tr");
    tr.dataset.rowIndex = row.row_index;
    tr.innerHTML =
      currentColumns.map(c => `<td><input class="field-input" data-col="${c}" value="${(row.row_data[c] ?? "").toString().replace(/"/g, "&quot;")}"></td>`).join("") +
      `<td>${row.reasons.map(r => `<span class="reason-pill">${r}</span>`).join("")}</td>`;
    tbody.appendChild(tr);
  });
}

function backToList() {
  loadList();
}

async function resubmit() {
  const rowsPayload = [...document.querySelectorAll("#detailRows tr")].map(tr => {
    const row_data = {};
    tr.querySelectorAll("input[data-col]").forEach(input => { row_data[input.dataset.col] = input.value; });
    return { row_index: parseInt(tr.dataset.rowIndex, 10), row_data };
  });

  const msgEl = document.getElementById("resubmitMsg");
  msgEl.innerHTML = "";
  try {
    const result = await apiFetch(`/pipelines/${SPEC_NAME}/quarantine/${currentQuarantineId}/resubmit`, {
      method: "POST",
      body: JSON.stringify({ rows: rowsPayload }),
    });
    if (result.accepted) {
      let text = `Accepted - ${result.row_count_upserted} row(s) upserted into stage. Record marked resolved.`;
      if (result.layer_reached === "gold") {
        text += "\\nGold refreshed successfully.";
      } else if (result.gold_error) {
        text += `\\nStage updated, but the gold refresh failed:\\n${result.gold_error}`;
      }
      msgEl.innerHTML = `<div class="msg ${result.gold_error ? 'err' : 'ok'}">${text}</div>`;
    } else {
      const details = result.failures.map(f => `row ${f.row_index ?? "?"}: ${(f.reasons || [f.error]).join(", ")}`).join("\\n");
      msgEl.innerHTML = `<div class="msg err">Rejected - one or more rows still fail validation, nothing was written:\\n${details}</div>`;
    }
  } catch (e) {
    msgEl.innerHTML = `<div class="msg err">${e.message}</div>`;
  }
}

const savedKey = sessionStorage.getItem("dais_api_key");
if (savedKey) {
  document.getElementById("apiKey").value = savedKey;
  loadList();
}
</script>
</body>
</html>
"""
