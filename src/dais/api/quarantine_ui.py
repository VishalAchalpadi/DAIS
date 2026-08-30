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
<title>DAIS quarantine review - __SPEC_NAME__</title>
<style>
  :root { color-scheme: light dark; }
  body { font-family: -apple-system, "Segoe UI", sans-serif; max-width: 960px; margin: 2rem auto; padding: 0 1rem; }
  h1 { font-size: 1.3rem; }
  h1 code { font-weight: 600; }
  .row { display: flex; gap: 0.5rem; align-items: center; margin: 0.75rem 0; }
  input[type=text], input[type=password] { padding: 0.4rem 0.6rem; border: 1px solid #8888; border-radius: 6px; font-family: inherit; }
  button { padding: 0.45rem 0.9rem; border: 1px solid #8888; border-radius: 6px; background: #6663; cursor: pointer; font-family: inherit; }
  button:hover { background: #6665; }
  table { width: 100%; border-collapse: collapse; margin: 1rem 0; }
  th, td { text-align: left; padding: 0.5rem 0.6rem; border-bottom: 1px solid #8884; font-size: 0.9rem; }
  th { font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.04em; opacity: 0.7; }
  .record-link { cursor: pointer; color: #4a90d9; text-decoration: underline; }
  .reasons { color: #c0392b; font-size: 0.82rem; }
  .field-input { width: 100%; box-sizing: border-box; padding: 0.3rem 0.4rem; font-family: ui-monospace, monospace; font-size: 0.85rem; }
  .msg { padding: 0.6rem 0.8rem; border-radius: 6px; margin: 0.75rem 0; font-size: 0.88rem; white-space: pre-wrap; }
  .msg.ok { background: #2ecc7133; }
  .msg.err { background: #e74c3c33; }
  .muted { opacity: 0.65; font-size: 0.85rem; }
  section { display: none; }
  section.visible { display: block; }
</style>
</head>
<body>
  <h1>Quarantine review &mdash; <code>__SPEC_NAME__</code></h1>

  <div class="row">
    <input type="password" id="apiKey" placeholder="X-API-Key" size="30">
    <button onclick="saveKeyAndLoad()">Connect</button>
    <span class="muted" id="connStatus"></span>
  </div>

  <section id="listSection">
    <h2>Pending quarantine records</h2>
    <table>
      <thead><tr><th>File</th><th>Rows</th><th></th></tr></thead>
      <tbody id="recordRows"></tbody>
    </table>
    <p class="muted" id="emptyState"></p>
  </section>

  <section id="detailSection">
    <h2>Edit and resubmit: <span id="detailFileName"></span></h2>
    <p class="muted">Correct the values below, then resubmit. Every row is
    re-validated against this pipeline's quality rules and upserted straight
    into stage - if any row still fails, nothing is written.</p>
    <table>
      <thead id="detailHead"></thead>
      <tbody id="detailRows"></tbody>
    </table>
    <button onclick="resubmit()">Resubmit corrected rows</button>
    <button onclick="backToList()">Back to list</button>
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
      `<td class="reasons">${row.reasons.join("; ")}</td>`;
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
      msgEl.innerHTML = `<div class="msg ok">Accepted - ${result.row_count_upserted} row(s) upserted into stage. Record marked resolved.</div>`;
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
