"""A single static HTML+JS page, served by DAIS's own FastAPI app at
GET /ui/spec-editor (new spec) or GET /ui/spec-editor/{spec_name} (edit an
existing one), for authoring pipeline spec YAML files without hand-writing
YAML.

The form is generated at runtime from PipelineSpec.model_json_schema() (see
GET /spec-schema in app.py) rather than hand-coded to match models.py field
by field: every Literal[...] in models.py becomes a dropdown automatically,
and a new/changed field shows up here with no UI change required, the same
"spec is the single source of truth" principle models.py itself follows.
The one field the JSON Schema can't fully describe - quality.rules[].checks,
a bare-name-or-comparison union - gets a small hand-built widget instead of
a generic one.

Saving always re-validates through the real PipelineSpec model server-side
(POST /pipelines/{spec_name}/spec) before writing YAML, so the form is a
convenience for shaping input correctly, not a second source of truth for
what's valid - cross-field rules the JSON Schema can't express (e.g.
quality.integrity_mode == 'group_level' requiring quarantine.group_by)
surface as real validation errors on save, same as they would from the CLI.

Deliberately not a Claude Artifact, for the same reason as quarantine_ui.py:
an Artifact's sandboxed CSP can't fetch a user's own localhost API.
"""
from __future__ import annotations

SPEC_EDITOR_HTML = """<!doctype html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>DAIS spec editor</title>
<style>
  :root {
    color-scheme: light dark;
    --bg: #f5f6f8;
    --surface: #ffffff;
    --surface-2: #eef0f3;
    --border: #dde1e6;
    --text: #1b1f24;
    --text-dim: #62697a;
    --accent: #2f6fed;
    --accent-text: #ffffff;
    --ok-bg: #e5f6ec;
    --ok-text: #1c7c4d;
    --err-bg: #fdecec;
    --err-text: #b3261e;
    --font-sans: -apple-system, "Segoe UI", Roboto, sans-serif;
    --font-mono: ui-monospace, "SFMono-Regular", Menlo, Consolas, monospace;
  }
  @media (prefers-color-scheme: dark) {
    :root {
      --bg: #14171c;
      --surface: #1c2028;
      --surface-2: #232830;
      --border: #333a45;
      --text: #e7e9ed;
      --text-dim: #9aa2b1;
      --accent: #6ea2ff;
      --accent-text: #0b1220;
      --ok-bg: #123425;
      --ok-text: #4fd88a;
      --err-bg: #3a1717;
      --err-text: #ff8a80;
    }
  }
  * { box-sizing: border-box; }
  body {
    font-family: var(--font-sans);
    background: var(--bg);
    color: var(--text);
    max-width: 960px;
    margin: 0 auto;
    padding: 2.5rem 1.5rem 5rem;
    line-height: 1.5;
  }
  header { display: flex; flex-direction: column; gap: 0.3rem; margin-bottom: 1.75rem; }
  .eyebrow { font-size: 0.72rem; letter-spacing: 0.08em; text-transform: uppercase; color: var(--text-dim); font-weight: 600; }
  h1 { font-size: 1.5rem; margin: 0; font-weight: 650; }
  h1 code { font-family: var(--font-mono); font-weight: 600; background: var(--surface-2); padding: 0.1rem 0.4rem; border-radius: 5px; font-size: 0.85em; }
  .card { background: var(--surface); border: 1px solid var(--border); border-radius: 10px; padding: 1.1rem 1.3rem; margin-bottom: 1.1rem; }
  .card.nested { background: var(--surface-2); margin: 0.5rem 0 0; }
  fieldset.card { border: 1px solid var(--border); }
  legend { padding: 0 0.35rem; font-weight: 600; font-size: 0.88rem; color: var(--text-dim); }
  .connect-row { display: flex; gap: 0.6rem; align-items: center; flex-wrap: wrap; }
  input[type=password], input[type=text], input[type=number], select, textarea {
    padding: 0.4rem 0.6rem; border: 1px solid var(--border); border-radius: 7px;
    font-family: inherit; background: var(--surface); color: var(--text); font-size: 0.88rem;
  }
  input[type=password] { min-width: 220px; }
  input:focus, select:focus, button:focus-visible, textarea:focus { outline: 2px solid var(--accent); outline-offset: 1px; }
  button {
    padding: 0.42rem 0.85rem; border: 1px solid var(--border); border-radius: 7px;
    background: var(--surface-2); color: var(--text); cursor: pointer; font-family: inherit;
    font-size: 0.85rem; font-weight: 500;
  }
  button:hover { background: var(--border); }
  button.primary { background: var(--accent); color: var(--accent-text); border-color: var(--accent); }
  button.primary:hover { filter: brightness(1.08); }
  button.remove-btn { font-size: 0.76rem; padding: 0.25rem 0.55rem; }
  .status-dot { display: inline-flex; align-items: center; gap: 0.4rem; font-size: 0.82rem; color: var(--text-dim); }
  .status-dot::before { content: ""; width: 0.5rem; height: 0.5rem; border-radius: 50%; background: var(--text-dim); }
  .status-dot.on::before { background: var(--ok-text); }
  .status-dot.err::before { background: var(--err-text); }
  .field { margin-bottom: 0.7rem; }
  .field > label { display: block; font-size: 0.8rem; font-weight: 600; color: var(--text-dim); margin-bottom: 0.25rem; }
  .field-header { display: flex; align-items: center; gap: 0.5rem; }
  .field-header > label { margin-bottom: 0; }
  .field input[type=text], .field input[type=number], .field select { width: 100%; max-width: 420px; }
  .array-field { display: flex; flex-direction: column; gap: 0.4rem; }
  .array-row { display: flex; gap: 0.5rem; align-items: flex-start; }
  .array-row > *:first-child { flex: 1; }
  .array-row.object-row { background: var(--surface-2); border-radius: 8px; padding: 0.6rem 0.7rem; }
  .check-row { display: flex; gap: 0.5rem; align-items: center; flex-wrap: wrap; }
  .const-badge { display: inline-block; padding: 0.2rem 0.5rem; border-radius: 6px; background: var(--surface-2); font-family: var(--font-mono); font-size: 0.82rem; }
  section { display: none; }
  section.visible { display: block; }
  .msg { padding: 0.75rem 0.9rem; border-radius: 8px; margin-top: 1rem; font-size: 0.85rem; white-space: pre-wrap; font-family: var(--font-mono); }
  .msg.ok { background: var(--ok-bg); color: var(--ok-text); }
  .msg.err { background: var(--err-bg); color: var(--err-text); }
  .muted { color: var(--text-dim); font-size: 0.85rem; }
  pre.yaml-out { background: var(--surface-2); border: 1px solid var(--border); border-radius: 8px; padding: 0.9rem 1rem; font-family: var(--font-mono); font-size: 0.82rem; overflow-x: auto; white-space: pre-wrap; }
  .actions { display: flex; gap: 0.6rem; margin-top: 1rem; align-items: center; }
</style>
</head>
<body>
  <header>
    <span class="eyebrow">DAIS &middot; spec editor</span>
    <h1 id="pageTitle">New pipeline spec</h1>
  </header>

  <div class="card">
    <div class="connect-row">
      <input type="password" id="apiKey" placeholder="X-API-Key">
      <select id="pipelinePicker"><option value="">-- new spec --</option></select>
      <button class="primary" onclick="connectAndLoad()">Connect &amp; load</button>
      <span class="status-dot" id="connStatus">not connected</span>
    </div>
  </div>

  <section id="formSection" class="card">
    <form id="specForm" onsubmit="return false;"></form>
    <div class="actions">
      <button class="primary" onclick="saveSpec()">Validate &amp; save</button>
      <span class="muted">Saving re-validates the whole spec server-side; nothing is written unless it's valid.</span>
    </div>
    <div id="saveMsg"></div>
    <div id="yamlOut"></div>
  </section>

<script>
const EMBEDDED_SPEC_NAME = "__SPEC_NAME__";
let schema = null;
let defs = null;
let checkNames = { bare: [], comparison_ops: [] };
let formData = {};
let editingExisting = false;

function apiKey() { return document.getElementById("apiKey").value; }

async function apiFetch(path, options) {
  const res = await fetch(path, {
    ...options,
    headers: { ...(options && options.headers), "X-API-Key": apiKey(), "Content-Type": "application/json" },
  });
  if (!res.ok) {
    const body = await res.text();
    const err = new Error(body);
    err.status = res.status;
    throw err;
  }
  return res.status === 204 ? null : res.json();
}

/* ---------- schema helpers ---------- */

function defsGet(ref) {
  return defs[ref.split("/").pop()];
}

function resolve(s) {
  if (!s) return s;
  if (s.$ref) {
    const { $ref, ...rest } = s;
    return { ...resolve(defsGet($ref)), ...rest };
  }
  if (s.allOf && s.allOf.length === 1) {
    const { allOf, ...rest } = s;
    return { ...resolve(allOf[0]), ...rest };
  }
  return s;
}

function extractOptional(s) {
  if (s.anyOf) {
    const nonNull = s.anyOf.filter((x) => x.type !== "null");
    const hasNull = s.anyOf.some((x) => x.type === "null");
    if (hasNull && nonNull.length === 1) {
      return { inner: resolve(nonNull[0]), nullable: true };
    }
  }
  return { inner: s, nullable: false };
}

function isCheckItemUnion(s) {
  if (!s.anyOf) return false;
  const kinds = s.anyOf.map((x) => resolve(x).type);
  return kinds.includes("string") && kinds.includes("object");
}

function defaultForSchema(rawSchema) {
  const { inner } = extractOptional(resolve(rawSchema));
  if (inner.const !== undefined) return inner.const;
  if (inner.default !== undefined && inner.default !== null) return inner.default;
  if (inner.enum) return inner.enum[0];
  if (inner.properties || inner.type === "object") return {};
  if (inner.type === "array") return [];
  if (inner.type === "boolean") return false;
  if (inner.type === "integer" || inner.type === "number") return 0;
  return "";
}

/* ---------- data path helpers (formData is the single source of truth) ---------- */

function getPath(obj, path) {
  return path.reduce((acc, k) => (acc == null ? undefined : acc[k]), obj);
}

function setPath(obj, path, value) {
  let cur = obj;
  for (let i = 0; i < path.length - 1; i++) {
    const k = path[i];
    if (cur[k] == null) cur[k] = typeof path[i + 1] === "number" ? [] : {};
    cur = cur[k];
  }
  const last = path[path.length - 1];
  if (value === undefined) delete cur[last];
  else cur[last] = value;
}

/* ---------- DOM builder ---------- */

function el(tag, attrs, children) {
  const e = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs || {})) {
    if (k === "class") e.className = v;
    else if (k.startsWith("on")) e.addEventListener(k.slice(2), v);
    else if (v !== undefined && v !== null) e.setAttribute(k, v);
  }
  (children || []).forEach((c) => e.appendChild(typeof c === "string" ? document.createTextNode(c) : c));
  return e;
}

function buildText(rawSchema, path) {
  const s = resolve(rawSchema);
  const input = el("input", { type: "text" });
  const existing = getPath(formData, path);
  input.value = existing !== undefined && existing !== null ? existing : (s.default ?? "");
  setPath(formData, path, input.value);
  input.addEventListener("input", () => setPath(formData, path, input.value));
  return input;
}

function buildNumber(rawSchema, path) {
  const s = resolve(rawSchema);
  const input = el("input", { type: "number", step: s.type === "number" ? "any" : "1" });
  if (s.minimum !== undefined) input.min = s.minimum;
  if (s.exclusiveMinimum !== undefined) input.min = s.exclusiveMinimum;
  const existing = getPath(formData, path);
  const val = existing !== undefined && existing !== null ? existing : (s.default ?? "");
  input.value = val;
  if (val !== "") setPath(formData, path, Number(val));
  input.addEventListener("input", () => setPath(formData, path, input.value === "" ? undefined : Number(input.value)));
  return input;
}

function buildCheckbox(rawSchema, path) {
  const s = resolve(rawSchema);
  const cb = el("input", { type: "checkbox" });
  const existing = getPath(formData, path);
  cb.checked = existing !== undefined ? !!existing : !!s.default;
  setPath(formData, path, cb.checked);
  cb.addEventListener("change", () => setPath(formData, path, cb.checked));
  return cb;
}

function buildSelect(rawSchema, path) {
  const s = resolve(rawSchema);
  const sel = el("select");
  sel.appendChild(el("option", { value: "" }, ["-- select --"]));
  s.enum.forEach((o) => sel.appendChild(el("option", { value: String(o) }, [String(o)])));
  const existing = getPath(formData, path);
  const initial = existing !== undefined && existing !== null ? existing : s.default;
  if (initial !== undefined && initial !== null) {
    sel.value = String(initial);
    setPath(formData, path, initial);
  }
  sel.addEventListener("change", () => {
    if (sel.value === "") { setPath(formData, path, undefined); return; }
    const isNumeric = s.enum.every((o) => typeof o === "number");
    setPath(formData, path, isNumeric ? Number(sel.value) : sel.value);
  });
  return sel;
}

function buildConst(rawSchema, path) {
  const s = resolve(rawSchema);
  setPath(formData, path, s.const);
  return el("span", { class: "const-badge" }, [String(s.const)]);
}

function buildCheckWidget(path) {
  const wrap = el("div", { class: "check-row" });
  const current = getPath(formData, path);
  const isComparison = current !== null && typeof current === "object";

  const kindSel = el("select", {}, []);
  kindSel.appendChild(el("option", { value: "bare" }, ["bare check"]));
  kindSel.appendChild(el("option", { value: "comparison" }, ["comparison check"]));
  kindSel.value = isComparison ? "comparison" : "bare";

  const bareSel = el("select");
  checkNames.bare.forEach((n) => bareSel.appendChild(el("option", { value: n }, [n])));
  if (!isComparison && current) bareSel.value = current;

  const opSel = el("select");
  checkNames.comparison_ops.forEach((n) => opSel.appendChild(el("option", { value: n }, [n])));
  const valInput = el("input", { type: "number", step: "any" });
  if (isComparison) {
    const [op, val] = Object.entries(current)[0];
    opSel.value = op;
    valInput.value = val;
  }

  function sync() {
    if (kindSel.value === "bare") setPath(formData, path, bareSel.value);
    else setPath(formData, path, { [opSel.value]: valInput.value === "" ? 0 : Number(valInput.value) });
  }
  function toggleVis() {
    bareSel.style.display = kindSel.value === "bare" ? "" : "none";
    opSel.style.display = kindSel.value === "comparison" ? "" : "none";
    valInput.style.display = kindSel.value === "comparison" ? "" : "none";
  }
  kindSel.addEventListener("change", () => { toggleVis(); sync(); });
  bareSel.addEventListener("change", sync);
  opSel.addEventListener("change", sync);
  valInput.addEventListener("input", sync);
  toggleVis();
  sync();

  wrap.append(kindSel, bareSel, opSel, valInput);
  return wrap;
}

function buildArray(rawSchema, path) {
  const s = resolve(rawSchema);
  const itemSchema = resolve(s.items || { type: "string" });
  let current = getPath(formData, path);
  if (!current) { current = []; setPath(formData, path, current); }

  const container = el("div", { class: "array-field" });
  const useCheckWidget = isCheckItemUnion(itemSchema);
  const isObjectItem = !useCheckWidget && (itemSchema.properties || itemSchema.$ref);

  current.forEach((_, idx) => {
    const rowPath = [...path, idx];
    const row = el("div", { class: isObjectItem ? "array-row object-row" : "array-row" });
    row.appendChild(useCheckWidget ? buildCheckWidget(rowPath) : buildWidget(itemSchema, rowPath));
    row.appendChild(el("button", { type: "button", class: "remove-btn", onclick: () => { current.splice(idx, 1); rerender(); } }, ["Remove"]));
    container.appendChild(row);
  });

  container.appendChild(el("button", {
    type: "button",
    onclick: () => {
      current.push(useCheckWidget ? "not_null" : defaultForSchema(itemSchema));
      rerender();
    },
  }, ["+ Add"]));
  return container;
}

function buildWidget(rawSchema, path) {
  const s = resolve(rawSchema);
  if (s.const !== undefined) return buildConst(s, path);
  if (s.enum) return buildSelect(s, path);
  if (s.properties || (s.$ref && !s.type)) {
    if (!getPath(formData, path)) setPath(formData, path, {});
    const card = el("fieldset", { class: "card nested" });
    renderObject(s, path, card);
    return card;
  }
  if (s.type === "array") return buildArray(s, path);
  if (s.type === "boolean") return buildCheckbox(s, path);
  if (s.type === "integer" || s.type === "number") return buildNumber(s, path);
  return buildText(s, path);
}

function renderField(key, rawSchema, path, isRequired) {
  const resolved = resolve(rawSchema);
  const { inner, nullable } = extractOptional(resolved);
  const wrap = el("div", { class: "field" });

  if (nullable) {
    const enableCb = el("input", { type: "checkbox" });
    const body = el("div", { class: "nested-body" });
    const has = getPath(formData, path) !== undefined && getPath(formData, path) !== null;
    enableCb.checked = has;
    body.style.display = has ? "block" : "none";
    if (has) body.appendChild(buildWidget(inner, path));

    enableCb.addEventListener("change", () => {
      body.style.display = enableCb.checked ? "block" : "none";
      body.innerHTML = "";
      if (enableCb.checked) {
        setPath(formData, path, defaultForSchema(inner));
        body.appendChild(buildWidget(inner, path));
      } else {
        setPath(formData, path, undefined);
      }
    });
    wrap.appendChild(el("div", { class: "field-header" }, [el("label", {}, [key + " (optional)"]), enableCb]));
    wrap.appendChild(body);
    return wrap;
  }

  wrap.appendChild(el("label", {}, [key + (isRequired ? " *" : "")]));
  wrap.appendChild(buildWidget(inner, path));
  return wrap;
}

function renderObject(rawSchema, path, container) {
  const s = resolve(rawSchema);
  const required = new Set(s.required || []);
  for (const [key, propSchema] of Object.entries(s.properties || {})) {
    container.appendChild(renderField(key, propSchema, [...path, key], required.has(key)));
  }
}

function rerender() {
  const form = document.getElementById("specForm");
  form.innerHTML = "";
  renderObject(schema, [], form);
}

/* ---------- load / save ---------- */

async function loadPipelineList() {
  try {
    const names = await apiFetch("/pipelines");
    const picker = document.getElementById("pipelinePicker");
    picker.innerHTML = '<option value="">-- new spec --</option>';
    names.forEach((n) => picker.appendChild(el("option", { value: n }, [n])));
    if (EMBEDDED_SPEC_NAME) picker.value = EMBEDDED_SPEC_NAME;
  } catch (e) { /* picker is a convenience; ignore failure here */ }
}

async function connectAndLoad() {
  sessionStorage.setItem("dais_api_key", apiKey());
  const statusEl = document.getElementById("connStatus");
  const picked = document.getElementById("pipelinePicker").value;
  try {
    const schemaResp = await apiFetch("/spec-schema");
    schema = schemaResp;
    defs = schemaResp["$defs"] || {};
    checkNames = schemaResp["x_dais_check_names"] || { bare: [], comparison_ops: [] };
    statusEl.textContent = "connected";
    statusEl.className = "status-dot on";

    if (picked) {
      const spec = await apiFetch(`/pipelines/${encodeURIComponent(picked)}/spec`);
      formData = spec.data;
      editingExisting = true;
      document.getElementById("pageTitle").textContent = "Edit pipeline spec: " + picked;
    } else {
      formData = {};
      editingExisting = false;
      document.getElementById("pageTitle").textContent = "New pipeline spec";
    }
    document.getElementById("formSection").classList.add("visible");
    document.getElementById("yamlOut").innerHTML = "";
    document.getElementById("saveMsg").innerHTML = "";
    rerender();
  } catch (e) {
    statusEl.textContent = "error: " + e.message;
    statusEl.className = "status-dot err";
  }
}

async function saveSpec() {
  const msgEl = document.getElementById("saveMsg");
  const yamlEl = document.getElementById("yamlOut");
  msgEl.innerHTML = "";
  yamlEl.innerHTML = "";
  const name = formData.pipeline_name;
  if (!name) {
    msgEl.innerHTML = '<div class="msg err">pipeline_name is required.</div>';
    return;
  }
  try {
    const result = await apiFetch(`/pipelines/${encodeURIComponent(name)}/spec?overwrite=${editingExisting}`, {
      method: "POST",
      body: JSON.stringify(formData),
    });
    msgEl.innerHTML = `<div class="msg ok">Saved to ${result.path}</div>`;
    yamlEl.innerHTML = "";
    yamlEl.appendChild(el("pre", { class: "yaml-out" }, [result.yaml]));
    editingExisting = true;
    loadPipelineList();
  } catch (e) {
    if (e.status === 409) {
      if (confirm(`A spec named "${name}" already exists. Overwrite it?`)) {
        editingExisting = true;
        return saveSpec();
      }
      return;
    }
    let text = e.message;
    try {
      const parsed = JSON.parse(e.message);
      const detail = parsed.detail;
      if (Array.isArray(detail)) {
        text = detail.map((d) => `${(d.loc || []).join(".")}: ${d.msg}`).join("\\n");
      } else if (detail) {
        text = detail;
      }
    } catch (_) { /* not JSON, use raw message */ }
    msgEl.innerHTML = `<div class="msg err">${text}</div>`;
  }
}

const savedKey = sessionStorage.getItem("dais_api_key");
if (savedKey) document.getElementById("apiKey").value = savedKey;
loadPipelineList();
</script>
</body>
</html>
"""
