---
name: dais-local-stack
description: Bring up the full local DAIS stack (API server, Dagster, Marquez), fix the recurring environment issues that break it, and author a new pipeline spec.yaml from a plain-language description of a data feed. Use whenever the user asks to start/restart Dagster, Marquez, or the DAIS API; asks why a UI or job is broken; or asks for a new spec.yaml.
---

# DAIS local stack: run it, fix it, author specs for it

This skill captures everything learned running DAIS's local stack (its own
FastAPI app, Dagster, and a from-source Marquez instance) end to end,
including every environment bug hit and fixed along the way. Re-read this
before touching any of these processes rather than rediscovering the same
failures.

## 1. The four services and how to start them

| Service | Command | Required env vars |
|---|---|---|
| DAIS API (spec editor, quarantine UI, docs, pipeline trigger) | `python -m dais.api.main` from the repo root | `DAIS_API_KEY`, `SECRETS_PROVIDER=hardcoded` |
| Dagster | `python -m dagster dev -f src/dais/orchestration/dagster/definitions.py` from the repo root | `DAGSTER_HOME`, `DAIS_API_KEY`, `SECRETS_PROVIDER=hardcoded` |
| Marquez API | `java -jar <marquez-clone>/api/build/libs/marquez-api-*.jar server marquez.local.yml`, cwd = the marquez clone dir | none (reads `marquez.local.yml`) |
| Marquez web | `npm run dev` from `<marquez-clone>/web`, needs Node on `PATH` | `MARQUEZ_HOST=localhost`, `MARQUEZ_PORT=5000` |

URLs once up:
- Dagster: `http://127.0.0.1:3000`
- DAIS spec editor: `http://localhost:8000/ui/spec-editor` (and `/ui/spec-editor/<name>` to edit an existing one)
- DAIS quarantine review: `http://localhost:8000/ui/quarantine/<spec_name>`
- DAIS API docs: `http://localhost:8000/docs`
- Marquez lineage graph: `http://localhost:1337`

API key for both DAIS UIs is whatever `DAIS_API_KEY` was set to (`dev-key` in this environment's convention).

### The #1 recurring mistake: `SECRETS_PROVIDER` set on only one process

`get_secrets_provider()` (`src/dais/secrets/factory.py`) defaults to a real
Vault client unless `SECRETS_PROVIDER=hardcoded` is set **in that specific
process's own environment** — there is no shared/global state. Both the
DAIS API server AND `dagster dev` independently resolve secrets in their
own process (the API server for pipeline runs; `dagster dev` directly,
inside `gold_assets.py`'s dbt run). Setting it on one and not the other
produces `Vault client is not authenticated - check VAULT_ADDR/VAULT_TOKEN`
from whichever process you forgot — this has happened repeatedly. Always
set `SECRETS_PROVIDER=hardcoded` before starting *every* Python process in
this stack, not just the one you're thinking about at the time.

Local secrets live in `secrets.local.yaml` at the repo root (real content:
`aurora_postgres_prod` → local Postgres creds).

### Windows/PowerShell specifics that have caused real confusion

- **Env vars set with `$env:X = ...` do NOT persist across separate
  PowerShell tool calls.** Set every env var the process needs in the same
  `Start-Process` call that launches it.
- **A venv `python.exe` on this machine is a trampoline** (uv-managed): it
  spawns the real interpreter as a child process. `Get-CimInstance
  Win32_Process` will show two PIDs with the identical command line for
  one logical process — this is normal, not a duplicate launch.
- **"Application Control policy has blocked this file"** can appear
  transiently, blocking ALL new process creation via that interpreter
  (even bare `--version`) while already-running processes keep working
  fine. This resolved itself between sessions in the one case it was hit;
  if seen, first confirm it's not this doc's own advice going stale by
  testing `python.exe --version` directly — if that alone fails, it's a
  machine-level block outside anything fixable from the coding session.
- **`npm`/`node` aren't on `PATH` for spawned processes** — always launch
  via `cmd.exe /c "set PATH=<nodeDir>;%PATH%&& cd /d ... && npm run dev"`,
  never bare `Start-Process npm`.
- **Before starting anything, check for stale processes on the target
  ports** (`netstat -ano | findstr ":8000 :3000 :5000 :1337"`). Long
  sessions accumulate orphaned `dagster dev`/`npm`/`java` processes from
  earlier restarts that silently keep answering on a port while you debug
  a freshly-started, never-actually-reachable duplicate.
- `dagster dev` bundles its own daemon (`AssetDaemon`, `SensorDaemon`,
  etc. — check its stdout log for the "Instance is configured with the
  following daemons" line). A separate `dagster-daemon run` process is
  unnecessary and will just fail (it needs its own workspace setup).

### Dagster: job discovery and automation

- Every spec under `specs/*.yaml` gets its own dedicated job
  (`<pipeline_name>_job`), regardless of whether any `gold_builds/*.yaml`
  depends on it — discovered by globbing `specs/`, not hardcoded.
- Each ingest job's op is named `<pipeline_name>__stage` (double
  underscore) — needed for Launchpad config:
  ```yaml
  resources:
    dais_api:
      config:
        api_key: {env: DAIS_API_KEY}
        base_url: http://localhost:8000
        poll_interval_seconds: 2
        timeout_seconds: 1800
  ops:
    <pipeline_name>__stage:
      config:
        file_path: "./data/<...>/incoming/<file>"
  ```
- A pipeline's own `gold:` block and a `gold_builds/*.yaml` build are two
  *different* things. A spec with no `gold:` block and `stop_after: stage`
  needs its gold logic (if any) to live in a separate `gold_builds/*.yaml`
  — that build's job (e.g. `regional_sales_gold_job`) needs no Launchpad
  config at all (the `dbt` resource is fully configured in Python).
- Gold dbt models carry `AutomationCondition.eager()` (source assets do
  not) so gold can auto-fire after stage — but the automation condition
  **sensor is off by default**. Turn it on once per code location:
  Dagster UI → **Overview → Automation** tab → toggle the
  `default_automation_condition_sensor` on. Without this, `eager()` is
  declared but never evaluated.

## 2. Marquez-from-source: known-fixed bugs, if ever rebuilding

The Marquez web clone needed several real fixes this session (not
DAIS bugs — bugs/version drift in Marquez's own vendored dependencies).
If the clone is ever redone from scratch, expect to hit these again, in
roughly this order:

1. **React 19 → 18 downgrade.** The clone's `package.json` pinned bleeding-edge
   `react@^19.2.4`, but `@mui/material@5.13.2`, `chakra-ui`, and others predate
   React 19 support. Pin `react`/`react-dom`/`@types/react`/`@types/react-dom`/
   `react-test-renderer` to `^18.3.x` and reinstall (`npm install
   --legacy-peer-deps`).
2. **`libs/graph` is a separate local package with its OWN `node_modules`**
   (installed via `"graph": "file:libs/graph"`). It needs the *same*
   downgrade applied to its own `package.json` (peerDependencies AND
   devDependencies) and its own `npm install` — the top-level fix does not
   propagate to it.
3. **SVG loader**: a duplicate `file-loader` rule (no issuer filter) was
   clobbering `@svgr/webpack`'s output for the same `.svg` files. Fix:
   exclude `svg` from the generic image `file-loader` rule, and chain
   `use: ['@svgr/webpack', 'file-loader']` on the SVG rule so imports get
   *both* a `ReactComponent` named export and a default URL export (CRA
   convention) from one import.
4. **`elkjs` "Worker is not a constructor"**: the `web-worker` npm package's
   `"exports"` map routes webpack to its ESM source under the `import`
   condition, but `elkjs` does a plain CJS `require('web-worker')`
   expecting the module itself to be the constructor. Fix: `resolve.alias`
   forcing `web-worker$` to its CJS build
   (`node_modules/web-worker/dist/browser/index.cjs`).
5. **`react-router` missing from `node_modules` entirely** —
   `@lagunovsky/redux-react-router` imports it directly, but only
   `react-router-dom` was a listed dependency. Install `react-router`
   explicitly, pinned to the exact version `react-router-dom` depends on.
6. **Duplicate React instances**: `libs/graph`'s own nested `react`/`react-dom`
   copy (even at the identical version) breaks hooks (`Cannot read
   properties of null (reading 'useRef')`) because React's hook dispatcher
   is a single mutable object per module instance. Fix: `resolve.alias`
   forcing `react`/`react-dom` to the one top-level copy, from anywhere.
   **Note**: this alias only affects webpack's runtime bundling, not
   `ts-loader`'s independent TypeScript resolution — a duplicate
   `@types/react` (e.g. reintroduced by a later `npm install` inside
   `libs/graph`) needs its own fix: physically delete
   `libs/graph/node_modules/@types/react{,-dom}` so type-checking falls
   through to the single top-level copy.
7. **`react-i18next` vs newer `@types/react`**: `react-i18next` globally
   augments `declare module 'react' { interface HTMLAttributes<T> {
   children?: ReactI18NextChildren } }`, a type that doesn't cleanly
   include `bigint` the way newer `@types/react`'s `ReactNode` does —
   surfaces as a `TS2430` error on any component extending an MUI/Chakra
   `BoxProps`. Fixed by patching `node_modules/react-i18next/index.d.ts`
   to revert that augmentation to plain `React.ReactNode`.
8. **Redundant `<BrowserRouter>`**: `App.tsx` already renders `<ReduxRouter>`
   (`@lagunovsky/redux-react-router`) as the app's one true router;
   `index.tsx` was ALSO wrapping it in `<BrowserRouter>`
   (`react-router-dom`), nesting two router providers ("You cannot render
   a `<Router>` inside another `<Router>`"). This bug was latent and
   invisible until fix #5 (react-router missing) was applied — before
   that the build failed outright. Fix: remove the outer `<BrowserRouter>`.
9. **`@uiw/react-json-view` alpha drift**: pinned `^2.0.0-alpha.24` (a caret
   on an alpha release), but `2.0.0-alpha.41` got installed — 17 alpha
   releases of undocumented breaking changes. Pin the exact version with no
   caret if this component (job/dataset facets JSON viewer) ever breaks
   again.
10. **Real app bug, not a dependency issue**: `ZoomPanSvg.tsx` had
    `{{children} as any}` instead of `{children}` — object-shorthand syntax
    wrapping `children` in `{children: ...}`, rendered directly as a JSX
    child. This is *exactly* React's "Objects are not valid as a React
    child (found: object with keys {children})" error, and it can look
    identical to a dependency-version symptom from the outside — don't
    assume it's a version issue without getting the **Component Stack**
    from the browser console (DevTools → Console → the block under
    "The above error occurred in the `<X>` component:") to find the actual
    component first.

General debugging lesson from this whole exercise: when a React error
gives only `react-dom` internal frames with no app code, **always ask for
the Component Stack before touching anything** — guessing at root cause
from a bare JS stack trace burned much more time than reading five extra
lines from the browser console would have.

## 3. Authoring a spec.yaml from a plain-language description

`PipelineSpec` (`src/dais/spec/models.py`) is the single source of truth
for the schema — read it directly (`Read` the file) before writing a spec
by hand, don't work from memory of past specs, since fields/validators can
change. `PipelineSpec.model_json_schema()` is also served live at
`GET /spec-schema` on the DAIS API and drives the spec-editor UI's
dropdowns — for a quick one-off, **prefer the UI**
(`http://localhost:8000/ui/spec-editor`) over hand-writing YAML, since it
enumerates every `Literal[...]`'s allowed values for you and validates
through the real model before saving. Hand-write only when scripting many
specs, or when learning the schema.

### Step-by-step method for a new pipeline

1. **Find and read the real source file(s)** before writing anything.
   Don't guess column names or structure from the request text alone — the
   actual shape of real data routinely differs from a plain-language
   description (see the FundsXML case below, where "an equity fund
   positions file" undersold how deeply nested the real values were).

2. **Identify the format and, for XML/JSON, verify the parser's actual
   capabilities against the real nesting** — don't assume the parser
   handles arbitrary structure. `src/dais/parsers/xml_parser.py`, for
   example, only flattens what it's told to (recursively, as of this
   session's fix, joining nested tag paths with `.` and attributes with
   `@`) from whatever `record_xpath` matches. Run the parser against a
   real sample file BEFORE writing quality rules, and read its actual
   output columns:
   ```python
   from dais.parsers import get_parser
   from dais.spec.models import ParserConfig
   df = get_parser("xml").parse(open(path, "rb").read(), ParserConfig(type="xml", record_xpath="..."))
   print(df.columns); print(df.to_dicts())
   ```
   Writing quality rules against columns that don't actually exist in the
   parsed output is the single easiest way to produce a spec that looks
   plausible and fails immediately on first real run.

3. **Map every field of `PipelineSpec` in order**, using the real column
   names discovered in step 2:
   - `pipeline_name` / `description` / `owner`: snake_case name matching
     `specs/<name>.yaml`; a one-line description of the feed; an owning
     team name (never leave a `<<placeholder>>` — the model rejects it).
   - `execution.stop_after`: `"raw"`, `"stage"`, or `"gold"`. Use `"stage"`
     if gold logic belongs in a separate `gold_builds/*.yaml` instead
     (Phase 9a convention — check whether one already exists or is
     planned before deciding).
   - `database` / `monitoring`: match the conventions of existing specs in
     `specs/` (read a couple first) — almost always
     `platform: postgres`, `connection: "aurora_postgres_prod"`,
     `monitoring: {schema: control, table: process_monitor}`.
   - `source` / `parser`: `parser.type` MUST equal `source.format` (a
     model validator enforces this). `source.location.file_pattern` is
     documentation only — it is never used to auto-discover files at
     runtime (the actual file path is always passed explicitly to
     `/pipelines/{spec}/run`), so a literal filename with no `{date}`
     token is fine if that's genuinely how the feed arrives.
   - `control_gates`: sane `min`/`max` bounds on file size and row count
     for the real file size you observed — not arbitrary round numbers.
   - `raw`: `preserve_metadata` almost always all five of
     `[file_name, file_path, file_checksum, load_timestamp, batch_id]`;
     `checksum_dedup: true` unless there's a specific reason not to.
   - `quality.integrity_mode`: `"strict"` (one bad row quarantines the
     whole file) vs `"row_level"` (only the bad row) vs `"group_level"`
     (a whole group sharing `quarantine.group_by`'s key is quarantined
     together) — pick based on whether records in the file are truly
     independent (row_level/strict) or logically grouped (e.g. one
     portfolio's multiple holdings — group_level).
   - `quality.rules`: one entry per real column that needs validation.
     Checks are either bare names (`not_null`, `non_empty`, `valid_date`,
     `is_numeric`) or single-key comparisons (`{greater_than_or_equal: 0}`,
     etc.) — see `BARE_CHECK_NAMES`/`COMPARISON_CHECK_OPS` in models.py
     for the exhaustive list, don't invent new check names. `valid_date`
     requires a `format` string. Numeric columns needing DB casting need
     `cast_to: decimal` plus `precision`/`scale` sized to the real values
     observed (e.g. a percentage needs less precision than a large
     currency amount).
   - `quality.quarantine`: `kind: local` for on-disk dev/demo pipelines,
     `s3` for anything pointing at a real bucket — `location` must match
     (`s3://...` vs a local path) or the model rejects it.
   - `stage.business_key` / `write_mode`: think concretely about whether
     the natural key you're about to declare is ACTUALLY unique across
     every file this pipeline will ever ingest, not just within one
     sample file. A per-record ID that resets/repeats across different
     dates or parent entities (e.g. `<Position><UniqueID>` scoped to one
     fund's one day, as opposed to a schema that carries the parent info
     down to each record) is not a safe `upsert` key — use `write_mode:
     append` instead and say so in a comment, rather than silently
     upserting into false collisions.
   - `gold` (optional, inline): only if this pipeline's own gold logic is
     simple and doesn't span other pipelines' data — otherwise it belongs
     in a separate `gold_builds/*.yaml` (`GoldBuildSpec`), which uses
     `depends_on` to name every pipeline it reads from and is what wires
     it into Dagster's asset graph.
   - `resilience.retry`: copy the convention from existing specs unless
     there's a specific reason to deviate (`max_attempts: 5, backoff:
     exponential, base_delay_seconds: 2, jitter: true`).
   - `lineage`: `namespace` should match the SAME namespace every other
     pipeline in this OpenLineage graph uses (so Marquez shows one
     connected graph, not islands) — check an existing spec's
     `lineage.namespace` rather than inventing a new one. `job_name`
     matches `pipeline_name`.
   - `anomaly_detection` (optional): only add if actually wanted; metric
     names are `"row_count"` or `"<column>_<sum|avg|null_rate|distinct_count>"`
     — the column must be one that genuinely exists in the parsed output.

4. **Validate, then run for real** before calling it done:
   ```python
   from dais.spec.loader import load_spec
   spec = load_spec("specs/<name>.yaml")  # raises with a clear message if invalid
   ```
   Then actually run it end to end against local Postgres (a scratch
   schema, not a production one) using the same pattern
   `tests/conftest.py`'s fixtures use (`HardcodedSecretsProvider` /
   `SECRETS_PROVIDER=hardcoded`, real `PostgresConnector`, real
   `run_pipeline(...)`) and inspect the actual landed rows — a spec that
   merely loads/validates is not proof it works. Clean up the scratch
   schema afterward.

5. **Write regression tests for any code change the spec exposed** (e.g. a
   parser bug the new format revealed) — don't just fix the immediate spec
   and move on; a bug in shared code (`src/dais/parsers/`, `src/dais/...`)
   will resurface for every future spec with similar shape unless it's
   covered by a test.
