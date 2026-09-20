---
name: dais-local-stack
description: Bring up the full local DAIS stack (API server, Dagster, OpenMetadata) and fix the recurring environment issues that break it. Use whenever the user asks to start/restart Dagster, OpenMetadata, or the DAIS API, or asks why a UI or job is broken. For authoring a new pipeline spec.yaml from a plain-language description, use the author-pipeline-spec skill instead.
---

# DAIS local stack: run it, fix it

This skill captures everything learned running DAIS's local stack (its own
FastAPI app, Dagster, and OpenMetadata as the lineage backend) end to end,
including every environment bug hit and fixed along the way. Re-read this
before touching any of these processes rather than rediscovering the same
failures.

**Lineage backend history**: Marquez was the original backend (from-source
Java/Node, port 5000/1337). Phase 11b replaced it with OpenMetadata - not a
drop-in swap, since OpenMetadata has no OpenLineage-compatible ingestion
endpoint of its own (verified against the real server: its lineage model
is entity-first - a lineage edge references existing entity ids, not an
arbitrary OpenLineage namespace/name pair). Neither DAIS's own
`lineage/emitter.py` nor dbt-ol changed at all for this swap - both still
just POST standard OpenLineage RunEvents to `OPENLINEAGE_URL`. What changed
is what's listening on the other end: `dais.lineage.openmetadata_forwarder`,
a small FastAPI service that receives those events and translates them into
OpenMetadata's entity + lineage-edge REST calls, still bound to
`localhost:5000` so `OPENLINEAGE_URL=http://localhost:5000` never needed to
change anywhere.

## 1. The six services and how to start them

Installed under `C:\Users\visha\tools\om-stack\` (outside the repo - large
binaries, not tracked in git): `jdk21/` (OpenMetadata needs Java 21;
system Java is 17, kept separate), `opensearch/` (bundles its own JDK),
`openmetadata-2.0.2/`.

| Service | Command | Required env vars |
|---|---|---|
| DAIS API (spec editor, quarantine UI, docs, pipeline trigger) | `python -m dais.api.main` from the repo root | `DAIS_API_KEY`, `SECRETS_PROVIDER=hardcoded`, `OPENLINEAGE_URL=http://localhost:5000` |
| Dagster | `python -m dagster dev -f src/dais/orchestration/dagster/definitions.py` from the repo root | `DAGSTER_HOME`, `DAIS_API_KEY`, `SECRETS_PROVIDER=hardcoded`, `OPENLINEAGE_URL=http://localhost:5000` |
| OpenSearch | `./bin/opensearch.bat` from `om-stack/opensearch` | `OPENSEARCH_JAVA_HOME=<om-stack>/opensearch/jdk` (its bundled JDK - the system `JAVA_HOME`/17 does NOT meet OpenSearch's Java 21 requirement and it will refuse to start) |
| OpenMetadata server | see below (script's own classpath-building is `:`-joined, wrong for Windows java - invoke java directly instead) | `JAVA_HOME=<om-stack>/jdk21/jdk-21.0.12.1+1`, `DB_DRIVER_CLASS=org.postgresql.Driver`, `DB_SCHEME=postgresql`, `DB_USER=openmetadata_user`, `DB_USER_PASSWORD=openmetadata_password`, `DB_HOST=localhost`, `DB_PORT=5432`, `OM_DATABASE=openmetadata_db`, `SEARCH_TYPE=opensearch`, `ELASTICSEARCH_HOST=localhost`, `ELASTICSEARCH_PORT=9200`, `ELASTICSEARCH_SCHEME=http` |
| OpenLineage-to-OpenMetadata forwarder | `python -m dais.lineage.openmetadata_forwarder` from the repo root | `OPENMETADATA_URL=http://localhost:8585/api/v1`, `OPENMETADATA_TOKEN=<bot token, see below>` |

OpenMetadata server start command (bypasses `bin/openmetadata.sh`'s broken
Windows classpath - build a `;`-joined one and invoke java directly, from
`om-stack/openmetadata-2.0.2`):
```
CP=$(printf '%s;' libs/*.jar); CP_WIN=$(cygpath -w -p "$CP")
"$JAVA_HOME/bin/java" -Xmx2g -Xms2g -Dbootstrap.dir="$(cygpath -w .)" -cp "$CP_WIN" \
  org.openmetadata.service.OpenMetadataApplication server "$(cygpath -w conf/openmetadata.yaml)"
```
The bootstrap/migration step (`bootstrap/openmetadata-ops.sh drop-create`)
has the same classpath bug - same fix, swap `drop-create` for the trailing
arg and target class `org.openmetadata.service.util.OpenMetadataOperations`.
Only needed once (or after wiping the `openmetadata_db` Postgres database).

**The bot token**: `dais-lineage-bot` is a bot user (isAdmin, non-expiring
JWT) created via the OpenMetadata API specifically so the forwarder never
has to re-authenticate as `admin`. Its token is saved at
`C:\Users\visha\tools\om-stack\bot_token.txt` - read it into
`OPENMETADATA_TOKEN` rather than regenerating. If it's ever lost: log in as
admin (`POST /api/v1/users/login`, `admin@open-metadata.org` /
`YWRtaW4=` base64), then `PUT /api/v1/users/generateToken/<dais-lineage-bot's
user id>` with `{"JWTTokenExpiry": "Unlimited"}`.

URLs once up:
- Dagster: `http://127.0.0.1:3000`
- DAIS spec editor: `http://localhost:8000/ui/spec-editor` (and `/ui/spec-editor/<name>` to edit an existing one)
- DAIS quarantine review: `http://localhost:8000/ui/quarantine/<spec_name>`
- DAIS API docs: `http://localhost:8000/docs`
- Great Expectations Data Docs (validation run history): `http://localhost:8000/ui/great-expectations/index.html`
- OpenMetadata UI (lineage graphs, entity catalog): `http://localhost:8585` (login `admin@open-metadata.org` / `admin`)

API key for both DAIS UIs is whatever `DAIS_API_KEY` was set to (`dev-key` in this environment's convention).

### The #1 recurring mistake: per-process env vars set on only one process

Both `SECRETS_PROVIDER` and `OPENLINEAGE_URL` are **per-process** env vars
with no shared/global state — every Python process in this stack resolves
each independently, and forgetting either on any *one* of them produces a
failure that looks like it's specific to that process, when actually it's
just this same class of mistake recurring:

- `get_secrets_provider()` (`src/dais/secrets/factory.py`) defaults to a
  real Vault client unless `SECRETS_PROVIDER=hardcoded` is set in that
  process's own environment. Both the DAIS API server (for pipeline runs)
  AND `dagster dev` (directly, inside `gold_assets.py`'s dbt run) resolve
  secrets independently. Missing it produces `Vault client is not
  authenticated - check VAULT_ADDR/VAULT_TOKEN` from whichever process you
  forgot.
- `build_client()` (`src/dais/lineage/emitter.py`) only sends real
  OpenLineage events to the forwarder (which then relays into
  OpenMetadata) if `OPENLINEAGE_URL` is set in that process's environment
  — otherwise it silently falls back to a `ConsoleTransport` that logs
  locally and sends nothing, **with no error at all**. This is the more
  dangerous of the two exactly because it fails silently: a pipeline run
  reports `succeeded` normally, and the only symptom is the run's
  job/table never appearing in OpenMetadata. Both the DAIS API server
  (pipeline lineage) and `dagster dev` (dbt-ol's gold lineage) need it
  independently. The forwarder itself failing (OpenMetadata down, bot
  token stale) is *also* silent to the caller by design (see
  `openmetadata_forwarder.py`'s docstring) - it logs a warning and still
  acks the event, so check the forwarder's own stderr, not just whether
  the pipeline run succeeded.

Always set **all three** (`SECRETS_PROVIDER=hardcoded`, `DAIS_API_KEY`,
`OPENLINEAGE_URL=http://localhost:5000`) before starting *every* Python
process in this stack, not just the one you're thinking about at the time
— and when something is missing from OpenMetadata that you expected to
see, check this (and that the forwarder + OpenMetadata + OpenSearch are
all actually up) before assuming the pipeline itself is broken: the run
very likely succeeded, it just never emitted.

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

**Historical - Marquez is no longer part of this stack** (replaced by
OpenMetadata, see section 1's lineage backend history note). Kept here
only in case Marquez is ever reintroduced; nothing below applies to the
current stack.

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

Moved to its own skill: `author-pipeline-spec`. Invoke that skill for
anything about writing a new `spec.yaml` from a plain-language feed
description.
