# Running each pipeline: DAIS API vs. Dagster

Two ways to execute any pipeline in this repo, per the actual running
local stack (see `.claude/skills/dais-local-stack/SKILL.md` for how to
bring that stack up and its known gotchas):

1. **The DAIS HTTP API** — the orchestrator-agnostic trigger/poll surface
   (`src/dais/api/app.py`). This is what Control-M calls in production:
   a `POST .../run` to submit, then poll `GET .../status` until terminal
   (see README.md's "Example Control-M-style wrapper script" for the
   canonical bash version of this pattern). Dagster's own ingest assets
   call this exact same API underneath — Dagster is a second trigger
   source, never a second execution path.
2. **Dagster** — a dedicated job per pipeline (`<pipeline_name>_job`),
   addressable from the UI (`http://127.0.0.1:3000`) or the CLI.

A third, simpler option exists for manual/ad-hoc runs with no server
needed at all: the `dais run` CLI (`dais.cli:main`) calls the same
`run_pipeline()` function directly, in-process. Useful for a one-off local
test; not how Control-M or Dagster actually invoke a pipeline.

Every example below uses `DAIS_API_KEY=dev-key`, `SECRETS_PROVIDER=hardcoded`,
and `OPENLINEAGE_URL=http://localhost:5000` per this environment's
convention (all three set on the DAIS API server and Dagster processes
themselves at startup — see `.claude/skills/dais-local-stack/SKILL.md`),
and a real file that currently exists under `data/`. Missing
`OPENLINEAGE_URL` specifically causes no visible error at all — the run
just succeeds without ever showing up in Marquez.

### A note on shells

The `curl ...` blocks below are real curl syntax — on Windows, PowerShell
aliases the bare `curl` name to `Invoke-WebRequest`, which does not
understand curl's flags (`-sf`, `-H`, `-d`) and will error. Either call
the real binary explicitly as `curl.exe ...` (included on Windows 10/11),
or use this native PowerShell equivalent, which every `curl` block below
has a one-line drop-in for:

```powershell
function Invoke-DaisRun($SpecName, $FilePath) {
  $body = @{ file_path = $FilePath } | ConvertTo-Json
  Invoke-RestMethod -Uri "http://localhost:8000/pipelines/$SpecName/run" -Method Post `
    -Headers @{"X-API-Key" = "dev-key"} -ContentType "application/json" -Body $body
}
function Get-DaisRunStatus($RunId) {
  Invoke-RestMethod -Uri "http://localhost:8000/pipelines/runs/$RunId/status" -Headers @{"X-API-Key" = "dev-key"}
}
```

---

## Gold layer: two different mechanisms — read this first

Whether a pipeline's `/run` call (or Dagster job) reaches gold depends on
which of two unrelated mechanisms it uses:

- **Inline `gold:` block** (`asset_ingest`, `holdings_ingest`,
  `price_ingest`): `execution.stop_after: gold` means the *same* API call
  or Dagster job runs raw → stage → gold in one shot. No separate trigger
  needed.
- **Separate `gold_builds/*.yaml`** (`regional_sales_gold` depends on
  `sales_ingest`; `portfolio_summary_gold` depends on `holdings_ingest` +
  `asset_ingest`): these are dbt builds, not `PipelineSpec`s. **There is
  no HTTP API endpoint for them at all** — `POST /pipelines/{name}/run`
  only ever loads a `PipelineSpec` (`load_spec`), never a `GoldBuildSpec`.
  A gold build is runnable only via its own Dagster job, or a direct
  Python call to `run_gold_build()` (`src/dais/medallion/gold.py`). If
  you need one to run right after its dependency's stage lands, see the
  "auto-materialize" note at the bottom of this doc.
- `benchmark_ingest` and `fund_positions_ingest` currently have **no gold
  layer at all** — neither an inline block nor a `gold_builds/*.yaml`
  depends on them. Their pipelines stop at `stage`.

---

## asset_ingest

Inline gold (`execution.stop_after: gold`) — one call reaches gold.

**DAIS API:**
```bash
curl -sf -X POST http://localhost:8000/pipelines/asset_ingest/run \
  -H "X-API-Key: dev-key" -H "Content-Type: application/json" \
  -d '{"file_path": "./data/assets/incoming/assets_20260828.csv"}'
# -> {"run_id": "...", "status": "running"}
curl -sf http://localhost:8000/pipelines/runs/<run_id>/status -H "X-API-Key: dev-key"
```
PowerShell: `Invoke-DaisRun asset_ingest "./data/assets/incoming/assets_20260828.csv"` then `Get-DaisRunStatus <run_id>`

**Dagster** — job `asset_ingest_job`, op `asset_ingest__stage`:
```yaml
resources:
  dais_api:
    config:
      api_key: {env: DAIS_API_KEY}
      base_url: http://localhost:8000
      poll_interval_seconds: 2
      timeout_seconds: 1800
ops:
  asset_ingest__stage:
    config:
      file_path: "./data/assets/incoming/assets_20260828.csv"
```
CLI: `dagster job execute -j asset_ingest_job -f src/dais/orchestration/dagster/definitions.py --config <above-as-a-file>.yaml`

---

## benchmark_ingest

Stops at `stage` — no gold layer exists for this pipeline yet.

**No sample file currently exists** under `data/benchmark/incoming/` — drop
a `BENCHMARK_<date>.csv` there first (format per the spec: `format:
"%Y-%m-%d"` for its date column).

**DAIS API:**
```bash
curl -sf -X POST http://localhost:8000/pipelines/benchmark_ingest/run \
  -H "X-API-Key: dev-key" -H "Content-Type: application/json" \
  -d '{"file_path": "./data/benchmark/incoming/BENCHMARK_2026-09-14.csv"}'
```
PowerShell: `Invoke-DaisRun benchmark_ingest "./data/benchmark/incoming/BENCHMARK_2026-09-14.csv"`

**Dagster** — job `benchmark_ingest_job`, op `benchmark_ingest__stage`:
```yaml
resources:
  dais_api:
    config: {api_key: {env: DAIS_API_KEY}, base_url: http://localhost:8000, poll_interval_seconds: 2, timeout_seconds: 1800}
ops:
  benchmark_ingest__stage:
    config:
      file_path: "./data/benchmark/incoming/BENCHMARK_2026-09-14.csv"
```

---

## fund_positions_ingest

Stops at `stage` — no gold layer exists for this pipeline yet (see the
header comment in `specs/fund_positions_ingest.yaml` for why `stage` uses
`write_mode: append` rather than `upsert`).

The real sample file currently on disk is
`data/fundPositions/incoming/Equity-Fund_Positions_20260913.xml` — note
this has a date suffix even though the spec's own `file_pattern` field
(documentation only, never enforced) says a bare `Equity-Fund_Positions.xml`;
always pass whatever the real current filename is as `file_path`, the
spec's `file_pattern` is not used to look it up.

**DAIS API:**
```bash
curl -sf -X POST http://localhost:8000/pipelines/fund_positions_ingest/run \
  -H "X-API-Key: dev-key" -H "Content-Type: application/json" \
  -d '{"file_path": "./data/fundPositions/incoming/Equity-Fund_Positions_20260913.xml"}'
```
PowerShell: `Invoke-DaisRun fund_positions_ingest "./data/fundPositions/incoming/Equity-Fund_Positions_20260913.xml"`

**Dagster** — job `fund_positions_ingest_job`, op `fund_positions_ingest__stage`:
```yaml
resources:
  dais_api:
    config: {api_key: {env: DAIS_API_KEY}, base_url: http://localhost:8000, poll_interval_seconds: 2, timeout_seconds: 1800}
ops:
  fund_positions_ingest__stage:
    config:
      file_path: "./data/fundPositions/incoming/Equity-Fund_Positions_20260913.xml"
```

---

## holdings_ingest

Inline gold (`execution.stop_after: gold`). Also a dependency of the
separate `portfolio_summary_gold` build — running this pipeline's own gold
does **not** refresh `portfolio_summary_gold`; that's a different job (see
below).

The spec's `source.location.kind` is `s3` (a real bucket path is just
documentation), but for local runs you still pass a real local
`file_path` directly — it's independent of `source.location`:

**DAIS API:**
```bash
curl -sf -X POST http://localhost:8000/pipelines/holdings_ingest/run \
  -H "X-API-Key: dev-key" -H "Content-Type: application/json" \
  -d '{"file_path": "./data/holdings/incoming/HOLDINGS_20260101.txt"}'
```
PowerShell: `Invoke-DaisRun holdings_ingest "./data/holdings/incoming/HOLDINGS_20260101.txt"`

**Dagster** — job `holdings_ingest_job`, op `holdings_ingest__stage`:
```yaml
resources:
  dais_api:
    config: {api_key: {env: DAIS_API_KEY}, base_url: http://localhost:8000, poll_interval_seconds: 2, timeout_seconds: 1800}
ops:
  holdings_ingest__stage:
    config:
      file_path: "./data/holdings/incoming/HOLDINGS_20260101.txt"
```

---

## price_ingest

Inline gold (`execution.stop_after: gold`).

**DAIS API:**
```bash
curl -sf -X POST http://localhost:8000/pipelines/price_ingest/run \
  -H "X-API-Key: dev-key" -H "Content-Type: application/json" \
  -d '{"file_path": "./data/prices/incoming/portfolio_prices_20260830.csv"}'
```
PowerShell: `Invoke-DaisRun price_ingest "./data/prices/incoming/portfolio_prices_20260830.csv"`

**Dagster** — job `price_ingest_job`, op `price_ingest__stage`:
```yaml
resources:
  dais_api:
    config: {api_key: {env: DAIS_API_KEY}, base_url: http://localhost:8000, poll_interval_seconds: 2, timeout_seconds: 1800}
ops:
  price_ingest__stage:
    config:
      file_path: "./data/prices/incoming/portfolio_prices_20260830.csv"
```

---

## sales_ingest

Stops at `stage` — its gold lives separately in `gold_builds/regional_sales_gold.yaml`.

**DAIS API:**
```bash
curl -sf -X POST http://localhost:8000/pipelines/sales_ingest/run \
  -H "X-API-Key: dev-key" -H "Content-Type: application/json" \
  -d '{"file_path": "./data/sales/incoming/sales_20260914.csv"}'
```
PowerShell: `Invoke-DaisRun sales_ingest "./data/sales/incoming/sales_20260914.csv"`

**Dagster** — job `sales_ingest_job`, op `sales_ingest__stage`:
```yaml
resources:
  dais_api:
    config: {api_key: {env: DAIS_API_KEY}, base_url: http://localhost:8000, poll_interval_seconds: 2, timeout_seconds: 1800}
ops:
  sales_ingest__stage:
    config:
      file_path: "./data/sales/incoming/sales_20260914.csv"
```

Then, separately, run its gold build — see `regional_sales_gold` below.

---

## regional_sales_gold (gold_builds/*.yaml — dbt, no HTTP API)

Depends on `sales_ingest`'s stage table. **No API endpoint exists for
this** — Dagster or a direct Python call are the only ways to run it.

**Dagster** — job `regional_sales_gold_job`, no Launchpad config needed
(the `dbt` resource is fully configured in `definitions.py`; the model
selection is baked into the `@dbt_assets` decorator via `dbt.select:
"tag:regional_sales_gold"`):
```yaml
{}
```
CLI: `dagster job execute -j regional_sales_gold_job -f src/dais/orchestration/dagster/definitions.py`

**Direct Python call** (what the CLI ultimately does, useful for a script
with no Dagster involved):
```python
from dais.medallion.gold import run_gold_build
from dais.spec.loader import load_gold_build_spec

spec = load_gold_build_spec("gold_builds/regional_sales_gold.yaml")
result = run_gold_build(spec, host="localhost", port=5432, dbname="GEODS", user="postgres", password="...")
```

---

## portfolio_summary_gold (gold_builds/*.yaml — dbt, no HTTP API)

Depends on **both** `holdings_ingest` and `asset_ingest`'s stage tables —
run both of those first (via either method above) or this build will
succeed against whatever stale/empty stage data already exists rather
than failing loudly, since dbt has no way to know you meant to refresh
its sources first.

**Dagster** — job `portfolio_summary_gold_job`, no Launchpad config needed:
```yaml
{}
```
CLI: `dagster job execute -j portfolio_summary_gold_job -f src/dais/orchestration/dagster/definitions.py`

---

## Everything at once: `dais_medallion_job`

Selects every ingest pipeline's asset plus every gold build, in one job.
Because every ingest asset's op has a *required* `file_path` config field
with no default, you must supply one `ops:` entry per pipeline in the same
Launchpad run — there's no way to run "just the gold builds plus whichever
ingest pipelines happen to have new files" from this one job. For that,
run the individual per-pipeline jobs instead; `dais_medallion_job` is
really only convenient when you're intentionally re-running the entire
graph at once, e.g. after a full local reset:
```yaml
resources:
  dais_api:
    config: {api_key: {env: DAIS_API_KEY}, base_url: http://localhost:8000, poll_interval_seconds: 2, timeout_seconds: 1800}
ops:
  asset_ingest__stage: {config: {file_path: "./data/assets/incoming/assets_20260828.csv"}}
  benchmark_ingest__stage: {config: {file_path: "./data/benchmark/incoming/BENCHMARK_2026-09-14.csv"}}
  fund_positions_ingest__stage: {config: {file_path: "./data/fundPositions/incoming/Equity-Fund_Positions_20260913.xml"}}
  holdings_ingest__stage: {config: {file_path: "./data/holdings/incoming/HOLDINGS_20260101.txt"}}
  price_ingest__stage: {config: {file_path: "./data/prices/incoming/portfolio_prices_20260830.csv"}}
  sales_ingest__stage: {config: {file_path: "./data/sales/incoming/sales_20260914.csv"}}
```

---

## Making a gold build fire automatically after its dependency's stage run

Gold dbt models carry `AutomationCondition.eager()` (see
`gold_assets.py`), so `regional_sales_gold`/`portfolio_summary_gold` *can*
auto-launch the moment their upstream stage asset materializes — from
either trigger method above, since both ultimately land the same
materialization event. This requires two things that are each easy to
forget:

1. `dagster dev` must actually be running (its bundled daemon evaluates
   the condition — a webserver-only setup does nothing).
2. The automation condition sensor is **off by default**: Dagster UI →
   **Overview → Automation** → toggle `default_automation_condition_sensor`
   on, once per code location.

Without both, `eager()` is declared in code but nothing ever acts on it.
