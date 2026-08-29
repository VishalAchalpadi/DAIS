# DAIS

A config-driven medallion (bronze/silver/gold) pipeline framework. A pipeline is
a YAML spec; `pipeline.py` executes any spec generically. Adding a **new**
pipeline for a new file/format almost always means writing a new YAML file
under `specs/` - not new Python code.

## Architecture

```
source file (S3 or local)
  -> control gates (whole-file size/row-count bounds)
  -> raw / bronze   (Postgres or Snowflake, untyped TEXT, checksum-deduped)
  -> stage / silver (DQ validation + typed columns, write_mode: truncate_load | append | upsert)
  -> gold           (dbt-core model, same platform/connection as raw & stage)
```

Every step writes a begin/complete/failed/quarantined row to the shared
`control.process_monitor` table. Every stage transition emits an OpenLineage
event. A pipeline can stop cleanly at any layer (`execution.stop_after`) - that
is a normal, complete run, not a partial one.

```
specs/                       pipeline specs - the config-driven layer
business_processes/          SLA-bound groups of pipelines
dbt/                         dbt-core project (gold layer)
src/dais/
  spec/                      PipelineSpec pydantic model + YAML loader
  parsers/                   csv, delimited, fixed_width, json, xml - common Parser interface
  quality/                   control gates, pandera-based DQ rules, quarantine, alerting
  resilience/                retry wrapper + Postgres/S3 connectors
  medallion/                 bronze.py, silver.py, gold.py, exports.py (Iceberg)
  monitoring/                shared control.process_monitor table
  secrets/                   SecretsProvider: Vault (prod) / hardcoded (phase-1 fallback)
  business_process/          SLA group model, loader, evaluator
  lineage/                   OpenLineage emitter
  api/                       FastAPI trigger/status layer
  pipeline.py                the orchestrator - wires all of the above together
  cli.py                     `dais run --spec ... --file ...`
tests/                       pytest suite (unit + real integration tests against local Postgres)
```

## Setup

Requires Python 3.11+ and a reachable Postgres instance. This project was
developed with [`uv`](https://docs.astral.sh/uv/):

```bash
uv venv --python 3.11 .venv
uv pip install -e ".[dev]"
```

Gold-layer runs also need `dbt-core`/`dbt-postgres` (already in
`pyproject.toml`), and `pyiceberg`'s optional dependency needs a C/C++
compiler on the system to build (MSVC Build Tools on Windows) - if that's not
available, everything except the Iceberg export path still works.

### Secrets (local dev)

Every `database.connection` name in a spec is resolved through a
`SecretsProvider`, never read directly from YAML. For local development,
create a git-ignored `secrets.local.yaml` at the repo root:

```yaml
aurora_postgres_prod:
  host: localhost
  port: 5432
  dbname: GEODS
  user: postgres
  password: "yourpassword"
```

and set `SECRETS_PROVIDER=hardcoded` in the environment before running
anything. **This is a phase-1 fallback only** - it logs a loud warning on
every use and cannot be selected from a spec file. Unset (or any other value)
resolves to `VaultSecretsProvider`, the production path.

### Running the tests

```bash
uv pip install -e ".[dev]"
pytest
```

Tests that need a live Postgres connection are automatically skipped if
`secrets.local.yaml` isn't present; when it is, those tests create a
throwaway `dais_test_<random>` schema per test and drop it afterward.

## Adding a brand-new pipeline (no code changes)

1. Copy `specs/holdings_ingest.yaml` (fixed-width) or `specs/benchmark_ingest.yaml`
   (CSV) as a starting point.
2. Fill in: `source` (where the file comes from), `parser` (how to split it
   into fields), `control_gates` (expected file size/row count bounds),
   `raw`/`stage`/`gold` table names, `quality.rules` (including any SQL
   reference-data lookups), and `resilience`/`lineage` blocks.
3. Save it under `specs/<your_pipeline_name>.yaml`. That's it - the loader
   validates it (`dais.spec.loader.load_spec`), and `pipeline.py`,
   `cli.py`, and the API all pick it up by name with no further changes.
4. If several pipelines share an SLA, add (or extend) a group file under
   `business_processes/` - see `morning_holdings_recon.yaml`.

A new **file format** (something other than csv/delimited/fixed_width/json/xml)
is the one case that *does* need a small code change: add a module under
`src/dais/parsers/` implementing the `Parser` interface and register it with
`@register_parser("your_format")` - see any existing parser for the pattern.

## Running a pipeline

No manual database setup is required beyond having Postgres reachable and
`secrets.local.yaml` in place: the `data_in`, `core`, and `control` schemas
and their tables (raw, stage, gold, `process_monitor`) are all created
automatically on first run (`ddl_mode: create_if_not_exists`) and never
dropped or altered afterward.

**Standalone (CLI):**

```bash
SECRETS_PROVIDER=hardcoded dais run \
  --spec specs/holdings_ingest.yaml \
  --file data/holdings/incoming/HOLDINGS_20260101.txt
# run_id=... status=succeeded layer_reached=gold
```

Exit code is `0` on `succeeded`, `1` on `failed`/`quarantined` - safe to use
directly in a shell script.

**Via the HTTP API**, for orchestrator-agnostic triggering (Dagster, Control-M,
anything that can make an HTTP call):

```bash
DAIS_API_KEY=devkey uvicorn dais.api.app:create_app --factory --reload
```

```
POST /pipelines/{spec_name}/run          -> 202 {run_id, status}   (X-API-Key header required)
GET  /pipelines/runs/{run_id}/status     -> {status, layer_reached, ...}
GET  /business-processes/{name}/status   -> {state, members: [...]}
```

A run is submitted, executed in the background (FastAPI `BackgroundTasks` -
see note below), and deduped by file checksum: submitting the same file twice
while the first run is in flight (or already done) returns the *same*
`run_id` instead of processing it twice - this is what makes a Control-M
retry-after-timeout safe.

### Example Control-M-style wrapper script

Control-M expects a script-like job (exit 0/non-zero), not a raw async HTTP
call, so a thin polling wrapper sits in front of the API:

```bash
#!/usr/bin/env bash
set -euo pipefail

API="http://localhost:8000"
SPEC="holdings_ingest"
FILE="/data/holdings/incoming/HOLDINGS_${1}.txt"

run_id=$(curl -sf -X POST "$API/pipelines/$SPEC/run" \
  -H "X-API-Key: $DAIS_API_KEY" -H "Content-Type: application/json" \
  -d "{\"file_path\": \"$FILE\"}" | python3 -c "import sys,json;print(json.load(sys.stdin)['run_id'])")

echo "submitted run_id=$run_id"

while true; do
  status=$(curl -sf "$API/pipelines/runs/$run_id/status" -H "X-API-Key: $DAIS_API_KEY" \
    | python3 -c "import sys,json;print(json.load(sys.stdin)['status'])")
  case "$status" in
    succeeded) echo "OK"; exit 0 ;;
    failed|quarantined) echo "FAILED: $status"; exit 1 ;;
    *) sleep 10 ;;
  esac
done
```

## Known scope decisions / limitations

A few things were deliberately scoped down rather than left half-built:

- **Background execution** uses FastAPI's `BackgroundTasks`, not `arq`/Redis -
  no Redis instance was available. The dispatch sits behind a
  `connector_factory` interface (`dais/api/worker.py`), so swapping in a real
  task queue later doesn't require touching the API routes.
- **Snowflake** has no connector implementation - only `PostgresConnector` was
  built and tested, since there was no Snowflake instance to validate against.
  `DatabaseConnector` is the abstract interface a `SnowflakeConnector` would
  implement.
- **Large fixed-width files** (>5MB by default, see
  `pipeline.CHUNKED_PARSE_THRESHOLD_BYTES`) are parsed as parallel byte-range
  chunks (`parsers.parse_fixed_width_chunked`), but always validated as one
  combined batch - chunks are never validated independently, even under
  `integrity_mode: strict`.
- **Retry** (`resilience/retry_policies.py`) retries the failed operation
  itself with backoff+jitter; it does not reconnect a dropped database
  connection, so it helps with transient errors (deadlocks, brief network
  blips) but not a connection that needs to be fully re-established.
