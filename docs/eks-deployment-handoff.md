# DAIS — infrastructure & deployment handoff (for an in-house / EKS build)

**Audience:** the team that will stand DAIS up in your own AWS account and make
it EKS-ready. **Goal:** you should not have to rediscover anything the original
developers had to figure out. Read sections 1–3 first; sections 4–9 are the
runbook.

## 0. Read this first: what is and isn't proven

| Statement | Status |
|---|---|
| The code, unit/integration tests (368) and the end-to-end flows below work | **Verified** — on a single Windows workstation, everything running as separate local processes |
| Every command, port, env var, dependency version, secret shape and gotcha in this document that is stated as fact | **Verified** against the running system or the source (file references given) |
| Anything labelled **Recommendation** | **Not verified** — sound engineering advice, but never run on EKS |
| Container images, Helm charts / manifests, CI, Terraform/CloudFormation | **Do not exist in this repository.** You build these. |
| EKS deployment | **Never attempted** |

There is no `Dockerfile`, no Helm chart, no compose file, and no IaC in the repo.
Everything in section 5 ("What you must build") is new work.

## 1. What DAIS is (one paragraph)

DAIS is a config-driven medallion pipeline framework. A YAML **spec**
(`specs/*.yaml`, one per feed) declares source location, parser, control gates,
raw table, data-quality rules, stage table, and a gold model; the generic engine
(`src/dais/pipeline.py`) executes `control gate → raw (bronze) → stage (silver,
validated with Great Expectations) → gold (dbt)`. A FastAPI service triggers and
reports on runs; Dagster orchestrates (and, via sensors, watches for new files);
OpenLineage events flow through a small translator into OpenMetadata for lineage
and catalog. Adding a pipeline means adding a spec file, not writing code.

## 2. Component inventory

| # | Component | What it is | Port | Where it runs | State it holds |
|---|---|---|---|---|---|
| 1 | **DAIS API** | FastAPI app, `python -m dais.api.main` (`src/dais/api/`). Trigger/status endpoints, quarantine-review UI, spec-editor UI, serves GX Data Docs | 8000 | Deployment (**1 replica, see 8.1**) | **In-memory run registry**; writes `gx_data_docs/` to local disk; may run dbt as a subprocess |
| 2 | **Dagster webserver + daemon** | Orchestrator UI (3000) and the daemon that evaluates sensors/schedules/queued runs | 3000 | Deployment(s) | Run/event/sensor-cursor storage (**must be Postgres on EKS, 5.3**) |
| 3 | **Dagster code location** | Loads `src/dais/orchestration/dagster/definitions.py` | (gRPC) | Deployment | none |
| 4 | **Application Postgres** | Holds raw/stage/gold data + `control.process_monitor` | 5432 | Aurora/RDS | all business data |
| 5 | **Lineage forwarder** | `python -m dais.lineage.openmetadata_forwarder` — receives OpenLineage HTTP events, writes OpenMetadata entities/edges | 5000 | Deployment | In-memory per-run accumulation (loses in-flight only) |
| 6 | **OpenMetadata server** | Catalog/lineage UI + API (v2.0.2 verified) | 8585 | Helm chart or Deployment | Postgres + OpenSearch |
| 7 | **OpenSearch** | OpenMetadata's search backend (3.3.0 verified) | 9200 | Managed OpenSearch or chart | indices |
| 8 | **OpenMetadata DB** | Separate Postgres database | 5432 | Aurora/RDS (separate DB) | catalog metadata |
| 9 | **S3** | Landing zone (files to ingest) and quarantine (rejected rows/files) | — | AWS | files |
| 10 | **Secrets store** | HashiCorp Vault (KV v2) — the production secrets path in code | 8200 | your Vault | DB creds |
| 11 | **dbt** | Runs *inside* the API and Dagster processes as a subprocess (gold layer). Not a service. | — | in image | `dbt/` project on disk |

Optional: an Anthropic API key (AI DQ-recommendations and anomaly explanations;
both degrade gracefully/skip if absent), an external scheduler such as Control-M
(the API is designed to be called by one; Dagster is the optional alternative).

## 3. How the pieces talk (network paths you must allow)

```
Control-M / user ──HTTP :8000──▶ DAIS API ──▶ Aurora :5432 (app DB)
Dagster ingest job ─HTTP :8000─▶ DAIS API ──▶ S3 (landing read, quarantine write)
Dagster sensor ──────────────────────────────▶ S3 / local dir (LIST only)
Dagster (gold assets) ──▶ dbt subprocess ──▶ Aurora :5432
DAIS API + Dagster ──OpenLineage HTTP POST :5000──▶ Lineage forwarder ──HTTP :8585──▶ OpenMetadata
OpenMetadata ──▶ OM Postgres :5432 , OpenSearch :9200
DAIS API / Dagster ──▶ Vault :8200 (secrets)
DAIS API ──▶ api.anthropic.com :443   (optional)
Dagster sensor / DQ alerts ──▶ your webhook URLs :443   (missed-file & quarantine alerts)
Dagster webserver ──▶ Dagster Postgres (run storage)
```

Two **non-obvious** facts about that diagram:
- The Dagster ingest asset does **not** run the pipeline itself — it calls the
  DAIS API and polls `GET /pipelines/runs/{id}/status`
  (`orchestration/dagster/api_resource.py`). The API and Dagster must be able to
  reach each other; and because status lives in the API's memory (8.1) the poll
  must land on the **same** API pod that took the trigger.
- dbt is launched by *both* the API (for `gold:` blocks in a spec) and Dagster
  (`gold_builds/*.yaml`, via dagster-dbt). Both containers therefore need dbt and
  a route to Aurora.

## 4. Verified versions

| Item | Version verified | Notes |
|---|---|---|
| Python | 3.14 (uv-managed) on the dev machine; `pyproject.toml` says `>=3.11` | Pick a base image (3.12 suggested) and **run the full test suite in it before trusting it** — only 3.14 has been exercised |
| Dagster | 1.13.23 (locked in `uv.lock`) | `dagster>=1.8` in pyproject |
| Great Expectations | 1.23.x | no native Polars engine; code converts to pandas |
| dbt-core / dbt-postgres | per `uv.lock` | `openlineage-dbt` provides `dbt-ol` |
| PostgreSQL | 18.6 (app DB, local) | OpenMetadata requires **15+** |
| OpenMetadata | **2.0.2** | needs **Java 21** (Java 17 fails); default login `admin@open-metadata.org` / `admin` |
| OpenSearch | **3.3.0** (security plugin disabled for local) | OM supports OpenSearch 3.x or Elasticsearch 9.x |
| Java | Temurin **21** for OpenMetadata; OpenSearch bundles its own | |

The lock file `uv.lock` is checked in; build images with `uv sync --frozen`
(or export requirements from it) so versions match.

## 5. What you must build (not in the repo)

### 5.1 Container images
Minimum: **one application image** used by three workloads (API, Dagster
webserver/daemon/code-server, lineage forwarder) with different commands.
Requirements — each is a real failure mode, not a preference:

1. **Keep the repo layout.** `definitions.py` and `dbt_project.py` locate
   `specs/`, `gold_builds/` and `dbt/` with
   `Path(__file__).resolve().parents[4]`
   (`src/dais/orchestration/dagster/definitions.py:27`), and the API defaults
   `specs_dir="specs"`, `business_processes_dir="business_processes"`, secrets file
   `secrets.local.yaml`, GX docs dir `gx_data_docs` **relative to the working
   directory** (`api/app.py`, `secrets/hardcoded_provider.py`,
   `quality/row_validator.py`). So: copy the whole repo into the image, install
   it **editable** (`pip install -e .`, not a wheel into site-packages — that
   would make `parents[4]` point somewhere wrong), and set `WORKDIR` to the repo
   root in every container.
2. **Bake in dbt**: dbt-core, dbt-postgres, openlineage-dbt, and the `dbt/`
   directory. The code expects a real `dbt` executable next to the Python
   interpreter (`REAL_DBT_EXECUTABLE = Path(sys.executable).parent / "dbt"`,
   `dbt_project.py`) — use a venv whose `bin/` contains `dbt`.
3. **tzdata**: slim images lack it and `zoneinfo.ZoneInfo("America/New_York")`
   (used by the business-process evaluator and the watch sensor) will raise.
   Install `tzdata`.
4. **Pre-generate the dbt manifest at build time.** `dbt_project.py` runs
   `prepare_if_dev()` (a real `dbt parse`) on every import of the Dagster module.
   In a container you want this done once at build (dagster-dbt's
   `dagster-dbt project prepare-and-package`) rather than at every pod start.
   *(Recommendation — untested.)*
5. **Add missing dependencies** to `pyproject.toml`: `dagster-postgres`
   (run storage, 5.3) and, if you use the Kubernetes run launcher,
   `dagster-k8s`. They are **not** currently dependencies.
6. Run as non-root; `secrets.local.yaml` must never be in the image (it is
   git-ignored, keep it that way).
7. Two more, smaller images if you prefer separation: OpenMetadata and OpenSearch
   come from their own charts/images, not from this repo.

### 5.2 Kubernetes manifests / Helm
Deployments: `dais-api`, `dagster-webserver`, `dagster-daemon`,
`dagster-code-server` (or the official `dagster` Helm chart with this image as
the user-code deployment), `lineage-forwarder`. Services for 8000/3000/5000.
Ingress with auth (8.6). ConfigMaps/Secrets for section 7. PVC or S3 strategy for
`gx_data_docs/` (8.4). `terminationGracePeriodSeconds` generous on the API pod —
pipeline runs execute *inside* the API process (FastAPI `BackgroundTasks`, then
a dbt subprocess for gold); killing the pod kills the run.

### 5.3 Dagster instance config (`dagster.yaml`) — required for EKS
Local dev uses SQLite under `DAGSTER_HOME`. On EKS use Postgres storage
(`dagster-postgres`) so sensor cursors and run history survive pod restarts —
**the sensor's cursor (last-arrival date, alerted-today flag) lives there.**
Also set a run-concurrency limit (8.2):
```yaml
run_coordinator:
  module: dagster.core.run_coordinator
  class: QueuedRunCoordinator
  config:
    tag_concurrency_limits:
      - key: "dagster/sensor_name"
        value: { applyLimitPerUniqueValue: true }
        limit: 1
```
*(Recommendation — the tag schema is Dagster's documented one, but this exact
setting was not exercised here. Default without it: up to 10 concurrent runs.)*

### 5.4 CI
The test suite (368 tests) needs: a reachable **Postgres** with the app
credentials (tests use `secrets.local.yaml` via the hardcoded provider and create
throwaway `dais_test_*` schemas), plus `moto` (S3 is mocked; no AWS needed). Some
tests run real `dbt`. Full run ≈ 2–4 minutes.

## 6. Infrastructure to provision (with exact requirements)

### 6.1 Aurora/RDS PostgreSQL — application database
- Create database (the reference used `GEODS`) and an application user with
  `CREATE` on the database. The engine **auto-creates** schemas and tables it
  needs (`data_in`, `core`, `control`, monitoring tables, anomaly-profile
  table) via `CREATE SCHEMA/TABLE IF NOT EXISTS` — you do not run migrations.
- **Exception — you must create and seed `ref.currencies` yourself** (column
  `currency_code TEXT UNIQUE`; the reference seed is USD/EUR/GBP). The
  `holdings_ingest` spec validates currency with a SQL lookup against it; no
  code creates it outside the test fixtures (`tests/conftest.py: ref_currencies`).
  Any other reference table a spec's `lookup:` names needs the same treatment.
  The FX pipeline's `core.t_ref_currency` is the exception to the exception: run
  `dais seed-currencies --spec specs/fx_rates_ingest.yaml` once per environment
  (idempotent - creates the table and adds any missing ISO codes, never deletes).
  Specs can also opt in to moving processed files with `source.location.archive`
  (local or S3) - see `docs/fx-rates-pipeline.md`.
- Network: reachable from API, Dagster (dbt), and CI.
- Only `platform: postgres` is implemented. `snowflake` is accepted in the spec
  schema but `build_connector_for_spec` raises `NotImplementedError`.

### 6.2 S3
Two prefixes (buckets or one bucket, your choice):
- **Landing** — where feeds arrive. `source.location.kind: s3`,
  `path: s3://bucket/prefix/`.
- **Quarantine** — `quality.quarantine.location: s3://bucket/prefix/`, with
  `kind: s3`. **Every pipeline needs S3 write access to its quarantine prefix,**
  even pipelines whose source is local (`db.py: build_s3_connector_for_spec`).
- Credentials come from boto3's default chain, never the spec. On EKS use
  **IRSA** (IAM role for service accounts). Minimum policy: `s3:ListBucket` on
  the bucket; `s3:GetObject` on landing; `s3:PutObject`, `s3:GetObject`,
  `s3:ListBucket`, `s3:DeleteObject` on quarantine (the quarantine-review UI reads
  and removes resolved records). The Dagster **sensor** additionally needs
  `s3:ListBucket` on landing to detect arrivals (it lists; it does not read).
- Note the existing sample specs point at placeholder buckets
  (`s3://dais-example-bucket/...`, `s3://acme-data/...`). Replace them.

### 6.3 Secrets — Vault
The production secrets path is HashiCorp Vault **KV v2** (`hvac`).
`SECRETS_PROVIDER` unset ⇒ Vault. For each spec's `database.connection` name
(e.g. `aurora_postgres_prod`) create a secret at `secret/<name>` with exactly the
keys **`host`, `port`, `dbname`, `user`, `password`**
(`secrets/vault_provider.py`, `db.py`). The API and Dagster processes need
`VAULT_ADDR` and a `VAULT_TOKEN` (or you extend the provider — see below).
- **`SECRETS_PROVIDER=hardcoded` must never be set in production** (it reads a
  plaintext file and logs a warning per use).
- If your standard is **AWS Secrets Manager** instead of Vault: implement
  `SecretsProvider.get_secret(name) -> dict` (`secrets/base.py`) and select it in
  `secrets/factory.py`. The interface is one method; it is a small change.
- Also needs a secret named for dbt's connection: dbt reads its connection from
  env vars set by the code from the same secret (`medallion/gold.py`) — nothing
  extra to provision.

### 6.4 OpenMetadata stack (catalog/lineage)
Use the official OpenMetadata Helm chart (**not verified here** — the reference
was installed from the bare release tarball) or your platform's standard.
Requirements verified:
- **Java 21**, **PostgreSQL ≥15** (a separate database, e.g. `openmetadata_db`,
  own user), **OpenSearch 3.x** (or Elasticsearch 9.x), reachable from the server.
  Server env: `DB_DRIVER_CLASS=org.postgresql.Driver`, `DB_SCHEME=postgresql`,
  `DB_HOST/PORT/USER/USER_PASSWORD`, `OM_DATABASE`, `SEARCH_TYPE=opensearch`,
  `ELASTICSEARCH_HOST/PORT/SCHEME` (env names are Elasticsearch-flavoured even for
  OpenSearch).
- First-time bootstrap creates schema + search indices (`openmetadata-ops`
  `drop-create` — **destructive**; use only on first install, and check the OM
  docs for the correct migrate command for later upgrades).
- **Change the default admin password immediately** (`admin@open-metadata.org` /
  `admin`).
- **Create a bot identity for DAIS** (don't reuse admin). Steps that worked:
  1. `POST /api/v1/users/login` (`admin@open-metadata.org`, password base64) →
     JWT.
  2. `PUT /api/v1/users` `{name:"dais-lineage-bot", email:..., isBot:true,
     authenticationMechanism:{authType:"JWT", config:{JWTTokenExpiry:"Unlimited"}}}`
     — the `authenticationMechanism` is **required** or the server 500s.
  3. `PUT /api/v1/bots` `{name:"dais-lineage-bot", botUser:"dais-lineage-bot"}`.
  4. `PATCH /api/v1/users/<id>` `[{"op":"add","path":"/isAdmin","value":true}]`
     (needed so it can create services/entities).
  5. `PUT /api/v1/users/generateToken/<id>` `{"JWTTokenExpiry":"Unlimited"}` →
     `JWTToken`. Store it as a Kubernetes secret → `OPENMETADATA_TOKEN`.
  (A truncated copy of this token fails with a signature error — copy the full
  value.)
- The forwarder creates its own services/entities on first event (a Postgres
  database service per host:port, a Dagster pipeline service per lineage
  namespace, tables, pipelines, edges). No pre-seeding needed. **Column-level
  lineage is automatic:** DAIS attaches OpenLineage `schema` and `columnLineage`
  facets to its raw/stage events (and `dbt-ol` does for gold), and the forwarder
  creates the columns and column-level edges. `dais lineage --spec specs/<x>.yaml
  --sync-openmetadata` is now only a fallback / a way to populate columns for
  `gold_builds` (`sync_dependent_gold_build_columns`).

### 6.5 Dagster
Webserver + daemon + code location (or the official `dagster` Helm chart). The
daemon **must** be running — it evaluates sensors. `dagster dev` bundles the
daemon; a production deployment runs `dagster-daemon` separately (the local
guidance "do not run a separate daemon" applies only to `dagster dev`).
`DAGSTER_HOME` on a persistent volume or, better, Postgres storage (5.3).

## 7. Environment variables (complete list found in the source)

| Variable | Used by | Purpose | Required? |
|---|---|---|---|
| `DAIS_API_KEY` | API, Dagster | Shared API key (`X-API-Key` header) | **Yes** — API auth (see 8.6) |
| `SECRETS_PROVIDER` | API, Dagster, CLI | `hardcoded` = local file; anything else = Vault | Leave **unset** in prod |
| `VAULT_ADDR`, `VAULT_TOKEN` | API, Dagster | Vault access | Yes (Vault path) |
| `OPENLINEAGE_URL` | API, Dagster, dbt-ol | e.g. `http://lineage-forwarder:5000`. **Unset ⇒ events silently go to console — no error** | Yes, for lineage |
| `DAIS_API_BASE_URL` | Dagster | API URL for ingest assets (default `http://localhost:8000`) | Yes on k8s |
| `DAIS_API_PORT` | API | listen port (default 8000) | no |
| `DAIS_GX_DATA_DOCS_DIR` | API | GX Data Docs output dir (default `./gx_data_docs`) | set to a mounted path |
| `OPENMETADATA_URL`, `OPENMETADATA_TOKEN` | forwarder, `dais lineage --sync-openmetadata` | OM API base (default `http://localhost:8585/api/v1`) and bot JWT | Yes |
| `OPENLINEAGE_FORWARDER_PORT` | forwarder | default 5000 | no |
| `DAGSTER_HOME` | Dagster | instance dir/config | Yes |
| `ANTHROPIC_API_KEY` | API (anomaly explanations), `dais-recommend-dq` | Optional AI features | no |
| `AWS_*` / IRSA | API, Dagster | boto3 default chain (S3) | via IRSA |
| `DBT_PG_*`, `DBT_STAGE_*`, `OPENLINEAGE_NAMESPACE` | dbt subprocess | **Set by the code** per run from the resolved secret — do not set them yourself (placeholders `unused` are set at import for manifest generation) | no |

### Per-process environment — the #1 recurring mistake
Each of API / Dagster / forwarder reads its own environment. Setting a variable
on one and forgetting another produces a failure that *looks* process-specific:
`Vault client is not authenticated` from whichever process lacked the secrets
config, or — worse — **no lineage and no error** when `OPENLINEAGE_URL` is
missing. Drive all pods' env from shared ConfigMap/Secret objects so they cannot
drift.

## 8. Known limitations you must design around (found the hard way)

**8.1 The API's run registry is in memory.** `api/runs.py: RunRegistry` holds
run status and the file-checksum de-duplication map in a Python dict. Consequences:
(a) **run exactly 1 API replica** — with 2+, a status poll can hit a pod that
never saw the run (404) and de-dup breaks; (b) a restart forgets everything — the
Dagster sensor will legitimately re-submit files that are still in the landing
directory, and they will execute again (raw-layer checksum de-dup and upserts
make this safe for data, but it is wasted work); (c) an in-flight run dies with
the pod. Real fix (**recommended before scale**): persist the registry in
Postgres (`control.process_monitor` already records the truth per step) and read
status from there.

**8.2 Concurrent runs vs. ordering.** Dagster runs up to 10 jobs at once. For a
feed where later files are corrections to earlier ones (the `multi_file` /
`portfolio_holdings_ingest` case) concurrent execution can apply upserts out of
order. The sensor submits files oldest-first, but ordering is only *guaranteed*
if runs execute one at a time — apply the per-sensor concurrency limit in 5.3.

**8.3 Sensors start OFF in Dagster by default.** DAIS creates watch sensors as
`RUNNING` when the spec says `watch.enabled: true`, but Dagster remembers state
per instance; check the Automation tab after a fresh install. Also the
**automation-condition sensor** (`default_automation_condition_sensor`) that lets
gold fire after stage is off by default — turn it on once per code location.

**8.4 GX Data Docs are files on local disk, served unauthenticated.**
`/ui/great-expectations/` is a static mount with **no API-key check** and
contains sample failing rows (potentially sensitive). Put the API behind an
authenticated ingress and/or restrict that path; mount a volume for
`DAIS_GX_DATA_DOCS_DIR`. The directory is regenerated by validation runs — do
not commit it (it is currently, unfortunately, tracked in git; add it to
`.gitignore` and remove it from the index).

**8.5 Business-process SLA breaches don't alert.** `business_process/evaluator.py`
computes met/breached only when `GET /business-processes/{name}/status` is
called; nothing pushes an alert (its docstring implies otherwise — it doesn't).
The **file-arrival** deadline alert added with the watch sensor *does* push (via
the webhook channel). Wiring the business-process evaluator into a Dagster sensor
is a straightforward follow-on.

**8.6 Auth is one shared API key.** The data/trigger/status routes require
`X-API-Key` = `DAIS_API_KEY`. **Unauthenticated by design or default:** the
`/ui/*` HTML pages (they then ask for the key in the browser and pass it on each
call), FastAPI's own `/docs` and `/openapi.json`, and the
`/ui/great-expectations/` static mount (8.4). There is no user identity or RBAC.
Front the whole service with your SSO/ingress and treat the key as a service
credential, not a user credential.

**8.6a Dagster has no access control in open source (decision needed).**
Open-source Dagster's webserver has no login and no RBAC: anyone who can reach
port 3000 can launch runs, toggle sensors and edit run config. RBAC (roles,
per-location permissions, SSO) is a **Dagster+ (Cloud)** feature. Options:
(a) Dagster+; (b) keep OSS behind an authenticating reverse proxy/ingress (SSO
via oauth2-proxy etc.) and run the webserver with `--read-only` for the wider
audience, with a separate proxied instance for operators; (c) network-restrict
to the platform team. Pick one before production; the code needs no change.

**8.7 Alert channels.** `log` and `webhook` (real HTTP POST — use for
Slack/Teams/PagerDuty) work. **`email` is a stub that raises
`NotImplementedError`** (`quality/dq_alerts.py`). Implement with SES/SMTP if
required; keep the `Alerter.send(destination, message, context)` interface.

**8.8 SFTP is not implemented.** `source.location.kind: sftp` validates in a
spec, but discovery raises `FileDiscoveryError("SFTP discovery is not yet
implemented")` and the sensor watches nothing. Needs a connector
(e.g. paramiko) mirroring `S3Connector`, plus a `discover_files` branch.

**8.9 Sensor has no file "settling" check.** A file still being written when the
sensor lists the directory can be picked up mid-write. Have producers write to a
temporary name and rename/`mv` into the landing path (atomic), or add a
min-age check to `discover_files`.

**8.10 OpenMetadata quirks.**
- Search box: bare terms return nothing; use a **trailing wildcard**
  (`portfolio_holdings*`) or open the entity URL directly
  (`/table/<service>.<db>.<schema>.<table>/lineage`).
- Column-level lineage view: on the lineage graph enable the **Layers →
  Column Level Lineage** toggle, then click the column.
- Columns and column-level edges are created automatically from the facets on
  each run. Caveats: columns known only from dbt (no schema facet) get type
  `UNKNOWN`, and `fx_rates_gold`-style dbt tables may lack columns dbt didn't
  report (e.g. `gold_loaded_at`); `dais lineage --sync-openmetadata` fills gaps.
  The forwarder looks a table up (GET) before creating it, because re-PUTting an
  existing table makes OpenMetadata prune its column-level lineage; columns are
  only appended, never replaced, so glossary tags survive.
  Raw *files* are not modelled (no OM entity type fits a local path) — lineage
  starts at the raw **table**.
- To tag columns with glossary terms via API, PATCH by array index
  (`/columns/7/tags/0`), not the whole `tags` array (the latter silently no-ops).
- A pipeline service's type cannot be changed by re-PUTting it; delete and
  recreate (the forwarder creates `Dagster`-type services).
- The lineage forwarder swallows OpenMetadata errors on purpose (never fail a
  pipeline run over lineage): **check the forwarder's logs**, not the run status,
  when lineage is missing.

**8.11 Windows-only workarounds in the code can be ignored on Linux.** Comments
about "Application Control policy", `dbt.exe` wrappers and PATH manipulation
(`medallion/gold.py`, `dbt_project.py`) exist for one locked-down Windows
machine. On Linux the only requirement is that `dbt` exists beside the Python
interpreter (5.1).

**8.12 Known repo hygiene items** (do these first): `gx_data_docs/` and
`scratch_holdings_demo/` / `scratch_portfolio_demo/` are demo/generated output that
should not be tracked; `uv.lock` is committed
(good — keep it); demo specs point at placeholder S3 buckets and a scratch local
directory.

## 9. Bring-up order and smoke tests

1. Provision: VPC access, Aurora (app DB + `ref.currencies`), S3 buckets + IAM
   (IRSA roles), Vault secret(s), Dagster Postgres, OpenMetadata DB + OpenSearch.
2. Build the image (5.1); run the **test suite inside it** against a throwaway
   Postgres — must be green before anything else.
3. Deploy OpenMetadata; change admin password; create the bot (6.4); store the
   token.
4. Deploy the lineage forwarder (`OPENMETADATA_URL`, `OPENMETADATA_TOKEN`).
   Smoke: `curl -X POST http://forwarder:5000/api/v1/lineage -d '{}'` → HTTP 201.
5. Deploy the DAIS API (1 replica) with Vault + `OPENLINEAGE_URL` + `DAIS_API_KEY`.
   Smoke: `GET /pipelines` with the key returns the spec list; `GET /docs` loads.
6. Deploy Dagster (webserver, daemon, code location, Postgres storage, 5.3).
   Smoke: UI lists `<pipeline>_job` for every spec; daemon log shows
   `SensorDaemon`.
7. **End-to-end**: put a known-good file in the landing prefix (or
   `POST /pipelines/<spec>/run` with `{"file_path": "s3://..."}`); expect status
   `succeeded`, `layer_reached: gold`; rows in `core.<gold table>`; a run in GX
   Data Docs; the table + lineage in OpenMetadata.
8. **Sensor**: enable `watch` on one spec pointed at an S3 prefix; drop a file;
   expect a Dagster run within `poll_interval_seconds`. Then test the missed-file
   alert by setting `expected_by` a few minutes ahead with a webhook URL you
   control.
9. Failure-path checks: a file with a bad row (quarantine object appears in
   S3; `/ui/quarantine` lists it), a control-gate failure (file quarantined,
   status `quarantined`), API pod restart during idle (no data loss).

## 10. Where to read more in this repo

| Topic | Where |
|---|---|
| How to run/pipelines day to day | `docs/running-pipelines.md` |
| Why Dagster alongside Control-M | `docs/dagster-vs-legacy-architecture.md` |
| Executive summary | `docs/executive-summary.md` |
| Local watcher walkthrough | `docs/testing-the-file-watcher.md` |
| FX rates pipeline (drop-a-file demo, archive, reference table) | `docs/fx-rates-pipeline.md` |
| Spec schema (source of truth) | `src/dais/spec/models.py`, `GET /spec-schema` |
| Authoring a new spec | `.claude/skills/author-pipeline-spec/SKILL.md` |
| Local stack + past environment bugs | `.claude/skills/dais-local-stack/SKILL.md` |
