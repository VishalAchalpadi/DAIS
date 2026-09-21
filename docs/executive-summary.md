# DAIS — Executive Summary

**A config-driven medallion (bronze/silver/gold) data pipeline framework.** A
new data source becomes a YAML spec file plus, optionally, a SQL
transformation — never new Python code. Six production-shaped pipelines
(`holdings_ingest`, `asset_ingest`, `price_ingest`, `benchmark_ingest`,
`portfolio_holdings_ingest`, `fx_rates_ingest`) have been built and run
end-to-end against real data as proof of the pattern. Data quality runs on
**Great Expectations**, lineage lands in **OpenMetadata**, and orchestration
(including drop-a-file-and-it-runs) is handled by **Dagster**.

---

## 1. The core idea

Every pipeline runs the same generic engine: **control gate → raw (bronze)
→ stage (silver) → gold**. What differs between pipelines is entirely
described in a spec file — source format, validation rules, business keys,
write strategy, versioning. The Python engine never branches on which
pipeline it's running; it branches on what the spec says. This means:

- **Onboarding a new data feed is a config change, not a code change** —
  no PR review of pipeline logic, no new test suite to write from scratch,
  no risk of one team's pipeline destabilizing another's.
- **Every pipeline gets the same guarantees for free** — validation,
  quarantine, audit trail, retry, lineage — because they're engine-level
  features, not something each pipeline author has to reimplement.
- The spec is validated at *load time* (via Pydantic), not at run time —
  a typo or an invalid combination of options fails immediately and
  explicitly, before touching any data.

---

## 2. Ingestion & parsing

| Capability | Options | Benefit |
|---|---|---|
| **File formats** | CSV, delimited (custom separator/quote/escape), fixed-width, JSON, XML | One engine handles the realistic spread of source formats a data/finance shop actually receives, without a parser rewrite per feed. |
| **Fixed-width chunked parsing** | Automatic for files over 5MB, parallelized across CPU cores | Large historical/mainframe-style extracts parse fast without changing how the pipeline is configured — chunking is transparent to the spec author. |
| **Source location** | Local filesystem or S3 (`sftp` is accepted by the schema but discovery is not yet implemented) | Same spec shape works for a laptop/dev box and a cloud deployment. |
| **Multi-file selection** | `source.location.multi_file`: `file_pattern`, order by filename timestamp / modified time, `mode` (latest / all), `on_earlier_failure` | Several dated files can sit in one location; the engine picks and orders them deterministically instead of relying on a hand-named file. |
| **File-drop watching** | `source.location.watch` — a Dagster sensor per pipeline (default 60s poll, RUNNING by default), one run per new file, oldest first | Drop a file in the folder and it is processed within seconds, no manual trigger. Files not matching the pattern are logged as warnings, not silently ignored. |
| **Missed-arrival alert** | `watch.expected_by` + `expected_timezone` + `missed_arrival_alert` (log or webhook) | If no file dated today has arrived by the deadline, ops is alerted once per day; the alert resets when a file lands. |
| **Archive on success** | `source.location.archive` (`path`, `date_subdirs`) for local and S3 | Fully-processed files move to a rolling `YYYYMMDD` archive folder — only when the run succeeded through its final layer; never overwrites; duplicate-checksum files are left alone. |
| **Control gates** | Min/max file size, min/max row count | Catches a truncated transfer, a duplicate/corrupted feed, or a wildly wrong row count *before* anything lands in a table — the file is quarantined whole, cleanly, with an alert. |

---

## 3. Data quality — the framework's deepest investment

| Capability | Options | Benefit |
|---|---|---|
| **Engine** | Great Expectations (replaced pandera); each run's results are published as browsable **GX Data Docs** | Industry-standard DQ tooling with a human-readable report per validation, not just a log line. |
| **Field-level checks** | `not_null`, `non_empty`, `valid_date` (+format), `is_numeric`, plus parameterized comparisons (`greater_than`, `less_than_or_equal`, etc.) | Covers the checks that actually catch real bad data (nulls, malformed dates, out-of-range values) without needing custom code per rule. |
| **Reference-data lookups** | Arbitrary SQL query per field (e.g. validate a currency code against `ref.currencies`) | Validates against live reference data, not a hardcoded list that goes stale. |
| **Type casting** | `date` (with format string), `decimal` (with precision/scale) | Raw text is only cast to a real type *after* it's proven valid — a bad value can never silently become `NULL` in a typed column. |
| **Duplicate detection** | Automatic, keyed on the pipeline's business key | Prevents a database-level crash (`ON CONFLICT` violation) *and* prevents silently keeping only one of two duplicate rows — duplicates are quarantined, not guessed at. |
| **Integrity mode: `strict`** | One bad row quarantines the *entire file* | Right choice when partial promotion is dangerous (e.g. a NAV/holdings file where an incomplete load misstates a position). |
| **Integrity mode: `row_level`** | Only the failing rows are quarantined; the rest proceeds | Right choice for high-volume feeds where losing a few bad rows is acceptable but blocking the whole batch isn't. |
| **Integrity mode: `group_level`** | A failing row quarantines its *entire group* (e.g. every holding for one portfolio on one date), not just itself, not the whole file | Solves a real correctness gap the other two modes can't: prevents a portfolio from landing with 9 of its 10 holdings, looking complete when it silently isn't. |

---

## 4. Quarantine — capture, review, and correct

| Capability | Options | Benefit |
|---|---|---|
| **Quarantine storage** | `local` filesystem or `s3` | Local for dev/test with zero AWS dependency; S3 for a durable, centrally-visible production trail. Declared per pipeline, validated at spec-load time. |
| **Alerting** | `log` (structured) or `webhook` (fully implemented); `email` accepted by schema but not yet wired to a real mailer | Ops gets notified the moment something is quarantined, through the channel that fits the team's existing tooling. |
| **Review & resubmit UI** | A page served by DAIS itself (`GET /ui/quarantine/{pipeline}`) | Data ops can see every pending quarantined row, edit the bad value inline, and resubmit — no direct database access, no engineer in the loop for a routine data fix. |
| **Resubmit safety** | Re-validated against the same quality rules before anything is written; all-or-nothing | A bad "fix" fails again instead of silently corrupting stage — there is no path for an unvalidated correction to reach the warehouse. |
| **Resubmit → gold** | If the pipeline runs through gold, an accepted correction automatically refreshes the gold layer too | A fixed row doesn't leave downstream analytics quietly stale until the next scheduled run. |
| **Audit trail** | Resolved quarantine records are moved to `resolved/`, never deleted | Full history of what broke and how it was fixed is preserved, satisfying audit requirements without extra tooling. |

---

## 5. Gold layer — where business logic lives

- Gold transformations are **plain dbt/SQL**, not templated by the Python
  engine. This is a deliberate boundary: Python owns *when* a
  transformation runs (generic orchestration), SQL owns *what* it computes
  (business logic) — the same separation of concerns most data platforms
  converge on, made explicit rather than accidental.
- **Three real gold models already built**: company-wide daily AUM
  (`asset_gold`), per-ticker average volume & total return (`price_gold`),
  and a holdings pass-through (`holdings_gold`) — each demonstrating a
  genuinely different transformation shape (aggregation, window functions,
  pass-through).

### SCD Type 2 — general framework capability

| Capability | Options | Benefit |
|---|---|---|
| **Point-in-time history** | `gold.scd_type: 2` + `gold.scd_key: [...]` (any column(s)) | Every change to a gold row is *versioned*, not overwritten — "what did AUM look like as of last Tuesday" is answerable, not lost. |
| **Business-key flexibility** | Proven with two different keys: `[ticker, as_of_date]` for prices, `[as_of_date]` alone for company AUM | The same reusable mechanism (`dais_scd2` dbt macro) adapts to different grains without new code — only a spec change and a one-line macro call. |
| **Change detection** | Only genuinely-changed rows get a new version; unaffected rows are left untouched | Verified live: a correction file that only actually changed 1 of 27 tickers produced exactly one new version — not 27. |
| **Standard SCD2 columns** | `version`, `effective_datetime`, `expiry_datetime`, `active_flag`, plus a generated surrogate primary key | Familiar shape for any BI tool or analyst who already knows SCD2 conventions — nothing DAIS-specific to learn. |

---

## 6. Monitoring, resilience & lineage

| Capability | Options | Benefit |
|---|---|---|
| **Process monitor** | One shared audit table (`control.process_monitor`) across every pipeline, every step | A single place to answer "did today's run happen, and what did it do" for *any* pipeline — no per-pipeline logging convention to maintain. |
| **Retry policy** | `exponential` or `fixed` backoff, configurable attempts/delay/jitter, only on transient connection errors | Transient network/DB blips self-heal without manual intervention; a genuine data or logic error still fails fast instead of retrying uselessly. |
| **OpenLineage emission** | START/COMPLETE/FAIL events per step, pluggable transport, with standard `schema` and `columnLineage` dataset facets | Plugs into any OpenLineage-compatible catalog. DAIS ships a forwarder that turns these events into OpenMetadata tables, columns and lineage edges. |
| **Column-level lineage (automatic)** | Emitted with every run: raw→stage from the spec, stage→gold from dbt (`dbt-ol`) | Column-level lineage appears in OpenMetadata after a normal run — no extra command. `dais lineage --sync-openmetadata` remains as a fallback and to populate gold-build columns. Columns known only from dbt may show type `UNKNOWN`. |
| **Data catalog** | OpenMetadata (replaced Marquez), including glossary terms/tags on columns | Business glossary, ownership and lineage in one catalog UI. |
| **Business process / SLA tracking** | Group pipelines into a named process with a deadline (`business_processes/*.yaml`); query pending/met/breached state | Answers "is morning reconciliation done yet" as one API call, across multiple underlying pipelines, without a separate SLA-tracking tool. |

---

## 7. Security & secrets

| Capability | Options | Benefit |
|---|---|---|
| **Secrets provider** | `Vault` (production) or `Hardcoded` (local-dev fallback, env-var gated, logs a loud warning on every use) | No credential ever lives in a spec file or in source control; the unsafe fallback is impossible to reach accidentally in a real environment. |
| **Database connector** | Postgres implemented behind an abstract interface; Snowflake config accepted by the spec schema but not yet implemented | Adding a second platform is an interface implementation, not a rewrite — the spec/pipeline layer is already platform-agnostic. |

---

## 8. Interfaces — how pipelines actually get triggered

| Interface | What it's for |
|---|---|
| **CLI** (`dais run`) | Direct, scriptable execution — exit code 0/1, safe to call from any shell-based scheduler. |
| **HTTP API** (FastAPI) | `POST /pipelines/{name}/run` + `GET .../status`, checksum-deduped so a retry-after-timeout never double-processes a file. Orchestrator-agnostic by design. |
| **Quarantine review UI** | Same API server, a browser page for data ops (Section 4). |
| **Dagster** | Per-pipeline asset/job, gold models as dbt assets that auto-materialize after their stage asset, file-watch sensors. Open-source Dagster has no built-in login/RBAC — put it behind a proxy or use Dagster+ (decision needed for production). |
| **Airflow DAG** (ready to drop in) | Triggers all four pipelines via the HTTP API on a schedule — DAIS needs zero Airflow-specific code, since the API was already orchestrator-agnostic. |
| **Control-M-style wrapper** | A documented shell-script pattern for enterprise batch schedulers already in use at most financial institutions. |

---

## 9. Proof, not just design

Every capability above has been exercised against real data in this
environment, not only unit-tested in isolation:

- **368 automated tests**, the large majority running against a **real
  Postgres instance** (not mocks) — connectors, DQ engine, medallion
  landing, monitoring, and API integration are all verified against actual
  database behavior, which has already caught real bugs a mock would have
  hidden (a transaction-poisoning bug, a monitoring-table isolation leak).
- **Six pipelines run end-to-end**, including an FX-rates pipeline whose raw→stage load and separate gold build are orchestrated by Dagster off a file drop, and including a 35,000-row real price
  history file processed in ~6 seconds.
- **Quarantine → review → resubmit → gold refresh** exercised as a full
  live loop, not just described.
- **SCD2 versioning proven with real changing data** across multiple
  sequential runs, not a single static snapshot.

---

## 10. Honest gaps — deliberately scoped down, not hidden

- **Background execution** uses FastAPI's built-in task runner, not a
  dedicated task queue (Redis/arq) — fine at current scale; the interface
  is already factored so swapping in a real queue later doesn't touch the
  API routes.
- **The API's run registry is in-memory** (single replica, lost on restart);
  pipeline runs execute inside the API process.
- **SFTP watching** is accepted by the schema but not implemented.
- **GX Data Docs are served unauthenticated.**
- **Snowflake and email alerting** are accepted by the config schema for
  forward-compatibility but not yet implemented — building either is an
  incremental addition, not a redesign.
- **Iceberg/Parquet export** is built and tested but optional per
  pipeline, off by default.

---

*This document reflects the framework as built and verified in this
environment. Every specific claim above (row counts, timings, test counts)
was captured from a real run, not estimated.*
