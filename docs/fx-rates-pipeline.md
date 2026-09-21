# FX rates pipeline — drop a file, watch it flow

Two **separate** pipelines, orchestrated by Dagster:

```
data/fx_rates/incoming/FX_RATES_<YYYYMMDDHHMM>.csv
        │  (watch sensor, every 15s)
        ▼
 fx_rates_ingest          control gate → raw → stage        (specs/fx_rates_ingest.yaml, stops at stage)
        │  file moves to data/fx_rates/archive/<YYYYMMDD>/ once the run succeeds
        ▼  (Dagster automation, when the stage asset updates)
 fx_rates_gold            stg_fx_rates → fx_rates_gold       (gold_builds/fx_rates_gold.yaml, dbt)
```

## 1. The file

Header is **case-sensitive** and must be exactly:

```
fxDate,fromCCY,toCCY,fxRate,sourceType
20260920,USD,USD,1,Reuters7AM
20260920,USD,INR,100,Reuters12PM
20260920,USD,AUD,1.8,Reuters12PM
20260920,USD,HKD,2,Reuters12PM
```

| Column | Rule |
|---|---|
| `fxDate` | `YYYYMMDD`, a real date |
| `fromCCY`, `toCCY` | not empty **and present in `core.t_ref_currency`** |
| `fxRate` | numeric, **> 0** (up to 8 decimal places) |
| `sourceType` | not empty (e.g. `Reuters7AM`, `Reuters12PM`) |

A row is unique by **(fxDate, fromCCY, toCCY, sourceType)** — re-sending the same
key overwrites it (upsert), so a corrected rate replaces the earlier one. A bad
row (unknown currency, non-numeric rate, bad date…) is **quarantined on its own**;
the good rows in the same file still load.

**File name:** `FX_RATES_<YYYYMMDDHHMM>.csv`, e.g. `FX_RATES_202609201200.csv`.
The 12-digit stamp orders several files in a day and dates the arrival for the
missed-file alert. A name that doesn't match (e.g. `FX_RATES_20260920.csv`) is
**skipped with a warning** in the Dagster log — it will not block other files.

Templates: `docs/samples/FX_RATES_TEMPLATE_blank.csv` (header only) and
`docs/samples/FX_RATES_202609201200.csv` (your sample). Copies are in
`data/fx_rates/template/`.

## 2. Currencies — `core.t_ref_currency`

Already created and seeded (31 ISO codes, including USD, INR, AUD, HKD). To
re-run or add codes later (safe to repeat; never deletes anything):

```bash
SECRETS_PROVIDER=hardcoded .venv/Scripts/python.exe -m dais.cli seed-currencies --spec specs/fx_rates_ingest.yaml
```

To allow a new currency, insert it into `core.t_ref_currency` (`currency_code`,
`currency_name`) — no spec change needed.

## 3. Directories (under `data/fx_rates/`)

| Directory | Purpose |
|---|---|
| `incoming/` | drop zone — the sensor watches this |
| `archive/<YYYYMMDD>/` | processed files, rolled into a per-day folder (created automatically) |
| `quarantine/` | rejected rows / files, for review |
| `template/` | the two template CSVs |

**Archiving is a spec setting** (`source.location.archive` in
`specs/fx_rates_ingest.yaml`): remove the block to leave files in `incoming/`;
set `date_subdirs: false` for one flat archive folder. Only a **successful** run
moves the file — a quarantined or failed file stays in `incoming/` for review.
An archived name that already exists is never overwritten (a timestamp is added).

## 4. Drop a file and watch it (Dagster at http://127.0.0.1:3000)

Nothing is processed until you drop a file. The API (:8000) and Dagster (:3000)
are running; the FX sensor and the automation sensor are both **Running**.

1. Fill in a copy of the template and save it into `data/fx_rates/incoming/` as
   `FX_RATES_<YYYYMMDDHHMM>.csv`. (Write it elsewhere and move it in, so the
   sensor never sees a half-written file.)
2. **Dagster → Runs** — within ~15 seconds a `fx_rates_ingest_job` run appears
   (tagged with the file name). It should finish `SUCCESS` in a few seconds.
3. The file leaves `incoming/` and appears in `archive/<today>/`.
4. **Dagster → Runs** again — a second run for the **gold** assets
   (`stg_fx_rates`, `fx_rates_gold`) starts by itself once the stage asset has
   updated. (This is Dagster's automation sensor; if it doesn't happen, check
   **Automation → default_automation_condition_sensor** is On.)
5. **Dagster → Assets → `fx_rates_ingest` / `fx_rates_gold`** shows the graph:
   `fx_rates_ingest/stage → stg_fx_rates → fx_rates_gold`.
6. Check the data:
   ```sql
   SELECT "fxDate","fromCCY","toCCY","fxRate","sourceType" FROM data_in.fx_rates_stage ORDER BY 1,2,3;
   SELECT * FROM core.fx_rates_gold ORDER BY from_ccy, to_ccy, source_type;
   ```

`core.fx_rates_gold` has one row per **(fromCCY, toCCY, sourceType)**:
`latest_fx_date`, `latest_rate`, `prior_rate` (previous date, same source),
`change_pct` (day-over-day %), `inverse_rate` (1 / rate), `gold_loaded_at`.
Snapshots from different sources (7AM vs 12PM) are never compared to each other.
Drop a second file dated the next day and `prior_rate` / `change_pct` fill in.

Also worth a look after the first run: validation history at
`http://localhost:8000/ui/great-expectations/index.html`, lineage in
OpenMetadata (`http://localhost:8585`, table `fx_rates_stage` /
`fx_rates_gold` → Lineage; search needs a trailing `*`, e.g. `fx_rates*`).

**Column-level lineage is automatic.** Each run emits the raw and stage column
lists plus raw→stage column mappings, and the gold build emits stage→gold
mappings via dbt; the OpenMetadata forwarder turns them into columns and
column-level edges (Lineage tab → Layers → Column Level Lineage). Columns that
only dbt knows about may show type `UNKNOWN`; `dais lineage --spec
specs/fx_rates_ingest.yaml --sync-openmetadata` fills any gaps.

## 5. Try the failure paths

| Drop this | Expect |
|---|---|
| A row with `toCCY` = `ZZZ` | run succeeds; that row is in `quarantine/`, the rest load |
| A file with only the header row / a tiny file | run **quarantined** (control gate); file **stays** in `incoming/` |
| The exact same file content again (any name) | not loaded twice (checksum de-dup); because nothing new ran it is **not archived either** and stays in `incoming/` — delete it |
| Nothing by 23:00 America/New_York | the missed-file alert fires once (a `dq_alert` line in the Dagster log; `expected_by` in the spec) |

## 6. Settings you'll likely change (all in `specs/fx_rates_ingest.yaml`)

- `watch.expected_by` / `expected_timezone` — the delivery deadline for the
  missed-file alert (currently 23:00 New York, a placeholder).
- `watch.missed_arrival_alert.channel` — `log` now; `webhook` + a URL sends a real
  notification (Slack/Teams incoming webhook). `email` is not implemented.
- `watch.poll_interval_seconds` — 15 for the demo; 60+ is fine in production.
- `source.location.path` / `archive.path` — an `s3://` location works the same way
  (set `kind: s3`); the archive path must then be an `s3://` URI too.
