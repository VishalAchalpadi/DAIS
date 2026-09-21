# Testing the file-watch sensor locally

This walks you from nothing running to "I dropped a file and watched it get
processed." Everything below was run end to end on the reference machine;
where a number is quoted (e.g. pickup time) it was measured, not estimated.

## What you're testing

`specs/portfolio_holdings_ingest.yaml` has a `source.location.watch` block.
Dagster turns that into a **sensor** (`portfolio_holdings_ingest_watch_sensor`)
that lists `scratch_portfolio_demo/landing/` every 30 seconds. Any file
matching `PORTFOLIO_HOLDINGS_{date}.csv` that it hasn't dispatched yet
becomes a Dagster run, which calls the DAIS API, which runs raw → stage →
gold. No one calls the API by hand.

If no file dated *today* has arrived by 09:00 America/New_York, the same
sensor fires the spec's `missed_arrival_alert` (channel `log` in this demo,
so it appears in the Dagster log; use `webhook` for a real notification).

> A second watched pipeline, `fx_rates_ingest` (15s poll, archive-on-success,
> separate `fx_rates_gold` build), is walked through in
> [fx-rates-pipeline.md](fx-rates-pipeline.md). Behaviours common to both:
> files are dispatched **oldest first**; files that don't match `file_pattern`
> are logged as warnings rather than skipped silently; a successfully
> processed file can be moved to an archive folder (`source.location.archive`).

## 0. Prerequisites (one-time)

- Local Postgres running with the `GEODS` database, and `secrets.local.yaml`
  at the repo root containing the `aurora_postgres_prod` entry (host, port,
  dbname, user, password).
- `uv sync --extra dev` has been run; use the repo's `.venv`.
- Optional but recommended: OpenSearch (9200), OpenMetadata (8585) and the
  lineage forwarder (5000) up, if you also want to see lineage. The pipeline
  runs fine without them; lineage events just go nowhere.

## 1. Start the two processes

Both need the **same three environment variables** — forgetting one on one
process is the most common failure here (see `docs/eks-deployment-handoff.md`,
"Per-process environment").

```bash
cd <repo root>
export SECRETS_PROVIDER=hardcoded        # read creds from secrets.local.yaml
export DAIS_API_KEY=dev-key
export OPENLINEAGE_URL=http://localhost:5000
export DAGSTER_HOME="$PWD/.dagster_home" # persists sensor cursors + run history
mkdir -p "$DAGSTER_HOME"

# terminal 1 - the DAIS API
.venv/Scripts/python.exe -m dais.api.main            # Windows venv
# .venv/bin/python -m dais.api.main                  # Linux/macOS venv

# terminal 2 - Dagster (webserver + daemon in one process)
.venv/Scripts/python.exe -m dagster dev -f src/dais/orchestration/dagster/definitions.py
```

`dagster dev` includes the **SensorDaemon** — do not start a separate
`dagster-daemon`. Wait for `Instance is configured with the following
daemons: [... 'SensorDaemon']` in its output and for
`http://127.0.0.1:3000` to load (first start takes ~30s: it runs `dbt parse`).

> **Restart the API after pulling code.** A long-running API process keeps the
> old spec model in memory; a spec with a newer field (like `watch`) is
> rejected as invalid until it restarts.

## 2. Confirm the sensor is on

Open `http://127.0.0.1:3000` → **Automation** (left nav) → find
`portfolio_holdings_ingest_watch_sensor`. It should show **Running**.

Sensors default to *stopped* in Dagster; this one is created running because
`watch.enabled: true` in the spec is the opt-in. If you ever see it stopped,
toggle it on here.

**Expect a burst on first start:** the demo directory already holds files
`seq01`–`seq11`, so the first tick submits a run for each. That is correct
behaviour (they are "new" to a sensor with an empty cursor). Let them finish
(watch **Runs**) before the next step — about a minute or two.

## 3. Drop a file and watch it get picked up

```bash
cat > scratch_portfolio_demo/landing/PORTFOLIO_HOLDINGS_seq13.csv <<'EOF'
date,portfolio,security,shares,price,currency,fx
20261909,USEIP,AAPL.US,500,100,USD,1
20261909,USEIP,TSLA.US,500,200,USD,1
20261909,USEIP,GOOG.US,500,120,USD,1
20261909,FACWI,AAPL.US,500,100,USD,1
20261909,FACWI,RELIANCE.IN,500,30000,INR,0.01
20261909,FACWI,BARC.UK,500,600,GBP,1.2
EOF
```

Rules for the file so the sensor recognises it:
- Name must match `PORTFOLIO_HOLDINGS_{date}.csv` (the `{date}` part can be any
  text — ordering here is by arrival time, not the name).
- It must be **new content**. The API de-duplicates by file checksum, so
  re-dropping identical bytes is (correctly) a no-op. Change a number.
- Write it completely before it lands in the folder if you use a real feed
  (write to a temp name and rename). The sensor has no "settling time" yet, so
  a half-written file could be picked up mid-write.

**Where to watch (in order):**
1. Dagster UI → **Runs**: a new `portfolio_holdings_ingest_job` run appears
   within one poll interval. Measured pickup on the reference machine: **8
   seconds** after the drop (30s is the worst case).
2. Click into the run → the `portfolio_holdings_ingest__stage` step logs
   `run_id`, `status: succeeded`, `layer_reached: gold`.
3. Data — in Postgres:
   ```sql
   SELECT portfolio, security, shares, market_value, round(weight,4)
   FROM core.portfolio_holdings_gold ORDER BY 1,2;
   ```
   `shares` should now be 500 on every row, `weight` summing to 1.0 per
   portfolio, `holding_date` = today.
4. Validation: `http://localhost:8000/ui/great-expectations/index.html`
   (new run under `portfolio_holdings_ingest`).
5. Lineage (if OpenMetadata is up): `http://localhost:8585` → table
   `portfolio_holdings_gold` → Lineage tab. (Use the direct URL
   `/table/dais_postgres_localhost_5432.GEODS.core.portfolio_holdings_gold/lineage`
   — the search box needs a trailing `*`, e.g. `portfolio_holdings*`.)

## 4. Test the missed-file alert

1. Edit `specs/portfolio_holdings_ingest.yaml`: set
   `expected_by:` to a time 2 minutes from now (in `expected_timezone`).
2. Move/delete today's-dated files from `landing/`, or just note that the
   demo files' modification dates are *older than today* — the sensor counts a
   file as "arrived today" only if its own date (arrival time, per
   `multi_file.order_by`) is today. A stale leftover file does not hide a miss.
3. Dagster reloads the code location automatically; wait for the deadline.
4. Dagster UI → sensor → **Tick history** / the daemon log shows a
   `dq_alert ... no file matching 'PORTFOLIO_HOLDINGS_{date}.csv' has arrived
   by HH:MM ...` line — exactly **once per day** (state is in the sensor
   cursor; it resets the next time a file dated today shows up).
5. For a real notification, set `missed_arrival_alert.channel: webhook` and
   `destination:` to a Slack/Teams incoming-webhook URL (a real HTTP POST).
   `channel: email` is **not implemented** (`EmailAlerter` raises
   `NotImplementedError`).

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| Sensor exists but shows **Stopped** | Turn it on in Automation; check the spec has `watch.enabled: true`. |
| Run starts then fails `Vault client is not authenticated` | `SECRETS_PROVIDER=hardcoded` not set on that process. |
| Run succeeds but nothing in OpenMetadata | `OPENLINEAGE_URL` not set on the API process (fails silently), or forwarder/OpenMetadata down. |
| Dropped a file, no run | Same bytes as an earlier file (dedup), name doesn't match the pattern, or API/Dagster is running old code — restart the API. |
| Run fails with `spec ... not found or invalid` | API process is stale (see restart note above). |
| Runs on a burst all fire at once | Expected: Dagster runs up to 10 concurrently. For strict arrival-order processing set a per-sensor concurrency limit of 1 (see the handoff doc, "Dagster"). |
| SFTP location | `kind: sftp` is accepted in a spec but discovery raises "not yet implemented"; the sensor logs it and watches nothing. |
