"""Airflow DAG that triggers DAIS's asset/holdings/price/benchmark
pipelines through its HTTP API (POST .../run, then poll .../status),
the same pattern as the Control-M wrapper script in the main README -
DAIS itself needs no changes or awareness of Airflow.

Setup (this DAG assumes nothing about where DAIS's API is running - it
just needs to reach it over HTTP):

1. Airflow Connection - create an HTTP connection named `dais_api`:
     Conn Type: HTTP
     Host:      the DAIS API's host, e.g. http://127.0.0.1 (or your
                deployment's real host)
     Port:      8000 (or wherever uvicorn/gunicorn is serving it)
     Password:  the DAIS_API_KEY value (sent as the X-API-Key header)

2. Airflow Variable (optional) - `dais_source_data_dir`, the base
   directory DAIS's file-based specs read from (defaults to a
   plausible value below if unset). Only matters for pipelines with
   `source.location.kind: local`; holdings_ingest reads from S3 and
   doesn't use this at all.

3. Drop this file into Airflow's `dags/` folder. Each task computes its
   expected file path from the run date using the same `file_pattern`
   each pipeline's own spec declares (specs/*.yaml), so this DAG stays
   in sync with the specs rather than guessing at naming separately.

This DAG does NOT run DAIS in-process - it only ever calls the API, so
it works whether DAIS is running on this same box, a separate server,
or in a container. If a pipeline run ends in `failed` or `quarantined`,
the Airflow task is marked failed too, so it shows up for someone to
either fix the data or resolve it through the quarantine review UI at
GET /ui/quarantine/{spec_name}.
"""
from __future__ import annotations

import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator

import requests

# Poll cadence while a run is in flight - DAIS runs are typically
# seconds to low minutes for these file sizes, so this stays responsive
# without hammering the API.
POLL_INTERVAL_SECONDS = 5
POLL_TIMEOUT_SECONDS = 30 * 60

TERMINAL_STATUSES = {"succeeded", "failed", "quarantined"}

# Mirrors specs/*.yaml's source.location.path + file_pattern - update
# here if a spec's path/pattern changes, since this DAG has no other
# way to know it.
PIPELINES = {
    "asset_ingest": "{base_dir}/assets/incoming/assets_{date}.csv",
    "holdings_ingest": "s3://dais-example-bucket/holdings/incoming/HOLDINGS_{date}.txt",
    "price_ingest": "{base_dir}/prices/incoming/prices_{date}.csv",
    "benchmark_ingest": "{base_dir}/benchmark/incoming/BENCHMARK_{date}.csv",
}


def _dais_conn():
    """Reads the `dais_api` HTTP Connection - base_url + api_key."""
    conn = BaseHook.get_connection("dais_api")
    scheme = conn.schema or "http"
    host = conn.host or "127.0.0.1"
    base_url = f"{scheme}://{host}"
    if conn.port:
        base_url += f":{conn.port}"
    api_key = conn.password
    if not api_key:
        raise AirflowException("dais_api connection is missing its password (used as X-API-Key)")
    return base_url, api_key


def trigger_and_wait(spec_name: str, file_path: str) -> dict:
    base_url, api_key = _dais_conn()
    headers = {"X-API-Key": api_key, "Content-Type": "application/json"}

    resp = requests.post(
        f"{base_url}/pipelines/{spec_name}/run",
        json={"file_path": file_path},
        headers=headers,
        timeout=30,
    )
    resp.raise_for_status()
    run_id = resp.json()["run_id"]

    deadline = time.monotonic() + POLL_TIMEOUT_SECONDS
    while True:
        status_resp = requests.get(
            f"{base_url}/pipelines/runs/{run_id}/status", headers=headers, timeout=30
        )
        status_resp.raise_for_status()
        body = status_resp.json()

        if body["status"] in TERMINAL_STATUSES:
            if body["status"] != "succeeded":
                raise AirflowException(
                    f"{spec_name} run {run_id} ended in status={body['status']!r}: "
                    f"{body.get('error')} - check GET /ui/quarantine/{spec_name} if quarantined"
                )
            return body

        if time.monotonic() > deadline:
            raise AirflowException(f"{spec_name} run {run_id} did not reach a terminal state within {POLL_TIMEOUT_SECONDS}s")

        time.sleep(POLL_INTERVAL_SECONDS)


def _make_task_callable(spec_name: str, path_template: str):
    def _run(**context):
        from airflow.models import Variable

        base_dir = Variable.get("dais_source_data_dir", default_var="./data")
        date = context["ds_nodash"]  # e.g. 20260830, matches the sample files' naming
        file_path = path_template.format(base_dir=base_dir, date=date)
        return trigger_and_wait(spec_name, file_path)

    return _run


default_args = {
    "owner": "data-eng-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dais_daily_pipelines",
    description="Triggers DAIS's asset/holdings/price/benchmark ingest pipelines via its HTTP API",
    default_args=default_args,
    # `schedule_interval` works across all Airflow 2.x versions; 2.4+
    # also accepts the newer `schedule=` name for the same thing.
    schedule_interval="0 6 * * *",  # 06:00 daily - adjust to when files actually land
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["dais"],
) as dag:
    for spec_name, path_template in PIPELINES.items():
        PythonOperator(
            task_id=spec_name,
            python_callable=_make_task_callable(spec_name, path_template),
        )
