"""OpenLineage-compatible HTTP receiver standing in for Marquez.

DAIS's own run-time lineage code (lineage/emitter.py) and the
openlineage-dbt wrapper (dbt-ol, invoked from medallion/gold.py for gold
dbt builds) both already speak the OpenLineage wire protocol as-is - they
POST RunEvent JSON to OPENLINEAGE_URL and neither needed to change for
this backend swap (verified against the installed openlineage-client:
HttpTransport always POSTs to "{OPENLINEAGE_URL}/api/v1/lineage", the
exact path Marquez served).

OpenMetadata has no equivalent OpenLineage-compatible ingestion endpoint
of its own (verified against a real running 2.0.2 server) - its lineage
model is entity-first: a lineage edge references two *existing* entities
by id, plus an optional pipeline entity reference, not an arbitrary
namespace/name pair the way an OpenLineage Dataset is. This service is
what now listens on OPENLINEAGE_URL instead of Marquez, translating each
incoming RunEvent into OpenMetadata's entity + lineage-edge REST calls.
Every entity/edge write uses PUT, which the real server confirmed is an
idempotent upsert (a create returns 201, a repeat returns 200 with the
same id) - safe to call on every event without tracking what already
exists.

Scope, deliberately: only inputs/outputs shaped like a real Postgres table
- namespace "postgres://{host}:{port}", name "{dbname}.{schema}.{table}",
the exact identity LineageEmitter._dataset() and dbt-ol both already use
so their events describe the SAME physical table - become OpenMetadata
table entities. A raw landing file (the source of the very first raw-layer
hop) isn't a database table and has no matching OpenMetadata entity type on
its own; it's skipped, so the file -> raw-table hop doesn't appear in
OpenMetadata even though it does in Marquez. Every hop from the raw table
onward (raw -> stage -> gold, including dbt's own stage -> gold column
lineage) is unaffected. Non-COMPLETE events (START, RUNNING, FAIL, ABORT)
don't have a meaningful, stable input/output set to record as lineage and
are acknowledged with no entities created.
"""
from __future__ import annotations

import logging
import os
import re
from itertools import product

import requests
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

logger = logging.getLogger(__name__)

OPENMETADATA_URL = os.environ.get("OPENMETADATA_URL", "http://localhost:8585/api/v1")
OPENMETADATA_TOKEN = os.environ.get("OPENMETADATA_TOKEN", "")

# Matches the namespace LineageEmitter._dataset() builds for a real table:
# f"postgres://{host}:{port}"
_TABLE_NAMESPACE_RE = re.compile(r"^postgres://(?P<host>[^:/]+):(?P<port>\d+)$")

app = FastAPI(title="DAIS OpenLineage-to-OpenMetadata forwarder")


def _headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {OPENMETADATA_TOKEN}", "Content-Type": "application/json"}


def _put(path: str, body: dict) -> dict:
    resp = requests.put(f"{OPENMETADATA_URL}/{path}", headers=_headers(), json=body, timeout=10)
    resp.raise_for_status()
    # PUT /lineage returns 200 with an empty body on success (verified
    # against the real server) - every other entity PUT returns the entity.
    return resp.json() if resp.content else {}


def _parse_table_dataset(namespace: str, name: str) -> tuple[str, str, str, str] | None:
    """(host, port, dbname, schema.table split) if this dataset is a real
    Postgres table by LineageEmitter's own identity convention, else None
    (a raw landing file, or anything else not shaped "schema.table")."""
    match = _TABLE_NAMESPACE_RE.match(namespace)
    if not match:
        return None
    parts = name.split(".")
    if len(parts) != 3:
        return None
    dbname, schema, table = parts
    return match["host"], match["port"], dbname, f"{schema}.{table}"


def _ensure_table_entity(host: str, port: str, dbname: str, schema_table: str) -> str:
    schema, table = schema_table.split(".", 1)
    service_name = f"dais_postgres_{host}_{port}"

    _put(
        "services/databaseServices",
        {
            "name": service_name,
            "serviceType": "Postgres",
            "connection": {
                "config": {
                    "type": "Postgres",
                    # Descriptive only - lineage edges are written directly via
                    # this API, OpenMetadata never connects out using this
                    # config, so a real password isn't available (or needed)
                    # from an OpenLineage event.
                    "username": "unknown",
                    "authType": {"password": "unset"},
                    "hostPort": f"{host}:{port}",
                    "database": dbname,
                }
            },
        },
    )
    _put("databases", {"name": dbname, "service": service_name})
    _put("databaseSchemas", {"name": schema, "database": f"{service_name}.{dbname}"})
    table_body = _put(
        "tables", {"name": table, "databaseSchema": f"{service_name}.{dbname}.{schema}"}
    )
    return table_body["id"]


def _ensure_pipeline_entity(namespace: str, job_name: str) -> str:
    service_name = namespace
    _put(
        "services/pipelineServices",
        {
            "name": service_name,
            "serviceType": "Airflow",
            "connection": {
                "config": {"type": "Airflow", "hostPort": "http://localhost:8080", "connection": {"type": "Backend"}}
            },
        },
    )
    pipeline_body = _put("pipelines", {"name": job_name, "service": service_name})
    return pipeline_body["id"]


def _add_lineage_edge(from_id: str, to_id: str, pipeline_id: str) -> None:
    _put(
        "lineage",
        {
            "edge": {
                "fromEntity": {"id": from_id, "type": "table"},
                "toEntity": {"id": to_id, "type": "table"},
                "lineageDetails": {"pipeline": {"id": pipeline_id, "type": "pipeline"}},
            }
        },
    )


def _dataset_key(dataset: dict) -> tuple[str, str]:
    return dataset.get("namespace", ""), dataset.get("name", "")


# DAIS's own LineageEmitter (lineage/emitter.py) splits a single logical
# step across two events - inputs are only ever sent on START
# (pipeline.py's emitter.start(step, run_id, inputs=..., outputs=...)),
# outputs only on COMPLETE. Marquez tolerates this because its model is
# job-centric (a job's current inputs/outputs get updated by whichever
# event carries them); OpenMetadata's is edge-centric and needs both sides
# at once. So accumulate every dataset seen across a (run id, job name)
# pair's events - job name, not run id alone, because one DAIS pipeline
# run reuses the SAME run_id across every step (control_gate/raw/stage/
# gold all share it; only job.name distinguishes them, e.g.
# "holdings_ingest.raw" vs "holdings_ingest.stage") - and act only once
# that step's COMPLETE arrives with its own accumulated set. Process-local
# and lost on restart - an acceptable limitation for a dev-local
# forwarder; a run in flight across a restart simply won't get lineage
# recorded for the step(s) that started before it.
_run_datasets: dict[tuple[str, str], dict[str, set[tuple[str, str]]]] = {}


def _handle_run_event(event: dict) -> None:
    job = event.get("job") or {}
    namespace, job_name = job.get("namespace"), job.get("name")
    run_id = (event.get("run") or {}).get("runId")
    if not namespace or not job_name or not run_id:
        return

    key = (run_id, job_name)
    state = _run_datasets.setdefault(key, {"inputs": set(), "outputs": set()})
    state["inputs"].update(_dataset_key(d) for d in event.get("inputs") or [])
    state["outputs"].update(_dataset_key(d) for d in event.get("outputs") or [])

    if event.get("eventType") != "COMPLETE":
        return
    state = _run_datasets.pop(key)

    def _table_ids(datasets: set[tuple[str, str]]) -> list[str]:
        ids = []
        for ds_namespace, ds_name in datasets:
            parsed = _parse_table_dataset(ds_namespace, ds_name)
            if parsed is None:
                continue
            host, port, dbname, schema_table = parsed
            ids.append(_ensure_table_entity(host, port, dbname, schema_table))
        return ids

    input_ids = _table_ids(state["inputs"])
    output_ids = _table_ids(state["outputs"])
    if not input_ids or not output_ids:
        return

    pipeline_id = _ensure_pipeline_entity(namespace, job_name)
    for from_id, to_id in product(input_ids, output_ids):
        _add_lineage_edge(from_id, to_id, pipeline_id)


@app.post("/api/v1/lineage")
async def receive_lineage_event(request: Request) -> JSONResponse:
    event = await request.json()
    try:
        _handle_run_event(event)
    except Exception:
        # Best-effort, same spirit as row_validator.py's Data Docs update:
        # a lineage-forwarding failure must never surface to (or retry-storm)
        # the caller (DAIS's own pipeline run, or dbt-ol) - log and ack.
        logger.warning("failed to forward OpenLineage event to OpenMetadata", exc_info=True)
    return JSONResponse(status_code=201, content={})


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    port = int(os.environ.get("OPENLINEAGE_FORWARDER_PORT", "5000"))
    uvicorn.run(app, host="0.0.0.0", port=port)


if __name__ == "__main__":
    main()
