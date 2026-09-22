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


def _get(path: str) -> dict | None:
    resp = requests.get(f"{OPENMETADATA_URL}/{path}", headers=_headers(), timeout=10)
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    return resp.json()


def _patch(path: str, ops: list[dict]) -> None:
    headers = {**_headers(), "Content-Type": "application/json-patch+json"}
    resp = requests.patch(f"{OPENMETADATA_URL}/{path}", headers=headers, json=ops, timeout=10)
    resp.raise_for_status()


def _om_type(source_type: str | None) -> str:
    """Best-effort OpenLineage/Postgres/DAIS type name -> OpenMetadata
    DataType. Anything unrecognised (dbt-ol sends no schema facet without a
    catalog, so columns known only from column lineage carry no type at
    all) is UNKNOWN rather than a wrong guess."""
    t = (source_type or "").lower()
    if t.startswith(("text", "varchar", "character", "string", "char")):
        return "VARCHAR"
    if t.startswith(("numeric", "decimal", "double", "float", "real")):
        return "NUMERIC"
    if t.startswith("bigint"):
        return "BIGINT"
    if t.startswith(("int", "smallint")):
        return "INT"
    if t.startswith("bool"):
        return "BOOLEAN"
    if t == "date":
        return "DATE"
    if t.startswith("timestamp") and "with time zone" in t:
        return "TIMESTAMPZ"
    if t.startswith("timestamp"):
        return "TIMESTAMP"
    return "UNKNOWN"


def _table_fqn(host: str, port: str, dbname: str, schema_table: str) -> str:
    return f"dais_postgres_{host}_{port}.{dbname}.{schema_table}"


def _ensure_columns(table_fqn: str, wanted: dict[str, str | None]) -> None:
    """Adds any of `wanted` (column -> source type) the OpenMetadata table
    doesn't have yet. Only ever appends: existing columns - and anything
    curated on them, like glossary tags - are never replaced."""
    table = _get(f"tables/name/{table_fqn}?fields=columns")
    if table is None:
        return
    existing = {c["name"] for c in table.get("columns", [])}
    ops = []
    for name, source_type in wanted.items():
        if name in existing:
            continue
        column = {"name": name, "dataType": _om_type(source_type)}
        if column["dataType"] == "VARCHAR":
            column["dataLength"] = 255
        ops.append({"op": "add", "path": "/columns/-", "value": column})
    if ops:
        _patch(f"tables/name/{table_fqn}", ops)


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
    # Create the table only if it isn't there. Re-PUTting an EXISTING table
    # (which carries no `columns` in this body) is read by OpenMetadata as a
    # column change and it prunes the column-level lineage pointing at that
    # table - so every later event for the same table used to silently wipe
    # column lineage an earlier event had just written.
    existing = _get(f"tables/name/{service_name}.{dbname}.{schema}.{table}")
    if existing is not None:
        return existing["id"]
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
            # A run reaching this forwarder may have been triggered via the
            # DAIS API directly, Control-M, or Dagster (src/dais/orchestration/
            # dagster/) - this event carries no reliable signal of which.
            # "CustomPipeline" is OpenMetadata's neutral serviceType for a
            # pipeline source it doesn't have a named connector for, so the
            # service type doesn't falsely claim one specific orchestrator.
            "serviceType": "CustomPipeline",
            "connection": {
                # Descriptive only, like the Postgres connection config in
                # _ensure_table_entity - lineage edges are written directly
                # via this API, OpenMetadata never actually connects out
                # using this config.
                "config": {"type": "CustomPipeline", "sourcePythonClass": "dais.lineage.openmetadata_forwarder"}
            },
        },
    )
    pipeline_body = _put("pipelines", {"name": job_name, "service": service_name})
    return pipeline_body["id"]


# Last non-empty columnsLineage written per (from, to) table pair. An edge is
# a single object in OpenMetadata, and DAIS's own step event (no column
# facets for a gold step) can arrive after dbt-ol's (which has them) for the
# SAME table pair - without this, that later plain PUT would wipe the column
# lineage. Process-local, like _run_datasets.
_edge_columns_cache: dict[tuple[str, str], list[dict]] = {}


def _add_lineage_edge(from_id: str, to_id: str, pipeline_id: str, columns_lineage: list[dict] | None = None) -> None:
    columns_lineage = columns_lineage or _edge_columns_cache.get((from_id, to_id)) or []
    if columns_lineage:
        _edge_columns_cache[(from_id, to_id)] = columns_lineage
    details: dict = {"pipeline": {"id": pipeline_id, "type": "pipeline"}}
    if columns_lineage:
        details["columnsLineage"] = columns_lineage
    _put(
        "lineage",
        {
            "edge": {
                "fromEntity": {"id": from_id, "type": "table"},
                "toEntity": {"id": to_id, "type": "table"},
                "lineageDetails": details,
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
_run_datasets: dict[tuple[str, str], dict] = {}


def _new_state() -> dict:
    return {
        "inputs": set(),
        "outputs": set(),
        "schemas": {},  # dataset key -> {column: source type}
        "column_lineage": {},  # output dataset key -> {out column: [(input key, input column, description)]}
    }


def _collect_facets(state: dict, event: dict) -> None:
    """Standard OpenLineage `schema` and `columnLineage` dataset facets -
    emitted by DAIS's own LineageEmitter for raw/stage and by dbt-ol for
    gold - are what carry column-level detail. Nothing else in an event does."""
    for side in ("inputs", "outputs"):
        for dataset in event.get(side) or []:
            key = _dataset_key(dataset)
            facets = dataset.get("facets") or {}
            for field in (facets.get("schema") or {}).get("fields", []):
                state["schemas"].setdefault(key, {})[field["name"]] = field.get("type")
            if side != "outputs":
                continue
            for out_column, info in ((facets.get("columnLineage") or {}).get("fields") or {}).items():
                description = info.get("transformationDescription")
                for input_field in info.get("inputFields", []):
                    source = (_dataset_key(input_field), input_field["field"], description)
                    # START and COMPLETE both carry the same facets - record each mapping once.
                    sources = state["column_lineage"].setdefault(key, {}).setdefault(out_column, [])
                    if source not in sources:
                        sources.append(source)


def _handle_run_event(event: dict) -> None:
    job = event.get("job") or {}
    namespace, job_name = job.get("namespace"), job.get("name")
    run_id = (event.get("run") or {}).get("runId")
    if not namespace or not job_name or not run_id:
        return

    key = (run_id, job_name)
    state = _run_datasets.setdefault(key, _new_state())
    state["inputs"].update(_dataset_key(d) for d in event.get("inputs") or [])
    state["outputs"].update(_dataset_key(d) for d in event.get("outputs") or [])
    _collect_facets(state, event)

    if event.get("eventType") != "COMPLETE":
        return
    state = _run_datasets.pop(key)

    tables: dict[tuple[str, str], tuple[str, str]] = {}  # dataset key -> (table id, table fqn)
    for ds_key in state["inputs"] | state["outputs"]:
        parsed = _parse_table_dataset(*ds_key)
        if parsed is None:
            continue
        host, port, dbname, schema_table = parsed
        tables[ds_key] = (
            _ensure_table_entity(host, port, dbname, schema_table),
            _table_fqn(host, port, dbname, schema_table),
        )

    # Every column an edge will reference must exist first: the ones a
    # schema facet lists, plus the ones only column lineage mentions.
    wanted: dict[tuple[str, str], dict[str, str | None]] = {}
    for ds_key, columns in state["schemas"].items():
        wanted.setdefault(ds_key, {}).update(columns)
    for out_key, by_column in state["column_lineage"].items():
        for out_column, sources in by_column.items():
            wanted.setdefault(out_key, {}).setdefault(out_column, None)
            for in_key, in_column, _description in sources:
                wanted.setdefault(in_key, {}).setdefault(in_column, None)
    for ds_key, columns in wanted.items():
        if ds_key in tables:
            _ensure_columns(tables[ds_key][1], columns)

    input_keys = [k for k in state["inputs"] if k in tables]
    output_keys = [k for k in state["outputs"] if k in tables]
    if not input_keys or not output_keys:
        return

    pipeline_id = _ensure_pipeline_entity(namespace, job_name)
    for from_key, to_key in product(input_keys, output_keys):
        from_id, from_fqn = tables[from_key]
        to_id, to_fqn = tables[to_key]
        columns_lineage = []
        for out_column, sources in state["column_lineage"].get(to_key, {}).items():
            from_columns = list(dict.fromkeys(f"{from_fqn}.{c}" for k, c, _d in sources if k == from_key))
            if from_columns:
                function = next((d for _k, _c, d in sources if d), None)
                entry = {"fromColumns": from_columns, "toColumn": f"{to_fqn}.{out_column}"}
                if function:
                    entry["function"] = function
                columns_lineage.append(entry)
        _add_lineage_edge(from_id, to_id, pipeline_id, columns_lineage)


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
