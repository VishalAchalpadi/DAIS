"""Pushes REAL column-level lineage into OpenMetadata, reusing
lineage.column_lineage's already-built column-edge computation (raw->stage
from the spec's own rules, stage->gold by parsing dbt's compiled SQL) -
previously only exposed via `dais lineage`, which just printed it as JSON.

Table-level lineage (this pipeline's tables and their raw->stage->gold
edges) already gets created automatically at run time by
openmetadata_forwarder.py, translating DAIS's own OpenLineage events.
Column-level detail isn't available there - an OpenLineage RunEvent
carries dataset names, not column-level transformation info - so this is
a separate, explicit sync step: run `dais lineage --spec <spec> --sync-openmetadata`
after a real pipeline run to attach column-level detail to the lineage
edges that run already created.

Idempotent by design: a table's columns are only populated here if empty
(so it never clobbers hand-curated metadata - e.g. glossary term tags
manually added to a column - by regenerating the whole columns list from
scratch), and every entity/edge write is the same idempotent PUT-based
upsert openmetadata_forwarder.py already relies on.
"""
from __future__ import annotations

import logging

import requests

from dais.lineage.column_lineage import ColumnEdge, build_lineage_graph
from dais.lineage.openmetadata_forwarder import (
    OPENMETADATA_URL,
    _ensure_pipeline_entity,
    _headers,
    _put,
)
from dais.spec.models import PipelineSpec

logger = logging.getLogger(__name__)

_PG_TYPE_MAP = {
    "character varying": "VARCHAR",
    "text": "VARCHAR",
    "numeric": "NUMERIC",
    "integer": "INT",
    "bigint": "BIGINT",
    "boolean": "BOOLEAN",
    "date": "DATE",
    "timestamp without time zone": "TIMESTAMP",
    "timestamp with time zone": "TIMESTAMPZ",
}


def _pg_columns(connection_params: dict, schema: str, table: str) -> list[tuple[str, str]]:
    import psycopg2

    conn = psycopg2.connect(
        host=connection_params["host"],
        port=connection_params["port"],
        dbname=connection_params["dbname"],
        user=connection_params["user"],
        password=connection_params["password"],
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position",
                (schema, table),
            )
            return cur.fetchall()
    finally:
        conn.close()


def _service_name(connection_params: dict) -> str:
    return f"dais_postgres_{connection_params['host']}_{connection_params['port']}"


def _table_fqn(connection_params: dict, schema: str, table: str) -> str:
    return f"{_service_name(connection_params)}.{connection_params['dbname']}.{schema}.{table}"


def _ensure_columns(connection_params: dict, schema: str, table: str) -> None:
    """Populates a table entity's columns from its real Postgres schema,
    but ONLY if it currently has none - never overwrites (would silently
    wipe any hand-curated column metadata, e.g. glossary tags)."""
    table_fqn = _table_fqn(connection_params, schema, table)
    resp = requests.get(f"{OPENMETADATA_URL}/tables/name/{table_fqn}", headers=_headers(), timeout=10)
    if resp.status_code != 200:
        logger.warning("table %r not found in OpenMetadata - has a pipeline run created it yet?", table_fqn)
        return
    if resp.json().get("columns"):
        return  # already populated, don't clobber

    columns = [
        {"name": name, "dataType": _PG_TYPE_MAP.get(pg_type, "UNKNOWN"), **({"dataLength": 255} if _PG_TYPE_MAP.get(pg_type) == "VARCHAR" else {})}
        for name, pg_type in _pg_columns(connection_params, schema, table)
    ]
    if not columns:
        return
    resp = requests.patch(
        f"{OPENMETADATA_URL}/tables/name/{table_fqn}",
        headers={**_headers(), "Content-Type": "application/json-patch+json"},
        json=[{"op": "add", "path": "/columns", "value": columns}],
        timeout=10,
    )
    resp.raise_for_status()


def _table_id(connection_params: dict, schema: str, table: str) -> str:
    table_fqn = _table_fqn(connection_params, schema, table)
    resp = requests.get(f"{OPENMETADATA_URL}/tables/name/{table_fqn}", headers=_headers(), timeout=10)
    resp.raise_for_status()
    return resp.json()["id"]


def _column_fqn(connection_params: dict, schema: str, table: str, column: str) -> str:
    return f"{_table_fqn(connection_params, schema, table)}.{column}"


def _push_hop(
    connection_params: dict,
    namespace: str,
    job_name: str,
    step: str,
    from_schema_table: str,
    to_schema_table: str,
    edges: list[ColumnEdge],
) -> None:
    from_schema, from_table = from_schema_table.split(".", 1)
    to_schema, to_table = to_schema_table.split(".", 1)

    _ensure_columns(connection_params, from_schema, from_table)
    _ensure_columns(connection_params, to_schema, to_table)

    from_id = _table_id(connection_params, from_schema, from_table)
    to_id = _table_id(connection_params, to_schema, to_table)
    pipeline_id = _ensure_pipeline_entity(namespace, f"{job_name}.{step}")

    columns_lineage = [
        {
            "fromColumns": [_column_fqn(connection_params, from_schema, from_table, e.source.column)],
            "toColumn": _column_fqn(connection_params, to_schema, to_table, e.target.column),
            "function": e.transformation,
        }
        for e in edges
    ]
    _put(
        "lineage",
        {
            "edge": {
                "fromEntity": {"id": from_id, "type": "table"},
                "toEntity": {"id": to_id, "type": "table"},
                "lineageDetails": {
                    "pipeline": {"id": pipeline_id, "type": "pipeline"},
                    "columnsLineage": columns_lineage,
                },
            }
        },
    )


def sync_column_lineage(spec: PipelineSpec, connection_params: dict) -> None:
    """Computes real column edges via lineage.column_lineage.build_lineage_graph
    (same function `dais lineage` already uses) and pushes them into
    OpenMetadata as column-level lineage detail on the raw->stage (and,
    if spec.gold is set, stage->gold) table edges. Requires those table
    edges to already exist (i.e. this pipeline has actually run at least
    once, so openmetadata_forwarder.py already created them)."""
    edges = build_lineage_graph(spec, **connection_params)

    raw_table = f"{spec.raw.schema_}.{spec.raw.table}"
    stage_table = f"{spec.stage.schema_}.{spec.stage.table}"
    raw_to_stage = [e for e in edges if e.source.layer == "raw" and e.target.layer == "stage"]
    if raw_to_stage:
        _push_hop(
            connection_params, spec.lineage.namespace, spec.lineage.job_name, "stage",
            raw_table, stage_table, raw_to_stage,
        )

    if spec.gold is not None:
        gold_table = f"{spec.gold.schema_}.{spec.gold.dbt_select}"
        stage_to_gold = [e for e in edges if e.source.layer == "stage" and e.target.layer == "gold"]
        if stage_to_gold:
            _push_hop(
                connection_params, spec.lineage.namespace, spec.lineage.job_name, "gold",
                stage_table, gold_table, stage_to_gold,
            )
