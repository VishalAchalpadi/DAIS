"""Creates control.process_monitor if it doesn't exist - shared across
ALL pipelines, not created per-spec. Schema/table name are fixed
platform-wide (a spec's `monitoring` block is descriptive, not a
per-pipeline table definition).
"""
from __future__ import annotations

from dais.resilience.connectors.base import ColumnDef, DatabaseConnector

PROCESS_MONITOR_COLUMNS = [
    ColumnDef("process_id", "TEXT"),
    ColumnDef("pipeline_name", "TEXT"),
    ColumnDef("step", "TEXT"),
    ColumnDef("target_table", "TEXT"),
    ColumnDef("row_count_in", "BIGINT"),
    ColumnDef("row_count_out", "BIGINT"),
    ColumnDef("status", "TEXT"),
    ColumnDef("started_at", "TIMESTAMP"),
    ColumnDef("completed_at", "TIMESTAMP"),
]


def ensure_process_monitor_table(connector: DatabaseConnector, schema: str, table: str) -> None:
    connector.create_table_if_not_exists(
        schema, table, PROCESS_MONITOR_COLUMNS, unique_columns=["process_id", "step"]
    )
