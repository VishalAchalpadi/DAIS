"""Shared control.process_monitor table: begin/complete a step, with row
counts. Each run touches only rows keyed by its own process_id, so many
pipelines can write concurrently without table-level locking - Postgres's
row-level UPSERT locking is enough.

A step writes a `begin` row when it starts and updates that SAME row to
complete/failed/quarantined when it finishes - never a second row for the
same (process_id, step). Implemented as an upsert keyed on
(process_id, step): begin is the initial insert, every later call is the
"update existing row" branch of the same upsert.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from dais.monitoring.ddl import ensure_process_monitor_table
from dais.resilience.connectors.base import DatabaseConnector

_COLUMNS = [
    "process_id",
    "pipeline_name",
    "step",
    "target_table",
    "row_count_in",
    "row_count_out",
    "status",
    "started_at",
    "completed_at",
]


@dataclass
class StepHandle:
    process_id: str
    pipeline_name: str
    step: str
    target_table: str
    started_at: datetime


class ProcessMonitor:
    def __init__(self, connector: DatabaseConnector, schema: str, table: str):
        self.connector = connector
        self.schema = schema
        self.table = table
        ensure_process_monitor_table(connector, schema, table)

    def _write(
        self,
        handle: StepHandle,
        status: str,
        row_count_in: int | None,
        row_count_out: int | None,
        completed_at: datetime | None,
    ) -> None:
        row = (
            handle.process_id,
            handle.pipeline_name,
            handle.step,
            handle.target_table,
            row_count_in,
            row_count_out,
            status,
            handle.started_at,
            completed_at,
        )
        self.connector.upsert(
            self.schema, self.table, _COLUMNS, [row], conflict_columns=["process_id", "step"]
        )

    def begin_step(self, process_id: str, pipeline_name: str, step: str, target_table: str) -> StepHandle:
        handle = StepHandle(
            process_id=process_id,
            pipeline_name=pipeline_name,
            step=step,
            target_table=target_table,
            started_at=datetime.now(timezone.utc),
        )
        self._write(handle, status="begin", row_count_in=None, row_count_out=None, completed_at=None)
        return handle

    def complete_step(self, handle: StepHandle, row_count_in: int, row_count_out: int) -> None:
        self._write(
            handle,
            status="complete",
            row_count_in=row_count_in,
            row_count_out=row_count_out,
            completed_at=datetime.now(timezone.utc),
        )

    def fail_step(self, handle: StepHandle, row_count_in: int | None = None, row_count_out: int | None = None) -> None:
        self._write(
            handle,
            status="failed",
            row_count_in=row_count_in,
            row_count_out=row_count_out,
            completed_at=datetime.now(timezone.utc),
        )

    def quarantine_step(self, handle: StepHandle, row_count_in: int | None = None, row_count_out: int | None = None) -> None:
        self._write(
            handle,
            status="quarantined",
            row_count_in=row_count_in,
            row_count_out=row_count_out,
            completed_at=datetime.now(timezone.utc),
        )

    def get_status(self, process_id: str, step: str) -> tuple | None:
        return self.connector.fetch_one(
            f'SELECT status, row_count_in, row_count_out FROM "{self.schema}"."{self.table}" '
            f"WHERE process_id = %(process_id)s AND step = %(step)s",
            {"process_id": process_id, "step": step},
        )
