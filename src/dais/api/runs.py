"""Run registry: tracks run_id -> status, dedups on checksum. This
extends bronze's checksum dedup up to the API layer, so a Control-M
retry-after-timeout can't double-process the same file - it gets back
the existing run_id instead of a second execution.
"""
from __future__ import annotations

import threading
import uuid
from dataclasses import dataclass

from dais.pipeline import PipelineRunResult


@dataclass
class RunRecord:
    run_id: str
    spec_name: str
    file_path: str
    checksum: str
    status: str  # "running" | "succeeded" | "failed" | "quarantined"
    layer_reached: str | None = None
    error: str | None = None


class RunRegistry:
    def __init__(self):
        self._lock = threading.Lock()
        self._by_run_id: dict[str, RunRecord] = {}
        self._dedup_key: dict[tuple[str, str], str] = {}

    def find_by_checksum(self, spec_name: str, checksum: str) -> RunRecord | None:
        with self._lock:
            run_id = self._dedup_key.get((spec_name, checksum))
            return self._by_run_id.get(run_id) if run_id else None

    def create(self, spec_name: str, file_path: str, checksum: str) -> RunRecord:
        with self._lock:
            run_id = str(uuid.uuid4())
            record = RunRecord(
                run_id=run_id, spec_name=spec_name, file_path=file_path, checksum=checksum, status="running"
            )
            self._by_run_id[run_id] = record
            self._dedup_key[(spec_name, checksum)] = run_id
            return record

    def get(self, run_id: str) -> RunRecord | None:
        with self._lock:
            return self._by_run_id.get(run_id)

    def update_from_result(self, run_id: str, result: PipelineRunResult) -> None:
        with self._lock:
            record = self._by_run_id[run_id]
            record.status = result.status
            record.layer_reached = result.layer_reached
            record.error = result.error

    def mark_failed(self, run_id: str, error: str) -> None:
        with self._lock:
            record = self._by_run_id[run_id]
            record.status = "failed"
            record.error = error
