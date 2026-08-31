from __future__ import annotations

from pydantic import BaseModel


class RunRequest(BaseModel):
    file_path: str
    stop_after: str | None = None


class RunResponse(BaseModel):
    run_id: str
    status: str


class RunStatusResponse(BaseModel):
    run_id: str
    spec_name: str
    status: str
    layer_reached: str | None = None
    checksum: str | None = None
    error: str | None = None


class BusinessProcessMemberStatus(BaseModel):
    pipeline_name: str
    success_layer: str
    state: str
    last_status: str | None


class BusinessProcessStatusResponse(BaseModel):
    process_name: str
    state: str
    sla_deadline: str
    now: str
    members: list[BusinessProcessMemberStatus]


class QuarantineRecordSummaryResponse(BaseModel):
    quarantine_id: str
    file_name: str
    row_count: int


class QuarantinedRowPayload(BaseModel):
    row_index: int
    row_data: dict
    reasons: list[str]
    quarantined_at: str


class ResubmitRow(BaseModel):
    row_index: int
    row_data: dict[str, str]


class ResubmitRequest(BaseModel):
    rows: list[ResubmitRow]


class ResubmitFailure(BaseModel):
    row_index: int | None = None
    reasons: list[str] = []
    error: str | None = None


class ResubmitResponse(BaseModel):
    accepted: bool
    row_count_upserted: int
    failures: list[ResubmitFailure] = []
    layer_reached: str | None = None
    gold_error: str | None = None
