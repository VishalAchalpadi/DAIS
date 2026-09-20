from __future__ import annotations

from pydantic import BaseModel


class RunRequest(BaseModel):
    # Omit to have the server discover the file(s) to process from the
    # spec's source.location (requires location.multi_file to be set) -
    # see dais.ingestion.file_discovery.
    file_path: str | None = None
    stop_after: str | None = None


class RunResponse(BaseModel):
    run_id: str
    status: str


class RunBatchResponse(BaseModel):
    """Returned instead of RunResponse when file_path was omitted and
    the spec's source.location.multi_file.mode resolved to more than one
    file ("all" mode) - one run_id per resolved file, in processing
    order. Poll each via GET /pipelines/runs/{run_id}/status."""

    run_ids: list[str]
    status: str


class RunStatusResponse(BaseModel):
    run_id: str
    spec_name: str
    status: str
    layer_reached: str | None = None
    checksum: str | None = None
    error: str | None = None
    quarantine_location: str | None = None
    failure_reasons: list[str] = []


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


class QuarantineSpecSummaryResponse(BaseModel):
    spec_name: str
    pending_count: int


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
