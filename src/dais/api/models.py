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
