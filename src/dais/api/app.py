"""Generic FastAPI trigger/status layer - one set of routes that works
for any spec, so external schedulers (Dagster, Control-M, anything else)
can invoke any pipeline without pipeline-specific code.
"""
from __future__ import annotations

import hashlib
import os
from pathlib import Path

from fastapi import BackgroundTasks, Depends, FastAPI, Header, HTTPException, status
from fastapi.responses import HTMLResponse

from dais.api.models import (
    BusinessProcessMemberStatus,
    BusinessProcessStatusResponse,
    QuarantinedRowPayload,
    QuarantineRecordSummaryResponse,
    ResubmitFailure,
    ResubmitRequest,
    ResubmitResponse,
    RunRequest,
    RunResponse,
    RunStatusResponse,
)
from dais.api.quarantine_ui import QUARANTINE_UI_HTML
from dais.api.runs import RunRegistry
from dais.api.worker import ConnectorFactory, S3Factory, execute_pipeline_background
from dais.business_process.evaluator import evaluate_business_process
from dais.business_process.loader import BusinessProcessLoadError, load_business_process
from dais.db import build_connector_for_spec, build_s3_connector_for_spec
from dais.quality.quarantine_review import list_quarantine_records, read_quarantine_record, resubmit_corrections
from dais.spec.loader import SpecLoadError, load_spec


def create_app(
    specs_dir: str | Path = "specs",
    business_processes_dir: str | Path = "business_processes",
    connector_factory: ConnectorFactory = build_connector_for_spec,
    s3_factory: S3Factory = build_s3_connector_for_spec,
    api_key: str | None = None,
) -> FastAPI:
    app = FastAPI(title="DAIS Pipeline API")
    registry = RunRegistry()
    specs_dir = Path(specs_dir)
    business_processes_dir = Path(business_processes_dir)
    expected_api_key = api_key if api_key is not None else os.environ.get("DAIS_API_KEY")

    def check_api_key(x_api_key: str | None = Header(default=None)) -> None:
        if not expected_api_key or x_api_key != expected_api_key:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid or missing API key")

    def load_spec_or_404(spec_name: str):
        try:
            return load_spec(specs_dir / f"{spec_name}.yaml")
        except SpecLoadError as exc:
            raise HTTPException(status_code=404, detail=f"spec {spec_name!r} not found or invalid: {exc}") from exc

    @app.post(
        "/pipelines/{spec_name}/run",
        status_code=status.HTTP_202_ACCEPTED,
        response_model=RunResponse,
        dependencies=[Depends(check_api_key)],
    )
    def trigger_run(spec_name: str, req: RunRequest, background_tasks: BackgroundTasks) -> RunResponse:
        spec = load_spec_or_404(spec_name)

        try:
            file_bytes = Path(req.file_path).read_bytes()
        except OSError as exc:
            raise HTTPException(status_code=400, detail=f"could not read file_path: {exc}") from exc
        checksum = hashlib.sha256(file_bytes).hexdigest()

        existing = registry.find_by_checksum(spec_name, checksum)
        if existing is not None:
            return RunResponse(run_id=existing.run_id, status=existing.status)

        record = registry.create(spec_name, req.file_path, checksum)
        background_tasks.add_task(
            execute_pipeline_background,
            registry,
            spec,
            req.file_path,
            connector_factory,
            record.run_id,
            req.stop_after,
            s3_factory,
        )
        return RunResponse(run_id=record.run_id, status=record.status)

    @app.get(
        "/pipelines/runs/{run_id}/status",
        response_model=RunStatusResponse,
        dependencies=[Depends(check_api_key)],
    )
    def get_run_status(run_id: str) -> RunStatusResponse:
        record = registry.get(run_id)
        if record is None:
            raise HTTPException(status_code=404, detail="run not found")
        return RunStatusResponse(
            run_id=record.run_id,
            spec_name=record.spec_name,
            status=record.status,
            layer_reached=record.layer_reached,
            checksum=record.checksum,
            error=record.error,
        )

    @app.get(
        "/business-processes/{name}/status",
        response_model=BusinessProcessStatusResponse,
        dependencies=[Depends(check_api_key)],
    )
    def get_business_process_status(name: str) -> BusinessProcessStatusResponse:
        try:
            group = load_business_process(business_processes_dir / f"{name}.yaml")
        except BusinessProcessLoadError as exc:
            raise HTTPException(status_code=404, detail=f"business process {name!r} not found: {exc}") from exc

        # No separate connection config on the group itself - it queries
        # the same control.process_monitor its member pipelines write to,
        # so borrow the first member's spec to resolve that connection.
        member_spec = load_spec_or_404(group.members[0].pipeline_name)
        connector, _ = connector_factory(member_spec)
        try:
            result = evaluate_business_process(
                group,
                connector,
                monitoring_schema=member_spec.monitoring.schema_,
                monitoring_table=member_spec.monitoring.table,
            )
        finally:
            connector.close()

        return BusinessProcessStatusResponse(
            process_name=result.process_name,
            state=result.state,
            sla_deadline=result.sla_deadline.isoformat(),
            now=result.now.isoformat(),
            members=[
                BusinessProcessMemberStatus(
                    pipeline_name=m.pipeline_name,
                    success_layer=m.success_layer,
                    state=m.state,
                    last_status=m.last_status,
                )
                for m in result.members
            ],
        )

    @app.get(
        "/pipelines/{spec_name}/quarantine",
        response_model=list[QuarantineRecordSummaryResponse],
        dependencies=[Depends(check_api_key)],
    )
    def list_quarantine(spec_name: str) -> list[QuarantineRecordSummaryResponse]:
        spec = load_spec_or_404(spec_name)
        s3 = s3_factory(spec) if spec.quality.quarantine.kind == "s3" else None
        records = list_quarantine_records(spec, s3)
        return [
            QuarantineRecordSummaryResponse(quarantine_id=r.quarantine_id, file_name=r.file_name, row_count=r.row_count)
            for r in records
        ]

    @app.get(
        "/pipelines/{spec_name}/quarantine/{quarantine_id}",
        response_model=list[QuarantinedRowPayload],
        dependencies=[Depends(check_api_key)],
    )
    def get_quarantine_record(spec_name: str, quarantine_id: str) -> list[QuarantinedRowPayload]:
        spec = load_spec_or_404(spec_name)
        s3 = s3_factory(spec) if spec.quality.quarantine.kind == "s3" else None
        try:
            rows = read_quarantine_record(spec, quarantine_id, s3)
        except (FileNotFoundError, OSError) as exc:
            raise HTTPException(status_code=404, detail=f"quarantine record not found: {exc}") from exc
        return [QuarantinedRowPayload(**row) for row in rows]

    @app.post(
        "/pipelines/{spec_name}/quarantine/{quarantine_id}/resubmit",
        response_model=ResubmitResponse,
        dependencies=[Depends(check_api_key)],
    )
    def resubmit_quarantine(spec_name: str, quarantine_id: str, req: ResubmitRequest) -> ResubmitResponse:
        spec = load_spec_or_404(spec_name)
        s3 = s3_factory(spec) if spec.quality.quarantine.kind == "s3" else None
        connector, _ = connector_factory(spec)
        try:
            outcome = resubmit_corrections(
                spec,
                quarantine_id,
                [{"row_index": r.row_index, "row_data": r.row_data} for r in req.rows],
                connector,
                s3,
            )
        finally:
            connector.close()
        return ResubmitResponse(
            accepted=outcome.accepted,
            row_count_upserted=outcome.row_count_upserted,
            failures=[ResubmitFailure(**f) for f in outcome.failures],
        )

    @app.get("/ui/quarantine/{spec_name}", response_class=HTMLResponse, include_in_schema=False)
    def quarantine_ui(spec_name: str) -> str:
        return QUARANTINE_UI_HTML.replace("__SPEC_NAME__", spec_name)

    return app
