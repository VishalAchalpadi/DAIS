"""Background execution: called via FastAPI's BackgroundTasks so a POST
/run returns 202 immediately and the pipeline actually runs after the
response is sent - never inline in the request.
"""
from __future__ import annotations

from typing import Callable

from dais.api.runs import RunRegistry
from dais.db import build_s3_connector_for_spec
from dais.pipeline import run_pipeline
from dais.resilience.connectors.s3_connector import S3Connector
from dais.spec.models import PipelineSpec

ConnectorFactory = Callable[[PipelineSpec], tuple]
S3Factory = Callable[[PipelineSpec], S3Connector]


def execute_pipeline_background(
    registry: RunRegistry,
    spec: PipelineSpec,
    file_path: str,
    connector_factory: ConnectorFactory,
    run_id: str,
    stop_after: str | None,
    s3_factory: S3Factory = build_s3_connector_for_spec,
) -> None:
    try:
        connector, connection_params = connector_factory(spec)
        try:
            result = run_pipeline(
                spec,
                file_path=file_path,
                connector=connector,
                connection_params=connection_params,
                s3=s3_factory(spec),
                run_id=run_id,
                stop_after=stop_after,
            )
            registry.update_from_result(run_id, result)
        finally:
            connector.close()
    except Exception as exc:  # never fail silently - the run must land in a terminal state
        registry.mark_failed(run_id, str(exc))
