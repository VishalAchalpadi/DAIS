"""Background execution: called via FastAPI's BackgroundTasks so a POST
/run returns 202 immediately and the pipeline actually runs after the
response is sent - never inline in the request.
"""
from __future__ import annotations

import logging
from typing import Callable

from dais.api.runs import RunRegistry
from dais.db import build_s3_connector_for_spec
from dais.ingestion.archiver import archive_source_file, should_archive
from dais.pipeline import run_pipeline
from dais.resilience.connectors.s3_connector import S3Connector
from dais.spec.models import PipelineSpec

log = logging.getLogger(__name__)

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
            s3 = s3_factory(spec)
            result = run_pipeline(
                spec,
                file_path=file_path,
                connector=connector,
                connection_params=connection_params,
                s3=s3,
                run_id=run_id,
                stop_after=stop_after,
            )
            registry.update_from_result(run_id, result)
            if should_archive(spec, result.status, result.layer_reached, stop_after):
                # The data is already landed - failing to move the file must
                # not turn a successful run into a failed one.
                try:
                    dest = archive_source_file(spec, file_path, s3)
                    log.info("archived %s -> %s", file_path, dest)
                except Exception:
                    log.exception("run %s succeeded but archiving %s failed", run_id, file_path)
        finally:
            connector.close()
    except Exception as exc:  # never fail silently - the run must land in a terminal state
        registry.mark_failed(run_id, str(exc))


def execute_multi_file_background(
    registry: RunRegistry,
    spec: PipelineSpec,
    ordered_runs: list[tuple[str, str, bool]],  # (run_id, file_path, already_ran)
    on_earlier_failure: str,  # "continue" | "stop"
    connector_factory: ConnectorFactory,
    stop_after: str | None,
    s3_factory: S3Factory = build_s3_connector_for_spec,
) -> None:
    """Runs an "all"-mode multi-file batch's files in order, one at a time
    (never in parallel - order is the whole point when files are a
    dependent chain). already_ran entries (a prior trigger already
    processed that exact checksum - see app.py's dedup-by-checksum check)
    are skipped without re-executing, but still counted as this batch's
    outcome so far for on_earlier_failure purposes.

    Runs entirely as ONE background task (unlike the single-file path's
    per-run task) because on_earlier_failure: "stop" needs to inspect
    each file's result before deciding whether to start the next one -
    that hook doesn't exist if every file were queued as independent
    background tasks up front."""
    for run_id, file_path, already_ran in ordered_runs:
        if not already_ran:
            execute_pipeline_background(
                registry, spec, file_path, connector_factory, run_id, stop_after, s3_factory
            )
        if on_earlier_failure == "stop":
            record = registry.get(run_id)
            if record is not None and record.status == "failed":
                _skip_remaining(registry, ordered_runs, after_run_id=run_id)
                return


def _skip_remaining(registry: RunRegistry, ordered_runs: list[tuple[str, str, bool]], *, after_run_id: str) -> None:
    skipping = False
    for run_id, _file_path, _already_ran in ordered_runs:
        if skipping:
            registry.mark_skipped(run_id)
        elif run_id == after_run_id:
            skipping = True
