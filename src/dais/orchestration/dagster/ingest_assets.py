"""Raw/stage as a Dagster asset - a thin wrapper around the DAIS HTTP API
(DaisApiResource), never a reimplementation of ingestion. One asset per
ingest pipeline, keyed AssetKey([pipeline_name, "stage"]) so gold_assets.py
can wire dbt's source() dependencies to these exact keys and get a real,
connected asset graph rather than two disconnected halves.
"""
from dagster import AssetExecutionContext, AssetKey, AssetsDefinition, Config, MaterializeResult, asset

from dais.orchestration.dagster.api_resource import DaisApiRunFailedError, DaisApiResource


class IngestFileConfig(Config):
    file_path: str
    stop_after: str | None = None


def build_ingest_asset(pipeline_name: str, *, group_name: str = "ingest") -> AssetsDefinition:
    """One asset per pipeline_name - a factory rather than N hand-written
    functions, since every pipeline is invoked identically (spec_name +
    file_path -> the same API endpoints). Adding a pipeline to the
    orchestration graph means adding a spec, not writing new Dagster code."""

    @asset(
        key=AssetKey([pipeline_name, "stage"]),
        group_name=group_name,
        description=(
            f"Runs the {pipeline_name} pipeline (raw->stage, and gold if the spec's "
            "execution.stop_after is 'gold') via the DAIS HTTP API - the same "
            "trigger/poll surface Control-M's shell wrapper uses. Ingestion logic "
            "itself (parsing, control gates, quality, quarantine) lives entirely in "
            "dais.pipeline; this asset is a black-box caller, not a second "
            "execution path."
        ),
    )
    def _ingest_asset(context: AssetExecutionContext, config: IngestFileConfig, dais_api: DaisApiResource) -> MaterializeResult:
        try:
            result = dais_api.run_and_wait(pipeline_name, config.file_path, config.stop_after)
        except DaisApiRunFailedError as exc:
            context.log.error(str(exc))
            raise
        return MaterializeResult(
            metadata={
                "run_id": result["run_id"],
                "status": result["status"],
                "layer_reached": result.get("layer_reached"),
            }
        )

    return _ingest_asset
