"""The orchestrator: runs control_gate -> raw -> stage -> gold generically
from a spec, honoring `execution.stop_after`. This is what the API's
background worker and the CLI both call - a new pipeline requires a new
YAML spec, never a change here.
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from dais.ai.anomaly_detector import detect_anomalies, get_trailing_profiles, write_profile_history
from dais.ai.profiler import compute_profile
from dais.execution import layers_to_run
from dais.lineage.emitter import LineageEmitter, build_emitter
from dais.medallion.bronze import land_raw
from dais.medallion.gold import run_gold
from dais.medallion.silver import land_stage
from dais.monitoring.process_monitor import ProcessMonitor
from dais.parsers import get_parser
from dais.parsers.fixed_width_parser import parse_fixed_width_chunked
from dais.quality.control_gates import run_control_gates
from dais.quality.dq_alerts import get_alerter
from dais.quality.file_validator import (
    enforce_integrity_mode,
    raise_alert,
    raise_control_gate_alert,
    write_quarantine,
    write_raw_file_quarantine,
)
from dais.quality.row_validator import validate_dataframe
from dais.resilience.connectors.base import DatabaseConnector
from dais.resilience.connectors.s3_connector import S3Connector, parse_s3_uri
from dais.spec.models import PipelineSpec


# Above this file size, a fixed_width source is parsed as parallel
# byte-range chunks instead of one pass - see parse_fixed_width_chunked.
CHUNKED_PARSE_THRESHOLD_BYTES = 5_000_000


@dataclass
class PipelineRunResult:
    run_id: str
    status: str  # "succeeded" | "failed" | "quarantined"
    layer_reached: str | None
    checksum: str | None = None
    quarantine_location: str | None = None
    error: str | None = None
    failure_reasons: list[str] = field(default_factory=list)


def _read_source_bytes(file_path: str, s3: S3Connector | None) -> bytes:
    if file_path.startswith("s3://"):
        if s3 is None:
            raise ValueError("an S3Connector is required to read s3:// source files")
        bucket, key = parse_s3_uri(file_path)
        return s3.get_object(bucket, key)
    return Path(file_path).read_bytes()


def run_pipeline(
    spec: PipelineSpec,
    *,
    file_path: str,
    connector: DatabaseConnector,
    connection_params: dict | None = None,
    s3: S3Connector | None = None,
    stop_after: str | None = None,
    run_id: str | None = None,
    lineage: LineageEmitter | None = None,
) -> PipelineRunResult:
    run_id = run_id or str(uuid.uuid4())
    stop_after = stop_after or spec.execution.stop_after
    layers = layers_to_run(stop_after)

    monitor = ProcessMonitor(connector, spec.monitoring.schema_, spec.monitoring.table)
    emitter = lineage or build_emitter(spec, connection_params=connection_params)
    file_name = Path(file_path).name

    raw_bytes = _read_source_bytes(file_path, s3)

    # --- control gate ---
    handle = monitor.begin_step(run_id, spec.pipeline_name, "control_gate", file_path)
    emitter.start("control_gate", run_id, inputs=[file_path])
    gate_result = run_control_gates(raw_bytes, spec.control_gates, spec.parser)
    if not gate_result.passed:
        monitor.quarantine_step(handle, row_count_in=gate_result.row_count, row_count_out=0)
        emitter.fail("control_gate", run_id)
        raise_control_gate_alert(gate_result.failures, spec.quality, file_name)
        quarantine_location = write_raw_file_quarantine(raw_bytes, spec.quality, file_name, s3)
        return PipelineRunResult(
            run_id=run_id,
            status="quarantined",
            layer_reached=None,
            quarantine_location=quarantine_location,
            error="; ".join(gate_result.failures),
            failure_reasons=gate_result.failures,
        )
    monitor.complete_step(handle, row_count_in=gate_result.row_count, row_count_out=gate_result.row_count)
    emitter.complete("control_gate", run_id)

    # --- raw (bronze) ---
    raw_target = f"{spec.raw.schema_}.{spec.raw.table}"
    handle = monitor.begin_step(run_id, spec.pipeline_name, "raw", raw_target)
    emitter.start("raw", run_id, inputs=[file_path], outputs=[raw_target])
    if spec.parser.type == "fixed_width" and len(raw_bytes) > CHUNKED_PARSE_THRESHOLD_BYTES:
        # Parsed as parallel byte-range chunks, but still landed and
        # validated as ONE combined batch below - chunks are never
        # validated independently, even under strict integrity_mode.
        parsed_df = parse_fixed_width_chunked(raw_bytes, spec.parser)
    else:
        parser = get_parser(spec.parser.type)
        parsed_df = parser.parse(raw_bytes, spec.parser)
    raw_result = land_raw(
        parsed_df, spec, connector, file_name=file_name, file_path=file_path, file_bytes=raw_bytes, batch_id=run_id
    )
    monitor.complete_step(handle, row_count_in=parsed_df.height, row_count_out=raw_result.row_count_out)
    emitter.complete("raw", run_id, outputs=[raw_target])

    if "stage" not in layers:
        return PipelineRunResult(
            run_id=run_id, status="succeeded", layer_reached="raw", checksum=raw_result.checksum
        )

    # --- stage (silver): DQ validation, then write ---
    stage_target = f"{spec.stage.schema_}.{spec.stage.table}"
    handle = monitor.begin_step(run_id, spec.pipeline_name, "stage", stage_target)
    emitter.start("stage", run_id, inputs=[raw_target], outputs=[stage_target])

    validation = validate_dataframe(
        parsed_df, spec.quality.rules, connector, spec.stage.business_key, spec.quality.quarantine.group_by
    )
    outcome = enforce_integrity_mode(validation, spec.quality)

    quarantine_location = None
    dq_failure_reasons: list[str] = []
    if outcome.quarantined_rows:
        raise_alert(outcome, spec.quality, file_name)
        quarantine_location = write_quarantine(outcome.quarantined_rows, spec.quality, file_name, s3)
        dq_failure_reasons = sorted({reason for row in outcome.quarantined_rows for reason in row.reasons})

    if outcome.file_quarantined:
        monitor.quarantine_step(handle, row_count_in=parsed_df.height, row_count_out=0)
        emitter.fail("stage", run_id)
        return PipelineRunResult(
            run_id=run_id,
            status="quarantined",
            layer_reached="raw",
            checksum=raw_result.checksum,
            quarantine_location=quarantine_location,
            error=f"{len(outcome.quarantined_rows)} row(s) failed strict DQ",
            failure_reasons=dq_failure_reasons,
        )

    stage_result = land_stage(outcome.promoted_df, spec, connector)
    monitor.complete_step(handle, row_count_in=parsed_df.height, row_count_out=stage_result.row_count_out)
    emitter.complete("stage", run_id, outputs=[stage_target])

    # --- anomaly detection (Phase 8b, additive/optional) - a no-op for
    # any pipeline that doesn't declare anomaly_detection in its spec ---
    if spec.anomaly_detection is not None and spec.anomaly_detection.enabled:
        profile = compute_profile(outcome.promoted_df, spec.anomaly_detection.metrics)
        trailing_profiles = get_trailing_profiles(
            connector, spec.pipeline_name, spec.anomaly_detection.window, exclude_process_id=run_id
        )
        anomalies = [
            a for a in detect_anomalies(profile, trailing_profiles, spec.anomaly_detection) if a.is_anomaly
        ]
        write_profile_history(
            connector,
            process_id=run_id,
            pipeline_name=spec.pipeline_name,
            run_date=datetime.now(timezone.utc).date(),
            profile=profile,
        )

        if anomalies:
            explanations: dict[str, str] = {}
            try:
                import anthropic

                from dais.ai.anomaly_explainer import explain_anomalies

                explanations = explain_anomalies(anthropic.Anthropic(), anomalies, pipeline_name=spec.pipeline_name)
            except Exception:
                pass  # the explanation is best-effort - never block an alert/quarantine on it

            alerter = get_alerter(spec.quality.quarantine.alert.channel)
            for a in anomalies:
                message = (
                    f"{spec.pipeline_name}: anomaly detected in {a.metric} (current={a.current_value}, "
                    f"baseline_mean={a.baseline_mean:.4f}, {a.method} score={a.score:.3f}, "
                    f"threshold={a.threshold})"
                )
                if a.metric in explanations:
                    message += f" - {explanations[a.metric]}"
                alerter.send(
                    spec.quality.quarantine.alert.destination,
                    message,
                    {"pipeline_name": spec.pipeline_name, "metric": a.metric, "score": a.score},
                )

            if spec.anomaly_detection.on_anomaly == "quarantine":
                monitor.quarantine_step(handle, row_count_in=parsed_df.height, row_count_out=stage_result.row_count_out)
                emitter.fail("stage", run_id)
                return PipelineRunResult(
                    run_id=run_id,
                    status="quarantined",
                    layer_reached="stage",
                    checksum=raw_result.checksum,
                    quarantine_location=quarantine_location,
                    error=f"{len(anomalies)} anomaly(ies) detected: {', '.join(a.metric for a in anomalies)}",
                    failure_reasons=[
                        f"{a.metric}: {a.method} score {a.score:.3f} exceeds threshold {a.threshold}"
                        for a in anomalies
                    ],
                )

    if "gold" not in layers:
        return PipelineRunResult(
            run_id=run_id,
            status="succeeded",
            layer_reached="stage",
            checksum=raw_result.checksum,
            quarantine_location=quarantine_location,
            failure_reasons=dq_failure_reasons,
        )

    # --- gold: dbt handoff ---
    if connection_params is None:
        raise ValueError("connection_params is required to run the gold layer (dbt needs its own connection)")

    gold_target = f"{spec.gold.schema_}.{spec.gold.dbt_select}"
    handle = monitor.begin_step(run_id, spec.pipeline_name, "gold", gold_target)
    emitter.start("gold", run_id, inputs=[stage_target], outputs=[gold_target])
    gold_result = run_gold(spec, file_name=file_name, **connection_params)

    if not gold_result.success:
        monitor.fail_step(handle, row_count_in=stage_result.row_count_out, row_count_out=0)
        emitter.fail("gold", run_id)
        return PipelineRunResult(
            run_id=run_id,
            status="failed",
            layer_reached="stage",
            checksum=raw_result.checksum,
            error=gold_result.stderr or gold_result.stdout,
        )

    monitor.complete_step(handle, row_count_in=stage_result.row_count_out, row_count_out=stage_result.row_count_out)
    emitter.complete("gold", run_id, outputs=[gold_target])

    return PipelineRunResult(
        run_id=run_id,
        status="succeeded",
        layer_reached="gold",
        checksum=raw_result.checksum,
        quarantine_location=quarantine_location,
        failure_reasons=dq_failure_reasons,
    )
