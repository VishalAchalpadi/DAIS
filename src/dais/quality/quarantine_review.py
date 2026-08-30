"""Data-ops workflow for quarantined rows: list what's pending, read one
record's full detail, and resubmit corrected values.

A resubmission always re-runs the same `quality.rules` validation a
normal ingest would (so a bad fix fails again rather than silently
landing), then upserts straight into `stage.table` keyed on
`stage.business_key` - deliberately NOT `land_stage`/`write_mode`,
since a correction is a targeted single-key fix regardless of whether
the pipeline's bulk loads are configured as append/truncate_load/upsert.
Resolved records are moved into a `resolved/` subdirectory (local) or
key prefix (S3) rather than deleted, keeping the audit trail intact.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import quote, unquote

import polars as pl

from dais.quality.row_validator import validate_dataframe
from dais.resilience.connectors.base import DatabaseConnector
from dais.resilience.connectors.s3_connector import S3Connector, parse_s3_uri
from dais.spec.models import PipelineSpec


@dataclass
class QuarantineRecordSummary:
    quarantine_id: str
    file_name: str
    row_count: int


@dataclass
class ResubmitOutcome:
    accepted: bool
    row_count_upserted: int
    failures: list[dict] = field(default_factory=list)  # [{row_index, reasons}]


def _local_dir(spec: PipelineSpec) -> Path:
    return Path(spec.quality.quarantine.location)


def _require_s3(s3: S3Connector | None) -> S3Connector:
    if s3 is None:
        raise ValueError("quality.quarantine.kind is 's3' but no S3Connector was provided")
    return s3


def list_quarantine_records(spec: PipelineSpec, s3: S3Connector | None = None) -> list[QuarantineRecordSummary]:
    quarantine_cfg = spec.quality.quarantine
    if quarantine_cfg.kind == "local":
        directory = _local_dir(spec)
        if not directory.is_dir():
            return []
        records = []
        for path in sorted(directory.glob("*.quarantine.json")):
            payload = json.loads(path.read_text(encoding="utf-8"))
            records.append(
                QuarantineRecordSummary(quarantine_id=quote(path.name, safe=""), file_name=path.name, row_count=len(payload))
            )
        return records

    s3 = _require_s3(s3)
    bucket, prefix = parse_s3_uri(quarantine_cfg.location)
    records = []
    for key in s3.list_objects(bucket, prefix):
        if not key.endswith(".quarantine.json"):
            continue
        payload = json.loads(s3.get_object(bucket, key))
        records.append(
            QuarantineRecordSummary(quarantine_id=quote(key, safe=""), file_name=key.rsplit("/", 1)[-1], row_count=len(payload))
        )
    return records


def read_quarantine_record(spec: PipelineSpec, quarantine_id: str, s3: S3Connector | None = None) -> list[dict]:
    quarantine_cfg = spec.quality.quarantine
    if quarantine_cfg.kind == "local":
        path = _local_dir(spec) / unquote(quarantine_id)
        return json.loads(path.read_text(encoding="utf-8"))

    s3 = _require_s3(s3)
    bucket, _ = parse_s3_uri(quarantine_cfg.location)
    return json.loads(s3.get_object(bucket, unquote(quarantine_id)))


def _mark_resolved(spec: PipelineSpec, quarantine_id: str, s3: S3Connector | None) -> None:
    quarantine_cfg = spec.quality.quarantine
    if quarantine_cfg.kind == "local":
        src = _local_dir(spec) / unquote(quarantine_id)
        resolved_dir = _local_dir(spec) / "resolved"
        resolved_dir.mkdir(parents=True, exist_ok=True)
        src.replace(resolved_dir / src.name)
        return

    s3 = _require_s3(s3)
    bucket, prefix = parse_s3_uri(quarantine_cfg.location)
    key = unquote(quarantine_id)
    body = s3.get_object(bucket, key)
    resolved_key = f"{prefix.rstrip('/')}/resolved/{key.rsplit('/', 1)[-1]}"
    s3.put_object(bucket, resolved_key, body)
    s3.delete_object(bucket, key)


def resubmit_corrections(
    spec: PipelineSpec,
    quarantine_id: str,
    corrected_rows: list[dict],
    connector: DatabaseConnector,
    s3: S3Connector | None = None,
) -> ResubmitOutcome:
    """corrected_rows: [{"row_index": <original int>, "row_data": {col: value, ...}}].
    All-or-nothing - if any corrected row still fails quality.rules,
    nothing is written and every failure is reported back."""
    if not corrected_rows:
        return ResubmitOutcome(accepted=False, row_count_upserted=0, failures=[{"error": "no rows submitted"}])

    columns = list(corrected_rows[0]["row_data"].keys())
    df = pl.DataFrame(
        [row["row_data"] for row in corrected_rows], schema={c: pl.Utf8 for c in columns}
    )

    validation = validate_dataframe(df, spec.quality.rules, connector, spec.stage.business_key)
    if validation.quarantined_rows:
        failures = [
            {"row_index": corrected_rows[q.row_index]["row_index"], "reasons": q.reasons}
            for q in validation.quarantined_rows
        ]
        return ResubmitOutcome(accepted=False, row_count_upserted=0, failures=failures)

    column_names = validation.valid_df.columns
    rows = list(validation.valid_df.iter_rows())
    row_count_out = connector.upsert(
        spec.stage.schema_, spec.stage.table, column_names, rows, conflict_columns=spec.stage.business_key
    )

    _mark_resolved(spec, quarantine_id, s3)
    return ResubmitOutcome(accepted=True, row_count_upserted=row_count_out, failures=[])
