"""Enforces `quality.integrity_mode` on top of row-level validation
results, writes quarantined rows to the configured location, and raises
the configured DQ alert.

strict: any row failure quarantines the WHOLE file - nothing is promoted.
row_level: only the failing rows are quarantined; the rest continues on
to stage.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone

import polars as pl

from dais.quality.dq_alerts import get_alerter
from dais.quality.row_validator import QuarantinedRow, ValidationResult
from dais.resilience.connectors.s3_connector import S3Connector, parse_s3_uri
from dais.spec.models import QualityConfig


@dataclass
class FileValidationOutcome:
    promoted_df: pl.DataFrame | None  # None when the whole file was quarantined
    file_quarantined: bool
    quarantined_rows: list[QuarantinedRow] = field(default_factory=list)


def enforce_integrity_mode(result: ValidationResult, quality_cfg: QualityConfig) -> FileValidationOutcome:
    if quality_cfg.integrity_mode == "strict":
        if result.quarantined_rows:
            return FileValidationOutcome(
                promoted_df=None, file_quarantined=True, quarantined_rows=result.quarantined_rows
            )
        return FileValidationOutcome(promoted_df=result.valid_df, file_quarantined=False, quarantined_rows=[])

    if quality_cfg.integrity_mode == "row_level":
        return FileValidationOutcome(
            promoted_df=result.valid_df,
            file_quarantined=False,
            quarantined_rows=result.quarantined_rows,
        )

    raise ValueError(f"unsupported integrity_mode: {quality_cfg.integrity_mode!r}")


def _quarantine_payload(rows: list[QuarantinedRow]) -> bytes:
    now = datetime.now(timezone.utc).isoformat()
    payload = [
        {
            "row_index": r.row_index,
            "row_data": r.row_data,
            "reasons": r.reasons,
            "quarantined_at": now,
        }
        for r in rows
    ]
    return json.dumps(payload, default=str, indent=2).encode("utf-8")


def write_quarantine(
    rows: list[QuarantinedRow], quality_cfg: QualityConfig, file_name: str, s3: S3Connector
) -> str:
    bucket, prefix = parse_s3_uri(quality_cfg.quarantine.location)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    key = f"{prefix.rstrip('/')}/{file_name}.{timestamp}.quarantine.json"
    s3.put_object(bucket, key, _quarantine_payload(rows))
    return f"s3://{bucket}/{key}"


def raise_alert(outcome: FileValidationOutcome, quality_cfg: QualityConfig, file_name: str) -> None:
    if not outcome.quarantined_rows:
        return
    alerter = get_alerter(quality_cfg.quarantine.alert.channel)
    if outcome.file_quarantined:
        message = f"{file_name}: entire file quarantined (strict integrity_mode) - {len(outcome.quarantined_rows)} row(s) failed DQ"
        alerter.send(
            quality_cfg.quarantine.alert.destination,
            message,
            {"file_name": file_name, "row_count": len(outcome.quarantined_rows)},
        )
    else:
        for row in outcome.quarantined_rows:
            message = f"{file_name}: row {row.row_index} quarantined - {'; '.join(row.reasons)}"
            alerter.send(
                quality_cfg.quarantine.alert.destination,
                message,
                {"file_name": file_name, "row_index": row.row_index, "reasons": row.reasons},
            )
