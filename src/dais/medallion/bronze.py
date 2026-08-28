"""Raw (bronze) landing: append-only, immutable, checksum-tagged.

Lands every parsed field as TEXT exactly as parsed, plus file metadata.
The table is created once (create_if_not_exists) and never dropped or
altered. Re-landing a file with a checksum already present is a no-op.
"""
from __future__ import annotations

import hashlib
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

import polars as pl

from dais.resilience.connectors.base import ColumnDef, DatabaseConnector
from dais.spec.models import PipelineSpec

_METADATA_SQL_TYPES = {
    "file_name": "TEXT",
    "file_path": "TEXT",
    "file_checksum": "TEXT",
    "load_timestamp": "TIMESTAMP",
    "batch_id": "TEXT",
}


@dataclass
class RawLoadResult:
    loaded: bool  # False when skipped by checksum dedup
    row_count_in: int
    row_count_out: int
    checksum: str
    batch_id: str


def compute_checksum(raw_bytes: bytes) -> str:
    return hashlib.sha256(raw_bytes).hexdigest()


def _raw_table_columns(df: pl.DataFrame, preserve_metadata: list[str]) -> list[ColumnDef]:
    columns = [ColumnDef(name=c, sql_type="TEXT") for c in df.columns]
    for meta_col in preserve_metadata:
        if meta_col not in _METADATA_SQL_TYPES:
            raise ValueError(f"unknown raw.preserve_metadata column: {meta_col!r}")
        columns.append(ColumnDef(name=meta_col, sql_type=_METADATA_SQL_TYPES[meta_col]))
    return columns


def land_raw(
    df: pl.DataFrame,
    spec: PipelineSpec,
    connector: DatabaseConnector,
    *,
    file_name: str,
    file_path: str,
    file_bytes: bytes,
    batch_id: str | None = None,
) -> RawLoadResult:
    raw_cfg = spec.raw
    checksum = compute_checksum(file_bytes)
    batch_id = batch_id or str(uuid.uuid4())

    columns = _raw_table_columns(df, raw_cfg.preserve_metadata)
    connector.create_table_if_not_exists(raw_cfg.schema_, raw_cfg.table, columns)

    if raw_cfg.checksum_dedup and "file_checksum" in raw_cfg.preserve_metadata:
        already_loaded = connector.value_exists(
            raw_cfg.schema_, raw_cfg.table, "file_checksum", checksum
        )
        if already_loaded:
            return RawLoadResult(
                loaded=False,
                row_count_in=df.height,
                row_count_out=0,
                checksum=checksum,
                batch_id=batch_id,
            )

    metadata_values = {
        "file_name": file_name,
        "file_path": file_path,
        "file_checksum": checksum,
        "load_timestamp": datetime.now(timezone.utc),
        "batch_id": batch_id,
    }
    column_names = [c.name for c in columns]
    data_column_names = df.columns
    rows = [
        tuple(row[i] for i in range(len(data_column_names)))
        + tuple(metadata_values[m] for m in raw_cfg.preserve_metadata)
        for row in df.iter_rows()
    ]

    row_count_out = connector.bulk_insert(raw_cfg.schema_, raw_cfg.table, column_names, rows)

    return RawLoadResult(
        loaded=True,
        row_count_in=df.height,
        row_count_out=row_count_out,
        checksum=checksum,
        batch_id=batch_id,
    )
