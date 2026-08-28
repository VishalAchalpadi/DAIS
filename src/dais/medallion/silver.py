"""Stage (silver) landing: typed, conformed data, written per the spec's
`write_mode` (truncate_load | append | upsert).

The DQ engine that validates and casts raw text into typed columns is a
Phase 3 concern (quality/row_validator.py, file_validator.py). This module
takes an already-typed DataFrame and owns only the DDL + write mechanics.
"""
from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from dais.resilience.connectors.base import ColumnDef, DatabaseConnector
from dais.spec.models import PipelineSpec

_POLARS_TO_POSTGRES: dict[type, str] = {
    pl.Utf8: "TEXT",
    pl.Boolean: "BOOLEAN",
    pl.Int8: "SMALLINT",
    pl.Int16: "SMALLINT",
    pl.Int32: "INTEGER",
    pl.Int64: "BIGINT",
    pl.UInt8: "SMALLINT",
    pl.UInt16: "INTEGER",
    pl.UInt32: "BIGINT",
    pl.UInt64: "NUMERIC",
    pl.Float32: "REAL",
    pl.Float64: "DOUBLE PRECISION",
    pl.Date: "DATE",
    pl.Datetime: "TIMESTAMP",
}


@dataclass
class StageLoadResult:
    row_count_out: int
    write_mode: str


def _sql_type_for(dtype: pl.DataType) -> str:
    if isinstance(dtype, pl.Decimal):
        precision = dtype.precision or 38
        scale = dtype.scale or 0
        return f"NUMERIC({precision},{scale})"
    return _POLARS_TO_POSTGRES.get(type(dtype), "TEXT")


def _stage_table_columns(df: pl.DataFrame) -> list[ColumnDef]:
    return [ColumnDef(name=name, sql_type=_sql_type_for(dtype)) for name, dtype in df.schema.items()]


def land_stage(df: pl.DataFrame, spec: PipelineSpec, connector: DatabaseConnector) -> StageLoadResult:
    stage_cfg = spec.stage
    write_mode = stage_cfg.write_mode

    columns = _stage_table_columns(df)
    unique_columns = stage_cfg.business_key if write_mode == "upsert" else None
    connector.create_table_if_not_exists(
        stage_cfg.schema_, stage_cfg.table, columns, unique_columns=unique_columns
    )

    column_names = df.columns
    rows = list(df.iter_rows())

    if write_mode == "truncate_load":
        connector.truncate(stage_cfg.schema_, stage_cfg.table)
        row_count_out = connector.bulk_insert(stage_cfg.schema_, stage_cfg.table, column_names, rows)
    elif write_mode == "append":
        row_count_out = connector.bulk_insert(stage_cfg.schema_, stage_cfg.table, column_names, rows)
    elif write_mode == "upsert":
        row_count_out = connector.upsert(
            stage_cfg.schema_,
            stage_cfg.table,
            column_names,
            rows,
            conflict_columns=stage_cfg.business_key,
        )
    else:
        raise ValueError(f"unsupported write_mode: {write_mode!r}")

    return StageLoadResult(row_count_out=row_count_out, write_mode=write_mode)
