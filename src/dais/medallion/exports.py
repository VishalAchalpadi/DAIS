"""Optional Parquet-into-Iceberg export, usable on any layer's
`exports` block independent of `execution.stop_after` - a stage-only run
can still export to Iceberg for analytics.

Catalog construction is the caller's concern (LEGO-block design): a
local `SqlCatalog` for dev/test, a REST/Glue catalog in production -
this module only knows how to write a DataFrame into a namespace.table
given any `pyiceberg.catalog.Catalog`.
"""
from __future__ import annotations

from dataclasses import dataclass

import polars as pl
from pyiceberg.catalog import Catalog

from dais.spec.models import ExportConfig


@dataclass
class ExportResult:
    table_identifier: str
    row_count: int
    created_table: bool


def _split_table_identifier(table: str) -> tuple[str, str]:
    if "." not in table:
        raise ValueError(f"export table must be `namespace.table`, got {table!r}")
    namespace, table_name = table.rsplit(".", 1)
    return namespace, table_name


def write_iceberg_export(df: pl.DataFrame, export_cfg: ExportConfig, catalog: Catalog) -> ExportResult:
    if not export_cfg.enabled:
        raise ValueError("write_iceberg_export called with an export that isn't enabled")
    if not export_cfg.table:
        raise ValueError("export.table is required to write an Iceberg export")

    namespace, _ = _split_table_identifier(export_cfg.table)
    if (namespace,) not in catalog.list_namespaces():
        catalog.create_namespace(namespace)

    arrow_table = df.to_arrow()
    created_table = not catalog.table_exists(export_cfg.table)
    if created_table:
        iceberg_table = catalog.create_table(export_cfg.table, schema=arrow_table.schema)
    else:
        iceberg_table = catalog.load_table(export_cfg.table)

    iceberg_table.append(arrow_table)

    return ExportResult(
        table_identifier=export_cfg.table, row_count=df.height, created_table=created_table
    )
