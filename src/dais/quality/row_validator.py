"""Row-level DQ validation: runs the pandera schema built from
`quality.rules`, splits the batch into passing rows (cast to their
`cast_to` types, ready for stage) and quarantined rows (original text +
the reasons they failed), one DQ alert per quarantined row.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pandera.errors
import polars as pl

from dais.quality.schema_registry import build_dataframe_schema
from dais.resilience.connectors.base import DatabaseConnector
from dais.spec.models import QualityRule


@dataclass
class QuarantinedRow:
    row_index: int
    row_data: dict[str, Any]
    reasons: list[str]


@dataclass
class ValidationResult:
    valid_df: pl.DataFrame
    quarantined_rows: list[QuarantinedRow] = field(default_factory=list)


def _failing_indices(df: pl.DataFrame, rules: list[QualityRule], connector: DatabaseConnector | None) -> dict[int, list[str]]:
    schema = build_dataframe_schema(rules, connector)
    try:
        schema.validate(df, lazy=True)
        return {}
    except pandera.errors.SchemaErrors as exc:
        failures: dict[int, list[str]] = {}
        for rec in exc.failure_cases.to_dicts():
            idx = rec["index"]
            if idx is None:
                continue
            reason = f"{rec['column']}: {rec['check']}"
            failures.setdefault(idx, []).append(reason)
        return failures


def _apply_casts(df: pl.DataFrame, rules: list[QualityRule]) -> pl.DataFrame:
    exprs = []
    for rule in rules:
        if rule.column not in df.columns:
            continue
        if rule.cast_to == "date":
            exprs.append(pl.col(rule.column).str.strptime(pl.Date, rule.format))
        elif rule.cast_to == "decimal":
            precision = rule.precision or 38
            scale = rule.scale or 0
            exprs.append(pl.col(rule.column).cast(pl.Decimal(precision, scale)))
    return df.with_columns(exprs) if exprs else df


def validate_dataframe(
    df: pl.DataFrame, rules: list[QualityRule], connector: DatabaseConnector | None = None
) -> ValidationResult:
    if df.height == 0:
        return ValidationResult(valid_df=_apply_casts(df, rules), quarantined_rows=[])

    failures = _failing_indices(df, rules, connector)

    if not failures:
        return ValidationResult(valid_df=_apply_casts(df, rules), quarantined_rows=[])

    valid_mask = pl.Series([i not in failures for i in range(df.height)])
    valid_df = _apply_casts(df.filter(valid_mask), rules)

    quarantined_rows = [
        QuarantinedRow(row_index=idx, row_data=df.row(idx, named=True), reasons=reasons)
        for idx, reasons in sorted(failures.items())
    ]

    return ValidationResult(valid_df=valid_df, quarantined_rows=quarantined_rows)
