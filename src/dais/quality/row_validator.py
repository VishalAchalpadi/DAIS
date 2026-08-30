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


def _duplicate_business_key_indices(df: pl.DataFrame, business_key: list[str]) -> dict[int, list[str]]:
    """Flags every occurrence of a business key after the first as a DQ
    failure, so duplicate rows are quarantined rather than silently
    dropped or (in upsert mode) crashing the stage write with a
    'ON CONFLICT DO UPDATE command cannot affect row a second time'
    Postgres error."""
    if not all(col in df.columns for col in business_key):
        return {}

    seen: set[tuple] = set()
    failures: dict[int, list[str]] = {}
    for idx, row in enumerate(df.select(business_key).iter_rows()):
        if row in seen:
            key_desc = ", ".join(f"{c}={v!r}" for c, v in zip(business_key, row))
            failures[idx] = [f"duplicate business key: ({key_desc})"]
        else:
            seen.add(row)
    return failures


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


def _cascade_group_failures(df: pl.DataFrame, failures: dict[int, list[str]], group_by: list[str]) -> dict[int, list[str]]:
    """integrity_mode: group_level - once ANY row in a group (e.g. a
    portfolio's account_id) has failed, every other row in that same
    group is quarantined too, even ones that individually passed. A
    partial, silently-incomplete group in stage (e.g. 9 of a portfolio's
    10 holdings) is worse than quarantining the whole group."""
    if not failures or not all(col in df.columns for col in group_by):
        return failures

    key_columns = df.select(group_by).rows()
    failing_keys = {key_columns[idx] for idx in failures}

    cascaded = dict(failures)
    for idx, key in enumerate(key_columns):
        if key in failing_keys and idx not in cascaded:
            key_desc = ", ".join(f"{c}={v!r}" for c, v in zip(group_by, key))
            cascaded[idx] = [f"quarantined with group ({key_desc}) - another row in this group failed DQ"]
    return cascaded


def validate_dataframe(
    df: pl.DataFrame,
    rules: list[QualityRule],
    connector: DatabaseConnector | None = None,
    business_key: list[str] | None = None,
    group_by: list[str] | None = None,
) -> ValidationResult:
    if df.height == 0:
        return ValidationResult(valid_df=_apply_casts(df, rules), quarantined_rows=[])

    failures = _failing_indices(df, rules, connector)

    if business_key:
        for idx, reasons in _duplicate_business_key_indices(df, business_key).items():
            failures.setdefault(idx, []).extend(reasons)

    if group_by:
        failures = _cascade_group_failures(df, failures, group_by)

    if not failures:
        return ValidationResult(valid_df=_apply_casts(df, rules), quarantined_rows=[])

    valid_mask = pl.Series([i not in failures for i in range(df.height)])
    valid_df = _apply_casts(df.filter(valid_mask), rules)

    quarantined_rows = [
        QuarantinedRow(row_index=idx, row_data=df.row(idx, named=True), reasons=reasons)
        for idx, reasons in sorted(failures.items())
    ]

    return ValidationResult(valid_df=valid_df, quarantined_rows=quarantined_rows)
