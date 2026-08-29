"""Builds a pandera schema from a spec's `quality.rules` - this IS the
expected schema (columns, nullability, business rules) pandera enforces
on the raw-text DataFrame during the raw -> stage transition.

Rules operate on Utf8 (text) columns, since that's how everything lands
in raw - no casting has happened yet. `cast_to` on a rule drives the
*output* type for stage, applied after validation passes (row_validator.py).
"""
from __future__ import annotations

import re
from typing import Any, Callable

import pandera.polars as pa
import polars as pl

from dais.resilience.connectors.base import DatabaseConnector
from dais.spec.models import QualityRule


def _non_empty_check() -> pa.Check:
    def _fn(data):
        col = pl.col(data.key)
        return data.lazyframe.select(col.is_null() | (col.str.len_chars() > 0))

    return pa.Check(_fn, element_wise=False, name="non_empty")


def _valid_date_check(fmt: str) -> pa.Check:
    def _fn(data):
        col = pl.col(data.key)
        parsed = col.str.strptime(pl.Date, fmt, strict=False)
        return data.lazyframe.select(col.is_null() | parsed.is_not_null())

    return pa.Check(_fn, element_wise=False, name="valid_date")


def _is_numeric_check() -> pa.Check:
    def _fn(data):
        col = pl.col(data.key)
        parsed = col.cast(pl.Float64, strict=False)
        return data.lazyframe.select(col.is_null() | parsed.is_not_null())

    return pa.Check(_fn, element_wise=False, name="is_numeric")


def _comparison_check(op: str, threshold: float) -> pa.Check:
    ops: dict[str, Callable[[pl.Expr, float], pl.Expr]] = {
        "greater_than_or_equal": lambda e, v: e >= v,
        "greater_than": lambda e, v: e > v,
        "less_than_or_equal": lambda e, v: e <= v,
        "less_than": lambda e, v: e < v,
    }
    if op not in ops:
        raise ValueError(f"unsupported comparison check: {op!r}")

    def _fn(data):
        col = pl.col(data.key)
        parsed = col.cast(pl.Float64, strict=False)
        condition = ops[op](parsed, threshold)
        return data.lazyframe.select(col.is_null() | (parsed.is_not_null() & condition))

    return pa.Check(_fn, element_wise=False, name=op)


# Specs write lookup queries with SQLAlchemy-style `:value` named
# placeholders (see holdings_ingest.yaml); psycopg2 needs `%(value)s`.
_NAMED_PARAM_RE = re.compile(r":(\w+)")


def _to_psycopg2_params(query: str) -> str:
    return _NAMED_PARAM_RE.sub(r"%(\1)s", query)


def _sql_lookup_check(query: str, connector: DatabaseConnector) -> pa.Check:
    psycopg2_query = _to_psycopg2_params(query)

    def _fn(data):
        col_name = data.key
        distinct_values = (
            data.lazyframe.select(pl.col(col_name)).unique().drop_nulls().collect()[col_name].to_list()
        )
        valid_values = {
            v for v in distinct_values if connector.fetch_one(psycopg2_query, {"value": v}) is not None
        }
        col = pl.col(col_name)
        return data.lazyframe.select(col.is_null() | col.is_in(list(valid_values)))

    return pa.Check(_fn, element_wise=False, name="sql_lookup")


def _build_checks(rule: QualityRule, connector: DatabaseConnector | None) -> list[pa.Check]:
    checks: list[pa.Check] = []
    for item in rule.checks:
        if isinstance(item, str):
            if item == "not_null":
                continue  # handled via Column(nullable=...)
            if item == "non_empty":
                checks.append(_non_empty_check())
            elif item == "valid_date":
                if not rule.format:
                    raise ValueError(f"rule for column {rule.column!r} needs `format` for valid_date")
                checks.append(_valid_date_check(rule.format))
            elif item == "is_numeric":
                checks.append(_is_numeric_check())
            else:
                raise ValueError(f"unknown check {item!r} on column {rule.column!r}")
        elif isinstance(item, dict):
            for op, threshold in item.items():
                checks.append(_comparison_check(op, float(threshold)))
        else:
            raise ValueError(f"unsupported check item: {item!r}")

    if rule.lookup is not None:
        if connector is None:
            raise ValueError(
                f"column {rule.column!r} has a SQL lookup rule but no connector was provided"
            )
        checks.append(_sql_lookup_check(rule.lookup.query, connector))

    return checks


def build_dataframe_schema(
    rules: list[QualityRule], connector: DatabaseConnector | None = None
) -> pa.DataFrameSchema:
    columns: dict[str, pa.Column] = {}
    for rule in rules:
        checks = _build_checks(rule, connector)
        nullable = not any(isinstance(c, str) and c == "not_null" for c in rule.checks)
        columns[rule.column] = pa.Column(pl.Utf8, checks=checks, nullable=nullable, coerce=False)
    return pa.DataFrameSchema(columns)
