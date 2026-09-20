"""Builds Great Expectations expectations from a spec's `quality.rules` -
these ARE the expected schema's business rules, run against the raw-text
DataFrame during the raw -> stage transition (Phase 11a: replaces pandera).

Rules operate on Utf8 (text) columns, since that's how everything lands
in raw - no casting has happened yet. `cast_to` on a rule drives the
*output* type for stage, applied after validation passes (row_validator.py).

Design notes (verified against the installed great_expectations==1.23.0,
GX Core's current API - see git history for the exploration that produced
these findings, not assumed from an older/different GX version):

- GX Core 1.x has no native Polars execution engine (`ctx.data_sources`
  exposes `add_pandas`/`add_spark`/various SQL backends, never
  `add_polars`). Validation runs against a pandas conversion of the raw
  DataFrame; row positions survive the polars -> pandas conversion
  unchanged (same 0..n-1 order), so GX's reported row indices map back to
  the original polars DataFrame directly via `.row(idx, named=True)`,
  exactly as the pandera version did.
- `batch.validate(expectation_or_suite, result_format=...)` defaults to
  `ResultFormat.SUMMARY`, which only returns a best-effort PARTIAL sample
  of failing rows (capped at 20). The full, exact list of every failing
  row's position - required here, since every failure must be quarantined,
  not just the first 20 - only appears under `result_format={"result_format":
  "COMPLETE"}` passed to `.validate()` itself (passing it via the
  expectation's own constructor is insufficient - verified empirically).
- GX's standard column-map-expectation machinery excludes null values from
  the underlying condition automatically (a null is neither "expected" nor
  "unexpected" for e.g. a length check) - this matches pandera's explicit
  `col.is_null() | <check>` pattern in the pre-swap version exactly, with
  no special-casing needed here.
- `not_null`/`non_empty`/`valid_date`/`is_numeric`/the comparison ops have
  no single GX built-in that exactly reproduces the prior pandera/polars
  semantics (e.g. GX has no "is this string numeric" expectation, and
  regex-approximating one risks silent behavioral drift from Polars'
  actual `cast(Float64, strict=False)` parsing). Each is implemented as a
  small custom GX Expectation whose condition function calls back into
  the SAME polars expression the pre-swap pandera check used - not a
  reimplementation, so there is no drift risk.
- SQL reference-data lookups (e.g. currency/country code checks) are NOT
  reimplemented via GX's native SQL-backed validation (deliberately, see
  requirement 5): a GX Batch is bound to one execution engine/Data Source,
  and the values under test live in the in-memory file being ingested
  (a pandas Batch), not as rows in a queryable SQL table - only the
  *reference* list is in SQL. GX's SQL-datasource validation validates a
  table that already lives in the database; it has no clean way to check
  an in-memory column's values against a separate SQL reference table in
  one Batch. The existing per-distinct-value `connector.fetch_one()`
  mechanism is kept unchanged and runs as a separate pass alongside GX's
  results (merged in row_validator.py).
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Callable

import pandas as pd
import polars as pl
from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.expectations import ExpectColumnValuesToNotBeNull
from great_expectations.expectations.expectation import ColumnMapExpectation, Expectation
from great_expectations.expectations.metrics.map_metric_provider import (
    ColumnMapMetricProvider,
    column_condition_partial,
)

from dais.resilience.connectors.base import DatabaseConnector
from dais.spec.models import QualityRule

# ---------------------------------------------------------------------------
# Custom column-map metrics/expectations. Registered once, at import time,
# in GX's process-global metric/expectation registries.
# ---------------------------------------------------------------------------


class _DaisNonEmptyMetric(ColumnMapMetricProvider):
    condition_metric_name = "column_values.dais_non_empty"

    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        lengths = pl.Series(column.astype(str)).str.len_chars()
        return pd.Series((lengths > 0).to_list(), index=column.index)


class ExpectColumnValuesToBeDaisNonEmpty(ColumnMapExpectation):
    map_metric = "column_values.dais_non_empty"
    success_keys = ("mostly",)
    args_keys = ("column",)


class _DaisValidDateMetric(ColumnMapMetricProvider):
    condition_metric_name = "column_values.dais_valid_date"
    condition_value_keys = ("date_format",)

    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, date_format, **kwargs):
        parsed = pl.Series(column.astype(str)).str.strptime(pl.Date, date_format, strict=False)
        return pd.Series(parsed.is_not_null().to_list(), index=column.index)


class ExpectColumnValuesToBeDaisValidDate(ColumnMapExpectation):
    map_metric = "column_values.dais_valid_date"
    success_keys = ("mostly", "date_format")
    args_keys = ("column", "date_format")
    date_format: str


class _DaisIsNumericMetric(ColumnMapMetricProvider):
    condition_metric_name = "column_values.dais_is_numeric"

    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        parsed = pl.Series(column.astype(str)).cast(pl.Float64, strict=False)
        return pd.Series(parsed.is_not_null().to_list(), index=column.index)


class ExpectColumnValuesToBeDaisNumeric(ColumnMapExpectation):
    map_metric = "column_values.dais_is_numeric"
    success_keys = ("mostly",)
    args_keys = ("column",)


_COMPARISON_OPS: dict[str, Callable[[pl.Expr, float], pl.Expr]] = {
    "greater_than_or_equal": lambda e, v: e >= v,
    "greater_than": lambda e, v: e > v,
    "less_than_or_equal": lambda e, v: e <= v,
    "less_than": lambda e, v: e < v,
}


class _DaisComparisonMetric(ColumnMapMetricProvider):
    condition_metric_name = "column_values.dais_comparison"
    condition_value_keys = ("op", "threshold")

    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, op, threshold, **kwargs):
        if op not in _COMPARISON_OPS:
            raise ValueError(f"unsupported comparison check: {op!r}")
        parsed = pl.Series(column.astype(str)).cast(pl.Float64, strict=False)
        condition = _COMPARISON_OPS[op](parsed, threshold)
        passed = (parsed.is_not_null() & condition).fill_null(False)
        return pd.Series(passed.to_list(), index=column.index)


class ExpectColumnValuesToBeDaisComparison(ColumnMapExpectation):
    map_metric = "column_values.dais_comparison"
    success_keys = ("mostly", "op", "threshold")
    args_keys = ("column", "op", "threshold")
    op: str
    threshold: float


# ---------------------------------------------------------------------------
# SQL lookup - kept as a standalone, non-GX check (see module docstring).
# Unchanged from the pre-GX implementation other than no longer being a
# pandera Check.
# ---------------------------------------------------------------------------

# Specs write lookup queries with SQLAlchemy-style `:value` named
# placeholders (see holdings_ingest.yaml); psycopg2 needs `%(value)s`.
_NAMED_PARAM_RE = re.compile(r":(\w+)")


def _to_psycopg2_params(query: str) -> str:
    return _NAMED_PARAM_RE.sub(r"%(\1)s", query)


@dataclass
class SqlLookupCheck:
    column: str
    query: str

    def failing_indices(self, df: pl.DataFrame, connector: DatabaseConnector) -> set[int]:
        psycopg2_query = _to_psycopg2_params(self.query)
        distinct_values = df.select(pl.col(self.column)).unique().drop_nulls()[self.column].to_list()
        valid_values = {
            v for v in distinct_values if connector.fetch_one(psycopg2_query, {"value": v}) is not None
        }
        col = df[self.column]
        return {
            idx
            for idx, value in enumerate(col)
            if value is not None and value not in valid_values
        }


# ---------------------------------------------------------------------------
# Expectation registry: one entry per (column, check) pair, plus a bare
# reason label matching the pre-swap pandera check names exactly - the
# quarantine JSON contract's `reasons` entries are `f"{column}: {label}"`,
# unchanged by this swap (row_validator.py builds that string).
# ---------------------------------------------------------------------------


@dataclass
class ColumnExpectation:
    column: str
    label: str  # e.g. "non_empty", "valid_date", "greater_than_or_equal"
    expectation: Expectation


def _build_expectations(rule: QualityRule) -> tuple[list[ColumnExpectation], SqlLookupCheck | None]:
    expectations: list[ColumnExpectation] = []
    for item in rule.checks:
        if isinstance(item, str):
            if item == "not_null":
                expectations.append(
                    ColumnExpectation(
                        rule.column,
                        "not_nullable",  # matches pandera's Column(nullable=False) reason exactly
                        ExpectColumnValuesToNotBeNull(column=rule.column),
                    )
                )
            elif item == "non_empty":
                expectations.append(
                    ColumnExpectation(
                        rule.column, "non_empty", ExpectColumnValuesToBeDaisNonEmpty(column=rule.column)
                    )
                )
            elif item == "valid_date":
                if not rule.format:
                    raise ValueError(f"rule for column {rule.column!r} needs `format` for valid_date")
                expectations.append(
                    ColumnExpectation(
                        rule.column,
                        "valid_date",
                        ExpectColumnValuesToBeDaisValidDate(column=rule.column, date_format=rule.format),
                    )
                )
            elif item == "is_numeric":
                expectations.append(
                    ColumnExpectation(
                        rule.column, "is_numeric", ExpectColumnValuesToBeDaisNumeric(column=rule.column)
                    )
                )
            else:
                raise ValueError(f"unknown check {item!r} on column {rule.column!r}")
        elif isinstance(item, dict):
            for op, threshold in item.items():
                if op not in _COMPARISON_OPS:
                    raise ValueError(f"unsupported comparison check: {op!r}")
                expectations.append(
                    ColumnExpectation(
                        rule.column,
                        op,
                        ExpectColumnValuesToBeDaisComparison(
                            column=rule.column, op=op, threshold=float(threshold)
                        ),
                    )
                )
        else:
            raise ValueError(f"unsupported check item: {item!r}")

    lookup = SqlLookupCheck(rule.column, rule.lookup.query) if rule.lookup is not None else None
    return expectations, lookup


@dataclass
class DataFrameValidation:
    expectations: list[ColumnExpectation]
    sql_lookups: list[SqlLookupCheck]


def build_dataframe_validation(rules: list[QualityRule]) -> DataFrameValidation:
    all_expectations: list[ColumnExpectation] = []
    sql_lookups: list[SqlLookupCheck] = []
    for rule in rules:
        expectations, lookup = _build_expectations(rule)
        all_expectations.extend(expectations)
        if lookup is not None:
            sql_lookups.append(lookup)
    return DataFrameValidation(expectations=all_expectations, sql_lookups=sql_lookups)
