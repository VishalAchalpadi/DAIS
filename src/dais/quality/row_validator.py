"""Row-level DQ validation: runs the Great Expectations-backed checks built
from `quality.rules` (see schema_registry.py), splits the batch into
passing rows (cast to their `cast_to` types, ready for stage) and
quarantined rows (original text + the reasons they failed), one DQ alert
per quarantined row.
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import great_expectations as gx
import polars as pl
from great_expectations.checkpoint import UpdateDataDocsAction

from dais.quality.schema_registry import DataFrameValidation, build_dataframe_validation
from dais.resilience.connectors.base import DatabaseConnector
from dais.spec.models import QualityRule

logger = logging.getLogger(__name__)

# A single ephemeral GX context/pandas Data Source is reused across every
# validate() call in this process - GX's metric/expectation registries and
# context setup are process-global anyway (see schema_registry.py's custom
# expectation classes, registered once at import time), so there is no
# per-run isolation to gain from constructing a new context each time, only
# avoidable overhead.
_GX_CONTEXT = gx.get_context(mode="ephemeral")
_GX_DATASOURCE = _GX_CONTEXT.data_sources.add_pandas(name="dais_row_validator")
_GX_ASSET = _GX_DATASOURCE.add_dataframe_asset(name="batch")
_GX_BATCH_DEFINITION = _GX_ASSET.add_batch_definition_whole_dataframe(name="whole_dataframe")

_COMPLETE_RESULT_FORMAT = {"result_format": "COMPLETE"}

# Data Docs: GX's own browsable HTML UI (expectation suites + validation run
# history, one page per run, with sample failing rows per expectation).
# ephemeral contexts ship with a default "local_site" pointed at a throwaway
# temp dir; repoint it at a stable project-local directory so it survives
# across runs/processes and can be served by the API (see /ui/great-expectations
# in api/app.py). Building docs is best-effort and must never break a
# pipeline run - if it fails for any reason, log and continue.
GX_DATA_DOCS_DIR = Path(os.environ.get("DAIS_GX_DATA_DOCS_DIR", "gx_data_docs")).resolve()
_GX_CONTEXT.update_data_docs_site(
    site_name="local_site",
    site_config={
        "class_name": "SiteBuilder",
        "show_how_to_buttons": True,
        "store_backend": {
            "class_name": "TupleFilesystemStoreBackend",
            "base_directory": str(GX_DATA_DOCS_DIR),
        },
        "site_index_builder": {"class_name": "DefaultSiteIndexBuilder"},
    },
)

_GX_CHECKPOINT_CACHE: dict[str, Any] = {}


def _get_or_build_checkpoint(spec_name: str, validation: DataFrameValidation):
    """One suite/validation-definition/checkpoint per spec, reused (and kept
    up to date via add_or_update) across runs so Data Docs accumulates real
    run history per pipeline instead of one-off, unrelated pages."""
    checkpoint = _GX_CHECKPOINT_CACHE.get(spec_name)
    if checkpoint is not None:
        return checkpoint

    suite = gx.ExpectationSuite(name=spec_name)
    for col_expectation in validation.expectations:
        suite.add_expectation(col_expectation.expectation)
    suite = _GX_CONTEXT.suites.add_or_update(suite)

    validation_definition = _GX_CONTEXT.validation_definitions.add_or_update(
        gx.ValidationDefinition(name=spec_name, data=_GX_BATCH_DEFINITION, suite=suite)
    )
    checkpoint = _GX_CONTEXT.checkpoints.add_or_update(
        gx.Checkpoint(
            name=spec_name,
            validation_definitions=[validation_definition],
            actions=[UpdateDataDocsAction(name="update_data_docs")],
            result_format=_COMPLETE_RESULT_FORMAT,
        )
    )
    _GX_CHECKPOINT_CACHE[spec_name] = checkpoint
    return checkpoint


def _update_data_docs(spec_name: str, pandas_df, validation: DataFrameValidation) -> None:
    """Runs the same expectations a second time through a GX Checkpoint
    purely to populate Data Docs. Deliberately independent of
    _run_expectations' own per-expectation batch.validate() calls below,
    which compute the row_index -> reasons mapping the quarantine JSON
    contract depends on - this function's failures/exceptions must never
    change that outcome."""
    if not validation.expectations:
        return
    try:
        checkpoint = _get_or_build_checkpoint(spec_name, validation)
        checkpoint.run(batch_parameters={"dataframe": pandas_df})
    except Exception:
        logger.warning("failed to update Great Expectations Data Docs for %r", spec_name, exc_info=True)


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


def _run_expectations(pandas_df, validation: DataFrameValidation, spec_name: str) -> dict[int, list[str]]:
    failures: dict[int, list[str]] = {}
    batch = _GX_BATCH_DEFINITION.get_batch(batch_parameters={"dataframe": pandas_df})
    for col_expectation in validation.expectations:
        result = batch.validate(col_expectation.expectation, result_format=_COMPLETE_RESULT_FORMAT)
        if result.success:
            continue
        reason = f"{col_expectation.column}: {col_expectation.label}"
        for idx in result.result["unexpected_index_list"]:
            failures.setdefault(idx, []).append(reason)
    _update_data_docs(spec_name, pandas_df, validation)
    return failures


def _run_sql_lookups(
    df: pl.DataFrame, validation: DataFrameValidation, connector: DatabaseConnector | None
) -> dict[int, list[str]]:
    if not validation.sql_lookups:
        return {}
    if connector is None:
        raise ValueError("a quality rule has a SQL lookup but no connector was provided")
    failures: dict[int, list[str]] = {}
    for lookup in validation.sql_lookups:
        reason = f"{lookup.column}: sql_lookup"
        for idx in lookup.failing_indices(df, connector):
            failures.setdefault(idx, []).append(reason)
    return failures


def _failing_indices(
    df: pl.DataFrame, rules: list[QualityRule], connector: DatabaseConnector | None, spec_name: str
) -> dict[int, list[str]]:
    validation = build_dataframe_validation(rules)
    failures = _run_expectations(df.to_pandas(), validation, spec_name)
    for idx, reasons in _run_sql_lookups(df, validation, connector).items():
        failures.setdefault(idx, []).extend(reasons)
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
    spec_name: str = "adhoc",
) -> ValidationResult:
    if df.height == 0:
        return ValidationResult(valid_df=_apply_casts(df, rules), quarantined_rows=[])

    failures = _failing_indices(df, rules, connector, spec_name)

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
