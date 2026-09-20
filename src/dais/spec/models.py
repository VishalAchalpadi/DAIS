"""Pydantic models defining the contract for a pipeline spec.

A PipelineSpec is the single source of truth for how a pipeline behaves.
`pipeline.py` executes generically off of this model - a new pipeline
should require a new YAML file here, not new Python code.
"""
from __future__ import annotations

import re
from typing import Any, Literal, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

_PLACEHOLDER_RE = re.compile(r"^<<.*>>$")
_PATTERN_TOKEN_RE = re.compile(r"\{(\w+)\}")


def _reject_placeholder(v: str) -> str:
    """Fail loudly if a required field was left as an unfilled '<<...>>' draft
    placeholder, instead of silently accepting it as a valid value."""
    if isinstance(v, str) and _PLACEHOLDER_RE.match(v.strip()):
        raise ValueError(
            f"unfilled placeholder value: {v!r} - this must be set before "
            "the spec can be used"
        )
    return v


class StrictModel(BaseModel):
    """Base for all spec models: unknown keys are errors, not silently
    dropped, so typos in a YAML spec surface immediately."""

    model_config = ConfigDict(extra="forbid")


# ---------------------------------------------------------------------------
# execution / database / monitoring
# ---------------------------------------------------------------------------

class ExecutionConfig(StrictModel):
    stop_after: Literal["raw", "stage", "gold"]


class DatabaseConfig(StrictModel):
    platform: Literal["postgres", "snowflake"]
    connection: str  # resolved via SecretsProvider, never a raw conn string

    _v_connection = field_validator("connection")(_reject_placeholder)


class MonitoringConfig(StrictModel):
    schema_: str = Field(alias="schema")
    table: str

    model_config = ConfigDict(extra="forbid", populate_by_name=True)


# ---------------------------------------------------------------------------
# source
# ---------------------------------------------------------------------------

class MultiFileSelection(StrictModel):
    """Configures how a pipeline trigger picks which file(s) to process
    when `SourceLocation.file_pattern` matches more than one file in
    `location.path` at trigger time (see `dais.ingestion.file_discovery`).
    Absent (the default on SourceLocation), a trigger must always name an
    exact file_path, exactly as before this feature existed."""

    mode: Literal["latest", "earliest", "all"]
    order_by: Literal["arrival_time", "filename_timestamp"] = "arrival_time"
    # strptime format for the substring the pattern's single {...} token
    # matches (e.g. "%Y%m%d" for "HOLDINGS_{date}.txt") - required, and
    # only meaningful, when order_by is filename_timestamp.
    filename_timestamp_format: str | None = None
    # Only meaningful when mode == "all" - processing order for the
    # resolved files.
    order: Literal["asc", "desc"] = "asc"
    # Only meaningful when mode == "all" - "stop" halts the rest of the
    # batch (marking them "skipped") the first time a file's run status
    # comes back exactly "failed" (not "quarantined" - a quarantined run
    # completed normally and landed its good rows). Use "stop" when later
    # files are deltas/corrections that assume an earlier file already
    # landed; "continue" (default) suits independent-snapshot files.
    on_earlier_failure: Literal["continue", "stop"] = "continue"

    @model_validator(mode="after")
    def _timestamp_format_required(self) -> "MultiFileSelection":
        if self.order_by == "filename_timestamp" and not self.filename_timestamp_format:
            raise ValueError(
                "filename_timestamp_format is required when order_by is filename_timestamp"
            )
        return self


class SourceLocation(StrictModel):
    kind: Literal["s3", "local"]
    path: str
    file_pattern: str | None = None
    multi_file: MultiFileSelection | None = None

    _v_path = field_validator("path")(_reject_placeholder)

    @model_validator(mode="after")
    def _multi_file_requires_pattern(self) -> "SourceLocation":
        if self.multi_file is None:
            return self
        if not self.file_pattern:
            raise ValueError("multi_file requires file_pattern to be set")
        token_count = len(_PATTERN_TOKEN_RE.findall(self.file_pattern))
        if token_count > 1:
            raise ValueError(
                f"file_pattern must contain at most one {{token}}, got {self.file_pattern!r}"
            )
        if self.multi_file.order_by == "filename_timestamp" and token_count == 0:
            raise ValueError(
                "file_pattern must contain a {token} (e.g. \"HOLDINGS_{date}.txt\") "
                "when multi_file.order_by is filename_timestamp"
            )
        return self


class SourceConfig(StrictModel):
    type: Literal["file"]
    format: Literal["csv", "delimited", "fixed_width", "json", "xml"]
    location: SourceLocation


# ---------------------------------------------------------------------------
# parser
# ---------------------------------------------------------------------------

class FixedWidthColumn(StrictModel):
    name: str
    start: int = Field(ge=1)
    end: int = Field(ge=1)

    @field_validator("end")
    @classmethod
    def _end_after_start(cls, v: int, info) -> int:
        start = info.data.get("start")
        if start is not None and v < start:
            raise ValueError(f"end ({v}) must be >= start ({start})")
        return v


class ParserConfig(StrictModel):
    type: Literal["csv", "delimited", "fixed_width", "json", "xml"]

    # fixed_width
    columns: list[FixedWidthColumn] | None = None

    # delimited (csv uses the same knobs with sensible defaults)
    delimiter: str | None = None
    quote_char: str | None = None
    escape_char: str | None = None
    has_header: bool | None = None

    # json
    record_path: str | None = None
    lines: bool | None = None

    # xml
    record_xpath: str | None = None


# ---------------------------------------------------------------------------
# control gates
# ---------------------------------------------------------------------------

class FileSizeGate(StrictModel):
    min_bytes: int = Field(ge=0)
    max_bytes: int = Field(gt=0)

    @field_validator("max_bytes")
    @classmethod
    def _max_gt_min(cls, v: int, info) -> int:
        min_bytes = info.data.get("min_bytes")
        if min_bytes is not None and v < min_bytes:
            raise ValueError(f"max_bytes ({v}) must be >= min_bytes ({min_bytes})")
        return v


class RowCountGate(StrictModel):
    min_rows: int = Field(ge=0)
    max_rows: int = Field(gt=0)

    @field_validator("max_rows")
    @classmethod
    def _max_gt_min(cls, v: int, info) -> int:
        min_rows = info.data.get("min_rows")
        if min_rows is not None and v < min_rows:
            raise ValueError(f"max_rows ({v}) must be >= min_rows ({min_rows})")
        return v


class ControlGatesConfig(StrictModel):
    file_size: FileSizeGate
    row_count: RowCountGate
    on_fail: Literal["quarantine_file"] = "quarantine_file"


# ---------------------------------------------------------------------------
# exports - optional Parquet-into-Iceberg sink, usable on any layer
# (raw, stage, gold), independent of `execution.stop_after`.
# ---------------------------------------------------------------------------

class ExportConfig(StrictModel):
    enabled: bool = False
    format: Literal["parquet"] = "parquet"
    target: Literal["iceberg"] = "iceberg"
    catalog: str | None = None
    table: str | None = None
    location: str | None = None

    @field_validator("catalog", "table", "location")
    @classmethod
    def _v_no_placeholder_if_enabled(cls, v: str | None, info) -> str | None:
        if info.data.get("enabled") and v is not None:
            return _reject_placeholder(v)
        return v


# ---------------------------------------------------------------------------
# raw (bronze)
# ---------------------------------------------------------------------------

class RawConfig(StrictModel):
    schema_: str = Field(alias="schema")
    table: str
    ddl_mode: Literal["create_if_not_exists"] = "create_if_not_exists"
    column_type: Literal["text"] = "text"
    preserve_metadata: list[str] = Field(default_factory=list)
    checksum_dedup: bool = True
    exports: list[ExportConfig] = Field(default_factory=list)

    model_config = ConfigDict(extra="forbid", populate_by_name=True)


# ---------------------------------------------------------------------------
# quality (raw -> stage)
# ---------------------------------------------------------------------------

class SqlLookup(StrictModel):
    type: Literal["sql"]
    query: str
    on_fail: Literal["quarantine"] = "quarantine"


# A check is either a bare name ("not_null") or a single-key parameterized
# check ({"greater_than_or_equal": 0}). Single source of truth for both
# spec validation (here) and the DQ engine (quality/schema_registry.py) -
# import from here rather than duplicating the name lists.
CheckItem = Union[str, dict[str, Any]]

BARE_CHECK_NAMES = {"not_null", "non_empty", "valid_date", "is_numeric"}
COMPARISON_CHECK_OPS = {
    "greater_than_or_equal",
    "greater_than",
    "less_than_or_equal",
    "less_than",
}


class QualityRule(StrictModel):
    column: str
    checks: list[CheckItem] = Field(default_factory=list)
    cast_to: Literal["date", "decimal"] | None = None
    precision: int | None = None
    scale: int | None = None
    format: str | None = None
    lookup: SqlLookup | None = None

    @field_validator("checks")
    @classmethod
    def _validate_check_items(cls, checks: list[CheckItem], info) -> list[CheckItem]:
        column = info.data.get("column", "<unknown>")
        for item in checks:
            if isinstance(item, str):
                if item not in BARE_CHECK_NAMES:
                    raise ValueError(
                        f"column {column!r}: unknown check {item!r}; "
                        f"must be one of {sorted(BARE_CHECK_NAMES)}"
                    )
            elif isinstance(item, dict):
                if len(item) != 1:
                    raise ValueError(
                        f"column {column!r}: a parameterized check must have exactly one "
                        f"key, got {item!r}"
                    )
                (op, value), = item.items()
                if op not in COMPARISON_CHECK_OPS:
                    raise ValueError(
                        f"column {column!r}: unknown comparison check {op!r}; "
                        f"must be one of {sorted(COMPARISON_CHECK_OPS)}"
                    )
                if not isinstance(value, (int, float)) or isinstance(value, bool):
                    raise ValueError(
                        f"column {column!r}: check {op!r} needs a numeric value, got {value!r}"
                    )
            else:
                raise ValueError(f"column {column!r}: unsupported check item {item!r}")
        return checks

    @model_validator(mode="after")
    def _valid_date_requires_format(self) -> "QualityRule":
        # A field_validator on `format` alone wouldn't catch this: pydantic
        # v2 skips validators on unset/default values, and `format` is
        # optional with a None default - a model-level check runs
        # regardless of whether the caller supplied it.
        if "valid_date" in self.checks and not self.format:
            raise ValueError(f"column {self.column!r}: `format` is required when `valid_date` is used")
        return self


class AlertConfig(StrictModel):
    channel: Literal["log", "webhook", "email"]
    destination: str

    _v_destination = field_validator("destination")(_reject_placeholder)


class QuarantineConfig(StrictModel):
    kind: Literal["local", "s3"] = "s3"
    location: str
    group_by: list[str] | None = None
    alert: AlertConfig

    _v_location = field_validator("location")(_reject_placeholder)

    @model_validator(mode="after")
    def _location_matches_kind(self) -> "QuarantineConfig":
        if self.kind == "s3" and not self.location.startswith("s3://"):
            raise ValueError(f"quarantine.location must be an s3:// URI when kind is 's3', got {self.location!r}")
        if self.kind == "local" and self.location.startswith("s3://"):
            raise ValueError("quarantine.location must be a local path when kind is 'local', not an s3:// URI")
        return self


class QualityConfig(StrictModel):
    # strict: one bad row quarantines the whole file.
    # row_level: only the failing row(s) are quarantined.
    # group_level: every row sharing quarantine.group_by's key with a
    # failing row is quarantined together, even rows that individually
    # passed - prevents a single bad row (e.g. one holding) from leaving
    # the rest of its group (e.g. that portfolio's other holdings) landed
    # in stage as a silently-incomplete set.
    integrity_mode: Literal["strict", "row_level", "group_level"]
    rules: list[QualityRule] = Field(default_factory=list)
    quarantine: QuarantineConfig

    @model_validator(mode="after")
    def _group_level_requires_group_by(self) -> "QualityConfig":
        if self.integrity_mode == "group_level" and not self.quarantine.group_by:
            raise ValueError("quality.quarantine.group_by is required when integrity_mode is 'group_level'")
        if self.integrity_mode != "group_level" and self.quarantine.group_by:
            raise ValueError("quality.quarantine.group_by is only used when integrity_mode is 'group_level'")
        return self


# ---------------------------------------------------------------------------
# stage (silver)
# ---------------------------------------------------------------------------

class StageConfig(StrictModel):
    schema_: str = Field(alias="schema")
    table: str
    ddl_mode: Literal["create_if_not_exists"] = "create_if_not_exists"
    business_key: list[str] = Field(min_length=1)
    write_mode: Literal["truncate_load", "append", "upsert"]
    exports: list[ExportConfig] = Field(default_factory=list)

    model_config = ConfigDict(extra="forbid", populate_by_name=True)


# ---------------------------------------------------------------------------
# gold
# ---------------------------------------------------------------------------

class GoldConfig(StrictModel):
    schema_: str = Field(alias="schema")
    dbt_project: str
    dbt_select: str
    exports: list[ExportConfig] = Field(default_factory=list)
    # SCD Type 2: the dbt model tracks every version of a row rather than
    # overwriting it, via active_flag/effective_datetime/expiry_datetime/
    # version columns - see dbt/macros/dais_scd2.sql. This field is
    # documentation/discoverability for the spec (which columns identify
    # a "row" for versioning purposes), not something DAIS's Python layer
    # enforces at runtime - the model's own SQL is what actually
    # implements it, same as any other gold transformation.
    scd_type: Literal[2] | None = None
    scd_key: list[str] | None = None

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    @model_validator(mode="after")
    def _scd_type_requires_scd_key(self) -> "GoldConfig":
        if self.scd_type is not None and not self.scd_key:
            raise ValueError("gold.scd_key is required when gold.scd_type is set")
        if self.scd_type is None and self.scd_key:
            raise ValueError("gold.scd_key is only used when gold.scd_type is set")
        return self


# ---------------------------------------------------------------------------
# resilience
# ---------------------------------------------------------------------------

class RetryConfig(StrictModel):
    max_attempts: int = Field(ge=1)
    backoff: Literal["exponential", "fixed"]
    base_delay_seconds: float = Field(gt=0)
    jitter: bool = True


class ResilienceConfig(StrictModel):
    retry: RetryConfig


# ---------------------------------------------------------------------------
# lineage
# ---------------------------------------------------------------------------

class LineageConfig(StrictModel):
    namespace: str
    job_name: str

    _v_namespace = field_validator("namespace")(_reject_placeholder)


# ---------------------------------------------------------------------------
# anomaly detection (Phase 8b) - optional, additive. Metric names are
# either "row_count" or "<column>_<aggregation>" (sum/avg/null_rate/
# distinct_count) - spec-driven, never a hardcoded column list, so this
# works generically for any pipeline. See src/dais/ai/profiler.py and
# src/dais/ai/anomaly_detector.py for the implementation - this model
# only declares and validates the config shape.
# ---------------------------------------------------------------------------

_ANOMALY_METRIC_SUFFIXES = ("_sum", "_avg", "_null_rate", "_distinct_count")


class AnomalyDetectionConfig(StrictModel):
    enabled: bool = True
    metrics: list[str] = Field(min_length=1)
    method: Literal["zscore", "pct_change"]
    threshold: float = Field(gt=0)
    window: int = Field(ge=2, default=20)
    on_anomaly: Literal["alert", "quarantine"] = "alert"

    @field_validator("metrics")
    @classmethod
    def _validate_metric_names(cls, metrics: list[str]) -> list[str]:
        for metric in metrics:
            if metric == "row_count":
                continue
            if not any(metric.endswith(suffix) for suffix in _ANOMALY_METRIC_SUFFIXES):
                raise ValueError(
                    f"unsupported anomaly_detection metric {metric!r}; must be 'row_count' "
                    f"or a column name ending in one of {_ANOMALY_METRIC_SUFFIXES}"
                )
        return metrics


# ---------------------------------------------------------------------------
# top-level spec
# ---------------------------------------------------------------------------

class PipelineSpec(StrictModel):
    pipeline_name: str
    description: str
    owner: str
    business_process: str | None = None

    execution: ExecutionConfig
    database: DatabaseConfig
    monitoring: MonitoringConfig
    source: SourceConfig
    parser: ParserConfig
    control_gates: ControlGatesConfig
    raw: RawConfig
    quality: QualityConfig
    stage: StageConfig
    # Optional as of Phase 9a: a new pipeline whose gold logic lives in a
    # gold_builds/*.yaml (GoldBuildSpec, below) instead of nested here
    # simply omits this block - existing pipelines keep it and are
    # completely unaffected. If omitted, execution.stop_after must not
    # be "gold" (there is nothing here to run).
    gold: GoldConfig | None = None
    resilience: ResilienceConfig
    lineage: LineageConfig
    anomaly_detection: AnomalyDetectionConfig | None = None

    @model_validator(mode="after")
    def _stop_after_gold_requires_gold_block(self) -> "PipelineSpec":
        if self.execution.stop_after == "gold" and self.gold is None:
            raise ValueError(
                "execution.stop_after is 'gold' but this spec has no gold: block - "
                "either add one or lower stop_after to 'stage'"
            )
        return self

    _v_owner = field_validator("owner")(_reject_placeholder)

    @field_validator("parser")
    @classmethod
    def _parser_matches_source_format(cls, v: ParserConfig, info) -> ParserConfig:
        source = info.data.get("source")
        if source is not None and v.type != source.format:
            raise ValueError(
                f"parser.type ({v.type!r}) must match source.format ({source.format!r})"
            )
        if v.type == "fixed_width" and not v.columns:
            raise ValueError("parser.columns is required when parser.type is fixed_width")
        return v


# ---------------------------------------------------------------------------
# gold_builds/*.yaml (Phase 9a) - gold-layer business logic that spans
# multiple ingest pipelines' stage tables, as its own spec type rather
# than nested inside a single pipeline's gold: block. depends_on is
# declared explicitly (not inferred from dbt ref()/source() calls) so
# orchestration/SLA tooling can reason about it without parsing SQL.
# ---------------------------------------------------------------------------

class GoldBuildDatabaseConfig(StrictModel):
    platform: Literal["postgres", "snowflake"]
    connection: str

    _v_connection = field_validator("connection")(_reject_placeholder)


class GoldBuildDbtConfig(StrictModel):
    project_dir: str
    select: str  # e.g. "tag:trade_blotter_gold" - scopes to this build's models only


class GoldBuildTargetConfig(StrictModel):
    schema_: str = Field(alias="schema")
    # The table(s) actually produced are whatever `dbt.select` materializes;
    # this is documentation/visibility only, not enforced against dbt's
    # output.
    primary_table: str

    model_config = ConfigDict(extra="forbid", populate_by_name=True)


class GoldBuildSlaConfig(StrictModel):
    complete_by: str
    timezone: str

    _v_complete_by = field_validator("complete_by")(_reject_placeholder)
    _v_timezone = field_validator("timezone")(_reject_placeholder)


class GoldBuildSpec(StrictModel):
    gold_build_name: str
    description: str
    owner: str
    depends_on: list[str] = Field(min_length=1)
    database: GoldBuildDatabaseConfig
    dbt: GoldBuildDbtConfig
    target: GoldBuildTargetConfig
    sla: GoldBuildSlaConfig | None = None
    # Phase 9b: reuses the SAME LineageConfig shape ingest pipelines use
    # (not a parallel schema) - set namespace to match the namespace of
    # the pipeline(s) in depends_on, so dbt-ol's emitted events land in
    # the same OpenLineage graph as raw/stage, not a disconnected one.
    lineage: LineageConfig | None = None

    _v_owner = field_validator("owner")(_reject_placeholder)
