"""Phase 8b: spec-driven data profiling.

Computes a run's profile - row count plus per-metric summary stats -
which anomaly_detector.py compares against a pipeline's own trailing
history. Which columns/aggregations to compute is entirely driven by
`anomaly_detection.metrics` in the spec (see spec/models.py), never
hardcoded to one pipeline's columns.

Metric name convention: "row_count" (the dataframe's row count - always
tracked as its own dedicated field, not part of the metrics dict), or
"<column>_<aggregation>" where aggregation is one of sum/avg/null_rate/
distinct_count.
"""
from __future__ import annotations

from dataclasses import dataclass

import polars as pl

_SUM = "_sum"
_AVG = "_avg"
_NULL_RATE = "_null_rate"
_DISTINCT_COUNT = "_distinct_count"
_SUFFIXES = (_SUM, _AVG, _NULL_RATE, _DISTINCT_COUNT)


@dataclass
class ProfileResult:
    row_count: int
    metrics: dict[str, float]


def _split_metric_name(metric_name: str, columns: list[str]) -> tuple[str, str]:
    for suffix in _SUFFIXES:
        if metric_name.endswith(suffix):
            column = metric_name[: -len(suffix)]
            if column not in columns:
                raise ValueError(
                    f"anomaly_detection metric {metric_name!r} refers to column {column!r}, "
                    f"which isn't in this dataframe (columns: {columns})"
                )
            return column, suffix
    raise ValueError(
        f"unsupported metric name {metric_name!r} - expected 'row_count' or a column "
        f"name suffixed with one of {_SUFFIXES}"
    )


def _compute_metric(df: pl.DataFrame, column: str, suffix: str) -> float:
    series = df[column]
    if suffix == _SUM:
        total = series.cast(pl.Float64, strict=False).sum()
        return float(total) if total is not None else 0.0
    if suffix == _AVG:
        mean = series.cast(pl.Float64, strict=False).mean()
        return float(mean) if mean is not None else 0.0
    if suffix == _NULL_RATE:
        return float(series.null_count()) / df.height if df.height else 0.0
    if suffix == _DISTINCT_COUNT:
        return float(series.n_unique())
    raise AssertionError(f"unreachable: unhandled suffix {suffix!r}")


def compute_profile(df: pl.DataFrame, metric_names: list[str]) -> ProfileResult:
    row_count = df.height
    metrics: dict[str, float] = {}
    for metric_name in metric_names:
        if metric_name == "row_count":
            continue
        column, suffix = _split_metric_name(metric_name, df.columns)
        metrics[metric_name] = _compute_metric(df, column, suffix)
    return ProfileResult(row_count=row_count, metrics=metrics)
