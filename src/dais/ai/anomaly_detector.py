"""Phase 8b: statistical anomaly detection against a pipeline's own
trailing history. Deliberately simple and explainable - z-score or
percent-change against a trailing mean, never an opaque ML model. Also
owns the control.data_profile_history table (idempotent DDL, same
pattern as raw/stage) that gives each pipeline its own history to
compare against.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date

import numpy as np
from scipy import stats as scipy_stats

from dais.ai.profiler import ProfileResult
from dais.resilience.connectors.base import ColumnDef, DatabaseConnector
from dais.spec.models import AnomalyDetectionConfig

PROFILE_SCHEMA = "control"
PROFILE_TABLE = "data_profile_history"


@dataclass
class AnomalyResult:
    metric: str
    current_value: float
    baseline_mean: float
    baseline_stdev: float
    baseline_size: int
    method: str
    threshold: float
    score: float
    is_anomaly: bool


def _metric_value(row_count: int, metrics: dict, name: str) -> float | None:
    if name == "row_count":
        return float(row_count)
    value = metrics.get(name)
    return float(value) if value is not None else None


def ensure_profile_table(connector: DatabaseConnector) -> None:
    connector.create_table_if_not_exists(
        PROFILE_SCHEMA,
        PROFILE_TABLE,
        [
            ColumnDef("process_id", "TEXT"),
            ColumnDef("pipeline_name", "TEXT"),
            ColumnDef("run_date", "DATE"),
            ColumnDef("row_count", "BIGINT"),
            ColumnDef("metrics", "JSONB"),
        ],
    )


def write_profile_history(
    connector: DatabaseConnector,
    *,
    process_id: str,
    pipeline_name: str,
    run_date: date,
    profile: ProfileResult,
) -> None:
    ensure_profile_table(connector)
    connector.bulk_insert(
        PROFILE_SCHEMA,
        PROFILE_TABLE,
        ["process_id", "pipeline_name", "run_date", "row_count", "metrics"],
        [(process_id, pipeline_name, run_date, profile.row_count, json.dumps(profile.metrics))],
    )


def get_trailing_profiles(
    connector: DatabaseConnector, pipeline_name: str, window: int, *, exclude_process_id: str | None = None
) -> list[dict]:
    """Most recent `window` profile rows for this pipeline, oldest-run
    exclusions aside - a run currently in flight shouldn't compare itself
    against its own not-yet-committed profile."""
    ensure_profile_table(connector)
    params: dict = {"name": pipeline_name, "n": window}
    exclude_clause = ""
    if exclude_process_id is not None:
        exclude_clause = "AND process_id != %(exclude_id)s "
        params["exclude_id"] = exclude_process_id
    rows = connector.fetch_all(
        f"SELECT row_count, metrics FROM {PROFILE_SCHEMA}.{PROFILE_TABLE} "
        f"WHERE pipeline_name = %(name)s {exclude_clause}"
        f"ORDER BY run_date DESC LIMIT %(n)s",
        params,
    )
    return [{"row_count": r[0], "metrics": r[1] or {}} for r in rows]


def detect_anomalies(
    profile: ProfileResult, trailing_profiles: list[dict], cfg: AnomalyDetectionConfig
) -> list[AnomalyResult]:
    results: list[AnomalyResult] = []
    for metric_name in cfg.metrics:
        current_value = _metric_value(profile.row_count, profile.metrics, metric_name)
        if current_value is None:
            continue

        baseline = [
            v
            for v in (_metric_value(p["row_count"], p["metrics"], metric_name) for p in trailing_profiles)
            if v is not None
        ]
        if len(baseline) < 2:
            # Not enough history to judge anything - never flag on
            # insufficient data, and never block a pipeline's first runs.
            continue

        baseline_arr = np.array(baseline, dtype=float)
        mean = float(np.mean(baseline_arr))
        stdev = float(np.std(baseline_arr, ddof=1))

        if cfg.method == "zscore":
            if stdev == 0:
                score = 0.0 if current_value == mean else float("inf")
            else:
                combined = np.append(baseline_arr, current_value)
                score = float(scipy_stats.zscore(combined)[-1])
            is_anomaly = abs(score) >= cfg.threshold
        elif cfg.method == "pct_change":
            if mean == 0:
                score = 0.0 if current_value == 0 else float("inf")
            else:
                score = (current_value - mean) / mean
            is_anomaly = abs(score) >= cfg.threshold
        else:
            raise ValueError(f"unsupported anomaly_detection.method: {cfg.method!r}")

        results.append(
            AnomalyResult(
                metric=metric_name,
                current_value=current_value,
                baseline_mean=mean,
                baseline_stdev=stdev,
                baseline_size=len(baseline),
                method=cfg.method,
                threshold=cfg.threshold,
                score=score,
                is_anomaly=is_anomaly,
            )
        )
    return results
