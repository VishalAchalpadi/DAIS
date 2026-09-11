from datetime import date, timedelta

import pytest

from dais.ai.anomaly_detector import (
    detect_anomalies,
    get_trailing_profiles,
    write_profile_history,
)
from dais.ai.profiler import ProfileResult
from dais.spec.models import AnomalyDetectionConfig
from tests.conftest import requires_local_postgres


def _cfg(**overrides) -> AnomalyDetectionConfig:
    defaults = dict(enabled=True, metrics=["row_count"], method="zscore", threshold=3.0, window=20)
    defaults.update(overrides)
    return AnomalyDetectionConfig(**defaults)


def _trailing(values: list[float]) -> list[dict]:
    return [{"row_count": v, "metrics": {}} for v in values]


# ---------------------------------------------------------------------------
# pure detection logic - no DB needed
# ---------------------------------------------------------------------------

def test_normal_run_is_not_flagged():
    # 20 prior runs tightly clustered around 1000 rows
    trailing = _trailing([1000, 1005, 998, 1002, 997, 1001, 1003, 999, 1000, 1004] * 2)
    profile = ProfileResult(row_count=1001, metrics={})
    cfg = _cfg(method="zscore", threshold=3.0)

    results = detect_anomalies(profile, trailing, cfg)

    assert len(results) == 1
    assert results[0].is_anomaly is False


def test_synthetic_outlier_is_flagged_zscore():
    trailing = _trailing([1000, 1005, 998, 1002, 997, 1001, 1003, 999, 1000, 1004] * 2)
    profile = ProfileResult(row_count=50, metrics={})  # a massive, genuine drop
    cfg = _cfg(method="zscore", threshold=3.0)

    results = detect_anomalies(profile, trailing, cfg)

    assert len(results) == 1
    assert results[0].is_anomaly is True
    assert results[0].current_value == 50
    assert results[0].score < -3.0


def test_synthetic_outlier_is_flagged_pct_change():
    trailing = _trailing([1000] * 10)
    profile = ProfileResult(row_count=1250, metrics={})  # +25%
    cfg = _cfg(method="pct_change", threshold=0.20)

    results = detect_anomalies(profile, trailing, cfg)

    assert results[0].is_anomaly is True
    assert results[0].score == pytest.approx(0.25, abs=0.01)


def test_pct_change_within_threshold_is_not_flagged():
    trailing = _trailing([1000] * 10)
    profile = ProfileResult(row_count=1100, metrics={})  # +10%, under a 20% threshold
    cfg = _cfg(method="pct_change", threshold=0.20)

    results = detect_anomalies(profile, trailing, cfg)

    assert results[0].is_anomaly is False


def test_insufficient_history_never_flags():
    profile = ProfileResult(row_count=50, metrics={})
    cfg = _cfg(method="zscore", threshold=3.0)

    # zero and one prior run - not enough to compute a meaningful baseline
    assert detect_anomalies(profile, [], cfg) == []
    assert detect_anomalies(profile, _trailing([1000]), cfg) == []


def test_metric_not_present_in_profile_is_skipped():
    profile = ProfileResult(row_count=1000, metrics={})  # no market_value_sum key
    trailing = [{"row_count": 1000, "metrics": {"market_value_sum": 500.0}}] * 5
    cfg = _cfg(metrics=["market_value_sum"], method="zscore", threshold=3.0)

    assert detect_anomalies(profile, trailing, cfg) == []


def test_generic_column_metric_flagged_not_just_row_count():
    trailing = [{"row_count": 1000, "metrics": {"market_value_sum": 1_000_000.0}} for _ in range(10)]
    profile = ProfileResult(row_count=1000, metrics={"market_value_sum": 100_000.0})  # a 90% drop
    cfg = _cfg(metrics=["market_value_sum"], method="pct_change", threshold=0.20)

    results = detect_anomalies(profile, trailing, cfg)

    assert results[0].metric == "market_value_sum"
    assert results[0].is_anomaly is True


def test_unsupported_method_raises():
    trailing = _trailing([1000] * 5)
    profile = ProfileResult(row_count=1000, metrics={})
    cfg = AnomalyDetectionConfig.model_construct(
        enabled=True, metrics=["row_count"], method="not_a_real_method", threshold=3.0, window=20, on_anomaly="alert"
    )
    with pytest.raises(ValueError, match="unsupported anomaly_detection.method"):
        detect_anomalies(profile, trailing, cfg)


# ---------------------------------------------------------------------------
# real Postgres round-trip: write then read back trailing history
# ---------------------------------------------------------------------------

@requires_local_postgres
def test_write_and_read_profile_history_round_trip(pg_connector):
    pipeline_name = "dq_recommender_test_pipeline_anomaly"
    today = date.today()

    for i in range(3):
        write_profile_history(
            pg_connector,
            process_id=f"proc-{i}",
            pipeline_name=pipeline_name,
            run_date=today - timedelta(days=3 - i),
            profile=ProfileResult(row_count=1000 + i, metrics={"market_value_sum": 500.0 + i}),
        )

    trailing = get_trailing_profiles(pg_connector, pipeline_name, window=20)

    assert len(trailing) == 3
    assert {p["row_count"] for p in trailing} == {1000, 1001, 1002}
    assert all("market_value_sum" in p["metrics"] for p in trailing)

    pg_connector.execute(
        "DELETE FROM control.data_profile_history WHERE pipeline_name = %(name)s", {"name": pipeline_name}
    )


@requires_local_postgres
def test_get_trailing_profiles_excludes_given_process_id(pg_connector):
    pipeline_name = "dq_recommender_test_pipeline_exclude"
    today = date.today()

    write_profile_history(
        pg_connector,
        process_id="keep-me",
        pipeline_name=pipeline_name,
        run_date=today,
        profile=ProfileResult(row_count=1, metrics={}),
    )
    write_profile_history(
        pg_connector,
        process_id="exclude-me",
        pipeline_name=pipeline_name,
        run_date=today,
        profile=ProfileResult(row_count=2, metrics={}),
    )

    trailing = get_trailing_profiles(pg_connector, pipeline_name, window=20, exclude_process_id="exclude-me")

    assert len(trailing) == 1
    assert trailing[0]["row_count"] == 1

    pg_connector.execute(
        "DELETE FROM control.data_profile_history WHERE pipeline_name = %(name)s", {"name": pipeline_name}
    )
