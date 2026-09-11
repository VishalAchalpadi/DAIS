import polars as pl
import pytest

from dais.ai.profiler import compute_profile


def test_row_count_always_captured():
    df = pl.DataFrame({"a": [1, 2, 3]})
    profile = compute_profile(df, ["row_count"])
    assert profile.row_count == 3
    assert profile.metrics == {}  # row_count is its own field, not in the metrics dict


def test_sum_metric():
    df = pl.DataFrame({"market_value": [100.0, 200.0, 300.0]})
    profile = compute_profile(df, ["market_value_sum"])
    assert profile.metrics["market_value_sum"] == pytest.approx(600.0)


def test_avg_metric():
    df = pl.DataFrame({"quantity": [10.0, 20.0, 30.0]})
    profile = compute_profile(df, ["quantity_avg"])
    assert profile.metrics["quantity_avg"] == pytest.approx(20.0)


def test_null_rate_metric():
    df = pl.DataFrame({"currency": ["USD", None, "EUR", None]})
    profile = compute_profile(df, ["currency_null_rate"])
    assert profile.metrics["currency_null_rate"] == pytest.approx(0.5)


def test_distinct_count_metric():
    df = pl.DataFrame({"currency": ["USD", "EUR", "USD", "GBP"]})
    profile = compute_profile(df, ["currency_distinct_count"])
    assert profile.metrics["currency_distinct_count"] == pytest.approx(3.0)


def test_multiple_metrics_computed_together():
    df = pl.DataFrame(
        {
            "market_value": [100.0, 200.0, 300.0],
            "quantity": [10.0, 20.0, 30.0],
            "currency": ["USD", "EUR", "USD"],
        }
    )
    profile = compute_profile(df, ["row_count", "market_value_sum", "quantity_avg", "currency_distinct_count"])
    assert profile.row_count == 3
    assert profile.metrics == {
        "market_value_sum": pytest.approx(600.0),
        "quantity_avg": pytest.approx(20.0),
        "currency_distinct_count": pytest.approx(2.0),
    }


def test_unknown_metric_suffix_raises():
    df = pl.DataFrame({"a": [1, 2, 3]})
    with pytest.raises(ValueError, match="unsupported metric name"):
        compute_profile(df, ["a_median"])


def test_metric_referring_to_missing_column_raises():
    df = pl.DataFrame({"a": [1, 2, 3]})
    with pytest.raises(ValueError, match="isn't in this dataframe"):
        compute_profile(df, ["b_sum"])


def test_empty_dataframe_null_rate_does_not_divide_by_zero():
    df = pl.DataFrame({"a": []}, schema={"a": pl.Utf8})
    profile = compute_profile(df, ["a_null_rate"])
    assert profile.metrics["a_null_rate"] == 0.0
