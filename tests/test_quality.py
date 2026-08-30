import polars as pl
import pytest

from dais.quality.file_validator import enforce_integrity_mode
from dais.quality.row_validator import validate_dataframe
from dais.spec.models import QualityConfig
from tests.conftest import requires_local_postgres


def _rules(*, integrity_mode="row_level", extra_rules=None):
    rules = [
        {"column": "account_id", "checks": ["not_null", "non_empty"]},
        {
            "column": "quantity",
            "checks": ["not_null", "is_numeric", {"greater_than_or_equal": 0}],
            "cast_to": "decimal",
            "precision": 18,
            "scale": 4,
        },
        {
            "column": "as_of_date",
            "checks": ["not_null", "valid_date"],
            "cast_to": "date",
            "format": "%Y%m%d",
        },
    ] + (extra_rules or [])
    cfg = QualityConfig(
        integrity_mode=integrity_mode,
        rules=rules,
        quarantine={
            "location": "s3://test-bucket/quarantine/",
            "alert": {"channel": "log", "destination": "n/a"},
        },
    )
    return cfg


def _good_bad_df():
    return pl.DataFrame(
        {
            "account_id": ["ACC01", "ACC02", ""],
            "quantity": ["100.0000", "-5.0000", "200.0000"],
            "as_of_date": ["20260101", "20260101", "notadate"],
        }
    )


# ---------------------------------------------------------------------------
# row_validator
# ---------------------------------------------------------------------------

def test_all_rows_pass_when_valid():
    df = pl.DataFrame(
        {
            "account_id": ["ACC01", "ACC02"],
            "quantity": ["100.0000", "200.0000"],
            "as_of_date": ["20260101", "20260102"],
        }
    )
    result = validate_dataframe(df, _rules().rules)
    assert result.quarantined_rows == []
    assert result.valid_df.height == 2
    assert result.valid_df["quantity"].dtype == pl.Decimal(18, 4)
    assert result.valid_df["as_of_date"].dtype == pl.Date


def test_row_level_quarantines_only_failing_rows():
    df = _good_bad_df()
    result = validate_dataframe(df, _rules().rules)

    # row 1 fails greater_than_or_equal(0); row 2 fails non_empty + valid_date
    assert result.valid_df.height == 1
    assert result.valid_df["account_id"][0] == "ACC01"
    failed_indices = {r.row_index for r in result.quarantined_rows}
    assert failed_indices == {1, 2}


def test_quarantined_row_carries_reasons_and_original_data():
    df = _good_bad_df()
    result = validate_dataframe(df, _rules().rules)
    row2 = next(r for r in result.quarantined_rows if r.row_index == 2)
    assert row2.row_data["account_id"] == ""
    assert any("non_empty" in reason for reason in row2.reasons)
    assert any("valid_date" in reason for reason in row2.reasons)


def test_empty_dataframe_produces_no_failures():
    df = pl.DataFrame({"account_id": [], "quantity": [], "as_of_date": []}, schema={"account_id": pl.Utf8, "quantity": pl.Utf8, "as_of_date": pl.Utf8})
    result = validate_dataframe(df, _rules().rules)
    assert result.quarantined_rows == []
    assert result.valid_df.height == 0


# ---------------------------------------------------------------------------
# row_validator - duplicate business_key detection
# ---------------------------------------------------------------------------

def test_duplicate_business_key_quarantines_later_occurrences():
    df = pl.DataFrame(
        {
            "account_id": ["ACC01", "ACC02", "ACC01"],
            "quantity": ["100.0000", "200.0000", "999.0000"],
            "as_of_date": ["20260101", "20260101", "20260101"],
        }
    )
    result = validate_dataframe(df, _rules().rules, business_key=["account_id", "as_of_date"])

    assert result.valid_df.height == 2
    assert result.valid_df["account_id"].to_list() == ["ACC01", "ACC02"]
    failed = {r.row_index: r for r in result.quarantined_rows}
    assert set(failed) == {2}
    assert any("duplicate business key" in reason for reason in failed[2].reasons)


def test_no_business_key_means_no_duplicate_check():
    df = pl.DataFrame(
        {
            "account_id": ["ACC01", "ACC01"],
            "quantity": ["100.0000", "200.0000"],
            "as_of_date": ["20260101", "20260101"],
        }
    )
    result = validate_dataframe(df, _rules().rules)
    assert result.quarantined_rows == []
    assert result.valid_df.height == 2


def test_duplicate_business_key_combines_with_other_dq_failures():
    df = pl.DataFrame(
        {
            "account_id": ["ACC01", "ACC01"],
            "quantity": ["100.0000", "-5.0000"],
            "as_of_date": ["20260101", "20260101"],
        }
    )
    result = validate_dataframe(df, _rules().rules, business_key=["account_id", "as_of_date"])
    row1 = next(r for r in result.quarantined_rows if r.row_index == 1)
    assert any("duplicate business key" in reason for reason in row1.reasons)
    assert any("greater_than_or_equal" in reason for reason in row1.reasons)


# ---------------------------------------------------------------------------
# file_validator - integrity_mode enforcement
# ---------------------------------------------------------------------------

def test_row_level_integrity_promotes_passing_rows_only():
    df = _good_bad_df()
    quality_cfg = _rules(integrity_mode="row_level")
    result = validate_dataframe(df, quality_cfg.rules)
    outcome = enforce_integrity_mode(result, quality_cfg)

    assert outcome.file_quarantined is False
    assert outcome.promoted_df.height == 1
    assert len(outcome.quarantined_rows) == 2


def test_strict_integrity_quarantines_whole_file_on_any_row_failure():
    df = _good_bad_df()
    quality_cfg = _rules(integrity_mode="strict")
    result = validate_dataframe(df, quality_cfg.rules)
    outcome = enforce_integrity_mode(result, quality_cfg)

    assert outcome.file_quarantined is True
    assert outcome.promoted_df is None
    assert len(outcome.quarantined_rows) == 2  # the rows that actually failed


def test_strict_integrity_promotes_all_rows_when_none_fail():
    df = pl.DataFrame(
        {
            "account_id": ["ACC01", "ACC02"],
            "quantity": ["100.0000", "200.0000"],
            "as_of_date": ["20260101", "20260102"],
        }
    )
    quality_cfg = _rules(integrity_mode="strict")
    result = validate_dataframe(df, quality_cfg.rules)
    outcome = enforce_integrity_mode(result, quality_cfg)

    assert outcome.file_quarantined is False
    assert outcome.promoted_df.height == 2


# ---------------------------------------------------------------------------
# SQL lookup rule - requires a live Postgres ref.currencies table
# ---------------------------------------------------------------------------

@requires_local_postgres
def test_sql_lookup_rejects_unknown_currency_code(ref_currencies):
    extra_rule = [
        {
            "column": "currency",
            "checks": ["not_null"],
            "lookup": {
                "type": "sql",
                "query": "SELECT 1 FROM ref.currencies WHERE currency_code = :value",
            },
        }
    ]
    quality_cfg = _rules(extra_rules=extra_rule)
    df = pl.DataFrame(
        {
            "account_id": ["ACC01", "ACC02"],
            "quantity": ["100.0000", "200.0000"],
            "as_of_date": ["20260101", "20260101"],
            "currency": ["USD", "ZZZ"],
        }
    )
    result = validate_dataframe(df, quality_cfg.rules, connector=ref_currencies)

    assert result.valid_df.height == 1
    assert result.valid_df["currency"][0] == "USD"
    failed = {r.row_index for r in result.quarantined_rows}
    assert 1 in failed
