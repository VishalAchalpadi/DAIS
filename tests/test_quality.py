import json

import polars as pl
import pytest

from dais.quality.file_validator import enforce_integrity_mode, write_quarantine, write_raw_file_quarantine
from dais.quality.row_validator import QuarantinedRow, validate_dataframe
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


def test_group_level_cascades_to_otherwise_valid_rows_in_same_group():
    # 3 holdings for ACC01 (2 valid, 1 invalid) and 1 for ACC02 (valid) -
    # the whole ACC01 group must be quarantined, ACC02 must survive.
    df = pl.DataFrame(
        {
            "account_id": ["ACC01", "ACC01", "ACC01", "ACC02"],
            "quantity": ["100.0000", "200.0000", "-5.0000", "50.0000"],
            "as_of_date": ["20260101", "20260101", "20260101", "20260101"],
        }
    )
    result = validate_dataframe(df, _rules().rules, group_by=["account_id"])

    assert result.valid_df.height == 1
    assert result.valid_df["account_id"][0] == "ACC02"
    failed_indices = {r.row_index for r in result.quarantined_rows}
    assert failed_indices == {0, 1, 2}
    cascaded_row = next(r for r in result.quarantined_rows if r.row_index == 0)
    assert "quarantined with group" in cascaded_row.reasons[0]


def test_group_level_does_not_touch_unrelated_groups():
    df = pl.DataFrame(
        {
            "account_id": ["ACC01", "ACC02", "ACC03"],
            "quantity": ["-5.0000", "100.0000", "200.0000"],
            "as_of_date": ["20260101", "20260101", "20260101"],
        }
    )
    result = validate_dataframe(df, _rules().rules, group_by=["account_id"])
    assert result.valid_df["account_id"].to_list() == ["ACC02", "ACC03"]


def test_no_group_by_means_no_cascading():
    df = pl.DataFrame(
        {
            "account_id": ["ACC01", "ACC01"],
            "quantity": ["100.0000", "-5.0000"],
            "as_of_date": ["20260101", "20260101"],
        }
    )
    result = validate_dataframe(df, _rules().rules)
    assert result.valid_df.height == 1
    assert result.valid_df["account_id"][0] == "ACC01"


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


# ---------------------------------------------------------------------------
# file_validator - local quarantine (no AWS account/S3Connector needed)
# ---------------------------------------------------------------------------

def _local_quality_cfg(tmp_path):
    return QualityConfig(
        integrity_mode="row_level",
        rules=[{"column": "account_id", "checks": ["not_null"]}],
        quarantine={
            "kind": "local",
            "location": str(tmp_path / "quarantine"),
            "alert": {"channel": "log", "destination": "n/a"},
        },
    )


def test_write_quarantine_local_creates_readable_json(tmp_path):
    quality_cfg = _local_quality_cfg(tmp_path)
    rows = [QuarantinedRow(row_index=2, row_data={"account_id": ""}, reasons=["account_id: non_empty"])]

    path = write_quarantine(rows, quality_cfg, "assets_20260830.csv")

    payload = json.loads((tmp_path / "quarantine").glob("*").__next__().read_text(encoding="utf-8"))
    assert path.endswith(".quarantine.json")
    assert payload[0]["row_index"] == 2
    assert payload[0]["reasons"] == ["account_id: non_empty"]


def test_write_quarantine_local_creates_directory_if_missing(tmp_path):
    quality_cfg = _local_quality_cfg(tmp_path)
    assert not (tmp_path / "quarantine").exists()

    write_quarantine([], quality_cfg, "empty.csv")

    assert (tmp_path / "quarantine").is_dir()


# ---------------------------------------------------------------------------
# Phase 11a regression: the quarantined-row JSON contract the exception
# remediation UI consumes must be byte-for-byte/field-for-field identical
# to what it was under pandera. This reproduces the exact scenario recorded
# as the pre-swap baseline (a real holdings_ingest run with account_id
# blanked out on one row - see the Phase 11 baseline capture): the same
# input must still produce the same row_index, the same row_data, and the
# same reasons string. quarantined_at is checked only for being a valid
# ISO8601 UTC timestamp (a wall-clock value, never expected to be identical
# across runs).
# ---------------------------------------------------------------------------

def test_known_bad_row_produces_unchanged_quarantine_json_contract(tmp_path):
    from datetime import datetime

    df = pl.DataFrame(
        {
            "account_id": ["", "ACC0002"],
            "security_id": ["SEC0001", "SEC0002"],
            "as_of_date": ["20260101", "20260101"],
            "quantity": ["10.5000", "20.5000"],
            "market_value": ["1000.25", "2000.25"],
            "currency": ["EUR", "GBP"],
        }
    )
    rules = QualityConfig(
        integrity_mode="row_level",
        rules=[
            {"column": "account_id", "checks": ["not_null", "non_empty"]},
            {"column": "security_id", "checks": ["not_null", "non_empty"]},
            {"column": "as_of_date", "checks": ["not_null", "valid_date"], "format": "%Y%m%d"},
            {"column": "quantity", "checks": ["not_null", "is_numeric", {"greater_than_or_equal": 0}]},
            {"column": "market_value", "checks": ["not_null", "is_numeric"]},
            {"column": "currency", "checks": ["not_null"]},
        ],
        quarantine={
            "kind": "local",
            "location": str(tmp_path / "quarantine"),
            "alert": {"channel": "log", "destination": "n/a"},
        },
    ).rules

    result = validate_dataframe(df, rules)
    assert len(result.quarantined_rows) == 1

    quality_cfg = _local_quality_cfg(tmp_path)
    write_quarantine(result.quarantined_rows, quality_cfg, "HOLDINGS_20260917_baseline_bad.txt")

    payload = json.loads((tmp_path / "quarantine").glob("*").__next__().read_text(encoding="utf-8"))
    assert len(payload) == 1
    record = payload[0]

    # Exact match against the recorded pre-swap (pandera) baseline.
    assert record["row_index"] == 0
    assert record["row_data"] == {
        "account_id": "",
        "security_id": "SEC0001",
        "as_of_date": "20260101",
        "quantity": "10.5000",
        "market_value": "1000.25",
        "currency": "EUR",
    }
    assert record["reasons"] == ["account_id: non_empty"]
    assert set(record.keys()) == {"row_index", "row_data", "reasons", "quarantined_at"}
    # ISO8601 UTC, parseable - the one field that's a wall-clock value and
    # was never expected to be identical across runs.
    datetime.fromisoformat(record["quarantined_at"])


def test_write_raw_file_quarantine_local_writes_original_bytes(tmp_path):
    quality_cfg = _local_quality_cfg(tmp_path)
    raw_bytes = b"portfolio_cd,as_of_date\nPORT0001,2026-08-30\n"

    path = write_raw_file_quarantine(raw_bytes, quality_cfg, "assets_20260830.csv")

    assert path.endswith(".quarantine")
    from pathlib import Path

    assert Path(path).read_bytes() == raw_bytes


def test_write_quarantine_s3_kind_without_connector_raises(tmp_path):
    quality_cfg = QualityConfig(
        integrity_mode="row_level",
        rules=[{"column": "account_id", "checks": ["not_null"]}],
        quarantine={
            "location": "s3://test-bucket/quarantine/",
            "alert": {"channel": "log", "destination": "n/a"},
        },
    )
    with pytest.raises(ValueError, match="S3Connector"):
        write_quarantine([], quality_cfg, "file.csv", s3=None)
