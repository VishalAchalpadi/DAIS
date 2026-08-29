import json

import boto3
import pytest
from moto import mock_aws

from dais.quality.dq_alerts import LogAlerter, get_alerter
from dais.quality.file_validator import (
    FileValidationOutcome,
    raise_alert,
    write_quarantine,
)
from dais.quality.row_validator import QuarantinedRow
from dais.resilience.connectors.s3_connector import S3Connector, parse_s3_uri
from dais.spec.models import QualityConfig


def _quality_cfg(channel="log", destination="n/a"):
    return QualityConfig(
        integrity_mode="row_level",
        rules=[{"column": "account_id", "checks": ["not_null"]}],
        quarantine={
            "location": "s3://test-bucket/holdings/quarantine/",
            "alert": {"channel": channel, "destination": destination},
        },
    )


def _rows():
    return [
        QuarantinedRow(row_index=2, row_data={"account_id": ""}, reasons=["account_id: non_empty"]),
    ]


# ---------------------------------------------------------------------------
# parse_s3_uri
# ---------------------------------------------------------------------------

def test_parse_s3_uri():
    assert parse_s3_uri("s3://bucket/a/b/") == ("bucket", "a/b/")
    assert parse_s3_uri("s3://bucket") == ("bucket", "")


def test_parse_s3_uri_rejects_non_s3():
    with pytest.raises(ValueError, match="not an s3"):
        parse_s3_uri("http://bucket/key")


# ---------------------------------------------------------------------------
# quarantine write (moto-mocked S3, no real AWS account needed)
# ---------------------------------------------------------------------------

@mock_aws
def test_write_quarantine_lands_readable_json_in_s3():
    boto3.client("s3", region_name="us-east-1").create_bucket(Bucket="test-bucket")
    s3 = S3Connector(region_name="us-east-1")
    quality_cfg = _quality_cfg()

    key_uri = write_quarantine(_rows(), quality_cfg, "HOLDINGS_20260101.txt", s3)

    assert key_uri.startswith("s3://test-bucket/holdings/quarantine/HOLDINGS_20260101.txt")
    bucket, key = parse_s3_uri(key_uri)
    body = s3.get_object(bucket, key)
    payload = json.loads(body)
    assert payload[0]["row_index"] == 2
    assert payload[0]["reasons"] == ["account_id: non_empty"]
    assert "quarantined_at" in payload[0]


@mock_aws
def test_write_quarantine_noop_on_empty_rows_still_writes_empty_list():
    boto3.client("s3", region_name="us-east-1").create_bucket(Bucket="test-bucket")
    s3 = S3Connector(region_name="us-east-1")
    quality_cfg = _quality_cfg()

    key_uri = write_quarantine([], quality_cfg, "empty.txt", s3)
    bucket, key = parse_s3_uri(key_uri)
    assert json.loads(s3.get_object(bucket, key)) == []


# ---------------------------------------------------------------------------
# alerting
# ---------------------------------------------------------------------------

def test_get_alerter_returns_registered_channels():
    assert isinstance(get_alerter("log"), LogAlerter)


def test_get_alerter_unknown_channel_raises():
    with pytest.raises(ValueError, match="no alerter registered"):
        get_alerter("carrier_pigeon")


def test_email_alerter_not_implemented_yet():
    from dais.quality.dq_alerts import EmailAlerter

    with pytest.raises(NotImplementedError):
        EmailAlerter().send("dq@acme.com", "msg", {})


def test_raise_alert_noop_when_nothing_quarantined():
    outcome = FileValidationOutcome(promoted_df=None, file_quarantined=False, quarantined_rows=[])
    # Should not raise, and should never even look up an alerter, since there's nothing to alert on.
    raise_alert(outcome, _quality_cfg(channel="log"), "file.txt")


def test_raise_alert_strict_sends_one_alert_for_whole_file():
    calls = []

    class RecordingAlerter:
        def send(self, destination, message, context):
            calls.append((destination, message, context))

    from dais.quality import dq_alerts

    original = dq_alerts._REGISTRY["log"]
    dq_alerts._REGISTRY["log"] = lambda: RecordingAlerter()
    try:
        outcome = FileValidationOutcome(promoted_df=None, file_quarantined=True, quarantined_rows=_rows())
        raise_alert(outcome, _quality_cfg(), "HOLDINGS_20260101.txt")
        assert len(calls) == 1
        assert "HOLDINGS_20260101.txt" in calls[0][1]
        assert calls[0][2]["row_count"] == 1
    finally:
        dq_alerts._REGISTRY["log"] = original


def test_raise_alert_row_level_sends_one_alert_per_row():
    calls = []

    class RecordingAlerter:
        def send(self, destination, message, context):
            calls.append((destination, message, context))

    from dais.quality import dq_alerts

    original = dq_alerts._REGISTRY["log"]
    dq_alerts._REGISTRY["log"] = lambda: RecordingAlerter()
    try:
        outcome = FileValidationOutcome(
            promoted_df=None,
            file_quarantined=False,
            quarantined_rows=[
                QuarantinedRow(row_index=1, row_data={}, reasons=["r1"]),
                QuarantinedRow(row_index=5, row_data={}, reasons=["r2"]),
            ],
        )
        raise_alert(outcome, _quality_cfg(channel="log"), "f.txt")
        assert len(calls) == 2
        assert calls[0][2]["row_index"] == 1
        assert calls[1][2]["row_index"] == 5
    finally:
        dq_alerts._REGISTRY["log"] = original
