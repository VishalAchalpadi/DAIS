import json
import os
from datetime import datetime, timezone
from pathlib import Path

import yaml
from dagster import build_sensor_context, job, op

from dais.orchestration.dagster import watch_sensors
from dais.orchestration.dagster.watch_sensors import build_watch_sensor
from dais.spec.models import PipelineSpec

FIXTURE = Path(__file__).parent / "fixtures" / "valid_holdings_ingest.yaml"
NOW = datetime(2026, 9, 21, 10, 0, tzinfo=timezone.utc)


class _FixedDatetime(datetime):
    @classmethod
    def now(cls, tz=None):
        return NOW.astimezone(tz) if tz else NOW


@op
def _noop():
    pass


@job
def _dummy_job():
    _noop()


class _RecordingAlerter:
    def __init__(self):
        self.sent = []

    def send(self, destination, message, context):
        self.sent.append((destination, message, context))


def _spec(landing_dir, *, expected_by=None, kind="local"):
    data = yaml.safe_load(FIXTURE.read_text(encoding="utf-8"))
    watch = {"poll_interval_seconds": 30}
    if expected_by:
        watch["expected_by"] = expected_by
        watch["missed_arrival_alert"] = {"channel": "webhook", "destination": "http://alerts.test/hook"}
    data["source"]["location"] = {
        "kind": kind,
        "path": str(landing_dir),
        "file_pattern": "HOLDINGS_{date}.txt",
        "multi_file": {"mode": "all", "order_by": "arrival_time"},
        "watch": watch,
    }
    return PipelineSpec.model_validate(data)


def _touch(path, name, when=NOW):
    p = path / name
    p.write_text("x")
    os.utime(p, (when.timestamp(), when.timestamp()))
    return p


def _tick(sensor_def, cursor=None):
    return sensor_def(build_sensor_context(cursor=cursor))


def _setup(monkeypatch, spec):
    monkeypatch.setattr(watch_sensors, "datetime", _FixedDatetime)
    alerter = _RecordingAlerter()
    monkeypatch.setattr(watch_sensors, "get_alerter", lambda channel: alerter)
    return build_watch_sensor("holdings_ingest", spec, _dummy_job), alerter


def test_no_matching_files_yields_no_run_requests(tmp_path, monkeypatch):
    sensor_def, _ = _setup(monkeypatch, _spec(tmp_path))
    result = _tick(sensor_def)
    assert result.run_requests == []


def test_new_file_yields_run_request_with_expected_shape(tmp_path, monkeypatch):
    f = _touch(tmp_path, "HOLDINGS_20260921.txt")
    sensor_def, _ = _setup(monkeypatch, _spec(tmp_path))
    result = _tick(sensor_def)

    assert len(result.run_requests) == 1
    req = result.run_requests[0]
    assert req.run_key.startswith(f"holdings_ingest:{f}")
    assert req.run_config["ops"]["holdings_ingest__stage"]["config"]["file_path"] == str(f)


def test_repeated_ticks_are_stable_and_do_not_grow_the_cursor(tmp_path, monkeypatch):
    _touch(tmp_path, "HOLDINGS_20260921.txt")
    sensor_def, _ = _setup(monkeypatch, _spec(tmp_path))

    first = _tick(sensor_def)
    second = _tick(sensor_def, cursor=first.cursor)

    assert [r.run_key for r in second.run_requests] == [r.run_key for r in first.run_requests]
    assert len(second.cursor) <= len(first.cursor)


def test_missed_arrival_alert_fires_once_after_deadline(tmp_path, monkeypatch):
    sensor_def, alerter = _setup(monkeypatch, _spec(tmp_path, expected_by="09:00"))

    first = _tick(sensor_def)
    assert len(alerter.sent) == 1
    destination, message, context = alerter.sent[0]
    assert destination == "http://alerts.test/hook"
    assert "holdings_ingest" in message and "09:00" in message

    _tick(sensor_def, cursor=first.cursor)
    assert len(alerter.sent) == 1  # not re-sent on the next tick


def test_no_alert_before_the_deadline(tmp_path, monkeypatch):
    sensor_def, alerter = _setup(monkeypatch, _spec(tmp_path, expected_by="11:00"))
    _tick(sensor_def)
    assert alerter.sent == []


def test_no_alert_when_a_file_arrived_today(tmp_path, monkeypatch):
    _touch(tmp_path, "HOLDINGS_20260921.txt")
    sensor_def, alerter = _setup(monkeypatch, _spec(tmp_path, expected_by="09:00"))
    _tick(sensor_def)
    assert alerter.sent == []


def test_stale_file_from_a_prior_day_does_not_mask_a_missed_arrival(tmp_path, monkeypatch):
    yesterday = datetime(2026, 9, 20, 10, 0, tzinfo=timezone.utc)
    _touch(tmp_path, "HOLDINGS_20260920.txt", when=yesterday)
    sensor_def, alerter = _setup(monkeypatch, _spec(tmp_path, expected_by="09:00"))
    _tick(sensor_def)
    assert len(alerter.sent) == 1


def test_arrival_after_an_alert_resets_the_alerted_flag(tmp_path, monkeypatch):
    sensor_def, alerter = _setup(monkeypatch, _spec(tmp_path, expected_by="09:00"))
    first = _tick(sensor_def)
    assert json.loads(first.cursor)["alerted_date"] == "2026-09-21"

    _touch(tmp_path, "HOLDINGS_20260921.txt")
    second = _tick(sensor_def, cursor=first.cursor)
    cursor = json.loads(second.cursor)
    assert cursor["alerted_date"] is None
    assert cursor["last_arrival_date"] == "2026-09-21"


def test_sftp_location_does_not_crash_the_tick(tmp_path, monkeypatch):
    sensor_def, _ = _setup(monkeypatch, _spec(tmp_path, kind="sftp"))
    result = _tick(sensor_def)
    assert result.run_requests == []


def test_filename_timestamp_ordering_uses_the_name_date_not_server_timezone(tmp_path, monkeypatch):
    # Naive date parsed from the filename (order_by: filename_timestamp) must
    # count as "arrived today" for a file named for today, regardless of the
    # timezone the server happens to be in.
    data = _spec(tmp_path, expected_by="09:00")
    data.source.location.multi_file.order_by = "filename_timestamp"
    data.source.location.multi_file.filename_timestamp_format = "%Y%m%d"
    _touch(tmp_path, "HOLDINGS_20260921.txt")
    sensor_def, alerter = _setup(monkeypatch, data)
    _tick(sensor_def)
    assert alerter.sent == []


def test_files_that_do_not_match_the_pattern_are_called_out_not_silently_ignored(tmp_path, monkeypatch):
    _touch(tmp_path, "HOLDINGS_20260921.xlsx")
    sensor_def, _ = _setup(monkeypatch, _spec(tmp_path))
    context = build_sensor_context()
    warnings = []
    monkeypatch.setattr(type(context.log), "warning", lambda self, msg, *a, **k: warnings.append(msg), raising=False)
    result = sensor_def(context)
    assert result.run_requests == []
    assert any("HOLDINGS_20260921.xlsx" in w for w in warnings)
