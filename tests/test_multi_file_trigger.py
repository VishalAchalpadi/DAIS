import os
import time
from pathlib import Path

import yaml
from fastapi.testclient import TestClient

from dais.api.app import create_app
from dais.resilience.connectors.postgres_connector import PostgresConnector
from tests.conftest import _local_pg_creds, requires_local_postgres

pytestmark = requires_local_postgres

FIXTURE = Path(__file__).parent / "fixtures" / "valid_holdings_ingest.yaml"


def _real_connector_factory(spec):
    creds = _local_pg_creds()
    connection_params = dict(
        host=creds["host"], port=creds["port"], dbname=creds["dbname"], user=creds["user"], password=creds["password"]
    )
    connector = PostgresConnector(retry_cfg=spec.resilience.retry, **connection_params)
    return connector, connection_params


def _failing_then_real_connector_factory(fail_count: int):
    """A connector_factory that raises for the first `fail_count` calls
    (simulating a file whose run genuinely fails, not just quarantines),
    then behaves normally - used to test on_earlier_failure without
    needing to corrupt actual pipeline internals."""
    calls = {"n": 0}

    def factory(spec):
        calls["n"] += 1
        if calls["n"] <= fail_count:
            raise RuntimeError(f"simulated failure on call {calls['n']}")
        return _real_connector_factory(spec)

    return factory


def _fw_field(value: str, width: int) -> str:
    return value.ljust(width)


def _sample_line(account_id, security_id, as_of_date, quantity, market_value, currency) -> str:
    return (
        _fw_field(account_id, 12)
        + _fw_field(security_id, 12)
        + _fw_field(as_of_date, 8)
        + _fw_field(quantity, 15)
        + _fw_field(market_value, 15)
        + _fw_field(currency, 3)
    )


def _setup_specs_dir(tmp_path, schema: str, landing_dir: Path, *, multi_file: dict, stop_after="raw"):
    specs_dir = tmp_path / "specs"
    specs_dir.mkdir()
    data = yaml.safe_load(FIXTURE.read_text(encoding="utf-8"))
    data["raw"]["schema"] = schema
    data["stage"]["schema"] = schema
    data["gold"]["schema"] = schema
    data["monitoring"]["schema"] = schema
    data["execution"]["stop_after"] = stop_after
    data["control_gates"] = {
        "file_size": {"min_bytes": 1, "max_bytes": 1_000_000},
        "row_count": {"min_rows": 1, "max_rows": 1000},
        "on_fail": "quarantine_file",
    }
    data["source"]["location"] = {
        "kind": "local",
        "path": str(landing_dir),
        "file_pattern": "HOLDINGS_{date}.txt",
        "multi_file": multi_file,
    }
    (specs_dir / "holdings_ingest.yaml").write_text(yaml.dump(data), encoding="utf-8")
    return specs_dir


def _write_file(
    landing_dir: Path, name: str, account_id: str, mtime_offset: float | None = None, as_of_date: str = "20260101"
) -> Path:
    path = landing_dir / name
    path.write_text(
        _sample_line(account_id, "SEC01", as_of_date, "100.0000", "5000.00", "USD") + "\n",
        encoding="utf-8",
    )
    if mtime_offset is not None:
        t = time.time() + mtime_offset
        os.utime(path, (t, t))
    return path


def test_all_mode_processes_every_file_in_order(pg_connector, test_schema, tmp_path):
    landing = tmp_path / "landing"
    landing.mkdir()
    _write_file(landing, "HOLDINGS_20260101.txt", "ACC01", mtime_offset=-20)
    _write_file(landing, "HOLDINGS_20260102.txt", "ACC02", mtime_offset=-10)
    _write_file(landing, "HOLDINGS_20260103.txt", "ACC03", mtime_offset=0)

    specs_dir = _setup_specs_dir(
        tmp_path, test_schema, landing,
        multi_file={"mode": "all", "order_by": "arrival_time"},
    )
    app = create_app(specs_dir=specs_dir, api_key="test-key", connector_factory=_real_connector_factory)

    with TestClient(app) as client:
        resp = client.post("/pipelines/holdings_ingest/run", json={}, headers={"X-API-Key": "test-key"})
        assert resp.status_code == 202
        body = resp.json()
        run_ids = body["run_ids"]
        assert len(run_ids) == 3

        statuses = [
            client.get(f"/pipelines/runs/{rid}/status", headers={"X-API-Key": "test-key"}).json()
            for rid in run_ids
        ]
        assert [s["status"] for s in statuses] == ["succeeded", "succeeded", "succeeded"]


def test_latest_mode_dispatches_single_run_for_newest_file(pg_connector, test_schema, tmp_path):
    landing = tmp_path / "landing"
    landing.mkdir()
    _write_file(landing, "HOLDINGS_20260101.txt", "ACC01", mtime_offset=-20)
    _write_file(landing, "HOLDINGS_20260102.txt", "ACC02", mtime_offset=0)

    specs_dir = _setup_specs_dir(tmp_path, test_schema, landing, multi_file={"mode": "latest"})
    app = create_app(specs_dir=specs_dir, api_key="test-key", connector_factory=_real_connector_factory)

    with TestClient(app) as client:
        resp = client.post("/pipelines/holdings_ingest/run", json={}, headers={"X-API-Key": "test-key"})
        assert resp.status_code == 202
        body = resp.json()
        assert "run_id" in body  # RunResponse, not RunBatchResponse
        status_resp = client.get(f"/pipelines/runs/{body['run_id']}/status", headers={"X-API-Key": "test-key"})
        assert status_resp.json()["status"] == "succeeded"


def test_on_earlier_failure_stop_skips_remaining_files(pg_connector, test_schema, tmp_path):
    landing = tmp_path / "landing"
    landing.mkdir()
    _write_file(landing, "HOLDINGS_20260101.txt", "ACC01", mtime_offset=-10)
    _write_file(landing, "HOLDINGS_20260102.txt", "ACC02", mtime_offset=0)

    specs_dir = _setup_specs_dir(
        tmp_path, test_schema, landing,
        multi_file={"mode": "all", "order_by": "arrival_time", "on_earlier_failure": "stop"},
    )
    app = create_app(
        specs_dir=specs_dir,
        api_key="test-key",
        connector_factory=_failing_then_real_connector_factory(fail_count=1),
    )

    with TestClient(app) as client:
        resp = client.post("/pipelines/holdings_ingest/run", json={}, headers={"X-API-Key": "test-key"})
        run_ids = resp.json()["run_ids"]
        assert len(run_ids) == 2

        first_status = client.get(f"/pipelines/runs/{run_ids[0]}/status", headers={"X-API-Key": "test-key"}).json()
        second_status = client.get(f"/pipelines/runs/{run_ids[1]}/status", headers={"X-API-Key": "test-key"}).json()
        assert first_status["status"] == "failed"
        assert second_status["status"] == "skipped"


def test_on_earlier_failure_continue_still_runs_later_files(pg_connector, test_schema, tmp_path):
    landing = tmp_path / "landing"
    landing.mkdir()
    _write_file(landing, "HOLDINGS_20260101.txt", "ACC01", mtime_offset=-10)
    _write_file(landing, "HOLDINGS_20260102.txt", "ACC02", mtime_offset=0)

    specs_dir = _setup_specs_dir(
        tmp_path, test_schema, landing,
        multi_file={"mode": "all", "order_by": "arrival_time", "on_earlier_failure": "continue"},
    )
    app = create_app(
        specs_dir=specs_dir,
        api_key="test-key",
        connector_factory=_failing_then_real_connector_factory(fail_count=1),
    )

    with TestClient(app) as client:
        resp = client.post("/pipelines/holdings_ingest/run", json={}, headers={"X-API-Key": "test-key"})
        run_ids = resp.json()["run_ids"]

        first_status = client.get(f"/pipelines/runs/{run_ids[0]}/status", headers={"X-API-Key": "test-key"}).json()
        second_status = client.get(f"/pipelines/runs/{run_ids[1]}/status", headers={"X-API-Key": "test-key"}).json()
        assert first_status["status"] == "failed"
        assert second_status["status"] == "succeeded"


def test_quarantined_earlier_file_does_not_halt_batch_even_under_stop(pg_connector, test_schema, tmp_path):
    landing = tmp_path / "landing"
    landing.mkdir()
    # An unparseable as_of_date under integrity_mode "strict" quarantines
    # the WHOLE file (run status "quarantined") without raising - this is
    # the one case that actually produces a "quarantined" run status
    # rather than "succeeded" (row_level/group_level quarantining still
    # lands the good rows and reports "succeeded" - see pipeline.py's
    # outcome.file_quarantined check, only true under "strict").
    _write_file(landing, "HOLDINGS_20260101.txt", "ACC01", mtime_offset=-10, as_of_date="notadat8")
    _write_file(landing, "HOLDINGS_20260102.txt", "ACC02", mtime_offset=0)

    specs_dir = _setup_specs_dir(
        tmp_path, test_schema, landing, stop_after="stage",
        multi_file={"mode": "all", "order_by": "arrival_time", "on_earlier_failure": "stop"},
    )
    quarantine_dir = tmp_path / "quarantine"
    quarantine_dir.mkdir()
    data = yaml.safe_load((specs_dir / "holdings_ingest.yaml").read_text(encoding="utf-8"))
    data["quality"]["integrity_mode"] = "strict"
    data["quality"]["quarantine"]["kind"] = "local"
    data["quality"]["quarantine"]["location"] = str(quarantine_dir)
    (specs_dir / "holdings_ingest.yaml").write_text(yaml.dump(data), encoding="utf-8")

    app = create_app(specs_dir=specs_dir, api_key="test-key", connector_factory=_real_connector_factory)

    with TestClient(app) as client:
        resp = client.post("/pipelines/holdings_ingest/run", json={}, headers={"X-API-Key": "test-key"})
        run_ids = resp.json()["run_ids"]

        first_status = client.get(f"/pipelines/runs/{run_ids[0]}/status", headers={"X-API-Key": "test-key"}).json()
        second_status = client.get(f"/pipelines/runs/{run_ids[1]}/status", headers={"X-API-Key": "test-key"}).json()
        assert first_status["status"] == "quarantined"
        assert second_status["status"] == "succeeded"


def test_missing_multi_file_config_and_missing_file_path_returns_400(pg_connector, test_schema, tmp_path):
    landing = tmp_path / "landing"
    landing.mkdir()
    specs_dir = tmp_path / "specs"
    specs_dir.mkdir()
    data = yaml.safe_load(FIXTURE.read_text(encoding="utf-8"))
    data["raw"]["schema"] = test_schema
    data["stage"]["schema"] = test_schema
    data["gold"]["schema"] = test_schema
    data["monitoring"]["schema"] = test_schema
    (specs_dir / "holdings_ingest.yaml").write_text(yaml.dump(data), encoding="utf-8")

    app = create_app(specs_dir=specs_dir, api_key="test-key", connector_factory=_real_connector_factory)
    with TestClient(app) as client:
        resp = client.post("/pipelines/holdings_ingest/run", json={}, headers={"X-API-Key": "test-key"})
        assert resp.status_code == 400
        assert "multi_file" in resp.json()["detail"]
