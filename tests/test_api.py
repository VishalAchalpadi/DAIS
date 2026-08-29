from pathlib import Path

import yaml
from fastapi.testclient import TestClient

from dais.api.app import create_app
from dais.resilience.connectors.postgres_connector import PostgresConnector
from tests.conftest import _local_pg_creds, requires_local_postgres

pytestmark = requires_local_postgres

FIXTURE = Path(__file__).parent / "fixtures" / "valid_holdings_ingest.yaml"


def _test_connector_factory(spec):
    """Bypasses the env-based SecretsProvider switch (Vault by default) -
    tests resolve local creds the same way conftest.py's pg_connector
    fixture does, via the real HardcodedSecretsProvider."""
    creds = _local_pg_creds()
    connection_params = dict(
        host=creds["host"], port=creds["port"], dbname=creds["dbname"], user=creds["user"], password=creds["password"]
    )
    connector = PostgresConnector(retry_cfg=spec.resilience.retry, **connection_params)
    return connector, connection_params


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


def _setup_specs_dir(tmp_path, schema: str, stop_after="raw"):
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
    (specs_dir / "holdings_ingest.yaml").write_text(yaml.dump(data), encoding="utf-8")
    return specs_dir


def _sample_file(tmp_path) -> Path:
    path = tmp_path / "HOLDINGS_20260101.txt"
    path.write_text(
        _sample_line("ACC01", "SEC01", "20260101", "100.0000", "5000.00", "USD") + "\n",
        encoding="utf-8",
    )
    return path


def test_run_and_poll_status_reaches_terminal_state(pg_connector, test_schema, tmp_path):
    specs_dir = _setup_specs_dir(tmp_path, test_schema)
    app = create_app(specs_dir=specs_dir, api_key="test-key", connector_factory=_test_connector_factory)
    file_path = _sample_file(tmp_path)

    with TestClient(app) as client:
        resp = client.post(
            "/pipelines/holdings_ingest/run",
            json={"file_path": str(file_path)},
            headers={"X-API-Key": "test-key"},
        )
        assert resp.status_code == 202
        run_id = resp.json()["run_id"]

        status_resp = client.get(f"/pipelines/runs/{run_id}/status", headers={"X-API-Key": "test-key"})
        assert status_resp.status_code == 200
        body = status_resp.json()
        assert body["status"] == "succeeded"
        assert body["layer_reached"] == "raw"


def test_duplicate_trigger_returns_same_run_id_and_does_not_reprocess(pg_connector, test_schema, tmp_path):
    specs_dir = _setup_specs_dir(tmp_path, test_schema)
    app = create_app(specs_dir=specs_dir, api_key="test-key", connector_factory=_test_connector_factory)
    file_path = _sample_file(tmp_path)

    with TestClient(app) as client:
        first = client.post(
            "/pipelines/holdings_ingest/run",
            json={"file_path": str(file_path)},
            headers={"X-API-Key": "test-key"},
        )
        second = client.post(
            "/pipelines/holdings_ingest/run",
            json={"file_path": str(file_path)},
            headers={"X-API-Key": "test-key"},
        )

    assert first.json()["run_id"] == second.json()["run_id"]
    count = pg_connector.fetch_all(f'SELECT COUNT(*) FROM "{test_schema}"."holdings_raw"')
    assert count[0][0] == 1


def test_missing_api_key_is_rejected(tmp_path):
    specs_dir = tmp_path / "specs"
    specs_dir.mkdir()
    app = create_app(specs_dir=specs_dir, api_key="test-key")

    with TestClient(app) as client:
        resp = client.post("/pipelines/holdings_ingest/run", json={"file_path": "x.txt"})

    assert resp.status_code == 401


def test_wrong_api_key_is_rejected(tmp_path):
    specs_dir = tmp_path / "specs"
    specs_dir.mkdir()
    app = create_app(specs_dir=specs_dir, api_key="test-key")

    with TestClient(app) as client:
        resp = client.get("/pipelines/runs/nonexistent/status", headers={"X-API-Key": "wrong"})

    assert resp.status_code == 401


def test_unknown_spec_returns_404(tmp_path):
    specs_dir = tmp_path / "specs"
    specs_dir.mkdir()
    app = create_app(specs_dir=specs_dir, api_key="test-key")

    with TestClient(app) as client:
        resp = client.post(
            "/pipelines/does_not_exist/run",
            json={"file_path": "x.txt"},
            headers={"X-API-Key": "test-key"},
        )

    assert resp.status_code == 404


def test_unknown_run_id_returns_404(tmp_path):
    specs_dir = tmp_path / "specs"
    specs_dir.mkdir()
    app = create_app(specs_dir=specs_dir, api_key="test-key")

    with TestClient(app) as client:
        resp = client.get("/pipelines/runs/nonexistent/status", headers={"X-API-Key": "test-key"})

    assert resp.status_code == 404


def test_business_process_status_endpoint(pg_connector, test_schema, tmp_path):
    specs_dir = _setup_specs_dir(tmp_path, test_schema)

    # minimal second member spec, pointed at the same test schema
    data = yaml.safe_load((Path(__file__).parent.parent / "specs" / "benchmark_ingest.yaml").read_text(encoding="utf-8"))
    data["raw"]["schema"] = test_schema
    data["stage"]["schema"] = test_schema
    data["monitoring"]["schema"] = test_schema
    (specs_dir / "benchmark_ingest.yaml").write_text(yaml.dump(data), encoding="utf-8")

    bp_dir = tmp_path / "business_processes"
    bp_dir.mkdir()
    (bp_dir / "morning_holdings_recon.yaml").write_text(
        yaml.dump(
            {
                "process_name": "morning_holdings_recon",
                "members": [
                    {"pipeline_name": "holdings_ingest", "success_layer": "raw"},
                    {"pipeline_name": "benchmark_ingest", "success_layer": "stage"},
                ],
                "sla": {"complete_by": "23:59", "timezone": "America/New_York"},
            }
        ),
        encoding="utf-8",
    )

    app = create_app(
        specs_dir=specs_dir,
        business_processes_dir=bp_dir,
        api_key="test-key",
        connector_factory=_test_connector_factory,
    )

    with TestClient(app) as client:
        resp = client.get(
            "/business-processes/morning_holdings_recon/status", headers={"X-API-Key": "test-key"}
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["process_name"] == "morning_holdings_recon"
    assert body["state"] == "pending"
    assert len(body["members"]) == 2
