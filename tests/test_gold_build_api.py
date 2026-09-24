"""POST /gold-builds/{name}/run + GET /gold-builds/runs/{run_id}/status -
the API layer for a gold_builds/*.yaml build (Sketch: "why isn't the gold
dbt run wrapped in a FastAPI endpoint" -> this adds one). A new,
independent registry and background worker (GoldBuildRunRegistry,
execute_gold_build_background) - the existing /pipelines/* routes,
RunRegistry, and checksum/layer_reached semantics are untouched; this
follows the SAME real-dbt-run pattern test_gold_build.py's
test_run_gold_build_materializes_real_output already uses, just invoked
over HTTP instead of calling run_gold_build() directly."""
from __future__ import annotations

from pathlib import Path

import yaml
from fastapi.testclient import TestClient

from dais.api.app import create_app
from tests.conftest import requires_local_postgres

pytestmark = requires_local_postgres

DBT_PROJECT = str(Path(__file__).parent.parent / "dbt")
EXAMPLE_SPEC = Path(__file__).parent.parent / "gold_builds" / "portfolio_summary_gold.yaml"


def _setup_gold_builds_dir(tmp_path, schema: str) -> Path:
    gold_builds_dir = tmp_path / "gold_builds"
    gold_builds_dir.mkdir()
    data = yaml.safe_load(EXAMPLE_SPEC.read_text(encoding="utf-8"))
    data["dbt"]["project_dir"] = DBT_PROJECT
    data["target"]["schema"] = schema
    (gold_builds_dir / "portfolio_summary_gold.yaml").write_text(yaml.dump(data), encoding="utf-8")
    return gold_builds_dir


def test_run_and_poll_status_reaches_succeeded(pg_connector, test_schema, tmp_path, monkeypatch):
    monkeypatch.setenv("SECRETS_PROVIDER", "hardcoded")
    gold_builds_dir = _setup_gold_builds_dir(tmp_path, test_schema)
    app = create_app(gold_builds_dir=gold_builds_dir, api_key="test-key")

    with TestClient(app) as client:
        resp = client.post(
            "/gold-builds/portfolio_summary_gold/run", headers={"X-API-Key": "test-key"}
        )
        assert resp.status_code == 202
        run_id = resp.json()["run_id"]

        status_resp = client.get(f"/gold-builds/runs/{run_id}/status", headers={"X-API-Key": "test-key"})

    assert status_resp.status_code == 200
    body = status_resp.json()
    assert body["gold_build_name"] == "portfolio_summary_gold"
    assert body["status"] == "succeeded", body["error"]

    rows = pg_connector.fetch_all(
        f'SELECT column_name FROM information_schema.columns '
        f"WHERE table_schema = '{test_schema}' AND table_name = 'portfolio_summary_gold'"
    )
    assert rows  # the real dbt run actually materialized the table


def test_unknown_gold_build_returns_404(tmp_path):
    app = create_app(gold_builds_dir=tmp_path / "gold_builds", api_key="test-key")
    with TestClient(app) as client:
        resp = client.post("/gold-builds/does_not_exist/run", headers={"X-API-Key": "test-key"})
    assert resp.status_code == 404


def test_unknown_run_id_returns_404(tmp_path):
    app = create_app(gold_builds_dir=tmp_path / "gold_builds", api_key="test-key")
    with TestClient(app) as client:
        resp = client.get("/gold-builds/runs/not-a-real-run-id/status", headers={"X-API-Key": "test-key"})
    assert resp.status_code == 404


def test_list_gold_builds(tmp_path):
    gold_builds_dir = tmp_path / "gold_builds"
    gold_builds_dir.mkdir()
    (gold_builds_dir / "fx_rates_gold.yaml").write_text("placeholder", encoding="utf-8")
    (gold_builds_dir / "regional_sales_gold.yaml").write_text("placeholder", encoding="utf-8")
    app = create_app(gold_builds_dir=gold_builds_dir, api_key="test-key")

    with TestClient(app) as client:
        resp = client.get("/gold-builds", headers={"X-API-Key": "test-key"})

    assert resp.status_code == 200
    assert resp.json() == ["fx_rates_gold", "regional_sales_gold"]


def test_missing_api_key_is_rejected(tmp_path):
    app = create_app(gold_builds_dir=tmp_path / "gold_builds", api_key="test-key")
    with TestClient(app) as client:
        resp = client.post("/gold-builds/portfolio_summary_gold/run")
    assert resp.status_code == 401
