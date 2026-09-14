"""Regression tests for the spec-editor API endpoints (GET /spec-schema,
GET /pipelines, GET/POST /pipelines/{spec_name}/spec, and the two
/ui/spec-editor... HTML routes) backing src/dais/api/spec_editor.py.

None of these routes touch a database, so unlike test_api.py this module
does not require a local Postgres instance.
"""
from pathlib import Path

import yaml
from fastapi.testclient import TestClient

from dais.api.app import create_app

FIXTURE = Path(__file__).parent / "fixtures" / "valid_holdings_ingest.yaml"
API_KEY = "test-key"


def _make_client(tmp_path) -> TestClient:
    specs_dir = tmp_path / "specs"
    specs_dir.mkdir()
    (specs_dir / "holdings_ingest.yaml").write_text(FIXTURE.read_text(encoding="utf-8"), encoding="utf-8")
    (specs_dir / "holdings_ingest.dq_suggestions.yaml").write_text("suggestions: []\n", encoding="utf-8")
    app = create_app(specs_dir=specs_dir, api_key=API_KEY)
    return TestClient(app)


def _headers() -> dict:
    return {"X-API-Key": API_KEY}


def test_spec_schema_requires_api_key():
    client = _make_client_no_specs()
    resp = client.get("/spec-schema")
    assert resp.status_code == 401


def _make_client_no_specs():
    import tempfile

    tmp = Path(tempfile.mkdtemp())
    specs_dir = tmp / "specs"
    specs_dir.mkdir()
    return TestClient(create_app(specs_dir=specs_dir, api_key=API_KEY))


def test_spec_schema_includes_pydantic_defs_and_check_name_metadata(tmp_path):
    client = _make_client(tmp_path)
    resp = client.get("/spec-schema", headers=_headers())
    assert resp.status_code == 200
    schema = resp.json()

    # Every Literal[...] field in models.py becomes a dropdown by reading
    # this - spot-check one to make sure the real pydantic schema (not a
    # hand-copied one) is what's served.
    assert schema["$defs"]["ExecutionConfig"]["properties"]["stop_after"]["enum"] == ["raw", "stage", "gold"]

    # QualityRule.checks (Union[str, dict]) can't be expressed as a plain
    # enum in JSON Schema - the UI needs these names shipped separately.
    assert schema["x_dais_check_names"]["bare"] == sorted(
        {"not_null", "non_empty", "valid_date", "is_numeric"}
    )
    assert schema["x_dais_check_names"]["comparison_ops"] == sorted(
        {"greater_than_or_equal", "greater_than", "less_than_or_equal", "less_than"}
    )


def test_list_pipelines_excludes_dq_suggestions_files(tmp_path):
    client = _make_client(tmp_path)
    resp = client.get("/pipelines", headers=_headers())
    assert resp.status_code == 200
    assert resp.json() == ["holdings_ingest"]


def test_get_pipeline_spec_returns_yaml_text_and_parsed_data(tmp_path):
    client = _make_client(tmp_path)
    resp = client.get("/pipelines/holdings_ingest/spec", headers=_headers())
    assert resp.status_code == 200
    body = resp.json()
    assert body["data"]["pipeline_name"] == "holdings_ingest"
    assert "gold:" in body["yaml"]


def test_get_pipeline_spec_404_for_unknown_spec(tmp_path):
    client = _make_client(tmp_path)
    resp = client.get("/pipelines/does_not_exist/spec", headers=_headers())
    assert resp.status_code == 404


def test_save_pipeline_spec_round_trips_unmodified_data(tmp_path):
    client = _make_client(tmp_path)
    data = client.get("/pipelines/holdings_ingest/spec", headers=_headers()).json()["data"]

    resp = client.post("/pipelines/holdings_ingest/spec?overwrite=true", json=data, headers=_headers())
    assert resp.status_code == 200
    body = resp.json()
    assert body["saved"] is True

    # What actually landed on disk must itself still validate, exactly as
    # it would if loaded by the CLI/pipeline runner.
    on_disk = yaml.safe_load(Path(body["path"]).read_text(encoding="utf-8"))
    assert on_disk["pipeline_name"] == "holdings_ingest"
    assert on_disk["gold"]["schema"] == "core"
    assert on_disk["quality"]["rules"][0]["column"] == "account_id"


def test_save_pipeline_spec_applies_edits(tmp_path):
    client = _make_client(tmp_path)
    data = client.get("/pipelines/holdings_ingest/spec", headers=_headers()).json()["data"]
    data["owner"] = "new-owner-team"

    resp = client.post("/pipelines/holdings_ingest/spec?overwrite=true", json=data, headers=_headers())
    assert resp.status_code == 200

    reread = client.get("/pipelines/holdings_ingest/spec", headers=_headers()).json()["data"]
    assert reread["owner"] == "new-owner-team"


def test_save_pipeline_spec_rejects_invalid_data_with_field_level_detail(tmp_path):
    client = _make_client(tmp_path)
    data = client.get("/pipelines/holdings_ingest/spec", headers=_headers()).json()["data"]
    del data["lineage"]  # required top-level block

    resp = client.post("/pipelines/holdings_ingest/spec?overwrite=true", json=data, headers=_headers())
    assert resp.status_code == 422
    errors = resp.json()["detail"]
    assert any(e["loc"] == ["lineage"] for e in errors)

    # nothing should have been written over the previously-valid file
    on_disk = yaml.safe_load((tmp_path / "specs" / "holdings_ingest.yaml").read_text(encoding="utf-8"))
    assert "lineage" in on_disk


def test_save_pipeline_spec_rejects_cross_field_rule_violation(tmp_path):
    """group_level integrity_mode requires quarantine.group_by - a rule the
    JSON Schema itself can't express, so this must be caught server-side."""
    client = _make_client(tmp_path)
    data = client.get("/pipelines/holdings_ingest/spec", headers=_headers()).json()["data"]
    data["quality"]["integrity_mode"] = "group_level"

    resp = client.post("/pipelines/holdings_ingest/spec?overwrite=true", json=data, headers=_headers())
    assert resp.status_code == 422
    assert "group_by" in str(resp.json()["detail"])


def test_save_pipeline_spec_rejects_pipeline_name_mismatch(tmp_path):
    client = _make_client(tmp_path)
    data = client.get("/pipelines/holdings_ingest/spec", headers=_headers()).json()["data"]

    resp = client.post("/pipelines/some_other_name/spec?overwrite=true", json=data, headers=_headers())
    assert resp.status_code == 400


def test_save_pipeline_spec_conflicts_without_overwrite_on_existing_file(tmp_path):
    client = _make_client(tmp_path)
    data = client.get("/pipelines/holdings_ingest/spec", headers=_headers()).json()["data"]

    resp = client.post("/pipelines/holdings_ingest/spec?overwrite=false", json=data, headers=_headers())
    assert resp.status_code == 409


def test_save_pipeline_spec_allows_new_file_without_overwrite(tmp_path):
    client = _make_client(tmp_path)
    data = client.get("/pipelines/holdings_ingest/spec", headers=_headers()).json()["data"]
    data["pipeline_name"] = "brand_new_pipeline"

    resp = client.post("/pipelines/brand_new_pipeline/spec?overwrite=false", json=data, headers=_headers())
    assert resp.status_code == 200
    assert (tmp_path / "specs" / "brand_new_pipeline.yaml").is_file()


def test_spec_editor_ui_serves_html_for_new_and_existing_spec(tmp_path):
    client = _make_client(tmp_path)

    new_resp = client.get("/ui/spec-editor")
    assert new_resp.status_code == 200
    assert "New pipeline spec" in new_resp.text or "spec editor" in new_resp.text.lower()

    edit_resp = client.get("/ui/spec-editor/holdings_ingest")
    assert edit_resp.status_code == 200
    assert "holdings_ingest" in edit_resp.text
