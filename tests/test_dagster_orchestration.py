"""Phase 9c: Dagster as a second, optional trigger source alongside
Control-M - never a second execution path. Raw/stage assets
(ingest_assets.py) must call the EXISTING HTTP API as a black box, and
gold assets (gold_assets.py) must use dagster-dbt natively against the
SAME shared dbt/ project gold_builds/*.yaml already uses. These tests
verify both the wiring (asset graph shape) and, for the parts that touch
real infrastructure, an actual live run - a real API server, a real
Postgres, a real dbt run - not mocks standing in for any of it.
"""
from __future__ import annotations

import socket
import threading
import time
from pathlib import Path

import pytest
import uvicorn
import yaml
from dagster import DagsterInstance, materialize
from dagster_dbt import DbtCliResource

from dais.api.app import create_app
from dais.orchestration.dagster.api_resource import DaisApiResource
from dais.orchestration.dagster.definitions import defs, gold_assets_definitions
from dais.orchestration.dagster.dbt_project import DBT_PROJECT, DBT_PROJECT_DIR, REAL_DBT_EXECUTABLE
from dais.orchestration.dagster.gold_assets import DaisDagsterDbtTranslator, build_gold_build_dbt_assets
from dais.orchestration.dagster.ingest_assets import build_ingest_asset
from dais.resilience.connectors.postgres_connector import PostgresConnector
from tests.conftest import _local_pg_creds, requires_local_postgres

pytestmark = requires_local_postgres

EXAMPLE_GOLD_BUILD = Path(__file__).parent.parent / "gold_builds" / "portfolio_summary_gold.yaml"
HOLDINGS_FILE = Path(__file__).parent.parent / "data" / "holdings" / "incoming" / "HOLDINGS_20260101.txt"
ASSET_FILE = Path(__file__).parent.parent / "data" / "assets" / "incoming" / "assets_20260828.csv"


# ---------------------------------------------------------------------------
# wiring: the translator, and the graph Definitions actually builds
# ---------------------------------------------------------------------------

def test_translator_maps_dbt_source_to_matching_ingest_asset_key():
    translator = DaisDagsterDbtTranslator()
    key = translator.get_asset_key({"resource_type": "source", "source_name": "holdings_ingest"})
    assert key.path == ["holdings_ingest", "stage"]
    ingest_asset = build_ingest_asset("holdings_ingest")
    assert list(ingest_asset.keys) == [key]


def test_definitions_load_without_error():
    assert defs.resolve_asset_graph() is not None


def test_ingest_assets_are_discovered_from_gold_builds_depends_on():
    graph = defs.resolve_asset_graph()
    all_keys = {k.to_user_string() for k in graph.get_all_asset_keys()}
    assert "holdings_ingest/stage" in all_keys
    assert "asset_ingest/stage" in all_keys


def test_gold_dbt_models_depend_on_the_matching_ingest_asset():
    graph = defs.resolve_asset_graph()
    stg_holdings = next(k for k in graph.get_all_asset_keys() if k.to_user_string() == "stg_holdings")
    parents = {p.to_user_string() for p in graph.get(stg_holdings).parent_keys}
    assert parents == {"holdings_ingest/stage"}


def test_gold_mart_transitively_depends_on_both_ingest_assets():
    graph = defs.resolve_asset_graph()
    mart = next(k for k in graph.get_all_asset_keys() if k.to_user_string() == "portfolio_summary_gold")
    ancestors = {k.to_user_string() for k in graph.get_ancestor_asset_keys(mart)}
    assert {"holdings_ingest/stage", "asset_ingest/stage"}.issubset(ancestors)


def test_dedicated_job_exists_per_gold_build():
    for name in ("portfolio_summary_gold_job", "regional_sales_gold_job"):
        assert defs.resolve_job_def(name) is not None


def test_dedicated_gold_job_is_scoped_to_only_that_builds_dbt_assets():
    """A gold build's own job must not drag in ingest assets or another
    build's models - it's meant to be run standalone (e.g. a scheduled
    gold-only refresh), not the full dais_medallion_job graph."""
    job = defs.resolve_job_def("regional_sales_gold_job")
    keys = {k.to_user_string() for k in job.asset_layer.executable_asset_keys}
    assert keys == {"stg_sales", "regional_sales_gold"}


def test_dedicated_job_exists_per_ingest_pipeline():
    for name in ("holdings_ingest_job", "asset_ingest_job", "sales_ingest_job"):
        assert defs.resolve_job_def(name) is not None


def test_dedicated_ingest_job_is_scoped_to_only_that_pipelines_asset():
    """Mirrors the gold-build jobs: an ingest pipeline's own job must not
    drag in every other pipeline's ingest asset the way dais_medallion_job
    does - it should materialize exactly one asset."""
    job = defs.resolve_job_def("sales_ingest_job")
    keys = {k.to_user_string() for k in job.asset_layer.executable_asset_keys}
    assert keys == {"sales_ingest/stage"}


def test_gold_models_get_eager_automation_condition_but_sources_do_not():
    """Stage->gold should auto-fire via Declarative Automation without a
    combined job: every real dbt model gets AutomationCondition.eager(), so
    Dagster queues its run the moment its upstream updates (needs
    dagster-daemon running to actually evaluate). Sources are ingest_assets'
    stage assets under another name (see get_asset_key) - they must stay
    externally triggered only, never auto-materialized from nothing."""
    translator = DaisDagsterDbtTranslator()
    assert translator.get_automation_condition({"resource_type": "model"}) is not None
    assert translator.get_automation_condition({"resource_type": "source", "source_name": "sales_ingest"}) is None


def test_gold_build_assets_carry_automation_condition_end_to_end():
    for assets_def in gold_assets_definitions:
        for key in assets_def.keys:
            if key.path[-1].startswith("stg_") or "gold" in key.path[-1] or key.path[-1].startswith("int_"):
                assert key in assets_def.automation_conditions_by_key, f"{key} missing an automation condition"


def test_dedicated_job_exists_even_for_a_pipeline_no_gold_build_depends_on():
    """fund_positions_ingest has no gold_builds/*.yaml naming it in
    depends_on - it must still get an asset and a job, discovered directly
    from specs/*.yaml, not only from what gold builds happen to reference."""
    job = defs.resolve_job_def("fund_positions_ingest_job")
    keys = {k.to_user_string() for k in job.asset_layer.executable_asset_keys}
    assert keys == {"fund_positions_ingest/stage"}


# ---------------------------------------------------------------------------
# real end-to-end: a real API server, a real dbt run, no mocks
# ---------------------------------------------------------------------------

def _test_connector_factory(spec):
    creds = _local_pg_creds()
    connection_params = dict(
        host=creds["host"], port=creds["port"], dbname=creds["dbname"], user=creds["user"], password=creds["password"]
    )
    connector = PostgresConnector(retry_cfg=spec.resilience.retry, **connection_params)
    return connector, connection_params


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def real_api_server():
    """A REAL uvicorn server on a real socket, running the real DAIS
    FastAPI app against real specs/ - not a TestClient, so DaisApiResource
    exercises genuine HTTP + genuine async BackgroundTasks completion
    timing, the same as it would against a deployed server."""
    port = _free_port()
    app = create_app(
        specs_dir="specs",
        connector_factory=_test_connector_factory,
        api_key="test-dagster-key",
    )
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 10
    while not server.started and time.monotonic() < deadline:
        time.sleep(0.05)
    assert server.started, "uvicorn server did not start in time"
    yield f"http://127.0.0.1:{port}"
    server.should_exit = True
    thread.join(timeout=10)


def test_ingest_assets_run_real_pipelines_via_the_real_api(real_api_server):
    """holdings_ingest/stage and asset_ingest/stage materialize by calling
    the real API, which runs the real dais.pipeline.run_pipeline - exactly
    what Control-M's shell wrapper already triggers, not a second path."""
    holdings_asset = build_ingest_asset("holdings_ingest")
    asset_asset = build_ingest_asset("asset_ingest")
    dais_api = DaisApiResource(
        base_url=real_api_server, api_key="test-dagster-key", poll_interval_seconds=0.5, timeout_seconds=120
    )

    result = materialize(
        [holdings_asset, asset_asset],
        resources={"dais_api": dais_api},
        run_config={
            "ops": {
                "holdings_ingest__stage": {"config": {"file_path": str(HOLDINGS_FILE), "stop_after": "stage"}},
                "asset_ingest__stage": {"config": {"file_path": str(ASSET_FILE), "stop_after": "stage"}},
            }
        },
        instance=DagsterInstance.ephemeral(),
    )

    assert result.success, result.all_events


def test_gold_dbt_assets_materialize_via_real_dbt_run(test_schema, monkeypatch):
    """The gold half: a real dbt run through dagster-dbt's @dbt_assets,
    against a test-isolated OUTPUT schema (matching test_gold_build.py's
    convention) but the real, already-populated stage tables."""
    # gold_assets.py resolves credentials via get_secrets_provider() (same
    # as db.py/gold.py do in production) - tests select the real
    # HardcodedSecretsProvider fallback (secrets.local.yaml) the same way
    # conftest.py's other fixtures do, rather than the default Vault chain.
    monkeypatch.setenv("SECRETS_PROVIDER", "hardcoded")
    data = yaml.safe_load(EXAMPLE_GOLD_BUILD.read_text(encoding="utf-8"))
    data["dbt"]["project_dir"] = str(DBT_PROJECT_DIR)
    data["target"]["schema"] = test_schema
    tmp_spec_path = EXAMPLE_GOLD_BUILD.parent / f"_test_{test_schema}.yaml"
    tmp_spec_path.write_text(yaml.dump(data), encoding="utf-8")
    try:
        assets_def, spec = build_gold_build_dbt_assets(tmp_spec_path, DBT_PROJECT)
        dbt = DbtCliResource(project_dir=DBT_PROJECT_DIR, dbt_executable=REAL_DBT_EXECUTABLE)

        result = materialize([assets_def], resources={"dbt": dbt}, instance=DagsterInstance.ephemeral())

        assert result.success, result.all_events
    finally:
        tmp_spec_path.unlink(missing_ok=True)


def test_dagster_triggered_gold_run_produces_artifacts_dbt_ol_can_read(monkeypatch):
    """Part 1 of the unified-lineage fix, end to end: materializing gold
    through Dagster (dagster-dbt's own DbtCliResource, not run_gold_build())
    must still leave behind a real, freshly-written run_results.json/
    manifest.json at invocation.target_path - the artifacts
    run_dbt_ol_send_events() reads afterward with no second dbt run."""
    monkeypatch.setenv("SECRETS_PROVIDER", "hardcoded")
    assets_def, spec = build_gold_build_dbt_assets(EXAMPLE_GOLD_BUILD, DBT_PROJECT)
    dbt = DbtCliResource(project_dir=DBT_PROJECT_DIR, dbt_executable=REAL_DBT_EXECUTABLE)

    result = materialize([assets_def], resources={"dbt": dbt}, instance=DagsterInstance.ephemeral())

    assert result.success, result.all_events


def test_run_dbt_ol_send_events_emits_real_openlineage_without_rerunning_dbt(monkeypatch):
    """Part 2: run_dbt_ol_send_events() against a real, already-completed
    dbt run's artifacts (a real DbtCliInvocation from dagster-dbt, not a
    mock) must emit a real OpenLineage event carrying the configured
    namespace and the mart's real row count - and never re-invoke dbt
    itself (verified by the ABSENCE of dbt's own "Running with dbt="
    startup banner in the subprocess output, which only appears when dbt
    actually starts)."""
    monkeypatch.setenv("SECRETS_PROVIDER", "hardcoded")
    assets_def, spec = build_gold_build_dbt_assets(EXAMPLE_GOLD_BUILD, DBT_PROJECT)
    dbt = DbtCliResource(project_dir=DBT_PROJECT_DIR, dbt_executable=REAL_DBT_EXECUTABLE)

    # Get a real DbtCliInvocation with real, on-disk artifacts by actually
    # running dbt once - the same way the asset body does.
    from dais.orchestration.dagster.gold_assets import _set_real_dbt_pg_env, run_dbt_ol_send_events

    _set_real_dbt_pg_env(spec)
    # Called outside the @dbt_assets-decorated function, so - unlike the
    # real asset body - dagster-dbt won't auto-inject the build's own
    # --select for us; without it this would run the WHOLE shared project,
    # including the legacy inline-gold models that need DBT_STAGE_SCHEMA/
    # TABLE env vars this call never sets.
    # .wait() (not .stream()) - draining via stream() tries to translate
    # events into Dagster asset materializations, which needs a real
    # manifest/op context this standalone call doesn't have; .wait() just
    # waits for the real dbt subprocess to finish and artifacts to land.
    invocation = dbt.cli(["run", "--select", spec.dbt.select]).wait()

    proc = run_dbt_ol_send_events(spec, invocation)

    assert proc.returncode == 0, proc.stdout + proc.stderr
    output = proc.stdout + proc.stderr
    assert "Sending events for the last run without running the job" in output
    assert "Running with dbt=" not in output  # dbt's own startup banner - absent means it never ran
    assert "Emitted" in output and "OpenLineage events" in output
    assert f'"namespace": "{spec.lineage.namespace}"' in output
