"""Phase 9a: GoldBuildSpec loading/validation, and that dbt_select's tag
selector correctly scopes to only that build's models.

Note: unlike a PipelineSpec's stage table, a gold_build's dbt models read
from sources.yml (dbt/models/sources.yml), which hardcodes the REAL
data_in.holdings_stage/data_in.asset_stage tables (the normal dbt
convention - sources.yml is a static file, not resolved per-test). So
these tests target an isolated test_schema for the OUTPUT only; the
inputs are the real (shared, already-populated-by-other-tests) stage
tables, same as running the actual reference example manually.
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from dais.medallion.gold import run_gold_build
from dais.spec.loader import SpecLoadError, load_gold_build_spec
from dais.spec.models import GoldBuildSpec
from tests.conftest import _local_pg_creds, requires_local_postgres

pytestmark = requires_local_postgres

DBT_PROJECT = str(Path(__file__).parent.parent / "dbt")
EXAMPLE_SPEC = Path(__file__).parent.parent / "gold_builds" / "portfolio_summary_gold.yaml"


def _valid_gold_build_dict(**overrides) -> dict:
    data = {
        "gold_build_name": "test_gold_build",
        "description": "A test gold build",
        "owner": "data-eng-team",
        "depends_on": ["holdings_ingest", "asset_ingest"],
        "database": {"platform": "postgres", "connection": "aurora_postgres_prod"},
        "dbt": {"project_dir": DBT_PROJECT, "select": "tag:portfolio_summary_gold"},
        "target": {"schema": "core", "primary_table": "portfolio_summary_gold"},
    }
    data.update(overrides)
    return data


# ---------------------------------------------------------------------------
# spec loading/validation
# ---------------------------------------------------------------------------

def test_valid_gold_build_dict_loads():
    spec = GoldBuildSpec.model_validate(_valid_gold_build_dict())
    assert spec.gold_build_name == "test_gold_build"
    assert spec.depends_on == ["holdings_ingest", "asset_ingest"]
    assert spec.sla is None


def test_load_real_reference_example():
    spec = load_gold_build_spec(EXAMPLE_SPEC)
    assert spec.gold_build_name == "portfolio_summary_gold"
    assert spec.depends_on == ["holdings_ingest", "asset_ingest"]
    assert spec.dbt.select == "tag:portfolio_summary_gold"
    assert spec.sla.complete_by == "07:30"


def test_missing_spec_file_raises():
    with pytest.raises(SpecLoadError, match="not found"):
        load_gold_build_spec("gold_builds/does_not_exist.yaml")


def test_depends_on_requires_at_least_one_pipeline():
    with pytest.raises(ValidationError):
        GoldBuildSpec.model_validate(_valid_gold_build_dict(depends_on=[]))


def test_placeholder_owner_rejected():
    with pytest.raises(ValidationError, match="placeholder"):
        GoldBuildSpec.model_validate(_valid_gold_build_dict(owner="<<team or person responsible>>"))


def test_placeholder_connection_rejected():
    data = _valid_gold_build_dict()
    data["database"]["connection"] = "<<vault secret name>>"
    with pytest.raises(ValidationError, match="placeholder"):
        GoldBuildSpec.model_validate(data)


def test_unknown_top_level_field_rejected():
    data = _valid_gold_build_dict()
    data["not_a_real_field"] = "oops"
    with pytest.raises(ValidationError):
        GoldBuildSpec.model_validate(data)


def test_sla_placeholder_rejected():
    data = _valid_gold_build_dict()
    data["sla"] = {"complete_by": "<<e.g. 07:30>>", "timezone": "America/New_York"}
    with pytest.raises(ValidationError, match="placeholder"):
        GoldBuildSpec.model_validate(data)


def test_valid_sla_accepted():
    data = _valid_gold_build_dict()
    data["sla"] = {"complete_by": "07:30", "timezone": "America/New_York"}
    spec = GoldBuildSpec.model_validate(data)
    assert spec.sla.complete_by == "07:30"
    assert spec.sla.timezone == "America/New_York"


# ---------------------------------------------------------------------------
# dbt_select tag scoping - a real `dbt ls` against the shared project
# ---------------------------------------------------------------------------

def _dbt_ls(select: str) -> list[str]:
    import os

    creds = _local_pg_creds()
    env = os.environ.copy()
    env.update(
        {
            "DBT_PG_HOST": str(creds["host"]),
            "DBT_PG_PORT": str(creds["port"]),
            "DBT_PG_DBNAME": str(creds["dbname"]),
            "DBT_PG_USER": str(creds["user"]),
            "DBT_PG_PASSWORD": str(creds["password"]),
            "DBT_PG_SCHEMA": "core",
        }
    )
    proc = subprocess.run(
        [
            sys.executable, "-m", "dbt.cli.main", "--quiet", "ls",
            "--project-dir", DBT_PROJECT, "--profiles-dir", DBT_PROJECT,
            "--select", select, "--resource-type", "model", "--output", "name",
        ],
        capture_output=True,
        text=True,
        env=env,
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr
    return [line.strip() for line in proc.stdout.splitlines() if line.strip()]


def test_tag_selector_scopes_to_exactly_this_builds_models():
    selected = set(_dbt_ls("tag:portfolio_summary_gold"))
    assert selected == {"stg_holdings", "stg_asset", "int_portfolio_positions", "portfolio_summary_gold"}


def test_tag_selector_excludes_legacy_inline_gold_models():
    selected = set(_dbt_ls("tag:portfolio_summary_gold"))
    assert "asset_gold" not in selected
    assert "holdings_gold" not in selected
    assert "price_gold" not in selected


# ---------------------------------------------------------------------------
# real end-to-end run against the shared (real) stage tables
# ---------------------------------------------------------------------------

def test_run_gold_build_materializes_real_output(pg_connector, test_schema):
    data = yaml.safe_load(EXAMPLE_SPEC.read_text(encoding="utf-8"))
    data["dbt"]["project_dir"] = DBT_PROJECT
    data["target"]["schema"] = test_schema
    spec = GoldBuildSpec.model_validate(data)

    creds = _local_pg_creds()
    result = run_gold_build(
        spec, host=creds["host"], port=creds["port"], dbname=creds["dbname"], user=creds["user"], password=creds["password"]
    )

    assert result.success, result.stdout + result.stderr
    rows = pg_connector.fetch_all(
        f'SELECT column_name FROM information_schema.columns '
        f"WHERE table_schema = '{test_schema}' AND table_name = 'portfolio_summary_gold' "
        f"ORDER BY ordinal_position"
    )
    columns = [r[0] for r in rows]
    assert columns == [
        "portfolio_id", "as_of_date", "holdings_market_value", "holding_count",
        "total_net_assets_usd", "base_currency", "reconciliation_diff", "gold_loaded_at",
    ]
