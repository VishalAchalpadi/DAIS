"""Phase 9b: dbt-ol (openlineage-dbt) integration for the gold layer.

Both run_gold() (legacy inline gold: block) and run_gold_build() (Phase 9a
gold_builds/*.yaml) route through dbt-ol when a lineage config is available,
so gold's OpenLineage events land in the same graph as raw/stage's
(lineage/emitter.py) rather than a disconnected one. GoldBuildSpec's
lineage: block is optional (unlike PipelineSpec's, which is required) - a
gold build only gets dbt-ol events when its yaml sets one.

The dbt-ol subprocess call needs a real dbt.exe resolvable on PATH (its
consume_local_artifacts() path hardcodes `Popen(["dbt"] + args)` - verified
against the installed openlineage-dbt v1.53.0 source, not assumed), which
_dbt_ol_env() supplies by prepending the interpreter's own Scripts/
directory. These tests exercise the real subprocess pipeline (dbt docs
generate -> dbt-ol run) end-to-end against the real reference example,
console-emitting real OpenLineage events (no OPENLINEAGE_URL configured in
this environment) rather than mocking any part of it.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest
import yaml

from dais.medallion.gold import _dbt_ol_env, run_gold_build
from dais.spec.loader import load_gold_build_spec
from dais.spec.models import GoldBuildSpec, LineageConfig
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
# spec-level: lineage is optional, reuses the same LineageConfig shape
# ---------------------------------------------------------------------------

def test_lineage_block_is_optional():
    spec = GoldBuildSpec.model_validate(_valid_gold_build_dict())
    assert spec.lineage is None


def test_lineage_block_accepted_when_present():
    data = _valid_gold_build_dict(
        lineage={"namespace": "datamesh-prod", "job_name": "gold-build-test_gold_build"}
    )
    spec = GoldBuildSpec.model_validate(data)
    assert spec.lineage.namespace == "datamesh-prod"
    assert spec.lineage.job_name == "gold-build-test_gold_build"


def test_reference_example_declares_lineage_matching_upstream_namespace():
    spec = load_gold_build_spec(EXAMPLE_SPEC)
    assert spec.lineage is not None
    assert spec.lineage.namespace == "datamesh-prod"


# ---------------------------------------------------------------------------
# _dbt_ol_env(): namespace env var + PATH prepend, verified against the
# installed openlineage-dbt package's actual configuration surface
# ---------------------------------------------------------------------------

def test_dbt_ol_env_sets_openlineage_namespace():
    lineage = LineageConfig(namespace="datamesh-prod", job_name="dbt-run-dais_gold")
    env = _dbt_ol_env({"PATH": "C:\\existing"}, lineage)
    assert env["OPENLINEAGE_NAMESPACE"] == "datamesh-prod"


def test_dbt_ol_env_prepends_interpreter_scripts_dir_to_path():
    lineage = LineageConfig(namespace="datamesh-prod", job_name="dbt-run-dais_gold")
    env = _dbt_ol_env({"PATH": "C:\\existing"}, lineage)
    scripts_dir = os.path.dirname(sys.executable)
    assert env["PATH"].startswith(scripts_dir + os.pathsep)
    assert env["PATH"].endswith("C:\\existing")


def test_dbt_ol_env_does_not_mutate_base_env():
    lineage = LineageConfig(namespace="datamesh-prod", job_name="dbt-run-dais_gold")
    base = {"PATH": "C:\\existing"}
    _dbt_ol_env(base, lineage)
    assert base == {"PATH": "C:\\existing"}


# ---------------------------------------------------------------------------
# real end-to-end: run_gold_build() routes through dbt-ol and emits real
# OpenLineage events when the spec's lineage: block is present
# ---------------------------------------------------------------------------

def test_run_gold_build_without_lineage_skips_dbt_ol(pg_connector, test_schema):
    """No lineage: block -> plain `dbt run`, no dbt-ol wrapper involved."""
    data = _valid_gold_build_dict()
    data["target"]["schema"] = test_schema
    spec = GoldBuildSpec.model_validate(data)
    assert spec.lineage is None

    creds = _local_pg_creds()
    result = run_gold_build(
        spec, host=creds["host"], port=creds["port"], dbname=creds["dbname"],
        user=creds["user"], password=creds["password"],
    )

    assert result.success, result.stdout + result.stderr
    assert "openlineage.dbt" not in result.stdout
    assert "OpenLineage events" not in result.stdout


def test_run_gold_build_with_lineage_emits_real_openlineage_events(pg_connector, test_schema):
    """lineage: block present -> dbt docs generate + dbt-ol run, with a real
    emitted event carrying the configured namespace and the upstream stage
    table as an input to the gold mart."""
    data = yaml.safe_load(EXAMPLE_SPEC.read_text(encoding="utf-8"))
    data["dbt"]["project_dir"] = DBT_PROJECT
    data["target"]["schema"] = test_schema
    spec = GoldBuildSpec.model_validate(data)
    assert spec.lineage is not None

    creds = _local_pg_creds()
    result = run_gold_build(
        spec, host=creds["host"], port=creds["port"], dbname=creds["dbname"],
        user=creds["user"], password=creds["password"],
    )

    assert result.success, result.stdout + result.stderr
    assert "Emitted" in result.stdout and "OpenLineage events" in result.stdout
    # The RunEvent job facet carries the configured namespace, not dbt-ol's
    # own "dbt" default - confirms OPENLINEAGE_NAMESPACE actually took effect.
    assert '"namespace": "datamesh-prod"' in result.stdout
    # int_portfolio_positions (the intermediate model gold reads from) shows
    # up as an input dataset somewhere in the emitted event stream - this is
    # the actual deliverable: a gold model's upstream table(s) correctly
    # listed as lineage inputs, not just a job-level namespace match.
    assert "int_portfolio_positions" in result.stdout
