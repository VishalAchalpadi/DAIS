"""Dagster as a second, optional trigger source alongside Control-M -
never a second execution path. Raw/stage assets call the existing HTTP API
as a black box (ingest_assets.py); gold uses dagster-dbt's @dbt_assets
natively against the same shared dbt/ project gold_builds/*.yaml already
uses (gold_assets.py). Whichever trigger source kicks off a run,
control.process_monitor (written inside dais.pipeline, unchanged by any of
this) remains the single source of truth for run status.

Run the UI locally with:
    dagster dev -f src/dais/orchestration/dagster/definitions.py
"""
from __future__ import annotations

import os
from pathlib import Path

from dagster import AssetSelection, Definitions, EnvVar, define_asset_job
from dagster_dbt import DbtCliResource

from dais.orchestration.dagster.api_resource import DaisApiResource
from dais.orchestration.dagster.dbt_project import DBT_PROJECT, DBT_PROJECT_DIR, REAL_DBT_EXECUTABLE
from dais.orchestration.dagster.gold_assets import build_gold_build_dbt_assets
from dais.orchestration.dagster.ingest_assets import build_ingest_asset

REPO_ROOT = Path(__file__).resolve().parents[4]
GOLD_BUILDS_DIR = REPO_ROOT / "gold_builds"

gold_assets_definitions = []
gold_build_specs = []
depends_on_pipelines: set[str] = set()

for spec_path in sorted(GOLD_BUILDS_DIR.glob("*.yaml")):
    assets_def, spec = build_gold_build_dbt_assets(spec_path, DBT_PROJECT)
    gold_assets_definitions.append(assets_def)
    gold_build_specs.append(spec)
    depends_on_pipelines.update(spec.depends_on)

# One ingest asset per pipeline any gold_builds/*.yaml spec depends on -
# discovered from the specs themselves, not hardcoded, so adding a new
# gold build that depends on a new pipeline wires up its ingest asset for
# free.
ingest_assets_definitions = [build_ingest_asset(name) for name in sorted(depends_on_pipelines)]

all_assets = [*ingest_assets_definitions, *gold_assets_definitions]

dais_medallion_job = define_asset_job(
    name="dais_medallion_job",
    selection=AssetSelection.assets(*all_assets),
    description=(
        "Materializes every known ingest pipeline's raw/stage/gold run "
        "(via the DAIS HTTP API) followed by every gold_builds/*.yaml "
        "build (via dbt) - the full hybrid graph in one job."
    ),
)

defs = Definitions(
    assets=all_assets,
    jobs=[dais_medallion_job],
    resources={
        "dais_api": DaisApiResource(
            base_url=os.environ.get("DAIS_API_BASE_URL", "http://localhost:8000"),
            api_key=EnvVar("DAIS_API_KEY"),
        ),
        "dbt": DbtCliResource(project_dir=DBT_PROJECT_DIR, dbt_executable=REAL_DBT_EXECUTABLE),
    },
)
