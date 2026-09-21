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

from dagster import AssetSelection, Definitions, EnvVar, define_asset_job, in_process_executor
from dagster_dbt import DbtCliResource

from dais.orchestration.dagster.api_resource import DaisApiResource
from dais.orchestration.dagster.dbt_project import DBT_PROJECT, DBT_PROJECT_DIR, REAL_DBT_EXECUTABLE
from dais.orchestration.dagster.gold_assets import build_gold_build_dbt_assets
from dais.orchestration.dagster.ingest_assets import build_ingest_asset
from dais.orchestration.dagster.watch_sensors import build_watch_sensor
from dais.spec.loader import load_spec

REPO_ROOT = Path(__file__).resolve().parents[4]
GOLD_BUILDS_DIR = REPO_ROOT / "gold_builds"
SPECS_DIR = REPO_ROOT / "specs"

gold_assets_definitions = []
gold_build_specs = []
gold_build_jobs = []
depends_on_pipelines: set[str] = set()

for spec_path in sorted(GOLD_BUILDS_DIR.glob("*.yaml")):
    assets_def, spec = build_gold_build_dbt_assets(spec_path, DBT_PROJECT)
    gold_assets_definitions.append(assets_def)
    gold_build_specs.append(spec)
    depends_on_pipelines.update(spec.depends_on)

    # A dedicated job per gold build - lets you run e.g. regional_sales_gold_job
    # directly (its stg_*/int_*/mart models, dependency-ordered) without
    # hand-selecting assets or dragging in every other pipeline's ingest
    # assets the way dais_medallion_job does.
    gold_build_jobs.append(
        define_asset_job(
            name=f"{spec.gold_build_name}_job",
            selection=AssetSelection.assets(assets_def),
            description=f"Runs the {spec.gold_build_name} gold build (gold_builds/{spec_path.name}) via dbt.",
        )
    )

# One ingest asset per pipeline spec under specs/*.yaml - discovered from
# the specs themselves, not hardcoded, so adding a new spec file wires up
# its ingest asset (and dedicated job, below) for free, whether or not any
# gold_builds/*.yaml depends on it yet. Union with depends_on_pipelines
# just in case a gold build's depends_on ever names a pipeline whose own
# specs/*.yaml file has since been removed/renamed - keeps that build from
# silently losing its upstream asset.
all_pipeline_names = sorted(
    {p.stem for p in SPECS_DIR.glob("*.yaml") if not p.stem.endswith(".dq_suggestions")} | depends_on_pipelines
)
ingest_assets_definitions = [build_ingest_asset(name) for name in all_pipeline_names]

# A dedicated job per ingest pipeline too, mirroring gold_build_jobs above -
# same asset (ingest_assets.py, still just an HTTP call to the real DAIS
# API), just addressable on its own instead of only via the Catalog page
# or the everything-at-once dais_medallion_job.
#
# in_process_executor: each of these jobs materializes exactly one asset
# (see test_dedicated_ingest_job_is_scoped_to_only_that_pipelines_asset),
# so there is no parallelism to lose - but Dagster's default multiprocess
# executor still spawns a whole separate OS process for that one step,
# which re-imports this entire definitions.py module (including
# dbt_project.py's prepare_if_dev(), a real `dbt parse`) a SECOND time on
# top of the run's own process already having just done the same thing.
# Measured: ~65s wall clock for a run whose actual pipeline work (per
# control.process_monitor) was ~0.2s - almost all of it two redundant
# cold imports. in_process_executor runs the step in the run's own
# process instead, cutting that to one.
ingest_jobs = [
    define_asset_job(
        name=f"{pipeline_name}_job",
        selection=AssetSelection.assets(asset_def),
        description=f"Runs the {pipeline_name} pipeline (raw/stage, via the DAIS HTTP API).",
        executor_def=in_process_executor,
    )
    for pipeline_name, asset_def in zip(all_pipeline_names, ingest_assets_definitions)
]

# A file-watching sensor per pipeline whose spec opts in via
# source.location.watch (watch_sensors.py) - targets that pipeline's own
# ingest job above, so a sensor-launched run is identical to a manual one.
watch_sensors = []
for pipeline_name, ingest_job in zip(all_pipeline_names, ingest_jobs):
    spec_path = SPECS_DIR / f"{pipeline_name}.yaml"
    if not spec_path.exists():
        continue
    pipeline_spec = load_spec(spec_path)
    if pipeline_spec.source.location.watch is not None and pipeline_spec.source.location.watch.enabled:
        watch_sensors.append(build_watch_sensor(pipeline_name, pipeline_spec, ingest_job))

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
    jobs=[dais_medallion_job, *gold_build_jobs, *ingest_jobs],
    sensors=watch_sensors,
    resources={
        "dais_api": DaisApiResource(
            base_url=os.environ.get("DAIS_API_BASE_URL", "http://localhost:8000"),
            api_key=EnvVar("DAIS_API_KEY"),
        ),
        "dbt": DbtCliResource(project_dir=DBT_PROJECT_DIR, dbt_executable=REAL_DBT_EXECUTABLE),
    },
)
