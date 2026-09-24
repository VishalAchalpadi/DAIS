"""Background execution for a gold_builds/*.yaml run - a sibling to
api/worker.py's execute_pipeline_background, not a change to it. Runs
run_gold_build() (medallion/gold.py) exactly as the CLI and Dagster's own
@dbt_assets already do (see gold_assets.py's _set_real_dbt_pg_env for the
same secret-resolution pattern) - this is a third caller, not a new
execution path.
"""
from __future__ import annotations

import logging

from dais.api.gold_build_runs import GoldBuildRunRegistry
from dais.db import build_connection_params_for_gold_build
from dais.medallion.gold import run_gold_build
from dais.spec.models import GoldBuildSpec

log = logging.getLogger(__name__)


def execute_gold_build_background(
    registry: GoldBuildRunRegistry,
    spec: GoldBuildSpec,
    run_id: str,
) -> None:
    try:
        connection_params = build_connection_params_for_gold_build(spec.database)
        result = run_gold_build(spec, **connection_params)
        if result.success:
            registry.finish(run_id, "succeeded")
        else:
            registry.finish(run_id, "failed", error=result.stderr[-2000:] or result.stdout[-2000:])
    except Exception as exc:  # never fail silently - the run must land in a terminal state
        registry.finish(run_id, "failed", error=str(exc))
