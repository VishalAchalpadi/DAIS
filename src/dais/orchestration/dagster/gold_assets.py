"""Gold, natively via dagster-dbt's @dbt_assets - real asset-level lineage
and dependency graphs straight from dbt's own manifest.json, rather than a
DAIS-specific reimplementation. One @dbt_assets group per gold_builds/*.yaml
spec (scoped via that spec's own dbt.select tag selector, Phase 9a's tag
convention), not one group for the whole shared project - so each build
shows up as its own set of assets, materializable independently, matching
how run_gold_build() already scopes a single build's dbt run.
"""
import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from dagster import AssetExecutionContext, AssetKey
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets

from dais.secrets.factory import get_secrets_provider
from dais.spec.loader import load_gold_build_spec
from dais.spec.models import GoldBuildSpec


class DaisDagsterDbtTranslator(DagsterDbtTranslator):
    """The one piece of wiring that makes ingest_assets.py's raw/stage
    assets and this module's gold dbt assets show up as ONE connected
    graph: a dbt source() maps to AssetKey([source_name, "stage"]), which
    is exactly the key build_ingest_asset(pipeline_name) uses - because
    dbt/models/sources.yml's source name IS the pipeline name (Phase 9a's
    convention), no extra config/mapping table is needed."""

    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> AssetKey:
        if dbt_resource_props["resource_type"] == "source":
            return AssetKey([dbt_resource_props["source_name"], "stage"])
        return super().get_asset_key(dbt_resource_props)


def _set_real_dbt_pg_env(spec: GoldBuildSpec) -> None:
    """Mutates process env immediately before a dbt run - DbtCliResource.cli()
    reads os.environ at call time and has no env= override (verified
    against the installed dagster-dbt source), so this is the equivalent of
    medallion/gold.py building a per-subprocess env dict, just expressed as
    a direct os.environ mutation since dagster-dbt owns the subprocess call
    here."""
    secret = get_secrets_provider().get_secret(spec.database.connection)
    os.environ.update(
        {
            "DBT_PG_HOST": str(secret["host"]),
            "DBT_PG_PORT": str(secret["port"]),
            "DBT_PG_DBNAME": str(secret["dbname"]),
            "DBT_PG_USER": str(secret["user"]),
            "DBT_PG_PASSWORD": str(secret["password"]),
            "DBT_PG_SCHEMA": spec.target.schema_,
        }
    )


def build_gold_build_dbt_assets(spec_path: str | Path, dbt_project: DbtProject):
    """Returns (assets_definition, spec) - spec is exposed too since
    definitions.py needs gold_build_name for the AssetsDefinition's implicit
    name and job wiring, without re-loading/re-parsing the yaml itself."""
    spec = load_gold_build_spec(spec_path)
    translator = DaisDagsterDbtTranslator()

    @dbt_assets(
        manifest=dbt_project.manifest_path,
        select=spec.dbt.select,
        name=f"{spec.gold_build_name}_dbt_assets",
        project=dbt_project,
        dagster_dbt_translator=translator,
    )
    def _gold_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
        _set_real_dbt_pg_env(spec)
        yield from dbt.cli(["run"], context=context).stream()

    return _gold_dbt_assets, spec
