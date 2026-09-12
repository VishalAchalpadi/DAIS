"""Gold, natively via dagster-dbt's @dbt_assets - real asset-level lineage
and dependency graphs straight from dbt's own manifest.json, rather than a
DAIS-specific reimplementation. One @dbt_assets group per gold_builds/*.yaml
spec (scoped via that spec's own dbt.select tag selector, Phase 9a's tag
convention), not one group for the whole shared project - so each build
shows up as its own set of assets, materializable independently, matching
how run_gold_build() already scopes a single build's dbt run.
"""
import os
import subprocess
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from dagster import AssetExecutionContext, AssetKey
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets
from dagster_dbt.core.dbt_cli_invocation import DbtCliInvocation

from dais.secrets.factory import get_secrets_provider
from dais.spec.loader import load_gold_build_spec
from dais.spec.models import GoldBuildSpec

# dbt-ol's "send-events" mode (verified against the installed
# openlineage-dbt v1.53.0 source: consume_local_artifacts() checks
# args[0]/args[1] == "send-events" and, when set, skips re-running dbt
# entirely - no Popen(["dbt"] + args) call at all - going straight to
# DbtLocalArtifactProcessor.parse(), which only reads run_results.json/
# manifest.json/catalog.json already on disk). That's the seam that lets a
# Dagster-triggered gold run emit the SAME real OpenLineage event
# medallion/gold.py's run_gold_build() (CLI path) already does, without
# running the SQL a second time: dbt.cli(["run"], ...) below produces
# those artifacts via Dagster's own native dbt integration; this then asks
# dbt-ol to parse THAT SAME run's artifacts rather than starting a new one.
_DBT_OL_SEND_EVENTS_SCRIPT = (
    "import sys; "
    "sys.argv = ['dbt-ol', 'run', 'send-events', "
    "'--project-dir', {project_dir!r}, "
    "'--target-path', {target_path!r}, "
    "'--select', {select!r}]; "
    "from openlineage.dbt import main; sys.exit(main())"
)


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


def run_dbt_ol_send_events(spec: GoldBuildSpec, invocation: DbtCliInvocation) -> subprocess.CompletedProcess:
    """Asks dbt-ol to parse the artifacts invocation.stream() just finished
    writing (run_results.json/manifest.json/catalog.json under
    invocation.target_path) and emit a real OpenLineage event from them -
    see _DBT_OL_SEND_EVENTS_SCRIPT above for why this needs no second dbt
    run. Invoked via `python -c` (not the dbt-ol console script) for the
    same reason medallion/gold.py does: a pip-regenerated .exe wrapper is
    what this machine's Application Control policy has repeatedly blocked,
    the interpreter itself never is. Unlike that module's dbt-ol call,
    send-events mode never spawns a real "dbt" process, so there is no
    PATH/dbt.exe dependency here at all. A plain function (not folded into
    the asset body) so it's independently testable against a real
    completed dbt run without needing a Dagster execution context."""
    env = os.environ.copy()
    env["OPENLINEAGE_NAMESPACE"] = spec.lineage.namespace
    inline_script = _DBT_OL_SEND_EVENTS_SCRIPT.format(
        project_dir=str(invocation.project_dir),
        target_path=str(invocation.target_path),
        select=spec.dbt.select,
    )
    return subprocess.run([sys.executable, "-c", inline_script], env=env, capture_output=True, text=True)


def _emit_unified_openlineage(
    spec: GoldBuildSpec, invocation: DbtCliInvocation, context: AssetExecutionContext
) -> None:
    proc = run_dbt_ol_send_events(spec, invocation)
    # dbt-ol logs to stderr (its logger's default handler) - surface both
    # streams into Dagster's own log, or "it worked" is otherwise invisible
    # next to dbt.cli()'s own already-visible output.
    for line in (proc.stdout + proc.stderr).splitlines():
        if line.strip():
            context.log.info(f"[dbt-ol] {line}")
    if proc.returncode != 0:
        raise RuntimeError(
            f"dbt-ol send-events failed for gold build {spec.gold_build_name!r} (rc={proc.returncode})"
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
        invocation = dbt.cli(["run"], context=context)
        yield from invocation.stream()
        # Only when this build has a lineage: block - same opt-in shape
        # run_gold_build() uses, and builds without one keep behaving
        # exactly as before this change.
        if spec.lineage is not None:
            _emit_unified_openlineage(spec, invocation, context)

    return _gold_dbt_assets, spec
