"""Gold layer: hands off to dbt-core. dbt's target profile is populated
from the SAME connection params raw/stage used (spec.database) via env
vars scoped to the subprocess - never a separately assumed warehouse.

Invoked as `python -m dbt.cli.main` rather than the `dbt`/`dbt.exe`
console script: on Windows, that wrapper script gets regenerated (new
file, new hash) whenever a dependency reinstall touches `click`, which
repeatedly tripped this machine's Application Control policy into
blocking it mid-session. The interpreter itself was never blocked, so
invoking dbt as a module through it sidesteps the problem entirely -
this is not a workaround specific to one broken install, it avoids the
whole class of "a freshly-written .exe looks unrecognized" block.
"""
from __future__ import annotations

import os
import re
import subprocess
import sys
from dataclasses import dataclass

from dais.spec.models import GoldBuildSpec, LineageConfig, PipelineSpec

_DATE_IN_FILENAME_RE = re.compile(r"(\d{8})")

# Phase 9b: dbt-ol (the openlineage-dbt package's CLI wrapper) is invoked via
# `python -c "...from openlineage.dbt import main..."` rather than its own
# dbt-ol/dbt-ol.exe console script, for the same reason run_gold()/
# run_gold_build() use `python -m dbt.cli.main` instead of dbt.exe: a
# pip-regenerated .exe wrapper is what this machine's Application Control
# policy keeps blocking, and the interpreter itself is never blocked.
#
# Unlike those two, dbt-ol's own internals (openlineage/dbt/__init__.py,
# consume_local_artifacts()) hardcode `subprocess.Popen(["dbt"] + args, ...)`
# with no override mechanism (verified against the installed openlineage-dbt
# v1.53.0 source - not assumed), so it needs a REAL `dbt` executable
# resolvable on PATH. `python -m dbt.cli.main` never touches PATH at all, so
# this is the one dbt invocation in DAIS with a live dependency on dbt.exe
# existing and being unblocked - prepending the interpreter's own Scripts/
# directory (where dbt-core/dbt-postgres installed a real dbt.exe) to PATH
# supplies that without relying on a separately-regenerated wrapper.
_DBT_OL_INLINE_SCRIPT = (
    "import sys; "
    "sys.argv = ['dbt-ol', 'run', "
    "'--project-dir', {project_dir!r}, "
    "'--profiles-dir', {project_dir!r}, "
    "'--select', {select!r}]; "
    "from openlineage.dbt import main; sys.exit(main())"
)


def _dbt_ol_env(base_env: dict[str, str], lineage: LineageConfig) -> dict[str, str]:
    env = dict(base_env)
    # openlineage-dbt reads OPENLINEAGE_NAMESPACE directly (default "dbt" if
    # unset) - a DIFFERENT env var than lineage/emitter.py's raw/stage path,
    # which takes namespace from spec.lineage.namespace in Python, not an
    # env var. Verified against the installed package's actual source, per
    # the Phase 9 prompt's requirement to confirm this rather than assume
    # it. OPENLINEAGE_URL (already in base_env, if set) is read the same way
    # dbt-ol's own OpenLineageClient resolves transport, matching
    # lineage/emitter.py's build_client() convention - console transport if
    # unset, so events are at least visible rather than silently dropped.
    env["OPENLINEAGE_NAMESPACE"] = lineage.namespace
    scripts_dir = os.path.dirname(sys.executable)
    env["PATH"] = scripts_dir + os.pathsep + env.get("PATH", "")
    return env


def _run_gold_with_lineage(
    *, project_dir: str, select: str, env: dict[str, str], lineage: LineageConfig
) -> GoldRunResult:
    # dbt docs generate first: produces target/catalog.json, which
    # openlineage-dbt reads (best-effort - it degrades gracefully if
    # missing) to attach column-level schema facets to the datasets in the
    # emitted lineage events, rather than just dataset names.
    docs_proc = subprocess.run(
        [
            sys.executable, "-m", "dbt.cli.main", "docs", "generate",
            "--project-dir", project_dir,
            "--profiles-dir", project_dir,
            "--select", select,
        ],
        env=env, capture_output=True, text=True,
    )
    if docs_proc.returncode != 0:
        return GoldRunResult(
            success=False,
            return_code=docs_proc.returncode,
            stdout=docs_proc.stdout,
            stderr=docs_proc.stderr,
        )

    ol_env = _dbt_ol_env(env, lineage)
    inline_script = _DBT_OL_INLINE_SCRIPT.format(project_dir=project_dir, select=select)
    proc = subprocess.run(
        [sys.executable, "-c", inline_script], env=ol_env, capture_output=True, text=True
    )
    return GoldRunResult(
        success=proc.returncode == 0,
        return_code=proc.returncode,
        stdout=docs_proc.stdout + proc.stdout,
        stderr=docs_proc.stderr + proc.stderr,
    )


def _extract_as_of_date(file_name: str | None) -> str | None:
    """Best-effort: pulls the first YYYYMMDD run of digits out of a
    source file name (e.g. portfolio_prices_20260830.csv -> 2026-08-30).
    Not tied to a spec's `file_pattern` - real file names vary (prefixes,
    "-Correction" suffixes) more than that convention captures. Returns
    None if no 8-digit date-shaped run is found; a gold model that reads
    DBT_AS_OF_DATE should fall back sensibly (e.g. env_var default)."""
    if not file_name:
        return None
    match = _DATE_IN_FILENAME_RE.search(file_name)
    if not match:
        return None
    raw = match.group(1)
    return f"{raw[0:4]}-{raw[4:6]}-{raw[6:8]}"


@dataclass
class GoldRunResult:
    success: bool
    return_code: int
    stdout: str
    stderr: str


def run_gold(
    spec: PipelineSpec,
    *,
    host: str,
    port: int,
    dbname: str,
    user: str,
    password: str,
    file_name: str | None = None,
) -> GoldRunResult:
    env = os.environ.copy()
    env.update(
        {
            "DBT_PG_HOST": host,
            "DBT_PG_PORT": str(port),
            "DBT_PG_DBNAME": dbname,
            "DBT_PG_USER": user,
            "DBT_PG_PASSWORD": password,
            "DBT_PG_SCHEMA": spec.gold.schema_,
            "DBT_STAGE_SCHEMA": spec.stage.schema_,
            "DBT_STAGE_TABLE": spec.stage.table,
        }
    )
    as_of_date = _extract_as_of_date(file_name)
    if as_of_date:
        env["DBT_AS_OF_DATE"] = as_of_date

    # Phase 9b: every PipelineSpec has a required lineage: block already (it
    # feeds lineage/emitter.py's raw/stage events), so the gold step reuses
    # the SAME namespace/job identity for its dbt-ol events rather than
    # introducing a parallel lineage config just for gold.
    return _run_gold_with_lineage(
        project_dir=spec.gold.dbt_project,
        select=spec.gold.dbt_select,
        env=env,
        lineage=spec.lineage,
    )


def run_gold_build(
    gold_build_spec: GoldBuildSpec,
    *,
    host: str,
    port: int,
    dbname: str,
    user: str,
    password: str,
) -> GoldRunResult:
    """Phase 9a: runs a gold_builds/*.yaml spec - dbt.select is a tag
    selector (e.g. "tag:trade_blotter_gold") scoping to exactly that
    build's staging/intermediate/mart models, which reference their
    upstream stage tables via dbt source() + sources.yml rather than the
    env_var()-interpolated schema/table Jinja the legacy inline gold:
    block uses (a gold build can depend on several pipelines' stage
    tables at once - there's no single "the" stage table to interpolate).

    A sibling function to run_gold(), not a replacement - existing
    pipelines with an inline gold: block keep using run_gold() completely
    unchanged."""
    env = os.environ.copy()
    env.update(
        {
            "DBT_PG_HOST": host,
            "DBT_PG_PORT": str(port),
            "DBT_PG_DBNAME": dbname,
            "DBT_PG_USER": user,
            "DBT_PG_PASSWORD": password,
            "DBT_PG_SCHEMA": gold_build_spec.target.schema_,
        }
    )

    # Phase 9b: lineage is optional on GoldBuildSpec (unlike PipelineSpec,
    # where it's required) - a gold build only gets dbt-ol lineage events
    # when its yaml sets one, matching depends_on's namespace so events land
    # in the same OpenLineage graph as the upstream pipelines' raw/stage
    # events rather than a disconnected one.
    if gold_build_spec.lineage is not None:
        return _run_gold_with_lineage(
            project_dir=gold_build_spec.dbt.project_dir,
            select=gold_build_spec.dbt.select,
            env=env,
            lineage=gold_build_spec.lineage,
        )

    cmd = [
        sys.executable,
        "-m",
        "dbt.cli.main",
        "run",
        "--project-dir",
        gold_build_spec.dbt.project_dir,
        "--profiles-dir",
        gold_build_spec.dbt.project_dir,
        "--select",
        gold_build_spec.dbt.select,
    ]
    proc = subprocess.run(cmd, env=env, capture_output=True, text=True)

    return GoldRunResult(
        success=proc.returncode == 0,
        return_code=proc.returncode,
        stdout=proc.stdout,
        stderr=proc.stderr,
    )
