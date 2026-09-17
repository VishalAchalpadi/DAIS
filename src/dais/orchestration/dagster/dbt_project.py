"""Points dagster-dbt at the SAME shared dbt/ project gold_builds/*.yaml
uses (Phase 9a/9b) - never a separate project or a copy of its models.
Module-level side effects here (PATH, placeholder env vars, manifest
prep) run once per process, at import time, the same way medallion/gold.py
mutates a subprocess env rather than requiring callers to set it up.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

from dagster_dbt import DbtProject

REPO_ROOT = Path(__file__).resolve().parents[4]
DBT_PROJECT_DIR = REPO_ROOT / "dbt"

# dagster-dbt's own manifest-generation step (DbtProject.prepare_if_dev/
# prepare) shells out via DbtCliResource(project_dir=project) using ITS
# default dbt_executable resolution (shutil.which("dbt")) - unlike the
# @dbt_assets-decorated function itself, this internal call takes no
# dbt_executable override. It needs a real `dbt` resolvable on PATH
# regardless of whether the venv was activated before `dagster dev` was
# launched - same reasoning as medallion/gold.py's dbt-ol PATH fix
# (Phase 9b), verified there against this machine's Application Control
# policy repeatedly blocking pip-regenerated .exe wrappers.
_scripts_dir = os.path.dirname(sys.executable)
if _scripts_dir not in os.environ.get("PATH", ""):
    os.environ["PATH"] = _scripts_dir + os.pathsep + os.environ.get("PATH", "")

# The real dbt executable, for callers (gold_assets.py) that construct a
# DbtCliResource directly and CAN pass dbt_executable explicitly.
REAL_DBT_EXECUTABLE = str(Path(sys.executable).parent / ("dbt.exe" if os.name == "nt" else "dbt"))

# dbt/profiles.yml's postgres target has no env_var() defaults on host/
# port/user/etc (unlike the legacy gold: models' Jinja, which Phase 9a
# gave defaults specifically so a gold_builds run could parse them) -
# `dbt parse` still needs SOME value to resolve the profile's Jinja, even
# though parsing never opens a real connection. These are placeholders for
# manifest generation only; gold_assets.py sets the REAL values (resolved
# from each gold build's own secrets) immediately before `dbt run`.
os.environ.setdefault("DBT_PG_HOST", "unused")
os.environ.setdefault("DBT_PG_PORT", "5432")
os.environ.setdefault("DBT_PG_DBNAME", "unused")
os.environ.setdefault("DBT_PG_USER", "unused")
os.environ.setdefault("DBT_PG_PASSWORD", "unused")
os.environ.setdefault("DBT_PG_SCHEMA", "unused")

DBT_PROJECT = DbtProject(project_dir=DBT_PROJECT_DIR)

_DBT_SOURCE_SUFFIXES = (".sql", ".yml", ".yaml")
_DBT_GENERATED_DIRS = {"target", "dbt_packages", "logs"}


def _dbt_project_changed_since(reference_mtime: float) -> bool:
    """True if any real dbt project source file (models/macros/project
    config - never target/dbt_packages/logs, which are outputs, not
    inputs) is newer than `reference_mtime`."""
    for path in DBT_PROJECT_DIR.rglob("*"):
        if not path.is_file() or path.suffix not in _DBT_SOURCE_SUFFIXES:
            continue
        if _DBT_GENERATED_DIRS & set(path.relative_to(DBT_PROJECT_DIR).parts):
            continue
        if path.stat().st_mtime > reference_mtime:
            return True
    return False


def _manifest_is_fresh() -> bool:
    manifest_path = DBT_PROJECT.manifest_path
    return manifest_path.exists() and not _dbt_project_changed_since(manifest_path.stat().st_mtime)


# prepare_if_dev() (and the manual prepare() fallback for non-dev-CLI
# callers, e.g. pytest) does a full `dbt parse` UNCONDITIONALLY on every
# process that imports this module - including a fresh subprocess Dagster
# spawns per run. Measured: ~45s per parse, dwarfing the actual pipeline
# work it was gating (often under 1s). Skip it entirely when nothing that
# would change the manifest has been touched since it was last generated -
# a run process almost never needs a fresh parse, only an actual dbt
# model/macro/project-config edit does. prepare_if_dev() only acts under
# `dagster dev` (DAGSTER_IS_DEV_CLI) in the first place; the explicit
# prepare() fallback below still runs whenever the manifest doesn't exist
# yet at all (e.g. a first-ever run, or under plain pytest).
if not _manifest_is_fresh():
    DBT_PROJECT.prepare_if_dev()  # only actually reparses under real `dagster dev`
    if not _manifest_is_fresh():  # still missing/stale (e.g. under pytest) - force it
        DBT_PROJECT.preparer.prepare(DBT_PROJECT)
