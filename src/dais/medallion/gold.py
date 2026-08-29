"""Gold layer: hands off to dbt-core. dbt's target profile is populated
from the SAME connection params raw/stage used (spec.database) via env
vars scoped to the subprocess - never a separately assumed warehouse.
"""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

from dais.spec.models import PipelineSpec


@dataclass
class GoldRunResult:
    success: bool
    return_code: int
    stdout: str
    stderr: str


def _dbt_executable() -> str:
    """dbt-core installs a `dbt` console script alongside the interpreter
    in this venv - prefer that over relying on PATH, since a venv's
    Scripts/bin dir isn't necessarily on it for a subprocess we launch."""
    venv_bin = Path(sys.executable).parent
    candidate = venv_bin / ("dbt.exe" if os.name == "nt" else "dbt")
    if candidate.is_file():
        return str(candidate)
    found = shutil.which("dbt")
    if found:
        return found
    raise RuntimeError("dbt executable not found - is dbt-core installed in this environment?")


def run_gold(
    spec: PipelineSpec,
    *,
    host: str,
    port: int,
    dbname: str,
    user: str,
    password: str,
) -> GoldRunResult:
    import os

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

    cmd = [
        _dbt_executable(),
        "run",
        "--project-dir",
        spec.gold.dbt_project,
        "--profiles-dir",
        spec.gold.dbt_project,
        "--select",
        spec.gold.dbt_select,
    ]
    proc = subprocess.run(cmd, env=env, capture_output=True, text=True)

    return GoldRunResult(
        success=proc.returncode == 0,
        return_code=proc.returncode,
        stdout=proc.stdout,
        stderr=proc.stderr,
    )
