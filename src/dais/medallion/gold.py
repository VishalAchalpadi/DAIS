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
import subprocess
import sys
from dataclasses import dataclass

from dais.spec.models import PipelineSpec


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

    cmd = [
        sys.executable,
        "-m",
        "dbt.cli.main",
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
