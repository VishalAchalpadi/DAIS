"""Column-level lineage capture, independent of the OpenLineage
job/dataset-level events emitted at run time (lineage/emitter.py).

Two hops, two different techniques - there's no single tool that covers
both, so this module stitches them together:

raw -> stage: not SQL (bronze/silver land data via Python + pandera), so
lineage comes from introspecting the spec itself - every column the
quality layer knows about (quality.rules plus stage.business_key) maps
1:1 by name from raw to stage, annotated with its cast/checks.

stage -> gold: real SQL (a dbt model), parsed with `sqllineage` against
dbt's *compiled* SQL (`dbt compile` resolves the `env_var()` Jinja into
literal schema/table names - the raw .sql source isn't parseable SQL on
its own). The compiled SELECT is wrapped in a synthetic
`CREATE TABLE <gold> AS <select>` so sqllineage has both a source and a
target to trace column-to-column.
"""
from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

from sqllineage.runner import LineageRunner

from dais.spec.models import PipelineSpec

_SELECT_CLAUSE_RE = re.compile(r"select\s+(.*?)\s+from\s", re.IGNORECASE | re.DOTALL)
_AS_ALIAS_RE = re.compile(r"^(?P<expr>.*?)\s+as\s+(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*$", re.IGNORECASE | re.DOTALL)
_BARE_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$")


def _split_top_level_commas(clause: str) -> list[str]:
    """Splits a SELECT clause's column list on commas, respecting
    parenthesis nesting (so `lag(x) over (order by y), z` splits into
    two items, not four)."""
    parts, current, depth = [], [], 0
    for ch in clause:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            parts.append("".join(current))
            current = []
        else:
            current.append(ch)
    parts.append("".join(current))
    return [p.strip() for p in parts if p.strip()]


def _expressions_by_alias(sql: str) -> dict[str, list[str]]:
    """Best-effort (regex, not a full SQL parser) map of column alias ->
    every expression seen defining it, across every SELECT clause in the
    compiled model (main query and CTEs alike) - used only to label
    lineage edges with a human-readable expression, not to compute the
    graph itself (sqllineage/sqlfluff already did that properly)."""
    result: dict[str, list[str]] = {}
    for clause in _SELECT_CLAUSE_RE.findall(sql):
        for item in _split_top_level_commas(clause):
            match = _AS_ALIAS_RE.match(item)
            if match:
                expr, alias = match.group("expr").strip(), match.group("alias").strip()
            else:
                alias = item.split(".")[-1].strip()
                expr = item.strip()
            result.setdefault(alias, []).append(re.sub(r"\s+", " ", expr))
    return result


def _best_expression(exprs_by_alias: dict[str, list[str]], alias: str) -> str | None:
    """Prefers the first expression that isn't just a bare column
    reference (e.g. a CTE's `sum(x) as total` over an outer query's
    plain passthrough `total`), since the bare reference doesn't explain
    the actual transformation."""
    candidates = exprs_by_alias.get(alias, [])
    for expr in candidates:
        if not _BARE_IDENTIFIER_RE.match(expr):
            return expr
    return candidates[0] if candidates else None


@dataclass(frozen=True)
class ColumnNode:
    layer: str  # "raw" | "stage" | "gold"
    table: str  # "schema.table"
    column: str

    @property
    def id(self) -> str:
        return f"{self.layer}:{self.table}.{self.column}"


@dataclass(frozen=True)
class ColumnEdge:
    source: ColumnNode
    target: ColumnNode
    transformation: str


def _rule_transformation_label(rule) -> str:
    parts = []
    if rule.cast_to:
        parts.append(f"cast_to={rule.cast_to}")
    if rule.checks:
        checks = ", ".join(c if isinstance(c, str) else str(c) for c in rule.checks)
        parts.append(f"checks=[{checks}]")
    if rule.lookup:
        parts.append("sql lookup validation")
    return "; ".join(parts) if parts else "passthrough"


def raw_to_stage_edges(spec: PipelineSpec) -> list[ColumnEdge]:
    raw_table = f"{spec.raw.schema_}.{spec.raw.table}"
    stage_table = f"{spec.stage.schema_}.{spec.stage.table}"

    rules_by_column = {rule.column: rule for rule in spec.quality.rules}
    columns = dict.fromkeys(rules_by_column)  # preserve rule order
    for col in spec.stage.business_key:
        columns.setdefault(col, None)

    edges = []
    for column in columns:
        rule = rules_by_column.get(column)
        label = _rule_transformation_label(rule) if rule else "passthrough (no quality.rules entry)"
        edges.append(
            ColumnEdge(
                source=ColumnNode("raw", raw_table, column),
                target=ColumnNode("stage", stage_table, column),
                transformation=label,
            )
        )
    return edges


def _dbt_executable() -> str:
    venv_bin = Path(sys.executable).parent
    candidate = venv_bin / ("dbt.exe" if os.name == "nt" else "dbt")
    if candidate.is_file():
        return str(candidate)
    found = shutil.which("dbt")
    if found:
        return found
    raise RuntimeError("dbt executable not found - is dbt-core installed in this environment?")


def _compile_gold_model(
    spec: PipelineSpec,
    *,
    host: str,
    port: int,
    dbname: str,
    user: str,
    password: str,
) -> str:
    """Runs `dbt compile` so the model's env_var() Jinja is resolved into
    literal schema/table names, then returns the compiled SELECT text.
    A live DB connection is required only because dbt validates the
    profile's connection before compiling - it never reads/writes data."""
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
    proc = subprocess.run(
        [
            _dbt_executable(),
            "compile",
            "--project-dir",
            spec.gold.dbt_project,
            "--profiles-dir",
            spec.gold.dbt_project,
            "--select",
            spec.gold.dbt_select,
        ],
        env=env,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"dbt compile failed:\n{proc.stdout}\n{proc.stderr}")

    project_name = _dbt_project_name(spec.gold.dbt_project)
    compiled_path = (
        Path(spec.gold.dbt_project)
        / "target"
        / "compiled"
        / project_name
        / "models"
        / f"{spec.gold.dbt_select}.sql"
    )
    return compiled_path.read_text(encoding="utf-8")


def _dbt_project_name(dbt_project_dir: str) -> str:
    import yaml

    project_yml = Path(dbt_project_dir) / "dbt_project.yml"
    return yaml.safe_load(project_yml.read_text(encoding="utf-8"))["name"]


def stage_to_gold_edges_from_sql(compiled_select_sql: str, spec: PipelineSpec) -> list[ColumnEdge]:
    gold_table = f"{spec.gold.schema_}.{spec.gold.dbt_select}"
    synthetic_sql = f"CREATE TABLE {gold_table} AS\n{compiled_select_sql}"
    exprs_by_alias = _expressions_by_alias(compiled_select_sql)

    runner = LineageRunner(synthetic_sql, dialect="postgres")
    edges: list[ColumnEdge] = []
    seen: set[tuple] = set()
    for path in runner.get_column_lineage():
        source_col = path[0]
        target_col = path[-1]
        key = (str(source_col), str(target_col))
        if key in seen:
            continue
        seen.add(key)
        expr = _best_expression(exprs_by_alias, target_col.raw_name)
        transformation = f"dbt: {expr}" if expr else "dbt: passthrough"
        edges.append(
            ColumnEdge(
                source=ColumnNode("stage", str(source_col.parent), source_col.raw_name),
                target=ColumnNode("gold", str(target_col.parent), target_col.raw_name),
                transformation=transformation,
            )
        )
    return edges


def stage_to_gold_edges(
    spec: PipelineSpec, *, host: str, port: int, dbname: str, user: str, password: str
) -> list[ColumnEdge]:
    compiled_sql = _compile_gold_model(
        spec, host=host, port=port, dbname=dbname, user=user, password=password
    )
    return stage_to_gold_edges_from_sql(compiled_sql, spec)


def build_lineage_graph(
    spec: PipelineSpec, *, host: str, port: int, dbname: str, user: str, password: str
) -> list[ColumnEdge]:
    edges = raw_to_stage_edges(spec)
    edges += stage_to_gold_edges(spec, host=host, port=port, dbname=dbname, user=user, password=password)
    return edges
