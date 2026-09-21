"""fx_rates_ingest (raw -> stage, files archived on success) and its
separate fx_rates_gold build (latest rate / prior / change / inverse).

Nothing here touches the real data_in.fx_rates_* tables: ingest tests run
against a throwaway schema, and the gold SQL is compiled from the real dbt
models then executed against a scratch copy of the stage table (the gold
build's source is a fixed data_in.fx_rates_stage in dbt/models/sources.yml,
so it can't simply be pointed at a test schema).
"""
from __future__ import annotations

import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
import yaml

from dais.api.runs import RunRegistry
from dais.api.worker import execute_pipeline_background
from dais.reference.currencies import ensure_currency_reference
from dais.resilience.connectors.postgres_connector import PostgresConnector
from dais.spec.models import PipelineSpec
from tests.conftest import _local_pg_creds, requires_local_postgres

pytestmark = requires_local_postgres

REPO = Path(__file__).parent.parent
SPEC_PATH = REPO / "specs" / "fx_rates_ingest.yaml"
SAMPLE = REPO / "docs" / "samples" / "FX_RATES_202609201200.csv"


def _connector_factory(spec):
    creds = _local_pg_creds()
    params = dict(host=creds["host"], port=creds["port"], dbname=creds["dbname"], user=creds["user"], password=creds["password"])
    return PostgresConnector(retry_cfg=spec.resilience.retry, **params), params


def _spec(tmp_path, schema) -> PipelineSpec:
    data = yaml.safe_load(SPEC_PATH.read_text(encoding="utf-8"))
    data["raw"]["schema"] = schema
    data["stage"]["schema"] = schema
    data["monitoring"]["schema"] = schema
    loc = data["source"]["location"]
    loc["path"] = str(tmp_path / "incoming")
    loc["archive"]["path"] = str(tmp_path / "archive")
    data["quality"]["quarantine"]["location"] = str(tmp_path / "quarantine")
    (tmp_path / "incoming").mkdir()
    (tmp_path / "quarantine").mkdir()
    return PipelineSpec.model_validate(data)


def _run(spec, file_path):
    registry = RunRegistry()
    record = registry.create(spec.pipeline_name, str(file_path), "checksum")
    execute_pipeline_background(registry, spec, str(file_path), _connector_factory, record.run_id, None, lambda s: None)
    return registry.get(record.run_id)


def _stage_rows(schema):
    c = _local_pg_creds()
    conn = psycopg2.connect(host=c["host"], port=c["port"], dbname=c["dbname"], user=c["user"], password=c["password"])
    try:
        with conn.cursor() as cur:
            cur.execute(f'SELECT "fromCCY", "toCCY", "fxRate", "sourceType" FROM "{schema}".fx_rates_stage ORDER BY 1, 2')
            return cur.fetchall()
    finally:
        conn.close()


def test_currency_reference_is_created_seeded_and_idempotent(pg_connector):
    ensure_currency_reference(pg_connector)
    assert ensure_currency_reference(pg_connector) == []
    for code in ("USD", "INR", "AUD", "HKD"):  # every currency in the sample file
        assert pg_connector.value_exists("core", "t_ref_currency", "currency_code", code)


def test_sample_file_lands_in_stage_and_moves_to_the_dated_archive(pg_connector, test_schema, tmp_path):
    ensure_currency_reference(pg_connector)
    spec = _spec(tmp_path, test_schema)
    src = tmp_path / "incoming" / "FX_RATES_202609201200.csv"
    src.write_text(SAMPLE.read_text(encoding="utf-8"), encoding="utf-8")

    record = _run(spec, src)

    assert (record.status, record.layer_reached) == ("succeeded", "stage")
    assert [r[:2] for r in _stage_rows(test_schema)] == [("USD", "AUD"), ("USD", "HKD"), ("USD", "INR"), ("USD", "USD")]
    assert not src.exists()
    today = datetime.now(timezone.utc).strftime("%Y%m%d")
    assert (tmp_path / "archive" / today / src.name).is_file()


def test_unknown_currency_row_is_quarantined_and_the_rest_still_land(pg_connector, test_schema, tmp_path):
    ensure_currency_reference(pg_connector)
    spec = _spec(tmp_path, test_schema)
    src = tmp_path / "incoming" / "FX_RATES_202609201200.csv"
    src.write_text(
        "fxDate,fromCCY,toCCY,fxRate,sourceType\n"
        "20260920,USD,INR,100,Reuters12PM\n"
        "20260920,USD,ZZZ,3,Reuters12PM\n",
        encoding="utf-8",
    )

    record = _run(spec, src)

    assert record.status == "succeeded"
    assert [r[:2] for r in _stage_rows(test_schema)] == [("USD", "INR")]
    assert any((tmp_path / "quarantine").iterdir()), "the ZZZ row should be written to quarantine"


def test_a_quarantined_file_is_not_archived(pg_connector, test_schema, tmp_path):
    ensure_currency_reference(pg_connector)
    spec = _spec(tmp_path, test_schema)
    src = tmp_path / "incoming" / "FX_RATES_202609201200.csv"
    src.write_text("x\n", encoding="utf-8")  # under the control gate's minimum size

    record = _run(spec, src)

    assert record.status == "quarantined"
    assert src.exists(), "a file that did not succeed must stay in incoming/ for review"
    assert not (tmp_path / "archive").exists()


def test_gold_sql_computes_latest_prior_change_and_inverse(pg_connector, test_schema):
    creds = _local_pg_creds()
    env = {
        **os.environ,
        "DBT_PG_HOST": str(creds["host"]), "DBT_PG_PORT": str(creds["port"]), "DBT_PG_DBNAME": creds["dbname"],
        "DBT_PG_USER": creds["user"], "DBT_PG_PASSWORD": creds["password"], "DBT_PG_SCHEMA": test_schema,
    }
    proc = subprocess.run(
        [sys.executable, "-m", "dbt.cli.main", "compile", "--project-dir", str(REPO / "dbt"),
         "--profiles-dir", str(REPO / "dbt"), "--select", "tag:fx_rates_gold"],
        env=env, capture_output=True, text=True,
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr
    compiled = REPO / "dbt" / "target" / "compiled" / "dais_gold" / "models"
    stg_sql = (compiled / "staging" / "fx_rates_ingest" / "stg_fx_rates.sql").read_text(encoding="utf-8")
    mart_sql = (compiled / "marts" / "fx" / "fx_rates_gold.sql").read_text(encoding="utf-8")
    assert '"data_in"."fx_rates_stage"' in stg_sql
    stg_sql = stg_sql.replace('"data_in"."fx_rates_stage"', f'"{test_schema}"."fx_rates_stage"')

    c = psycopg2.connect(host=creds["host"], port=creds["port"], dbname=creds["dbname"], user=creds["user"], password=creds["password"])
    c.autocommit = True
    with c.cursor() as cur:
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{test_schema}"')
        cur.execute(
            f'CREATE TABLE "{test_schema}".fx_rates_stage ("fxDate" date, "fromCCY" text, "toCCY" text, "fxRate" numeric, "sourceType" text)'
        )
        cur.execute(
            f'INSERT INTO "{test_schema}".fx_rates_stage VALUES '
            "('2026-09-19','USD','INR',80,'Reuters12PM'),"
            "('2026-09-20','USD','INR',100,'Reuters12PM'),"
            "('2026-09-20','USD','INR',90,'Reuters7AM'),"
            "('2026-09-20','USD','USD',1,'Reuters7AM')"
        )
        cur.execute(f'CREATE VIEW "{test_schema}".stg_fx_rates AS {stg_sql}')
        cur.execute(f'CREATE TABLE "{test_schema}".fx_rates_gold AS {mart_sql}')
        cur.execute(
            f'SELECT from_ccy, to_ccy, source_type, latest_fx_date, latest_rate, prior_rate, change_pct, inverse_rate '
            f'FROM "{test_schema}".fx_rates_gold ORDER BY 1, 2, 3'
        )
        rows = {(r[0], r[1], r[2]): r[3:] for r in cur.fetchall()}
    c.close()

    from datetime import date
    from decimal import Decimal

    # 12PM snapshot: latest 100 vs prior 80 -> +25%, inverse 0.01
    assert rows[("USD", "INR", "Reuters12PM")] == (date(2026, 9, 20), Decimal("100"), Decimal("80"), Decimal("25.0000"), Decimal("0.01000000"))
    # the 7AM snapshot is its own grain: no prior, so no change - never compared to the 12PM one
    r7 = rows[("USD", "INR", "Reuters7AM")]
    assert (r7[2], r7[3], r7[4]) == (None, None, Decimal("0.01111111"))
    assert len(rows) == 3
