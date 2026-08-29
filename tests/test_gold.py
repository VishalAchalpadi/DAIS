from pathlib import Path

import polars as pl
import yaml

from dais.medallion.gold import run_gold
from dais.medallion.silver import land_stage
from dais.spec.models import PipelineSpec
from tests.conftest import _local_pg_creds, requires_local_postgres

pytestmark = requires_local_postgres

FIXTURE = Path(__file__).parent / "fixtures" / "valid_holdings_ingest.yaml"
DBT_PROJECT = str(Path(__file__).parent.parent / "dbt")


def _spec_for_schema(schema: str) -> PipelineSpec:
    data = yaml.safe_load(FIXTURE.read_text(encoding="utf-8"))
    data["raw"]["schema"] = schema
    data["stage"]["schema"] = schema
    data["gold"]["schema"] = schema
    data["gold"]["dbt_project"] = DBT_PROJECT
    return PipelineSpec.model_validate(data)


def test_dbt_gold_materializes_stage_data(pg_connector, test_schema):
    spec = _spec_for_schema(test_schema)

    stage_df = pl.DataFrame(
        {
            "account_id": ["ACC01", "ACC02"],
            "security_id": ["SEC01", "SEC02"],
            "as_of_date": ["2026-01-01", "2026-01-01"],
            "quantity": [100.0, 200.0],
        }
    )
    land_stage(stage_df, spec, pg_connector)

    creds = _local_pg_creds()
    result = run_gold(
        spec,
        host=creds["host"],
        port=creds["port"],
        dbname=creds["dbname"],
        user=creds["user"],
        password=creds["password"],
    )

    assert result.success, result.stdout + result.stderr
    rows = pg_connector.fetch_all(
        f'SELECT account_id FROM "{test_schema}"."holdings_gold" ORDER BY account_id'
    )
    assert [r[0] for r in rows] == ["ACC01", "ACC02"]
