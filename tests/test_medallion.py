from pathlib import Path

import polars as pl
import pytest
import yaml

from dais.medallion.bronze import land_raw
from dais.medallion.silver import land_stage
from dais.spec.models import PipelineSpec
from tests.conftest import requires_local_postgres

pytestmark = requires_local_postgres

FIXTURE = Path(__file__).parent / "fixtures" / "valid_holdings_ingest.yaml"


def _spec_for_schema(schema: str) -> PipelineSpec:
    data = yaml.safe_load(FIXTURE.read_text(encoding="utf-8"))
    data["raw"]["schema"] = schema
    data["stage"]["schema"] = schema
    return PipelineSpec.model_validate(data)


def _sample_raw_df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "account_id": ["ACC01", "ACC02"],
            "security_id": ["SEC01", "SEC02"],
            "quantity": ["100", "200"],
        }
    )


# ---------------------------------------------------------------------------
# bronze / raw
# ---------------------------------------------------------------------------

def test_land_raw_creates_table_and_inserts_rows(pg_connector, test_schema):
    spec = _spec_for_schema(test_schema)
    df = _sample_raw_df()

    result = land_raw(
        df,
        spec,
        pg_connector,
        file_name="HOLDINGS_20260101.txt",
        file_path="s3://bucket/HOLDINGS_20260101.txt",
        file_bytes=b"fake file contents 1",
    )

    assert result.loaded is True
    assert result.row_count_out == 2
    assert pg_connector.table_exists(test_schema, spec.raw.table)

    rows = pg_connector.fetch_all(
        f'SELECT account_id, file_name, file_checksum FROM "{test_schema}"."{spec.raw.table}"'
        " ORDER BY account_id"
    )
    assert rows[0][0] == "ACC01"
    assert rows[0][1] == "HOLDINGS_20260101.txt"
    assert rows[0][2] == result.checksum


def test_land_raw_checksum_dedup_is_noop_on_rerun(pg_connector, test_schema):
    spec = _spec_for_schema(test_schema)
    df = _sample_raw_df()
    kwargs = dict(
        file_name="HOLDINGS_20260101.txt",
        file_path="s3://bucket/HOLDINGS_20260101.txt",
        file_bytes=b"identical file contents",
    )

    first = land_raw(df, spec, pg_connector, **kwargs)
    second = land_raw(df, spec, pg_connector, **kwargs)

    assert first.loaded is True
    assert second.loaded is False
    assert second.row_count_out == 0

    count = pg_connector.fetch_all(f'SELECT COUNT(*) FROM "{test_schema}"."{spec.raw.table}"')
    assert count[0][0] == 2  # only the first load's rows


def test_land_raw_different_checksum_loads_again(pg_connector, test_schema):
    spec = _spec_for_schema(test_schema)
    df = _sample_raw_df()

    land_raw(df, spec, pg_connector, file_name="a.txt", file_path="s3://b/a.txt", file_bytes=b"content A")
    result = land_raw(
        df, spec, pg_connector, file_name="b.txt", file_path="s3://b/b.txt", file_bytes=b"content B"
    )

    assert result.loaded is True
    count = pg_connector.fetch_all(f'SELECT COUNT(*) FROM "{test_schema}"."{spec.raw.table}"')
    assert count[0][0] == 4


# ---------------------------------------------------------------------------
# silver / stage
# ---------------------------------------------------------------------------

def _typed_stage_df(quantities=("100.0000", "200.0000")):
    return pl.DataFrame(
        {
            "account_id": ["ACC01", "ACC02"],
            "security_id": ["SEC01", "SEC02"],
            "as_of_date": ["2026-01-01", "2026-01-01"],
            "quantity": [float(q) for q in quantities],
        }
    )


def test_land_stage_upsert_inserts_then_updates(pg_connector, test_schema):
    spec = _spec_for_schema(test_schema)
    assert spec.stage.write_mode == "upsert"

    land_stage(_typed_stage_df(), spec, pg_connector)
    result = land_stage(_typed_stage_df(quantities=("999.0000", "200.0000")), spec, pg_connector)

    assert result.write_mode == "upsert"
    rows = dict(
        pg_connector.fetch_all(
            f'SELECT account_id, quantity FROM "{test_schema}"."{spec.stage.table}" ORDER BY account_id'
        )
    )
    assert rows["ACC01"] == 999.0


def test_land_stage_append_mode_keeps_both_loads(pg_connector, test_schema):
    data = yaml.safe_load(FIXTURE.read_text(encoding="utf-8"))
    data["raw"]["schema"] = test_schema
    data["stage"]["schema"] = test_schema
    data["stage"]["write_mode"] = "append"
    spec = PipelineSpec.model_validate(data)

    land_stage(_typed_stage_df(), spec, pg_connector)
    land_stage(_typed_stage_df(), spec, pg_connector)

    count = pg_connector.fetch_all(f'SELECT COUNT(*) FROM "{test_schema}"."{spec.stage.table}"')
    assert count[0][0] == 4


def test_land_stage_truncate_load_replaces_prior_rows(pg_connector, test_schema):
    data = yaml.safe_load(FIXTURE.read_text(encoding="utf-8"))
    data["raw"]["schema"] = test_schema
    data["stage"]["schema"] = test_schema
    data["stage"]["write_mode"] = "truncate_load"
    spec = PipelineSpec.model_validate(data)

    land_stage(_typed_stage_df(), spec, pg_connector)
    land_stage(_typed_stage_df(quantities=("5.0000", "6.0000")), spec, pg_connector)

    rows = pg_connector.fetch_all(f'SELECT quantity FROM "{test_schema}"."{spec.stage.table}"')
    assert len(rows) == 2
    assert set(r[0] for r in rows) == {5.0, 6.0}
