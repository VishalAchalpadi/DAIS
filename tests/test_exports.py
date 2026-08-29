import polars as pl
import pytest
from pyiceberg.catalog.sql import SqlCatalog

from dais.medallion.exports import write_iceberg_export
from dais.spec.models import ExportConfig


@pytest.fixture
def iceberg_catalog(tmp_path):
    return SqlCatalog(
        "test",
        uri=f"sqlite:///{tmp_path}/catalog.db",
        warehouse=f"file://{tmp_path}/warehouse",
    )


def _export_cfg(**overrides):
    defaults = dict(
        enabled=True,
        format="parquet",
        target="iceberg",
        catalog="test",
        table="analytics.holdings_stage",
        location="s3://bucket/iceberg-warehouse/holdings_stage/",
    )
    defaults.update(overrides)
    return ExportConfig(**defaults)


def _df():
    return pl.DataFrame({"account_id": ["ACC01", "ACC02"], "quantity": [100.0, 200.0]})


def test_write_creates_namespace_and_table_on_first_write(iceberg_catalog):
    result = write_iceberg_export(_df(), _export_cfg(), iceberg_catalog)

    assert result.created_table is True
    assert result.row_count == 2
    loaded = iceberg_catalog.load_table("analytics.holdings_stage")
    assert loaded.scan().to_polars().height == 2


def test_write_appends_to_existing_table_on_second_write(iceberg_catalog):
    write_iceberg_export(_df(), _export_cfg(), iceberg_catalog)
    result = write_iceberg_export(_df(), _export_cfg(), iceberg_catalog)

    assert result.created_table is False
    loaded = iceberg_catalog.load_table("analytics.holdings_stage")
    assert loaded.scan().to_polars().height == 4  # both writes accumulated


def test_write_disabled_export_raises():
    with pytest.raises(ValueError, match="isn't enabled"):
        write_iceberg_export(_df(), _export_cfg(enabled=False), catalog=None)


def test_write_requires_dotted_table_identifier(iceberg_catalog):
    with pytest.raises(ValueError, match="namespace.table"):
        write_iceberg_export(_df(), _export_cfg(table="no_namespace"), iceberg_catalog)
