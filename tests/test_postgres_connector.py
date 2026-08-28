from dais.resilience.connectors.base import ColumnDef
from tests.conftest import requires_local_postgres

pytestmark = requires_local_postgres


def test_create_schema_and_table_idempotent(pg_connector, test_schema):
    assert not pg_connector.schema_exists(test_schema)
    pg_connector.create_schema_if_not_exists(test_schema)
    assert pg_connector.schema_exists(test_schema)
    # calling again must not raise
    pg_connector.create_schema_if_not_exists(test_schema)

    columns = [ColumnDef("id", "TEXT"), ColumnDef("val", "TEXT")]
    assert not pg_connector.table_exists(test_schema, "widgets")
    pg_connector.create_table_if_not_exists(test_schema, "widgets", columns)
    assert pg_connector.table_exists(test_schema, "widgets")
    # calling again must not raise or drop/alter
    pg_connector.bulk_insert(test_schema, "widgets", ["id", "val"], [("1", "a")])
    pg_connector.create_table_if_not_exists(test_schema, "widgets", columns)
    rows = pg_connector.fetch_all(f'SELECT id, val FROM "{test_schema}"."widgets"')
    assert rows == [("1", "a")]


def test_bulk_insert_and_value_exists(pg_connector, test_schema):
    columns = [ColumnDef("id", "TEXT"), ColumnDef("val", "TEXT")]
    pg_connector.create_table_if_not_exists(test_schema, "widgets", columns)

    n = pg_connector.bulk_insert(test_schema, "widgets", ["id", "val"], [("1", "a"), ("2", "b")])
    assert n == 2
    assert pg_connector.value_exists(test_schema, "widgets", "id", "1")
    assert not pg_connector.value_exists(test_schema, "widgets", "id", "999")


def test_upsert_inserts_new_and_updates_existing(pg_connector, test_schema):
    columns = [ColumnDef("id", "TEXT"), ColumnDef("val", "TEXT")]
    pg_connector.create_table_if_not_exists(test_schema, "widgets", columns, unique_columns=["id"])

    pg_connector.upsert(test_schema, "widgets", ["id", "val"], [("1", "a")], conflict_columns=["id"])
    pg_connector.upsert(
        test_schema,
        "widgets",
        ["id", "val"],
        [("1", "updated"), ("2", "b")],
        conflict_columns=["id"],
    )
    rows = dict(pg_connector.fetch_all(f'SELECT id, val FROM "{test_schema}"."widgets" ORDER BY id'))
    assert rows == {"1": "updated", "2": "b"}


def test_truncate_clears_rows_but_keeps_table(pg_connector, test_schema):
    columns = [ColumnDef("id", "TEXT")]
    pg_connector.create_table_if_not_exists(test_schema, "widgets", columns)
    pg_connector.bulk_insert(test_schema, "widgets", ["id"], [("1",), ("2",)])

    pg_connector.truncate(test_schema, "widgets")

    assert pg_connector.table_exists(test_schema, "widgets")
    assert pg_connector.fetch_all(f'SELECT id FROM "{test_schema}"."widgets"') == []
