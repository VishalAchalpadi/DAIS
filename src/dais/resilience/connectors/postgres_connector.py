from __future__ import annotations

from typing import Any, Iterable

import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values

from dais.resilience.connectors.base import ColumnDef, DatabaseConnector


class PostgresConnector(DatabaseConnector):
    """Postgres implementation. Takes already-resolved connection params -
    resolving a spec's `database.connection` secret name into these is a
    SecretsProvider concern (Phase 5), not this class's job."""

    def __init__(self, host: str, port: int, dbname: str, user: str, password: str):
        self._conn = psycopg2.connect(
            host=host, port=port, dbname=dbname, user=user, password=password
        )

    def schema_exists(self, schema: str) -> bool:
        row = self.fetch_one(
            "SELECT 1 FROM information_schema.schemata WHERE schema_name = %(schema)s",
            {"schema": schema},
        )
        return row is not None

    def create_schema_if_not_exists(self, schema: str) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema))
            )
        self._conn.commit()

    def table_exists(self, schema: str, table: str) -> bool:
        row = self.fetch_one(
            """
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = %(schema)s AND table_name = %(table)s
            """,
            {"schema": schema, "table": table},
        )
        return row is not None

    def create_table_if_not_exists(
        self,
        schema: str,
        table: str,
        columns: list[ColumnDef],
        unique_columns: list[str] | None = None,
    ) -> None:
        self.create_schema_if_not_exists(schema)
        if self.table_exists(schema, table):
            return

        column_sql = [
            sql.SQL("{} {}").format(sql.Identifier(c.name), sql.SQL(c.sql_type))
            for c in columns
        ]
        if unique_columns:
            column_sql.append(
                sql.SQL("UNIQUE ({})").format(
                    sql.SQL(", ").join(sql.Identifier(c) for c in unique_columns)
                )
            )

        stmt = sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} ({})").format(
            sql.Identifier(schema), sql.Identifier(table), sql.SQL(", ").join(column_sql)
        )
        with self._conn.cursor() as cur:
            cur.execute(stmt)
        self._conn.commit()

    def execute(self, sql_text: str, params: dict[str, Any] | None = None) -> None:
        with self._conn.cursor() as cur:
            cur.execute(sql_text, params)
        self._conn.commit()

    def fetch_all(self, sql_text: str, params: dict[str, Any] | None = None) -> list[tuple]:
        with self._conn.cursor() as cur:
            cur.execute(sql_text, params)
            return cur.fetchall()

    def fetch_one(self, sql_text: str, params: dict[str, Any] | None = None) -> tuple | None:
        with self._conn.cursor() as cur:
            cur.execute(sql_text, params)
            return cur.fetchone()

    def value_exists(self, schema: str, table: str, column: str, value: Any) -> bool:
        stmt = sql.SQL("SELECT 1 FROM {}.{} WHERE {} = %(value)s LIMIT 1").format(
            sql.Identifier(schema), sql.Identifier(table), sql.Identifier(column)
        )
        with self._conn.cursor() as cur:
            cur.execute(stmt, {"value": value})
            return cur.fetchone() is not None

    def bulk_insert(
        self, schema: str, table: str, columns: list[str], rows: Iterable[tuple]
    ) -> int:
        rows = list(rows)
        if not rows:
            return 0
        stmt = sql.SQL("INSERT INTO {}.{} ({}) VALUES %s").format(
            sql.Identifier(schema),
            sql.Identifier(table),
            sql.SQL(", ").join(sql.Identifier(c) for c in columns),
        )
        with self._conn.cursor() as cur:
            execute_values(cur, stmt.as_string(self._conn), rows)
        self._conn.commit()
        return len(rows)

    def upsert(
        self,
        schema: str,
        table: str,
        columns: list[str],
        rows: Iterable[tuple],
        conflict_columns: list[str],
    ) -> int:
        rows = list(rows)
        if not rows:
            return 0

        update_columns = [c for c in columns if c not in conflict_columns]
        set_clause = sql.SQL(", ").join(
            sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c))
            for c in update_columns
        )
        stmt = sql.SQL(
            "INSERT INTO {}.{} ({}) VALUES %s ON CONFLICT ({}) DO UPDATE SET {}"
        ).format(
            sql.Identifier(schema),
            sql.Identifier(table),
            sql.SQL(", ").join(sql.Identifier(c) for c in columns),
            sql.SQL(", ").join(sql.Identifier(c) for c in conflict_columns),
            set_clause,
        )
        with self._conn.cursor() as cur:
            execute_values(cur, stmt.as_string(self._conn), rows)
        self._conn.commit()
        return len(rows)

    def truncate(self, schema: str, table: str) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                sql.SQL("TRUNCATE TABLE {}.{}").format(
                    sql.Identifier(schema), sql.Identifier(table)
                )
            )
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    def drop_schema_cascade(self, schema: str) -> None:
        """Test/dev-only teardown helper. Production pipeline code never
        drops or alters anything - see the immutability design principle."""
        with self._conn.cursor() as cur:
            cur.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema))
            )
        self._conn.commit()
