from __future__ import annotations

from typing import Any, Callable, Iterable, TypeVar

import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values

from dais.resilience.connectors.base import ColumnDef, DatabaseConnector
from dais.resilience.retry_policies import with_retry
from dais.spec.models import RetryConfig

# Only retry errors that are plausibly transient - a syntax error or
# constraint violation will never succeed on a later attempt.
_RETRYABLE_EXCEPTIONS = (psycopg2.OperationalError, psycopg2.InterfaceError)

T = TypeVar("T")


class PostgresConnector(DatabaseConnector):
    """Postgres implementation. Takes already-resolved connection params -
    resolving a spec's `database.connection` secret name into these is a
    SecretsProvider concern (Phase 5), not this class's job."""

    def __init__(
        self,
        host: str,
        port: int,
        dbname: str,
        user: str,
        password: str,
        retry_cfg: RetryConfig | None = None,
    ):
        self._conn = psycopg2.connect(
            host=host, port=port, dbname=dbname, user=user, password=password
        )
        self._retry_cfg = retry_cfg

    def _run(self, fn: Callable[[], T]) -> T:
        def _fn_with_rollback_on_error() -> T:
            try:
                return fn()
            except Exception:
                # A failed statement poisons the transaction until rolled
                # back - without this, every later call on this connection
                # (including a retry of this same call) would fail with
                # "current transaction is aborted", masking the real error.
                self._conn.rollback()
                raise

        if self._retry_cfg is None:
            return _fn_with_rollback_on_error()
        return with_retry(_fn_with_rollback_on_error, self._retry_cfg, exceptions=_RETRYABLE_EXCEPTIONS)

    def schema_exists(self, schema: str) -> bool:
        row = self.fetch_one(
            "SELECT 1 FROM information_schema.schemata WHERE schema_name = %(schema)s",
            {"schema": schema},
        )
        return row is not None

    def create_schema_if_not_exists(self, schema: str) -> None:
        def _do():
            with self._conn.cursor() as cur:
                cur.execute(
                    sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema))
                )
            self._conn.commit()

        self._run(_do)

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

        def _do():
            with self._conn.cursor() as cur:
                cur.execute(stmt)
            self._conn.commit()

        self._run(_do)

    def execute(self, sql_text: str, params: dict[str, Any] | None = None) -> None:
        def _do():
            with self._conn.cursor() as cur:
                cur.execute(sql_text, params)
            self._conn.commit()

        self._run(_do)

    def fetch_all(self, sql_text: str, params: dict[str, Any] | None = None) -> list[tuple]:
        def _do():
            with self._conn.cursor() as cur:
                cur.execute(sql_text, params)
                return cur.fetchall()

        return self._run(_do)

    def fetch_one(self, sql_text: str, params: dict[str, Any] | None = None) -> tuple | None:
        def _do():
            with self._conn.cursor() as cur:
                cur.execute(sql_text, params)
                return cur.fetchone()

        return self._run(_do)

    def value_exists(self, schema: str, table: str, column: str, value: Any) -> bool:
        stmt = sql.SQL("SELECT 1 FROM {}.{} WHERE {} = %(value)s LIMIT 1").format(
            sql.Identifier(schema), sql.Identifier(table), sql.Identifier(column)
        )

        def _do():
            with self._conn.cursor() as cur:
                cur.execute(stmt, {"value": value})
                return cur.fetchone() is not None

        return self._run(_do)

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

        def _do():
            with self._conn.cursor() as cur:
                execute_values(cur, stmt.as_string(self._conn), rows)
            self._conn.commit()

        self._run(_do)
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

        def _do():
            with self._conn.cursor() as cur:
                execute_values(cur, stmt.as_string(self._conn), rows)
            self._conn.commit()

        self._run(_do)
        return len(rows)

    def truncate(self, schema: str, table: str) -> None:
        def _do():
            with self._conn.cursor() as cur:
                cur.execute(
                    sql.SQL("TRUNCATE TABLE {}.{}").format(
                        sql.Identifier(schema), sql.Identifier(table)
                    )
                )
            self._conn.commit()

        self._run(_do)

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
