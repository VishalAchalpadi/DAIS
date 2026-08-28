"""Abstract database connector interface. raw/stage/gold all speak through
one of these, selected by `database.platform` in the spec - never mixed
within a pipeline. Concrete implementations own connection lifecycle and
platform-specific SQL dialect details (quoting, upsert syntax, type names).
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Iterable


@dataclass(frozen=True)
class ColumnDef:
    name: str
    sql_type: str


class DatabaseConnector(ABC):
    @abstractmethod
    def schema_exists(self, schema: str) -> bool: ...

    @abstractmethod
    def create_schema_if_not_exists(self, schema: str) -> None: ...

    @abstractmethod
    def table_exists(self, schema: str, table: str) -> bool: ...

    @abstractmethod
    def create_table_if_not_exists(
        self,
        schema: str,
        table: str,
        columns: list[ColumnDef],
        unique_columns: list[str] | None = None,
    ) -> None:
        """Idempotent create. Never drops/alters an existing table."""

    @abstractmethod
    def execute(self, sql: str, params: dict[str, Any] | None = None) -> None: ...

    @abstractmethod
    def fetch_all(self, sql: str, params: dict[str, Any] | None = None) -> list[tuple]: ...

    @abstractmethod
    def fetch_one(self, sql: str, params: dict[str, Any] | None = None) -> tuple | None: ...

    @abstractmethod
    def value_exists(self, schema: str, table: str, column: str, value: Any) -> bool:
        """True if any row has `column` == `value`. Identifiers are quoted
        safely by the implementation - callers never build SQL strings."""

    @abstractmethod
    def bulk_insert(
        self, schema: str, table: str, columns: list[str], rows: Iterable[tuple]
    ) -> int:
        """Plain insert-only bulk load. Returns rows inserted."""

    @abstractmethod
    def upsert(
        self,
        schema: str,
        table: str,
        columns: list[str],
        rows: Iterable[tuple],
        conflict_columns: list[str],
    ) -> int:
        """Merge on conflict_columns: insert new rows, update existing ones.
        Returns rows affected."""

    @abstractmethod
    def truncate(self, schema: str, table: str) -> None: ...

    @abstractmethod
    def close(self) -> None: ...
