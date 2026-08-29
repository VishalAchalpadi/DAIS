"""Test-only Postgres fixture.

Reads local dev credentials from secrets.local.yaml at the repo root.
This is a stand-in for the real SecretsProvider abstraction (Phase 5) -
tests don't wait on that; they just need a real connection now to prove
the Phase 2 connector/DDL/write-mode code actually works.
"""
from __future__ import annotations

import uuid
from pathlib import Path

import pytest
import yaml

from dais.resilience.connectors.postgres_connector import PostgresConnector

SECRETS_FILE = Path(__file__).parent.parent / "secrets.local.yaml"


def _local_pg_creds() -> dict | None:
    if not SECRETS_FILE.is_file():
        return None
    data = yaml.safe_load(SECRETS_FILE.read_text(encoding="utf-8"))
    return data.get("aurora_postgres_prod")


requires_local_postgres = pytest.mark.skipif(
    _local_pg_creds() is None,
    reason="secrets.local.yaml with an aurora_postgres_prod entry is required for DB integration tests",
)


@pytest.fixture
def pg_connector():
    creds = _local_pg_creds()
    connector = PostgresConnector(
        host=creds["host"],
        port=creds["port"],
        dbname=creds["dbname"],
        user=creds["user"],
        password=creds["password"],
    )
    yield connector
    connector.close()


@pytest.fixture
def test_schema(pg_connector):
    schema = f"dais_test_{uuid.uuid4().hex[:8]}"
    yield schema
    pg_connector.drop_schema_cascade(schema)


@pytest.fixture
def ref_currencies(pg_connector):
    """Ensures ref.currencies exists and is seeded - mirrors how this
    shared reference table would be maintained in a real deployment, so
    it's created idempotently and left in place, not torn down."""
    from dais.resilience.connectors.base import ColumnDef

    pg_connector.create_table_if_not_exists(
        "ref", "currencies", [ColumnDef("currency_code", "TEXT")], unique_columns=["currency_code"]
    )
    for code in ("USD", "EUR", "GBP"):
        if not pg_connector.value_exists("ref", "currencies", "currency_code", code):
            pg_connector.bulk_insert("ref", "currencies", ["currency_code"], [(code,)])
    return pg_connector
