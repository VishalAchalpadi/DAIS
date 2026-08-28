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
