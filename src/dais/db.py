"""Resolves a spec's `database.connection` name into a live connector,
via whichever SecretsProvider is active (env-level switch, see
secrets/factory.py). Shared by the API worker and the CLI so connection
resolution isn't duplicated per entry point.
"""
from __future__ import annotations

from dais.resilience.connectors.postgres_connector import PostgresConnector
from dais.secrets.factory import get_secrets_provider
from dais.spec.models import PipelineSpec


def build_connector_for_spec(spec: PipelineSpec) -> tuple[PostgresConnector, dict]:
    if spec.database.platform != "postgres":
        raise NotImplementedError(
            f"platform {spec.database.platform!r} has no connector implementation yet"
        )

    secret = get_secrets_provider().get_secret(spec.database.connection)
    connection_params = {
        "host": secret["host"],
        "port": int(secret["port"]),
        "dbname": secret["dbname"],
        "user": secret["user"],
        "password": secret["password"],
    }
    connector = PostgresConnector(retry_cfg=spec.resilience.retry, **connection_params)
    return connector, connection_params
