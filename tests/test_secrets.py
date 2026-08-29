import logging
from unittest.mock import MagicMock

import pytest
import yaml

from dais.secrets.factory import get_secrets_provider
from dais.secrets.hardcoded_provider import HardcodedSecretsProvider
from dais.secrets.vault_provider import VaultSecretsProvider


# ---------------------------------------------------------------------------
# HardcodedSecretsProvider
# ---------------------------------------------------------------------------

@pytest.fixture
def local_secrets_file(tmp_path):
    path = tmp_path / "secrets.local.yaml"
    path.write_text(
        yaml.dump(
            {
                "aurora_postgres_prod": {
                    "host": "localhost",
                    "port": 5432,
                    "dbname": "GEODS",
                    "user": "postgres",
                    "password": "hunter2",
                }
            }
        ),
        encoding="utf-8",
    )
    return path


def test_hardcoded_provider_resolves_a_known_secret(local_secrets_file):
    provider = HardcodedSecretsProvider(local_secrets_file)
    secret = provider.get_secret("aurora_postgres_prod")
    assert secret["host"] == "localhost"
    assert secret["user"] == "postgres"


def test_hardcoded_provider_unknown_secret_raises(local_secrets_file):
    provider = HardcodedSecretsProvider(local_secrets_file)
    with pytest.raises(KeyError, match="no secret named"):
        provider.get_secret("does_not_exist")


def test_hardcoded_provider_missing_file_raises():
    provider = HardcodedSecretsProvider("/nonexistent/secrets.local.yaml")
    with pytest.raises(FileNotFoundError):
        provider.get_secret("anything")


def test_hardcoded_provider_logs_warning_on_every_use(local_secrets_file, caplog):
    import structlog

    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(logging.WARNING),
        logger_factory=structlog.stdlib.LoggerFactory(),
    )
    provider = HardcodedSecretsProvider(local_secrets_file)
    with caplog.at_level(logging.WARNING):
        provider.get_secret("aurora_postgres_prod")
        provider.get_secret("aurora_postgres_prod")

    warnings = [r for r in caplog.records if "hardcoded secrets provider" in r.getMessage()]
    assert len(warnings) == 2  # every call warns, not just the first


# ---------------------------------------------------------------------------
# VaultSecretsProvider
# ---------------------------------------------------------------------------

def test_vault_provider_resolves_a_secret_via_hvac_client():
    provider = VaultSecretsProvider(vault_addr="http://vault.local", vault_token="fake-token")
    provider._client = MagicMock()
    provider._client.is_authenticated.return_value = True
    provider._client.secrets.kv.v2.read_secret_version.return_value = {
        "data": {"data": {"host": "aurora.internal", "user": "app"}}
    }

    secret = provider.get_secret("aurora_postgres_prod")

    assert secret == {"host": "aurora.internal", "user": "app"}
    provider._client.secrets.kv.v2.read_secret_version.assert_called_once_with(
        path="aurora_postgres_prod", mount_point="secret", raise_on_deleted_version=True
    )


def test_vault_provider_raises_when_not_authenticated():
    provider = VaultSecretsProvider(vault_addr="http://vault.local", vault_token="bad-token")
    provider._client = MagicMock()
    provider._client.is_authenticated.return_value = False

    with pytest.raises(RuntimeError, match="not authenticated"):
        provider.get_secret("aurora_postgres_prod")


# ---------------------------------------------------------------------------
# factory - env-level switch, not spec-selectable
# ---------------------------------------------------------------------------

def test_factory_defaults_to_vault_when_unset(monkeypatch):
    monkeypatch.delenv("SECRETS_PROVIDER", raising=False)
    assert isinstance(get_secrets_provider(), VaultSecretsProvider)


def test_factory_returns_hardcoded_only_when_explicitly_selected(monkeypatch):
    monkeypatch.setenv("SECRETS_PROVIDER", "hardcoded")
    assert isinstance(get_secrets_provider(), HardcodedSecretsProvider)


def test_factory_unrecognized_value_falls_back_to_vault_not_hardcoded(monkeypatch):
    monkeypatch.setenv("SECRETS_PROVIDER", "typo_value")
    # never a silent fallback to the hardcoded/insecure path
    assert isinstance(get_secrets_provider(), VaultSecretsProvider)


def test_database_config_has_no_provider_field():
    # Structural guarantee that a spec can't select the hardcoded provider -
    # DatabaseConfig only carries a connection *name*, never a provider choice.
    from dais.spec.models import DatabaseConfig

    assert "provider" not in DatabaseConfig.model_fields
