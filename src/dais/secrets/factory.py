"""Selects a SecretsProvider by environment variable - an env-level
switch, deliberately NOT something a pipeline spec can choose, so the
hardcoded fallback can't accidentally ship inside a reusable spec file.

SECRETS_PROVIDER=hardcoded is the only way to get the Phase-1 fallback;
anything else (including unset) resolves to the production Vault path.
"""
from __future__ import annotations

import os

from dais.secrets.base import SecretsProvider
from dais.secrets.hardcoded_provider import HardcodedSecretsProvider
from dais.secrets.vault_provider import VaultSecretsProvider


def get_secrets_provider() -> SecretsProvider:
    if os.environ.get("SECRETS_PROVIDER") == "hardcoded":
        return HardcodedSecretsProvider()
    return VaultSecretsProvider()
