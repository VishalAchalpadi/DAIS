"""PHASE 1 ONLY fallback, for use while Vault infra is being set up.
Reads from a local, git-ignored secrets.local.yaml instead of Vault.

Must be selected explicitly via the SECRETS_PROVIDER=hardcoded
environment variable (see secrets/factory.py) - never a silent default -
and logs a loud warning on every single use, so it can never be mistaken
for the real path once Vault is live. Deliberately not selectable from
a pipeline spec itself, so it can't accidentally ship inside a spec file
that gets reused later.
"""
from __future__ import annotations

from pathlib import Path

import structlog
import yaml

from dais.secrets.base import SecretsProvider

log = structlog.get_logger(__name__)

DEFAULT_PATH = Path("secrets.local.yaml")


class HardcodedSecretsProvider(SecretsProvider):
    def __init__(self, path: str | Path = DEFAULT_PATH):
        self.path = Path(path)

    def get_secret(self, name: str) -> dict[str, str]:
        log.warning(
            "WARNING: using hardcoded secrets provider - not for production use",
            secret_name=name,
            path=str(self.path),
        )
        if not self.path.is_file():
            raise FileNotFoundError(f"hardcoded secrets file not found: {self.path}")

        data = yaml.safe_load(self.path.read_text(encoding="utf-8")) or {}
        if name not in data:
            raise KeyError(f"no secret named {name!r} in {self.path}")
        return data[name]
