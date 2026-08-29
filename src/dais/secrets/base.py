"""Every connection reference in a spec (`connection: "aurora_postgres_prod"`)
is resolved through this interface, never read directly from YAML or
hardcoded in application code.
"""
from __future__ import annotations

from abc import ABC, abstractmethod


class SecretsProvider(ABC):
    @abstractmethod
    def get_secret(self, name: str) -> dict[str, str]:
        """Resolves a connection name to its fields (host/port/dbname/user/
        password, or whatever the secret actually contains)."""
