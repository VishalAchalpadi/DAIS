"""Production path: resolves a connection name to a real secret in
HashiCorp Vault Cloud via hvac. Expects a KV v2 secret at
`<mount_point>/<name>` containing the connection fields.
"""
from __future__ import annotations

import os

import hvac

from dais.secrets.base import SecretsProvider


class VaultSecretsProvider(SecretsProvider):
    def __init__(
        self,
        vault_addr: str | None = None,
        vault_token: str | None = None,
        mount_point: str = "secret",
    ):
        self._client = hvac.Client(
            url=vault_addr or os.environ.get("VAULT_ADDR"),
            token=vault_token or os.environ.get("VAULT_TOKEN"),
        )
        self.mount_point = mount_point

    def get_secret(self, name: str) -> dict[str, str]:
        if not self._client.is_authenticated():
            raise RuntimeError(
                "Vault client is not authenticated - check VAULT_ADDR/VAULT_TOKEN"
            )
        response = self._client.secrets.kv.v2.read_secret_version(
            path=name, mount_point=self.mount_point, raise_on_deleted_version=True
        )
        return response["data"]["data"]
