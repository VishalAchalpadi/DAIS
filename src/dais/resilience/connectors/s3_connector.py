from __future__ import annotations

from typing import Callable, TypeVar

import boto3

from dais.resilience.retry_policies import with_retry
from dais.spec.models import RetryConfig

T = TypeVar("T")


def parse_s3_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("s3://"):
        raise ValueError(f"not an s3:// URI: {uri!r}")
    without_scheme = uri[len("s3://") :]
    bucket, _, prefix = without_scheme.partition("/")
    return bucket, prefix


class S3Connector:
    def __init__(self, retry_cfg: RetryConfig | None = None, **boto3_client_kwargs):
        self._client = boto3.client("s3", **boto3_client_kwargs)
        self._retry_cfg = retry_cfg

    def _run(self, fn: Callable[[], T]) -> T:
        if self._retry_cfg is None:
            return fn()
        return with_retry(fn, self._retry_cfg)

    def put_object(self, bucket: str, key: str, body: bytes) -> None:
        self._run(lambda: self._client.put_object(Bucket=bucket, Key=key, Body=body))

    def get_object(self, bucket: str, key: str) -> bytes:
        def _do():
            response = self._client.get_object(Bucket=bucket, Key=key)
            return response["Body"].read()

        return self._run(_do)

    def list_objects(self, bucket: str, prefix: str) -> list[str]:
        def _do():
            response = self._client.list_objects_v2(Bucket=bucket, Prefix=prefix)
            return [obj["Key"] for obj in response.get("Contents", [])]

        return self._run(_do)
