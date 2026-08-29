"""Retry wrapper for every external call (S3, Postgres, API). Exponential
backoff with jitter, configurable per spec via `resilience.retry`. Never
fails silently - after attempts are exhausted, the original exception is
re-raised (with a structured log) rather than swallowed.
"""
from __future__ import annotations

from typing import Callable, TypeVar

import structlog
from tenacity import (
    RetryCallState,
    Retrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
    wait_fixed,
)

from dais.spec.models import RetryConfig

log = structlog.get_logger(__name__)

T = TypeVar("T")


def _before_sleep(retry_state: RetryCallState) -> None:
    log.warning(
        "retrying_after_failure",
        attempt=retry_state.attempt_number,
        wait_seconds=retry_state.next_action.sleep if retry_state.next_action else None,
        exception=repr(retry_state.outcome.exception()) if retry_state.outcome else None,
    )


def build_retrying(
    retry_cfg: RetryConfig, exceptions: tuple[type[BaseException], ...] = (Exception,)
) -> Retrying:
    if retry_cfg.backoff == "exponential":
        wait = wait_exponential_jitter(
            initial=retry_cfg.base_delay_seconds, jitter=1 if retry_cfg.jitter else 0
        )
    else:
        wait = wait_fixed(retry_cfg.base_delay_seconds)

    return Retrying(
        stop=stop_after_attempt(retry_cfg.max_attempts),
        wait=wait,
        retry=retry_if_exception_type(exceptions),
        before_sleep=_before_sleep,
        reraise=True,
    )


def with_retry(
    func: Callable[..., T],
    retry_cfg: RetryConfig,
    *args,
    exceptions: tuple[type[BaseException], ...] = (Exception,),
    **kwargs,
) -> T:
    """Runs func(*args, **kwargs) under the configured retry policy. On
    exhaustion, logs loudly and re-raises the last exception - callers
    must not treat a missing return value as success."""
    retrying = build_retrying(retry_cfg, exceptions)
    try:
        return retrying(func, *args, **kwargs)
    except Exception:
        log.error("retry_exhausted", func=getattr(func, "__name__", repr(func)), max_attempts=retry_cfg.max_attempts)
        raise
