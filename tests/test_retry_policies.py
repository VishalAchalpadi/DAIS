import pytest

from dais.resilience.retry_policies import with_retry
from dais.spec.models import RetryConfig


def _retry_cfg(max_attempts=3, base_delay_seconds=0.001, jitter=False, backoff="exponential"):
    return RetryConfig(
        max_attempts=max_attempts,
        backoff=backoff,
        base_delay_seconds=base_delay_seconds,
        jitter=jitter,
    )


def test_succeeds_without_retry_when_first_call_works():
    calls = {"n": 0}

    def flaky():
        calls["n"] += 1
        return "ok"

    result = with_retry(flaky, _retry_cfg())
    assert result == "ok"
    assert calls["n"] == 1


def test_retries_then_succeeds():
    calls = {"n": 0}

    def flaky():
        calls["n"] += 1
        if calls["n"] < 3:
            raise ConnectionError("transient")
        return "ok"

    result = with_retry(flaky, _retry_cfg(max_attempts=5))
    assert result == "ok"
    assert calls["n"] == 3


def test_retry_exhaustion_raises_original_exception_and_stops_at_max_attempts():
    calls = {"n": 0}

    def always_fails():
        calls["n"] += 1
        raise ConnectionError("still down")

    with pytest.raises(ConnectionError, match="still down"):
        with_retry(always_fails, _retry_cfg(max_attempts=4))

    assert calls["n"] == 4  # never retries beyond max_attempts, never fails silently


def test_only_retries_configured_exception_types():
    calls = {"n": 0}

    def raises_value_error():
        calls["n"] += 1
        raise ValueError("not transient")

    with pytest.raises(ValueError):
        with_retry(raises_value_error, _retry_cfg(max_attempts=5), exceptions=(ConnectionError,))

    assert calls["n"] == 1  # ValueError isn't in the retryable set - fails immediately


def test_fixed_backoff_mode():
    calls = {"n": 0}

    def flaky():
        calls["n"] += 1
        if calls["n"] < 2:
            raise ConnectionError("blip")
        return "ok"

    result = with_retry(flaky, _retry_cfg(backoff="fixed", max_attempts=3))
    assert result == "ok"
