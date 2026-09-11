"""Fake Anthropic client throughout - never a real API call in the suite."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from dais.ai.anomaly_detector import AnomalyResult
from dais.ai.anomaly_explainer import _build_user_content, explain_anomalies, explain_anomaly


@dataclass
class _FakeTextBlock:
    text: str
    type: str = "text"


@dataclass
class _FakeResponse:
    content: list[Any]


@dataclass
class _FakeMessages:
    reply_text: str
    calls: list[dict] = field(default_factory=list)

    def create(self, **kwargs):
        self.calls.append(kwargs)
        return _FakeResponse(content=[_FakeTextBlock(text=self.reply_text)])


@dataclass
class _FakeClient:
    messages: _FakeMessages


def _result(**overrides) -> AnomalyResult:
    defaults = dict(
        metric="market_value_sum",
        current_value=100_000.0,
        baseline_mean=1_000_000.0,
        baseline_stdev=50_000.0,
        baseline_size=20,
        method="pct_change",
        threshold=0.20,
        score=-0.90,
        is_anomaly=True,
    )
    defaults.update(overrides)
    return AnomalyResult(**defaults)


def test_user_content_references_only_the_real_flagged_numbers():
    result = _result()
    content = _build_user_content(result, pipeline_name="asset_ingest", context=None)

    assert "market_value_sum" in content
    assert "100000.0" in content
    assert "1000000.0" in content
    assert "below" in content  # current (100k) is below baseline mean (1M)
    # no fabricated metric name should appear
    assert "quantity" not in content
    assert "currency" not in content


def test_direction_is_above_when_current_exceeds_baseline():
    result = _result(current_value=2_000_000.0, baseline_mean=1_000_000.0)
    content = _build_user_content(result, pipeline_name="asset_ingest", context=None)
    assert "above" in content


def test_explain_anomaly_returns_the_fake_clients_text():
    client = _FakeClient(messages=_FakeMessages(reply_text="Total market value is 90% below the 20-day average."))
    result = _result()

    explanation = explain_anomaly(client, result, pipeline_name="asset_ingest")

    assert explanation == "Total market value is 90% below the 20-day average."
    assert len(client.messages.calls) == 1


def test_explain_anomalies_calls_once_per_flagged_metric():
    client = _FakeClient(messages=_FakeMessages(reply_text="Explanation."))
    results = [
        _result(metric="market_value_sum"),
        _result(metric="quantity_avg", current_value=5.0, baseline_mean=10.0),
    ]

    explanations = explain_anomalies(client, results, pipeline_name="asset_ingest")

    assert set(explanations) == {"market_value_sum", "quantity_avg"}
    assert len(client.messages.calls) == 2


def test_context_is_included_only_when_provided():
    result = _result()
    with_context = _build_user_content(result, pipeline_name="asset_ingest", context={"EUR_sum": 10_000.0})
    without_context = _build_user_content(result, pipeline_name="asset_ingest", context=None)

    assert "EUR_sum" in with_context
    assert "EUR_sum" not in without_context
