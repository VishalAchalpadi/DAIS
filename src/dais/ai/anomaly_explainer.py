"""Phase 8b: plain-language anomaly explanations via Claude, for an ops
alert. Describes only what the profile actually shows - current value,
trailing baseline, detection method/score - never speculates about a
root cause the data doesn't support. If a caller has genuine breakdown
metrics available (e.g. a per-currency sum), passing them in `context`
lets the explanation reference them; otherwise it sticks to the single
flagged number.
"""
from __future__ import annotations

import anthropic

from dais.ai.anomaly_detector import AnomalyResult

DEFAULT_MODEL = "claude-sonnet-5"

SYSTEM_PROMPT = """\
You write short, plain-language explanations of a statistical anomaly for \
an operations alert. You are given the flagged metric's current value, its \
trailing baseline (mean, standard deviation, and how many prior runs it's \
based on), and the detection method/threshold/score used.

Rules:
- Describe ONLY what the numbers actually show - the size and direction of \
the deviation from the trailing baseline, as one or two plain sentences.
- If additional breakdown context is provided, you may reference it - \
otherwise describe only the single flagged metric. NEVER invent a root \
cause ("driven by X") that isn't directly supported by data you were \
actually given.
- Suitable for an ops alert someone reads in a chat channel - no hedging \
disclaimers, no restating the raw numbers verbatim, just the plain-English \
takeaway.
"""


def _build_user_content(result: AnomalyResult, *, pipeline_name: str, context: dict | None) -> str:
    direction = "above" if result.current_value > result.baseline_mean else "below"
    lines = [
        f"Pipeline: {pipeline_name}",
        f"Flagged metric: {result.metric}",
        f"Current value: {result.current_value}",
        f"Trailing baseline mean: {result.baseline_mean} (stdev {result.baseline_stdev:.4f}, "
        f"based on {result.baseline_size} prior run(s))",
        f"Direction: current value is {direction} the trailing mean",
        f"Detection method: {result.method}, threshold {result.threshold}, score {result.score:.3f}",
    ]
    if context:
        lines.append(f"Additional breakdown context (use only if directly relevant): {context}")
    return "\n".join(lines)


def explain_anomaly(
    client: anthropic.Anthropic,
    result: AnomalyResult,
    *,
    pipeline_name: str,
    context: dict | None = None,
    model: str = DEFAULT_MODEL,
) -> str:
    response = client.messages.create(
        model=model,
        max_tokens=300,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": _build_user_content(result, pipeline_name=pipeline_name, context=context)}],
    )
    return "".join(block.text for block in response.content if getattr(block, "type", None) == "text").strip()


def explain_anomalies(
    client: anthropic.Anthropic,
    results: list[AnomalyResult],
    *,
    pipeline_name: str,
    model: str = DEFAULT_MODEL,
) -> dict[str, str]:
    return {r.metric: explain_anomaly(client, r, pipeline_name=pipeline_name, model=model) for r in results}
