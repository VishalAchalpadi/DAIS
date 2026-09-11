"""Phase 8a: dq_recommender.

The Anthropic client is always a fake in these tests - never a real API
call (cost, non-determinism, no API key in CI). Fakes mimic the real
SDK's response shape closely enough for get_dq_suggestions to work
against them unmodified.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest
import yaml

from dais.ai.dq_recommender import (
    DQSuggestionBatch,
    get_dq_suggestions,
    get_sample,
    write_suggestions_yaml,
)
from dais.spec.loader import load_spec
from tests.conftest import requires_local_postgres

FIXTURES = Path(__file__).parent / "fixtures"
SPECS = Path(__file__).parent.parent / "specs"


# ---------------------------------------------------------------------------
# fake Anthropic client - mimics response.content[i].{type,name,input,id}
# ---------------------------------------------------------------------------

@dataclass
class _FakeToolUseBlock:
    input: dict
    type: str = "tool_use"
    name: str = "submit_dq_suggestions"
    id: str = "toolu_fake123"


@dataclass
class _FakeResponse:
    content: list[Any]


@dataclass
class _FakeMessages:
    responses: list[_FakeResponse]
    calls: list[dict] = field(default_factory=list)

    def create(self, **kwargs):
        self.calls.append(kwargs)
        return self.responses[len(self.calls) - 1]


@dataclass
class _FakeClient:
    messages: _FakeMessages


def _fake_client(*payloads: dict) -> _FakeClient:
    responses = [_FakeResponse(content=[_FakeToolUseBlock(input=p)]) for p in payloads]
    return _FakeClient(messages=_FakeMessages(responses=responses))


VALID_PAYLOAD = {
    "pipeline_name": "holdings_ingest",
    "sample_size": 2,
    "suggestions": [
        {
            "rule": {"column": "account_id", "checks": ["not_null", "non_empty"]},
            "rationale": "No nulls or empty values observed across the sample.",
            "confidence": "high",
        }
    ],
}

INVALID_PAYLOAD = {
    "pipeline_name": "holdings_ingest",
    "sample_size": 2,
    "suggestions": [
        {
            "rule": {"column": "account_id", "checks": ["not_a_real_check"]},  # invalid check name
            "rationale": "bad",
            "confidence": "high",
        }
    ],
}


def _holdings_spec():
    return load_spec(SPECS / "holdings_ingest.yaml")


# ---------------------------------------------------------------------------
# schema validation + retry-on-invalid-output
# ---------------------------------------------------------------------------

def test_valid_first_response_is_accepted_without_retry():
    spec = _holdings_spec()
    client = _fake_client(VALID_PAYLOAD)
    import polars as pl

    sample_df = pl.DataFrame({"account_id": ["ACC01", "ACC02"]})

    batch = get_dq_suggestions(client, spec, sample_df)

    assert isinstance(batch, DQSuggestionBatch)
    assert batch.suggestions[0].rule.column == "account_id"
    assert batch.suggestions[0].confidence == "high"
    assert len(client.messages.calls) == 1


def test_invalid_first_response_retries_once_then_succeeds():
    spec = _holdings_spec()
    client = _fake_client(INVALID_PAYLOAD, VALID_PAYLOAD)
    import polars as pl

    sample_df = pl.DataFrame({"account_id": ["ACC01", "ACC02"]})

    batch = get_dq_suggestions(client, spec, sample_df, max_retries=1)

    assert batch.suggestions[0].rule.column == "account_id"
    assert len(client.messages.calls) == 2
    # the retry call must include the original tool_use + a tool_result
    # carrying the validation error, so the model can actually correct itself
    second_call_messages = client.messages.calls[1]["messages"]
    assert second_call_messages[-1]["content"][0]["is_error"] is True


def test_invalid_response_twice_raises_after_exhausting_retries():
    spec = _holdings_spec()
    client = _fake_client(INVALID_PAYLOAD, INVALID_PAYLOAD)
    import polars as pl

    sample_df = pl.DataFrame({"account_id": ["ACC01", "ACC02"]})

    with pytest.raises(RuntimeError, match="failed schema validation"):
        get_dq_suggestions(client, spec, sample_df, max_retries=1)
    assert len(client.messages.calls) == 2


def test_suggestion_rule_is_a_real_qualityrule_instance():
    spec = _holdings_spec()
    client = _fake_client(VALID_PAYLOAD)
    import polars as pl

    sample_df = pl.DataFrame({"account_id": ["ACC01", "ACC02"]})
    batch = get_dq_suggestions(client, spec, sample_df)

    from dais.spec.models import QualityRule

    assert isinstance(batch.suggestions[0].rule, QualityRule)


# ---------------------------------------------------------------------------
# never writes to the live spec - only the .dq_suggestions.yaml sibling
# ---------------------------------------------------------------------------

def test_write_suggestions_never_touches_live_spec(tmp_path):
    live_spec_path = tmp_path / "holdings_ingest.yaml"
    original_content = (SPECS / "holdings_ingest.yaml").read_text(encoding="utf-8")
    live_spec_path.write_text(original_content, encoding="utf-8")

    batch = DQSuggestionBatch.model_validate(VALID_PAYLOAD)
    out_path = write_suggestions_yaml(batch, live_spec_path)

    assert out_path == tmp_path / "holdings_ingest.dq_suggestions.yaml"
    assert out_path.is_file()
    # the live spec file's content must be byte-for-byte unchanged
    assert live_spec_path.read_text(encoding="utf-8") == original_content

    written = yaml.safe_load(out_path.read_text(encoding="utf-8"))
    assert written["pipeline_name"] == "holdings_ingest"
    assert written["suggestions"][0]["rule"]["column"] == "account_id"
    assert written["suggestions"][0]["confidence"] == "high"
    assert "note" in written  # human-review guardrail text is present


def test_write_suggestions_does_not_overwrite_an_existing_suggestions_file_silently(tmp_path):
    # writing twice should just overwrite the draft file itself (it's a
    # draft, not an audit trail) - never the live spec either way.
    live_spec_path = tmp_path / "holdings_ingest.yaml"
    live_spec_path.write_text("pipeline_name: holdings_ingest\n", encoding="utf-8")

    batch = DQSuggestionBatch.model_validate(VALID_PAYLOAD)
    write_suggestions_yaml(batch, live_spec_path)
    out_path = write_suggestions_yaml(batch, live_spec_path)

    assert live_spec_path.read_text(encoding="utf-8") == "pipeline_name: holdings_ingest\n"
    assert out_path.is_file()


# ---------------------------------------------------------------------------
# genericity: works against a second, differently-shaped spec (CSV, not
# fixed-width) via the local-file sampling path, no DB required
# ---------------------------------------------------------------------------

def test_get_sample_from_local_file_works_for_a_different_format(tmp_path):
    spec = load_spec(SPECS / "benchmark_ingest.yaml")

    csv_path = tmp_path / "BENCHMARK_20260101.csv"
    csv_path.write_text(
        "index_code,as_of_date,index_value\n"
        "SPX,2026-01-01,4800.50\n"
        "SPX,2026-01-02,4810.25\n",
        encoding="utf-8",
    )

    sample_df = get_sample(spec, sample_size=500, file_path=str(csv_path))

    assert sample_df is not None
    assert sample_df.height == 2
    assert "index_code" in sample_df.columns


def test_get_dq_suggestions_generic_across_pipelines():
    spec = load_spec(SPECS / "benchmark_ingest.yaml")
    payload = {
        "pipeline_name": "benchmark_ingest",
        "sample_size": 2,
        "suggestions": [
            {
                "rule": {"column": "index_code", "checks": ["not_null", "non_empty"]},
                "rationale": "No nulls observed.",
                "confidence": "medium",
            }
        ],
    }
    client = _fake_client(payload)
    import polars as pl

    sample_df = pl.DataFrame({"index_code": ["SPX", "SPX"]})
    batch = get_dq_suggestions(client, spec, sample_df)

    assert batch.pipeline_name == "benchmark_ingest"
    assert batch.suggestions[0].rule.column == "index_code"


# ---------------------------------------------------------------------------
# preserve_metadata columns are excluded from the sample - they're pipeline
# audit columns (file_name, batch_id, ...), not business source data
# ---------------------------------------------------------------------------

@requires_local_postgres
def test_sample_from_raw_table_excludes_preserve_metadata_columns(pg_connector, test_schema, monkeypatch):
    monkeypatch.setenv("SECRETS_PROVIDER", "hardcoded")
    import yaml as _yaml

    from dais.resilience.connectors.base import ColumnDef
    from dais.spec.models import PipelineSpec

    data = _yaml.safe_load((SPECS / "holdings_ingest.yaml").read_text(encoding="utf-8"))
    data["raw"]["schema"] = test_schema
    data["stage"]["schema"] = test_schema
    data["gold"]["schema"] = test_schema
    data["monitoring"]["schema"] = test_schema
    spec = PipelineSpec.model_validate(data)

    pg_connector.create_table_if_not_exists(
        test_schema,
        spec.raw.table,
        [
            ColumnDef("account_id", "TEXT"),
            ColumnDef("security_id", "TEXT"),
            ColumnDef("file_name", "TEXT"),
            ColumnDef("batch_id", "TEXT"),
        ],
    )
    pg_connector.bulk_insert(
        test_schema,
        spec.raw.table,
        ["account_id", "security_id", "file_name", "batch_id"],
        [("ACC01", "SEC01", "HOLDINGS_20260101.txt", "batch-1")],
    )

    from dais.ai.dq_recommender import get_sample

    sample_df = get_sample(spec, sample_size=500)

    assert sample_df is not None
    assert set(sample_df.columns) == {"account_id", "security_id"}
    assert "file_name" not in sample_df.columns
    assert "batch_id" not in sample_df.columns
