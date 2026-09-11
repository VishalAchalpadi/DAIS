"""AI-assisted DQ rule recommendation - Phase 8a.

Offline authoring aid only: given a sample of a pipeline's raw data,
asks Claude to draft `quality.rules`-shaped suggestions (reusing the
real `QualityRule` pydantic model from spec/models.py - not a parallel
schema), each with a plain-language rationale and a confidence level
for a human reviewer to prioritize.

Guardrails (see specs/*.dq_suggestions.yaml, never specs/*.yaml):
- This module is NEVER invoked as part of a normal pipeline run - only
  via the standalone `dais-recommend-dq` command.
- Output always goes to a separate `<pipeline>.dq_suggestions.yaml`
  sibling file next to the spec. It never writes to, overwrites, or
  auto-merges into a live spec. Promoting a suggestion into
  quality.rules is always a manual, human-driven edit.
- It never proposes a `lookup` (SQL reference-data) rule - the model
  has no visibility into what reference tables actually exist in this
  deployment, so that would be pure speculation rather than something
  inferred from the sample.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

import anthropic
import polars as pl
import yaml
from pydantic import BaseModel, ValidationError

from dais.db import build_connector_for_spec
from dais.parsers import get_parser
from dais.spec.loader import SpecLoadError, load_spec
from dais.spec.models import PipelineSpec, QualityRule

DEFAULT_SAMPLE_SIZE = 500
DEFAULT_MODEL = "claude-sonnet-5"
MAX_RETRIES = 1
TOOL_NAME = "submit_dq_suggestions"

SYSTEM_PROMPT = """\
You are a data quality analyst reviewing a sample of raw data before it is \
validated by an automated pipeline. Propose data quality rules for columns \
that would benefit from validation.

Rules for your suggestions:
- Rely ONLY on evidence visible in the provided sample - never invent a \
constraint the sample doesn't support.
- NEVER propose a `lookup` rule (SQL reference-data validation) - you have \
no visibility into what reference tables exist in this deployment. Leave \
that field out entirely.
- `checks` values must come from exactly this set: "not_null", \
"non_empty", "valid_date", "is_numeric", or a single-key comparison object \
like {"greater_than_or_equal": 0} (also: greater_than, less_than_or_equal, \
less_than).
- `valid_date` requires a `format` field on the same rule (a Python \
strptime pattern, e.g. "%Y%m%d" or "%Y-%m-%d") matching what you actually \
observed in the sample.
- `cast_to` may only be "date" or "decimal" (with precision/scale) - never \
invent another type. Leave it out for identifier/code columns that should \
stay text.
- Every suggestion needs a plain-language `rationale` citing what you \
actually observed (e.g. "no nulls or empty values across all 500 sampled \
rows"), and a `confidence` of "high", "medium", or "low".
- Be conservative about confidence when the sample is small relative to \
the claim: a categorical column where every sampled value happens to be \
identical could reflect a real constraint, or could just be bad luck of \
sampling - prefer "low" or "medium" confidence in that case and say so in \
the rationale, rather than overclaiming "high".
- Only propose rules for columns actually present in the sample.
- Submit your answer using the submit_dq_suggestions tool - do not respond \
in plain text.
"""


class DQSuggestion(BaseModel):
    rule: QualityRule
    rationale: str
    confidence: Literal["high", "medium", "low"]


class DQSuggestionBatch(BaseModel):
    pipeline_name: str
    sample_size: int
    suggestions: list[DQSuggestion]


def _sample_from_raw_table(spec: PipelineSpec, sample_size: int) -> pl.DataFrame | None:
    """None if the raw table doesn't exist yet - e.g. authoring a spec
    before the pipeline has ever run."""
    connector, _ = build_connector_for_spec(spec)
    try:
        if not connector.table_exists(spec.raw.schema_, spec.raw.table):
            return None
        all_columns = [
            row[0]
            for row in connector.fetch_all(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = %(schema)s AND table_name = %(table)s "
                "ORDER BY ordinal_position",
                {"schema": spec.raw.schema_, "table": spec.raw.table},
            )
        ]
        # preserve_metadata columns (file_name, batch_id, etc.) are pipeline
        # audit columns, not business source data - a "no nulls observed"
        # rule for them is always true by construction and just adds noise
        # to what a human has to review.
        metadata_cols = set(spec.raw.preserve_metadata)
        columns = [c for c in all_columns if c not in metadata_cols]
        if not columns:
            return None
        quoted_cols = ", ".join(f'"{c}"' for c in columns)
        rows = connector.fetch_all(
            f'SELECT {quoted_cols} FROM "{spec.raw.schema_}"."{spec.raw.table}" '
            f"ORDER BY random() LIMIT %(n)s",
            {"n": sample_size},
        )
        return pl.DataFrame(rows, schema=columns, orient="row")
    finally:
        connector.close()


def _sample_from_file(spec: PipelineSpec, file_path: str, sample_size: int) -> pl.DataFrame:
    parser = get_parser(spec.parser.type)
    raw_bytes = Path(file_path).read_bytes()
    df = parser.parse(raw_bytes, spec.parser)
    n = min(sample_size, df.height)
    return df.sample(n=n) if n < df.height else df


def get_sample(spec: PipelineSpec, *, sample_size: int = DEFAULT_SAMPLE_SIZE, file_path: str | None = None) -> pl.DataFrame | None:
    """Prefers the raw table (real, already-validated-shape data) unless a
    local file is explicitly given - a spec author drafting rules before
    the pipeline has ever run has no raw table to sample from yet."""
    if file_path is not None:
        return _sample_from_file(spec, file_path, sample_size)
    return _sample_from_raw_table(spec, sample_size)


def _build_tool_schema() -> dict:
    return DQSuggestionBatch.model_json_schema()


def get_dq_suggestions(
    client: anthropic.Anthropic,
    spec: PipelineSpec,
    sample_df: pl.DataFrame,
    *,
    model: str = DEFAULT_MODEL,
    max_retries: int = MAX_RETRIES,
) -> DQSuggestionBatch:
    """Calls Claude with forced tool-use so the response is structured
    JSON, then validates it against DQSuggestionBatch (which embeds the
    real QualityRule model). If validation fails, retries up to
    `max_retries` times, feeding the validation error back to the model
    as a tool_result so it can correct itself - never silently accepts
    invalid output."""
    sample_rows = sample_df.to_dicts()
    tool = {
        "name": TOOL_NAME,
        "description": "Submit proposed data quality rule suggestions for human review.",
        "input_schema": _build_tool_schema(),
    }
    user_content = (
        f"Pipeline: {spec.pipeline_name}\n"
        f"Sample size: {len(sample_rows)} rows\n\n"
        f"Sample data (JSON records):\n{json.dumps(sample_rows, default=str, indent=2)}"
    )
    messages: list[dict] = [{"role": "user", "content": user_content}]

    last_error: ValidationError | None = None
    for attempt in range(max_retries + 1):
        response = client.messages.create(
            model=model,
            max_tokens=8192,
            system=SYSTEM_PROMPT,
            tools=[tool],
            tool_choice={"type": "tool", "name": TOOL_NAME},
            messages=messages,
        )
        tool_use_block = next((b for b in response.content if b.type == "tool_use"), None)
        if tool_use_block is None:
            raise RuntimeError("Claude did not return a tool_use block")

        try:
            return DQSuggestionBatch.model_validate(tool_use_block.input)
        except ValidationError as exc:
            last_error = exc
            if attempt >= max_retries:
                break
            messages.append({"role": "assistant", "content": response.content})
            messages.append(
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "tool_result",
                            "tool_use_id": tool_use_block.id,
                            "content": f"Your output failed schema validation: {exc}. Please correct it and resubmit.",
                            "is_error": True,
                        }
                    ],
                }
            )

    raise RuntimeError(
        f"Claude's output failed schema validation after {max_retries + 1} attempt(s): {last_error}"
    )


def write_suggestions_yaml(batch: DQSuggestionBatch, spec_path: Path) -> Path:
    """Writes to <spec_stem>.dq_suggestions.yaml, a sibling of spec_path.
    Never touches spec_path itself."""
    out_path = spec_path.parent / f"{spec_path.stem}.dq_suggestions.yaml"
    payload = {
        "pipeline_name": batch.pipeline_name,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "sample_size": batch.sample_size,
        "note": (
            "Draft DQ rule suggestions generated by dais-recommend-dq. Human "
            "review required before promoting any of these into quality.rules "
            "in the real spec - never auto-applied, never auto-merged."
        ),
        "suggestions": [
            {
                "rule": s.rule.model_dump(exclude_none=True, by_alias=True),
                "rationale": s.rationale,
                "confidence": s.confidence,
            }
            for s in batch.suggestions
        ],
    }
    out_path.write_text(yaml.dump(payload, sort_keys=False), encoding="utf-8")
    return out_path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="dais-recommend-dq")
    parser.add_argument("--spec", required=True, help="path to the pipeline spec YAML")
    parser.add_argument("--sample-size", type=int, default=DEFAULT_SAMPLE_SIZE)
    parser.add_argument(
        "--file",
        default=None,
        help="local sample file to use instead of the raw table (required if the pipeline has never run)",
    )
    parser.add_argument("--model", default=DEFAULT_MODEL)
    args = parser.parse_args(argv)

    try:
        spec = load_spec(args.spec)
    except SpecLoadError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    sample_df = get_sample(spec, sample_size=args.sample_size, file_path=args.file)
    if sample_df is None or sample_df.height == 0:
        print(
            "error: no sample data available - run the pipeline at least once, or pass --file",
            file=sys.stderr,
        )
        return 1

    client = anthropic.Anthropic()  # reads ANTHROPIC_API_KEY from the environment
    batch = get_dq_suggestions(client, spec, sample_df, model=args.model)
    out_path = write_suggestions_yaml(batch, Path(args.spec))
    print(f"wrote {len(batch.suggestions)} suggestion(s) to {out_path}")
    for s in batch.suggestions:
        print(f"  [{s.confidence:6}] {s.rule.column}: {s.rationale}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
