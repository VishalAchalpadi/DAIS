---
name: author-pipeline-spec
description: Author a new DAIS pipeline spec.yaml from a plain-language description of a data feed - reads the real source file, verifies the parser's actual output against it, and maps every PipelineSpec field deliberately rather than guessing from the description alone. Use whenever the user describes a new data feed/pipeline in plain language and wants a spec.yaml authored, or asks how to add a new pipeline to DAIS.
---

# Authoring a DAIS pipeline spec.yaml from a plain-language description

`PipelineSpec` (`src/dais/spec/models.py`) is the single source of truth
for the schema — read it directly (`Read` the file) before writing a spec
by hand, don't work from memory of past specs, since fields/validators can
change. `PipelineSpec.model_json_schema()` is also served live at
`GET /spec-schema` on the DAIS API and drives the spec-editor UI's
dropdowns — for a quick one-off, **prefer the UI**
(`http://localhost:8000/ui/spec-editor`) over hand-writing YAML, since it
enumerates every `Literal[...]`'s allowed values for you and validates
through the real model before saving. Hand-write only when scripting many
specs, or when learning the schema.

The single most important discipline here: a plain-language description of
a feed ("an equity fund positions file", "a daily holdings extract") is a
starting point, not a spec of the data's actual shape. Real files routinely
turn out more deeply nested, more irregularly formatted, or differently
keyed than the description implies — `specs/fund_positions_ingest.yaml` is
a real example where "an equity fund positions file" undersold how deeply
nested the actual FundsXML values were. Every step below exists to catch
that gap before it becomes a spec that looks plausible and fails on first
real run.

## Step-by-step method for a new pipeline

1. **Find and read the real source file(s)** before writing anything.
   Don't guess column names or structure from the request text alone.

2. **Identify the format and, for XML/JSON, verify the parser's actual
   capabilities against the real nesting** — don't assume the parser
   handles arbitrary structure. `src/dais/parsers/xml_parser.py`, for
   example, only flattens what it's told to (recursively, joining nested
   tag paths with `.` and attributes with `@`) from whatever
   `record_xpath` matches. Run the parser against a real sample file
   BEFORE writing quality rules, and read its actual output columns:
   ```python
   from dais.parsers import get_parser
   from dais.spec.models import ParserConfig
   df = get_parser("xml").parse(open(path, "rb").read(), ParserConfig(type="xml", record_xpath="..."))
   print(df.columns); print(df.to_dicts())
   ```
   Writing quality rules against columns that don't actually exist in the
   parsed output is the single easiest way to produce a spec that looks
   plausible and fails immediately on first real run.

3. **Map every field of `PipelineSpec` in order**, using the real column
   names discovered in step 2:
   - `pipeline_name` / `description` / `owner`: snake_case name matching
     `specs/<name>.yaml`; a one-line description of the feed; an owning
     team name (never leave a `<<placeholder>>` — the model rejects it).
   - `execution.stop_after`: `"raw"`, `"stage"`, or `"gold"`. Use `"stage"`
     if gold logic belongs in a separate `gold_builds/*.yaml` instead
     (check whether one already exists or is planned before deciding).
   - `database` / `monitoring`: match the conventions of existing specs in
     `specs/` (read a couple first) — almost always
     `platform: postgres`, `connection: "aurora_postgres_prod"`,
     `monitoring: {schema: control, table: process_monitor}`.
   - `source` / `parser`: `parser.type` MUST equal `source.format` (a
     model validator enforces this). `source.location.file_pattern` is
     documentation only — it is never used to auto-discover files at
     runtime (the actual file path is always passed explicitly to
     `/pipelines/{spec}/run`), so a literal filename with no `{date}`
     token is fine if that's genuinely how the feed arrives.
   - `control_gates`: sane `min`/`max` bounds on file size and row count
     for the real file size you observed — not arbitrary round numbers.
   - `raw`: `preserve_metadata` almost always all five of
     `[file_name, file_path, file_checksum, load_timestamp, batch_id]`;
     `checksum_dedup: true` unless there's a specific reason not to.
   - `quality.integrity_mode`: `"strict"` (one bad row quarantines the
     whole file) vs `"row_level"` (only the bad row) vs `"group_level"`
     (a whole group sharing `quarantine.group_by`'s key is quarantined
     together) — pick based on whether records in the file are truly
     independent (row_level/strict) or logically grouped (e.g. one
     portfolio's multiple holdings — group_level).
   - `quality.rules`: one entry per real column that needs validation.
     Checks are either bare names (`not_null`, `non_empty`, `valid_date`,
     `is_numeric`) or single-key comparisons (`{greater_than_or_equal: 0}`,
     etc.) — see `BARE_CHECK_NAMES`/`COMPARISON_CHECK_OPS` in
     `src/dais/spec/models.py` for the exhaustive list, don't invent new
     check names. `valid_date` requires a `format` string. Numeric columns
     needing DB casting need `cast_to: decimal` plus `precision`/`scale`
     sized to the real values observed (e.g. a percentage needs less
     precision than a large currency amount). Validation itself runs on
     Great Expectations under the hood (`src/dais/quality/`) — this field
     shape is unaffected by that; write rules the same way regardless.
   - `quality.quarantine`: `kind: local` for on-disk dev/demo pipelines,
     `s3` for anything pointing at a real bucket — `location` must match
     (`s3://...` vs a local path) or the model rejects it.
   - `stage.business_key` / `write_mode`: think concretely about whether
     the natural key you're about to declare is ACTUALLY unique across
     every file this pipeline will ever ingest, not just within one
     sample file. A per-record ID that resets/repeats across different
     dates or parent entities (e.g. `<Position><UniqueID>` scoped to one
     fund's one day, as opposed to a schema that carries the parent info
     down to each record) is not a safe `upsert` key — use `write_mode:
     append` instead and say so in a comment, rather than silently
     upserting into false collisions.
   - `gold` (optional, inline): only if this pipeline's own gold logic is
     simple and doesn't span other pipelines' data — otherwise it belongs
     in a separate `gold_builds/*.yaml` (`GoldBuildSpec`), which uses
     `depends_on` to name every pipeline it reads from and is what wires
     it into Dagster's asset graph.
   - `resilience.retry`: copy the convention from existing specs unless
     there's a specific reason to deviate (`max_attempts: 5, backoff:
     exponential, base_delay_seconds: 2, jitter: true`).
   - `lineage`: `namespace` should match the SAME namespace every other
     pipeline in this OpenLineage graph uses (so the lineage graph shows
     one connected graph, not islands) — check an existing spec's
     `lineage.namespace` rather than inventing a new one. `job_name`
     matches `pipeline_name`. This is what shows up in OpenMetadata once
     the pipeline runs (`src/dais/lineage/emitter.py` +
     `openmetadata_forwarder.py`) - no extra lineage config needed here
     beyond namespace/job_name.
   - `anomaly_detection` (optional): only add if actually wanted; metric
     names are `"row_count"` or `"<column>_<sum|avg|null_rate|distinct_count>"`
     — the column must be one that genuinely exists in the parsed output.

4. **Validate, then run for real** before calling it done:
   ```python
   from dais.spec.loader import load_spec
   spec = load_spec("specs/<name>.yaml")  # raises with a clear message if invalid
   ```
   Then actually run it end to end against local Postgres (a scratch
   schema, not a production one) using the same pattern
   `tests/conftest.py`'s fixtures use (`HardcodedSecretsProvider` /
   `SECRETS_PROVIDER=hardcoded`, real `PostgresConnector`, real
   `run_pipeline(...)`) and inspect the actual landed rows — a spec that
   merely loads/validates is not proof it works. Clean up the scratch
   schema afterward. Alternatively, trigger it through the real running
   DAIS API (`POST /pipelines/{spec_name}/run` with a local `file_path` -
   see the `dais-local-stack` skill for how to bring that up) so the run
   also exercises quality validation and lineage emission end to end, not
   just the parse/load path.

5. **Write regression tests for any code change the spec exposed** (e.g. a
   parser bug the new format revealed) — don't just fix the immediate spec
   and move on; a bug in shared code (`src/dais/parsers/`, `src/dais/...`)
   will resurface for every future spec with similar shape unless it's
   covered by a test.

## Related

- `dais-local-stack` skill: bringing up the API/Dagster/OpenMetadata stack
  needed to actually run the spec you just authored, and fixing the
  environment issues that come up doing so.
