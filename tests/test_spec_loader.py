from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from dais.spec.loader import SpecLoadError, load_spec
from dais.spec.models import PipelineSpec

FIXTURES = Path(__file__).parent / "fixtures"
VALID_SPEC = FIXTURES / "valid_holdings_ingest.yaml"
REFERENCE_SPEC = Path(__file__).parent.parent / "specs" / "holdings_ingest.yaml"
REFERENCE_SPEC_2 = Path(__file__).parent.parent / "specs" / "benchmark_ingest.yaml"


def test_load_valid_spec():
    spec = load_spec(VALID_SPEC)
    assert isinstance(spec, PipelineSpec)
    assert spec.pipeline_name == "holdings_ingest"
    assert spec.database.platform == "postgres"
    assert spec.execution.stop_after == "gold"
    assert spec.stage.business_key == ["account_id", "security_id", "as_of_date"]
    assert spec.parser.columns[0].name == "account_id"


def test_missing_file_raises():
    with pytest.raises(SpecLoadError, match="not found"):
        load_spec(FIXTURES / "does_not_exist.yaml")


def test_reference_spec_holdings_ingest_is_valid():
    spec = load_spec(REFERENCE_SPEC)
    assert spec.pipeline_name == "holdings_ingest"
    assert spec.source.format == "fixed_width"


def test_reference_spec_benchmark_ingest_is_valid_and_a_different_format():
    spec = load_spec(REFERENCE_SPEC_2)
    assert spec.pipeline_name == "benchmark_ingest"
    assert spec.source.format == "csv"


def _load_dict(base_path: Path) -> dict:
    return yaml.safe_load(base_path.read_text(encoding="utf-8"))


def test_placeholder_owner_raises():
    data = _load_dict(VALID_SPEC)
    data["owner"] = "<<team or person responsible>>"
    with pytest.raises(ValidationError, match="placeholder"):
        PipelineSpec.model_validate(data)


def test_placeholder_connection_raises():
    data = _load_dict(VALID_SPEC)
    data["database"]["connection"] = "<<vault secret name>>"
    with pytest.raises(ValidationError, match="placeholder"):
        PipelineSpec.model_validate(data)


def test_unknown_top_level_field_rejected():
    data = _load_dict(VALID_SPEC)
    data["not_a_real_field"] = "oops"
    with pytest.raises(ValidationError):
        PipelineSpec.model_validate(data)


def test_invalid_write_mode_rejected():
    data = _load_dict(VALID_SPEC)
    data["stage"]["write_mode"] = "delete_everything"
    with pytest.raises(ValidationError):
        PipelineSpec.model_validate(data)


def test_fixed_width_end_before_start_rejected():
    data = _load_dict(VALID_SPEC)
    data["parser"]["columns"][0]["end"] = 0
    with pytest.raises(ValidationError, match="end"):
        PipelineSpec.model_validate(data)


def test_parser_type_must_match_source_format():
    data = _load_dict(VALID_SPEC)
    data["source"]["format"] = "csv"
    with pytest.raises(ValidationError, match="must match"):
        PipelineSpec.model_validate(data)


def test_control_gate_max_less_than_min_rejected():
    data = _load_dict(VALID_SPEC)
    data["control_gates"]["row_count"]["max_rows"] = 1
    data["control_gates"]["row_count"]["min_rows"] = 10
    with pytest.raises(ValidationError, match="max_rows"):
        PipelineSpec.model_validate(data)


def test_missing_required_section_rejected():
    data = _load_dict(VALID_SPEC)
    del data["quality"]
    with pytest.raises(ValidationError):
        PipelineSpec.model_validate(data)


def test_export_enabled_with_placeholder_rejected():
    data = _load_dict(VALID_SPEC)
    data["stage"]["exports"][0]["enabled"] = True
    data["stage"]["exports"][0]["catalog"] = "<<iceberg catalog name>>"
    with pytest.raises(ValidationError, match="placeholder"):
        PipelineSpec.model_validate(data)


def test_fixed_width_missing_columns_rejected():
    data = _load_dict(VALID_SPEC)
    data["parser"]["columns"] = []
    with pytest.raises(ValidationError, match="columns"):
        PipelineSpec.model_validate(data)


# ---------------------------------------------------------------------------
# quality.rules[].checks - validated at spec-load time, not pipeline-run time
# ---------------------------------------------------------------------------

def test_unknown_bare_check_name_rejected_at_load_time():
    data = _load_dict(VALID_SPEC)
    data["quality"]["rules"][0]["checks"] = ["not_a_real_check"]
    with pytest.raises(ValidationError, match="unknown check"):
        PipelineSpec.model_validate(data)


def test_unknown_comparison_check_op_rejected():
    data = _load_dict(VALID_SPEC)
    data["quality"]["rules"][0]["checks"] = [{"totally_bogus": 5}]
    with pytest.raises(ValidationError, match="unknown comparison check"):
        PipelineSpec.model_validate(data)


def test_comparison_check_with_multiple_keys_rejected():
    data = _load_dict(VALID_SPEC)
    data["quality"]["rules"][0]["checks"] = [{"greater_than": 1, "less_than": 2}]
    with pytest.raises(ValidationError, match="exactly one"):
        PipelineSpec.model_validate(data)


def test_comparison_check_with_non_numeric_value_rejected():
    data = _load_dict(VALID_SPEC)
    data["quality"]["rules"][0]["checks"] = [{"greater_than_or_equal": "not_a_number"}]
    with pytest.raises(ValidationError, match="numeric value"):
        PipelineSpec.model_validate(data)


def test_valid_date_without_format_rejected():
    data = _load_dict(VALID_SPEC)
    data["quality"]["rules"][2]["checks"] = ["not_null", "valid_date"]
    del data["quality"]["rules"][2]["format"]
    with pytest.raises(ValidationError, match="format.*required"):
        PipelineSpec.model_validate(data)


def test_valid_date_with_format_accepted():
    data = _load_dict(VALID_SPEC)
    spec = PipelineSpec.model_validate(data)
    assert spec.quality.rules[2].format == "%Y%m%d"


def test_unknown_cast_to_rejected():
    data = _load_dict(VALID_SPEC)
    data["quality"]["rules"][3]["cast_to"] = "integer"  # only date/decimal are implemented
    with pytest.raises(ValidationError):
        PipelineSpec.model_validate(data)


# ---------------------------------------------------------------------------
# quality.quarantine.kind - local | s3, validated against location's shape
# ---------------------------------------------------------------------------

def test_quarantine_kind_defaults_to_s3():
    data = _load_dict(VALID_SPEC)
    spec = PipelineSpec.model_validate(data)
    assert spec.quality.quarantine.kind == "s3"


def test_quarantine_kind_s3_requires_s3_uri():
    data = _load_dict(VALID_SPEC)
    data["quality"]["quarantine"]["location"] = "./data/holdings/quarantine/"
    with pytest.raises(ValidationError, match="s3://"):
        PipelineSpec.model_validate(data)


def test_quarantine_kind_local_rejects_s3_uri():
    data = _load_dict(VALID_SPEC)
    data["quality"]["quarantine"]["kind"] = "local"
    # location is still the fixture's s3:// URI - must be rejected
    with pytest.raises(ValidationError, match="local path"):
        PipelineSpec.model_validate(data)


# ---------------------------------------------------------------------------
# anomaly_detection (Phase 8b) - optional, additive
# ---------------------------------------------------------------------------

def _valid_anomaly_block(**overrides):
    block = {"enabled": True, "metrics": ["row_count"], "method": "zscore", "threshold": 3.0, "window": 20}
    block.update(overrides)
    return block


def test_anomaly_detection_is_optional_and_absent_by_default():
    data = _load_dict(VALID_SPEC)
    spec = PipelineSpec.model_validate(data)
    assert spec.anomaly_detection is None


def test_anomaly_detection_valid_block_accepted():
    data = _load_dict(VALID_SPEC)
    data["anomaly_detection"] = _valid_anomaly_block()
    spec = PipelineSpec.model_validate(data)
    assert spec.anomaly_detection.enabled is True
    assert spec.anomaly_detection.on_anomaly == "alert"  # default


def test_anomaly_detection_on_anomaly_quarantine_accepted():
    data = _load_dict(VALID_SPEC)
    data["anomaly_detection"] = _valid_anomaly_block(on_anomaly="quarantine")
    spec = PipelineSpec.model_validate(data)
    assert spec.anomaly_detection.on_anomaly == "quarantine"


def test_anomaly_detection_rejects_unknown_metric_shape():
    data = _load_dict(VALID_SPEC)
    data["anomaly_detection"] = _valid_anomaly_block(metrics=["market_value_median"])
    with pytest.raises(ValidationError, match="unsupported anomaly_detection metric"):
        PipelineSpec.model_validate(data)


def test_anomaly_detection_accepts_column_suffixed_metrics():
    data = _load_dict(VALID_SPEC)
    data["anomaly_detection"] = _valid_anomaly_block(
        metrics=["row_count", "market_value_sum", "quantity_avg", "currency_null_rate", "currency_distinct_count"]
    )
    spec = PipelineSpec.model_validate(data)
    assert len(spec.anomaly_detection.metrics) == 5


def test_anomaly_detection_rejects_unknown_method():
    data = _load_dict(VALID_SPEC)
    data["anomaly_detection"] = _valid_anomaly_block(method="linear_regression")
    with pytest.raises(ValidationError):
        PipelineSpec.model_validate(data)


def test_anomaly_detection_rejects_unknown_on_anomaly_value():
    data = _load_dict(VALID_SPEC)
    data["anomaly_detection"] = _valid_anomaly_block(on_anomaly="ignore")
    with pytest.raises(ValidationError):
        PipelineSpec.model_validate(data)


def test_anomaly_detection_requires_at_least_one_metric():
    data = _load_dict(VALID_SPEC)
    data["anomaly_detection"] = _valid_anomaly_block(metrics=[])
    with pytest.raises(ValidationError):
        PipelineSpec.model_validate(data)


def test_anomaly_detection_threshold_must_be_positive():
    data = _load_dict(VALID_SPEC)
    data["anomaly_detection"] = _valid_anomaly_block(threshold=0)
    with pytest.raises(ValidationError):
        PipelineSpec.model_validate(data)


def test_quarantine_kind_local_with_local_path_accepted():
    data = _load_dict(VALID_SPEC)
    data["quality"]["quarantine"]["kind"] = "local"
    data["quality"]["quarantine"]["location"] = "./data/holdings/quarantine/"
    spec = PipelineSpec.model_validate(data)
    assert spec.quality.quarantine.kind == "local"


# ---------------------------------------------------------------------------
# gold: is now optional (Phase 9a - gold logic can live in a
# gold_builds/*.yaml instead), as long as execution.stop_after isn't "gold"
# ---------------------------------------------------------------------------

def test_gold_block_optional_when_stop_after_is_stage():
    data = _load_dict(VALID_SPEC)
    data["execution"]["stop_after"] = "stage"
    del data["gold"]
    spec = PipelineSpec.model_validate(data)
    assert spec.gold is None


def test_gold_block_still_required_when_stop_after_is_gold():
    data = _load_dict(VALID_SPEC)
    data["execution"]["stop_after"] = "gold"
    del data["gold"]
    with pytest.raises(ValidationError, match="no gold: block"):
        PipelineSpec.model_validate(data)


def test_existing_specs_with_gold_block_unaffected():
    # backward compatibility: a spec that already has gold: + stop_after: gold
    # continues to validate exactly as before
    data = _load_dict(VALID_SPEC)
    spec = PipelineSpec.model_validate(data)
    assert spec.gold is not None


# ---------------------------------------------------------------------------
# quality.integrity_mode: group_level, requires quality.quarantine.group_by
# ---------------------------------------------------------------------------

def test_group_level_without_group_by_rejected():
    data = _load_dict(VALID_SPEC)
    data["quality"]["integrity_mode"] = "group_level"
    with pytest.raises(ValidationError, match="group_by is required"):
        PipelineSpec.model_validate(data)


def test_group_by_without_group_level_rejected():
    data = _load_dict(VALID_SPEC)
    data["quality"]["quarantine"]["group_by"] = ["account_id"]
    with pytest.raises(ValidationError, match="only used when integrity_mode"):
        PipelineSpec.model_validate(data)


def test_group_level_with_group_by_accepted():
    data = _load_dict(VALID_SPEC)
    data["quality"]["integrity_mode"] = "group_level"
    data["quality"]["quarantine"]["group_by"] = ["account_id"]
    spec = PipelineSpec.model_validate(data)
    assert spec.quality.integrity_mode == "group_level"
    assert spec.quality.quarantine.group_by == ["account_id"]


# ---------------------------------------------------------------------------
# gold.scd_type: 2, requires gold.scd_key
# ---------------------------------------------------------------------------

def test_scd_type_without_scd_key_rejected():
    data = _load_dict(VALID_SPEC)
    data["gold"]["scd_type"] = 2
    with pytest.raises(ValidationError, match="scd_key is required"):
        PipelineSpec.model_validate(data)


def test_scd_key_without_scd_type_rejected():
    data = _load_dict(VALID_SPEC)
    data["gold"]["scd_key"] = ["account_id"]
    with pytest.raises(ValidationError, match="only used when gold.scd_type"):
        PipelineSpec.model_validate(data)


def test_scd_type_with_scd_key_accepted():
    data = _load_dict(VALID_SPEC)
    data["gold"]["scd_type"] = 2
    data["gold"]["scd_key"] = ["account_id"]
    spec = PipelineSpec.model_validate(data)
    assert spec.gold.scd_type == 2
    assert spec.gold.scd_key == ["account_id"]


def test_scd_type_only_accepts_2():
    data = _load_dict(VALID_SPEC)
    data["gold"]["scd_type"] = 1
    data["gold"]["scd_key"] = ["account_id"]
    with pytest.raises(ValidationError):
        PipelineSpec.model_validate(data)
