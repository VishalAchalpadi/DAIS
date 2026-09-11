from pathlib import Path

import yaml

from dais.pipeline import run_pipeline
from dais.spec.models import PipelineSpec
from tests.conftest import _local_pg_creds, requires_local_postgres

pytestmark = requires_local_postgres

FIXTURE = Path(__file__).parent / "fixtures" / "valid_holdings_ingest.yaml"
DBT_PROJECT = str(Path(__file__).parent.parent / "dbt")


def _spec_for_schema(schema: str, *, stop_after: str = "gold") -> PipelineSpec:
    data = yaml.safe_load(FIXTURE.read_text(encoding="utf-8"))
    data["raw"]["schema"] = schema
    data["stage"]["schema"] = schema
    data["gold"]["schema"] = schema
    data["monitoring"]["schema"] = schema
    data["gold"]["dbt_project"] = DBT_PROJECT
    data["execution"]["stop_after"] = stop_after
    return PipelineSpec.model_validate(data)


def _fw_field(value: str, width: int) -> str:
    if len(value) > width:
        raise ValueError(f"{value!r} exceeds width {width}")
    return value.ljust(width)


def _sample_line(account_id, security_id, as_of_date, quantity, market_value, currency) -> str:
    return (
        _fw_field(account_id, 12)
        + _fw_field(security_id, 12)
        + _fw_field(as_of_date, 8)
        + _fw_field(quantity, 15)
        + _fw_field(market_value, 15)
        + _fw_field(currency, 3)
    )


def _write_sample_file(tmp_path, lines: list[str]) -> Path:
    path = tmp_path / "HOLDINGS_20260101.txt"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def _connection_kwargs():
    creds = _local_pg_creds()
    return dict(host=creds["host"], port=creds["port"], dbname=creds["dbname"], user=creds["user"], password=creds["password"])


def test_full_pipeline_reaches_gold(pg_connector, test_schema, tmp_path, ref_currencies):
    spec = _spec_for_schema(test_schema, stop_after="gold")
    # widen bounds - the fixture spec's control gates are placeholder strings
    spec.control_gates.row_count.min_rows = 1
    spec.control_gates.row_count.max_rows = 1000
    spec.control_gates.file_size.min_bytes = 1
    spec.control_gates.file_size.max_bytes = 100_000

    file_path = _write_sample_file(
        tmp_path,
        [
            _sample_line("ACC01", "SEC01", "20260101", "100.0000", "5000.00", "USD"),
            _sample_line("ACC02", "SEC02", "20260101", "200.0000", "8000.00", "EUR"),
        ],
    )

    result = run_pipeline(
        spec,
        file_path=str(file_path),
        connector=pg_connector,
        connection_params=_connection_kwargs(),
    )

    assert result.status == "succeeded"
    assert result.layer_reached == "gold"
    rows = pg_connector.fetch_all(
        f'SELECT account_id FROM "{test_schema}"."holdings_gold" ORDER BY account_id'
    )
    assert [r[0] for r in rows] == ["ACC01", "ACC02"]


def test_stop_after_raw_does_not_touch_stage_or_gold(pg_connector, test_schema, tmp_path, ref_currencies):
    spec = _spec_for_schema(test_schema, stop_after="raw")
    spec.control_gates.row_count.min_rows = 1
    spec.control_gates.row_count.max_rows = 1000
    spec.control_gates.file_size.min_bytes = 1
    spec.control_gates.file_size.max_bytes = 100_000

    file_path = _write_sample_file(
        tmp_path, [_sample_line("ACC01", "SEC01", "20260101", "100.0000", "5000.00", "USD")]
    )

    result = run_pipeline(spec, file_path=str(file_path), connector=pg_connector)

    assert result.status == "succeeded"
    assert result.layer_reached == "raw"
    assert pg_connector.table_exists(test_schema, spec.raw.table)
    assert not pg_connector.table_exists(test_schema, spec.stage.table)


def test_rerun_same_file_is_checksum_deduped(pg_connector, test_schema, tmp_path, ref_currencies):
    spec = _spec_for_schema(test_schema, stop_after="raw")
    spec.control_gates.row_count.min_rows = 1
    spec.control_gates.row_count.max_rows = 1000
    spec.control_gates.file_size.min_bytes = 1
    spec.control_gates.file_size.max_bytes = 100_000

    file_path = _write_sample_file(
        tmp_path, [_sample_line("ACC01", "SEC01", "20260101", "100.0000", "5000.00", "USD")]
    )

    first = run_pipeline(spec, file_path=str(file_path), connector=pg_connector)
    second = run_pipeline(spec, file_path=str(file_path), connector=pg_connector)

    assert first.checksum == second.checksum
    count = pg_connector.fetch_all(f'SELECT COUNT(*) FROM "{test_schema}"."{spec.raw.table}"')
    assert count[0][0] == 1  # second run was a no-op


def test_control_gate_failure_quarantines_before_raw(pg_connector, test_schema, tmp_path):
    spec = _spec_for_schema(test_schema, stop_after="raw")
    spec.control_gates.row_count.min_rows = 100  # our 1-row file will fail this
    spec.control_gates.row_count.max_rows = 1000
    spec.control_gates.file_size.min_bytes = 1
    spec.control_gates.file_size.max_bytes = 100_000
    # local quarantine - no AWS account/S3Connector needed for this test
    spec.quality.quarantine.kind = "local"
    spec.quality.quarantine.location = str(tmp_path / "quarantine")

    file_path = _write_sample_file(
        tmp_path, [_sample_line("ACC01", "SEC01", "20260101", "100.0000", "5000.00", "USD")]
    )

    result = run_pipeline(spec, file_path=str(file_path), connector=pg_connector)

    assert result.status == "quarantined"
    assert result.layer_reached is None
    assert not pg_connector.table_exists(test_schema, spec.raw.table)
    assert result.quarantine_location is not None
    assert Path(result.quarantine_location).is_file()
    assert any("row_count" in r for r in result.failure_reasons)


def test_strict_dq_quarantine_surfaces_failure_reasons(pg_connector, test_schema, tmp_path):
    spec = _spec_for_schema(test_schema, stop_after="stage")
    spec.control_gates.row_count.min_rows = 1
    spec.control_gates.row_count.max_rows = 1000
    spec.control_gates.file_size.min_bytes = 1
    spec.control_gates.file_size.max_bytes = 100_000
    spec.quality.quarantine.kind = "local"
    spec.quality.quarantine.location = str(tmp_path / "quarantine")

    file_path = _write_sample_file(
        tmp_path,
        [
            _sample_line("ACC01", "SEC01", "20260101", "100.0000", "5000.00", "USD"),
            _sample_line("ACC02", "SEC02", "20260101", "-5.0000", "5000.00", "USD"),  # negative quantity
        ],
    )

    result = run_pipeline(spec, file_path=str(file_path), connector=pg_connector)

    assert result.status == "quarantined"
    assert result.quarantine_location is not None
    assert Path(result.quarantine_location).is_file()
    assert any("quantity" in r and "greater_than_or_equal" in r for r in result.failure_reasons)


def test_large_file_takes_the_chunked_parse_path(pg_connector, test_schema, tmp_path, ref_currencies, monkeypatch):
    import dais.pipeline as pipeline_module

    monkeypatch.setattr(pipeline_module, "CHUNKED_PARSE_THRESHOLD_BYTES", 100)  # force chunking

    spec = _spec_for_schema(test_schema, stop_after="raw")
    spec.control_gates.row_count.min_rows = 1
    spec.control_gates.row_count.max_rows = 10000
    spec.control_gates.file_size.min_bytes = 1
    spec.control_gates.file_size.max_bytes = 1_000_000

    lines = [
        _sample_line(f"ACC{i:02d}", f"SEC{i:02d}", "20260101", "100.0000", "5000.00", "USD")
        for i in range(50)
    ]
    file_path = _write_sample_file(tmp_path, lines)

    result = run_pipeline(spec, file_path=str(file_path), connector=pg_connector)

    assert result.status == "succeeded"
    count = pg_connector.fetch_all(f'SELECT COUNT(*) FROM "{test_schema}"."{spec.raw.table}"')
    assert count[0][0] == 50


# ---------------------------------------------------------------------------
# Phase 8b: anomaly detection - on_anomaly: quarantine must actually block
# promotion to gold, identically to a strict DQ failure from the pipeline's
# perspective. control.data_profile_history is a real SHARED table (not
# scoped to test_schema like everything else here), so these tests use a
# unique pipeline_name and clean up their own rows afterward.
# ---------------------------------------------------------------------------

def _spec_with_anomaly_detection(test_schema, pipeline_name, *, on_anomaly, stop_after="gold"):
    data = yaml.safe_load(FIXTURE.read_text(encoding="utf-8"))
    data["pipeline_name"] = pipeline_name
    data["raw"]["schema"] = test_schema
    data["stage"]["schema"] = test_schema
    data["gold"]["schema"] = test_schema
    data["monitoring"]["schema"] = test_schema
    data["gold"]["dbt_project"] = DBT_PROJECT
    data["execution"]["stop_after"] = stop_after
    data["anomaly_detection"] = {
        "enabled": True,
        "metrics": ["row_count"],
        "method": "zscore",
        "threshold": 3.0,
        "window": 20,
        "on_anomaly": on_anomaly,
    }
    return PipelineSpec.model_validate(data)


def _seed_trailing_profiles(pg_connector, pipeline_name, *, row_count, n=20):
    from datetime import date, timedelta

    from dais.ai.anomaly_detector import write_profile_history
    from dais.ai.profiler import ProfileResult

    today = date.today()
    for i in range(n):
        write_profile_history(
            pg_connector,
            process_id=f"seed-{pipeline_name}-{i}",
            pipeline_name=pipeline_name,
            run_date=today - timedelta(days=n - i),
            profile=ProfileResult(row_count=row_count, metrics={}),
        )


def _cleanup_profile_history(pg_connector, pipeline_name):
    pg_connector.execute(
        "DELETE FROM control.data_profile_history WHERE pipeline_name = %(name)s", {"name": pipeline_name}
    )


def test_anomaly_quarantine_blocks_promotion_to_gold(pg_connector, test_schema, tmp_path, ref_currencies):
    pipeline_name = f"holdings_anomaly_quarantine_{test_schema}"
    spec = _spec_with_anomaly_detection(test_schema, pipeline_name, on_anomaly="quarantine")
    _seed_trailing_profiles(pg_connector, pipeline_name, row_count=15)

    try:
        # a file with far more rows than the 15-row trailing baseline - a genuine anomaly
        lines = [
            _sample_line(f"ACC{i:02d}", f"SEC{i:02d}", "20260101", "100.0000", "5000.00", "USD")
            for i in range(50)
        ]
        file_path = _write_sample_file(tmp_path, lines)

        result = run_pipeline(
            spec, file_path=str(file_path), connector=pg_connector, connection_params=_connection_kwargs()
        )

        assert result.status == "quarantined"
        assert result.layer_reached == "stage"
        assert "anomaly" in result.error.lower()
        assert any("row_count" in r for r in result.failure_reasons)
        # data DID land in stage (profiling needs it there) but never reached gold
        assert pg_connector.table_exists(test_schema, spec.stage.table)
        assert not pg_connector.table_exists(test_schema, spec.gold.dbt_select)
    finally:
        _cleanup_profile_history(pg_connector, pipeline_name)


def test_anomaly_alert_mode_does_not_block_promotion(pg_connector, test_schema, tmp_path, ref_currencies):
    pipeline_name = f"holdings_anomaly_alert_{test_schema}"
    spec = _spec_with_anomaly_detection(test_schema, pipeline_name, on_anomaly="alert")
    _seed_trailing_profiles(pg_connector, pipeline_name, row_count=15)

    try:
        lines = [
            _sample_line(f"ACC{i:02d}", f"SEC{i:02d}", "20260101", "100.0000", "5000.00", "USD")
            for i in range(50)
        ]
        file_path = _write_sample_file(tmp_path, lines)

        result = run_pipeline(
            spec, file_path=str(file_path), connector=pg_connector, connection_params=_connection_kwargs()
        )

        # same anomaly, but alert-only mode never blocks promotion
        assert result.status == "succeeded"
        assert result.layer_reached == "gold"
        assert pg_connector.table_exists(test_schema, spec.gold.dbt_select)
    finally:
        _cleanup_profile_history(pg_connector, pipeline_name)


def test_no_anomaly_when_run_matches_trailing_history(pg_connector, test_schema, tmp_path, ref_currencies):
    pipeline_name = f"holdings_anomaly_normal_{test_schema}"
    spec = _spec_with_anomaly_detection(test_schema, pipeline_name, on_anomaly="quarantine")
    spec.control_gates.file_size.min_bytes = 1  # a 10-row fixed-width file is under the fixture's default 1000
    # baseline already includes some natural spread so stdev isn't zero
    from datetime import date, timedelta

    from dais.ai.anomaly_detector import write_profile_history
    from dais.ai.profiler import ProfileResult

    today = date.today()
    for i, rc in enumerate([9, 10, 11, 10, 9, 10, 11, 10, 9, 10] * 2):
        write_profile_history(
            pg_connector,
            process_id=f"seed-{pipeline_name}-{i}",
            pipeline_name=pipeline_name,
            run_date=today - timedelta(days=20 - i),
            profile=ProfileResult(row_count=rc, metrics={}),
        )

    try:
        lines = [
            _sample_line(f"ACC{i:02d}", f"SEC{i:02d}", "20260101", "100.0000", "5000.00", "USD")
            for i in range(10)  # right in line with the trailing baseline
        ]
        file_path = _write_sample_file(tmp_path, lines)

        result = run_pipeline(
            spec, file_path=str(file_path), connector=pg_connector, connection_params=_connection_kwargs()
        )

        assert result.status == "succeeded"
        assert result.layer_reached == "gold"
    finally:
        _cleanup_profile_history(pg_connector, pipeline_name)
