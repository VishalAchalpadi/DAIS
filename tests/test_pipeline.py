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
