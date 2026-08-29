import os
from pathlib import Path

import pytest
import yaml

from dais import cli
from tests.conftest import requires_local_postgres

pytestmark = requires_local_postgres

FIXTURE = Path(__file__).parent / "fixtures" / "valid_holdings_ingest.yaml"


def _fw_field(value: str, width: int) -> str:
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


def _write_spec(tmp_path, schema, *, min_rows=1, stop_after="raw"):
    data = yaml.safe_load(FIXTURE.read_text(encoding="utf-8"))
    data["raw"]["schema"] = schema
    data["stage"]["schema"] = schema
    data["gold"]["schema"] = schema
    data["monitoring"]["schema"] = schema
    data["execution"]["stop_after"] = stop_after
    data["control_gates"] = {
        "file_size": {"min_bytes": 1, "max_bytes": 1_000_000},
        "row_count": {"min_rows": min_rows, "max_rows": 1000},
        "on_fail": "quarantine_file",
    }
    spec_path = tmp_path / "holdings_ingest.yaml"
    spec_path.write_text(yaml.dump(data), encoding="utf-8")
    return spec_path


@pytest.fixture(autouse=True)
def _hardcoded_secrets_env(monkeypatch):
    monkeypatch.setenv("SECRETS_PROVIDER", "hardcoded")


def test_cli_run_succeeds_and_exits_zero(pg_connector, test_schema, tmp_path, capsys):
    spec_path = _write_spec(tmp_path, test_schema)
    file_path = tmp_path / "HOLDINGS_20260101.txt"
    file_path.write_text(_sample_line("ACC01", "SEC01", "20260101", "100.0000", "5000.00", "USD") + "\n", encoding="utf-8")

    exit_code = cli.main(["run", "--spec", str(spec_path), "--file", str(file_path)])

    assert exit_code == 0
    out = capsys.readouterr().out
    assert "status=succeeded" in out
    assert "layer_reached=raw" in out


def test_cli_run_quarantined_exits_nonzero(pg_connector, test_schema, tmp_path, capsys):
    spec_path = _write_spec(tmp_path, test_schema, min_rows=100)  # our 1-row file will fail this
    file_path = tmp_path / "HOLDINGS_20260101.txt"
    file_path.write_text(_sample_line("ACC01", "SEC01", "20260101", "100.0000", "5000.00", "USD") + "\n", encoding="utf-8")

    exit_code = cli.main(["run", "--spec", str(spec_path), "--file", str(file_path)])

    assert exit_code == 1
    out = capsys.readouterr().out
    assert "status=quarantined" in out


def test_cli_run_invalid_spec_exits_nonzero(tmp_path, capsys):
    bad_spec = tmp_path / "broken.yaml"
    bad_spec.write_text("not: a valid: pipeline spec: at all", encoding="utf-8")

    exit_code = cli.main(["run", "--spec", str(bad_spec), "--file", "irrelevant.txt"])

    assert exit_code == 1
    err = capsys.readouterr().err
    assert "error:" in err


def test_cli_stop_after_override(pg_connector, test_schema, tmp_path, capsys):
    spec_path = _write_spec(tmp_path, test_schema, stop_after="gold")
    file_path = tmp_path / "HOLDINGS_20260101.txt"
    file_path.write_text(_sample_line("ACC01", "SEC01", "20260101", "100.0000", "5000.00", "USD") + "\n", encoding="utf-8")

    exit_code = cli.main(
        ["run", "--spec", str(spec_path), "--file", str(file_path), "--stop-after", "raw"]
    )

    assert exit_code == 0
    out = capsys.readouterr().out
    assert "layer_reached=raw" in out  # override wins over the spec's stop_after: gold
    assert not pg_connector.table_exists(test_schema, "holdings_stage")
