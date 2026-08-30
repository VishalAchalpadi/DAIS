import json
from pathlib import Path

import pytest
import yaml

from dais.quality.quarantine_review import (
    list_quarantine_records,
    read_quarantine_record,
    resubmit_corrections,
)
from dais.spec.models import PipelineSpec
from tests.conftest import requires_local_postgres

FIXTURE = Path(__file__).parent / "fixtures" / "valid_holdings_ingest.yaml"


def _spec_with_local_quarantine(tmp_path, schema="dais_test_review") -> PipelineSpec:
    data = yaml.safe_load(FIXTURE.read_text(encoding="utf-8"))
    data["raw"]["schema"] = schema
    data["stage"]["schema"] = schema
    data["gold"]["schema"] = schema
    data["monitoring"]["schema"] = schema
    data["quality"]["quarantine"] = {
        "kind": "local",
        "location": str(tmp_path / "quarantine"),
        "alert": {"channel": "log", "destination": "n/a"},
    }
    return PipelineSpec.model_validate(data)


def _write_record(tmp_path, file_name, rows):
    directory = tmp_path / "quarantine"
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{file_name}.20260101T000000000000Z.quarantine.json"
    path.write_text(json.dumps(rows), encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# list / read (local kind, no DB needed)
# ---------------------------------------------------------------------------

def test_list_quarantine_records_empty_when_no_directory(tmp_path):
    spec = _spec_with_local_quarantine(tmp_path)
    assert list_quarantine_records(spec) == []


def test_list_and_read_quarantine_record(tmp_path):
    spec = _spec_with_local_quarantine(tmp_path)
    rows = [{"row_index": 2, "row_data": {"account_id": ""}, "reasons": ["non_empty"], "quarantined_at": "x"}]
    _write_record(tmp_path, "HOLDINGS_20260101.txt", rows)

    records = list_quarantine_records(spec)
    assert len(records) == 1
    assert records[0].file_name.startswith("HOLDINGS_20260101.txt")
    assert records[0].row_count == 1

    fetched = read_quarantine_record(spec, records[0].quarantine_id)
    assert fetched == rows


# ---------------------------------------------------------------------------
# resubmit_corrections (requires real Postgres - upserts into stage)
# ---------------------------------------------------------------------------

@requires_local_postgres
def test_resubmit_corrections_upserts_and_marks_resolved(pg_connector, test_schema, tmp_path):
    spec = _spec_with_local_quarantine(tmp_path, schema=test_schema)
    from dais.resilience.connectors.base import ColumnDef

    pg_connector.create_table_if_not_exists(
        test_schema,
        spec.stage.table,
        [
            ColumnDef("account_id", "TEXT"),
            ColumnDef("security_id", "TEXT"),
            ColumnDef("as_of_date", "DATE"),
            ColumnDef("quantity", "NUMERIC(18,4)"),
            ColumnDef("market_value", "NUMERIC(18,2)"),
            ColumnDef("currency", "TEXT"),
        ],
        unique_columns=spec.stage.business_key,
    )

    record_path = _write_record(
        tmp_path,
        "HOLDINGS_20260101.txt",
        [
            {
                "row_index": 0,
                "row_data": {
                    "account_id": "ACC01",
                    "security_id": "SEC01",
                    "as_of_date": "20260101",
                    "quantity": "100.0000",
                    "market_value": "5000.00",
                    "currency": "USD",
                },
                "reasons": ["quantity: greater_than_or_equal"],
                "quarantined_at": "x",
            }
        ],
    )
    quarantine_id = record_path.name

    corrected = [
        {
            "row_index": 0,
            "row_data": {
                "account_id": "ACC01",
                "security_id": "SEC01",
                "as_of_date": "20260101",
                "quantity": "100.0000",  # now non-negative, passes
                "market_value": "5000.00",
                "currency": "USD",
            },
        }
    ]

    outcome = resubmit_corrections(spec, quarantine_id, corrected, pg_connector)

    assert outcome.accepted is True
    assert outcome.row_count_upserted == 1
    assert not record_path.exists()
    assert (tmp_path / "quarantine" / "resolved" / quarantine_id).exists()

    rows = pg_connector.value_exists(test_schema, spec.stage.table, "account_id", "ACC01")
    assert rows is True


@requires_local_postgres
def test_resubmit_corrections_still_failing_rejects_all(pg_connector, test_schema, tmp_path):
    spec = _spec_with_local_quarantine(tmp_path, schema=test_schema)
    from dais.resilience.connectors.base import ColumnDef

    pg_connector.create_table_if_not_exists(
        test_schema,
        spec.stage.table,
        [
            ColumnDef("account_id", "TEXT"),
            ColumnDef("security_id", "TEXT"),
            ColumnDef("as_of_date", "DATE"),
            ColumnDef("quantity", "NUMERIC(18,4)"),
            ColumnDef("market_value", "NUMERIC(18,2)"),
            ColumnDef("currency", "TEXT"),
        ],
        unique_columns=spec.stage.business_key,
    )

    record_path = _write_record(
        tmp_path,
        "HOLDINGS_20260101.txt",
        [
            {
                "row_index": 0,
                "row_data": {
                    "account_id": "",  # still invalid - non_empty will fail again
                    "security_id": "SEC01",
                    "as_of_date": "20260101",
                    "quantity": "100.0000",
                    "market_value": "5000.00",
                    "currency": "USD",
                },
                "reasons": ["account_id: non_empty"],
                "quarantined_at": "x",
            }
        ],
    )
    quarantine_id = record_path.name

    outcome = resubmit_corrections(spec, quarantine_id, [{"row_index": 0, "row_data": {
        "account_id": "",
        "security_id": "SEC01",
        "as_of_date": "20260101",
        "quantity": "100.0000",
        "market_value": "5000.00",
        "currency": "USD",
    }}], pg_connector)

    assert outcome.accepted is False
    assert outcome.row_count_upserted == 0
    assert outcome.failures[0]["row_index"] == 0
    assert record_path.exists()  # not moved to resolved/ - correction wasn't valid
