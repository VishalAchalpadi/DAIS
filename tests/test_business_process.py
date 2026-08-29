from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

import pytest

from dais.business_process.evaluator import evaluate_business_process
from dais.business_process.loader import BusinessProcessLoadError, load_business_process
from dais.business_process.models import BusinessProcessGroup
from dais.monitoring.ddl import ensure_process_monitor_table
from tests.conftest import requires_local_postgres

pytestmark = requires_local_postgres

TZ = "America/New_York"


def _group(complete_by="23:59"):
    return BusinessProcessGroup(
        process_name="morning_holdings_recon",
        members=[
            {"pipeline_name": "holdings_ingest", "success_layer": "gold"},
            {"pipeline_name": "benchmark_ingest", "success_layer": "stage"},
        ],
        sla={"complete_by": complete_by, "timezone": TZ},
    )


def _insert_status(pg_connector, schema, pipeline_name, step, status, started_at):
    ensure_process_monitor_table(pg_connector, schema, "process_monitor")
    row = (
        f"proc-{pipeline_name}-{step}",
        pipeline_name,
        step,
        f"data_in.{pipeline_name}",
        None,
        None,
        status,
        started_at,
        started_at if status != "begin" else None,
    )
    columns = [
        "process_id", "pipeline_name", "step", "target_table",
        "row_count_in", "row_count_out", "status", "started_at", "completed_at",
    ]
    pg_connector.upsert(schema, "process_monitor", columns, [row], conflict_columns=["process_id", "step"])


def test_no_runs_today_is_pending(pg_connector, test_schema):
    ensure_process_monitor_table(pg_connector, test_schema, "process_monitor")
    group = _group(complete_by="23:59")
    status = evaluate_business_process(group, pg_connector, monitoring_schema=test_schema)

    assert status.state == "pending"
    assert all(m.state == "pending" for m in status.members)


def test_one_member_started_is_in_progress(pg_connector, test_schema):
    group = _group(complete_by="23:59")
    now = datetime.now(ZoneInfo(TZ))
    _insert_status(pg_connector, test_schema, "holdings_ingest", "gold", "begin", now)

    status = evaluate_business_process(group, pg_connector, monitoring_schema=test_schema)

    assert status.state == "in_progress"
    holdings = next(m for m in status.members if m.pipeline_name == "holdings_ingest")
    assert holdings.state == "in_progress"
    benchmark = next(m for m in status.members if m.pipeline_name == "benchmark_ingest")
    assert benchmark.state == "pending"


def test_all_members_complete_is_met_even_after_sla(pg_connector, test_schema):
    group = _group(complete_by="00:01")  # SLA almost certainly already passed today
    now = datetime.now(ZoneInfo(TZ))
    _insert_status(pg_connector, test_schema, "holdings_ingest", "gold", "complete", now)
    _insert_status(pg_connector, test_schema, "benchmark_ingest", "stage", "complete", now)

    status = evaluate_business_process(group, pg_connector, monitoring_schema=test_schema)

    assert status.state == "met"
    assert all(m.state == "met" for m in status.members)


def test_sla_passed_with_incomplete_member_is_breached(pg_connector, test_schema):
    yesterday = (datetime.now(ZoneInfo(TZ)) - timedelta(days=1)).date()
    group = _group(complete_by="12:00")
    started_at = datetime.combine(yesterday, time(9, 0), tzinfo=ZoneInfo(TZ))
    _insert_status(pg_connector, test_schema, "holdings_ingest", "gold", "complete", started_at)
    # benchmark_ingest never ran on this business date

    status = evaluate_business_process(group, pg_connector, monitoring_schema=test_schema, business_date=yesterday)

    assert status.state == "breached"
    benchmark = next(m for m in status.members if m.pipeline_name == "benchmark_ingest")
    assert benchmark.state == "pending"


def test_sla_not_yet_passed_with_partial_progress_is_in_progress_not_breached(pg_connector, test_schema):
    tomorrow = (datetime.now(ZoneInfo(TZ)) + timedelta(days=1)).date()
    group = _group(complete_by="23:59")
    started_at = datetime.combine(tomorrow, time(1, 0), tzinfo=ZoneInfo(TZ))
    _insert_status(pg_connector, test_schema, "holdings_ingest", "gold", "complete", started_at)

    status = evaluate_business_process(group, pg_connector, monitoring_schema=test_schema, business_date=tomorrow)

    assert status.state == "in_progress"


# ---------------------------------------------------------------------------
# loader
# ---------------------------------------------------------------------------

def test_loader_loads_the_example_business_process():
    from pathlib import Path

    path = Path(__file__).parent.parent / "business_processes" / "morning_holdings_recon.yaml"
    group = load_business_process(path)
    assert group.process_name == "morning_holdings_recon"
    assert {m.pipeline_name for m in group.members} == {"holdings_ingest", "benchmark_ingest"}


def test_loader_missing_file_raises():
    with pytest.raises(BusinessProcessLoadError, match="not found"):
        load_business_process("/nonexistent/foo.yaml")
