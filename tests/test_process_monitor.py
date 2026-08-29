import uuid

from dais.monitoring.process_monitor import ProcessMonitor
from tests.conftest import requires_local_postgres

pytestmark = requires_local_postgres


def _monitor(pg_connector, test_schema):
    return ProcessMonitor(pg_connector, test_schema, "process_monitor")


def test_begin_writes_a_begin_row(pg_connector, test_schema):
    monitor = _monitor(pg_connector, test_schema)
    process_id = str(uuid.uuid4())

    monitor.begin_step(process_id, "holdings_ingest", "raw", "data_in.holdings_raw")

    status = monitor.get_status(process_id, "raw")
    assert status == ("begin", None, None)


def test_complete_updates_the_same_row_not_a_new_one(pg_connector, test_schema):
    monitor = _monitor(pg_connector, test_schema)
    process_id = str(uuid.uuid4())

    handle = monitor.begin_step(process_id, "holdings_ingest", "raw", "data_in.holdings_raw")
    monitor.complete_step(handle, row_count_in=100, row_count_out=100)

    status = monitor.get_status(process_id, "raw")
    assert status == ("complete", 100, 100)

    count = pg_connector.fetch_all(
        f'SELECT COUNT(*) FROM "{test_schema}"."process_monitor" '
        "WHERE process_id = %(pid)s AND step = %(step)s",
        {"pid": process_id, "step": "raw"},
    )
    assert count[0][0] == 1  # never a second row for the same (process_id, step)


def test_failed_step_records_failed_status(pg_connector, test_schema):
    monitor = _monitor(pg_connector, test_schema)
    process_id = str(uuid.uuid4())

    handle = monitor.begin_step(process_id, "holdings_ingest", "stage", "data_in.holdings_stage")
    monitor.fail_step(handle, row_count_in=50, row_count_out=0)

    status = monitor.get_status(process_id, "stage")
    assert status == ("failed", 50, 0)


def test_quarantined_step_records_quarantined_status(pg_connector, test_schema):
    monitor = _monitor(pg_connector, test_schema)
    process_id = str(uuid.uuid4())

    handle = monitor.begin_step(process_id, "holdings_ingest", "raw", "data_in.holdings_raw")
    monitor.quarantine_step(handle, row_count_in=10, row_count_out=0)

    status = monitor.get_status(process_id, "raw")
    assert status == ("quarantined", 10, 0)


def test_multiple_steps_for_same_process_id_are_independent_rows(pg_connector, test_schema):
    monitor = _monitor(pg_connector, test_schema)
    process_id = str(uuid.uuid4())

    raw_handle = monitor.begin_step(process_id, "holdings_ingest", "raw", "data_in.holdings_raw")
    monitor.complete_step(raw_handle, row_count_in=10, row_count_out=10)
    stage_handle = monitor.begin_step(process_id, "holdings_ingest", "stage", "data_in.holdings_stage")

    assert monitor.get_status(process_id, "raw") == ("complete", 10, 10)
    assert monitor.get_status(process_id, "stage") == ("begin", None, None)


def test_concurrent_process_ids_do_not_collide(pg_connector, test_schema):
    monitor = _monitor(pg_connector, test_schema)
    process_id_a = str(uuid.uuid4())
    process_id_b = str(uuid.uuid4())

    handle_a = monitor.begin_step(process_id_a, "holdings_ingest", "raw", "data_in.holdings_raw")
    handle_b = monitor.begin_step(process_id_b, "holdings_ingest", "raw", "data_in.holdings_raw")

    monitor.complete_step(handle_a, row_count_in=1, row_count_out=1)

    # completing A must not affect B's still-in-progress row
    assert monitor.get_status(process_id_a, "raw") == ("complete", 1, 1)
    assert monitor.get_status(process_id_b, "raw") == ("begin", None, None)


def test_ensure_table_is_idempotent(pg_connector, test_schema):
    _monitor(pg_connector, test_schema)
    _monitor(pg_connector, test_schema)  # constructing again must not raise
    assert pg_connector.table_exists(test_schema, "process_monitor")
