import uuid
from pathlib import Path

from openlineage.client import OpenLineageClient
from openlineage.client.run import RunState
from openlineage.client.transport.transport import Transport

from dais.lineage.emitter import LineageEmitter, build_client, build_emitter
from dais.spec.loader import load_spec
from dais.spec.models import LineageConfig

VALID_SPEC = Path(__file__).parent / "fixtures" / "valid_holdings_ingest.yaml"


class RecordingTransport(Transport):
    kind = "recording"

    def __init__(self, config=None):
        self.events = []

    def emit(self, event):
        self.events.append(event)


def _emitter():
    transport = RecordingTransport()
    client = OpenLineageClient(transport=transport)
    return LineageEmitter("datamesh-prod", "holdings_ingest", client), transport


def _run_id() -> str:
    return str(uuid.uuid4())  # OpenLineage run IDs must be valid UUIDs


def test_start_emits_start_event_with_correct_job_and_run_id():
    emitter, transport = _emitter()
    run_id = _run_id()
    emitter.start("raw", run_id, inputs=["s3://bucket/file.txt"], outputs=["data_in.holdings_raw"])

    assert len(transport.events) == 1
    event = transport.events[0]
    assert event.eventType == RunState.START
    assert event.run.runId == run_id
    assert event.job.name == "holdings_ingest.raw"
    assert event.job.namespace == "datamesh-prod"
    assert event.inputs[0].name == "s3://bucket/file.txt"
    assert event.outputs[0].name == "data_in.holdings_raw"


def test_complete_emits_complete_event():
    emitter, transport = _emitter()
    emitter.complete("stage", _run_id(), outputs=["data_in.holdings_stage"])

    assert transport.events[0].eventType == RunState.COMPLETE
    assert transport.events[0].job.name == "holdings_ingest.stage"


def test_fail_emits_fail_event():
    emitter, transport = _emitter()
    emitter.fail("gold", _run_id())

    assert transport.events[0].eventType == RunState.FAIL
    assert transport.events[0].job.name == "holdings_ingest.gold"


def test_full_lifecycle_start_then_complete_same_run_id():
    emitter, transport = _emitter()
    run_id = _run_id()
    emitter.start("control_gate", run_id)
    emitter.complete("control_gate", run_id)

    assert [e.eventType for e in transport.events] == [RunState.START, RunState.COMPLETE]
    assert transport.events[0].run.runId == transport.events[1].run.runId == run_id


def test_build_emitter_from_spec_uses_lineage_config():
    transport = RecordingTransport()
    spec_lineage = LineageConfig(namespace="datamesh-prod", job_name="holdings_ingest")

    emitter = LineageEmitter(spec_lineage.namespace, spec_lineage.job_name, build_client(transport))
    emitter.start("raw", _run_id())

    assert transport.events[0].job.namespace == "datamesh-prod"
    assert transport.events[0].job.name == "holdings_ingest.raw"


def test_build_client_defaults_to_console_transport_when_no_url_configured(monkeypatch):
    monkeypatch.delenv("OPENLINEAGE_URL", raising=False)
    client = build_client()  # must not raise - this is the no-config default path
    emitter = LineageEmitter("ns", "job", client)
    emitter.start("raw", _run_id())  # must not raise either


def test_build_client_uses_provided_transport_without_network_call():
    transport = RecordingTransport()
    client = build_client(transport)
    emitter = LineageEmitter("ns", "job", client)
    emitter.start("raw", _run_id())
    assert len(transport.events) == 1


# ---------------------------------------------------------------------------
# db_namespace/dbname: table datasets (raw/stage/gold, shaped "schema.table")
# get a dataset identity matching dbt-ol's own convention (verified against
# the installed openlineage-dbt source) - postgres://{host}:{port} +
# {dbname}.{schema}.{table} - so a gold_builds dbt model's dbt-ol lineage
# and this pipeline's own raw/stage lineage register the SAME physical
# table as the SAME dataset in a real OpenLineage backend, rather than two
# disconnected identities.
# ---------------------------------------------------------------------------

def test_table_dataset_uses_postgres_namespace_and_dbname_qualified_name_when_configured():
    transport = RecordingTransport()
    client = build_client(transport)
    emitter = LineageEmitter(
        "datamesh-prod", "sales_ingest", client, db_namespace="postgres://localhost:5432", dbname="GEODS"
    )
    emitter.complete("stage", _run_id(), outputs=["data_in.sales_stage"])

    dataset = transport.events[0].outputs[0]
    assert dataset.namespace == "postgres://localhost:5432"
    assert dataset.name == "GEODS.data_in.sales_stage"


def test_file_dataset_keeps_job_namespace_even_when_db_namespace_configured():
    """A source file path isn't a database table - it has no external
    system's dataset identity to match, so it must stay under the job's
    own logical namespace even when db_namespace/dbname ARE set."""
    transport = RecordingTransport()
    client = build_client(transport)
    emitter = LineageEmitter(
        "datamesh-prod", "sales_ingest", client, db_namespace="postgres://localhost:5432", dbname="GEODS"
    )
    emitter.start("control_gate", _run_id(), inputs=["data/sales/incoming/sales_20260901.csv"])

    dataset = transport.events[0].inputs[0]
    assert dataset.namespace == "datamesh-prod"
    assert dataset.name == "data/sales/incoming/sales_20260901.csv"


def test_table_dataset_falls_back_to_job_namespace_without_db_namespace_configured():
    """Backward compatible: omitting db_namespace/dbname (the pre-fix
    shape) must behave exactly as before - a real caller only omits these
    when connection_params wasn't available to build_emitter."""
    emitter, transport = _emitter()
    emitter.complete("stage", _run_id(), outputs=["data_in.sales_stage"])

    dataset = transport.events[0].outputs[0]
    assert dataset.namespace == "datamesh-prod"
    assert dataset.name == "data_in.sales_stage"


def test_build_emitter_derives_db_namespace_from_connection_params():
    spec = load_spec(VALID_SPEC)
    transport = RecordingTransport()

    emitter = build_emitter(
        spec,
        transport,
        connection_params={"host": "localhost", "port": 5432, "dbname": "GEODS", "user": "x", "password": "y"},
    )
    emitter.complete("stage", _run_id(), outputs=["data_in.holdings_stage"])

    dataset = transport.events[0].outputs[0]
    assert dataset.namespace == "postgres://localhost:5432"
    assert dataset.name == "GEODS.data_in.holdings_stage"


def test_build_emitter_without_connection_params_falls_back_to_job_namespace():
    spec = load_spec(VALID_SPEC)
    transport = RecordingTransport()

    emitter = build_emitter(spec, transport)
    emitter.complete("stage", _run_id(), outputs=["data_in.holdings_stage"])

    dataset = transport.events[0].outputs[0]
    assert dataset.namespace == spec.lineage.namespace
    assert dataset.name == "data_in.holdings_stage"
