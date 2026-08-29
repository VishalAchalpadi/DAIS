import uuid

from openlineage.client import OpenLineageClient
from openlineage.client.run import RunState
from openlineage.client.transport.transport import Transport

from dais.lineage.emitter import LineageEmitter, build_client
from dais.spec.models import LineageConfig


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


def test_build_client_uses_provided_transport_without_network_call():
    transport = RecordingTransport()
    client = build_client(transport)
    emitter = LineageEmitter("ns", "job", client)
    emitter.start("raw", _run_id())
    assert len(transport.events) == 1
