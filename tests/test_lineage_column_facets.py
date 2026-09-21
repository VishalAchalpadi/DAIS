"""Column-level lineage travels as standard OpenLineage `schema` and
`columnLineage` dataset facets: LineageEmitter attaches them for DAIS's own
raw/stage steps (dbt-ol already does for gold), and the OpenMetadata
forwarder turns them into table columns + column-level edges. No DB access,
extra command or spec parsing is involved on the receiving side."""
import uuid

import pytest
from openlineage.client import OpenLineageClient
from openlineage.client.transport.transport import Transport

from dais.lineage import openmetadata_forwarder as fwd
from dais.lineage.emitter import LineageEmitter

NS = "postgres://localhost:5432"
RAW = "GEODS.data_in.fx_raw"
STAGE = "GEODS.data_in.fx_stage"
RAW_FQN = "dais_postgres_localhost_5432.GEODS.data_in.fx_raw"
STAGE_FQN = "dais_postgres_localhost_5432.GEODS.data_in.fx_stage"


class _Recording(Transport):
    kind = "recording"

    def __init__(self, config=None):
        self.events = []

    def emit(self, event):
        self.events.append(event)


def test_emitter_attaches_schema_and_column_lineage_facets_to_the_output_dataset():
    transport = _Recording()
    emitter = LineageEmitter(
        "datamesh-prod", "fx", OpenLineageClient(transport=transport), db_namespace=NS, dbname="GEODS"
    )
    emitter.complete(
        "stage",
        str(uuid.uuid4()),
        outputs=["data_in.fx_stage"],
        schemas={"data_in.fx_stage": [("fxRate", "NUMERIC(18, 8)"), ("fxDate", "DATE")]},
        column_lineage={"data_in.fx_stage": [("fxRate", "data_in.fx_raw", "fxRate", "cast_to=decimal")]},
    )

    output = transport.events[0].outputs[0]
    assert [(f.name, f.type) for f in output.facets["schema"].fields] == [("fxRate", "NUMERIC(18, 8)"), ("fxDate", "DATE")]
    field = output.facets["columnLineage"].fields["fxRate"]
    assert field.transformationDescription == "cast_to=decimal"
    assert (field.inputFields[0].namespace, field.inputFields[0].name, field.inputFields[0].field) == (NS, RAW, "fxRate")


def test_emitter_without_facet_arguments_still_emits_plain_datasets():
    transport = _Recording()
    emitter = LineageEmitter("datamesh-prod", "fx", OpenLineageClient(transport=transport))
    emitter.complete("raw", str(uuid.uuid4()), outputs=["data_in.fx_raw"])
    assert transport.events[0].outputs[0].facets == {}


# ---------------------------------------------------------------------------
# forwarder, against a fake OpenMetadata
# ---------------------------------------------------------------------------

class _FakeOpenMetadata:
    def __init__(self):
        self.puts = []
        self.patches = []
        self.columns = {}  # table fqn -> existing column names
        self.tables = set()  # table fqns that exist

    def put(self, path, body):
        self.puts.append((path, body))
        if path == "tables":
            self.tables.add(f"{body['databaseSchema']}.{body['name']}")
            return {"id": f"id-{body['name']}"}
        if path == "pipelines":
            return {"id": "pipeline-1"}
        return {}

    def get(self, path):
        fqn = path.split("?")[0].removeprefix("tables/name/")
        if fqn not in self.tables and fqn not in self.columns:
            return None
        return {"id": f"id-{fqn.rsplit('.', 1)[-1]}", "columns": [{"name": n} for n in self.columns.get(fqn, [])]}

    def patch(self, path, ops):
        self.patches.append((path.removeprefix("tables/name/"), ops))

    def edges(self):
        return [body["edge"] for path, body in self.puts if path == "lineage"]


@pytest.fixture
def om(monkeypatch):
    fake = _FakeOpenMetadata()
    monkeypatch.setattr(fwd, "_put", fake.put)
    monkeypatch.setattr(fwd, "_get", fake.get)
    monkeypatch.setattr(fwd, "_patch", fake.patch)
    fwd._run_datasets.clear()
    fwd._edge_columns_cache.clear()
    return fake


def _event(kind, job, run_id, *, inputs=(), outputs=()):
    return {
        "eventType": kind,
        "job": {"namespace": "datamesh-prod", "name": job},
        "run": {"runId": run_id},
        "inputs": [{"namespace": NS, "name": n, "facets": f} for n, f in inputs],
        "outputs": [{"namespace": NS, "name": n, "facets": f} for n, f in outputs],
    }


STAGE_FACETS = {
    "schema": {"fields": [{"name": "fxRate", "type": "NUMERIC(18, 8)"}, {"name": "fxDate", "type": "DATE"}]},
    "columnLineage": {
        "fields": {
            "fxRate": {
                "inputFields": [{"namespace": NS, "name": RAW, "field": "fxRate"}],
                "transformationDescription": "cast_to=decimal",
            }
        }
    },
}


def test_stage_step_creates_typed_columns_and_a_column_level_edge(om):
    run = str(uuid.uuid4())
    fwd._handle_run_event(_event("START", "fx.stage", run, inputs=[(RAW, {})], outputs=[(STAGE, {})]))
    assert om.edges() == []  # nothing until COMPLETE
    fwd._handle_run_event(_event("COMPLETE", "fx.stage", run, outputs=[(STAGE, STAGE_FACETS)]))

    added = {fqn: [op["value"] for op in ops] for fqn, ops in om.patches}
    assert added[STAGE_FQN] == [
        {"name": "fxRate", "dataType": "NUMERIC"},
        {"name": "fxDate", "dataType": "DATE"},
    ]
    assert added[RAW_FQN] == [{"name": "fxRate", "dataType": "UNKNOWN"}]  # known only from column lineage

    (edge,) = om.edges()
    assert edge["lineageDetails"]["columnsLineage"] == [
        {"fromColumns": [f"{RAW_FQN}.fxRate"], "toColumn": f"{STAGE_FQN}.fxRate", "function": "cast_to=decimal"}
    ]


def test_dbt_ol_style_event_with_inputs_outputs_and_column_lineage_only(om):
    facets = {"columnLineage": {"fields": {"from_ccy": {"inputFields": [{"namespace": NS, "name": STAGE, "field": "fromCCY"}]}}}}
    fwd._handle_run_event(_event("COMPLETE", "dbt.model", str(uuid.uuid4()), inputs=[(STAGE, {})], outputs=[("GEODS.core.stg", facets)]))

    (edge,) = om.edges()
    assert edge["lineageDetails"]["columnsLineage"] == [
        {
            "fromColumns": [f"{STAGE_FQN}.fromCCY"],
            "toColumn": "dais_postgres_localhost_5432.GEODS.core.stg.from_ccy",
        }
    ]


def test_existing_columns_are_never_re_added_or_replaced(om):
    om.columns[STAGE_FQN] = ["fxRate", "fxDate"]  # e.g. already tagged with glossary terms
    om.columns[RAW_FQN] = ["fxRate"]
    run = str(uuid.uuid4())
    fwd._handle_run_event(_event("START", "fx.stage", run, inputs=[(RAW, {})]))
    fwd._handle_run_event(_event("COMPLETE", "fx.stage", run, outputs=[(STAGE, STAGE_FACETS)]))
    assert om.patches == []


def test_a_later_event_without_column_facets_does_not_wipe_the_edges_column_lineage(om):
    run = str(uuid.uuid4())
    fwd._handle_run_event(_event("START", "fx.stage", run, inputs=[(RAW, {})]))
    fwd._handle_run_event(_event("COMPLETE", "fx.stage", run, outputs=[(STAGE, STAGE_FACETS)]))
    # e.g. DAIS's own gold-step event for the same table pair, which carries no facets
    run2 = str(uuid.uuid4())
    fwd._handle_run_event(_event("START", "fx.other", run2, inputs=[(RAW, {})]))
    fwd._handle_run_event(_event("COMPLETE", "fx.other", run2, outputs=[(STAGE, {})]))

    first, second = om.edges()
    assert second["lineageDetails"]["columnsLineage"] == first["lineageDetails"]["columnsLineage"]


@pytest.mark.parametrize(
    "source,expected",
    [("TEXT", "VARCHAR"), ("NUMERIC(18, 8)", "NUMERIC"), ("DATE", "DATE"), ("BIGINT", "BIGINT"), (None, "UNKNOWN"), ("geometry", "UNKNOWN")],
)
def test_type_mapping(source, expected):
    assert fwd._om_type(source) == expected


def test_a_mapping_repeated_across_start_and_complete_is_recorded_once(om):
    facets = {"columnLineage": {"fields": {"from_ccy": {"inputFields": [{"namespace": NS, "name": STAGE, "field": "fromCCY"}]}}}}
    run = str(uuid.uuid4())
    fwd._handle_run_event(_event("START", "dbt.model", run, inputs=[(STAGE, {})], outputs=[("GEODS.core.stg", facets)]))
    fwd._handle_run_event(_event("COMPLETE", "dbt.model", run, inputs=[(STAGE, {})], outputs=[("GEODS.core.stg", facets)]))

    (edge,) = om.edges()
    assert edge["lineageDetails"]["columnsLineage"][0]["fromColumns"] == [f"{STAGE_FQN}.fromCCY"]


def test_an_existing_table_is_never_re_put_because_that_prunes_its_column_lineage(om):
    run = str(uuid.uuid4())
    fwd._handle_run_event(_event("START", "fx.stage", run, inputs=[(RAW, {})]))
    fwd._handle_run_event(_event("COMPLETE", "fx.stage", run, outputs=[(STAGE, STAGE_FACETS)]))
    tables_put_first = [b["name"] for p, b in om.puts if p == "tables"]

    run2 = str(uuid.uuid4())
    fwd._handle_run_event(_event("START", "fx.stage", run2, inputs=[(RAW, {})]))
    fwd._handle_run_event(_event("COMPLETE", "fx.stage", run2, outputs=[(STAGE, STAGE_FACETS)]))

    assert sorted(tables_put_first) == ["fx_raw", "fx_stage"]
    assert [b["name"] for p, b in om.puts if p == "tables"] == tables_put_first  # no second PUT
