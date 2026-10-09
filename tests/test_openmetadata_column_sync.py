"""sync_column_lineage's stage->gold grouping - exercising two real bugs
found while debugging asset_gold's missing FX lineage in OpenMetadata:

1. _push_hop used to assume every stage->gold edge shared ONE upstream
   table (this pipeline's own stage table) - wrong the moment a gold
   model reads a second table too (asset_gold's FX rate lookup against
   fx_rates_ingest's stage table). Edges must be grouped by each edge's
   OWN source table instead.
2. The SCD2 macro's internal self-join makes sqllineage report some
   columns' source as the gold table itself (a real but useless self-loop)
   or as an unresolved internal CTE name (no real table at all, e.g.
   effective_datetime from current_timestamp) - both must be dropped,
   not pushed as if they were real table-to-table OM edges.

build_lineage_graph() is mocked throughout - a real dbt compile isn't
needed to test this grouping/filtering logic, only load_spec against the
real spec is used so schema/table/namespace values are realistic."""
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from dais.lineage import openmetadata_column_sync as sync
from dais.lineage.column_lineage import ColumnEdge, ColumnNode
from dais.spec.loader import load_spec

SPEC = load_spec(Path(__file__).parent.parent / "specs" / "asset_ingest.yaml")


def _edge(source_table, source_col, target_col, transformation="dbt: x") -> ColumnEdge:
    return ColumnEdge(
        source=ColumnNode("stage", source_table, source_col),
        target=ColumnNode("gold", f"{SPEC.gold.schema_}.{SPEC.gold.dbt_select}", target_col),
        transformation=transformation,
    )


@pytest.fixture
def mocks(monkeypatch):
    m = {
        "ensure_columns": MagicMock(),
        "table_id": MagicMock(side_effect=lambda cp, schema, table: f"id-{schema}.{table}"),
        "ensure_pipeline": MagicMock(return_value="pipeline-1"),
        "put": MagicMock(),
    }
    monkeypatch.setattr(sync, "_ensure_columns", m["ensure_columns"])
    monkeypatch.setattr(sync, "_table_id", m["table_id"])
    monkeypatch.setattr(sync, "_ensure_pipeline_entity", m["ensure_pipeline"])
    monkeypatch.setattr(sync, "_put", m["put"])
    return m


def _lineage_calls(put_mock) -> list[dict]:
    return [call.args[1]["edge"] for call in put_mock.call_args_list if call.args[0] == "lineage"]


def test_edges_from_two_different_source_tables_produce_two_separate_edges(mocks, monkeypatch):
    edges = [
        _edge("data_in.asset_stage", "total_net_assets", "tna_usd", "dbt: case when ... end"),
        _edge("data_in.fx_rates_stage", "fxRate", "tna_usd", "dbt: case when ... end"),
    ]
    monkeypatch.setattr(sync, "build_lineage_graph", lambda spec, **cp: edges)

    sync.sync_column_lineage(SPEC, {"host": "h", "port": 1, "dbname": "d", "user": "u", "password": "p"})

    calls = _lineage_calls(mocks["put"])
    assert len(calls) == 2
    from_fqns = {c["fromEntity"]["id"] for c in calls}
    assert from_fqns == {"id-data_in.asset_stage", "id-data_in.fx_rates_stage"}
    for c in calls:
        assert c["toEntity"]["id"] == f"id-{SPEC.gold.schema_}.{SPEC.gold.dbt_select}"
        assert c["lineageDetails"]["columnsLineage"][0]["function"] == "dbt: case when ... end"


def test_a_3part_source_table_and_a_2part_source_table_for_the_same_table_merge_into_one_edge(mocks, monkeypatch):
    # sqllineage returns "data_in.fx_rates_stage" (2-part, env_var-style)
    # for one column and "GEODS.data_in.fx_rates_stage" (3-part, from a
    # real source()) for another - both the SAME physical table.
    edges = [
        _edge("data_in.fx_rates_stage", "fxRate", "tna_usd"),
        _edge("GEODS.data_in.fx_rates_stage", "fxDate", "as_of_date"),
    ]
    monkeypatch.setattr(sync, "build_lineage_graph", lambda spec, **cp: edges)

    sync.sync_column_lineage(SPEC, {"host": "h", "port": 1, "dbname": "d", "user": "u", "password": "p"})

    calls = _lineage_calls(mocks["put"])
    assert len(calls) == 1
    assert calls[0]["fromEntity"]["id"] == "id-data_in.fx_rates_stage"
    assert len(calls[0]["lineageDetails"]["columnsLineage"]) == 2


def test_self_loop_edge_to_the_gold_table_itself_is_dropped(mocks, monkeypatch):
    gold_table = f"{SPEC.gold.schema_}.{SPEC.gold.dbt_select}"
    edges = [
        _edge("data_in.asset_stage", "total_net_assets", "total_net_assets"),
        _edge(f"GEODS.{gold_table}", "version", "version"),  # SCD2's own prior-version self-join
    ]
    monkeypatch.setattr(sync, "build_lineage_graph", lambda spec, **cp: edges)

    sync.sync_column_lineage(SPEC, {"host": "h", "port": 1, "dbname": "d", "user": "u", "password": "p"})

    calls = _lineage_calls(mocks["put"])
    assert len(calls) == 1
    assert calls[0]["fromEntity"]["id"] == "id-data_in.asset_stage"


def test_edge_with_no_resolvable_source_table_is_dropped(mocks, monkeypatch):
    # effective_datetime <- current_timestamp: sqllineage can report the
    # internal macro CTE name ("expired") as the "source table" when it
    # can't trace a literal/function call back to any real table.
    edges = [
        _edge("data_in.asset_stage", "total_net_assets", "total_net_assets"),
        _edge("expired", "_expire_at", "expiry_datetime"),
    ]
    monkeypatch.setattr(sync, "build_lineage_graph", lambda spec, **cp: edges)

    sync.sync_column_lineage(SPEC, {"host": "h", "port": 1, "dbname": "d", "user": "u", "password": "p"})

    calls = _lineage_calls(mocks["put"])
    assert len(calls) == 1
    assert calls[0]["fromEntity"]["id"] == "id-data_in.asset_stage"


def test_normalize_schema_table():
    assert sync._normalize_schema_table("data_in.asset_stage") == "data_in.asset_stage"
    assert sync._normalize_schema_table("GEODS.data_in.fx_rates_stage") == "data_in.fx_rates_stage"
    assert sync._normalize_schema_table("CORE.Asset_Gold") == "core.asset_gold"
    assert sync._normalize_schema_table("expired") == "expired"  # no dot - caller must detect this
