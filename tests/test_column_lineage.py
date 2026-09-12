from pathlib import Path

from dais.lineage.column_lineage import (
    build_lineage_graph,
    raw_to_stage_edges,
    stage_to_gold_edges_from_sql,
)
from dais.spec.loader import load_spec

FIXTURES = Path(__file__).parent / "fixtures"
VALID_SPEC = FIXTURES / "valid_holdings_ingest.yaml"


# ---------------------------------------------------------------------------
# raw -> stage: derived from the spec, no SQL involved
# ---------------------------------------------------------------------------

def test_raw_to_stage_edges_cover_every_quality_rule_column():
    spec = load_spec(VALID_SPEC)
    edges = raw_to_stage_edges(spec)

    edge_columns = {e.source.column for e in edges}
    rule_columns = {rule.column for rule in spec.quality.rules}
    assert rule_columns.issubset(edge_columns)


def test_raw_to_stage_edges_use_correct_layer_and_table_names():
    spec = load_spec(VALID_SPEC)
    edges = raw_to_stage_edges(spec)

    account_id_edge = next(e for e in edges if e.source.column == "account_id")
    assert account_id_edge.source.layer == "raw"
    assert account_id_edge.source.table == f"{spec.raw.schema_}.{spec.raw.table}"
    assert account_id_edge.target.layer == "stage"
    assert account_id_edge.target.table == f"{spec.stage.schema_}.{spec.stage.table}"
    assert account_id_edge.target.column == "account_id"


def test_raw_to_stage_edge_transformation_label_reflects_cast_and_checks():
    spec = load_spec(VALID_SPEC)
    edges = raw_to_stage_edges(spec)

    quantity_edge = next(e for e in edges if e.source.column == "quantity")
    assert "cast_to=decimal" in quantity_edge.transformation
    assert "checks=" in quantity_edge.transformation


def test_raw_to_stage_includes_business_key_columns_without_a_rule():
    spec = load_spec(VALID_SPEC)
    data = spec.model_copy(deep=True)
    # business_key columns must always appear even if a column happens to
    # carry no quality.rules entry of its own.
    edges = raw_to_stage_edges(data)
    edge_columns = {e.source.column for e in edges}
    assert set(data.stage.business_key).issubset(edge_columns)


# ---------------------------------------------------------------------------
# stage -> gold: parsed from compiled dbt SQL via sqllineage
# ---------------------------------------------------------------------------

_COMPILED_SQL = """
with daily_aum as (

    select
        as_of_date,
        sum(total_net_assets_usd) as company_aum
    from "data_in"."asset_stage"
    group by as_of_date

)

select
    as_of_date,
    company_aum,
    (company_aum - lag(company_aum) over (order by as_of_date))
        / lag(company_aum) over (order by as_of_date) as daily_aum_change_pct,
    current_timestamp as gold_loaded_at
from daily_aum
order by as_of_date
"""


def _asset_gold_spec():
    return load_spec(FIXTURES.parent.parent / "specs" / "asset_ingest.yaml")


def test_stage_to_gold_edges_trace_aggregation_column():
    spec = _asset_gold_spec()
    edges = stage_to_gold_edges_from_sql(_COMPILED_SQL, spec)

    aum_edge = next(e for e in edges if e.target.column == "company_aum")
    assert aum_edge.source.column == "total_net_assets_usd"
    assert aum_edge.source.layer == "stage"
    assert aum_edge.target.layer == "gold"
    assert "sum(total_net_assets_usd)" in aum_edge.transformation


def test_stage_to_gold_edges_trace_window_function_to_both_source_columns():
    spec = _asset_gold_spec()
    edges = stage_to_gold_edges_from_sql(_COMPILED_SQL, spec)

    change_edges = [e for e in edges if e.target.column == "daily_aum_change_pct"]
    source_columns = {e.source.column for e in change_edges}
    assert source_columns == {"as_of_date", "total_net_assets_usd"}
    assert all("lag(company_aum)" in e.transformation for e in change_edges)


def test_stage_to_gold_edges_omit_columns_with_no_source_column():
    # gold_loaded_at comes from `current_timestamp`, a literal with no
    # upstream column - it must not appear as an edge target at all.
    spec = _asset_gold_spec()
    edges = stage_to_gold_edges_from_sql(_COMPILED_SQL, spec)
    assert not any(e.target.column == "gold_loaded_at" for e in edges)


def test_stage_to_gold_edge_target_table_matches_gold_config():
    spec = _asset_gold_spec()
    edges = stage_to_gold_edges_from_sql(_COMPILED_SQL, spec)
    assert all(e.target.table == f"{spec.gold.schema_}.{spec.gold.dbt_select}" for e in edges)


# ---------------------------------------------------------------------------
# build_lineage_graph: spec.gold is optional since Phase 9a (gold logic can
# live in a gold_builds/*.yaml GoldBuildSpec instead) - this must not
# attempt to compile a dbt model that doesn't exist on the PipelineSpec.
# ---------------------------------------------------------------------------

def test_build_lineage_graph_skips_stage_to_gold_when_gold_block_absent():
    spec = load_spec(Path(__file__).parent.parent / "specs" / "sales_ingest.yaml")
    assert spec.gold is None

    edges = build_lineage_graph(spec, host="unused", port=5432, dbname="unused", user="unused", password="unused")

    assert len(edges) > 0
    assert all(e.target.layer == "stage" for e in edges)
