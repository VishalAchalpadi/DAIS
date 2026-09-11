-- Final gold mart for the portfolio_summary_gold build: one row per
-- (portfolio_id, as_of_date), comparing holdings_ingest's rolled-up
-- position value against asset_ingest's independently-reported NAV.

{{ config(tags=['portfolio_summary_gold'], materialized='table') }}

select
    portfolio_id,
    as_of_date,
    holdings_market_value,
    holding_count,
    total_net_assets_usd,
    base_currency,
    (total_net_assets_usd - holdings_market_value) as reconciliation_diff,
    current_timestamp as gold_loaded_at
from {{ ref('int_portfolio_positions') }}
