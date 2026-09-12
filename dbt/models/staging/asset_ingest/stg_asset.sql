-- Thin, ~1:1 model over asset_ingest's stage table - renamed for
-- downstream convenience (portfolio_cd -> portfolio_id, matching
-- stg_holdings' naming).

{{ config(tags=['portfolio_summary_gold'], materialized='view') }}

select
    portfolio_cd as portfolio_id,
    as_of_date,
    total_net_assets_usd,
    base_currency
from {{ source('asset_ingest', 'asset_stage') }}
