-- Thin, ~1:1 model over holdings_ingest's stage table - heavy cleaning
-- already happened in raw->stage (DAIS's own DQ engine), so this layer
-- only renames for downstream convenience (account_id -> portfolio_id,
-- a naming-convention normalization for this demo join, not a real
-- account/portfolio mapping table).

{{ config(tags=['portfolio_summary_gold'], materialized='view') }}

select
    account_id as portfolio_id,
    security_id,
    as_of_date,
    quantity,
    market_value,
    currency
from {{ source('holdings_ingest', 'stage') }}
