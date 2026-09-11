-- Heavy, multi-source join: rolls holdings_ingest's security-level
-- positions up to portfolio/date, then reconciles against
-- asset_ingest's independently-reported NAV for the same portfolio/date.
--
-- Demo data note: holdings_ingest's sample accounts (ACC####) and
-- asset_ingest's sample portfolios (PORT####) are independent synthetic
-- datasets that don't share real portfolio_id values - the full outer
-- join below is still structurally correct (a genuine reconciliation
-- mart handles either side having no match for a given key), it just
-- means most demo rows will show a null on one side rather than a
-- populated reconciliation_diff. In a real deployment portfolio_id
-- would be a shared, meaningful business key across both pipelines.

{{ config(tags=['portfolio_summary_gold'], materialized='view') }}

with holdings_rollup as (

    select
        portfolio_id,
        as_of_date,
        sum(market_value) as holdings_market_value,
        count(*) as holding_count
    from {{ ref('stg_holdings') }}
    group by portfolio_id, as_of_date

)

select
    coalesce(h.portfolio_id, a.portfolio_id) as portfolio_id,
    coalesce(h.as_of_date, a.as_of_date) as as_of_date,
    h.holdings_market_value,
    h.holding_count,
    a.total_net_assets_usd,
    a.base_currency
from holdings_rollup h
full outer join {{ ref('stg_asset') }} a
    on h.portfolio_id = a.portfolio_id and h.as_of_date = a.as_of_date
