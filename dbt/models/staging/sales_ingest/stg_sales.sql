-- Thin, ~1:1 model over sales_ingest's stage table - no renaming needed,
-- column names already match the mart's grouping/aggregation needs.

{{ config(tags=['regional_sales_gold'], materialized='view') }}

select
    order_id,
    region,
    country,
    item_type,
    sales_channel,
    order_date,
    units_sold,
    total_revenue,
    total_profit
from {{ source('sales_ingest', 'sales_stage') }}
