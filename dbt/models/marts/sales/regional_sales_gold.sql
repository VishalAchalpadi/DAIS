-- Final gold mart for the regional_sales_gold build: one row per
-- (region, item_type), rolling up order-level sales into a summary a
-- BI dashboard would actually query. No intermediate layer needed here -
-- unlike portfolio_summary_gold, this is a single-source aggregation,
-- not a multi-pipeline join.

{{ config(tags=['regional_sales_gold'], materialized='table') }}

select
    region,
    item_type,
    count(*) as order_count,
    sum(units_sold) as total_units_sold,
    sum(total_revenue) as total_revenue,
    sum(total_profit) as total_profit,
    current_timestamp as gold_loaded_at
from {{ ref('stg_sales') }}
group by region, item_type
