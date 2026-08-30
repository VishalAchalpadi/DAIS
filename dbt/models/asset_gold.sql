-- Company-wide AUM: aggregates every portfolio's USD-normalized
-- total_net_assets_usd (already validated/typed/currency-converted in
-- stage) by as_of_date, then compares each day's total against the
-- prior day via LAG() to produce the daily percentage change
-- (e.g. $1.0m -> $1.1m becomes 0.10, i.e. +10%).

with daily_aum as (

    select
        as_of_date,
        sum(total_net_assets_usd) as company_aum
    from "{{ env_var('DBT_STAGE_SCHEMA') }}"."{{ env_var('DBT_STAGE_TABLE') }}"
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
