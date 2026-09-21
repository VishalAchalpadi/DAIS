-- One row per (currency pair, source snapshot): its most recent rate, the
-- rate on the previous available date from the SAME source, the
-- day-over-day change, and the inverse rate (to_ccy -> from_ccy).
-- source_type is part of the grain because one date can carry several
-- snapshots of the same pair (e.g. Reuters7AM and Reuters12PM) and they must
-- not be compared against each other. Recomputed from the full stage history
-- on every build, so a correction to an older date is picked up too.

{{ config(tags=['fx_rates_gold'], materialized='table') }}

with ranked as (

    select
        from_ccy,
        to_ccy,
        source_type,
        fx_date,
        fx_rate,
        lag(fx_rate) over (partition by from_ccy, to_ccy, source_type order by fx_date) as prior_rate,
        row_number() over (partition by from_ccy, to_ccy, source_type order by fx_date desc) as recency
    from {{ ref('stg_fx_rates') }}

)

select
    from_ccy,
    to_ccy,
    source_type,
    fx_date as latest_fx_date,
    fx_rate as latest_rate,
    prior_rate,
    case when prior_rate is null or prior_rate = 0 then null
         else round((fx_rate - prior_rate) / prior_rate * 100, 4) end as change_pct,
    round(1 / fx_rate, 8) as inverse_rate,
    current_timestamp as gold_loaded_at
from ranked
where recency = 1
