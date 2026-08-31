-- Per-ticker gold summary from stage's daily price rows: average trading
-- volume, and total price return over the period covered by stage
-- ((last Close - first Close) / first Close, ordered by Date).

with ordered as (

    select
        "Ticker",
        "Date",
        "Close",
        "Volume",
        row_number() over (partition by "Ticker" order by "Date" asc) as rn_first,
        row_number() over (partition by "Ticker" order by "Date" desc) as rn_last
    from "{{ env_var('DBT_STAGE_SCHEMA') }}"."{{ env_var('DBT_STAGE_TABLE') }}"

),

first_last_close as (

    select
        "Ticker",
        max(case when rn_first = 1 then "Close" end) as first_close,
        max(case when rn_last = 1 then "Close" end) as last_close
    from ordered
    group by "Ticker"

),

avg_volume as (

    select
        "Ticker",
        avg("Volume") as avg_volume
    from "{{ env_var('DBT_STAGE_SCHEMA') }}"."{{ env_var('DBT_STAGE_TABLE') }}"
    group by "Ticker"

)

select
    fl."Ticker" as ticker,
    av.avg_volume,
    (fl.last_close - fl.first_close) / fl.first_close as total_return,
    current_timestamp as gold_loaded_at
from first_last_close fl
join avg_volume av on av."Ticker" = fl."Ticker"
order by fl."Ticker"
