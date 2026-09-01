-- Per-ticker gold summary from stage's daily price rows: average trading
-- volume, and total price return over the period covered by stage
-- ((last Close - first Close) / first Close, ordered by Date).
--
-- SCD Type 2 (see specs/price_ingest.yaml's gold.scd_type/scd_key and
-- dbt/macros/dais_scd2.sql): every time avg_volume/total_return change
-- for a ticker, a new version is added rather than overwriting the old
-- one - active_flag=true marks the current version, expiry_datetime is
-- set on whichever version got superseded.

{{
  config(
    materialized='incremental',
    unique_key='scd_pk'
  )
}}

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

avg_volume_cte as (

    select
        "Ticker",
        avg("Volume") as avg_volume
    from "{{ env_var('DBT_STAGE_SCHEMA') }}"."{{ env_var('DBT_STAGE_TABLE') }}"
    group by "Ticker"

),

current_snapshot as (

    select
        fl."Ticker" as ticker,
        av.avg_volume,
        (fl.last_close - fl.first_close) / fl.first_close as total_return
    from first_last_close fl
    join avg_volume_cte av on av."Ticker" = fl."Ticker"

)

{{ dais_scd2('current_snapshot', ['ticker'], ['avg_volume', 'total_return']) }}
