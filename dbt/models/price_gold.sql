-- Per-(ticker, as_of_date) gold summary from stage's daily price rows:
-- average trading volume, and total price return over the period
-- covered by stage ((last Close - first Close) / first Close, ordered
-- by Date).
--
-- as_of_date is NOT derived from stage data - it's the date embedded in
-- the SOURCE FILE NAME that triggered this run (e.g.
-- portfolio_prices_20260830.csv -> 2026-08-30), passed in via the
-- DBT_AS_OF_DATE env var by medallion/gold.py. Falls back to
-- current_date if unset (e.g. a quarantine-resubmit-triggered gold
-- refresh, which has no single source file).
--
-- SCD Type 2 (see specs/price_ingest.yaml's gold.scd_type/scd_key and
-- dbt/macros/dais_scd2.sql): the business key is (ticker, as_of_date),
-- NOT ticker alone - a new as_of_date is a different key, not a new
-- version of an existing one. A version only increments when
-- avg_volume/total_return change for the SAME (ticker, as_of_date)
-- pair on a later run (e.g. a same-day correction file) -
-- active_flag=true marks the current version of each pair,
-- expiry_datetime is set on whichever version got superseded.
--
-- env_var() defaults (Phase 9a): see the matching note in asset_gold.sql
-- - dbt parses every model in the shared project up front, so a
-- gold_builds/*.yaml run needs these to have a default even though this
-- model is never selected/executed in that case.

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
    from "{{ env_var('DBT_STAGE_SCHEMA', 'unused') }}"."{{ env_var('DBT_STAGE_TABLE', 'unused') }}"

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
    from "{{ env_var('DBT_STAGE_SCHEMA', 'unused') }}"."{{ env_var('DBT_STAGE_TABLE', 'unused') }}"
    group by "Ticker"

),

current_snapshot as (

    select
        fl."Ticker" as ticker,
        av.avg_volume,
        (fl.last_close - fl.first_close) / fl.first_close as total_return,
        coalesce(cast(nullif('{{ env_var("DBT_AS_OF_DATE", "") }}', '') as date), current_date) as as_of_date
    from first_last_close fl
    join avg_volume_cte av on av."Ticker" = fl."Ticker"

)

{{ dais_scd2('current_snapshot', ['ticker', 'as_of_date'], ['avg_volume', 'total_return']) }}
