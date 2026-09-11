-- Company-wide AUM: aggregates every portfolio's USD-normalized
-- total_net_assets_usd (already validated/typed/currency-converted in
-- stage) by as_of_date, then compares each day's total against the
-- prior day via LAG() to produce the daily percentage change
-- (e.g. $1.0m -> $1.1m becomes 0.10, i.e. +10%).
--
-- as_of_date here is a real business column already in stage (unlike
-- price_gold, which has to derive its as_of_date from the source file
-- name) - it's the natural SCD Type 2 key: one row per as_of_date,
-- versioned if a later run recomputes a different company_aum/
-- daily_aum_change_pct for a date already seen (e.g. a late-arriving
-- correction file for an earlier date) - see specs/asset_ingest.yaml's
-- gold.scd_type/scd_key and dbt/macros/dais_scd2.sql.
--
-- env_var() defaults (Phase 9a): dbt parses every model in this shared
-- project before executing any --select subset, so a gold_builds/*.yaml
-- run (which never sets DBT_STAGE_SCHEMA/TABLE - it uses source() and
-- sources.yml instead) would otherwise fail at parse time on this
-- model even though it's never selected/executed. The default is only
-- ever read when this specific model isn't the one actually running.

{{
  config(
    materialized='incremental',
    unique_key='scd_pk'
  )
}}

with daily_aum as (

    select
        as_of_date,
        sum(total_net_assets_usd) as company_aum
    from "{{ env_var('DBT_STAGE_SCHEMA', 'unused') }}"."{{ env_var('DBT_STAGE_TABLE', 'unused') }}"
    group by as_of_date

),

current_snapshot as (

    select
        as_of_date,
        company_aum,
        (company_aum - lag(company_aum) over (order by as_of_date))
            / lag(company_aum) over (order by as_of_date) as daily_aum_change_pct
    from daily_aum

)

{{ dais_scd2('current_snapshot', ['as_of_date'], ['company_aum', 'daily_aum_change_pct']) }}
