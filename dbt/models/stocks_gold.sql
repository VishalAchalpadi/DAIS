-- Per-symbol return metrics from stage's stock reference snapshot:
-- total return from initial_price to price_2007, and the return over
-- just the 2002->2007 window.
--
-- symbol is the natural SCD Type 2 key (see specs/stocks_ingest.yaml's
-- gold.scd_type/scd_key and dbt/macros/dais_scd2.sql): this is static
-- reference data, not a daily time series, so there's no as_of_date to
-- key on the way price_gold/asset_gold do - a symbol gets a new version
-- only when a later ingest of this feed reports different prices for it
-- (e.g. a corrected source file), not on every run.
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

with current_snapshot as (

    select
        symbol,
        (price_2007 - initial_price) / initial_price as total_return_pct,
        (price_2007 - price_2002) / price_2002 as return_2002_to_2007_pct
    from "{{ env_var('DBT_STAGE_SCHEMA', 'unused') }}"."{{ env_var('DBT_STAGE_TABLE', 'unused') }}"

)

{{ dais_scd2('current_snapshot', ['symbol'], ['total_return_pct', 'return_2002_to_2007_pct']) }}
