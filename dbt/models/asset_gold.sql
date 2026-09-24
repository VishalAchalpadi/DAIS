-- Portfolio-level pass-through of asset_ingest's stage table, plus
-- tna_usd: total_net_assets converted to USD via a daily rate looked up
-- from fx_rates_ingest's own stage table (fx_rates_ingest is a separate
-- pipeline - referenced here as a shared reference lookup via source(),
-- the same spirit as the core.t_ref_currency SQL lookup quality.rules
-- already do at validation time, not a business dependency the way
-- portfolio_summary_gold's depends_on is).
--
-- NOTE: this used to aggregate every portfolio into one company-wide
-- daily AUM row (company_aum/daily_aum_change_pct) - that rollup was
-- dropped when the source file stopped supplying total_net_assets_usd
-- and the grain moved to portfolio-level (portfolio_cd, as_of_date) -
-- see specs/asset_ingest.yaml. THIS ALSO BREAKS portfolio_summary_gold
-- (via int_portfolio_positions.sql / stg_asset.sql), which still reads
-- total_net_assets_usd from asset_stage directly - left broken on
-- purpose, a separate fix.
--
-- Multiple same-day FX quotes (e.g. Reuters7AM + Reuters12PM) are
-- averaged into one daily rate per (from_ccy, as_of_date) rather than
-- picking one source arbitrarily. base_currency = 'USD' short-circuits
-- to tna_usd = total_net_assets (no FX lookup needed - and none
-- required to exist). A (base_currency, as_of_date) with no FX rate at
-- all leaves tna_usd NULL rather than failing the run.
--
-- env_var() defaults (Phase 9a): dbt parses every model in this shared
-- project before executing any --select subset, so a gold_builds/*.yaml
-- run (which never sets DBT_STAGE_SCHEMA/TABLE - it uses source() and
-- sources.yml instead) would otherwise fail at parse time on this
-- model even though it's never selected/executed. The default is only
-- ever read when this specific model isn't the one actually running -
-- the source() reference to fx_rates_ingest's stage table below is
-- static and unaffected by that.

{{
  config(
    materialized='incremental',
    unique_key='scd_pk'
  )
}}

with fx_daily as (

    select
        "fromCCY" as from_ccy,
        "fxDate" as fx_date,
        avg("fxRate") as fx_rate
    from {{ source('fx_rates_ingest', 'fx_rates_stage') }}
    where "toCCY" = 'USD'
    group by "fromCCY", "fxDate"

),

current_snapshot as (

    select
        a.portfolio_cd,
        a.as_of_date,
        a.total_net_assets,
        a.base_currency,
        case
            when a.base_currency = 'USD' then a.total_net_assets
            else a.total_net_assets * fx.fx_rate
        end as tna_usd
    from "{{ env_var('DBT_STAGE_SCHEMA', 'unused') }}"."{{ env_var('DBT_STAGE_TABLE', 'unused') }}" a
    left join fx_daily fx
        on fx.from_ccy = a.base_currency
       and fx.fx_date = a.as_of_date

)

{{ dais_scd2('current_snapshot', ['portfolio_cd', 'as_of_date'], ['total_net_assets', 'base_currency', 'tna_usd']) }}
