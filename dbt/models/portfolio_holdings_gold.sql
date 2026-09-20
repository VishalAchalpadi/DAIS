-- Computes each position's market value (shares * price * fx - fx
-- converts the position's local-currency price into a common base
-- currency) and its weight within its own portfolio (market_value
-- divided by that portfolio's total market_value on this holding_date).
--
-- holding_date is set to the date this gold model actually runs
-- (current_date), not the source file's own "date" field - the gold
-- table always reflects "as of today" regardless of which of the day's
-- resubmitted source files (see source.location.multi_file in the spec)
-- produced the row.
--
-- Stage schema/table come from env vars (set by medallion/gold.py from
-- spec.stage.schema/table), same convention as every other inline
-- `gold:` model in this project - see holdings_gold.sql's comment for
-- why (a shared dbt project can't hardcode a spec-specific schema name).
with positions as (

    select
        portfolio,
        security,
        "date" as source_date,
        shares,
        price,
        currency,
        fx,
        (shares * price * fx) as market_value,
        current_date as holding_date
    from "{{ env_var('DBT_STAGE_SCHEMA', 'unused') }}"."{{ env_var('DBT_STAGE_TABLE', 'unused') }}"

)

select
    *,
    market_value / sum(market_value) over (partition by portfolio, holding_date) as weight
from positions
