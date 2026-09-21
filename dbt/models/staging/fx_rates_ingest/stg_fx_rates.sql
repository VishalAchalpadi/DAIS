-- Renames fx_rates_ingest's stage columns (which keep the feed's own
-- camelCase header: "fxDate", "fromCCY", ...) to snake_case for everything
-- downstream. The quoting is required - Postgres keeps the exact case
-- because DAIS creates these columns as quoted identifiers.

{{ config(tags=['fx_rates_gold'], materialized='view') }}

select
    "fxDate"     as fx_date,
    "fromCCY"    as from_ccy,
    "toCCY"      as to_ccy,
    "fxRate"     as fx_rate,
    "sourceType" as source_type
from {{ source('fx_rates_ingest', 'fx_rates_stage') }}
