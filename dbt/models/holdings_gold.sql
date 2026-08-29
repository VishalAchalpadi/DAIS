-- Minimal silver -> gold pass-through: holdings_ingest's stage table,
-- already validated and typed by the DQ layer, materialized as the
-- gold table analytics consumers query.
--
-- The stage schema/table are resolved from env vars (set by
-- medallion/gold.py from spec.stage.schema/table) rather than a fixed
-- dbt source(), since a spec's schema names aren't compile-time
-- constants - the SAME database/connection as raw and stage, but the
-- schema itself is spec-driven.
select *
from "{{ env_var('DBT_STAGE_SCHEMA') }}"."{{ env_var('DBT_STAGE_TABLE') }}"
