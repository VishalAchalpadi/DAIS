-- Minimal silver -> gold pass-through: holdings_ingest's stage table,
-- already validated and typed by the DQ layer, materialized as the
-- gold table analytics consumers query.
--
-- The stage schema/table are resolved from env vars (set by
-- medallion/gold.py from spec.stage.schema/table) rather than a fixed
-- dbt source(), since a spec's schema names aren't compile-time
-- constants - the SAME database/connection as raw and stage, but the
-- schema itself is spec-driven.
--
-- env_var() defaults (Phase 9a): dbt parses every model in this shared
-- project before executing any --select subset, so a gold_builds/*.yaml
-- run (which uses source()/sources.yml instead of these env vars) needs
-- a default here even though this model is never selected/executed in
-- that case.
select *
from "{{ env_var('DBT_STAGE_SCHEMA', 'unused') }}"."{{ env_var('DBT_STAGE_TABLE', 'unused') }}"
