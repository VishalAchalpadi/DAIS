{#
  Generic SCD Type 2 versioning for an incremental gold model.

  Call this from inside a `with ... {{ dais_scd2(...) }}` chain, after
  defining a CTE that computes the CURRENT snapshot of one row per
  business key (the normal, non-historized transformation logic - e.g.
  an aggregation over stage). This macro handles the versioning: on the
  first run, everything lands as version 1/active; on later runs, a
  business key whose tracked columns changed gets a new version row
  (active) while its previous version is expired in place (its
  expiry_datetime/active_flag get updated, not deleted) - unchanged
  keys are left untouched.

  Output columns, in order: <business key columns>, <tracked columns>,
  version, effective_datetime, expiry_datetime, active_flag, scd_pk.

  scd_pk is a surrogate key (md5 hash of the business key + version) -
  it's what makes each (key, version) row unique, since the business
  key alone repeats across versions. It's also this model's required
  `unique_key` config, since dbt's incremental "delete+insert" strategy
  uses it to know which existing row to replace when a version's
  expiry_datetime/active_flag are updated in place.

  Usage - the macro's own CTEs (on incremental runs) continue the same
  `with` clause the caller started for source_cte, joined by a comma,
  not a second `with` keyword:
    {{ config(materialized='incremental', unique_key='scd_pk') }}

    with current_snapshot as (
        select ticker, avg(volume) as avg_volume from ... group by ticker
    )

    {{ dais_scd2('current_snapshot', ['ticker'], ['avg_volume']) }}

  Args:
    source_cte: name of the CTE (already defined above this macro call)
      producing the current snapshot - one row per unique_key.
    unique_key: list of column names forming the business/natural key.
    tracked_columns: list of column names whose changes trigger a new
      version. Must be non-empty and disjoint from unique_key.
#}
{% macro dais_scd2(source_cte, unique_key, tracked_columns) %}

{% set surrogate_expr %}
    md5(
        {% for k in unique_key %}coalesce(cast(__ALIAS__.{{ k }} as text), ''){% if not loop.last %} || '|' || {% endif %}{% endfor %}
        || '|' || cast(__VERSION__ as text)
    )
{% endset %}

{% if not is_incremental() %}

select
    {% for k in unique_key %}sd.{{ k }},
    {% endfor -%}
    {% for c in tracked_columns %}sd.{{ c }},
    {% endfor -%}
    1 as version,
    current_timestamp as effective_datetime,
    cast(null as timestamp) as expiry_datetime,
    true as active_flag,
    {{ surrogate_expr | replace('__ALIAS__', 'sd') | replace('__VERSION__', '1') }} as scd_pk
from {{ source_cte }} sd

{% else %}

, current_active as (

    select *
    from {{ this }}
    where active_flag = true

),

changed_or_new as (

    select sd.*
    from {{ source_cte }} sd
    left join current_active ca
        on {% for k in unique_key %}sd.{{ k }} = ca.{{ k }}{% if not loop.last %} and {% endif %}{% endfor %}
    where ca.{{ unique_key[0] }} is null
       or (
            {% for c in tracked_columns %}sd.{{ c }} is distinct from ca.{{ c }}{% if not loop.last %} or {% endif %}{% endfor %}
          )

),

expired as (

    select ca.*, current_timestamp as _expire_at
    from current_active ca
    inner join changed_or_new co
        on {% for k in unique_key %}ca.{{ k }} = co.{{ k }}{% if not loop.last %} and {% endif %}{% endfor %}

),

prior_versions as (

    select {{ unique_key | join(', ') }}, max(version) as max_version
    from {{ this }}
    group by {{ unique_key | join(', ') }}

)

select
    {% for k in unique_key %}co.{{ k }},
    {% endfor -%}
    {% for c in tracked_columns %}co.{{ c }},
    {% endfor -%}
    coalesce(pv.max_version, 0) + 1 as version,
    current_timestamp as effective_datetime,
    cast(null as timestamp) as expiry_datetime,
    true as active_flag,
    {{ surrogate_expr | replace('__ALIAS__', 'co') | replace('__VERSION__', "coalesce(pv.max_version, 0) + 1") }} as scd_pk
from changed_or_new co
left join prior_versions pv
    on {% for k in unique_key %}co.{{ k }} = pv.{{ k }}{% if not loop.last %} and {% endif %}{% endfor %}

union all

select
    {% for k in unique_key %}{{ k }},
    {% endfor -%}
    {% for c in tracked_columns %}{{ c }},
    {% endfor -%}
    version,
    effective_datetime,
    _expire_at as expiry_datetime,
    false as active_flag,
    scd_pk
from expired

{% endif %}

{% endmacro %}
