# Two Roads to Gold — Legacy (Control-M) vs. Dagster Orchestration

Dagster didn't replace anything — it sits beside Control-M as a second way
to *trigger* the same pipeline code. Where the two paths genuinely diverge
is gold, and that divergence has a real, currently-open gap in it.

**Core rule:** raw and stage always run through the exact same code —
`dais.pipeline.run_pipeline()` — no matter which trigger fired it.
Dagster's ingest assets don't reimplement ingestion; they call the same
HTTP API Control-M's shell wrapper calls. Gold is where the two paths
actually differ, both in mechanism and in what lineage you get.

---

## Fig. 1 — Raw → stage: one execution path, two front doors

Control-M and Dagster are both just callers. Whichever one fires, the run
lands in the same registry, writes the same `process_monitor` rows, and
emits the same OpenLineage events.

```mermaid
flowchart LR
    CM["Control-M / CLI\ndais run --spec ... --file ..."]
    DG["Dagster\nmaterialize sales_ingest/stage"]
    API["DAIS HTTP API\napi/app.py"]
    PIPE["run_pipeline()\nraw -> quality/DQ -> stage"]
    MON[("control.process_monitor\nsingle source of truth")]
    LIN["OpenLineage events\nlineage/emitter.py"]

    CM -->|direct call| PIPE
    DG -->|"POST /pipelines/{name}/run"| API
    API --> PIPE
    PIPE --> MON
    PIPE --> LIN

    style CM fill:#f6ece0,stroke:#a8672a,color:#1c1e24
    style DG fill:#ecebf9,stroke:#4a44b8,color:#1c1e24
    style API fill:#ecebf9,stroke:#4a44b8,color:#1c1e24
    style PIPE fill:#e6f2f1,stroke:#0e7c7b,color:#1c1e24
    style MON fill:#e6f2f1,stroke:#0e7c7b,color:#1c1e24
    style LIN fill:#e6f2f1,stroke:#0e7c7b,color:#1c1e24
```

- 🟧 Control-M / legacy trigger
- 🟪 Dagster trigger
- 🟩 shared code — identical either way

---

## Fig. 2 — Gold: two mechanisms, and a lineage gap inside one of them

An ingest pipeline picks exactly one gold mechanism. The legacy inline
block only ever aggregates its own stage table; a `gold_builds` spec can
join several. But `gold_builds` itself behaves differently depending on
*how* it's run.

```mermaid
flowchart TB
    STAGE["stage: typed, validated data"]
    CHOICE{"Which gold\nmechanism?"}

    INLINE["inline gold: block\n(asset_ingest, price_ingest,\nholdings_ingest)"]
    RUNGOLD["run_gold()\nenv_var-driven dbt model,\none pipeline's stage table only"]
    OL1["dbt-ol -> real OpenLineage event"]

    BUILDS["gold_builds/*.yaml\n(portfolio_summary_gold,\nregional_sales_gold)"]
    HOW{"Triggered how?"}
    CLIRUN["CLI / script\nrun_gold_build()"]
    OL2["dbt-ol -> real OpenLineage event"]
    DAGRUN["Dagster @dbt_assets\nDbtCliResource.cli()"]
    NOOL["Dagster's own asset lineage only\nNO OpenLineage event emitted"]

    STAGE --> CHOICE
    CHOICE -->|"single pipeline"| INLINE --> RUNGOLD --> OL1
    CHOICE -->|"spans pipelines"| BUILDS --> HOW
    HOW --> CLIRUN --> OL2
    HOW --> DAGRUN --> NOOL

    style STAGE fill:#e6f2f1,stroke:#0e7c7b,color:#1c1e24
    style INLINE fill:#f6ece0,stroke:#a8672a,color:#1c1e24
    style RUNGOLD fill:#f6ece0,stroke:#a8672a,color:#1c1e24
    style OL1 fill:#e6f2f1,stroke:#0e7c7b,color:#1c1e24
    style BUILDS fill:#ecebf9,stroke:#4a44b8,color:#1c1e24
    style CLIRUN fill:#ecebf9,stroke:#4a44b8,color:#1c1e24
    style OL2 fill:#e6f2f1,stroke:#0e7c7b,color:#1c1e24
    style DAGRUN fill:#ecebf9,stroke:#4a44b8,color:#1c1e24
    style NOOL fill:#f8e9e9,stroke:#a83b3b,color:#1c1e24
```

- 🟧 legacy inline gold
- 🟪 gold_builds / Dagster
- 🟩 real OpenLineage emitted
- 🟥 gap — no OpenLineage emitted

> **Open gap:** Materializing `regional_sales_gold` in Dagster runs the
> identical dbt models as running it from the CLI — but Dagster's path
> calls `DbtCliResource.cli()` directly instead of `run_gold_build()`, so
> it never goes through `dbt-ol`. Same data, same SQL, two different
> lineage outcomes depending on which button you pressed. **Not yet
> fixed** as of Phase 9c.

---

## Fig. 3 — Side by side

| | **Legacy** — inline `gold:` | **Current** — `gold_builds/*.yaml` |
|---|---|---|
| Scope | One pipeline's own stage table only | Any number of pipelines' stage tables (`depends_on`) |
| dbt wiring | `env_var('DBT_STAGE_SCHEMA')` interpolation | Static `source()` against `sources.yml` |
| Triggered from | `execution.stop_after: gold` on the pipeline spec | CLI script, or a dedicated Dagster job/asset |
| Runs in Dagster? | Not modeled as a Dagster asset today | Yes — `@dbt_assets`, auto-discovered from the yaml |
| OpenLineage | Always, via `dbt-ol` (Phase 9b) | Only when run via CLI/script — not yet when run via Dagster |

---

*Reflects `src/dais/orchestration/dagster/` and `src/dais/medallion/gold.py`
as of Phase 9c. The Fig. 2 gap is a real, open item — not yet fixed.*
