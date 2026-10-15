# DAIS — Enterprise Technical Specification

**Status:** Draft for review | **Date:** 2026-10-14 | **Audience:** Enterprise architecture review, platform engineering, data engineering leads, security/compliance, SRE/platform operations

This document specifies DAIS (config-driven medallion data pipeline framework) as a set of **functional and non-functional requirements for enterprise production deployment** — not a description of the reference implementation as it stands today. Where the current implementation already meets a requirement, that is noted as evidence of a proven approach. Where it does not, that is called out explicitly as a gap to close before, or shortly after, go-live. Section 5 consolidates every gap into a single prioritized roadmap.

---

## 1. Purpose & Scope

DAIS exists to replace bespoke, per-feed ETL scripts with a single, config-driven engine: onboarding a new data feed becomes a declarative spec change, not a new application. This specification defines what that engine must do (functional requirements) and how it must behave as a piece of enterprise infrastructure — secure, observable, recoverable, and operable by a team that didn't build it (non-functional requirements) — in an organization with regulatory, audit, and change-control obligations (e.g., a financial-services data platform).

**In scope:** ingestion, data quality, exception handling, orchestration, transformation, lineage/cataloging, monitoring, and the cross-cutting security/reliability/observability/compliance requirements that apply to all of the above.

**Out of scope:** the specific business logic of any one data feed; choice of specific enterprise vendor products (SSO provider, logging platform, etc.) beyond stating the integration requirement.

---

## 2. Solution Architecture Overview

**Processing model:** every pipeline runs the same generic engine through up to four stages, driven entirely by a per-feed YAML spec: **control gate → raw (bronze) → stage (silver) → gold (curated)**. The engine never branches on *which* pipeline is running — only on what its spec declares (format, validation rules, write strategy, versioning). This is the foundational design decision the rest of this specification builds on: it is what makes "add a feed" a configuration exercise rather than a software project.

**Core components:**

| Component | Role |
|---|---|
| Spec layer | Versioned, schema-validated YAML per feed; the single source of truth for a pipeline's behavior |
| Trigger API | Orchestrator-agnostic HTTP surface (submit a run, poll status) — the stable contract an enterprise scheduler or any orchestrator calls |
| Orchestrator | DAG-based scheduling/dependency engine, with event-driven (file arrival) and schedule-driven triggering |
| Data quality engine | Declarative, per-field validation with configurable failure semantics (whole-file / row / group) |
| Exception management UI | Operator-facing review-and-correct workflow for quarantined data |
| Transformation layer | SQL-based, version-controlled business logic for the curated layer |
| Lineage & catalog | Open-format lineage events feeding a searchable catalog with ownership, glossary, and column-level detail |
| Monitoring & audit | Shared, queryable record of every processing step across every pipeline |
| Secrets management | Centralized credential resolution, never embedded in config |

**Trigger paths (by design, all converging on the same execution path):** an enterprise batch scheduler (Control-M–class), a DAG orchestrator (manual, scheduled, or event-driven), a direct API call, and a CLI for ad hoc/local operation. Whichever path initiates a run, the same control-gate → raw → stage → gold execution, the same audit trail, and the same lineage emission occur — there is exactly one execution path, with multiple front doors.

**Data flow (textual):** a file lands in a source location → a control gate validates the file as a whole (size/row-count bounds) → raw lands the file's data untyped with a full audit trail (checksum, source metadata) → stage promotes it to typed, validated data via the data quality engine, routing failures to the exception/quarantine store → gold applies curated-layer business transformations, optionally joining other pipelines' or shared reference data → lineage events are emitted at every step and consolidated in the catalog → every step's outcome is written to the shared monitoring record.

---

## 1. Functional Requirements

### 1.1 Pipeline Specification & Spec Validator

| ID | Requirement |
|---|---|
| FR-SPEC-01 | Onboarding a new data feed shall require only a declarative spec (no application code change) for any already-supported format/platform combination. |
| FR-SPEC-02 | The spec shall be validated against a versioned, strongly-typed schema at load time — before any data movement — rejecting unrecognized fields, placeholder values, and invalid field combinations with a specific, actionable error. |
| FR-SPEC-03 | The current schema shall be machine-introspectable via an API endpoint, so editor tooling, CI validation, and documentation stay in sync with the engine automatically as the schema evolves. |
| FR-SPEC-04 | A browser-based spec authoring/validation tool shall let a data steward (not necessarily an engineer) draft and validate a spec with immediate feedback, before it is committed. |
| FR-SPEC-05 *(enterprise)* | Spec changes shall flow through version control with mandatory peer review; promotion to production shall require data-governance sign-off, not a direct file edit on a running system. |
| FR-SPEC-06 *(enterprise)* | A CI gate shall validate every spec touched by a change (schema validation plus a dry run against a representative sample file) before merge. |

**Current state:** FR-SPEC-01 through 04 are implemented — strict schema validation at load time (extra fields rejected, not ignored), a schema-introspection endpoint, and a browser-based spec editor all exist. FR-SPEC-05/06 (governance workflow, CI gate) do not exist yet; specs today are validated only at the moment a pipeline using them is loaded, not at commit time.

### 1.2 Connector Framework

| ID | Requirement |
|---|---|
| FR-CONN-01 | The platform shall support ingestion from local filesystem, cloud object storage, and enterprise managed file transfer (SFTP or equivalent). |
| FR-CONN-02 | Database/warehouse connectivity shall be abstracted behind a common interface so a new platform (additional cloud data warehouse, etc.) can be added without modifying the core engine. |
| FR-CONN-03 | Every connector shall retry transient failures automatically (configurable backoff strategy and attempt count) and fail fast on non-transient errors. |
| FR-CONN-04 | Every connector's credentials shall be resolved exclusively through the enterprise secrets manager at call time — never read from a spec file or any version-controlled configuration. |

**Current state:** the connector interface is properly abstracted and one database platform is fully implemented and tested end-to-end (object storage ingestion/quarantine included); retry with configurable exponential/fixed backoff and jitter is implemented, scoped to genuinely transient errors. **Gap:** a second warehouse platform is defined at the interface level but not implemented; SFTP/managed file transfer is accepted by the schema but not implemented (fails with an explicit, honest error rather than silently no-op'ing).

### 1.3 Data Ingestion, Parsing & Control Gates

| ID | Requirement |
|---|---|
| FR-ING-01 | The platform shall parse the common enterprise feed formats: delimited/CSV, fixed-width, JSON, and XML, including large fixed-width files via parallelized chunked parsing. |
| FR-ING-02 | A whole-file control gate (size and row-count bounds) shall quarantine a truncated, duplicated, or grossly malformed file *before* any of its data is landed. |
| FR-ING-03 | The platform shall support feeds that deliver more than one file per cycle: pattern-based file discovery, deterministic ordering, and a configurable policy for handling multiple matching files. |
| FR-ING-04 | Arrival of a new file in a watched location shall automatically start processing within a defined latency target, without requiring a scheduler or operator to initiate it. |
| FR-ING-05 | The platform shall alert once, proactively, when an expected file fails to arrive by a defined deadline. |

**Current state:** all formats, the whole-file control gate, multi-file discovery/ordering, file-watch-triggered automatic processing, and missed-arrival alerting are implemented. **Note:** today's event-driven triggering is poll-based (configurable interval); it does not yet integrate with native cloud event notifications (e.g., object-store event → queue → trigger), which would reduce both latency and polling overhead at scale — see Section 5.

### 1.4 Data Quality Engine

| ID | Requirement |
|---|---|
| FR-DQ-01 | Validation rules shall be declarative and per-field: null/empty checks, type and format validation, parameterized numeric comparisons, and live reference-data lookups (not hardcoded lists). |
| FR-DQ-02 | The "blast radius" of a failing record shall be configurable per feed: an entire file, only the failing rows, or an entire logical group containing the failing row (e.g., one portfolio's full set of holdings) — because the correct behavior genuinely differs by use case. |
| FR-DQ-03 | Every validation run shall produce a human-readable, browsable report, viewable by a data steward without direct database access. |
| FR-DQ-04 | Duplicate records on a configured business key shall be detected and handled according to a configurable write strategy, never causing a silent data loss or an unhandled database constraint failure. |
| FR-DQ-05 *(enterprise)* | Data-quality reports, since they can surface sample failing (and potentially sensitive) data, shall be served behind the same authentication/authorization boundary as every other platform surface. |

**Current state:** implemented on an industry-standard data quality framework (not custom pass/fail scripting), with all three failure-blast-radius modes and live reference-data validation. **Gap (FR-DQ-05):** the validation report UI is currently served as an unauthenticated static site — a confidentiality gap, since reports can include sample failing row content.

### 1.5 Exception / Quarantine Management

| ID | Requirement |
|---|---|
| FR-EXC-01 | Every row or file failing validation shall be captured with its specific failure reason(s) — never silently dropped and never a hard pipeline failure for a correctable data issue. |
| FR-EXC-02 | An exception-review UI shall let an authorized operator browse pending exceptions, view full context, correct the value, and resubmit. |
| FR-EXC-03 | A resubmitted correction shall be re-validated against the identical rule set as a first-time load; a correction that still fails shall be rejected in full, never partially written. |
| FR-EXC-04 | A successful correction shall automatically propagate to any dependent curated-layer output, so a fix doesn't leave downstream consumers silently stale. |
| FR-EXC-05 | Resolved exceptions shall be retained, not deleted, preserving a full history of what broke and how it was corrected. |
| FR-EXC-06 *(enterprise)* | Every resubmit action shall be attributed to an individually identifiable, authenticated user and written to an immutable audit record — not merely "an API call was made." |
| FR-EXC-07 *(enterprise)* | Access to resolve exceptions shall be scoped by role/ownership (a team can act on its own pipelines' exceptions; a platform admin can act on any) rather than one shared credential granting blanket access. |

**Current state:** FR-EXC-01 through 05 are fully implemented, including the downstream-refresh behavior and the resolved-record audit trail. **Gap (FR-EXC-06/07):** the current authentication model is a single shared API key with no per-user identity, so "who approved this correction" cannot currently be answered from the audit trail — this is the same root gap as NFR-SEC-01/03 below and should be closed once, not per-feature.

### 1.6 Orchestration, Scheduling & Triggering

| ID | Requirement |
|---|---|
| FR-ORC-01 | Pipelines shall be triggerable by an enterprise batch scheduler via a stable HTTP submit/poll contract, independent of whichever orchestration engine executes the run. |
| FR-ORC-02 | A DAG-based orchestrator shall provide visual dependency graphs, run history, and both manual and automated triggering as a modern alternative/complement to the legacy scheduler. |
| FR-ORC-03 | Event-driven triggering (new file arrival) and automatic downstream firing (a dependent transformation starting once its upstream data lands) shall both be supported without manual re-chaining. |
| FR-ORC-04 *(enterprise)* | Every orchestration surface with a UI or API shall require enterprise SSO authentication and enforce role-based authorization — no orchestration surface shall be reachable with no login. |

**Current state:** FR-ORC-01 through 03 are implemented — the trigger/poll API, the DAG orchestrator with both file-watch and automatic downstream firing, all converging on one execution path regardless of trigger source. **Gap (FR-ORC-04):** the open-source orchestrator in use has no built-in authentication or role-based access control; a commercial tier or a fronting authenticating reverse proxy is required before this surface is exposed in any shared environment.

### 1.7 Transformation (Gold / Curated Layer)

| ID | Requirement |
|---|---|
| FR-XFM-01 | Business-logic transformations shall be expressed in a standard, version-controlled SQL transformation tool, auditable and reviewable like application code — not embedded procedurally inside the orchestration engine. |
| FR-XFM-02 | The platform shall support point-in-time historization (slowly-changing-dimension style versioning) for curated tables where regulatory or analytical requirements require "what did this look like as of a prior date." |
| FR-XFM-03 | A transformation that must incorporate shared/reference data from a different domain (e.g., an FX rate lookup) shall declare that dependency explicitly, not couple to it implicitly. |
| FR-XFM-04 | A transformation spanning more than one source pipeline (a reconciliation, a cross-domain mart) shall be modeled as its own independently schedulable unit, not appended to one contributing pipeline's definition. |

**Current state:** fully implemented, including a reusable historization mechanism and an explicit multi-pipeline transformation construct, with real column-level lineage traced through multi-table joins (see 1.8).

### 1.8 Lineage, Cataloging & Metadata

| ID | Requirement |
|---|---|
| FR-LIN-01 | Every pipeline run shall emit standard, open-format lineage events at every processing step. |
| FR-LIN-02 | Lineage shall be visible through an enterprise data catalog with business glossary, ownership, and tagging — not only as raw event logs. |
| FR-LIN-03 | Column-level lineage (which source column, through what transformation, produced a given curated-layer column) shall be captured automatically on every run, with no manual sync step required, to support impact analysis and regulatory data-tracing requests. |
| FR-LIN-04 | Catalog writes shall be idempotent and additive — a routine run shall never silently remove previously captured metadata (e.g., a manually applied glossary tag). |

**Current state:** implemented using an open lineage event standard feeding a searchable catalog with glossary/ownership support; automatic column-level lineage is implemented end to end, including through multi-table joins and versioned (historized) transformations — this was hardened during the current implementation cycle specifically to resolve cases where lineage silently stopped at an internal transformation boundary instead of reaching the true source column.

### 1.9 Monitoring, Alerting & SLA Tracking

| ID | Requirement |
|---|---|
| FR-MON-01 | Every processing step of every pipeline shall write to one shared, queryable record — "did pipeline X complete step Y today" shall be answerable without reading application logs. |
| FR-MON-02 | Alerting on quality failures, control-gate failures, missed arrivals, and anomalous volumes/values shall be configurable, with native connectors to the organization's incident/notification tooling. |
| FR-MON-03 | Related pipelines shall be groupable into a named business process with a completion deadline, queryable as a single status. |
| FR-MON-04 *(enterprise)* | A business-process SLA breach shall proactively notify the owning team, not merely become visible on the next status poll. |

**Current state:** FR-MON-01 through 03 are implemented. **Gap (FR-MON-04):** SLA breach detection today is pull-based (computed when queried); it does not yet push a proactive notification on breach. Alert channels today cover logging and generic webhook (sufficient to reach most incident platforms); a native email channel is stubbed but not implemented.

---

## 2. Non-Functional Requirements

### 2.1 Security — Authentication, Authorization & Secrets Management

> **This is the single highest-priority gap in the current implementation and should be treated as a go-live blocker, not a post-launch hardening item.**

| ID | Requirement |
|---|---|
| NFR-SEC-01 | Every platform surface (trigger API, orchestrator UI, data-quality reports, catalog UI, exception-review UI) shall require authenticated access; no surface shall be reachable anonymously. |
| NFR-SEC-02 | Authentication shall integrate with the enterprise identity provider (SSO / OIDC / SAML), not rely on a static shared secret. |
| NFR-SEC-03 | Authorization shall be role- and/or ownership-based: a team can act on its own pipelines; a platform administrator can act on all; a read-only/audit role can view without acting. |
| NFR-SEC-04 | Every credential shall be resolved at runtime from a centralized enterprise secrets manager — never stored in a spec file, a configuration file, source control, or application logs. |
| NFR-SEC-05 | Any local-development credential fallback shall be impossible to select in a non-development environment — fail closed by configuration, not merely discouraged by a warning. |
| NFR-SEC-06 | All inter-service network traffic shall be encrypted in transit in every non-local environment. |
| NFR-SEC-07 | Data at rest (warehouse, object storage landing/quarantine zones) shall use the organization's standard encryption-at-rest controls. |

**Current state:** a centralized secrets-manager integration is implemented and correctly used for all database credentials, with an explicit, loudly-logged, environment-variable-gated local-development fallback that cannot be selected from a spec file (good practice, but not yet a hard environment-level block). **Gap:** the trigger API, the orchestrator UI, and the data-quality report UI are each authenticated (if at all) by a single static shared key or not authenticated at all — there is no enterprise SSO integration, no per-user identity, and no role-based authorization anywhere in the platform today. Every requirement in Sections 1.5–1.6 that depends on "an authenticated, individually identifiable user" is blocked on closing this gap.

### 2.2 Reliability, Availability & Scalability

| ID | Requirement |
|---|---|
| NFR-REL-01 | Pipeline run state (status, deduplication history) shall be externalized and consistent across multiple replicas of the trigger API, not held only in one process's memory. |
| NFR-REL-02 | The trigger API shall scale horizontally without correctness loss. |
| NFR-REL-03 | Transient downstream failures shall retry automatically with backoff; non-transient (data/logic) errors shall fail fast. |
| NFR-REL-04 | Concurrency shall be configurably limited for pipelines whose correctness depends on strict processing order. |
| NFR-REL-05 | A recovery time objective (RTO) and recovery point objective (RPO) shall be defined and tested for each data tier (landing files, raw/stage/gold tables, orchestrator history, catalog metadata). |

**Current state:** the retry framework (NFR-REL-03) is implemented and well-scoped to genuinely transient errors. **Gap:** run state today lives only in the trigger API process's memory — it is explicitly single-replica and is lost on restart (a documented, known limitation, not an oversight); RTO/RPO targets have not yet been defined for any data tier.

### 2.3 Observability — Logging, Metrics, Tracing & Audit

| ID | Requirement |
|---|---|
| NFR-OBS-01 | All components shall emit structured logs correlated by a run/correlation ID traceable across every layer. |
| NFR-OBS-02 | Logs shall ship to the enterprise centralized logging platform. |
| NFR-OBS-03 | Key operational metrics (throughput, failure rate, quarantine volume, SLA attainment, queue depth) shall be exported to the enterprise metrics/dashboarding platform. |
| NFR-OBS-04 | Every data-mutating action shall produce an immutable, attributable audit record retained per the organization's audit-retention policy. |
| NFR-OBS-05 | Distributed tracing across the trigger-API → orchestrator → transformation chain shall be available for root-cause analysis. |

**Current state:** structured logging is implemented throughout. **Gap:** there is currently no integration with a centralized logging platform, no metrics export, and no distributed tracing; the closest thing to an audit trail today (the shared monitoring record and the resolved-exception history) is not yet attributable to an individual user (depends on closing Section 2.1 first).

### 2.4 Data Governance, Compliance & Retention

| ID | Requirement |
|---|---|
| NFR-GOV-01 | Every curated dataset shall have a documented owner, data classification, and retention policy. |
| NFR-GOV-02 | Column-level lineage shall be sufficient, on its own, to answer a regulatory data-tracing request without manual reconstruction. |
| NFR-GOV-03 | PII/sensitive fields shall be tagged in the catalog and subject to masking/restricted access in non-production environments and in data-quality reports. |
| NFR-GOV-04 | Retention and deletion of all platform-held records, including quarantine and resolved-exception history, shall comply with the organization's records-retention schedule. |

**Current state:** the catalog's glossary/tagging and the engine's automatic column-level lineage (Section 1.8) together meet most of the mechanical requirement for NFR-GOV-02. **Gap:** no PII classification or masking policy is implemented; quarantine and resolved-exception records are currently retained indefinitely with no policy-driven expiry.

### 2.5 Test Automation, CI/CD & Release Management

| ID | Requirement |
|---|---|
| NFR-QA-01 | The engine and every connector/validation-rule type shall have automated test coverage, including integration tests against a real database, run on every change. |
| NFR-QA-02 | A CI pipeline shall gate merges on full test-suite success and independently validate every committed spec. |
| NFR-QA-03 | Container images shall be built, vulnerability/license-scanned, and promoted through environment tiers via a controlled, auditable pipeline. |
| NFR-QA-04 | Infrastructure (container orchestration manifests, workflow-engine configuration, database provisioning) shall be defined as code and version-controlled. |
| NFR-QA-05 | A production deployment shall be preceded by a full automated test-suite run against a target-equivalent build. |

**Current state:** a substantial automated test suite already exists — unit and real-database integration tests, exercised locally on every change, including end-to-end runs with no mocked components where it matters most (data quality, lineage, connectors). **Gap:** none of this currently runs in a CI pipeline; there are no container images, no infrastructure-as-code, and no vulnerability scanning in the repository today. These are foundational build items for any real deployment, not refinements of a partial capability.

### 2.6 Maintainability & Extensibility

| ID | Requirement |
|---|---|
| NFR-MNT-01 | Adding a new feed using an already-supported format/platform shall require only a configuration change. |
| NFR-MNT-02 | Adding a new source/target platform shall require implementing one well-defined interface, not modifying the core engine. |
| NFR-MNT-03 | The spec schema shall remain self-documenting and machine-introspectable as it evolves. |
| NFR-MNT-04 | Where more than one mechanism exists for the same capability (e.g., two transformation-trigger mechanisms) for a deliberate reason, that reason and the mechanism each is intended for shall be explicitly documented, to prevent long-term confusion for teams who didn't make the original decision. |

**Current state:** strong alignment already — config-driven onboarding, an abstracted connector interface, and a self-documenting schema are all proven in production use. One deliberate architectural duality exists (an original, single-pipeline transformation trigger mechanism alongside a newer, multi-pipeline one) which is documented but should be called out explicitly to any new team as an intentional design choice, not an inconsistency to "fix."

---

## 3. Gap Analysis & Enterprise Readiness Roadmap

**Priority key:** **P1** — must close before production go-live; **P2** — should close shortly after go-live; **P3** — longer-term hardening.

| Area | Current State | Enterprise Requirement | Gap Size | Priority |
|---|---|---|---|---|
| Authentication & authorization | Single shared static key, no RBAC, no per-user identity | SSO integration + role-based access | Large | **P1** |
| Audit attribution | Shared-key actions, no individual identity | Per-user, immutable audit trail | Medium (depends on auth) | **P1** |
| Orchestrator access control | No built-in login/RBAC in current tier | SSO-fronted or commercial tier | Medium | **P1** |
| Data-quality report access | Unauthenticated static site | Behind the same auth boundary as everything else | Small–Medium | **P1** |
| CI/CD pipeline | None | Full build/test/scan/promote pipeline | Large | **P1** |
| Containerization & IaC | None | Container images + infrastructure as code | Large | **P1** |
| Run-state persistence / HA | In-memory, single replica | Externalized, multi-replica safe | Large | **P1** |
| Centralized logging | Structured, local only | Shipped to enterprise platform | Medium | **P1/P2** |
| Metrics & dashboards | None exported | Exported to enterprise platform | Medium | P2 |
| Distributed tracing | None | End-to-end trace correlation | Medium | P2/P3 |
| Secrets manager enforcement | Supported, dev fallback exists | Hard-blocked outside dev | Small | **P1** |
| RTO/RPO definition | Undefined | Defined and tested per data tier | Medium | P2 |
| SLA breach alerting | Pull-only | Proactive push/page | Small | P2 |
| Native email alert channel | Stubbed | Implemented | Small | P3 |
| Enterprise MFT / SFTP connector | Schema-ready, not implemented | Implemented | Medium | P2 |
| Additional warehouse connector | Interface-ready, not implemented | Implemented | Medium | P2 |
| PII classification & masking | Not implemented | Implemented | Medium | P2 |
| Data retention policy enforcement | Indefinite retention | Policy-driven expiry | Small–Medium | P2 |

---

## Appendix

### A. Glossary

- **Medallion architecture** — the bronze (raw) / silver (stage) / gold (curated) layering model this platform is built around.
- **Spec** — the declarative YAML configuration defining one data feed's entire behavior (source, parsing, validation, write strategy, transformation).
- **Control gate** — a whole-file pre-check (size/row-count bounds) run before any data is landed.
- **Quarantine / exception** — a row or file that failed validation and is held for review rather than dropped or hard-failing the run.
- **SCD Type 2** — a historization technique that versions every change to a record rather than overwriting it, preserving point-in-time history.
- **Lineage** — the record of which upstream data and transformation produced a given downstream dataset/column.
- **Business process** — a named SLA grouping of related pipelines, tracked as a single completion status.

### B. Document History

| Version | Date | Change |
|---|---|---|
| 1.0 | 2026-10-14 | Initial draft |
