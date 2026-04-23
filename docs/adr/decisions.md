# ADR-001: Data Vault 2.0 over Star Schema for Silver Layer

## Status
**Accepted** — 2026-01-15

## Context
The enterprise requires a data modeling methodology that provides full historical auditing, source-agnostic integration, and resilience to upstream schema changes across 5+ source systems.

## Decision
Use **Data Vault 2.0** as the Silver Layer modeling methodology, with Star Schema reserved for the Gold Layer (consumption).

## Rationale
- **Auditability**: DV2.0 provides immutable insert-only history with full lineage
- **Agility**: New source systems can be integrated by adding Links without redesigning existing models
- **Parallel loading**: Hub/Link/Satellite pattern enables parallel ETL execution
- **Compliance**: Complete point-in-time traceability for financial/healthcare audits

## Consequences
- **Positive**: Ultra-flexible, resilient to change, fully auditable
- **Negative**: Higher query complexity for analysts (mitigated by Gold Layer star schema)

---

# ADR-002: Dagster over Airflow for Orchestration

## Status
**Accepted** — 2026-03-01

## Context
The platform needs an orchestrator that natively understands data lineage and can automatically halt downstream assets when upstream anomalies are detected.

## Decision
Use **Dagster** (Software-Defined Assets) as the primary orchestrator. Retain Airflow DAGs as an alternative.

## Rationale
- **Data-aware**: Treats data assets as primary citizens, not arbitrary tasks
- **Native dbt support**: `dagster-dbt` integration provides full lineage visibility
- **Anomaly propagation**: Sensors can halt downstream materialization on upstream failures
- **Freshness policies**: Built-in SLA monitoring per asset

## Consequences
- **Positive**: Superior lineage, intelligent failure handling
- **Negative**: Smaller community than Airflow; some team retraining required

---

# ADR-003: Semantic Views for AI Readiness

## Status
**Accepted** — 2026-04-01

## Context
Multiple BI tools define conflicting metrics (e.g., "revenue" includes/excludes tax/discounts). Cortex AI requires machine-readable metric context to prevent hallucination.

## Decision
Implement **Snowflake Semantic Views** in the Gold Layer with centralized KPI definitions.

## Rationale
- **Single source of truth**: Metrics defined once, consumed everywhere
- **AI guardrail**: Cortex AI reads semantic annotations instead of inferring from column names
- **BI consistency**: All dashboards use the same metric definitions

## Consequences
- **Positive**: Eliminates metric conflicts; enables reliable Cortex AI integration
- **Negative**: Requires Semantic View maintenance as KPIs evolve

---

# ADR-004: Multi-Cloud Failover with Co-located Objects

## Status
**Accepted** — 2026-04-10

## Context
Cross-cloud replication fails if dependent objects (masking policies, roles) are in separate failover groups — causing "dangling reference" errors.

## Decision
**Co-locate ALL dependent objects** (databases + roles + policies + warehouses + integrations) within a single failover group.

## Rationale
- Prevents dangling reference failures during replication
- Ensures security posture is identical across all regions
- Simplifies failover operations (single group to promote)

## Consequences
- **Positive**: Reliable replication; consistent security posture globally
- **Negative**: Larger failover group = more data to replicate (managed via tiered frequency)
