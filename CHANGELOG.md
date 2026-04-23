# CHANGELOG.md

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] — 2026-04-22

### Added

#### Phase 1: MVP Foundation
- **Project Scaffolding**: dbt_project.yml, profiles.yml (4 targets), packages.yml, pre-commit config
- **Snowflake Bootstrap**: 15 SQL scripts (00-14) — RBAC, databases, file formats, stages, streams, tasks, masking, replication, resource monitors, network policies, multi-cloud ingestion, semantic views, row access policies, failover runbooks
- **Bronze Layer**: 4 staging models, 3 hubs, 2 links, 6 satellites, 1 effectivity satellite
- **Silver Layer**: 2 business vault models (RFM classification, order lifecycle), 2 PIT tables, 1 bridge table, 2 conformed models
- **Gold Layer**: 2 facts, 3 dimensions, 2 aggregates, 1 secure view
- **Macros**: 7 Data Vault macros, 3 ingestion macros, 4 governance macros, 4 observability macros, 3 environment macros, 3 utility macros
- **Tests**: 3 generic tests (hub integrity, link integrity, satellite SCD2), 4 layer-specific tests
- **Seeds**: 4 reference data files (country codes, order status, payment methods, product categories)
- **Snapshots**: 1 SCD Type 2 snapshot (customer details)
- **Analyses**: Vault health check, pipeline SLA monitor

#### Phase 2: Orchestration & Ingestion
- **Dagster**: 12 files — definitions, resources, jobs, schedules, sensors, 5 pipeline assets
- **Airflow**: 5 DAGs — ELT, CDC streaming, data quality, replication monitor, maintenance
- **CI/CD**: 3 GitHub Actions workflows — dbt CI/CD (Slim CI), Terraform CI/CD, weekly DQ report

#### Phase 3: Infrastructure & Governance
- **Terraform**: 7 modules (databases, warehouses, RBAC, storage integrations, replication, network policies, resource monitors) + dev environment
- **Semantic Views**: 3 Snowflake Semantic Views (Customer 360, Revenue Analytics, Product Performance)
- **Row Access Policies**: Country-based GDPR filtering, sensitivity-based access control
- **Data Classification**: Automated SYSTEM$CLASSIFY with custom regex classifiers
- **Enhanced Failover**: 3 continuity strategies, tiered replication, Client Redirect

#### Documentation
- Architecture Guide, Data Vault 2.0 Guide, Multi-Cloud Strategy, Security & Governance, CI/CD Guide, Onboarding Guide, ADR Log, Contributing Guide, DR Runbook
