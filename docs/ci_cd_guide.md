# CI/CD Guide — Continuous Integration & Deployment

## Executive Summary

This document outlines the Continuous Integration and Continuous Deployment (CI/CD) pipelines utilized to manage, test, and deploy both the Data Platform infrastructure (via Terraform) and the Data Models (via dbt). The pipeline emphasizes "Slim CI" principles to optimize compute costs and strict environment segregation (`dev` → `staging` → `prod`).

---

## Pipeline Overview

The following flow represents the lifecycle of a data model change from Pull Request to Production Deployment:

```mermaid
graph LR
    classDef git fill:#f9f9f9,stroke:#333,stroke-width:1px
    classDef test fill:#e1f5fe,stroke:#0288d1,stroke-width:2px
    classDef deploy fill:#e8f5e9,stroke:#2e7d32,stroke-width:2px
    
    PR["Pull Request"]:::git --> LINT["SQLFluff Lint"]:::test
    LINT --> CI["Slim CI Build<br>(state:modified)"]:::test
    CI --> TEST["dbt Test<br>(modified models)"]:::test
    TEST --> REVIEW["Peer Review"]:::git
    REVIEW --> MERGE["Merge to main"]:::git
    MERGE --> DEPLOY_B["Deploy Bronze"]:::deploy
    DEPLOY_B --> TEST_B["Test Bronze ✓"]:::test
    TEST_B --> DEPLOY_S["Deploy Silver"]:::deploy
    DEPLOY_S --> TEST_S["Test Silver ✓"]:::test
    TEST_S --> DEPLOY_G["Deploy Gold"]:::deploy
    DEPLOY_G --> TEST_G["Test Gold ✓"]:::test
    TEST_G --> DOCS["Generate Docs"]:::deploy
```

---

## 3 GitHub Actions Workflows

### 1. dbt CI/CD (`dbt_ci_cd.yml`)

**PR Trigger**: Automatically runs when changes are detected in `models/`, `macros/`, `tests/`, `seeds/`, or `snapshots/`.

| Stage | Action | Environment Target |
|---|---|---|
| **Lint** | SQLFluff with `--dialect snowflake` | N/A |
| **CI Build** | `dbt build --select state:modified+` | `ci_{run_id}` (ephemeral schema) |
| **Production**| Sequential Build: Bronze → Test → Silver → Test → Gold → Test | `prod` |

> [!TIP]
> **Slim CI Benefits**
> The platform strictly leverages deferral (`state:modified+`). We only build and test models explicitly changed in the PR, alongside their downstream dependents, comparing against the `main` branch manifest. This singular optimization reduces our ephemeral CI compute credit consumption by ~70%.

### 2. Terraform CI/CD (`terraform_ci_cd.yml`)

| Event | Pipeline Action |
|---|---|
| **Pull Request** | `terraform fmt -check` → `terraform validate` → `terraform plan` (Plan Output posted directly as a GitHub PR comment) |
| **Merge to Main**| `terraform apply -auto-approve` (Protected by manual release gate approval) |

> [!IMPORTANT]
> All infrastructure modifications must originate through Terraform. Manual DDL operations within the Snowflake console are prohibited in `prod`.

### 3. Data Quality Report (`data_quality_report.yml`)

- **Schedule**: Every Monday 08:00 UTC
- **Action**: Queries the `AUDIT.DQ_RESULTS` table, generates a markdown report, and auto-creates GitHub Issues for persistent severity failures.

---

## Environment Promotion

```
feature/* → develop → staging → main (production)
   └── CI schema (ephemeral)   └── staging schema   └── production schema
```

1. **Development**: Developer branches from `develop` and writes models.
2. **Integration**: PR triggers a Slim CI build inside an ephemeral `ci_{run_id}` schema.
3. **Staging**: After peer review, the branch is merged to `develop` → triggering a staging deployment.
4. **Production**: Validation on `develop` culminates in a PR to `main` → deploying to production with strict sequential test gates.

---

## Secrets Management

| Secret Key | Used By | Secure Storage Mechanism |
|---|---|---|
| `SNOWFLAKE_ACCOUNT` | dbt CI/CD, DQ Report | GitHub Secrets (`Dependabot` shielded) |
| `SNOWFLAKE_USER` | dbt CI/CD | GitHub Secrets |
| `SNOWFLAKE_PASSWORD` | dbt CI/CD | GitHub Secrets |
| `TF_SNOWFLAKE_USER` | Terraform | GitHub Secrets |
| `TF_SNOWFLAKE_PRIVATE_KEY_PATH`| Terraform | GitHub Secrets (RSA PEM format) |

> [!WARNING]
> **Zero Plain-Text Policy**
> Under no circumstances should database credentials or RSA Keys be hard-coded into `dbt_project.yml`, `profiles.yml`, or Terraform variables. Always dynamically read these values via environment variables during pipeline execution.
