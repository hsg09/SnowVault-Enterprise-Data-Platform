-- =============================================================================
-- 00_rbac_setup.sql — Role-Based Access Control
-- Data Vault 2.0 Snowflake Data Platform
--
-- PURPOSE: Establish a hierarchical RBAC model with least-privilege access.
--          Roles are designed around workload patterns (load, transform, query).
--
-- EXECUTION ORDER: Run FIRST, before any other bootstrap script.
-- REQUIRES: ACCOUNTADMIN or SECURITYADMIN privileges.
-- =============================================================================

USE ROLE ACCOUNTADMIN;

-- =============================================================================
-- 1. FUNCTIONAL ROLES (Workload-Specific)
-- =============================================================================

-- LOADER: Service account for ingestion pipelines (Snowpipe, external stages)
CREATE ROLE IF NOT EXISTS LOADER
    COMMENT = 'Ingestion service — read stages, write to raw landing tables';

-- TRANSFORMER: dbt service account for ELT transformations
CREATE ROLE IF NOT EXISTS TRANSFORMER
    COMMENT = 'dbt transformation service — read raw, write vault/analytics';

-- ANALYST: BI tool / analyst read access to gold layer
CREATE ROLE IF NOT EXISTS ANALYST
    COMMENT = 'Read-only access to gold analytics layer';

-- DATA_ENGINEER: Human role for development and debugging
CREATE ROLE IF NOT EXISTS DATA_ENGINEER
    COMMENT = 'Development role — read/write across all layers, manage pipelines';

-- DATA_STEWARD: Governance role for data classification and masking
CREATE ROLE IF NOT EXISTS DATA_STEWARD
    COMMENT = 'Governance role — manage tags, masking policies, access reviews';

-- PLATFORM_ADMIN: Infrastructure management (Terraform service account)
CREATE ROLE IF NOT EXISTS PLATFORM_ADMIN
    COMMENT = 'Infrastructure role — manage warehouses, databases, integrations';

-- =============================================================================
-- 2. ROLE HIERARCHY
-- =============================================================================
-- ACCOUNTADMIN
--   └── PLATFORM_ADMIN
--         ├── DATA_ENGINEER
--         │     ├── TRANSFORMER
--         │     │     └── LOADER
--         │     └── DATA_STEWARD
--         └── ANALYST
-- =============================================================================

-- Build hierarchy bottom-up
GRANT ROLE LOADER       TO ROLE TRANSFORMER;
GRANT ROLE TRANSFORMER  TO ROLE DATA_ENGINEER;
GRANT ROLE DATA_STEWARD TO ROLE DATA_ENGINEER;
GRANT ROLE DATA_ENGINEER TO ROLE PLATFORM_ADMIN;
GRANT ROLE ANALYST      TO ROLE PLATFORM_ADMIN;
GRANT ROLE PLATFORM_ADMIN TO ROLE ACCOUNTADMIN;

-- =============================================================================
-- 3. VIRTUAL WAREHOUSES (Workload-Isolated)
-- =============================================================================

-- Ingestion warehouse (auto-suspend aggressive for cost control)
CREATE WAREHOUSE IF NOT EXISTS LOADER_WH
    WITH
        WAREHOUSE_SIZE      = 'XSMALL'
        AUTO_SUSPEND        = 60        -- 1 minute idle
        AUTO_RESUME         = TRUE
        MIN_CLUSTER_COUNT   = 1
        MAX_CLUSTER_COUNT   = 2
        SCALING_POLICY      = 'STANDARD'
        INITIALLY_SUSPENDED = TRUE
        COMMENT             = 'Ingestion workloads — Snowpipe, COPY INTO, staging';

-- Transformation warehouse (sized for dbt batch runs)
CREATE WAREHOUSE IF NOT EXISTS TRANSFORMER_WH
    WITH
        WAREHOUSE_SIZE      = 'SMALL'
        AUTO_SUSPEND        = 120       -- 2 minutes idle
        AUTO_RESUME         = TRUE
        MIN_CLUSTER_COUNT   = 1
        MAX_CLUSTER_COUNT   = 3
        SCALING_POLICY      = 'STANDARD'
        INITIALLY_SUSPENDED = TRUE
        COMMENT             = 'dbt transformation workloads — batch ELT';

-- CI warehouse (small, ephemeral — for PR validation)
CREATE WAREHOUSE IF NOT EXISTS CI_WH
    WITH
        WAREHOUSE_SIZE      = 'XSMALL'
        AUTO_SUSPEND        = 60
        AUTO_RESUME         = TRUE
        MIN_CLUSTER_COUNT   = 1
        MAX_CLUSTER_COUNT   = 1
        INITIALLY_SUSPENDED = TRUE
        COMMENT             = 'CI/CD pipeline — PR validation builds';

-- Development warehouse (per-developer, small)
CREATE WAREHOUSE IF NOT EXISTS DEV_WH
    WITH
        WAREHOUSE_SIZE      = 'XSMALL'
        AUTO_SUSPEND        = 60
        AUTO_RESUME         = TRUE
        MIN_CLUSTER_COUNT   = 1
        MAX_CLUSTER_COUNT   = 1
        INITIALLY_SUSPENDED = TRUE
        COMMENT             = 'Developer workstation — ad-hoc queries, dbt dev runs';

-- Analytics warehouse (serving BI queries)
CREATE WAREHOUSE IF NOT EXISTS ANALYTICS_WH
    WITH
        WAREHOUSE_SIZE      = 'SMALL'
        AUTO_SUSPEND        = 300       -- 5 minutes idle (BI session reuse)
        AUTO_RESUME         = TRUE
        MIN_CLUSTER_COUNT   = 1
        MAX_CLUSTER_COUNT   = 4
        SCALING_POLICY      = 'ECONOMY'
        INITIALLY_SUSPENDED = TRUE
        COMMENT             = 'BI / reporting queries — Tableau, Looker, Power BI';

-- =============================================================================
-- 4. WAREHOUSE GRANTS
-- =============================================================================

GRANT USAGE ON WAREHOUSE LOADER_WH      TO ROLE LOADER;
GRANT USAGE ON WAREHOUSE TRANSFORMER_WH TO ROLE TRANSFORMER;
GRANT USAGE ON WAREHOUSE CI_WH          TO ROLE DATA_ENGINEER;
GRANT USAGE ON WAREHOUSE DEV_WH         TO ROLE DATA_ENGINEER;
GRANT USAGE ON WAREHOUSE ANALYTICS_WH   TO ROLE ANALYST;
GRANT USAGE ON WAREHOUSE DEV_WH         TO ROLE DATA_STEWARD;

-- PLATFORM_ADMIN can manage all warehouses
GRANT ALL PRIVILEGES ON WAREHOUSE LOADER_WH      TO ROLE PLATFORM_ADMIN;
GRANT ALL PRIVILEGES ON WAREHOUSE TRANSFORMER_WH TO ROLE PLATFORM_ADMIN;
GRANT ALL PRIVILEGES ON WAREHOUSE CI_WH          TO ROLE PLATFORM_ADMIN;
GRANT ALL PRIVILEGES ON WAREHOUSE DEV_WH         TO ROLE PLATFORM_ADMIN;
GRANT ALL PRIVILEGES ON WAREHOUSE ANALYTICS_WH   TO ROLE PLATFORM_ADMIN;

-- =============================================================================
-- 5. SERVICE USERS
-- =============================================================================

-- dbt service account (key-pair auth only, no password)
CREATE USER IF NOT EXISTS SVC_DBT_TRANSFORMER
    DEFAULT_ROLE       = TRANSFORMER
    DEFAULT_WAREHOUSE  = TRANSFORMER_WH
    MUST_CHANGE_PASSWORD = FALSE
    COMMENT            = 'dbt transformation service account';

GRANT ROLE TRANSFORMER TO USER SVC_DBT_TRANSFORMER;

-- Airflow service account
CREATE USER IF NOT EXISTS SVC_AIRFLOW
    DEFAULT_ROLE       = DATA_ENGINEER
    DEFAULT_WAREHOUSE  = TRANSFORMER_WH
    MUST_CHANGE_PASSWORD = FALSE
    COMMENT            = 'Apache Airflow orchestration service account';

GRANT ROLE DATA_ENGINEER TO USER SVC_AIRFLOW;

-- Snowpipe service account
CREATE USER IF NOT EXISTS SVC_SNOWPIPE
    DEFAULT_ROLE       = LOADER
    DEFAULT_WAREHOUSE  = LOADER_WH
    MUST_CHANGE_PASSWORD = FALSE
    COMMENT            = 'Snowpipe ingestion service account';

GRANT ROLE LOADER TO USER SVC_SNOWPIPE;

-- Terraform service account
CREATE USER IF NOT EXISTS SVC_TERRAFORM
    DEFAULT_ROLE       = PLATFORM_ADMIN
    DEFAULT_WAREHOUSE  = DEV_WH
    MUST_CHANGE_PASSWORD = FALSE
    COMMENT            = 'Terraform IaC service account';

GRANT ROLE PLATFORM_ADMIN TO USER SVC_TERRAFORM;

-- =============================================================================
-- 6. VERIFIED
-- =============================================================================
-- Run: SHOW GRANTS TO ROLE <role_name>;
-- Run: SHOW ROLES;
-- Run: SHOW WAREHOUSES;
