-- =============================================================================
-- 01_databases_and_schemas.sql — Database & Schema Architecture
-- Data Vault 2.0 Snowflake Data Platform
--
-- PURPOSE: Create the multi-database layout aligned with Medallion + Data Vault.
--          Each database isolates a functional domain with granular schema access.
--
-- EXECUTION ORDER: Run AFTER 00_rbac_setup.sql
-- REQUIRES: PLATFORM_ADMIN or ACCOUNTADMIN
-- =============================================================================

USE ROLE PLATFORM_ADMIN;

-- =============================================================================
-- 1. RAW_VAULT DATABASE (Bronze — Data Vault 2.0 Raw Vault)
-- =============================================================================
CREATE DATABASE IF NOT EXISTS RAW_VAULT
    DATA_RETENTION_TIME_IN_DAYS = 7
    COMMENT = 'Bronze layer — Raw Vault (Data Vault 2.0). Hubs, Links, Satellites.';

USE DATABASE RAW_VAULT;

-- Landing schemas (per source system)
CREATE SCHEMA IF NOT EXISTS ECOMMERCE
    COMMENT = 'Raw landing — E-Commerce source system (batch files from S3)';
CREATE SCHEMA IF NOT EXISTS CRM
    COMMENT = 'Raw landing — CRM source system';
CREATE SCHEMA IF NOT EXISTS CDC
    COMMENT = 'Raw landing — CDC streams (Debezium / Kafka)';
CREATE SCHEMA IF NOT EXISTS STREAMING
    COMMENT = 'Raw landing — Snowpipe Streaming / real-time feeds';

-- Data Vault structural schemas
CREATE SCHEMA IF NOT EXISTS RAW_VAULT
    COMMENT = 'Raw Vault entities — Hubs, Links, Satellites (dbt-managed)';
CREATE SCHEMA IF NOT EXISTS STAGING
    COMMENT = 'Pre-vault staging — hash key generation, record source, metadata';

-- =============================================================================
-- 2. BUSINESS_VAULT DATABASE (Silver — Business Vault)
-- =============================================================================
CREATE DATABASE IF NOT EXISTS BUSINESS_VAULT
    DATA_RETENTION_TIME_IN_DAYS = 7
    COMMENT = 'Silver layer — Business Vault, PIT tables, Bridge tables, Conformed.';

USE DATABASE BUSINESS_VAULT;

CREATE SCHEMA IF NOT EXISTS BUSINESS_VAULT
    COMMENT = 'Business Vault — derived calculations, KPIs, classifications';
CREATE SCHEMA IF NOT EXISTS PIT
    COMMENT = 'Point-In-Time snapshot tables for performant satellite joins';
CREATE SCHEMA IF NOT EXISTS BRIDGE
    COMMENT = 'Bridge tables for walking multi-hop link paths';
CREATE SCHEMA IF NOT EXISTS CONFORMED
    COMMENT = 'Conformed / cleansed integration layer for Gold consumption';

-- =============================================================================
-- 3. ANALYTICS DATABASE (Gold — BI / Reporting)
-- =============================================================================
CREATE DATABASE IF NOT EXISTS ANALYTICS
    DATA_RETENTION_TIME_IN_DAYS = 14
    COMMENT = 'Gold layer — BI-ready facts, dimensions, aggregates, secure views.';

USE DATABASE ANALYTICS;

CREATE SCHEMA IF NOT EXISTS FACTS
    COMMENT = 'Fact tables — transactional grain metrics';
CREATE SCHEMA IF NOT EXISTS DIMENSIONS
    COMMENT = 'Dimension tables — SCD Type 2 entities';
CREATE SCHEMA IF NOT EXISTS AGGREGATES
    COMMENT = 'Pre-built analytical aggregations — revenue, LTV, cohorts';
CREATE SCHEMA IF NOT EXISTS SECURE_VIEWS
    COMMENT = 'Secure views with row/column masking for external consumers';
CREATE SCHEMA IF NOT EXISTS REFERENCE_DATA
    COMMENT = 'dbt seed reference data — country codes, calendars, mappings';

-- =============================================================================
-- 4. AUDIT DATABASE (Control Plane / Observability)
-- =============================================================================
CREATE DATABASE IF NOT EXISTS AUDIT
    DATA_RETENTION_TIME_IN_DAYS = 90
    COMMENT = 'Control plane — pipeline metadata, DQ results, lineage, cost tracking.';

USE DATABASE AUDIT;

CREATE SCHEMA IF NOT EXISTS CONTROL
    COMMENT = 'Pipeline orchestration metadata — ingestion logs, task status';
CREATE SCHEMA IF NOT EXISTS LINEAGE
    COMMENT = 'Data lineage tracking from ACCESS_HISTORY and dbt metadata';
CREATE SCHEMA IF NOT EXISTS DQ_RESULTS
    COMMENT = 'Data quality test results — dbt test failures, anomaly detection';
CREATE SCHEMA IF NOT EXISTS COST_TRACKING
    COMMENT = 'Credit usage attribution, warehouse utilization metrics';
CREATE SCHEMA IF NOT EXISTS SNAPSHOTS
    COMMENT = 'dbt snapshot SCD Type 2 history tables';

-- =============================================================================
-- 5. DATABASE GRANTS
-- =============================================================================

-- LOADER: read/write to raw landing schemas
GRANT USAGE ON DATABASE RAW_VAULT TO ROLE LOADER;
GRANT USAGE ON SCHEMA RAW_VAULT.ECOMMERCE  TO ROLE LOADER;
GRANT USAGE ON SCHEMA RAW_VAULT.CRM        TO ROLE LOADER;
GRANT USAGE ON SCHEMA RAW_VAULT.CDC        TO ROLE LOADER;
GRANT USAGE ON SCHEMA RAW_VAULT.STREAMING  TO ROLE LOADER;
GRANT CREATE TABLE ON SCHEMA RAW_VAULT.ECOMMERCE  TO ROLE LOADER;
GRANT CREATE TABLE ON SCHEMA RAW_VAULT.CRM        TO ROLE LOADER;
GRANT CREATE TABLE ON SCHEMA RAW_VAULT.CDC        TO ROLE LOADER;
GRANT CREATE TABLE ON SCHEMA RAW_VAULT.STREAMING  TO ROLE LOADER;
GRANT SELECT, INSERT ON FUTURE TABLES IN SCHEMA RAW_VAULT.ECOMMERCE TO ROLE LOADER;
GRANT SELECT, INSERT ON FUTURE TABLES IN SCHEMA RAW_VAULT.CRM      TO ROLE LOADER;
GRANT SELECT, INSERT ON FUTURE TABLES IN SCHEMA RAW_VAULT.CDC      TO ROLE LOADER;
GRANT SELECT, INSERT ON FUTURE TABLES IN SCHEMA RAW_VAULT.STREAMING TO ROLE LOADER;

-- TRANSFORMER: read raw, write vault + business vault + analytics
GRANT USAGE ON DATABASE RAW_VAULT        TO ROLE TRANSFORMER;
GRANT USAGE ON DATABASE BUSINESS_VAULT   TO ROLE TRANSFORMER;
GRANT USAGE ON DATABASE ANALYTICS        TO ROLE TRANSFORMER;
GRANT USAGE ON DATABASE AUDIT            TO ROLE TRANSFORMER;

GRANT USAGE ON ALL SCHEMAS IN DATABASE RAW_VAULT       TO ROLE TRANSFORMER;
GRANT USAGE ON ALL SCHEMAS IN DATABASE BUSINESS_VAULT  TO ROLE TRANSFORMER;
GRANT USAGE ON ALL SCHEMAS IN DATABASE ANALYTICS       TO ROLE TRANSFORMER;
GRANT USAGE ON ALL SCHEMAS IN DATABASE AUDIT           TO ROLE TRANSFORMER;

GRANT SELECT ON FUTURE TABLES IN DATABASE RAW_VAULT       TO ROLE TRANSFORMER;
GRANT SELECT ON FUTURE TABLES IN DATABASE BUSINESS_VAULT  TO ROLE TRANSFORMER;

GRANT CREATE TABLE, CREATE VIEW ON ALL SCHEMAS IN DATABASE RAW_VAULT       TO ROLE TRANSFORMER;
GRANT CREATE TABLE, CREATE VIEW ON ALL SCHEMAS IN DATABASE BUSINESS_VAULT  TO ROLE TRANSFORMER;
GRANT CREATE TABLE, CREATE VIEW ON ALL SCHEMAS IN DATABASE ANALYTICS       TO ROLE TRANSFORMER;
GRANT CREATE TABLE, CREATE VIEW ON ALL SCHEMAS IN DATABASE AUDIT           TO ROLE TRANSFORMER;

-- ANALYST: read-only on analytics
GRANT USAGE ON DATABASE ANALYTICS TO ROLE ANALYST;
GRANT USAGE ON ALL SCHEMAS IN DATABASE ANALYTICS TO ROLE ANALYST;
GRANT SELECT ON FUTURE TABLES IN DATABASE ANALYTICS TO ROLE ANALYST;
GRANT SELECT ON FUTURE VIEWS  IN DATABASE ANALYTICS TO ROLE ANALYST;

-- DATA_STEWARD: metadata + governance access
GRANT USAGE ON DATABASE RAW_VAULT      TO ROLE DATA_STEWARD;
GRANT USAGE ON DATABASE BUSINESS_VAULT TO ROLE DATA_STEWARD;
GRANT USAGE ON DATABASE ANALYTICS      TO ROLE DATA_STEWARD;
GRANT USAGE ON DATABASE AUDIT          TO ROLE DATA_STEWARD;

-- PLATFORM_ADMIN: full control
GRANT ALL PRIVILEGES ON DATABASE RAW_VAULT      TO ROLE PLATFORM_ADMIN;
GRANT ALL PRIVILEGES ON DATABASE BUSINESS_VAULT TO ROLE PLATFORM_ADMIN;
GRANT ALL PRIVILEGES ON DATABASE ANALYTICS      TO ROLE PLATFORM_ADMIN;
GRANT ALL PRIVILEGES ON DATABASE AUDIT          TO ROLE PLATFORM_ADMIN;
