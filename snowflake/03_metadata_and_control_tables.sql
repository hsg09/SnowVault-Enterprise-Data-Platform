-- =============================================================================
-- 03_metadata_and_control_tables.sql — Pipeline Metadata & Control Plane
-- Data Vault 2.0 Snowflake Data Platform
--
-- PURPOSE: Operational metadata for pipeline observability, schema tracking,
--          data quality results, and cost attribution.
--
-- EXECUTION ORDER: Run AFTER 01_databases_and_schemas.sql
-- REQUIRES: PLATFORM_ADMIN or ACCOUNTADMIN
-- =============================================================================

USE ROLE PLATFORM_ADMIN;
USE DATABASE AUDIT;
USE SCHEMA CONTROL;

-- =============================================================================
-- 1. FILE INGESTION LOG
-- Tracks every Snowpipe / COPY INTO load event with row counts + status.
-- =============================================================================
CREATE TABLE IF NOT EXISTS FILE_INGESTION_LOG (
    INGESTION_ID        VARCHAR(64)     DEFAULT UUID_STRING()   NOT NULL,
    FILE_NAME           VARCHAR(1000)   NOT NULL,
    FILE_PATH           VARCHAR(4000)   NOT NULL,
    STAGE_NAME          VARCHAR(500)    NOT NULL,
    SOURCE_SYSTEM       VARCHAR(100)    NOT NULL,
    FILE_FORMAT         VARCHAR(100)    NOT NULL,
    FILE_SIZE_BYTES     NUMBER(18,0),
    ROW_COUNT           NUMBER(18,0),
    ROWS_PARSED         NUMBER(18,0),
    ROWS_LOADED         NUMBER(18,0),
    ERRORS_SEEN         NUMBER(18,0)    DEFAULT 0,
    FIRST_ERROR_MESSAGE VARCHAR(4000),
    STATUS              VARCHAR(20)     NOT NULL    -- QUEUED | LOADING | LOADED | FAILED | SKIPPED
                        DEFAULT 'QUEUED',
    PIPE_NAME           VARCHAR(500),
    LOAD_START_TS       TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    LOAD_END_TS         TIMESTAMP_NTZ,
    CREATED_AT          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_FILE_INGESTION_LOG PRIMARY KEY (INGESTION_ID)
)
CLUSTER BY (SOURCE_SYSTEM, CREATED_AT)
COMMENT = 'Tracks every file ingestion event — Snowpipe and COPY INTO';

-- =============================================================================
-- 2. SCHEMA REGISTRY
-- Stores inferred or declared schemas per source, enabling schema drift detection.
-- =============================================================================
CREATE TABLE IF NOT EXISTS SCHEMA_REGISTRY (
    REGISTRY_ID         VARCHAR(64)     DEFAULT UUID_STRING()   NOT NULL,
    SOURCE_SYSTEM       VARCHAR(100)    NOT NULL,
    TABLE_NAME          VARCHAR(500)    NOT NULL,
    COLUMN_NAME         VARCHAR(500)    NOT NULL,
    DATA_TYPE           VARCHAR(100)    NOT NULL,
    ORDINAL_POSITION    NUMBER(6,0)     NOT NULL,
    IS_NULLABLE         BOOLEAN         DEFAULT TRUE,
    IS_PRIMARY_KEY      BOOLEAN         DEFAULT FALSE,
    SCHEMA_VERSION      NUMBER(6,0)     DEFAULT 1,
    DETECTED_AT         TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    IS_CURRENT          BOOLEAN         DEFAULT TRUE,
    PREVIOUS_DATA_TYPE  VARCHAR(100),
    CHANGE_TYPE         VARCHAR(20),    -- ADD | REMOVE | TYPE_CHANGE | RENAME
    CONSTRAINT PK_SCHEMA_REGISTRY PRIMARY KEY (REGISTRY_ID)
)
COMMENT = 'Schema registry — inferred schemas per source for drift detection';

-- =============================================================================
-- 3. TASK EXECUTION LOG
-- Records every dbt / Airflow / Snowflake task run with timing + status.
-- =============================================================================
CREATE TABLE IF NOT EXISTS TASK_EXECUTION_LOG (
    EXECUTION_ID        VARCHAR(64)     DEFAULT UUID_STRING()   NOT NULL,
    TASK_NAME           VARCHAR(500)    NOT NULL,
    TASK_TYPE           VARCHAR(50)     NOT NULL,   -- DBT_RUN | DBT_TEST | AIRFLOW_TASK | SF_TASK
    DAG_ID              VARCHAR(250),
    RUN_ID              VARCHAR(250),
    MODEL_NAME          VARCHAR(500),
    STATUS              VARCHAR(20)     NOT NULL    -- RUNNING | SUCCESS | FAILED | SKIPPED | UPSTREAM_FAILED
                        DEFAULT 'RUNNING',
    ROWS_AFFECTED       NUMBER(18,0),
    EXECUTION_TIME_SEC  NUMBER(12,2),
    ERROR_MESSAGE       VARCHAR(4000),
    STARTED_AT          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    COMPLETED_AT        TIMESTAMP_NTZ,
    CONSTRAINT PK_TASK_EXECUTION_LOG PRIMARY KEY (EXECUTION_ID)
)
CLUSTER BY (TASK_TYPE, STARTED_AT)
COMMENT = 'Pipeline task execution log — dbt, Airflow, Snowflake tasks';

-- =============================================================================
-- 4. DATA QUALITY RESULTS
-- Stores dbt test results + custom DQ checks for trend analysis.
-- =============================================================================
USE SCHEMA DQ_RESULTS;

CREATE TABLE IF NOT EXISTS DQ_TEST_RESULTS (
    TEST_ID             VARCHAR(64)     DEFAULT UUID_STRING()   NOT NULL,
    TEST_NAME           VARCHAR(500)    NOT NULL,
    TEST_TYPE           VARCHAR(50)     NOT NULL,   -- GENERIC | SINGULAR | CUSTOM | DBT_EXPECTATIONS
    MODEL_NAME          VARCHAR(500),
    COLUMN_NAME         VARCHAR(500),
    DATABASE_NAME       VARCHAR(250),
    SCHEMA_NAME         VARCHAR(250),
    TABLE_NAME          VARCHAR(500),
    SEVERITY            VARCHAR(20)     DEFAULT 'WARN',     -- WARN | ERROR
    STATUS              VARCHAR(20)     NOT NULL,           -- PASS | FAIL | WARN | ERROR
    FAILURES_COUNT      NUMBER(18,0)    DEFAULT 0,
    ROWS_SCANNED        NUMBER(18,0),
    FAILURE_PERCENTAGE  NUMBER(8,4),
    ERROR_MESSAGE       VARCHAR(4000),
    EXECUTED_AT         TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    EXECUTION_TIME_SEC  NUMBER(12,2),
    CONSTRAINT PK_DQ_TEST_RESULTS PRIMARY KEY (TEST_ID)
)
CLUSTER BY (MODEL_NAME, EXECUTED_AT)
COMMENT = 'Data quality test results — dbt tests, dbt_expectations, custom checks';

-- =============================================================================
-- 5. REPLICATION STATUS
-- Tracks cross-region replication lag and status for DR monitoring.
-- =============================================================================
USE SCHEMA CONTROL;

CREATE TABLE IF NOT EXISTS REPLICATION_STATUS (
    STATUS_ID               VARCHAR(64)     DEFAULT UUID_STRING()   NOT NULL,
    REPLICATION_GROUP_NAME  VARCHAR(500)    NOT NULL,
    SOURCE_ACCOUNT          VARCHAR(250)    NOT NULL,
    TARGET_ACCOUNT          VARCHAR(250)    NOT NULL,
    DATABASE_NAME           VARCHAR(250)    NOT NULL,
    REPLICATION_LAG_SECONDS NUMBER(12,0),
    BYTES_TRANSFERRED       NUMBER(18,0),
    STATUS                  VARCHAR(20)     NOT NULL,   -- ACTIVE | SUSPENDED | FAILED | INITIALIZING
    LAST_REFRESH_START      TIMESTAMP_NTZ,
    LAST_REFRESH_END        TIMESTAMP_NTZ,
    CHECKED_AT              TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_REPLICATION_STATUS PRIMARY KEY (STATUS_ID)
)
COMMENT = 'Cross-region replication lag and status monitoring';

-- =============================================================================
-- 6. COST ATTRIBUTION
-- Records credit usage per warehouse, query tag, and cost center.
-- =============================================================================
USE SCHEMA COST_TRACKING;

CREATE TABLE IF NOT EXISTS COST_ATTRIBUTION (
    ATTRIBUTION_ID      VARCHAR(64)     DEFAULT UUID_STRING()   NOT NULL,
    WAREHOUSE_NAME      VARCHAR(250)    NOT NULL,
    QUERY_TAG           VARCHAR(500),
    COST_CENTER         VARCHAR(100),
    CREDITS_USED        NUMBER(12,4)    NOT NULL,
    CREDITS_USED_CLOUD  NUMBER(12,4)    DEFAULT 0,
    QUERIES_EXECUTED    NUMBER(12,0)    DEFAULT 0,
    BYTES_SCANNED       NUMBER(18,0)    DEFAULT 0,
    PERIOD_START        TIMESTAMP_NTZ   NOT NULL,
    PERIOD_END          TIMESTAMP_NTZ   NOT NULL,
    CREATED_AT          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_COST_ATTRIBUTION PRIMARY KEY (ATTRIBUTION_ID)
)
CLUSTER BY (WAREHOUSE_NAME, PERIOD_START)
COMMENT = 'Credit usage attribution per warehouse and cost center';

-- =============================================================================
-- 7. GRANTS
-- =============================================================================

GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA AUDIT.CONTROL       TO ROLE TRANSFORMER;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA AUDIT.DQ_RESULTS    TO ROLE TRANSFORMER;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA AUDIT.COST_TRACKING TO ROLE TRANSFORMER;

GRANT SELECT ON ALL TABLES IN SCHEMA AUDIT.CONTROL       TO ROLE DATA_ENGINEER;
GRANT SELECT ON ALL TABLES IN SCHEMA AUDIT.DQ_RESULTS    TO ROLE DATA_ENGINEER;
GRANT SELECT ON ALL TABLES IN SCHEMA AUDIT.COST_TRACKING TO ROLE DATA_ENGINEER;

GRANT SELECT ON ALL TABLES IN SCHEMA AUDIT.COST_TRACKING TO ROLE DATA_STEWARD;
GRANT SELECT ON ALL TABLES IN SCHEMA AUDIT.DQ_RESULTS    TO ROLE DATA_STEWARD;
