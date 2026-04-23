-- =============================================================================
-- 17_iceberg_tables.sql — Apache Iceberg Tables (Open Lakehouse)
-- Data Vault 2.0 Snowflake Data Platform
--
-- PURPOSE: Configure Snowflake-managed Iceberg Tables for interoperability
--          with open-source engines (Spark, Trino, Flink, Presto).
--          Enables the "Open Lakehouse" pattern where Snowflake manages
--          the Iceberg metadata catalog while data resides in cloud storage.
--
-- USE CASES:
--   - Archival/historical data accessible from both Snowflake and Spark
--   - Cross-engine analytics without data movement
--   - Cost-optimized storage for cold-tier data
--
-- REQUIRES: ACCOUNTADMIN (for catalog integration)
-- =============================================================================

USE ROLE ACCOUNTADMIN;

-- =============================================================================
-- 1. EXTERNAL VOLUME — Object Storage for Iceberg Data Files
-- =============================================================================

CREATE EXTERNAL VOLUME IF NOT EXISTS EV_ICEBERG_DATA_LAKE
    STORAGE_LOCATIONS = (
        (
            NAME = 's3_iceberg_us'
            STORAGE_BASE_URL = 's3://your-company-iceberg-data/'
            STORAGE_PROVIDER = 'S3'
            STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::role/snowflake-iceberg-access'
        )
    )
    COMMENT = 'External volume for Iceberg table data files (Parquet + metadata)';

-- =============================================================================
-- 2. ICEBERG CATALOG INTEGRATION — Snowflake as Catalog
-- =============================================================================

-- Use Snowflake as the Iceberg catalog (managed by Snowflake)
-- External engines connect via Snowflake Open Catalog (Polaris)

-- =============================================================================
-- 3. ICEBERG TABLES — Archive Layer
-- =============================================================================

-- Archival customer data (accessible from Spark/Trino)
CREATE ICEBERG TABLE IF NOT EXISTS RAW_VAULT.ECOMMERCE.ICE_CUSTOMER_ARCHIVE
    CATALOG = 'SNOWFLAKE'
    EXTERNAL_VOLUME = 'EV_ICEBERG_DATA_LAKE'
    BASE_LOCATION = 'iceberg/customer_archive/'
    AS SELECT
        HK_CUSTOMER,
        CUSTOMER_ID,
        LOAD_DATETIME,
        RECORD_SOURCE
    FROM RAW_VAULT.RAW_VAULT.HUB_CUSTOMER
    WHERE LOAD_DATETIME < DATEADD('year', -2, CURRENT_DATE());

-- Archival order history (accessible from Spark/Trino)
CREATE ICEBERG TABLE IF NOT EXISTS RAW_VAULT.ECOMMERCE.ICE_ORDER_ARCHIVE
    CATALOG = 'SNOWFLAKE'
    EXTERNAL_VOLUME = 'EV_ICEBERG_DATA_LAKE'
    BASE_LOCATION = 'iceberg/order_archive/'
    AS SELECT
        HK_ORDER,
        ORDER_ID,
        LOAD_DATETIME,
        RECORD_SOURCE
    FROM RAW_VAULT.RAW_VAULT.HUB_ORDER
    WHERE LOAD_DATETIME < DATEADD('year', -2, CURRENT_DATE());

-- =============================================================================
-- 4. ICEBERG TABLE MONITORING
-- =============================================================================

CREATE OR REPLACE VIEW AUDIT.CONTROL.ICEBERG_TABLE_STATUS AS
SELECT
    TABLE_CATALOG,
    TABLE_SCHEMA,
    TABLE_NAME,
    IS_ICEBERG,
    BYTES,
    ROW_COUNT,
    LAST_ALTERED,
    COMMENT
FROM INFORMATION_SCHEMA.TABLES
WHERE IS_ICEBERG = 'YES'
ORDER BY TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME;

GRANT SELECT ON VIEW AUDIT.CONTROL.ICEBERG_TABLE_STATUS TO ROLE DATA_ENGINEER;

-- =============================================================================
-- 5. GRANTS
-- =============================================================================

GRANT USAGE ON EXTERNAL VOLUME EV_ICEBERG_DATA_LAKE TO ROLE TRANSFORMER;
GRANT USAGE ON EXTERNAL VOLUME EV_ICEBERG_DATA_LAKE TO ROLE DATA_ENGINEER;
