-- =============================================================================
-- 05_streams_and_tasks.sql — Snowflake Streams & Tasks
-- Data Vault 2.0 Snowflake Data Platform
--
-- PURPOSE: CDC-style change tracking on raw landing tables using Snowflake
--          Streams, with scheduled Tasks to load Data Vault entities.
--
-- EXECUTION ORDER: Run AFTER raw landing tables are created (or defer to runtime)
-- REQUIRES: DATA_ENGINEER or PLATFORM_ADMIN
-- =============================================================================

USE ROLE PLATFORM_ADMIN;
USE DATABASE RAW_VAULT;

-- =============================================================================
-- 1. RAW LANDING TABLES (for Snowpipe to land into)
-- =============================================================================

-- Customers landing table
CREATE TABLE IF NOT EXISTS RAW_VAULT.ECOMMERCE.RAW_CUSTOMERS (
    CUSTOMER_ID         VARCHAR(100)    NOT NULL,
    FIRST_NAME          VARCHAR(200),
    LAST_NAME           VARCHAR(200),
    EMAIL               VARCHAR(500),
    PHONE               VARCHAR(50),
    DATE_OF_BIRTH       DATE,
    GENDER              VARCHAR(20),
    COUNTRY_CODE        VARCHAR(10),
    CITY                VARCHAR(200),
    STATE               VARCHAR(200),
    POSTAL_CODE         VARCHAR(20),
    REGISTRATION_DATE   TIMESTAMP_NTZ,
    CUSTOMER_SEGMENT    VARCHAR(50),
    LOYALTY_TIER        VARCHAR(50),
    _LOADED_AT          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    _FILE_NAME          VARCHAR(1000)   DEFAULT METADATA$FILENAME,
    _FILE_ROW_NUMBER    NUMBER(18,0)    DEFAULT METADATA$FILE_ROW_NUMBER
)
CLUSTER BY (CUSTOMER_ID)
COMMENT = 'Raw landing — E-Commerce customers (batch file ingestion)';

-- Orders landing table
CREATE TABLE IF NOT EXISTS RAW_VAULT.ECOMMERCE.RAW_ORDERS (
    ORDER_ID            VARCHAR(100)    NOT NULL,
    CUSTOMER_ID         VARCHAR(100)    NOT NULL,
    ORDER_DATE          TIMESTAMP_NTZ   NOT NULL,
    ORDER_STATUS        VARCHAR(50),
    TOTAL_AMOUNT        NUMBER(18,4),
    CURRENCY_CODE       VARCHAR(10),
    PAYMENT_METHOD      VARCHAR(50),
    SHIPPING_METHOD     VARCHAR(50),
    SHIPPING_ADDRESS    VARCHAR(1000),
    BILLING_ADDRESS     VARCHAR(1000),
    DISCOUNT_AMOUNT     NUMBER(18,4)    DEFAULT 0,
    TAX_AMOUNT          NUMBER(18,4)    DEFAULT 0,
    FULFILLMENT_DATE    TIMESTAMP_NTZ,
    _LOADED_AT          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    _FILE_NAME          VARCHAR(1000)   DEFAULT METADATA$FILENAME,
    _FILE_ROW_NUMBER    NUMBER(18,0)    DEFAULT METADATA$FILE_ROW_NUMBER
)
CLUSTER BY (ORDER_ID, ORDER_DATE)
COMMENT = 'Raw landing — E-Commerce orders (batch file ingestion)';

-- Products landing table
CREATE TABLE IF NOT EXISTS RAW_VAULT.ECOMMERCE.RAW_PRODUCTS (
    PRODUCT_ID          VARCHAR(100)    NOT NULL,
    PRODUCT_NAME        VARCHAR(500)    NOT NULL,
    CATEGORY            VARCHAR(200),
    SUBCATEGORY         VARCHAR(200),
    BRAND               VARCHAR(200),
    UNIT_PRICE          NUMBER(18,4),
    COST_PRICE          NUMBER(18,4),
    WEIGHT_KG           NUMBER(10,3),
    IS_ACTIVE           BOOLEAN         DEFAULT TRUE,
    CREATED_AT          TIMESTAMP_NTZ,
    UPDATED_AT          TIMESTAMP_NTZ,
    _LOADED_AT          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    _FILE_NAME          VARCHAR(1000)   DEFAULT METADATA$FILENAME,
    _FILE_ROW_NUMBER    NUMBER(18,0)    DEFAULT METADATA$FILE_ROW_NUMBER
)
CLUSTER BY (PRODUCT_ID)
COMMENT = 'Raw landing — E-Commerce products (batch file ingestion)';

-- Order Items landing table
CREATE TABLE IF NOT EXISTS RAW_VAULT.ECOMMERCE.RAW_ORDER_ITEMS (
    ORDER_ITEM_ID       VARCHAR(100)    NOT NULL,
    ORDER_ID            VARCHAR(100)    NOT NULL,
    PRODUCT_ID          VARCHAR(100)    NOT NULL,
    QUANTITY            NUMBER(10,0)    NOT NULL,
    UNIT_PRICE          NUMBER(18,4)    NOT NULL,
    DISCOUNT_PERCENT    NUMBER(5,2)     DEFAULT 0,
    LINE_TOTAL          NUMBER(18,4),
    _LOADED_AT          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    _FILE_NAME          VARCHAR(1000)   DEFAULT METADATA$FILENAME,
    _FILE_ROW_NUMBER    NUMBER(18,0)    DEFAULT METADATA$FILE_ROW_NUMBER
)
CLUSTER BY (ORDER_ID)
COMMENT = 'Raw landing — E-Commerce order line items (batch file ingestion)';

-- =============================================================================
-- 2. STREAMS (CDC on landing tables)
-- =============================================================================

-- Stream on customers — captures inserts, updates, deletes
CREATE STREAM IF NOT EXISTS RAW_VAULT.ECOMMERCE.STREAM_CUSTOMERS
    ON TABLE RAW_VAULT.ECOMMERCE.RAW_CUSTOMERS
    APPEND_ONLY = FALSE
    SHOW_INITIAL_ROWS = TRUE
    COMMENT = 'CDC stream on raw customers — feeds Hub/Satellite loading';

-- Stream on orders
CREATE STREAM IF NOT EXISTS RAW_VAULT.ECOMMERCE.STREAM_ORDERS
    ON TABLE RAW_VAULT.ECOMMERCE.RAW_ORDERS
    APPEND_ONLY = FALSE
    SHOW_INITIAL_ROWS = TRUE
    COMMENT = 'CDC stream on raw orders — feeds Hub/Link/Satellite loading';

-- Stream on products
CREATE STREAM IF NOT EXISTS RAW_VAULT.ECOMMERCE.STREAM_PRODUCTS
    ON TABLE RAW_VAULT.ECOMMERCE.RAW_PRODUCTS
    APPEND_ONLY = FALSE
    SHOW_INITIAL_ROWS = TRUE
    COMMENT = 'CDC stream on raw products — feeds Hub/Satellite loading';

-- Stream on order items
CREATE STREAM IF NOT EXISTS RAW_VAULT.ECOMMERCE.STREAM_ORDER_ITEMS
    ON TABLE RAW_VAULT.ECOMMERCE.RAW_ORDER_ITEMS
    APPEND_ONLY = FALSE
    SHOW_INITIAL_ROWS = TRUE
    COMMENT = 'CDC stream on raw order items — feeds Link/Satellite loading';

-- =============================================================================
-- 3. TASKS (Scheduled Data Vault loading via dbt)
-- =============================================================================

-- Root task — orchestrates the full Data Vault loading pipeline
-- NOTE: In production, Airflow manages dbt runs. These tasks serve as
--       a fallback / near-real-time micro-batch alternative.

CREATE TASK IF NOT EXISTS RAW_VAULT.ECOMMERCE.TASK_DV_LOAD_ROOT
    WAREHOUSE = TRANSFORMER_WH
    SCHEDULE  = 'USING CRON 0 */4 * * * UTC'     -- Every 4 hours
    COMMENT   = 'Root task — triggers Data Vault loading pipeline'
    WHEN SYSTEM$STREAM_HAS_DATA('RAW_VAULT.ECOMMERCE.STREAM_CUSTOMERS')
      OR SYSTEM$STREAM_HAS_DATA('RAW_VAULT.ECOMMERCE.STREAM_ORDERS')
      OR SYSTEM$STREAM_HAS_DATA('RAW_VAULT.ECOMMERCE.STREAM_PRODUCTS')
      OR SYSTEM$STREAM_HAS_DATA('RAW_VAULT.ECOMMERCE.STREAM_ORDER_ITEMS')
AS
    -- Placeholder: In production, this calls an external function or
    -- triggers an Airflow DAG via REST API.
    -- For standalone mode, run dbt via Snowpark:
    SELECT 'Data Vault load triggered at ' || CURRENT_TIMESTAMP()::VARCHAR AS STATUS;

-- Child task — log execution
CREATE TASK IF NOT EXISTS RAW_VAULT.ECOMMERCE.TASK_DV_LOAD_LOG
    WAREHOUSE = TRANSFORMER_WH
    AFTER RAW_VAULT.ECOMMERCE.TASK_DV_LOAD_ROOT
    COMMENT = 'Logs Data Vault load execution to AUDIT.CONTROL.TASK_EXECUTION_LOG'
AS
    INSERT INTO AUDIT.CONTROL.TASK_EXECUTION_LOG
        (TASK_NAME, TASK_TYPE, STATUS, STARTED_AT)
    VALUES
        ('TASK_DV_LOAD_ROOT', 'SF_TASK', 'SUCCESS', CURRENT_TIMESTAMP());

-- =============================================================================
-- 4. GRANTS
-- =============================================================================

-- Stream access for TRANSFORMER (dbt reads streams)
GRANT SELECT ON ALL STREAMS IN SCHEMA RAW_VAULT.ECOMMERCE TO ROLE TRANSFORMER;

-- Task management for DATA_ENGINEER
GRANT MONITOR, OPERATE ON ALL TASKS IN SCHEMA RAW_VAULT.ECOMMERCE TO ROLE DATA_ENGINEER;

-- =============================================================================
-- 5. TASK ACTIVATION (uncomment when ready)
-- =============================================================================
-- ALTER TASK RAW_VAULT.ECOMMERCE.TASK_DV_LOAD_LOG  RESUME;
-- ALTER TASK RAW_VAULT.ECOMMERCE.TASK_DV_LOAD_ROOT RESUME;
-- NOTE: Always resume child tasks BEFORE parent tasks
