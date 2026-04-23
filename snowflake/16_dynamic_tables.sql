-- =============================================================================
-- 16_dynamic_tables.sql — Snowflake Dynamic Tables (Silver/Gold Acceleration)
-- Data Vault 2.0 Snowflake Data Platform
--
-- PURPOSE: Replace traditional incremental dbt models with Snowflake Dynamic
--          Tables for declarative, auto-refreshing transformations.
--          Dynamic Tables automatically manage scheduling, dependency tracking,
--          and incremental computation — reducing orchestration overhead.
--
-- USE CASES:
--   - PIT tables (auto-refresh when upstream satellites change)
--   - Bridge tables (auto-refresh when upstream links change)
--   - Gold-layer aggregates (auto-refresh for real-time dashboards)
--
-- REQUIRES: TRANSFORMER role with CREATE DYNAMIC TABLE privilege
-- =============================================================================

USE ROLE TRANSFORMER;
USE WAREHOUSE TRANSFORMER_WH;

-- =============================================================================
-- 1. DYNAMIC PIT TABLE — Customer Point-in-Time (Silver Layer)
-- Automatically refreshes when HUB_CUSTOMER or its satellites update.
-- TARGET_LAG: 10 minutes (near real-time for BI dashboards)
-- =============================================================================

CREATE OR REPLACE DYNAMIC TABLE BUSINESS_VAULT.PIT_TABLES.DYN_PIT_CUSTOMER
    TARGET_LAG = '10 minutes'
    WAREHOUSE  = TRANSFORMER_WH
    REFRESH_MODE = AUTO
    INITIALIZE = ON_CREATE
    COMMENT = 'Dynamic PIT for Customer — auto-refreshes from Hub + Satellites'
AS
SELECT
    h.HK_CUSTOMER,
    h.CUSTOMER_ID,
    COALESCE(sd.LOAD_DATETIME, '1900-01-01'::TIMESTAMP) AS SAT_DETAILS_LOAD_DT,
    COALESCE(sdm.LOAD_DATETIME, '1900-01-01'::TIMESTAMP) AS SAT_DEMOGRAPHICS_LOAD_DT,
    sd.HK_CUSTOMER AS SAT_DETAILS_HK,
    sdm.HK_CUSTOMER AS SAT_DEMOGRAPHICS_HK,
    CURRENT_TIMESTAMP() AS PIT_LOAD_DATETIME
FROM RAW_VAULT.RAW_VAULT.HUB_CUSTOMER h
LEFT JOIN (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY HK_CUSTOMER ORDER BY LOAD_DATETIME DESC) AS RN
    FROM RAW_VAULT.RAW_VAULT.SAT_CUSTOMER_DETAILS
) sd ON h.HK_CUSTOMER = sd.HK_CUSTOMER AND sd.RN = 1
LEFT JOIN (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY HK_CUSTOMER ORDER BY LOAD_DATETIME DESC) AS RN
    FROM RAW_VAULT.RAW_VAULT.SAT_CUSTOMER_DEMOGRAPHICS
) sdm ON h.HK_CUSTOMER = sdm.HK_CUSTOMER AND sdm.RN = 1;

-- =============================================================================
-- 2. DYNAMIC BRIDGE TABLE — Customer Orders (Silver Layer)
-- =============================================================================

CREATE OR REPLACE DYNAMIC TABLE BUSINESS_VAULT.BRIDGE_TABLES.DYN_BRIDGE_CUSTOMER_ORDERS
    TARGET_LAG = '30 minutes'
    WAREHOUSE  = TRANSFORMER_WH
    REFRESH_MODE = AUTO
    INITIALIZE = ON_CREATE
    COMMENT = 'Dynamic Bridge: Customer → Orders — auto-refreshes from Hub + Link'
AS
SELECT
    h.HK_CUSTOMER,
    h.CUSTOMER_ID,
    l.HK_LINK_CUSTOMER_ORDER,
    l.HK_ORDER,
    ho.ORDER_ID,
    l.LOAD_DATETIME AS LINK_LOAD_DATETIME
FROM RAW_VAULT.RAW_VAULT.HUB_CUSTOMER h
INNER JOIN RAW_VAULT.RAW_VAULT.LINK_CUSTOMER_ORDER l ON h.HK_CUSTOMER = l.HK_CUSTOMER
INNER JOIN RAW_VAULT.RAW_VAULT.HUB_ORDER ho ON l.HK_ORDER = ho.HK_ORDER;

-- =============================================================================
-- 3. DYNAMIC AGGREGATE — Revenue by Customer Segment (Gold Layer)
-- TARGET_LAG: Downstream — refreshes when upstream dynamic tables refresh.
-- =============================================================================

CREATE OR REPLACE DYNAMIC TABLE ANALYTICS.AGGREGATES.DYN_AGG_REVENUE_BY_SEGMENT
    TARGET_LAG = DOWNSTREAM
    WAREHOUSE  = ANALYTICS_WH
    REFRESH_MODE = AUTO
    INITIALIZE = ON_CREATE
    COMMENT = 'Dynamic Aggregate: Revenue by customer segment — auto-refreshes from PIT + Sats'
AS
SELECT
    sdm.CUSTOMER_SEGMENT,
    sdm.LOYALTY_TIER,
    COUNT(DISTINCT pit.HK_CUSTOMER) AS CUSTOMER_COUNT,
    COUNT(DISTINCT bo.HK_ORDER) AS ORDER_COUNT,
    SUM(sof.TOTAL_AMOUNT) AS TOTAL_REVENUE,
    AVG(sof.TOTAL_AMOUNT) AS AVG_ORDER_VALUE,
    SUM(sof.DISCOUNT_AMOUNT) AS TOTAL_DISCOUNTS,
    CURRENT_TIMESTAMP() AS REFRESHED_AT
FROM BUSINESS_VAULT.PIT_TABLES.DYN_PIT_CUSTOMER pit
LEFT JOIN RAW_VAULT.RAW_VAULT.SAT_CUSTOMER_DEMOGRAPHICS sdm
    ON pit.SAT_DEMOGRAPHICS_HK = sdm.HK_CUSTOMER
LEFT JOIN BUSINESS_VAULT.BRIDGE_TABLES.DYN_BRIDGE_CUSTOMER_ORDERS bo
    ON pit.HK_CUSTOMER = bo.HK_CUSTOMER
LEFT JOIN (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY HK_ORDER ORDER BY LOAD_DATETIME DESC) AS RN
    FROM RAW_VAULT.RAW_VAULT.SAT_ORDER_FINANCIALS
) sof ON bo.HK_ORDER = sof.HK_ORDER AND sof.RN = 1
GROUP BY sdm.CUSTOMER_SEGMENT, sdm.LOYALTY_TIER;

-- =============================================================================
-- 4. MONITORING — Dynamic Table Refresh Status
-- =============================================================================

CREATE OR REPLACE VIEW AUDIT.CONTROL.DYNAMIC_TABLE_HEALTH AS
SELECT
    NAME,
    DATABASE_NAME,
    SCHEMA_NAME,
    TARGET_LAG,
    REFRESH_MODE,
    SCHEDULING_STATE,
    LAST_COMPLETED_REFRESH_TIME,
    DATEDIFF('minute', LAST_COMPLETED_REFRESH_TIME, CURRENT_TIMESTAMP()) AS MINUTES_SINCE_REFRESH,
    CASE
        WHEN SCHEDULING_STATE = 'RUNNING' THEN 'HEALTHY'
        WHEN DATEDIFF('minute', LAST_COMPLETED_REFRESH_TIME, CURRENT_TIMESTAMP()) > 60 THEN 'STALE'
        ELSE 'OK'
    END AS HEALTH_STATUS
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLES())
ORDER BY LAST_COMPLETED_REFRESH_TIME DESC;

GRANT SELECT ON VIEW AUDIT.CONTROL.DYNAMIC_TABLE_HEALTH TO ROLE DATA_ENGINEER;
GRANT SELECT ON VIEW AUDIT.CONTROL.DYNAMIC_TABLE_HEALTH TO ROLE PLATFORM_ADMIN;
