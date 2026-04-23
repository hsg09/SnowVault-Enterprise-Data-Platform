-- =============================================================================
-- 08_resource_monitors.sql — Credit Monitoring & Budget Controls
-- Data Vault 2.0 Snowflake Data Platform
--
-- PURPOSE: Enforce credit budgets per warehouse with tiered alerting.
--          Prevents cost overruns with automatic suspend at thresholds.
--
-- EXECUTION ORDER: Run AFTER 00_rbac_setup.sql (warehouses must exist)
-- REQUIRES: ACCOUNTADMIN
-- =============================================================================

USE ROLE ACCOUNTADMIN;

-- =============================================================================
-- 1. ACCOUNT-LEVEL RESOURCE MONITOR
-- Global safety net — hard limit across all warehouses.
-- =============================================================================

CREATE RESOURCE MONITOR IF NOT EXISTS RM_ACCOUNT_GLOBAL
    WITH
        CREDIT_QUOTA = 5000            -- Monthly credit quota (adjust per contract)
        FREQUENCY    = MONTHLY
        START_TIMESTAMP = IMMEDIATELY
        TRIGGERS
            ON 50 PERCENT DO NOTIFY                     -- Early warning at 50%
            ON 75 PERCENT DO NOTIFY                     -- Alert at 75%
            ON 90 PERCENT DO NOTIFY AND SUSPEND         -- Suspend new queries at 90%
            ON 100 PERCENT DO NOTIFY AND SUSPEND_IMMEDIATE   -- Hard stop at 100%
    ;

ALTER ACCOUNT SET RESOURCE_MONITOR = RM_ACCOUNT_GLOBAL;

-- =============================================================================
-- 2. WAREHOUSE-LEVEL RESOURCE MONITORS
-- =============================================================================

-- Loader warehouse — low budget (batch loading)
CREATE RESOURCE MONITOR IF NOT EXISTS RM_LOADER_WH
    WITH
        CREDIT_QUOTA = 200
        FREQUENCY    = MONTHLY
        START_TIMESTAMP = IMMEDIATELY
        TRIGGERS
            ON 70 PERCENT DO NOTIFY
            ON 90 PERCENT DO NOTIFY AND SUSPEND
            ON 100 PERCENT DO NOTIFY AND SUSPEND_IMMEDIATE
    ;
ALTER WAREHOUSE LOADER_WH SET RESOURCE_MONITOR = RM_LOADER_WH;

-- Transformer warehouse — medium budget (dbt batch runs)
CREATE RESOURCE MONITOR IF NOT EXISTS RM_TRANSFORMER_WH
    WITH
        CREDIT_QUOTA = 1500
        FREQUENCY    = MONTHLY
        START_TIMESTAMP = IMMEDIATELY
        TRIGGERS
            ON 60 PERCENT DO NOTIFY
            ON 80 PERCENT DO NOTIFY
            ON 95 PERCENT DO NOTIFY AND SUSPEND
            ON 100 PERCENT DO NOTIFY AND SUSPEND_IMMEDIATE
    ;
ALTER WAREHOUSE TRANSFORMER_WH SET RESOURCE_MONITOR = RM_TRANSFORMER_WH;

-- CI warehouse — small budget (PR validation only)
CREATE RESOURCE MONITOR IF NOT EXISTS RM_CI_WH
    WITH
        CREDIT_QUOTA = 100
        FREQUENCY    = MONTHLY
        START_TIMESTAMP = IMMEDIATELY
        TRIGGERS
            ON 80 PERCENT DO NOTIFY
            ON 100 PERCENT DO NOTIFY AND SUSPEND_IMMEDIATE
    ;
ALTER WAREHOUSE CI_WH SET RESOURCE_MONITOR = RM_CI_WH;

-- Dev warehouse — per-developer budget
CREATE RESOURCE MONITOR IF NOT EXISTS RM_DEV_WH
    WITH
        CREDIT_QUOTA = 200
        FREQUENCY    = MONTHLY
        START_TIMESTAMP = IMMEDIATELY
        TRIGGERS
            ON 75 PERCENT DO NOTIFY
            ON 95 PERCENT DO NOTIFY AND SUSPEND
            ON 100 PERCENT DO NOTIFY AND SUSPEND_IMMEDIATE
    ;
ALTER WAREHOUSE DEV_WH SET RESOURCE_MONITOR = RM_DEV_WH;

-- Analytics warehouse — BI query budget
CREATE RESOURCE MONITOR IF NOT EXISTS RM_ANALYTICS_WH
    WITH
        CREDIT_QUOTA = 2000
        FREQUENCY    = MONTHLY
        START_TIMESTAMP = IMMEDIATELY
        TRIGGERS
            ON 50 PERCENT DO NOTIFY
            ON 75 PERCENT DO NOTIFY
            ON 90 PERCENT DO NOTIFY AND SUSPEND
            ON 100 PERCENT DO NOTIFY AND SUSPEND_IMMEDIATE
    ;
ALTER WAREHOUSE ANALYTICS_WH SET RESOURCE_MONITOR = RM_ANALYTICS_WH;

-- =============================================================================
-- 3. EMAIL NOTIFICATION INTEGRATION
-- =============================================================================
-- NOTE: Configure email notification integration for resource monitor alerts.
-- Requires account-level setup.

-- CREATE NOTIFICATION INTEGRATION IF NOT EXISTS NI_RESOURCE_ALERTS
--     TYPE = EMAIL
--     ENABLED = TRUE
--     ALLOWED_RECIPIENTS = ('data-platform-team@yourcompany.com')
--     COMMENT = 'Email alerts for resource monitor threshold notifications';

-- =============================================================================
-- 4. CREDIT USAGE MONITORING VIEWS
-- =============================================================================

-- Current month credit usage by warehouse
-- SELECT
--     WAREHOUSE_NAME,
--     SUM(CREDITS_USED) AS CREDITS_USED,
--     SUM(CREDITS_USED_CLOUD_SERVICES) AS CLOUD_CREDITS,
--     SUM(CREDITS_USED_COMPUTE) AS COMPUTE_CREDITS
-- FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
-- WHERE START_TIME >= DATE_TRUNC('month', CURRENT_DATE())
-- GROUP BY WAREHOUSE_NAME
-- ORDER BY CREDITS_USED DESC;
