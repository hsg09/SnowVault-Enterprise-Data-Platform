-- =============================================================================
-- 07_replication_and_failover.sql — Cross-Region Replication & DR
-- Data Vault 2.0 Snowflake Data Platform
--
-- PURPOSE: Configure database replication groups and failover groups for
--          disaster recovery across Snowflake accounts/regions.
--
-- EXECUTION ORDER: Run AFTER all databases and objects are created
-- REQUIRES: ACCOUNTADMIN on both primary and secondary accounts
-- NOTE: Multi-region accounts must be configured and linked first
-- =============================================================================

USE ROLE ACCOUNTADMIN;

-- =============================================================================
-- 1. REPLICATION GROUP (Database Replication)
-- Replicates RAW_VAULT, BUSINESS_VAULT, and ANALYTICS to secondary region.
-- =============================================================================

-- NOTE: Replace <secondary_account> with your actual secondary account locator
-- Example: my_org.my_secondary_account

-- CREATE REPLICATION GROUP IF NOT EXISTS RG_DATA_PLATFORM
--     OBJECT_TYPES = DATABASES, SHARES
--     ALLOWED_DATABASES = RAW_VAULT, BUSINESS_VAULT, ANALYTICS
--     ALLOWED_ACCOUNTS = <secondary_account>
--     REPLICATION_SCHEDULE = 'USING CRON 0 */2 * * * UTC'    -- Every 2 hours
--     COMMENT = 'Primary → Secondary replication for core data platform databases';

-- =============================================================================
-- 2. FAILOVER GROUP (Automatic Failover)
-- Enables automatic failover for business continuity.
-- =============================================================================

-- CREATE FAILOVER GROUP IF NOT EXISTS FG_DATA_PLATFORM
--     OBJECT_TYPES = DATABASES, ROLES, WAREHOUSES, NETWORK POLICIES, INTEGRATIONS
--     ALLOWED_DATABASES = RAW_VAULT, BUSINESS_VAULT, ANALYTICS, AUDIT
--     ALLOWED_ACCOUNTS = <secondary_account>
--     REPLICATION_SCHEDULE = 'USING CRON 0 */1 * * * UTC'    -- Every hour
--     COMMENT = 'Failover group — includes databases, roles, warehouses, network policies';

-- =============================================================================
-- 3. CLIENT REDIRECT (Connection Failover)
-- Redirects client connections to the secondary account during failover.
-- =============================================================================

-- ALTER CONNECTION DATA_PLATFORM_CONNECTION
--     ENABLE FAILOVER TO ACCOUNTS <secondary_account>;

-- =============================================================================
-- 4. SECONDARY ACCOUNT SETUP
-- Run these commands on the SECONDARY account to accept replication.
-- =============================================================================

-- -- On secondary account:
-- CREATE REPLICATION GROUP IF NOT EXISTS RG_DATA_PLATFORM
--     AS REPLICA OF <primary_account>.RG_DATA_PLATFORM;

-- CREATE FAILOVER GROUP IF NOT EXISTS FG_DATA_PLATFORM
--     AS REPLICA OF <primary_account>.FG_DATA_PLATFORM;

-- =============================================================================
-- 5. MONITORING VIEWS
-- Query these to monitor replication lag and status.
-- =============================================================================

-- View replication history
-- SELECT * FROM TABLE(INFORMATION_SCHEMA.REPLICATION_GROUP_REFRESH_HISTORY())
-- WHERE REPLICATION_GROUP_NAME = 'RG_DATA_PLATFORM'
-- ORDER BY PHASE_4_FINALIZING_END DESC
-- LIMIT 20;

-- View current replication status
-- SELECT
--     REPLICATION_GROUP_NAME,
--     PHASE_1_BEGIN,
--     PHASE_4_FINALIZING_END,
--     DATEDIFF('second', PHASE_1_BEGIN, PHASE_4_FINALIZING_END) AS TOTAL_SECONDS,
--     BYTES_TRANSFERRED,
--     OBJECT_COUNT
-- FROM TABLE(INFORMATION_SCHEMA.REPLICATION_GROUP_REFRESH_HISTORY())
-- WHERE REPLICATION_GROUP_NAME = 'RG_DATA_PLATFORM'
-- ORDER BY PHASE_1_BEGIN DESC
-- LIMIT 5;

-- =============================================================================
-- 6. FAILOVER PROCEDURES
-- =============================================================================

-- Initiate failover (run on SECONDARY account):
-- ALTER FAILOVER GROUP FG_DATA_PLATFORM PRIMARY;

-- Failback to original primary:
-- ALTER FAILOVER GROUP FG_DATA_PLATFORM PRIMARY;  -- run on original primary
