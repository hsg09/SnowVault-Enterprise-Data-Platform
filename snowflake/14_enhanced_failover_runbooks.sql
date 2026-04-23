-- =============================================================================
-- 14_enhanced_failover_runbooks.sql — Failover Groups + Continuity Runbooks
-- Data Vault 2.0 Snowflake Data Platform
--
-- PURPOSE: Production-ready failover groups with dangling reference prevention,
--          Client Redirect, and 3 continuity strategy runbooks.
--
-- BLUEPRINT: "security policies, users, and databases governed by them must
--            be co-located within the same failover group hierarchy"
-- =============================================================================

USE ROLE ACCOUNTADMIN;

-- =============================================================================
-- 1. FAILOVER GROUP — Co-locates ALL dependent objects
-- Prevents dangling references by including ROLES + NETWORK POLICIES + INTEGRATIONS
-- alongside the 4 databases.
-- =============================================================================

-- PRIMARY ACCOUNT — Create failover group
CREATE FAILOVER GROUP IF NOT EXISTS FG_DATA_PLATFORM
    OBJECT_TYPES = DATABASES, ROLES, WAREHOUSES, NETWORK POLICIES, INTEGRATIONS
    ALLOWED_DATABASES = RAW_VAULT, BUSINESS_VAULT, ANALYTICS, AUDIT
    ALLOWED_ACCOUNTS = <org_name>.<secondary_account_1>, <org_name>.<secondary_account_2>
    REPLICATION_SCHEDULE = 'USING CRON 0 */1 * * * UTC'
    COMMENT = 'Primary failover group — all dependent objects co-located';

-- =============================================================================
-- 2. CLIENT REDIRECT — DNS-level connection abstraction
-- "provides a unified connection URL for all downstream applications"
-- =============================================================================

ALTER CONNECTION DATA_PLATFORM_CONNECTION
    ENABLE FAILOVER TO ACCOUNTS
        <org_name>.<secondary_account_1>,
        <org_name>.<secondary_account_2>;

-- =============================================================================
-- 3. SECONDARY ACCOUNT SETUP — Run on each secondary account
-- =============================================================================

-- ON SECONDARY ACCOUNT 1 (e.g., Azure westeurope):
-- CREATE FAILOVER GROUP IF NOT EXISTS FG_DATA_PLATFORM
--     AS REPLICA OF <org_name>.<primary_account>.FG_DATA_PLATFORM;

-- ON SECONDARY ACCOUNT 2 (e.g., GCP us-central1):
-- CREATE FAILOVER GROUP IF NOT EXISTS FG_DATA_PLATFORM
--     AS REPLICA OF <org_name>.<primary_account>.FG_DATA_PLATFORM;

-- =============================================================================
-- 4. TIERED REPLICATION FREQUENCY (FinOps optimization)
-- Blueprint: "Critical Tier 1 Data Vault Hubs replicate every 10 min,
--            massive historical archives replicate only once daily"
-- =============================================================================

-- Tier 1: Critical — Every 10 minutes (Hubs, active Satellites)
-- RAW_VAULT has the most aggressive replication for minimal RPO.

CREATE REPLICATION GROUP IF NOT EXISTS RG_TIER1_CRITICAL
    OBJECT_TYPES = DATABASES
    ALLOWED_DATABASES = RAW_VAULT
    ALLOWED_ACCOUNTS = <org_name>.<secondary_account_1>, <org_name>.<secondary_account_2>
    REPLICATION_SCHEDULE = 'USING CRON */10 * * * * UTC'
    COMMENT = 'Tier 1: Critical data — 10-minute replication for minimal RPO';

-- Tier 3: Archive — Daily (Reference data, historical aggregates)
-- Reduces cross-region egress costs for high-volume, low-urgency data.

CREATE REPLICATION GROUP IF NOT EXISTS RG_TIER3_ARCHIVE
    OBJECT_TYPES = DATABASES
    ALLOWED_DATABASES = AUDIT
    ALLOWED_ACCOUNTS = <org_name>.<secondary_account_1>, <org_name>.<secondary_account_2>
    REPLICATION_SCHEDULE = 'USING CRON 0 3 * * * UTC'
    COMMENT = 'Tier 3: Archive — daily replication to reduce egress costs';

-- =============================================================================
-- 5. CONTINUITY STRATEGY RUNBOOKS
-- Blueprint defines 3 strategies based on RTO/RPO requirements.
-- =============================================================================

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │ STRATEGY 1: READS BEFORE WRITES                                        │
-- │ Objective: Dashboard continuity during brief outages                    │
-- │ RPO: Minutes  |  RTO: Seconds                                          │
-- ├─────────────────────────────────────────────────────────────────────────┤
-- │                                                                         │
-- │ Step 1: Redirect clients to secondary read-only replicas                │
-- │   ALTER CONNECTION DATA_PLATFORM_CONNECTION                             │
-- │       PRIMARY ACCOUNT <secondary_account>;                              │
-- │                                                                         │
-- │ Step 2: BI tools now query read-only replicated data                    │
-- │   (Ingestion pipelines remain paused)                                   │
-- │                                                                         │
-- │ Step 3: When primary recovers, revert:                                  │
-- │   ALTER CONNECTION DATA_PLATFORM_CONNECTION                             │
-- │       PRIMARY ACCOUNT <primary_account>;                                │
-- │                                                                         │
-- └─────────────────────────────────────────────────────────────────────────┘

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │ STRATEGY 2: WRITES BEFORE READS                                        │
-- │ Objective: Data integrity first — zero data loss                        │
-- │ RPO: Zero  |  RTO: Minutes-to-hours                                    │
-- ├─────────────────────────────────────────────────────────────────────────┤
-- │                                                                         │
-- │ Step 1: Promote secondary failover group to primary:                    │
-- │   -- (Run on SECONDARY account)                                         │
-- │   ALTER FAILOVER GROUP FG_DATA_PLATFORM PRIMARY;                        │
-- │                                                                         │
-- │ Step 2: Run reconciliation ETL to catch missing records:                │
-- │   -- Kafka connector resumes from last committed offset token           │
-- │   -- Snowpipe replays files from last COPY_HISTORY checkpoint           │
-- │   -- Dagster CDC sensor detects streams with pending data               │
-- │                                                                         │
-- │ Step 3: Once caught up, redirect clients:                               │
-- │   ALTER CONNECTION DATA_PLATFORM_CONNECTION                             │
-- │       PRIMARY ACCOUNT <secondary_account>;                              │
-- │                                                                         │
-- │ Step 4: Failback when original primary recovers:                        │
-- │   -- Refresh FG on original primary                                     │
-- │   ALTER FAILOVER GROUP FG_DATA_PLATFORM REFRESH;                        │
-- │   -- Promote back                                                       │
-- │   ALTER FAILOVER GROUP FG_DATA_PLATFORM PRIMARY;                        │
-- │   -- Revert client redirect                                             │
-- │   ALTER CONNECTION DATA_PLATFORM_CONNECTION                             │
-- │       PRIMARY ACCOUNT <primary_account>;                                │
-- │                                                                         │
-- └─────────────────────────────────────────────────────────────────────────┘

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │ STRATEGY 3: SIMULTANEOUS FAILOVER                                      │
-- │ Objective: Near-zero RTO for both reads and writes                      │
-- │ RPO: Seconds (most aggressive)  |  RTO: Near-zero                     │
-- ├─────────────────────────────────────────────────────────────────────────┤
-- │                                                                         │
-- │ Step 1: Execute Client Redirect AND promote failover group concurrently │
-- │   -- (Both run on SECONDARY account simultaneously)                     │
-- │   ALTER CONNECTION DATA_PLATFORM_CONNECTION                             │
-- │       PRIMARY ACCOUNT <secondary_account>;                              │
-- │   ALTER FAILOVER GROUP FG_DATA_PLATFORM PRIMARY;                        │
-- │                                                                         │
-- │ Step 2: Consumers MAY query slightly stale data while Kafka/Snowpipe    │
-- │         offsets are automatically resolved by the streaming connector.   │
-- │                                                                         │
-- │ Step 3: Dagster replication_lag_sensor detects the failover event       │
-- │         and automatically adjusts pipeline orchestration.               │
-- │                                                                         │
-- │ Step 4: Standard failback (same as Strategy 2, Steps 4).               │
-- │                                                                         │
-- └─────────────────────────────────────────────────────────────────────────┘

-- =============================================================================
-- 6. REPLICATION MONITORING VIEWS
-- =============================================================================

CREATE OR REPLACE VIEW AUDIT.CONTROL.REPLICATION_HEALTH AS
SELECT
    REPLICATION_GROUP_NAME,
    PHASE_1_BEGIN         AS REFRESH_START,
    PHASE_4_FINALIZING_END AS REFRESH_END,
    DATEDIFF('second', PHASE_1_BEGIN, PHASE_4_FINALIZING_END) AS LAG_SECONDS,
    BYTES_TRANSFERRED,
    OBJECT_COUNT,
    CASE
        WHEN DATEDIFF('second', PHASE_1_BEGIN, PHASE_4_FINALIZING_END) <= 300
            THEN 'HEALTHY'
        WHEN DATEDIFF('second', PHASE_1_BEGIN, PHASE_4_FINALIZING_END) <= 3600
            THEN 'WARNING'
        ELSE 'CRITICAL'
    END AS SLA_STATUS
FROM TABLE(INFORMATION_SCHEMA.REPLICATION_GROUP_REFRESH_HISTORY())
ORDER BY PHASE_4_FINALIZING_END DESC;

GRANT SELECT ON VIEW AUDIT.CONTROL.REPLICATION_HEALTH TO ROLE PLATFORM_ADMIN;
GRANT SELECT ON VIEW AUDIT.CONTROL.REPLICATION_HEALTH TO ROLE DATA_ENGINEER;
