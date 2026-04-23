-- =============================================================================
-- 10_backup_and_retention.sql — Time Travel, Fail-Safe & Backup Strategy
-- Data Vault 2.0 Snowflake Data Platform
--
-- PURPOSE: Configure data retention policies per environment and layer.
--          Create backup tables with retention lock for critical datasets.
--
-- EXECUTION ORDER: Run AFTER all databases and tables are created
-- REQUIRES: PLATFORM_ADMIN or ACCOUNTADMIN
-- =============================================================================

USE ROLE PLATFORM_ADMIN;

-- =============================================================================
-- 1. TIME TRAVEL RETENTION — PER DATABASE
-- =============================================================================

-- Bronze (Raw Vault) — 7 days standard, 90 days for production
ALTER DATABASE RAW_VAULT SET DATA_RETENTION_TIME_IN_DAYS = 7;

-- Silver (Business Vault) — 7 days standard
ALTER DATABASE BUSINESS_VAULT SET DATA_RETENTION_TIME_IN_DAYS = 7;

-- Gold (Analytics) — 14 days for BI impact analysis
ALTER DATABASE ANALYTICS SET DATA_RETENTION_TIME_IN_DAYS = 14;

-- Audit — 90 days for compliance and investigation
ALTER DATABASE AUDIT SET DATA_RETENTION_TIME_IN_DAYS = 90;

-- =============================================================================
-- 2. SCHEMA-LEVEL RETENTION OVERRIDES
-- =============================================================================

-- Hub tables — extended retention (core structural entities, rare changes)
ALTER SCHEMA RAW_VAULT.RAW_VAULT SET DATA_RETENTION_TIME_IN_DAYS = 30;

-- Landing schemas — short retention (raw files are re-loadable from S3)
ALTER SCHEMA RAW_VAULT.ECOMMERCE SET DATA_RETENTION_TIME_IN_DAYS = 3;
ALTER SCHEMA RAW_VAULT.CRM       SET DATA_RETENTION_TIME_IN_DAYS = 3;
ALTER SCHEMA RAW_VAULT.CDC       SET DATA_RETENTION_TIME_IN_DAYS = 3;
ALTER SCHEMA RAW_VAULT.STREAMING SET DATA_RETENTION_TIME_IN_DAYS = 1;

-- DQ results — longer retention for trend analysis
ALTER SCHEMA AUDIT.DQ_RESULTS SET DATA_RETENTION_TIME_IN_DAYS = 90;

-- =============================================================================
-- 3. BACKUP SCHEMA (Point-in-Time Snapshots)
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS AUDIT.BACKUPS
    DATA_RETENTION_TIME_IN_DAYS = 90
    COMMENT = 'Manual point-in-time backup snapshots of critical tables';

-- =============================================================================
-- 4. BACKUP PROCEDURES
-- =============================================================================

-- Procedure to create a timestamped backup of any table
CREATE OR REPLACE PROCEDURE AUDIT.BACKUPS.CREATE_TABLE_BACKUP(
    SOURCE_DATABASE VARCHAR,
    SOURCE_SCHEMA   VARCHAR,
    SOURCE_TABLE    VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'Creates a timestamped backup clone of the specified table'
AS
DECLARE
    backup_name VARCHAR;
    backup_sql  VARCHAR;
BEGIN
    backup_name := SOURCE_TABLE || '_BACKUP_' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS');
    backup_sql  := 'CREATE TABLE AUDIT.BACKUPS.' || :backup_name ||
                   ' CLONE ' || SOURCE_DATABASE || '.' || SOURCE_SCHEMA || '.' || SOURCE_TABLE;
    EXECUTE IMMEDIATE :backup_sql;
    RETURN 'Backup created: AUDIT.BACKUPS.' || :backup_name;
END;

-- Procedure to restore from a point in time (using Time Travel)
CREATE OR REPLACE PROCEDURE AUDIT.BACKUPS.RESTORE_TABLE_AT_TIMESTAMP(
    TARGET_DATABASE VARCHAR,
    TARGET_SCHEMA   VARCHAR,
    TARGET_TABLE    VARCHAR,
    RESTORE_TIMESTAMP VARCHAR       -- ISO 8601 format: '2026-01-15 10:30:00'
)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'Restores a table from Time Travel at the specified timestamp'
AS
DECLARE
    restore_sql VARCHAR;
    backup_sql  VARCHAR;
    backup_name VARCHAR;
BEGIN
    -- First, backup current state
    backup_name := TARGET_TABLE || '_PRE_RESTORE_' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS');
    backup_sql  := 'CREATE TABLE AUDIT.BACKUPS.' || :backup_name ||
                   ' CLONE ' || TARGET_DATABASE || '.' || TARGET_SCHEMA || '.' || TARGET_TABLE;
    EXECUTE IMMEDIATE :backup_sql;

    -- Then restore from Time Travel
    restore_sql := 'CREATE OR REPLACE TABLE ' || TARGET_DATABASE || '.' || TARGET_SCHEMA || '.' || TARGET_TABLE ||
                   ' CLONE ' || TARGET_DATABASE || '.' || TARGET_SCHEMA || '.' || TARGET_TABLE ||
                   ' AT (TIMESTAMP => ''' || RESTORE_TIMESTAMP || '''::TIMESTAMP_NTZ)';
    EXECUTE IMMEDIATE :restore_sql;
    RETURN 'Restored: ' || TARGET_TABLE || ' to ' || RESTORE_TIMESTAMP ||
           '. Pre-restore backup: AUDIT.BACKUPS.' || :backup_name;
END;

-- =============================================================================
-- 5. AUTOMATED BACKUP TASK (Weekly snapshots of critical tables)
-- =============================================================================

CREATE TASK IF NOT EXISTS AUDIT.BACKUPS.TASK_WEEKLY_CRITICAL_BACKUP
    WAREHOUSE = DEV_WH
    SCHEDULE  = 'USING CRON 0 2 * * 0 UTC'     -- Sunday 2 AM UTC
    COMMENT   = 'Weekly backup of critical hub and dimension tables'
AS
BEGIN
    -- Hub backups
    CALL AUDIT.BACKUPS.CREATE_TABLE_BACKUP('RAW_VAULT', 'RAW_VAULT', 'HUB_CUSTOMER');
    CALL AUDIT.BACKUPS.CREATE_TABLE_BACKUP('RAW_VAULT', 'RAW_VAULT', 'HUB_ORDER');
    CALL AUDIT.BACKUPS.CREATE_TABLE_BACKUP('RAW_VAULT', 'RAW_VAULT', 'HUB_PRODUCT');

    -- Gold dimension backups
    CALL AUDIT.BACKUPS.CREATE_TABLE_BACKUP('ANALYTICS', 'DIMENSIONS', 'DIM_CUSTOMER');
    CALL AUDIT.BACKUPS.CREATE_TABLE_BACKUP('ANALYTICS', 'DIMENSIONS', 'DIM_PRODUCT');
END;

-- =============================================================================
-- 6. CLEANUP TASK (Remove backups older than 90 days)
-- =============================================================================

CREATE TASK IF NOT EXISTS AUDIT.BACKUPS.TASK_CLEANUP_OLD_BACKUPS
    WAREHOUSE = DEV_WH
    SCHEDULE  = 'USING CRON 0 3 1 * * UTC'     -- 1st of month, 3 AM UTC
    COMMENT   = 'Monthly cleanup of backup tables older than 90 days'
AS
    -- Dynamically drop backup tables older than 90 days
    -- Uses INFORMATION_SCHEMA to find tables with _BACKUP_ in the name
    EXECUTE IMMEDIATE $$
    DECLARE
        c1 CURSOR FOR
            SELECT TABLE_NAME
            FROM AUDIT.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = 'BACKUPS'
              AND TABLE_NAME LIKE '%_BACKUP_%'
              AND CREATED < DATEADD('day', -90, CURRENT_DATE());
        table_to_drop VARCHAR;
    BEGIN
        FOR record IN c1 DO
            table_to_drop := record.TABLE_NAME;
            EXECUTE IMMEDIATE 'DROP TABLE IF EXISTS AUDIT.BACKUPS.' || table_to_drop;
        END FOR;
    END;
    $$;

-- =============================================================================
-- 7. GRANTS
-- =============================================================================
GRANT USAGE ON SCHEMA AUDIT.BACKUPS TO ROLE DATA_ENGINEER;
GRANT USAGE ON PROCEDURE AUDIT.BACKUPS.CREATE_TABLE_BACKUP(VARCHAR, VARCHAR, VARCHAR) TO ROLE DATA_ENGINEER;
GRANT USAGE ON PROCEDURE AUDIT.BACKUPS.RESTORE_TABLE_AT_TIMESTAMP(VARCHAR, VARCHAR, VARCHAR, VARCHAR) TO ROLE DATA_ENGINEER;
