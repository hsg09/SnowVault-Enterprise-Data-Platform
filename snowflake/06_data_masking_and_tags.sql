-- =============================================================================
-- 06_data_masking_and_tags.sql — Object Tags & Dynamic Masking Policies
-- Data Vault 2.0 Snowflake Data Platform
--
-- PURPOSE: Implement tag-based governance with dynamic data masking for PII
--          and sensitive data. Tags drive automatic policy assignment.
--
-- EXECUTION ORDER: Run AFTER 01_databases_and_schemas.sql
-- REQUIRES: ACCOUNTADMIN or DATA_STEWARD with appropriate permissions
-- =============================================================================

USE ROLE ACCOUNTADMIN;

-- =============================================================================
-- 1. OBJECT TAGS
-- =============================================================================

-- Governance schema for tags and policies
CREATE SCHEMA IF NOT EXISTS RAW_VAULT.GOVERNANCE;

-- PII classification tag
CREATE TAG IF NOT EXISTS RAW_VAULT.GOVERNANCE.PII
    ALLOWED_VALUES 'EMAIL', 'PHONE', 'SSN', 'NAME', 'ADDRESS', 'DOB', 'NONE'
    COMMENT = 'PII classification — drives automatic masking policy assignment';

-- Data sensitivity level
CREATE TAG IF NOT EXISTS RAW_VAULT.GOVERNANCE.SENSITIVITY_LEVEL
    ALLOWED_VALUES 'PUBLIC', 'INTERNAL', 'CONFIDENTIAL', 'RESTRICTED'
    COMMENT = 'Data sensitivity tier — governs access and masking behaviour';

-- Data domain (for lineage and ownership)
CREATE TAG IF NOT EXISTS RAW_VAULT.GOVERNANCE.DATA_DOMAIN
    ALLOWED_VALUES 'CUSTOMER', 'ORDER', 'PRODUCT', 'FINANCIAL', 'MARKETING', 'OPERATIONAL'
    COMMENT = 'Business domain classification — used for lineage and ownership';

-- Cost center (for credit attribution)
CREATE TAG IF NOT EXISTS RAW_VAULT.GOVERNANCE.COST_CENTER
    ALLOWED_VALUES 'ENGINEERING', 'DATA_SCIENCE', 'BI_ANALYTICS', 'FINANCE', 'MARKETING'
    COMMENT = 'Cost center tag — drives credit usage attribution';

-- Data retention class
CREATE TAG IF NOT EXISTS RAW_VAULT.GOVERNANCE.RETENTION_CLASS
    ALLOWED_VALUES 'EPHEMERAL', 'SHORT_TERM', 'STANDARD', 'LONG_TERM', 'REGULATORY'
    COMMENT = 'Retention class — governs Time Travel and fail-safe retention';

-- =============================================================================
-- 2. DYNAMIC MASKING POLICIES
-- =============================================================================

-- Email masking: analysts see masked, engineers see full
CREATE MASKING POLICY IF NOT EXISTS RAW_VAULT.GOVERNANCE.MASK_EMAIL
    AS (VAL STRING) RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() IN ('DATA_ENGINEER', 'PLATFORM_ADMIN', 'ACCOUNTADMIN')
            THEN VAL
        WHEN CURRENT_ROLE() = 'DATA_STEWARD'
            THEN VAL
        ELSE
            REGEXP_REPLACE(VAL, '^(.{2})(.*)(@.*)$', '\\1***\\3')
    END
    COMMENT = 'Email masking — shows first 2 chars + domain to non-privileged roles';

-- Phone masking: show last 4 digits only
CREATE MASKING POLICY IF NOT EXISTS RAW_VAULT.GOVERNANCE.MASK_PHONE
    AS (VAL STRING) RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() IN ('DATA_ENGINEER', 'PLATFORM_ADMIN', 'ACCOUNTADMIN')
            THEN VAL
        WHEN CURRENT_ROLE() = 'DATA_STEWARD'
            THEN VAL
        ELSE
            CONCAT('***-***-', RIGHT(VAL, 4))
    END
    COMMENT = 'Phone masking — shows last 4 digits to non-privileged roles';

-- Name masking: first initial + asterisks
CREATE MASKING POLICY IF NOT EXISTS RAW_VAULT.GOVERNANCE.MASK_NAME
    AS (VAL STRING) RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() IN ('DATA_ENGINEER', 'PLATFORM_ADMIN', 'ACCOUNTADMIN', 'DATA_STEWARD')
            THEN VAL
        ELSE
            CONCAT(LEFT(VAL, 1), '****')
    END
    COMMENT = 'Name masking — shows first initial to non-privileged roles';

-- SSN / National ID masking: full redaction for most roles
CREATE MASKING POLICY IF NOT EXISTS RAW_VAULT.GOVERNANCE.MASK_SSN
    AS (VAL STRING) RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() IN ('PLATFORM_ADMIN', 'ACCOUNTADMIN')
            THEN VAL
        ELSE
            '***-**-****'
    END
    COMMENT = 'SSN masking — full redaction except PLATFORM_ADMIN';

-- Date masking (DOB): year only for analysts
CREATE MASKING POLICY IF NOT EXISTS RAW_VAULT.GOVERNANCE.MASK_DOB
    AS (VAL DATE) RETURNS DATE ->
    CASE
        WHEN CURRENT_ROLE() IN ('DATA_ENGINEER', 'PLATFORM_ADMIN', 'ACCOUNTADMIN', 'DATA_STEWARD')
            THEN VAL
        ELSE
            DATE_FROM_PARTS(YEAR(VAL), 1, 1)
    END
    COMMENT = 'DOB masking — shows year only (Jan 1) to non-privileged roles';

-- Address masking: city/state only
CREATE MASKING POLICY IF NOT EXISTS RAW_VAULT.GOVERNANCE.MASK_ADDRESS
    AS (VAL STRING) RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() IN ('DATA_ENGINEER', 'PLATFORM_ADMIN', 'ACCOUNTADMIN', 'DATA_STEWARD')
            THEN VAL
        ELSE
            '*** REDACTED ***'
    END
    COMMENT = 'Address masking — full redaction for non-privileged roles';

-- =============================================================================
-- 3. TAG-BASED MASKING ASSIGNMENT
-- Apply masking policies to columns tagged with PII types.
-- =============================================================================

-- Apply tags to raw landing table columns
ALTER TABLE RAW_VAULT.ECOMMERCE.RAW_CUSTOMERS MODIFY COLUMN
    EMAIL           SET TAG RAW_VAULT.GOVERNANCE.PII = 'EMAIL',
    PHONE           SET TAG RAW_VAULT.GOVERNANCE.PII = 'PHONE',
    FIRST_NAME      SET TAG RAW_VAULT.GOVERNANCE.PII = 'NAME',
    LAST_NAME       SET TAG RAW_VAULT.GOVERNANCE.PII = 'NAME',
    DATE_OF_BIRTH   SET TAG RAW_VAULT.GOVERNANCE.PII = 'DOB';

-- Apply masking policies to tagged columns
ALTER TABLE RAW_VAULT.ECOMMERCE.RAW_CUSTOMERS MODIFY COLUMN
    EMAIL           SET MASKING POLICY RAW_VAULT.GOVERNANCE.MASK_EMAIL,
    PHONE           SET MASKING POLICY RAW_VAULT.GOVERNANCE.MASK_PHONE,
    FIRST_NAME      SET MASKING POLICY RAW_VAULT.GOVERNANCE.MASK_NAME,
    LAST_NAME       SET MASKING POLICY RAW_VAULT.GOVERNANCE.MASK_NAME,
    DATE_OF_BIRTH   SET MASKING POLICY RAW_VAULT.GOVERNANCE.MASK_DOB;

-- Tag tables with domain and sensitivity
ALTER TABLE RAW_VAULT.ECOMMERCE.RAW_CUSTOMERS SET TAG
    RAW_VAULT.GOVERNANCE.DATA_DOMAIN = 'CUSTOMER',
    RAW_VAULT.GOVERNANCE.SENSITIVITY_LEVEL = 'CONFIDENTIAL';

ALTER TABLE RAW_VAULT.ECOMMERCE.RAW_ORDERS SET TAG
    RAW_VAULT.GOVERNANCE.DATA_DOMAIN = 'ORDER',
    RAW_VAULT.GOVERNANCE.SENSITIVITY_LEVEL = 'INTERNAL';

ALTER TABLE RAW_VAULT.ECOMMERCE.RAW_PRODUCTS SET TAG
    RAW_VAULT.GOVERNANCE.DATA_DOMAIN = 'PRODUCT',
    RAW_VAULT.GOVERNANCE.SENSITIVITY_LEVEL = 'INTERNAL';

ALTER TABLE RAW_VAULT.ECOMMERCE.RAW_ORDER_ITEMS SET TAG
    RAW_VAULT.GOVERNANCE.DATA_DOMAIN = 'ORDER',
    RAW_VAULT.GOVERNANCE.SENSITIVITY_LEVEL = 'INTERNAL';

-- =============================================================================
-- 4. GRANTS
-- =============================================================================

GRANT USAGE ON SCHEMA RAW_VAULT.GOVERNANCE TO ROLE DATA_STEWARD;
GRANT APPLY TAG ON ACCOUNT TO ROLE DATA_STEWARD;
GRANT APPLY MASKING POLICY ON ACCOUNT TO ROLE DATA_STEWARD;
