-- =============================================================================
-- 13_row_access_and_classification.sql — Row Access Policies + Data Classification
-- Data Vault 2.0 Snowflake Data Platform
--
-- PURPOSE: Implement Row Access Policies (restrict row visibility by role)
--          and automated Data Classification (Snowflake CLASSIFY).
--
-- BLUEPRINT: "Through Horizon, data stewards define Row Access Policies,
--            Tag-Based Column Masking, and Data Classification rules exactly once.
--            These policies propagate automatically across all regions."
--
-- EXECUTION ORDER: Run AFTER 06_data_masking_and_tags.sql
-- REQUIRES: ACCOUNTADMIN or DATA_STEWARD
-- =============================================================================

USE ROLE ACCOUNTADMIN;

-- =============================================================================
-- 1. ROW ACCESS POLICIES (Data Filtering by Role)
-- =============================================================================

-- Country-based row access: Analysts only see data for their assigned region
CREATE ROW ACCESS POLICY IF NOT EXISTS RAW_VAULT.GOVERNANCE.RAP_COUNTRY_FILTER
    AS (COUNTRY_CODE VARCHAR) RETURNS BOOLEAN ->
    CASE
        -- Platform admins and engineers see all data
        WHEN CURRENT_ROLE() IN ('PLATFORM_ADMIN', 'DATA_ENGINEER', 'ACCOUNTADMIN')
            THEN TRUE
        -- Data stewards see all data (for governance review)
        WHEN CURRENT_ROLE() = 'DATA_STEWARD'
            THEN TRUE
        -- Analysts see only their region (mapped via session variable)
        WHEN CURRENT_ROLE() = 'ANALYST'
            AND COUNTRY_CODE IN (
                SELECT VALUE FROM TABLE(FLATTEN(
                    INPUT => PARSE_JSON(
                        COALESCE(CURRENT_SESSION()::VARIANT:allowed_countries, '["ALL"]')
                    )
                ))
            )
            THEN TRUE
        -- Transformers see all data (for pipeline processing)
        WHEN CURRENT_ROLE() = 'TRANSFORMER'
            THEN TRUE
        ELSE FALSE
    END
    COMMENT = 'Row Access Policy: Country-based data filtering for GDPR/data residency';

-- Sensitivity-based row access: Restrict CONFIDENTIAL rows
CREATE ROW ACCESS POLICY IF NOT EXISTS RAW_VAULT.GOVERNANCE.RAP_SENSITIVITY_FILTER
    AS (SENSITIVITY_LEVEL VARCHAR) RETURNS BOOLEAN ->
    CASE
        WHEN CURRENT_ROLE() IN ('PLATFORM_ADMIN', 'DATA_STEWARD', 'ACCOUNTADMIN')
            THEN TRUE
        WHEN CURRENT_ROLE() = 'DATA_ENGINEER'
            THEN TRUE
        WHEN CURRENT_ROLE() = 'ANALYST'
            AND SENSITIVITY_LEVEL IN ('PUBLIC', 'INTERNAL')
            THEN TRUE
        ELSE FALSE
    END
    COMMENT = 'Row Access Policy: Sensitivity-based filtering — CONFIDENTIAL restricted';

-- Apply RAP to customer dimension (country-based)
-- ALTER TABLE ANALYTICS.DIMENSIONS.DIM_CUSTOMER
--     ADD ROW ACCESS POLICY RAW_VAULT.GOVERNANCE.RAP_COUNTRY_FILTER
--     ON (COUNTRY_CODE);

-- =============================================================================
-- 2. AUTOMATED DATA CLASSIFICATION (Snowflake CLASSIFY)
-- =============================================================================

-- Run classification on all raw landing tables to auto-detect PII
-- This uses Snowflake's built-in ML-based classification engine

-- Classify RAW_CUSTOMERS
CALL SYSTEM$CLASSIFY('RAW_VAULT.ECOMMERCE.RAW_CUSTOMERS', {
    'auto_tag': TRUE,
    'custom_classifiers': {
        'EMAIL': {'regex_pattern': '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$'},
        'PHONE': {'regex_pattern': '^\\+?[1-9]\\d{1,14}$'},
        'SSN':   {'regex_pattern': '^\\d{3}-\\d{2}-\\d{4}$'}
    }
});

-- Classify RAW_ORDERS
CALL SYSTEM$CLASSIFY('RAW_VAULT.ECOMMERCE.RAW_ORDERS', {
    'auto_tag': TRUE
});

-- Classify RAW_PRODUCTS  
CALL SYSTEM$CLASSIFY('RAW_VAULT.ECOMMERCE.RAW_PRODUCTS', {
    'auto_tag': TRUE
});

-- Classify RAW_ORDER_ITEMS
CALL SYSTEM$CLASSIFY('RAW_VAULT.ECOMMERCE.RAW_ORDER_ITEMS', {
    'auto_tag': TRUE
});

-- =============================================================================
-- 3. CLASSIFICATION RESULTS AUDIT
-- =============================================================================

-- View classification results and auto-applied tags
-- SELECT *
-- FROM TABLE(INFORMATION_SCHEMA.TAG_REFERENCES(
--     'RAW_VAULT.ECOMMERCE.RAW_CUSTOMERS', 'TABLE'
-- ));

-- View all classified columns across the estate
CREATE OR REPLACE VIEW AUDIT.CONTROL.DATA_CLASSIFICATION_REPORT AS
SELECT
    TAG_DATABASE,
    TAG_SCHEMA,
    TAG_NAME,
    OBJECT_DATABASE,
    OBJECT_SCHEMA,
    OBJECT_NAME,
    COLUMN_NAME,
    TAG_VALUE,
    'AUTO_CLASSIFIED' AS CLASSIFICATION_METHOD
FROM SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES
WHERE TAG_NAME IN ('PII', 'SENSITIVITY_LEVEL', 'SEMANTIC_CATEGORY')
    AND DELETED IS NULL
ORDER BY OBJECT_DATABASE, OBJECT_SCHEMA, OBJECT_NAME, COLUMN_NAME;

-- =============================================================================
-- 4. GRANTS
-- =============================================================================

GRANT APPLY ROW ACCESS POLICY ON ACCOUNT TO ROLE DATA_STEWARD;
GRANT SELECT ON VIEW AUDIT.CONTROL.DATA_CLASSIFICATION_REPORT TO ROLE DATA_STEWARD;
GRANT SELECT ON VIEW AUDIT.CONTROL.DATA_CLASSIFICATION_REPORT TO ROLE DATA_ENGINEER;
