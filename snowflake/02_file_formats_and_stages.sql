-- =============================================================================
-- 02_file_formats_and_stages.sql — File Formats, Storage Integrations & Stages
-- Data Vault 2.0 Snowflake Data Platform
--
-- PURPOSE: Define file formats and external stages for multi-cloud ingestion.
--          AWS (S3) is primary; Azure (ADLS) and GCP (GCS) are prepared.
--
-- EXECUTION ORDER: Run AFTER 01_databases_and_schemas.sql
-- REQUIRES: PLATFORM_ADMIN or ACCOUNTADMIN
-- =============================================================================

USE ROLE PLATFORM_ADMIN;
USE DATABASE RAW_VAULT;

-- =============================================================================
-- 1. FILE FORMATS
-- =============================================================================

-- CSV with header, pipe-delimited (common ERP/legacy exports)
CREATE FILE FORMAT IF NOT EXISTS RAW_VAULT.ECOMMERCE.FF_CSV_PIPE
    TYPE                 = 'CSV'
    FIELD_DELIMITER      = '|'
    RECORD_DELIMITER     = '\n'
    SKIP_HEADER          = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF              = ('NULL', 'null', '', 'NA', 'N/A')
    EMPTY_FIELD_AS_NULL  = TRUE
    TRIM_SPACE           = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    COMMENT              = 'Pipe-delimited CSV with header — ERP/legacy batch files';

-- CSV with header, comma-delimited (standard exports)
CREATE FILE FORMAT IF NOT EXISTS RAW_VAULT.ECOMMERCE.FF_CSV_COMMA
    TYPE                 = 'CSV'
    FIELD_DELIMITER      = ','
    RECORD_DELIMITER     = '\n'
    SKIP_HEADER          = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF              = ('NULL', 'null', '', 'NA', 'N/A')
    EMPTY_FIELD_AS_NULL  = TRUE
    TRIM_SPACE           = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    COMMENT              = 'Comma-delimited CSV with header — standard batch files';

-- JSON (semi-structured API responses, event payloads)
CREATE FILE FORMAT IF NOT EXISTS RAW_VAULT.ECOMMERCE.FF_JSON
    TYPE                    = 'JSON'
    STRIP_OUTER_ARRAY       = TRUE
    STRIP_NULL_VALUES       = FALSE
    IGNORE_UTF8_ERRORS      = TRUE
    ALLOW_DUPLICATE         = FALSE
    COMMENT                 = 'JSON — API responses, event payloads';

-- Parquet (columnar data lake files)
CREATE FILE FORMAT IF NOT EXISTS RAW_VAULT.ECOMMERCE.FF_PARQUET
    TYPE                    = 'PARQUET'
    SNAPPY_COMPRESSION      = TRUE
    COMMENT                 = 'Parquet — columnar data lake files';

-- Avro (Kafka / schema registry payloads)
CREATE FILE FORMAT IF NOT EXISTS RAW_VAULT.STREAMING.FF_AVRO
    TYPE                    = 'AVRO'
    COMMENT                 = 'Avro — Kafka payloads, schema registry';

-- =============================================================================
-- 2. STORAGE INTEGRATIONS
-- =============================================================================

-- AWS S3 Storage Integration (primary)
CREATE STORAGE INTEGRATION IF NOT EXISTS S3_RAW_INTEGRATION
    TYPE                      = EXTERNAL_STAGE
    STORAGE_PROVIDER          = 'S3'
    ENABLED                   = TRUE
    STORAGE_AWS_ROLE_ARN      = 'arn:aws:iam::role/snowflake-data-lake-access'  -- Replace with actual ARN
    STORAGE_ALLOWED_LOCATIONS = (
        's3://your-company-data-lake-raw/',
        's3://your-company-data-lake-raw-eu/'
    )
    STORAGE_BLOCKED_LOCATIONS = (
        's3://your-company-data-lake-raw/restricted/'
    )
    COMMENT = 'AWS S3 integration — primary data lake raw bucket';

-- Azure ADLS Gen2 (prepared for Phase 2 multi-cloud)
-- CREATE STORAGE INTEGRATION IF NOT EXISTS ADLS_RAW_INTEGRATION
--     TYPE                      = EXTERNAL_STAGE
--     STORAGE_PROVIDER          = 'AZURE'
--     ENABLED                   = TRUE
--     AZURE_TENANT_ID           = '<your-tenant-id>'
--     STORAGE_ALLOWED_LOCATIONS = (
--         'azure://yourstorageaccount.blob.core.windows.net/raw/'
--     )
--     COMMENT = 'Azure ADLS Gen2 integration — secondary data lake';

-- GCP GCS (prepared for Phase 2 multi-cloud)
-- CREATE STORAGE INTEGRATION IF NOT EXISTS GCS_RAW_INTEGRATION
--     TYPE                      = EXTERNAL_STAGE
--     STORAGE_PROVIDER          = 'GCS'
--     ENABLED                   = TRUE
--     STORAGE_ALLOWED_LOCATIONS = (
--         'gcs://your-gcs-data-lake-raw/'
--     )
--     COMMENT = 'GCP GCS integration — tertiary data lake';

-- =============================================================================
-- 3. EXTERNAL STAGES
-- =============================================================================

-- AWS S3 — E-Commerce batch files (CSV, JSON, Parquet)
CREATE STAGE IF NOT EXISTS RAW_VAULT.ECOMMERCE.STG_S3_ECOMMERCE
    STORAGE_INTEGRATION = S3_RAW_INTEGRATION
    URL                 = 's3://your-company-data-lake-raw/ecommerce/'
    FILE_FORMAT         = RAW_VAULT.ECOMMERCE.FF_CSV_COMMA
    COMMENT             = 'S3 external stage — E-Commerce source files';

-- AWS S3 — JSON event payloads
CREATE STAGE IF NOT EXISTS RAW_VAULT.ECOMMERCE.STG_S3_EVENTS
    STORAGE_INTEGRATION = S3_RAW_INTEGRATION
    URL                 = 's3://your-company-data-lake-raw/events/'
    FILE_FORMAT         = RAW_VAULT.ECOMMERCE.FF_JSON
    COMMENT             = 'S3 external stage — event/API JSON payloads';

-- AWS S3 — Parquet data lake files
CREATE STAGE IF NOT EXISTS RAW_VAULT.ECOMMERCE.STG_S3_PARQUET
    STORAGE_INTEGRATION = S3_RAW_INTEGRATION
    URL                 = 's3://your-company-data-lake-raw/parquet/'
    FILE_FORMAT         = RAW_VAULT.ECOMMERCE.FF_PARQUET
    COMMENT             = 'S3 external stage — Parquet data lake exports';

-- AWS S3 — CRM system files
CREATE STAGE IF NOT EXISTS RAW_VAULT.CRM.STG_S3_CRM
    STORAGE_INTEGRATION = S3_RAW_INTEGRATION
    URL                 = 's3://your-company-data-lake-raw/crm/'
    FILE_FORMAT         = RAW_VAULT.ECOMMERCE.FF_CSV_COMMA
    COMMENT             = 'S3 external stage — CRM source files';

-- =============================================================================
-- 4. GRANTS
-- =============================================================================

-- LOADER needs stage access for Snowpipe
GRANT USAGE ON INTEGRATION S3_RAW_INTEGRATION TO ROLE LOADER;

GRANT USAGE ON STAGE RAW_VAULT.ECOMMERCE.STG_S3_ECOMMERCE TO ROLE LOADER;
GRANT USAGE ON STAGE RAW_VAULT.ECOMMERCE.STG_S3_EVENTS    TO ROLE LOADER;
GRANT USAGE ON STAGE RAW_VAULT.ECOMMERCE.STG_S3_PARQUET   TO ROLE LOADER;
GRANT USAGE ON STAGE RAW_VAULT.CRM.STG_S3_CRM             TO ROLE LOADER;

-- TRANSFORMER needs read access to stages for schema inference
GRANT USAGE ON STAGE RAW_VAULT.ECOMMERCE.STG_S3_ECOMMERCE TO ROLE TRANSFORMER;
GRANT USAGE ON STAGE RAW_VAULT.ECOMMERCE.STG_S3_EVENTS    TO ROLE TRANSFORMER;
GRANT USAGE ON STAGE RAW_VAULT.ECOMMERCE.STG_S3_PARQUET   TO ROLE TRANSFORMER;
GRANT USAGE ON STAGE RAW_VAULT.CRM.STG_S3_CRM             TO ROLE TRANSFORMER;
