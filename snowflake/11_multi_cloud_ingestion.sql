-- =============================================================================
-- 11_multi_cloud_ingestion.sql — Pipelines 2-5 (Azure ADLS, GCP GCS, Kafka, Openflow)
-- Data Vault 2.0 Snowflake Data Platform
--
-- PURPOSE: Complete the 5-pipeline omnichannel ingestion architecture.
--          Pipeline 1 (AWS S3) is in 02_file_formats_and_stages.sql.
--          This script adds Pipelines 2-5 per the enterprise blueprint.
--
-- EXECUTION ORDER: Run AFTER 02_file_formats_and_stages.sql
-- REQUIRES: ACCOUNTADMIN (for storage integrations)
-- =============================================================================

USE ROLE ACCOUNTADMIN;

-- =============================================================================
-- PIPELINE 2: Azure ADLS Gen2 — Storage Integration + Snowpipe
-- (managed identity, Event Grid → Snowpipe)
-- =============================================================================

CREATE STORAGE INTEGRATION IF NOT EXISTS ADLS_RAW_INTEGRATION
    TYPE                      = EXTERNAL_STAGE
    STORAGE_PROVIDER          = 'AZURE'
    ENABLED                   = TRUE
    AZURE_TENANT_ID           = '<your-azure-tenant-id>'  -- Replace
    STORAGE_ALLOWED_LOCATIONS = (
        'azure://yourstorageaccount.blob.core.windows.net/raw/',
        'azure://yourstorageaccount.blob.core.windows.net/raw-eu/'
    )
    COMMENT = 'Pipeline 2: Azure ADLS Gen2 — secondary cloud (managed identity)';

-- Azure external stage
CREATE STAGE IF NOT EXISTS RAW_VAULT.ECOMMERCE.STG_ADLS_ECOMMERCE
    STORAGE_INTEGRATION = ADLS_RAW_INTEGRATION
    URL                 = 'azure://yourstorageaccount.blob.core.windows.net/raw/ecommerce/'
    FILE_FORMAT         = RAW_VAULT.ECOMMERCE.FF_CSV_COMMA
    COMMENT             = 'Azure ADLS Gen2 stage — E-Commerce (Event Grid → Snowpipe)';

-- Azure Snowpipe (auto-ingest via Event Grid notification)
CREATE PIPE IF NOT EXISTS RAW_VAULT.ECOMMERCE.PIPE_ADLS_CUSTOMERS
    AUTO_INGEST = TRUE
    -- INTEGRATION = '<azure-event-grid-notification-integration>'
    COMMENT = 'Pipeline 2: Azure ADLS → RAW_CUSTOMERS (Event Grid trigger)'
    AS
    COPY INTO RAW_VAULT.ECOMMERCE.RAW_CUSTOMERS
    FROM @RAW_VAULT.ECOMMERCE.STG_ADLS_ECOMMERCE/customers/
    FILE_FORMAT = RAW_VAULT.ECOMMERCE.FF_CSV_COMMA
    ON_ERROR = 'SKIP_FILE'
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- =============================================================================
-- PIPELINE 3: GCP GCS — Storage Integration + Snowpipe
-- (service account, Pub/Sub → Snowpipe)
-- =============================================================================

CREATE STORAGE INTEGRATION IF NOT EXISTS GCS_RAW_INTEGRATION
    TYPE                      = EXTERNAL_STAGE
    STORAGE_PROVIDER          = 'GCS'
    ENABLED                   = TRUE
    STORAGE_ALLOWED_LOCATIONS = (
        'gcs://your-gcs-data-lake-raw/',
        'gcs://your-gcs-data-lake-raw-eu/'
    )
    COMMENT = 'Pipeline 3: GCP GCS — tertiary cloud (service account)';

-- GCS external stage
CREATE STAGE IF NOT EXISTS RAW_VAULT.ECOMMERCE.STG_GCS_ECOMMERCE
    STORAGE_INTEGRATION = GCS_RAW_INTEGRATION
    URL                 = 'gcs://your-gcs-data-lake-raw/ecommerce/'
    FILE_FORMAT         = RAW_VAULT.ECOMMERCE.FF_PARQUET
    COMMENT             = 'GCP GCS stage — E-Commerce (Pub/Sub → Snowpipe)';

-- GCS Snowpipe (auto-ingest via Pub/Sub notification)
CREATE PIPE IF NOT EXISTS RAW_VAULT.ECOMMERCE.PIPE_GCS_ARCHIVE
    AUTO_INGEST = TRUE
    -- GCP_PUBSUB_SUBSCRIPTION_NAME = '<your-subscription>'
    COMMENT = 'Pipeline 3: GCS → Archival loads (Pub/Sub trigger)'
    AS
    COPY INTO RAW_VAULT.ECOMMERCE.RAW_CUSTOMERS
    FROM @RAW_VAULT.ECOMMERCE.STG_GCS_ECOMMERCE/customers/
    FILE_FORMAT = RAW_VAULT.ECOMMERCE.FF_PARQUET
    ON_ERROR = 'SKIP_FILE'
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- =============================================================================
-- PIPELINE 4: Kafka — Snowpipe Streaming v2 (Direct Row Ingest)
-- Bypasses cloud storage entirely for sub-second latency.
-- =============================================================================

-- Landing table for Kafka Snowpipe Streaming v2
CREATE TABLE IF NOT EXISTS RAW_VAULT.STREAMING.RAW_KAFKA_EVENTS (
    RECORD_CONTENT          VARIANT         NOT NULL,
    RECORD_METADATA         VARIANT,
    _LOADED_AT              TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    _CHANNEL_NAME           VARCHAR(256),
    _OFFSET_TOKEN           VARCHAR(256),
    _PARTITION_ID           NUMBER,
    _TOPIC_NAME             VARCHAR(256)
)
CLUSTER BY (_LOADED_AT)
DATA_RETENTION_TIME_IN_DAYS = 90
COMMENT = 'Pipeline 4: Kafka Streaming v2 — sub-second direct row ingest';

-- Kafka events stream for CDC processing
CREATE STREAM IF NOT EXISTS RAW_VAULT.STREAMING.STREAM_KAFKA_EVENTS
    ON TABLE RAW_VAULT.STREAMING.RAW_KAFKA_EVENTS
    APPEND_ONLY = TRUE
    SHOW_INITIAL_ROWS = FALSE
    COMMENT = 'CDC stream on Kafka events for incremental processing';

-- =============================================================================
-- Kafka Connector Configuration Reference
-- (deployed externally via Confluent Cloud / self-managed Kafka Connect)
-- =============================================================================
-- {
--   "name": "snowflake-kafka-streaming-connector",
--   "config": {
--     "connector.class": "com.snowflake.kafka.connector.SnowflakeSinkConnector",
--     "tasks.max": "4",
--     "topics": "ecommerce.orders,ecommerce.events,iot.telemetry",
--     "snowflake.url.name": "<account>.snowflakecomputing.com",
--     "snowflake.user.name": "KAFKA_SVC_USER",
--     "snowflake.private.key": "${env:SNOWFLAKE_KAFKA_PRIVATE_KEY}",
--     "snowflake.database.name": "RAW_VAULT",
--     "snowflake.schema.name": "STREAMING",
--     "snowflake.role.name": "LOADER",
--     "snowflake.ingestion.method": "SNOWPIPE_STREAMING",
--     "snowflake.enable.schematization": "true",
--     "buffer.count.records": "10000",
--     "buffer.flush.time": "10",
--     "buffer.size.bytes": "20000000",
--     "key.converter": "org.apache.kafka.connect.storage.StringConverter",
--     "value.converter": "com.snowflake.kafka.connector.records.SnowflakeJsonConverter",
--     "snowflake.streaming.channel.name.format": "ecommerce-prod-{topic}-{partition}",
--     "errors.tolerance": "all",
--     "errors.deadletterqueue.topic.name": "snowflake-dlq",
--     "errors.deadletterqueue.context.headers.enable": "true"
--   }
-- }

-- =============================================================================
-- PIPELINE 5: Snowflake Openflow (Apache NiFi CDC)
-- CDC from operational databases (PostgreSQL, Oracle, SQL Server)
-- =============================================================================

-- CDC landing schema
CREATE SCHEMA IF NOT EXISTS RAW_VAULT.CDC;

-- CDC raw events table (NiFi writes via Snowpipe Streaming)
CREATE TABLE IF NOT EXISTS RAW_VAULT.CDC.RAW_CDC_EVENTS (
    CDC_ID                  VARCHAR(64)     NOT NULL,
    SOURCE_DATABASE         VARCHAR(128)    NOT NULL,
    SOURCE_SCHEMA           VARCHAR(128),
    SOURCE_TABLE            VARCHAR(128)    NOT NULL,
    CDC_OPERATION           VARCHAR(10)     NOT NULL,  -- INSERT, UPDATE, DELETE
    RECORD_BEFORE           VARIANT,                   -- Previous state (for UPDATE/DELETE)
    RECORD_AFTER            VARIANT,                   -- New state (for INSERT/UPDATE)
    CDC_TIMESTAMP           TIMESTAMP_NTZ   NOT NULL,
    TRANSACTION_ID          VARCHAR(64),
    LSN                     VARCHAR(64),               -- Log Sequence Number (PostgreSQL WAL)
    _LOADED_AT              TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    _FLOW_FILE_UUID         VARCHAR(64)                -- NiFi provenance tracking
)
CLUSTER BY (SOURCE_TABLE, _LOADED_AT)
DATA_RETENTION_TIME_IN_DAYS = 90
COMMENT = 'Pipeline 5: Openflow CDC — operational DB replication via Apache NiFi';

-- CDC stream for incremental merge processing
CREATE STREAM IF NOT EXISTS RAW_VAULT.CDC.STREAM_CDC_EVENTS
    ON TABLE RAW_VAULT.CDC.RAW_CDC_EVENTS
    APPEND_ONLY = TRUE
    COMMENT = 'CDC stream for merge orchestration into landing tables';

-- =============================================================================
-- Openflow NiFi Flow Configuration Reference
-- (deployed via Snowflake Openflow UI or NiFi REST API)
-- =============================================================================
-- Flow: PostgreSQL CDC → Snowpipe Streaming
-- 1. CaptureChangePostgreSQL processor → reads WAL replication slot
-- 2. RouteOnAttribute → routes by table name
-- 3. ConvertRecord → transforms to JSON
-- 4. PutSnowpipeStreaming → writes to RAW_CDC_EVENTS via Ingest SDK
--
-- Flow: Oracle XStream CDC → Snowpipe Streaming
-- 1. CaptureChangeOracle (XStream) → reads redo log
-- 2. Same pipeline as above

-- =============================================================================
-- GRANTS
-- =============================================================================

GRANT USAGE ON INTEGRATION ADLS_RAW_INTEGRATION TO ROLE LOADER;
GRANT USAGE ON INTEGRATION GCS_RAW_INTEGRATION  TO ROLE LOADER;

GRANT USAGE ON STAGE RAW_VAULT.ECOMMERCE.STG_ADLS_ECOMMERCE TO ROLE LOADER;
GRANT USAGE ON STAGE RAW_VAULT.ECOMMERCE.STG_GCS_ECOMMERCE  TO ROLE LOADER;

GRANT INSERT, SELECT ON TABLE RAW_VAULT.STREAMING.RAW_KAFKA_EVENTS TO ROLE LOADER;
GRANT INSERT, SELECT ON TABLE RAW_VAULT.CDC.RAW_CDC_EVENTS         TO ROLE LOADER;

GRANT USAGE ON SCHEMA RAW_VAULT.STREAMING TO ROLE TRANSFORMER;
GRANT USAGE ON SCHEMA RAW_VAULT.CDC       TO ROLE TRANSFORMER;
GRANT SELECT ON ALL TABLES IN SCHEMA RAW_VAULT.STREAMING TO ROLE TRANSFORMER;
GRANT SELECT ON ALL TABLES IN SCHEMA RAW_VAULT.CDC       TO ROLE TRANSFORMER;
