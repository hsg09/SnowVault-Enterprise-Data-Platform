"""
Ingestion Assets — External source monitoring and Snowpipe/Streaming status.

These assets represent the external data sources (S3, ADLS, GCS, Kafka, Openflow)
and monitor their ingestion health as Dagster observable source assets.
"""

from dagster import (
    asset,
    AssetExecutionContext,
    Output,
    MetadataValue,
    FreshnessPolicy,
)
from dagster_snowflake import SnowflakeResource


@asset(
    group_name="ingestion",
    compute_kind="snowflake",
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=60),
    description="Monitor AWS S3 Snowpipe ingestion health and copy history.",
)
def s3_snowpipe_status(context: AssetExecutionContext, snowflake: SnowflakeResource):
    """Pipeline 1: AWS S3 → Snowpipe status check."""
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT COUNT(*) AS files_loaded,
                   SUM(ROW_COUNT) AS total_rows,
                   MAX(LAST_LOAD_TIME) AS latest_load
            FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
                TABLE_NAME => 'RAW_VAULT.ECOMMERCE.RAW_CUSTOMERS',
                START_TIME => DATEADD('hour', -1, CURRENT_TIMESTAMP())
            ))
        """)
        result = cursor.fetchone()

    return Output(
        value={"files_loaded": result[0], "total_rows": result[1]},
        metadata={
            "files_loaded": MetadataValue.int(result[0] or 0),
            "total_rows": MetadataValue.int(result[1] or 0),
            "latest_load": MetadataValue.text(str(result[2])),
            "pipeline": MetadataValue.text("Pipeline 1: AWS S3"),
        },
    )


@asset(
    group_name="ingestion",
    compute_kind="snowflake",
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=60),
    description="Monitor Azure ADLS Gen2 Snowpipe ingestion health.",
)
def adls_snowpipe_status(context: AssetExecutionContext, snowflake: SnowflakeResource):
    """Pipeline 2: Azure ADLS Gen2 → Snowpipe status check."""
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT COUNT(*) AS files_loaded, MAX(LAST_LOAD_TIME) AS latest_load
            FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
                TABLE_NAME => 'RAW_VAULT.ECOMMERCE.RAW_ADLS_LANDING',
                START_TIME => DATEADD('hour', -1, CURRENT_TIMESTAMP())
            ))
        """)
        result = cursor.fetchone()

    return Output(
        value={"files_loaded": result[0]},
        metadata={
            "files_loaded": MetadataValue.int(result[0] or 0),
            "pipeline": MetadataValue.text("Pipeline 2: Azure ADLS Gen2"),
        },
    )


@asset(
    group_name="ingestion",
    compute_kind="snowflake",
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=60),
    description="Monitor GCP GCS Snowpipe ingestion health.",
)
def gcs_snowpipe_status(context: AssetExecutionContext, snowflake: SnowflakeResource):
    """Pipeline 3: GCP GCS → Snowpipe status check."""
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT COUNT(*) AS files_loaded, MAX(LAST_LOAD_TIME) AS latest_load
            FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
                TABLE_NAME => 'RAW_VAULT.ECOMMERCE.RAW_GCS_LANDING',
                START_TIME => DATEADD('hour', -1, CURRENT_TIMESTAMP())
            ))
        """)
        result = cursor.fetchone()

    return Output(
        value={"files_loaded": result[0]},
        metadata={
            "files_loaded": MetadataValue.int(result[0] or 0),
            "pipeline": MetadataValue.text("Pipeline 3: GCP GCS"),
        },
    )


@asset(
    group_name="ingestion",
    compute_kind="kafka",
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=5),
    description="Monitor Kafka Snowpipe Streaming v2 channel health.",
)
def kafka_streaming_status(context: AssetExecutionContext, snowflake: SnowflakeResource):
    """Pipeline 4: Kafka → Snowpipe Streaming v2 status check."""
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT
                COUNT(*) AS active_channels,
                MAX(SNOWPIPE_STREAMING_FILL_PUBLISH_TIME) AS latest_publish
            FROM TABLE(INFORMATION_SCHEMA.PIPE_USAGE_HISTORY(
                DATE_RANGE_START => DATEADD('hour', -1, CURRENT_TIMESTAMP())
            ))
            WHERE PIPE_NAME LIKE '%KAFKA%'
        """)
        result = cursor.fetchone()

    return Output(
        value={"active_channels": result[0]},
        metadata={
            "active_channels": MetadataValue.int(result[0] or 0),
            "pipeline": MetadataValue.text("Pipeline 4: Kafka Streaming v2"),
        },
    )


@asset(
    group_name="ingestion",
    compute_kind="nifi",
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=30),
    description="Monitor Snowflake Openflow CDC pipeline health.",
)
def openflow_cdc_status(context: AssetExecutionContext, snowflake: SnowflakeResource):
    """Pipeline 5: Openflow (NiFi CDC) → Status check."""
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT
                COUNT(*) AS cdc_records_pending,
                MAX(_LOADED_AT) AS latest_cdc_record
            FROM RAW_VAULT.CDC.RAW_CDC_EVENTS
            WHERE _LOADED_AT >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
        """)
        result = cursor.fetchone()

    return Output(
        value={"cdc_records_pending": result[0]},
        metadata={
            "cdc_records": MetadataValue.int(result[0] or 0),
            "pipeline": MetadataValue.text("Pipeline 5: Openflow CDC (NiFi)"),
        },
    )


ingestion_assets = [
    s3_snowpipe_status,
    adls_snowpipe_status,
    gcs_snowpipe_status,
    kafka_streaming_status,
    openflow_cdc_status,
]
