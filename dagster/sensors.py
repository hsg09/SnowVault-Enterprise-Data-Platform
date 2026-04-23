"""
Dagster Sensors — Event-driven triggers for data-aware orchestration.

Key blueprint alignment:
- stream_has_data_sensor: Checks Snowflake Streams for pending CDC data
- replication_lag_sensor: Monitors cross-region replication lag SLA
"""

from dagster import (
    sensor,
    RunRequest,
    SensorEvaluationContext,
    SkipReason,
    DefaultSensorStatus,
)
from dagster_snowflake import SnowflakeResource


STREAMS_TO_MONITOR = [
    "RAW_VAULT.ECOMMERCE.STREAM_CUSTOMERS",
    "RAW_VAULT.ECOMMERCE.STREAM_ORDERS",
    "RAW_VAULT.ECOMMERCE.STREAM_PRODUCTS",
    "RAW_VAULT.ECOMMERCE.STREAM_ORDER_ITEMS",
]

REPLICATION_SLA_SECONDS = 7200  # 2 hours


@sensor(
    job_name="cdc_streaming_pipeline",
    minimum_interval_seconds=300,  # Check every 5 minutes
    default_status=DefaultSensorStatus.RUNNING,
    description=(
        "Monitors Snowflake Streams for pending CDC data. "
        "Triggers incremental load when new change records are detected. "
        "This is data-aware orchestration — Dagster understands which "
        "downstream Gold assets are impacted by the CDC changes."
    ),
)
def stream_has_data_sensor(
    context: SensorEvaluationContext,
    snowflake: SnowflakeResource,
):
    """Check if any Snowflake Stream has pending data."""
    streams_with_data = []

    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        for stream in STREAMS_TO_MONITOR:
            try:
                cursor.execute(
                    f"SELECT SYSTEM$STREAM_HAS_DATA('{stream}') AS HAS_DATA"
                )
                result = cursor.fetchone()
                if result and result[0]:
                    streams_with_data.append(stream)
            except Exception as e:
                context.log.warning(f"Failed to check stream {stream}: {e}")

    if streams_with_data:
        context.log.info(
            f"CDC data detected in {len(streams_with_data)} streams: "
            f"{streams_with_data}"
        )
        yield RunRequest(
            run_key=f"cdc-{context.cursor}",
            tags={
                "trigger": "stream_sensor",
                "streams_with_data": str(streams_with_data),
            },
        )
    else:
        yield SkipReason("No pending CDC data in any stream.")


@sensor(
    job_name="full_elt_pipeline",
    minimum_interval_seconds=3600,  # Check every hour
    default_status=DefaultSensorStatus.RUNNING,
    description=(
        "Monitors cross-region replication lag. "
        "Alerts if lag exceeds SLA threshold (2 hours). "
        "If lag is critical, halts downstream assets to prevent "
        "consumers from querying stale replicated data."
    ),
)
def replication_lag_sensor(
    context: SensorEvaluationContext,
    snowflake: SnowflakeResource,
):
    """Monitor replication lag and alert on SLA breach."""
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT
                    REPLICATION_GROUP_NAME,
                    DATEDIFF('second', PHASE_1_BEGIN, PHASE_4_FINALIZING_END)
                        AS LAG_SECONDS
                FROM TABLE(INFORMATION_SCHEMA.REPLICATION_GROUP_REFRESH_HISTORY())
                WHERE REPLICATION_GROUP_NAME = 'RG_DATA_PLATFORM'
                ORDER BY PHASE_4_FINALIZING_END DESC
                LIMIT 1
            """)
            result = cursor.fetchone()

            if result:
                lag_seconds = result[1] or 0
                if lag_seconds > REPLICATION_SLA_SECONDS:
                    context.log.error(
                        f"CRITICAL: Replication lag = {lag_seconds}s "
                        f"(SLA = {REPLICATION_SLA_SECONDS}s). "
                        f"Halting downstream assets."
                    )
                    # Do NOT trigger a run — this prevents stale data propagation
                    yield SkipReason(
                        f"Replication SLA BREACH: lag={lag_seconds}s > "
                        f"threshold={REPLICATION_SLA_SECONDS}s. "
                        f"Downstream assets HALTED."
                    )
                else:
                    context.log.info(
                        f"Replication healthy: lag={lag_seconds}s"
                    )
                    yield SkipReason(
                        f"Replication within SLA: {lag_seconds}s"
                    )
            else:
                yield SkipReason("No replication history found.")

        except Exception as e:
            context.log.warning(f"Replication check failed: {e}")
            yield SkipReason(f"Replication check error: {e}")
