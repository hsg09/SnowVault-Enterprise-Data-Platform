"""
cdc_streaming_dag.py — CDC / Streaming Pipeline DAG
Data Vault 2.0 Snowflake Data Platform

PURPOSE:
    Monitor CDC/streaming ingestion pipelines:
    - Check Snowflake Stream status (has_data)
    - Monitor Snowpipe health and copy history
    - Trigger incremental dbt runs if new CDC data exists
    - Alert on ingestion failures or SLA breaches

SCHEDULE: Every 30 minutes (near-real-time monitoring)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.task_group import TaskGroup

# =============================================================================
# Configuration
# =============================================================================

SNOWFLAKE_CONN_ID = "snowflake_default"
DBT_PROJECT_DIR = "/opt/airflow/dbt/data_vault_2_0"
DBT_PROFILES_DIR = "/opt/airflow/dbt/data_vault_2_0"
DBT_CMD_PREFIX = f"cd {DBT_PROJECT_DIR} && dbt"
DBT_GLOBAL_FLAGS = f"--profiles-dir {DBT_PROFILES_DIR} --target prod"

# Streams to monitor
STREAMS = [
    {"name": "RAW_VAULT.ECOMMERCE.STREAM_CUSTOMERS", "entity": "customers"},
    {"name": "RAW_VAULT.ECOMMERCE.STREAM_ORDERS", "entity": "orders"},
    {"name": "RAW_VAULT.ECOMMERCE.STREAM_PRODUCTS", "entity": "products"},
    {"name": "RAW_VAULT.ECOMMERCE.STREAM_ORDER_ITEMS", "entity": "order_items"},
]

# Pipes to monitor
PIPES = [
    "RAW_VAULT.ECOMMERCE.PIPE_CUSTOMERS",
    "RAW_VAULT.ECOMMERCE.PIPE_ORDERS",
    "RAW_VAULT.ECOMMERCE.PIPE_PRODUCTS",
    "RAW_VAULT.ECOMMERCE.PIPE_ORDER_ITEMS",
]

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "email": ["data-platform-team@yourcompany.com"],
    "email_on_failure": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=30),
}


# =============================================================================
# Helper Functions
# =============================================================================

def check_streams_have_data(**context):
    """
    Check if any Snowflake Streams have pending data.
    Returns the task_id to branch to:
    - 'run_incremental_load' if data exists
    - 'skip_no_data' if no data
    """
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    has_data = False
    streams_with_data = []

    for stream in STREAMS:
        result = hook.get_first(
            f"SELECT SYSTEM$STREAM_HAS_DATA('{stream['name']}') AS HAS_DATA"
        )
        if result and result[0]:
            has_data = True
            streams_with_data.append(stream["entity"])

    context["ti"].xcom_push(key="streams_with_data", value=streams_with_data)

    if has_data:
        return "cdc_processing.run_incremental_load"
    return "skip_no_data"


def log_ingestion_metrics(**context):
    """
    Query Snowpipe copy history and log metrics to AUDIT.CONTROL.FILE_INGESTION_LOG.
    """
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

    for pipe in PIPES:
        try:
            results = hook.get_records(f"""
                SELECT
                    FILE_NAME,
                    STATUS,
                    ROW_COUNT,
                    FILE_SIZE,
                    FIRST_ERROR_MESSAGE,
                    LAST_LOAD_TIME
                FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
                    TABLE_NAME => '{pipe}',
                    START_TIME => DATEADD('minute', -30, CURRENT_TIMESTAMP())
                ))
            """)

            for row in results:
                hook.run(f"""
                    INSERT INTO AUDIT.CONTROL.FILE_INGESTION_LOG
                        (FILE_NAME, FILE_PATH, STAGE_NAME, SOURCE_SYSTEM,
                         FILE_FORMAT, ROW_COUNT, STATUS, PIPE_NAME)
                    VALUES
                        ('{row[0]}', '{row[0]}', '{pipe}', 'ECOMMERCE',
                         'AUTO', {row[2] or 0}, '{row[1]}', '{pipe}')
                """)
        except Exception as e:
            context["ti"].log.warning(f"Failed to log metrics for {pipe}: {e}")


# =============================================================================
# DAG Definition
# =============================================================================

with DAG(
    dag_id="cdc_streaming_pipeline",
    description="CDC/Streaming pipeline — monitors streams, triggers incremental loads",
    default_args=default_args,
    schedule_interval="*/30 * * * *",  # Every 30 minutes
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["cdc", "streaming", "production"],
    doc_md=__doc__,
) as dag:

    start = EmptyOperator(task_id="pipeline_start")
    end = EmptyOperator(task_id="pipeline_end", trigger_rule="none_failed")

    # =========================================================================
    # Stream Health Check
    # =========================================================================
    check_streams = BranchPythonOperator(
        task_id="check_streams_have_data",
        python_callable=check_streams_have_data,
    )

    skip_no_data = EmptyOperator(task_id="skip_no_data")

    # =========================================================================
    # CDC Processing (only runs if streams have data)
    # =========================================================================
    with TaskGroup("cdc_processing", tooltip="Process CDC stream data") as cdc:

        # Run incremental dbt for staging + Raw Vault
        run_incremental = BashOperator(
            task_id="run_incremental_load",
            bash_command=(
                f"{DBT_CMD_PREFIX} run "
                f"--select tag:staging tag:hub tag:link tag:satellite tag:effectivity_satellite "
                f"{DBT_GLOBAL_FLAGS}"
            ),
        )

        # Test the incremental load
        test_incremental = BashOperator(
            task_id="test_incremental_load",
            bash_command=f"{DBT_CMD_PREFIX} test --select tag:bronze {DBT_GLOBAL_FLAGS}",
        )

        run_incremental >> test_incremental

    # =========================================================================
    # Ingestion Metrics Logging
    # =========================================================================
    log_metrics = PythonOperator(
        task_id="log_ingestion_metrics",
        python_callable=log_ingestion_metrics,
        trigger_rule="none_failed",
    )

    # =========================================================================
    # Dependency Chain
    # =========================================================================
    start >> check_streams >> [cdc, skip_no_data]
    [cdc, skip_no_data] >> log_metrics >> end
