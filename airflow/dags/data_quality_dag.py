"""
data_quality_dag.py — Data Quality Monitoring DAG
Data Vault 2.0 Snowflake Data Platform

PURPOSE:
    Scheduled data quality checks across all layers:
    - Source freshness validation
    - Volume anomaly detection (row count thresholds)
    - Vault referential integrity checks
    - dbt test suite execution with failure logging
    - DQ trend reporting to AUDIT.DQ_RESULTS

SCHEDULE: Twice daily (06:00 and 18:00 UTC)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
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

# Volume thresholds (min expected rows per entity)
VOLUME_THRESHOLDS = {
    "hub_customer": 100,
    "hub_order": 100,
    "hub_product": 10,
    "sat_customer_details": 100,
    "sat_order_details": 100,
    "fct_orders": 100,
    "dim_customer": 100,
}

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "email": ["data-platform-team@yourcompany.com"],
    "email_on_failure": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}


# =============================================================================
# Helper Functions
# =============================================================================

def check_volume_anomalies(**context):
    """
    Compare current row counts against expected thresholds.
    Log anomalies to AUDIT.DQ_RESULTS.DQ_TEST_RESULTS.
    """
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    anomalies = []

    for model, min_rows in VOLUME_THRESHOLDS.items():
        try:
            # Determine database and schema based on model prefix
            if model.startswith("hub_") or model.startswith("sat_") or model.startswith("link_"):
                db_schema = "RAW_VAULT.RAW_VAULT"
            elif model.startswith("bv_") or model.startswith("pit_") or model.startswith("bridge_"):
                db_schema = "BUSINESS_VAULT.BUSINESS_VAULT"
            elif model.startswith("fct_"):
                db_schema = "ANALYTICS.FACTS"
            elif model.startswith("dim_"):
                db_schema = "ANALYTICS.DIMENSIONS"
            elif model.startswith("agg_"):
                db_schema = "ANALYTICS.AGGREGATES"
            else:
                db_schema = "RAW_VAULT.RAW_VAULT"

            result = hook.get_first(
                f"SELECT COUNT(*) FROM {db_schema}.{model.upper()}"
            )
            row_count = result[0] if result else 0

            status = "PASS" if row_count >= min_rows else "FAIL"
            if status == "FAIL":
                anomalies.append(f"{model}: {row_count} rows (expected >= {min_rows})")

            # Log to DQ_RESULTS
            hook.run(f"""
                INSERT INTO AUDIT.DQ_RESULTS.DQ_TEST_RESULTS
                    (TEST_NAME, TEST_TYPE, MODEL_NAME, STATUS,
                     FAILURES_COUNT, ROWS_SCANNED, SEVERITY)
                VALUES
                    ('volume_check_{model}', 'CUSTOM', '{model}', '{status}',
                     {0 if status == 'PASS' else 1}, {row_count}, 'ERROR')
            """)

        except Exception as e:
            context["ti"].log.warning(f"Volume check failed for {model}: {e}")

    if anomalies:
        context["ti"].log.error(f"Volume anomalies detected: {anomalies}")

    context["ti"].xcom_push(key="anomalies", value=anomalies)


def generate_dq_report(**context):
    """
    Generate a DQ summary report from recent test results.
    """
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

    results = hook.get_records("""
        SELECT
            STATUS,
            COUNT(*) AS TEST_COUNT,
            SUM(FAILURES_COUNT) AS TOTAL_FAILURES
        FROM AUDIT.DQ_RESULTS.DQ_TEST_RESULTS
        WHERE EXECUTED_AT >= DATEADD('day', -1, CURRENT_TIMESTAMP())
        GROUP BY STATUS
        ORDER BY STATUS
    """)

    report = "=== Data Quality Report (Last 24h) ===\n"
    for row in results:
        report += f"  {row[0]}: {row[1]} tests, {row[2]} failures\n"

    context["ti"].log.info(report)
    context["ti"].xcom_push(key="dq_report", value=report)


# =============================================================================
# DAG Definition
# =============================================================================

with DAG(
    dag_id="data_quality_monitoring",
    description="Scheduled DQ checks — freshness, volume, integrity, trends",
    default_args=default_args,
    schedule_interval="0 6,18 * * *",  # Twice daily
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["data-quality", "monitoring", "production"],
    doc_md=__doc__,
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", trigger_rule="none_failed")

    # =========================================================================
    # Source Freshness
    # =========================================================================
    source_freshness = BashOperator(
        task_id="check_source_freshness",
        bash_command=f"{DBT_CMD_PREFIX} source freshness {DBT_GLOBAL_FLAGS}",
    )

    # =========================================================================
    # dbt Test Suites (by layer)
    # =========================================================================
    with TaskGroup("dbt_tests", tooltip="Run dbt tests per layer") as tests:

        test_bronze = BashOperator(
            task_id="test_bronze",
            bash_command=f"{DBT_CMD_PREFIX} test --select tag:bronze {DBT_GLOBAL_FLAGS}",
        )

        test_silver = BashOperator(
            task_id="test_silver",
            bash_command=f"{DBT_CMD_PREFIX} test --select tag:silver {DBT_GLOBAL_FLAGS}",
        )

        test_gold = BashOperator(
            task_id="test_gold",
            bash_command=f"{DBT_CMD_PREFIX} test --select tag:gold {DBT_GLOBAL_FLAGS}",
        )

    # =========================================================================
    # Volume Anomaly Detection
    # =========================================================================
    volume_checks = PythonOperator(
        task_id="check_volume_anomalies",
        python_callable=check_volume_anomalies,
    )

    # =========================================================================
    # DQ Report Generation
    # =========================================================================
    dq_report = PythonOperator(
        task_id="generate_dq_report",
        python_callable=generate_dq_report,
        trigger_rule="none_failed",
    )

    # =========================================================================
    # Dependencies
    # =========================================================================
    start >> [source_freshness, tests, volume_checks] >> dq_report >> end
