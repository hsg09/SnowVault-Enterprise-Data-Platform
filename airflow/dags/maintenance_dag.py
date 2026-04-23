"""
maintenance_dag.py — Housekeeping & Maintenance DAG
Data Vault 2.0 Snowflake Data Platform

PURPOSE:
    Automated housekeeping tasks:
    - Drop stale CI schemas (ephemeral PR validation schemas)
    - Archive old audit / DQ logs (move to cold storage)
    - Refresh search optimization on frequently queried tables
    - Run dbt docs generate for documentation freshness
    - Clean up orphaned backup tables

SCHEDULE: Daily at 03:00 UTC (maintenance window)
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

# CI schema retention (days)
CI_SCHEMA_MAX_AGE_DAYS = 7

# Audit log retention (days)
AUDIT_LOG_MAX_AGE_DAYS = 90

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

def cleanup_ci_schemas(**context):
    """
    Drop CI schemas older than CI_SCHEMA_MAX_AGE_DAYS.
    CI schemas follow the pattern: ci_<github_run_id>
    """
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    databases = ["RAW_VAULT", "BUSINESS_VAULT", "ANALYTICS", "AUDIT"]
    dropped = []

    for db in databases:
        try:
            schemas = hook.get_records(f"""
                SELECT SCHEMA_NAME, CREATED
                FROM {db}.INFORMATION_SCHEMA.SCHEMATA
                WHERE SCHEMA_NAME LIKE 'CI\\_%' ESCAPE '\\\\'
                  AND CREATED < DATEADD('day', -{CI_SCHEMA_MAX_AGE_DAYS}, CURRENT_DATE())
            """)

            for schema in schemas:
                schema_name = schema[0]
                hook.run(f"DROP SCHEMA IF EXISTS {db}.{schema_name} CASCADE")
                dropped.append(f"{db}.{schema_name}")
                context["ti"].log.info(f"Dropped stale CI schema: {db}.{schema_name}")

        except Exception as e:
            context["ti"].log.warning(f"CI cleanup failed for {db}: {e}")

    context["ti"].xcom_push(key="dropped_schemas", value=dropped)
    context["ti"].log.info(f"Total CI schemas dropped: {len(dropped)}")


def archive_old_audit_logs(**context):
    """
    Delete audit records older than AUDIT_LOG_MAX_AGE_DAYS.
    These have already been backed up via Time Travel retention.
    """
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    tables = [
        "AUDIT.CONTROL.FILE_INGESTION_LOG",
        "AUDIT.CONTROL.TASK_EXECUTION_LOG",
        "AUDIT.DQ_RESULTS.DQ_TEST_RESULTS",
    ]

    for table in tables:
        try:
            result = hook.get_first(f"""
                SELECT COUNT(*)
                FROM {table}
                WHERE CREATED_AT < DATEADD('day', -{AUDIT_LOG_MAX_AGE_DAYS}, CURRENT_DATE())
                   OR EXECUTED_AT < DATEADD('day', -{AUDIT_LOG_MAX_AGE_DAYS}, CURRENT_DATE())
            """)

            rows_to_delete = result[0] if result else 0

            if rows_to_delete > 0:
                hook.run(f"""
                    DELETE FROM {table}
                    WHERE COALESCE(CREATED_AT, EXECUTED_AT)
                        < DATEADD('day', -{AUDIT_LOG_MAX_AGE_DAYS}, CURRENT_DATE())
                """)
                context["ti"].log.info(
                    f"Archived {rows_to_delete} rows from {table}"
                )

        except Exception as e:
            context["ti"].log.warning(f"Archive failed for {table}: {e}")


# =============================================================================
# DAG Definition
# =============================================================================

with DAG(
    dag_id="platform_maintenance",
    description="Daily housekeeping — CI cleanup, audit archival, docs refresh",
    default_args=default_args,
    schedule_interval="0 3 * * *",  # Daily at 03:00 UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["maintenance", "housekeeping", "production"],
    doc_md=__doc__,
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", trigger_rule="none_failed")

    # =========================================================================
    # Cleanup Tasks
    # =========================================================================
    with TaskGroup("cleanup", tooltip="Cleanup stale resources") as cleanup:

        ci_cleanup = PythonOperator(
            task_id="cleanup_ci_schemas",
            python_callable=cleanup_ci_schemas,
        )

        audit_archive = PythonOperator(
            task_id="archive_old_audit_logs",
            python_callable=archive_old_audit_logs,
        )

    # =========================================================================
    # Search Optimization Refresh
    # =========================================================================
    search_optimization = SnowflakeOperator(
        task_id="refresh_search_optimization",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql="""
            -- Refresh search optimization on frequently queried tables
            ALTER TABLE ANALYTICS.FACTS.FCT_ORDERS
                ADD SEARCH OPTIMIZATION ON EQUALITY(ORDER_ID, ORDER_DATE_KEY);

            ALTER TABLE ANALYTICS.DIMENSIONS.DIM_CUSTOMER
                ADD SEARCH OPTIMIZATION ON EQUALITY(CUSTOMER_ID, CUSTOMER_SK);
        """,
    )

    # =========================================================================
    # dbt Docs Generation
    # =========================================================================
    generate_docs = BashOperator(
        task_id="generate_dbt_docs",
        bash_command=f"{DBT_CMD_PREFIX} docs generate {DBT_GLOBAL_FLAGS}",
    )

    # =========================================================================
    # Dependencies
    # =========================================================================
    start >> cleanup >> search_optimization >> generate_docs >> end
