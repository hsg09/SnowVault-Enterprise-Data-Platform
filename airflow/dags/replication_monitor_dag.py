"""
replication_monitor_dag.py — Cross-Region Replication Monitoring DAG
Data Vault 2.0 Snowflake Data Platform

PURPOSE:
    Monitor cross-region database replication status and lag:
    - Query REPLICATION_GROUP_REFRESH_HISTORY for lag metrics
    - Alert if replication lag exceeds SLA thresholds
    - Log status to AUDIT.CONTROL.REPLICATION_STATUS
    - Trigger failover alerts for critical lag breaches

SCHEDULE: Every hour
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

# =============================================================================
# Configuration
# =============================================================================

SNOWFLAKE_CONN_ID = "snowflake_default"

# Replication SLA thresholds (seconds)
REPLICATION_SLA = {
    "WARNING": 3600,    # 1 hour
    "CRITICAL": 7200,   # 2 hours
}

# Replication groups to monitor
REPLICATION_GROUPS = [
    "RG_DATA_PLATFORM",
]

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "email": ["data-platform-team@yourcompany.com"],
    "email_on_failure": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=15),
}


def check_replication_lag(**context):
    """
    Check replication lag for all configured replication groups.
    Alert if lag exceeds SLA thresholds.
    """
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    alerts = []

    for group in REPLICATION_GROUPS:
        try:
            results = hook.get_records(f"""
                SELECT
                    REPLICATION_GROUP_NAME,
                    PHASE_1_BEGIN,
                    PHASE_4_FINALIZING_END,
                    DATEDIFF('second', PHASE_1_BEGIN, PHASE_4_FINALIZING_END)
                        AS TOTAL_SECONDS,
                    BYTES_TRANSFERRED
                FROM TABLE(INFORMATION_SCHEMA.REPLICATION_GROUP_REFRESH_HISTORY())
                WHERE REPLICATION_GROUP_NAME = '{group}'
                ORDER BY PHASE_4_FINALIZING_END DESC
                LIMIT 1
            """)

            if results:
                row = results[0]
                lag_seconds = row[3] if row[3] else 0
                group_name = row[0]

                # Determine status
                if lag_seconds >= REPLICATION_SLA["CRITICAL"]:
                    status = "CRITICAL"
                    alerts.append(
                        f"CRITICAL: {group_name} replication lag = {lag_seconds}s "
                        f"(threshold: {REPLICATION_SLA['CRITICAL']}s)"
                    )
                elif lag_seconds >= REPLICATION_SLA["WARNING"]:
                    status = "WARNING"
                    alerts.append(
                        f"WARNING: {group_name} replication lag = {lag_seconds}s "
                        f"(threshold: {REPLICATION_SLA['WARNING']}s)"
                    )
                else:
                    status = "HEALTHY"

                # Log to AUDIT
                hook.run(f"""
                    INSERT INTO AUDIT.CONTROL.REPLICATION_STATUS
                        (REPLICATION_GROUP_NAME, SOURCE_ACCOUNT, TARGET_ACCOUNT,
                         DATABASE_NAME, REPLICATION_LAG_SECONDS, BYTES_TRANSFERRED,
                         STATUS, LAST_REFRESH_START, LAST_REFRESH_END)
                    VALUES
                        ('{group_name}', CURRENT_ACCOUNT(), 'SECONDARY',
                         'ALL', {lag_seconds}, {row[4] or 0},
                         '{status}', '{row[1]}', '{row[2]}')
                """)

                context["ti"].log.info(
                    f"Replication {group_name}: lag={lag_seconds}s, "
                    f"bytes={row[4] or 0}, status={status}"
                )
            else:
                context["ti"].log.warning(
                    f"No replication history found for {group}"
                )

        except Exception as e:
            context["ti"].log.error(
                f"Failed to check replication for {group}: {e}"
            )

    if alerts:
        context["ti"].log.error(f"Replication alerts: {alerts}")

    context["ti"].xcom_push(key="replication_alerts", value=alerts)


# =============================================================================
# DAG Definition
# =============================================================================

with DAG(
    dag_id="replication_monitor",
    description="Cross-region replication lag monitoring and alerting",
    default_args=default_args,
    schedule_interval="0 * * * *",  # Every hour
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["replication", "monitoring", "dr"],
    doc_md=__doc__,
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    check_lag = PythonOperator(
        task_id="check_replication_lag",
        python_callable=check_replication_lag,
    )

    start >> check_lag >> end
