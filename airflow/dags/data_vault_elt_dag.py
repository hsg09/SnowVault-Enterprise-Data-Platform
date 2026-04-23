"""
data_vault_elt_dag.py — Main ELT Pipeline DAG
Data Vault 2.0 Snowflake Data Platform

PURPOSE:
    Orchestrates the full Data Vault ELT pipeline:
    S3 Ingest → Raw Vault Staging → Hubs/Links/Sats → Business Vault → PIT/Bridge → Gold

SCHEDULE: Every 4 hours (aligned with Snowflake task schedule)
LAYER EXECUTION ORDER:
    1. Bronze: Staging → Hubs → Links → Satellites → Effectivity Satellites
    2. Silver: Business Vault → PIT Tables → Bridge Tables → Conformed
    3. Gold:   Facts → Dimensions → Aggregates → Secure Views

NOTES:
    - Uses TaskGroups to organize layers visually in the Airflow UI
    - Test gates between layers prevent bad data from propagating
    - dbt commands use --profiles-dir for CI portability
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

# =============================================================================
# DAG Configuration
# =============================================================================

DBT_PROJECT_DIR = "/opt/airflow/dbt/data_vault_2_0"
DBT_PROFILES_DIR = "/opt/airflow/dbt/data_vault_2_0"
DBT_CMD_PREFIX = f"cd {DBT_PROJECT_DIR} && dbt"
DBT_GLOBAL_FLAGS = f"--profiles-dir {DBT_PROFILES_DIR} --target prod"

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "email": ["data-platform-team@yourcompany.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(hours=2),
}

# =============================================================================
# Helper: Create dbt run/test BashOperator
# =============================================================================

def dbt_run(task_id: str, select: str, **kwargs) -> BashOperator:
    """Create a dbt run task for a specific model or tag selection."""
    return BashOperator(
        task_id=task_id,
        bash_command=f"{DBT_CMD_PREFIX} run --select {select} {DBT_GLOBAL_FLAGS}",
        **kwargs,
    )


def dbt_test(task_id: str, select: str, **kwargs) -> BashOperator:
    """Create a dbt test task for a specific model or tag selection."""
    return BashOperator(
        task_id=task_id,
        bash_command=f"{DBT_CMD_PREFIX} test --select {select} {DBT_GLOBAL_FLAGS}",
        **kwargs,
    )


def dbt_snapshot(task_id: str, **kwargs) -> BashOperator:
    """Run dbt snapshots."""
    return BashOperator(
        task_id=task_id,
        bash_command=f"{DBT_CMD_PREFIX} snapshot {DBT_GLOBAL_FLAGS}",
        **kwargs,
    )


# =============================================================================
# DAG Definition
# =============================================================================

with DAG(
    dag_id="data_vault_elt_pipeline",
    description="Main Data Vault 2.0 ELT pipeline: Bronze → Silver → Gold",
    default_args=default_args,
    schedule_interval="0 */4 * * *",  # Every 4 hours
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["data-vault", "elt", "production"],
    doc_md=__doc__,
) as dag:

    # =========================================================================
    # Pipeline Start / End
    # =========================================================================
    start = EmptyOperator(task_id="pipeline_start")
    end = EmptyOperator(task_id="pipeline_end", trigger_rule="none_failed")

    # =========================================================================
    # BRONZE LAYER: Raw Vault
    # =========================================================================
    with TaskGroup("bronze_raw_vault", tooltip="Bronze: Raw Vault loading") as bronze:

        # --- Staging ---
        with TaskGroup("staging") as staging:
            stg_customers = dbt_run("stg_customers", "stg_ecommerce__customers")
            stg_orders = dbt_run("stg_orders", "stg_ecommerce__orders")
            stg_products = dbt_run("stg_products", "stg_ecommerce__products")
            stg_order_items = dbt_run("stg_order_items", "stg_ecommerce__order_items")

        # --- Hubs ---
        with TaskGroup("hubs") as hubs:
            hub_customer = dbt_run("hub_customer", "hub_customer")
            hub_order = dbt_run("hub_order", "hub_order")
            hub_product = dbt_run("hub_product", "hub_product")

        # --- Links ---
        with TaskGroup("links") as links:
            link_customer_order = dbt_run("link_customer_order", "link_customer_order")
            link_order_product = dbt_run("link_order_product", "link_order_product")

        # --- Satellites ---
        with TaskGroup("satellites") as satellites:
            sat_cust_det = dbt_run("sat_customer_details", "sat_customer_details")
            sat_cust_dem = dbt_run("sat_customer_demographics", "sat_customer_demographics")
            sat_ord_det = dbt_run("sat_order_details", "sat_order_details")
            sat_ord_fin = dbt_run("sat_order_financials", "sat_order_financials")
            sat_prod_det = dbt_run("sat_product_details", "sat_product_details")
            sat_prod_prc = dbt_run("sat_product_pricing", "sat_product_pricing")

        # --- Effectivity Satellites ---
        with TaskGroup("effectivity_satellites") as eff_sats:
            eff_sat_co = dbt_run("eff_sat_customer_order", "eff_sat_customer_order")

        # --- Bronze Test Gate ---
        bronze_tests = dbt_test(
            "bronze_test_gate",
            "tag:bronze",
            retries=0,  # No retry on test failures — fail fast
        )

        # --- Snapshots ---
        snapshots = dbt_snapshot("run_snapshots")

        # Dependency chain within Bronze
        staging >> hubs >> links >> [satellites, eff_sats] >> bronze_tests >> snapshots

    # =========================================================================
    # SILVER LAYER: Business Vault
    # =========================================================================
    with TaskGroup("silver_business_vault", tooltip="Silver: Business Vault") as silver:

        # --- Business Vault ---
        with TaskGroup("business_vault") as bv:
            bv_customer = dbt_run("bv_customer_classification", "bv_customer_classification")
            bv_order = dbt_run("bv_order_lifecycle", "bv_order_lifecycle")

        # --- PIT Tables ---
        with TaskGroup("pit_tables") as pit:
            pit_customer = dbt_run("pit_customer", "pit_customer")
            pit_order = dbt_run("pit_order", "pit_order")

        # --- Bridge Tables ---
        with TaskGroup("bridge_tables") as bridge:
            bridge_co = dbt_run("bridge_customer_order", "bridge_customer_order")

        # --- Conformed Layer ---
        with TaskGroup("conformed") as conformed:
            conf_customers = dbt_run("conformed_customers", "conformed_customers")
            conf_orders = dbt_run("conformed_orders", "conformed_orders")

        # --- Silver Test Gate ---
        silver_tests = dbt_test(
            "silver_test_gate",
            "tag:silver",
            retries=0,
        )

        # Dependency chain within Silver
        bv >> [pit, bridge] >> conformed >> silver_tests

    # =========================================================================
    # GOLD LAYER: Analytics / Reporting
    # =========================================================================
    with TaskGroup("gold_analytics", tooltip="Gold: Analytics layer") as gold:

        # --- Dimensions (must run before facts) ---
        with TaskGroup("dimensions") as dims:
            dim_customer = dbt_run("dim_customer", "dim_customer")
            dim_product = dbt_run("dim_product", "dim_product")
            dim_date = dbt_run("dim_date", "dim_date")

        # --- Facts ---
        with TaskGroup("facts") as facts:
            fct_orders = dbt_run("fct_orders", "fct_orders")
            fct_order_items = dbt_run("fct_order_items", "fct_order_items")

        # --- Aggregates ---
        with TaskGroup("aggregates") as aggs:
            agg_revenue = dbt_run("agg_monthly_revenue", "agg_monthly_revenue")
            agg_ltv = dbt_run("agg_customer_ltv", "agg_customer_ltv")

        # --- Secure Views ---
        with TaskGroup("secure_views") as secure:
            sv_profiles = dbt_run("secure_customer_profiles", "secure_customer_profiles")

        # --- Gold Test Gate ---
        gold_tests = dbt_test(
            "gold_test_gate",
            "tag:gold",
            retries=0,
        )

        # Dependency chain within Gold
        dims >> facts >> aggs >> secure >> gold_tests

    # =========================================================================
    # Full Pipeline Dependency Chain
    # =========================================================================
    start >> bronze >> silver >> gold >> end
