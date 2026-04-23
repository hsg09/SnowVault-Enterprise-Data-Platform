"""
Dagster Definitions — Data Vault 2.0 Snowflake Data Platform

Software-Defined Assets (SDAs) orchestration using Dagster.
Replaces Airflow with data-aware, asset-centric orchestration.

Key advantages over Airflow (per blueprint):
- Treats DATA (not tasks) as the primary citizen
- Natively understands dbt project structure
- Automatically halts downstream assets on upstream anomalies
- Full lineage visualization across Bronze → Silver → Gold
"""

from dagster import Definitions, define_asset_job, ScheduleDefinition
from dagster_dbt import DbtCliResource
from dagster_snowflake import SnowflakeResource

from .assets.bronze_assets import bronze_assets
from .assets.silver_assets import silver_assets
from .assets.gold_assets import gold_assets
from .assets.ingestion_assets import ingestion_assets
from .jobs import (
    full_elt_job,
    cdc_streaming_job,
    data_quality_job,
    maintenance_job,
)
from .schedules import (
    elt_schedule,
    cdc_schedule,
    dq_schedule,
    maintenance_schedule,
)
from .sensors import stream_has_data_sensor, replication_lag_sensor
from .resources import get_resources

# =============================================================================
# Dagster Definitions — Single entry point
# =============================================================================

defs = Definitions(
    assets=[
        *ingestion_assets,
        *bronze_assets,
        *silver_assets,
        *gold_assets,
    ],
    jobs=[
        full_elt_job,
        cdc_streaming_job,
        data_quality_job,
        maintenance_job,
    ],
    schedules=[
        elt_schedule,
        cdc_schedule,
        dq_schedule,
        maintenance_schedule,
    ],
    sensors=[
        stream_has_data_sensor,
        replication_lag_sensor,
    ],
    resources=get_resources(),
)
