"""
Dagster Jobs — Named job definitions for triggering by schedules/sensors.
"""

from dagster import define_asset_job, AssetSelection


# Full ELT pipeline: Bronze → Silver → Gold
full_elt_job = define_asset_job(
    name="full_elt_pipeline",
    selection=AssetSelection.groups("bronze", "silver", "gold"),
    description="Full Data Vault ELT pipeline across all layers.",
    tags={"dagster/priority": "1", "team": "data-platform"},
)

# CDC-only incremental load
cdc_streaming_job = define_asset_job(
    name="cdc_streaming_pipeline",
    selection=AssetSelection.groups("ingestion"),
    description="CDC/Streaming ingestion health check and incremental load.",
    tags={"dagster/priority": "2", "team": "data-platform"},
)

# Data quality checks
data_quality_job = define_asset_job(
    name="data_quality_checks",
    selection=AssetSelection.groups("bronze", "silver", "gold"),
    description="Run dbt tests across all layers for data quality.",
    tags={"dagster/priority": "3", "team": "data-quality"},
)

# Maintenance
maintenance_job = define_asset_job(
    name="platform_maintenance",
    selection=AssetSelection.groups("ingestion"),
    description="Housekeeping: CI schema cleanup, audit archival.",
    tags={"dagster/priority": "4", "team": "data-platform"},
)
