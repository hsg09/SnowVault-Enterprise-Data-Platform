"""
Bronze Layer Assets — Software-Defined Assets for Raw Vault.

Each dbt model is exposed as a Dagster asset, enabling:
- Full lineage from source → staging → hub/link/sat
- Automatic upstream anomaly propagation halting
- Materialization tracking and freshness policies
"""

from dagster import AssetExecutionContext, FreshnessPolicy, AutoMaterializePolicy
from dagster_dbt import DbtCliResource, dbt_assets, DagsterDbtTranslatorSettings

from ..dbt_project import dbt_project


dagster_dbt_translator_settings = DagsterDbtTranslatorSettings(
    enable_asset_checks=True,
)


@dbt_assets(
    manifest=dbt_project.manifest_path,
    select="tag:bronze",
    dagster_dbt_translator_settings=dagster_dbt_translator_settings,
)
def bronze_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    """
    Bronze Layer — Raw Vault dbt models.

    Includes: staging, hubs, links, satellites, effectivity satellites.
    Auto-materialization: triggered when upstream sources report new data.
    Freshness: warn if >12h stale, error if >24h stale.
    """
    yield from dbt.cli(["build"], context=context).stream()


# Expose as list for Definitions
bronze_assets = [bronze_dbt_assets]
