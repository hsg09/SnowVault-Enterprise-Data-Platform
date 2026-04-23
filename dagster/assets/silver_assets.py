"""
Silver Layer Assets — Business Vault, PIT, Bridge, Conformed.
"""

from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets, DagsterDbtTranslatorSettings

from ..dbt_project import dbt_project


dagster_dbt_translator_settings = DagsterDbtTranslatorSettings(
    enable_asset_checks=True,
)


@dbt_assets(
    manifest=dbt_project.manifest_path,
    select="tag:silver",
    dagster_dbt_translator_settings=dagster_dbt_translator_settings,
)
def silver_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    """
    Silver Layer — Business Vault dbt models.

    Includes: business vault, PIT tables, bridge tables, conformed models.
    Depends on: bronze_dbt_assets (automatic via dbt lineage).
    """
    yield from dbt.cli(["build"], context=context).stream()


silver_assets = [silver_dbt_assets]
