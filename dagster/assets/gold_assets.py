"""
Gold Layer Assets — Facts, Dimensions, Aggregates, Secure Views.
"""

from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets, DagsterDbtTranslatorSettings

from ..dbt_project import dbt_project


dagster_dbt_translator_settings = DagsterDbtTranslatorSettings(
    enable_asset_checks=True,
)


@dbt_assets(
    manifest=dbt_project.manifest_path,
    select="tag:gold",
    dagster_dbt_translator_settings=dagster_dbt_translator_settings,
)
def gold_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    """
    Gold Layer — Analytics dbt models.

    Includes: facts, dimensions, aggregates, secure views.
    Depends on: silver_dbt_assets (automatic via dbt lineage).
    """
    yield from dbt.cli(["build"], context=context).stream()


gold_assets = [gold_dbt_assets]
