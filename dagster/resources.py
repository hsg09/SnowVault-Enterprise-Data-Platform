"""
Dagster Resources — Shared infrastructure connections.

Resources are injected into assets, enabling environment-specific
configuration (dev vs prod) without code changes.
"""

import os

from dagster import EnvVar
from dagster_dbt import DbtCliResource
from dagster_snowflake import SnowflakeResource


def get_resources() -> dict:
    """Return environment-aware resource definitions."""
    return {
        "snowflake": SnowflakeResource(
            account=EnvVar("SNOWFLAKE_ACCOUNT"),
            user=EnvVar("SNOWFLAKE_USER"),
            password=EnvVar("SNOWFLAKE_PASSWORD"),
            role=EnvVar.int_or_default("SNOWFLAKE_ROLE", "DATA_ENGINEER"),
            warehouse=EnvVar.int_or_default("SNOWFLAKE_WAREHOUSE", "TRANSFORMER_WH"),
            database=EnvVar.int_or_default("SNOWFLAKE_DATABASE", "RAW_VAULT"),
        ),
        "dbt": DbtCliResource(
            project_dir=os.path.join(os.path.dirname(__file__), ".."),
            profiles_dir=os.path.join(os.path.dirname(__file__), ".."),
            target=os.getenv("DBT_TARGET", "dev"),
        ),
    }
