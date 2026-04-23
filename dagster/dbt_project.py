"""
dbt Project helper — Provides manifest path for Dagster dbt integration.
"""

import os
from pathlib import Path

from dagster_dbt import DbtProject

dbt_project = DbtProject(
    project_dir=Path(__file__).parent.parent,
    packaged_project_dir=Path(__file__).parent.parent / "target",
)

# Prepare dbt manifest for Dagster at load time
dbt_project.prepare_if_dev()
