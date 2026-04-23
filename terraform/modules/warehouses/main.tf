# =============================================================================
# Module: Warehouses — Workload-Isolated Compute
# =============================================================================

terraform {
  required_providers {
    snowflake = {
      source = "Snowflake-Labs/snowflake"
    }
  }
}

locals {
  wh_size = var.environment == "prod" ? "MEDIUM" : "XSMALL"
}

resource "snowflake_warehouse" "ingestion" {
  name                = "INGESTION_WH"
  warehouse_size      = local.wh_size
  auto_suspend        = 60
  auto_resume         = true
  min_cluster_count   = 1
  max_cluster_count   = var.environment == "prod" ? 3 : 1
  scaling_policy      = "STANDARD"
  comment             = "Snowpipe, Streaming, CDC ingestion workloads"
}

resource "snowflake_warehouse" "transformer" {
  name                = "TRANSFORMER_WH"
  warehouse_size      = var.environment == "prod" ? "LARGE" : "SMALL"
  auto_suspend        = 120
  auto_resume         = true
  min_cluster_count   = 1
  max_cluster_count   = var.environment == "prod" ? 4 : 1
  scaling_policy      = "STANDARD"
  comment             = "dbt Data Vault transformations"
}

resource "snowflake_warehouse" "analytics" {
  name                = "ANALYTICS_WH"
  warehouse_size      = local.wh_size
  auto_suspend        = 300
  auto_resume         = true
  min_cluster_count   = 1
  max_cluster_count   = var.environment == "prod" ? 6 : 1
  scaling_policy      = "ECONOMY"
  comment             = "BI dashboards, ad-hoc analytics"
}

resource "snowflake_warehouse" "dev" {
  name                = "DEV_WH"
  warehouse_size      = "XSMALL"
  auto_suspend        = 60
  auto_resume         = true
  comment             = "Developer sandbox"
}

resource "snowflake_warehouse" "ci" {
  name                = "CI_WH"
  warehouse_size      = "SMALL"
  auto_suspend        = 60
  auto_resume         = true
  comment             = "CI/CD pipeline ephemeral builds"
}
