# =============================================================================
# Module: Resource Monitors — FinOps Cost Control
# Consolidated from SQL into Terraform for idempotent multi-region deployment.
# Account-level + per-warehouse monitors with tiered alerting.
# =============================================================================

terraform {
  required_providers {
    snowflake = {
      source = "Snowflake-Labs/snowflake"
    }
  }
}

locals {
  env = upper(var.environment)
  # Scale quotas based on environment
  quota_multiplier = var.environment == "prod" ? 1.0 : (var.environment == "staging" ? 0.3 : 0.1)
}

# =============================================================================
# 1. ACCOUNT-LEVEL RESOURCE MONITOR (Global Safety Net)
# =============================================================================

resource "snowflake_resource_monitor" "account_global" {
  name            = "RM_${local.env}_ACCOUNT_GLOBAL"
  credit_quota    = var.credit_quota_monthly
  frequency       = "MONTHLY"
  start_timestamp = "IMMEDIATELY"

  notify_triggers            = [50, 75, 90]
  suspend_triggers           = [95]
  suspend_immediate_triggers = [100]

  notify_users = ["PLATFORM_ADMIN"]
}

# =============================================================================
# 2. PER-WAREHOUSE RESOURCE MONITORS
# =============================================================================

resource "snowflake_resource_monitor" "ingestion" {
  name            = "RM_${local.env}_INGESTION_WH"
  credit_quota    = ceil(200 * local.quota_multiplier)
  frequency       = "MONTHLY"
  start_timestamp = "IMMEDIATELY"

  notify_triggers            = [70, 90]
  suspend_triggers           = [100]
  suspend_immediate_triggers = [110]

  notify_users = ["PLATFORM_ADMIN"]
}

resource "snowflake_resource_monitor" "transformer" {
  name            = "RM_${local.env}_TRANSFORMER_WH"
  credit_quota    = ceil(1500 * local.quota_multiplier)
  frequency       = "MONTHLY"
  start_timestamp = "IMMEDIATELY"

  notify_triggers            = [60, 80, 95]
  suspend_triggers           = [100]
  suspend_immediate_triggers = [110]

  notify_users = ["PLATFORM_ADMIN"]
}

resource "snowflake_resource_monitor" "analytics" {
  name            = "RM_${local.env}_ANALYTICS_WH"
  credit_quota    = ceil(2000 * local.quota_multiplier)
  frequency       = "MONTHLY"
  start_timestamp = "IMMEDIATELY"

  notify_triggers            = [50, 75, 90]
  suspend_triggers           = [95]
  suspend_immediate_triggers = [100]

  notify_users = ["PLATFORM_ADMIN"]
}

resource "snowflake_resource_monitor" "ci" {
  name            = "RM_${local.env}_CI_WH"
  credit_quota    = ceil(100 * local.quota_multiplier)
  frequency       = "MONTHLY"
  start_timestamp = "IMMEDIATELY"

  notify_triggers            = [80]
  suspend_triggers           = [100]
  suspend_immediate_triggers = [110]

  notify_users = ["PLATFORM_ADMIN"]
}

resource "snowflake_resource_monitor" "dev" {
  name            = "RM_${local.env}_DEV_WH"
  credit_quota    = ceil(200 * local.quota_multiplier)
  frequency       = "MONTHLY"
  start_timestamp = "IMMEDIATELY"

  notify_triggers            = [75, 95]
  suspend_triggers           = [100]
  suspend_immediate_triggers = [110]

  notify_users = ["PLATFORM_ADMIN"]
}
