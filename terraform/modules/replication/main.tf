# =============================================================================
# Module: Replication — Failover Groups + Tiered Replication + Client Redirect
# Blueprint: Multi-Region Topology with Dangling Reference Prevention
#
# TIERED REPLICATION (FinOps Optimization):
#   Tier 1 (Critical): RAW_VAULT Hubs/active Sats — every 10 minutes
#   Tier 2 (Standard): BUSINESS_VAULT, ANALYTICS — hourly
#   Tier 3 (Archive):  AUDIT, historical data — daily
# =============================================================================

terraform {
  required_providers {
    snowflake = {
      source = "Snowflake-Labs/snowflake"
    }
  }
}

locals {
  has_secondaries = length(var.secondary_accounts) > 0
  is_prod         = var.environment == "prod"
  enable_repl     = local.has_secondaries && local.is_prod
}

# =============================================================================
# PRIMARY FAILOVER GROUP — Co-locates ALL dependent objects
# Prevents dangling references by including ROLES + NETWORK POLICIES +
# INTEGRATIONS alongside the 4 databases.
#
# Per blueprint: "security policies, the users associated with them, and the
# databases governed by them must be co-located within the same failover
# group hierarchy."
# =============================================================================

resource "snowflake_failover_group" "data_platform" {
  count = local.enable_repl ? 1 : 0

  name = "FG_DATA_PLATFORM"

  object_types = [
    "DATABASES",
    "ROLES",
    "WAREHOUSES",
    "NETWORK POLICIES",
    "INTEGRATIONS",
  ]

  # Co-locate governance database with data databases to prevent dangling refs
  # (masking policies in GOVERNANCE schema referenced by RAW_VAULT tables)
  allowed_databases = [
    "RAW_VAULT",
    "BUSINESS_VAULT",
    "ANALYTICS",
    "AUDIT",
  ]

  allowed_accounts = var.secondary_accounts

  replication_schedule {
    cron {
      expression = "0 */1 * * *"  # Tier 2: hourly for standard data
      time_zone  = "UTC"
    }
  }
}

# =============================================================================
# TIER 1 — Critical Data Replication (Every 10 Minutes)
# RAW_VAULT: Hubs, active Satellites, CDC streams
# Minimizes RPO for business-critical data.
# =============================================================================

resource "snowflake_failover_group" "tier1_critical" {
  count = local.enable_repl ? 1 : 0

  name = "RG_TIER1_CRITICAL"

  object_types = [
    "DATABASES",
  ]

  allowed_databases = [
    "RAW_VAULT",
  ]

  allowed_accounts = var.secondary_accounts

  replication_schedule {
    cron {
      expression = "*/10 * * * *"  # Every 10 minutes
      time_zone  = "UTC"
    }
  }
}

# =============================================================================
# TIER 3 — Archive Data Replication (Daily at 03:00 UTC)
# AUDIT: Control plane data, DQ results, cost attribution
# Reduces cross-region egress costs for high-volume, low-urgency data.
# =============================================================================

resource "snowflake_failover_group" "tier3_archive" {
  count = local.enable_repl ? 1 : 0

  name = "RG_TIER3_ARCHIVE"

  object_types = [
    "DATABASES",
  ]

  allowed_databases = [
    "AUDIT",
  ]

  allowed_accounts = var.secondary_accounts

  replication_schedule {
    cron {
      expression = "0 3 * * *"  # Daily at 03:00 UTC
      time_zone  = "UTC"
    }
  }
}
