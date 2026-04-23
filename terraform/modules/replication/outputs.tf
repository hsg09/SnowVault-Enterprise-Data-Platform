# =============================================================================
# Module: Replication — Outputs
# =============================================================================

output "failover_group_name" {
  description = "Primary failover group name (null if replication disabled)"
  value       = local.enable_repl ? snowflake_failover_group.data_platform[0].name : null
}

output "tier1_group_name" {
  description = "Tier 1 critical replication group (10-min RPO)"
  value       = local.enable_repl ? snowflake_failover_group.tier1_critical[0].name : null
}

output "tier3_group_name" {
  description = "Tier 3 archive replication group (daily)"
  value       = local.enable_repl ? snowflake_failover_group.tier3_archive[0].name : null
}

output "replication_enabled" {
  description = "Whether cross-region replication is active"
  value       = local.enable_repl
}
